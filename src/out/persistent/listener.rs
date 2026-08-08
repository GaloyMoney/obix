use futures::Stream;
use serde::{Serialize, de::DeserializeOwned};
use std::{collections::BTreeMap, pin::Pin, sync::Arc, task::Poll};
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream, errors::BroadcastStreamRecvError};

use super::cache::CacheHandle;
use crate::out::event::{PersistentDelivery, PersistentOutboxEvent, UndecodableEventError};
use crate::sequence::EventSequence;

pub struct PersistentOutboxListener<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    last_returned_sequence: EventSequence,
    latest_known: EventSequence,
    event_receiver: BroadcastStream<PersistentDelivery<P>>,
    buffer_size: usize,
    local_cache: BTreeMap<EventSequence, PersistentDelivery<P>>,
    cache_handle: CacheHandle<P>,
    backfill_receiver: Option<ReceiverStream<PersistentDelivery<P>>>,
    /// Whether the current backfill round has contributed anything new to
    /// the local cache. Folded into [`Self::backfill_barren`] when the
    /// round completes.
    backfill_progress: bool,
    /// Set when a backfill round completes without contributing anything;
    /// cleared by any live-broadcast signal (delivery or lag). While set,
    /// no new backfill is requested: a listener parked at a frontier gap
    /// would otherwise re-request in a tight loop — backfill reads
    /// (deliberately, since the age-gated fill redesign) cannot resolve a
    /// young gap, whose resolution always arrives through the live
    /// broadcast once the writer commits or the grace-gated fill
    /// placeholder lands. Everything below the broadcast cursor is
    /// committed-or-placeholder in the table, so a barren round means
    /// there is nothing more the database can currently add.
    backfill_barren: bool,
}

impl<P> PersistentOutboxListener<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub(crate) fn new(
        mut cache_handle: CacheHandle<P>,
        start_after: impl Into<Option<EventSequence>>,
        buffer: usize,
    ) -> Self {
        let latest_known = cache_handle.latest_known_persisted();
        let start_after = start_after.into().unwrap_or(latest_known);
        Self {
            last_returned_sequence: start_after,
            latest_known,
            event_receiver: cache_handle.persistent_event_stream(),
            local_cache: BTreeMap::new(),
            buffer_size: buffer,
            cache_handle,
            backfill_receiver: None,
            backfill_progress: false,
            backfill_barren: false,
        }
    }

    /// Returns whether the delivery contributed anything new (it was
    /// neither behind the returned cursor nor already buffered).
    fn maybe_add_to_cache(&mut self, delivery: PersistentDelivery<P>) -> bool {
        let sequence = delivery.sequence();
        self.latest_known = self.latest_known.max(sequence);
        if sequence <= self.last_returned_sequence {
            return false;
        }
        if self.local_cache.insert(sequence, delivery).is_some() {
            return false;
        }
        if self.local_cache.len() > self.buffer_size {
            self.local_cache.pop_last();
        }
        true
    }

    fn request_backfill(&mut self) {
        if self.backfill_receiver.is_none() {
            self.backfill_progress = false;
            self.backfill_receiver = Some(
                self.cache_handle
                    .request_old_persistent_events(self.last_returned_sequence),
            );
        }
    }
}

impl<P> Stream for PersistentOutboxListener<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    /// An undecodable event is yielded as the `Err` arm, in its sequence
    /// position — the delivery of that event in degraded form. The stream
    /// continues past it; whether the *consumer* moves past it is the
    /// consumer's explicit decision (`?` fails loudly).
    type Item = Result<Arc<PersistentOutboxEvent<P>>, UndecodableEventError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();

        loop {
            match Pin::new(&mut this.event_receiver).poll_next(cx) {
                Poll::Ready(None) => break,
                Poll::Ready(Some(Ok(event))) => {
                    // Any live signal is new information: a barren backfill
                    // round no longer proves the database has nothing to
                    // add, so re-requesting is allowed again.
                    this.backfill_barren = false;
                    this.maybe_add_to_cache(event);
                }
                Poll::Ready(Some(Err(BroadcastStreamRecvError::Lagged(n)))) => {
                    this.backfill_barren = false;
                    record_lagged(
                        n,
                        u64::from(this.last_returned_sequence),
                        u64::from(this.latest_known),
                    );
                }
                Poll::Pending => break,
            }
        }

        let mut backfill_events = Vec::new();
        let mut backfill_done = false;
        while let Some(backfill_receiver) = this.backfill_receiver.as_mut() {
            match Pin::new(backfill_receiver).poll_next(cx) {
                Poll::Ready(Some(event)) => {
                    backfill_events.push(event);
                }
                Poll::Ready(None) => {
                    backfill_done = true;
                    break;
                }
                Poll::Pending => break,
            }
        }

        for event in backfill_events {
            if this.maybe_add_to_cache(event) {
                this.backfill_progress = true;
            }
        }
        if backfill_done {
            this.backfill_receiver = None;
            this.backfill_barren = !this.backfill_progress;
        }

        while let Some((seq, event)) = this.local_cache.pop_first() {
            if seq <= this.last_returned_sequence {
                continue;
            }
            if seq == this.last_returned_sequence.next() {
                this.last_returned_sequence = seq;
                return Poll::Ready(Some(event.into_item()));
            }
            this.local_cache.insert(seq, event);
            break;
        }

        if this.last_returned_sequence < this.latest_known
            && this.backfill_receiver.is_none()
            && !this.backfill_barren
        {
            this.request_backfill();
            // need to register the cx with the backfill_receiver to get woken up
            return self.poll_next(cx);
        }

        Poll::Pending
    }
}

#[tracing::instrument(name = "obix.persistent_listener.lagged", level = "warn")]
fn record_lagged(dropped: u64, last_returned_sequence: u64, latest_known: u64) {}
