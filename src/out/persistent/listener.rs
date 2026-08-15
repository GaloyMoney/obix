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
    /// At most one backfill request is ever outstanding, and one request
    /// serves its whole range: the cache's backfill task delivers the
    /// range in order and *parks* on any not-yet-servable gap (in-flight
    /// writer, raced fill) until it resolves, rather than terminating.
    /// The listener therefore never re-requests a range it already asked
    /// for — gap semantics stay entirely inside the backfill task.
    backfill_receiver: Option<ReceiverStream<PersistentDelivery<P>>>,
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
            // At least one: the drain loop is guarded on remaining capacity,
            // so a zero-capacity cache would never poll the broadcast at all
            // — never registering a waker, never waking.
            buffer_size: buffer.max(1),
            cache_handle,
            backfill_receiver: None,
        }
    }

    /// Take an event into the local view.
    ///
    /// Eviction is a backstop: only the unguarded backfill drain can exceed
    /// `buffer_size`, and its events must always be accepted. Dropping the
    /// *highest* is what makes that safe — the least urgent event held, never
    /// the one blocking the cursor.
    fn maybe_add_to_cache(&mut self, delivery: PersistentDelivery<P>) {
        let sequence = delivery.sequence();
        self.latest_known = self.latest_known.max(sequence);
        if sequence > self.last_returned_sequence
            && self.local_cache.insert(sequence, delivery).is_none()
            && self.local_cache.len() > self.buffer_size
        {
            self.local_cache.pop_last();
        }
    }

    fn request_backfill(&mut self) {
        if self.backfill_receiver.is_none() {
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

        // Backfill first and without a capacity guard: it carries the lowest
        // outstanding sequences, so a cache full of newer broadcast events
        // must never starve it. This also registers its waker before the
        // guarded loop below can return without registering one.
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

        if backfill_done {
            this.backfill_receiver = None;
        }
        for event in backfill_events {
            this.maybe_add_to_cache(event);
        }

        // Then take from the broadcast only while there is room, so an
        // overflow surfaces as a visible `Lagged` rather than a silent drop.
        //
        // Breaking out on a full cache without registering a waker is safe: a
        // full cache holds sequences above the cursor, so either the next one
        // is contiguous and this poll returns an event (the consumer polls
        // again), or it is not and the backfill request below registers a
        // waker for the range that unblocks it.
        while this.local_cache.len() < this.buffer_size {
            match Pin::new(&mut this.event_receiver).poll_next(cx) {
                Poll::Ready(None) => break,
                Poll::Ready(Some(Ok(event))) => {
                    this.maybe_add_to_cache(event);
                }
                Poll::Ready(Some(Err(BroadcastStreamRecvError::Lagged(n)))) => {
                    record_lagged(
                        n,
                        u64::from(this.last_returned_sequence),
                        u64::from(this.latest_known),
                    );
                }
                Poll::Pending => break,
            }
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

        if this.last_returned_sequence < this.latest_known && this.backfill_receiver.is_none() {
            this.request_backfill();
            // need to register the cx with the backfill_receiver to get woken up
            return self.poll_next(cx);
        }

        Poll::Pending
    }
}

#[tracing::instrument(name = "obix.persistent_listener.lagged", level = "warn")]
fn record_lagged(dropped: u64, last_returned_sequence: u64, latest_known: u64) {}
