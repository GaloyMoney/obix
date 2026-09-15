use futures::Stream;
use serde::{Serialize, de::DeserializeOwned};
use std::{collections::BTreeMap, pin::Pin, sync::Arc, task::Poll};
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream, errors::BroadcastStreamRecvError};

use super::cache::CommitLaneHandle;
use crate::out::event::{PersistentDelivery, PersistentOutboxEvent, UndecodableEventError};
use crate::sequence::CommitSequence;

/// Delivers events in commit order: a source transaction's events arrive
/// contiguously and are never split.
///
/// Simpler than [`PersistentOutboxListener`](super::PersistentOutboxListener)
/// because the commit log is dense: nothing parks on a gap and no gap is ever
/// reported.
pub struct CommitOrderedListener<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    last_returned: CommitSequence,
    latest_known: CommitSequence,
    event_receiver: BroadcastStream<PersistentDelivery<P>>,
    buffer_size: usize,
    local_cache: BTreeMap<CommitSequence, PersistentDelivery<P>>,
    handle: CommitLaneHandle<P>,
    backfill_receiver: Option<ReceiverStream<PersistentDelivery<P>>>,
}

impl<P> CommitOrderedListener<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub(crate) fn new(
        mut handle: CommitLaneHandle<P>,
        start_after: impl Into<Option<CommitSequence>>,
        buffer: usize,
    ) -> Self {
        let latest_known = handle.head();
        let start_after = start_after.into().unwrap_or(latest_known);
        Self {
            last_returned: start_after,
            latest_known,
            event_receiver: handle.commit_event_stream(),
            local_cache: BTreeMap::new(),
            buffer_size: buffer.max(1),
            handle,
            backfill_receiver: None,
        }
    }

    /// Take a delivery into the local view, dropping the highest on overflow:
    /// the least urgent held, never the one blocking the cursor.
    fn maybe_add_to_cache(&mut self, delivery: PersistentDelivery<P>) {
        let Some(commit_sequence) = delivery.commit_sequence() else {
            return;
        };
        self.latest_known = self.latest_known.max(commit_sequence);
        if commit_sequence > self.last_returned
            && self.local_cache.insert(commit_sequence, delivery).is_none()
            && self.local_cache.len() > self.buffer_size
        {
            self.local_cache.pop_last();
        }
    }

    fn request_backfill(&mut self) {
        if self.backfill_receiver.is_none() {
            self.backfill_receiver = Some(self.handle.request_commit_backfill(self.last_returned));
        }
    }
}

impl<P> Stream for CommitOrderedListener<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    type Item = Result<Arc<PersistentOutboxEvent<P>>, UndecodableEventError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();

        let mut backfill_events = Vec::new();
        let mut backfill_done = false;
        let needed = this.last_returned.next();
        let mut can_deliver = this.local_cache.contains_key(&needed);
        while let Some(backfill_receiver) = this.backfill_receiver.as_mut() {
            if can_deliver && this.local_cache.len() + backfill_events.len() >= this.buffer_size {
                break;
            }
            match Pin::new(backfill_receiver).poll_next(cx) {
                Poll::Ready(Some(event)) => {
                    can_deliver |= event.commit_sequence() == Some(needed);
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

        while this.local_cache.len() < this.buffer_size {
            match Pin::new(&mut this.event_receiver).poll_next(cx) {
                Poll::Ready(None) => break,
                Poll::Ready(Some(Ok(event))) => {
                    this.maybe_add_to_cache(event);
                }
                Poll::Ready(Some(Err(BroadcastStreamRecvError::Lagged(n)))) => {
                    record_lagged(
                        n,
                        u64::from(this.last_returned),
                        u64::from(this.latest_known),
                    );
                }
                Poll::Pending => break,
            }
        }

        this.latest_known = this.latest_known.max(this.handle.head());

        while let Some((commit_sequence, event)) = this.local_cache.pop_first() {
            if commit_sequence <= this.last_returned {
                continue;
            }
            if commit_sequence == this.last_returned.next() {
                this.last_returned = commit_sequence;
                return Poll::Ready(Some(event.into_item()));
            }
            this.local_cache.insert(commit_sequence, event);
            break;
        }

        if this.last_returned < this.latest_known && this.backfill_receiver.is_none() {
            this.request_backfill();
            return self.poll_next(cx);
        }

        Poll::Pending
    }
}

#[tracing::instrument(name = "obix.commit_listener.lagged", level = "warn")]
fn record_lagged(dropped: u64, last_returned: u64, latest_known: u64) {}
