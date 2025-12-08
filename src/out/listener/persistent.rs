use futures::Stream;
use serde::{Serialize, de::DeserializeOwned};
use std::{collections::BTreeMap, pin::Pin, sync::Arc, task::Poll};
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream, errors::BroadcastStreamRecvError};

use crate::out::cache::CacheHandle;
use crate::out::event::PersistentOutboxEvent;
use crate::sequence::EventSequence;

pub struct PersistentOutboxListener<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    last_returned_sequence: EventSequence,
    latest_known: EventSequence,
    event_receiver: BroadcastStream<Arc<PersistentOutboxEvent<P>>>,
    buffer_size: usize,
    local_cache: BTreeMap<EventSequence, Arc<PersistentOutboxEvent<P>>>,
    cache_handle: CacheHandle<P>,
    backfill_receiver: Option<ReceiverStream<Arc<PersistentOutboxEvent<P>>>>,
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
        }
    }

    fn maybe_add_to_cache(&mut self, event: Arc<PersistentOutboxEvent<P>>) {
        self.latest_known = self.latest_known.max(event.sequence);
        if event.sequence > self.last_returned_sequence
            && self.local_cache.insert(event.sequence, event).is_none()
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
    type Item = Arc<PersistentOutboxEvent<P>>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();

        loop {
            match Pin::new(&mut this.event_receiver).poll_next(cx) {
                Poll::Ready(None) => {
                    return Poll::Ready(None);
                }
                Poll::Ready(Some(Ok(event))) => {
                    this.maybe_add_to_cache(event);
                }
                Poll::Ready(Some(Err(BroadcastStreamRecvError::Lagged(_)))) => (),
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

        if backfill_done {
            this.backfill_receiver = None;
        }
        for event in backfill_events {
            this.maybe_add_to_cache(event);
        }

        while let Some((seq, event)) = this.local_cache.pop_first() {
            if seq <= this.last_returned_sequence {
                continue;
            }
            if seq == this.last_returned_sequence.next() {
                this.last_returned_sequence = seq;
                return Poll::Ready(Some(event));
            }
            this.local_cache.insert(seq, event);
            break;
        }

        if this.last_returned_sequence < this.latest_known && this.backfill_receiver.is_none() {
            this.request_backfill();
        }

        Poll::Pending
    }
}
