use futures::Stream;
use serde::{Serialize, de::DeserializeOwned};
use std::{collections::BTreeMap, pin::Pin, sync::Arc, task::Poll};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::{BroadcastStream, errors::BroadcastStreamRecvError};

use super::cache::CacheHandle;
use super::event::PersistentOutboxEvent;
use crate::sequence::EventSequence;

pub struct OutboxListener<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    last_returned_sequence: EventSequence,
    latest_known: EventSequence,
    event_receiver: Pin<Box<BroadcastStream<Arc<PersistentOutboxEvent<P>>>>>,
    buffer_size: usize,
    local_cache: BTreeMap<EventSequence, Arc<PersistentOutboxEvent<P>>>,
    cache_handle: CacheHandle<P>,
    backfill_receiver: Option<mpsc::Receiver<Arc<PersistentOutboxEvent<P>>>>,
}

impl<P> OutboxListener<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub(super) fn new(
        event_receiver: broadcast::Receiver<Arc<PersistentOutboxEvent<P>>>,
        start_after: EventSequence,
        latest_known: EventSequence,
        buffer: usize,
        cache_handle: CacheHandle<P>,
    ) -> Self {
        Self {
            last_returned_sequence: start_after,
            latest_known,
            event_receiver: Box::pin(BroadcastStream::new(event_receiver)),
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

impl<P> Stream for OutboxListener<P>
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
            match this.event_receiver.as_mut().poll_next(cx) {
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
        if let Some(ref mut receiver) = this.backfill_receiver {
            loop {
                match Pin::new(&mut *receiver).poll_recv(cx) {
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
