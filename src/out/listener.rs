use futures::{FutureExt, Stream, StreamExt};
use serde::{Serialize, de::DeserializeOwned};
use tokio::{sync::broadcast, task::JoinHandle};
use tokio_stream::wrappers::{BroadcastStream, errors::BroadcastStreamRecvError};

use std::{collections::BTreeMap, pin::Pin, sync::Arc, task::Poll};

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
    cache: BTreeMap<EventSequence, PersistentOutboxEvent<P>>,
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
    ) -> Self {
        Self {
            last_returned_sequence: start_after,
            latest_known,
            event_receiver: Box::pin(BroadcastStream::new(event_receiver)),
            cache: BTreeMap::new(),
            buffer_size: buffer,
        }
    }

    fn maybe_add_to_cache(&mut self, event: impl Into<PersistentOutboxEvent<P>>) {
        let event = event.into();
        self.latest_known = self.latest_known.max(event.sequence);

        if event.sequence > self.last_returned_sequence
            && self.cache.insert(event.sequence, event).is_none()
            && self.cache.len() > self.buffer_size
        {
            self.cache.pop_last();
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
        unimplemented!()
    }
}
