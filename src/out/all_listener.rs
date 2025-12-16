use futures::Stream;
use serde::{Serialize, de::DeserializeOwned};
use std::{pin::Pin, task::Poll};

use super::{
    ephemeral::{CacheHandle as EphemeralCacheHandle, EphemeralOutboxListener},
    event::OutboxEvent,
    persistent::{CacheHandle as PersistentCacheHandle, PersistentOutboxListener},
};
use crate::sequence::EventSequence;

pub struct AllOutboxListener<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    persistent_listener: PersistentOutboxListener<P>,
    ephemeral_listener: EphemeralOutboxListener<P>,
    poll_count: usize,
    alt_attempt: bool,
}

impl<P> AllOutboxListener<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub(crate) fn new(
        persistent_handle: PersistentCacheHandle<P>,
        ephemeral_handle: EphemeralCacheHandle<P>,
        start_after: impl Into<Option<EventSequence>>,
        event_buffer_size: usize,
    ) -> Self {
        Self {
            persistent_listener: PersistentOutboxListener::new(
                persistent_handle,
                start_after,
                event_buffer_size,
            ),
            ephemeral_listener: EphemeralOutboxListener::new(ephemeral_handle),
            poll_count: 0,
            alt_attempt: false,
        }
    }
}

impl<P> Stream for AllOutboxListener<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    type Item = OutboxEvent<P>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();

        let result = if this.poll_count.is_multiple_of(2) {
            Pin::new(&mut this.persistent_listener)
                .poll_next(cx)
                .map(|opt| opt.map(OutboxEvent::Persistent))
        } else {
            Pin::new(&mut this.ephemeral_listener)
                .poll_next(cx)
                .map(|opt| opt.map(OutboxEvent::Ephemeral))
        };

        this.poll_count = this.poll_count.wrapping_add(1);

        match result {
            Poll::Ready(Some(event)) => Poll::Ready(Some(event)),
            _ if !this.alt_attempt => {
                this.alt_attempt = true;
                let ret = self.as_mut().poll_next(cx);
                self.get_mut().alt_attempt = false;
                ret
            }
            other => other,
        }
    }
}
