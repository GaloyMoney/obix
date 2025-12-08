use futures::Stream;
use serde::{Serialize, de::DeserializeOwned};
use std::{pin::Pin, sync::Arc, task::Poll};
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream};

use super::cache::CacheHandle;
use crate::out::event::EphemeralOutboxEvent;

pub struct EphemeralOutboxListener<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    event_receiver: BroadcastStream<Arc<EphemeralOutboxEvent<P>>>,
    backfill_receiver: Option<ReceiverStream<Arc<EphemeralOutboxEvent<P>>>>,
}

impl<P> EphemeralOutboxListener<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub(crate) fn new(mut cache_handle: CacheHandle<P>) -> Self {
        let event_receiver = BroadcastStream::new(cache_handle.ephemeral_event_stream());
        let backfill_receiver = Some(cache_handle.request_current_ephemeral_events());

        Self {
            event_receiver,
            backfill_receiver,
        }
    }
}

impl<P> Stream for EphemeralOutboxListener<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    type Item = Arc<EphemeralOutboxEvent<P>>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();

        // First, drain the backfill receiver if it exists
        if let Some(backfill_receiver) = this.backfill_receiver.as_mut() {
            match Pin::new(backfill_receiver).poll_next(cx) {
                Poll::Ready(Some(event)) => {
                    return Poll::Ready(Some(event));
                }
                Poll::Ready(None) => {
                    // Backfill is done
                    this.backfill_receiver = None;
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }

        // Then, poll the event receiver for new events
        match Pin::new(&mut this.event_receiver).poll_next(cx) {
            Poll::Ready(Some(Ok(event))) => Poll::Ready(Some(event)),
            Poll::Ready(Some(Err(_))) => {
                // On lagged error, just continue to next event
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
