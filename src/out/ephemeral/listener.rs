use futures::Stream;
use serde::{Serialize, de::DeserializeOwned};
use std::{pin::Pin, sync::Arc, task::Poll};
use tokio::sync::oneshot;
use tokio_stream::wrappers::BroadcastStream;

use super::cache::CacheHandle;
use crate::out::event::{EphemeralEventType, EphemeralOutboxEvent};

enum BackfillState<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    WaitingForHashMap(
        oneshot::Receiver<im::HashMap<EphemeralEventType, Arc<EphemeralOutboxEvent<P>>>>,
    ),
    IteratingHashMap(
        im::hashmap::ConsumingIter<(EphemeralEventType, Arc<EphemeralOutboxEvent<P>>)>,
    ),

    Done,
}

pub struct EphemeralOutboxListener<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    event_receiver: BroadcastStream<Arc<EphemeralOutboxEvent<P>>>,
    backfill_state: BackfillState<P>,
}

impl<P> EphemeralOutboxListener<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub(crate) fn new(mut cache_handle: CacheHandle<P>) -> Self {
        let event_receiver = BroadcastStream::new(cache_handle.ephemeral_event_stream());
        let backfill_receiver = cache_handle.request_current_ephemeral_events();
        Self {
            event_receiver,
            backfill_state: BackfillState::WaitingForHashMap(backfill_receiver),
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

        // Handle backfill state machine
        loop {
            match &mut this.backfill_state {
                BackfillState::WaitingForHashMap(receiver) => {
                    match Pin::new(receiver).poll(cx) {
                        Poll::Ready(Ok(hashmap)) => {
                            this.backfill_state =
                                BackfillState::IteratingHashMap(hashmap.into_iter());
                            continue;
                        }
                        Poll::Ready(Err(_)) => {
                            // Failed to receive HashMap, skip backfill
                            this.backfill_state = BackfillState::Done;
                            continue;
                        }
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                    }
                }
                BackfillState::IteratingHashMap(iter) => {
                    if let Some((_, event)) = iter.next() {
                        return Poll::Ready(Some(event));
                    } else {
                        this.backfill_state = BackfillState::Done;
                        continue;
                    }
                }
                BackfillState::Done => {
                    break;
                }
            }
        }

        // Poll the event receiver for new events
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
