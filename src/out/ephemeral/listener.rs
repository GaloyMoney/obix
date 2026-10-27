use futures::Stream;
use std::{pin::Pin, sync::Arc, task::Poll};
use tokio::sync::oneshot;
use tokio_stream::wrappers::{BroadcastStream, errors::BroadcastStreamRecvError};

use super::{
    cache::{CacheHandle, CachedEvent, CachedEvents},
    event::{EphemeralEventType, EphemeralOutboxEvent, EphemeralSelection, RawPayload},
};
use crate::record_ephemeral_payload_undecodable;

type Decode<P> = fn(&CachedEvent) -> Option<Arc<EphemeralOutboxEvent<P>>>;

enum BackfillState {
    WaitingForHashMap(oneshot::Receiver<CachedEvents>),
    Iterating(std::vec::IntoIter<CachedEvent>),
    Done,
}

/// A stream of the ephemeral events a listener asked for: the current row of
/// each of its keys first, then live rows in broadcast order.
///
/// A row whose payload does not decode as the listener's type is skipped and
/// recorded, never yielded.
pub struct EphemeralOutboxListener<P> {
    event_receiver: BroadcastStream<CachedEvent>,
    backfill_state: BackfillState,
    keys: &'static [EphemeralEventType],
    decode: Decode<P>,
}

impl<P> EphemeralOutboxListener<P> {
    pub(super) fn new(
        mut cache_handle: CacheHandle,
        keys: &'static [EphemeralEventType],
        decode: Decode<P>,
    ) -> Self {
        let event_receiver = BroadcastStream::new(cache_handle.ephemeral_event_stream());
        let backfill_receiver = cache_handle.request_current_ephemeral_events();
        Self {
            event_receiver,
            backfill_state: BackfillState::WaitingForHashMap(backfill_receiver),
            keys,
            decode,
        }
    }
}

pub(super) fn decode_selection<S: EphemeralSelection>(
    row: &CachedEvent,
) -> Option<Arc<EphemeralOutboxEvent<S>>> {
    match S::decode(&row.event_type, &row.payload)? {
        Ok(payload) => Some(Arc::new(EphemeralOutboxEvent {
            event_type: row.event_type.clone(),
            payload,
            tracing_context: row.tracing_context.clone(),
            recorded_at: row.recorded_at,
        })),
        Err(error) => {
            record_ephemeral_payload_undecodable(&error, row.event_type.as_str());
            None
        }
    }
}

pub(super) fn decode_raw(row: &CachedEvent) -> Option<Arc<EphemeralOutboxEvent<RawPayload>>> {
    Some(Arc::clone(row))
}

impl<P> Stream for EphemeralOutboxListener<P> {
    type Item = Arc<EphemeralOutboxEvent<P>>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();

        loop {
            match &mut this.backfill_state {
                BackfillState::WaitingForHashMap(receiver) => {
                    match Pin::new(receiver).poll(cx) {
                        Poll::Ready(Ok(hashmap)) => {
                            let keys = this.keys;
                            let rows: Vec<CachedEvent> = hashmap
                                .into_iter()
                                .filter(|(event_type, _)| keys.contains(event_type))
                                .map(|(_, row)| row)
                                .collect();
                            this.backfill_state = BackfillState::Iterating(rows.into_iter());
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
                BackfillState::Iterating(iter) => {
                    if let Some(row) = iter.next() {
                        if let Some(event) = (this.decode)(&row) {
                            return Poll::Ready(Some(event));
                        }
                        continue;
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

        loop {
            match Pin::new(&mut this.event_receiver).poll_next(cx) {
                Poll::Ready(Some(Ok(row))) => {
                    if !this.keys.contains(&row.event_type) {
                        continue;
                    }
                    if let Some(event) = (this.decode)(&row) {
                        return Poll::Ready(Some(event));
                    }
                    continue;
                }
                Poll::Ready(Some(Err(BroadcastStreamRecvError::Lagged(n)))) => {
                    record_lagged(n);
                    continue;
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[tracing::instrument(name = "obix.ephemeral_listener.lagged", level = "warn")]
fn record_lagged(dropped: u64) {}
