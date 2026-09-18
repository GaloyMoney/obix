use futures::Stream;
use serde::{Serialize, de::DeserializeOwned};
use std::{collections::BTreeMap, pin::Pin, task::Poll};
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream, errors::BroadcastStreamRecvError};

use crate::out::event::{EventDelivery, Transport, UndecodableDelivery};
use crate::out::lane::{CommitOrder, InsertOrder, Lane, LaneHandle};
use crate::out::subscription::singleton::Ordering;

/// Delivers one lane in order, from a cursor, with a bounded in-memory view.
/// One state machine for both lanes: a delivery arrives already positioned, so
/// only the streams differ — the commit lane is dense, the insert lane
/// gap-filled, so only the latter parks in the contiguity loop.
pub struct LaneListener<L, P>
where
    L: Lane,
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    last_returned: L::Position,
    latest_known: L::Position,
    event_receiver: BroadcastStream<Transport<L, P>>,
    buffer_size: usize,
    local_cache: BTreeMap<L::Position, Transport<L, P>>,
    handle: LaneHandle<L, P>,
    /// At most one request outstanding, serving its whole range: the backfill
    /// task parks on an unservable gap rather than terminating, so the listener
    /// never re-requests a range.
    backfill_receiver: Option<ReceiverStream<Transport<L, P>>>,
}

/// The insert-ordered lane: a contiguous, gap-filled
/// [`EventSequence`](crate::EventSequence).
pub type PersistentOutboxListener<P> = LaneListener<InsertOrder, P>;

/// The commit-ordered lane: a dense [`CommitSequence`](crate::CommitSequence),
/// with a source transaction's events contiguous and never split.
pub type CommitOrderedListener<P> = LaneListener<CommitOrder, P>;

impl<L, P> LaneListener<L, P>
where
    L: Lane,
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub(crate) fn new(
        mut handle: LaneHandle<L, P>,
        start_after: impl Into<Option<L::Position>>,
        buffer: usize,
    ) -> Self {
        let latest_known = handle.head();
        let start_after = start_after.into().unwrap_or(latest_known);
        Self {
            last_returned: start_after,
            latest_known,
            event_receiver: handle.event_stream(),
            local_cache: BTreeMap::new(),
            // At least one: the drain loop is guarded on remaining capacity,
            // so a zero-capacity cache would never poll the broadcast at all
            // — never registering a waker, never waking.
            buffer_size: buffer.max(1),
            handle,
            backfill_receiver: None,
        }
    }

    /// Take a delivery into the local view. Evicting the *highest* is what makes
    /// the backstop safe: never the event blocking the cursor.
    fn maybe_add_to_cache(&mut self, delivery: Transport<L, P>) {
        let position = delivery.position();
        self.latest_known = self.latest_known.max(position);
        if position > self.last_returned
            && self.local_cache.insert(position, delivery).is_none()
            && self.local_cache.len() > self.buffer_size
        {
            self.local_cache.pop_last();
        }
    }

    fn request_backfill(&mut self) {
        if self.backfill_receiver.is_none() {
            self.backfill_receiver = Some(self.handle.request_backfill(self.last_returned));
        }
    }
}

impl<P> PersistentOutboxListener<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// The same stream as the internal transport, for the sequencer: it reads
    /// each delivery's group and must not pay `into_item`'s `Err`-arm clone.
    pub(crate) fn transport_stream(
        handle: LaneHandle<InsertOrder, P>,
        start_after: impl Into<Option<<InsertOrder as Lane>::Position>>,
        buffer: usize,
    ) -> TransportStream<InsertOrder, P> {
        TransportStream(Self::new(handle, start_after, buffer))
    }
}

/// [`LaneListener`] yielding the internal transport; see
/// [`transport_stream`](PersistentOutboxListener::transport_stream).
pub(crate) struct TransportStream<L, P>(LaneListener<L, P>)
where
    L: Lane,
    P: Serialize + DeserializeOwned + Send + Sync + 'static;

impl<L, P> Stream for TransportStream<L, P>
where
    L: Lane,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    type Item = Transport<L, P>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0).poll_transport(cx)
    }
}

impl<L, P> LaneListener<L, P>
where
    L: Lane,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    fn poll_transport(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Transport<L, P>>> {
        let this = self.as_mut().get_mut();

        // Backfill first: it carries the lowest outstanding positions, so a
        // cache full of newer broadcast events must never starve it.
        let mut backfill_events = Vec::new();
        let mut backfill_done = false;
        let needed = L::next(this.last_returned);
        let mut can_deliver = this.local_cache.contains_key(&needed);
        while let Some(backfill_receiver) = this.backfill_receiver.as_mut() {
            // Stop pulling at capacity — but only once this poll is certain
            // to yield an event. Draining the channel dry every poll lets the
            // reader run arbitrarily far ahead of the consumer, and
            // everything past `buffer_size` is then evicted by
            // `maybe_add_to_cache` and re-read later. Leaving it in the
            // channel instead blocks the reader in `send`, which is the
            // backpressure the demand gate cannot supply on its own.
            //
            // The `can_deliver` half is what keeps that safe: breaking early
            // skips the poll that would register a waker, so it is only
            // sound when the delivery loop below returns `Ready` and the
            // consumer therefore polls again. Otherwise keep draining to
            // `Pending` — which registers the waker — so the backfill can
            // never be starved by a cache full of newer broadcast events.
            if can_deliver && this.local_cache.len() + backfill_events.len() >= this.buffer_size {
                break;
            }
            match Pin::new(backfill_receiver).poll_next(cx) {
                Poll::Ready(Some(event)) => {
                    can_deliver |= event.position() == needed;
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
        // full cache holds positions above the cursor, so either the next one
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
                    record_lagged::<L>(
                        n,
                        L::position_to_u64(this.last_returned),
                        L::position_to_u64(this.latest_known),
                    );
                }
                Poll::Pending => break,
            }
        }

        // INVARIANT: refresh from the head BEFORE the pop loop, or a listener
        // that lagged out of the broadcast parks until the next broadcast.
        this.latest_known = this.latest_known.max(this.handle.head());

        while let Some((position, event)) = this.local_cache.pop_first() {
            if position <= this.last_returned {
                continue;
            }
            if position == L::next(this.last_returned) {
                this.last_returned = position;
                return Poll::Ready(Some(event));
            }
            this.local_cache.insert(position, event);
            break;
        }

        if this.last_returned < this.latest_known && this.backfill_receiver.is_none() {
            this.request_backfill();
            // need to register the cx with the backfill_receiver to get woken up
            return self.poll_transport(cx);
        }

        Poll::Pending
    }
}

impl<L, P> Stream for LaneListener<L, P>
where
    L: Lane,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    /// An undecodable event is yielded as the `Err` arm at the position it
    /// occupies, so moving past it is the consumer's explicit decision.
    type Item = Result<EventDelivery<P, L>, UndecodableDelivery<L>>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_transport(cx)
            .map(|delivery| delivery.map(Transport::into_item))
    }
}

/// One span name per lane: `#[instrument]`'s `name` takes a literal, so the lane
/// picks the recorder rather than supplying the string.
fn record_lagged<L: Lane>(dropped: u64, last_returned: u64, latest_known: u64) {
    match L::ORDERING {
        Ordering::Insert => record_persistent_lagged(dropped, last_returned, latest_known),
        Ordering::Commit => record_commit_lagged(dropped, last_returned, latest_known),
    }
}

#[tracing::instrument(name = "obix.persistent_listener.lagged", level = "warn")]
fn record_persistent_lagged(dropped: u64, last_returned_sequence: u64, latest_known: u64) {}

#[tracing::instrument(name = "obix.commit_listener.lagged", level = "warn")]
fn record_commit_lagged(dropped: u64, last_returned: u64, latest_known: u64) {}
