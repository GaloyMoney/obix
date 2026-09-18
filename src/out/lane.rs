//! The delivery lane as a type: [`InsertOrder`] positions deliveries by
//! [`crate::EventSequence`], [`CommitOrder`] by [`crate::CommitSequence`]. The
//! lane is a type parameter on everything a subscriber touches, so the compiler
//! refuses to run a commit-lane handler on the insert lane.

use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream};

use std::sync::{
    Arc,
    atomic::{self, AtomicU64},
};
use std::time::Duration;

use super::Outbox;
use super::event::Transport;
use super::subscription::singleton::{LaneChoice, Ordering, decide_lane};
use super::subscription::{
    StreamPosition, Subscription, SubscriptionError, await_caught_up_commit_lane,
    await_caught_up_insert_lane, read_frontier,
};
use crate::config::{CommitLaneDisabled, FrontierError};
use crate::sequence::{CommitSequence, EventSequence};
use crate::tables::MailboxTables;

pub(crate) mod sealed {
    /// Seals [`Lane`](super::Lane): exactly two lanes exist.
    pub trait Sealed {}
}

/// A delivery lane, and with it what a "position" means.
///
/// Sealed — the two implementors are [`InsertOrder`] and [`CommitOrder`].
pub trait Lane: sealed::Sealed + Sized + Send + Sync + 'static {
    /// Where a delivery sits on this lane.
    type Position: Copy
        + Ord
        + std::fmt::Debug
        + std::fmt::Display
        + Into<StreamPosition>
        + Send
        + Sync
        + Unpin
        + 'static;

    /// The dynamic name of this lane, as stored state reports it.
    const ORDERING: Ordering;

    #[doc(hidden)]
    fn require<P, Tables>(outbox: &Outbox<P, Tables>) -> Result<(), CommitLaneDisabled>
    where
        P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
        Tables: MailboxTables;

    /// This lane's source handle on `outbox`: the insert cache or the sequencer.
    #[doc(hidden)]
    fn handle<P, Tables>(
        outbox: &Outbox<P, Tables>,
    ) -> Result<LaneHandle<Self, P>, CommitLaneDisabled>
    where
        P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
        Tables: MailboxTables;

    #[doc(hidden)]
    fn begin() -> Self::Position;

    #[doc(hidden)]
    fn next(position: Self::Position) -> Self::Position;

    #[doc(hidden)]
    fn position_to_u64(position: Self::Position) -> u64;

    #[doc(hidden)]
    fn position_from_u64(value: u64) -> Self::Position;

    #[doc(hidden)]
    fn checkpoint(sequence: EventSequence, commit: Option<CommitSequence>) -> Self::Position;

    #[doc(hidden)]
    fn resume_from(
        sequence: EventSequence,
        commit: Option<CommitSequence>,
    ) -> Result<Self::Position, String>;

    #[doc(hidden)]
    fn record(commit_cursor: &mut Option<CommitSequence>, position: Self::Position);

    /// This lane's frontier: the sequence generator, or this process's fold head.
    #[doc(hidden)]
    fn frontier<'a, P, Tables>(
        subscription: &'a Subscription<P, Tables, Self>,
    ) -> impl std::future::Future<Output = Result<Self::Position, SubscriptionError>> + Send + 'a
    where
        P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
        Tables: MailboxTables;

    #[doc(hidden)]
    fn outbox_frontier<'a, P, Tables>(
        outbox: &'a Outbox<P, Tables>,
    ) -> impl std::future::Future<Output = Result<Self::Position, FrontierError>> + Send + 'a
    where
        P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
        Tables: MailboxTables;

    #[doc(hidden)]
    fn await_caught_up<'a, P, Tables>(
        subscription: &'a Subscription<P, Tables, Self>,
        timeout: Duration,
    ) -> impl std::future::Future<Output = Result<(), SubscriptionError>> + Send + 'a
    where
        P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
        Tables: MailboxTables;
}

/// Insert order: a contiguous, gap-filled [`EventSequence`]. The default lane.
pub struct InsertOrder;

/// Commit order: a dense [`CommitSequence`]. A source transaction's events
/// arrive contiguously and a batch flush never splits one. Requires
/// [`commit_lane`](crate::MailboxConfig::commit_lane) =
/// [`Enabled`](crate::CommitLane::Enabled), and a singleton subscriber.
pub struct CommitOrder;

impl sealed::Sealed for InsertOrder {}
impl sealed::Sealed for CommitOrder {}

impl Lane for InsertOrder {
    type Position = EventSequence;
    const ORDERING: Ordering = Ordering::Insert;

    fn require<P, Tables>(_outbox: &Outbox<P, Tables>) -> Result<(), CommitLaneDisabled>
    where
        P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
        Tables: MailboxTables,
    {
        Ok(())
    }

    fn handle<P, Tables>(
        outbox: &Outbox<P, Tables>,
    ) -> Result<LaneHandle<Self, P>, CommitLaneDisabled>
    where
        P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
        Tables: MailboxTables,
    {
        Ok(outbox.persistent_cache_handle())
    }

    fn begin() -> EventSequence {
        EventSequence::BEGIN
    }

    fn next(position: EventSequence) -> EventSequence {
        position.next()
    }

    fn position_to_u64(position: EventSequence) -> u64 {
        u64::from(position)
    }

    fn position_from_u64(value: u64) -> EventSequence {
        EventSequence::from(value)
    }

    fn checkpoint(sequence: EventSequence, _commit: Option<CommitSequence>) -> EventSequence {
        sequence
    }

    fn resume_from(
        sequence: EventSequence,
        commit: Option<CommitSequence>,
    ) -> Result<EventSequence, String> {
        decide_lane(commit, sequence, Self::ORDERING)?
            .insert()
            .ok_or_else(|| lane_choice_mismatch(Self::ORDERING))
    }

    fn record(_commit_cursor: &mut Option<CommitSequence>, _position: EventSequence) {}

    async fn frontier<P, Tables>(
        subscription: &Subscription<P, Tables, Self>,
    ) -> Result<EventSequence, SubscriptionError>
    where
        P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
        Tables: MailboxTables,
    {
        Ok(read_frontier::<Tables>(subscription.pool()).await?)
    }

    async fn outbox_frontier<P, Tables>(
        outbox: &Outbox<P, Tables>,
    ) -> Result<EventSequence, FrontierError>
    where
        P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
        Tables: MailboxTables,
    {
        Ok(read_frontier::<Tables>(outbox.pool()).await?)
    }

    fn await_caught_up<'a, P, Tables>(
        subscription: &'a Subscription<P, Tables, Self>,
        timeout: Duration,
    ) -> impl std::future::Future<Output = Result<(), SubscriptionError>> + Send + 'a
    where
        P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
        Tables: MailboxTables,
    {
        await_caught_up_insert_lane(subscription, timeout)
    }
}

impl Lane for CommitOrder {
    type Position = CommitSequence;
    const ORDERING: Ordering = Ordering::Commit;

    fn require<P, Tables>(outbox: &Outbox<P, Tables>) -> Result<(), CommitLaneDisabled>
    where
        P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
        Tables: MailboxTables,
    {
        outbox.commit_lane()?;
        Ok(())
    }

    fn handle<P, Tables>(
        outbox: &Outbox<P, Tables>,
    ) -> Result<LaneHandle<Self, P>, CommitLaneDisabled>
    where
        P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
        Tables: MailboxTables,
    {
        Ok(outbox.commit_lane()?.handle())
    }

    fn begin() -> CommitSequence {
        CommitSequence::BEGIN
    }

    fn next(position: CommitSequence) -> CommitSequence {
        position.next()
    }

    fn position_to_u64(position: CommitSequence) -> u64 {
        u64::from(position)
    }

    fn position_from_u64(value: u64) -> CommitSequence {
        CommitSequence::from(value)
    }

    fn checkpoint(_sequence: EventSequence, commit: Option<CommitSequence>) -> CommitSequence {
        commit.unwrap_or_default()
    }

    fn resume_from(
        sequence: EventSequence,
        commit: Option<CommitSequence>,
    ) -> Result<CommitSequence, String> {
        decide_lane(commit, sequence, Self::ORDERING)?
            .commit()
            .ok_or_else(|| lane_choice_mismatch(Self::ORDERING))
    }

    fn record(commit_cursor: &mut Option<CommitSequence>, position: CommitSequence) {
        *commit_cursor = Some(position);
    }

    async fn frontier<P, Tables>(
        subscription: &Subscription<P, Tables, Self>,
    ) -> Result<CommitSequence, SubscriptionError>
    where
        P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
        Tables: MailboxTables,
    {
        subscription.sequencer_positions().map(|p| p.commit_head())
    }

    async fn outbox_frontier<P, Tables>(
        outbox: &Outbox<P, Tables>,
    ) -> Result<CommitSequence, FrontierError>
    where
        P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
        Tables: MailboxTables,
    {
        Ok(outbox.commit_lane()?.positions().commit_head())
    }

    fn await_caught_up<'a, P, Tables>(
        subscription: &'a Subscription<P, Tables, Self>,
        timeout: Duration,
    ) -> impl std::future::Future<Output = Result<(), SubscriptionError>> + Send + 'a
    where
        P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
        Tables: MailboxTables,
    {
        await_caught_up_commit_lane(subscription, timeout)
    }
}

/// What a listener needs from whichever source produces its lane: the head,
/// the live fan-out, and a way to ask for what it missed.
pub struct LaneHandle<L, P>
where
    L: Lane,
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    head: Arc<AtomicU64>,
    receiver: Option<broadcast::Receiver<Transport<L, P>>>,
    backfill_request: BackfillRequests<L, P>,
    backfill_buffer_size: usize,
}

/// A listener's "serve me everything after this position" channel.
pub type BackfillRequests<L, P> =
    mpsc::UnboundedSender<(<L as Lane>::Position, mpsc::Sender<Transport<L, P>>)>;

impl<L, P> LaneHandle<L, P>
where
    L: Lane,
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub(crate) fn new(
        head: Arc<AtomicU64>,
        receiver: broadcast::Receiver<Transport<L, P>>,
        backfill_request: BackfillRequests<L, P>,
        backfill_buffer_size: usize,
    ) -> Self {
        Self {
            head,
            receiver: Some(receiver),
            backfill_request,
            backfill_buffer_size,
        }
    }

    /// The highest position this source has handed out.
    pub(crate) fn head(&self) -> L::Position {
        L::position_from_u64(self.head.load(atomic::Ordering::Relaxed))
    }

    /// Take the live fan-out. Once per handle — a listener owns its receiver.
    pub(crate) fn event_stream(&mut self) -> BroadcastStream<Transport<L, P>> {
        BroadcastStream::new(self.receiver.take().expect("receiver already taken"))
    }

    pub(crate) fn request_backfill(
        &self,
        start_after: L::Position,
    ) -> ReceiverStream<Transport<L, P>> {
        let (tx, rx) = mpsc::channel(self.backfill_buffer_size);
        let _ = self.backfill_request.send((start_after, tx));
        ReceiverStream::new(rx)
    }
}

/// Unreachable: `decide_lane` is total on each `(state, ordering)` pair.
fn lane_choice_mismatch(ordering: Ordering) -> String {
    format!("lane resolution yielded the other lane's cursor for Ordering::{ordering:?}")
}

impl LaneChoice {
    pub(crate) fn insert(self) -> Option<EventSequence> {
        match self {
            Self::Insert(sequence) => Some(sequence),
            Self::Commit(_) => None,
        }
    }

    pub(crate) fn commit(self) -> Option<CommitSequence> {
        match self {
            Self::Commit(commit_sequence) => Some(commit_sequence),
            Self::Insert(_) => None,
        }
    }
}
