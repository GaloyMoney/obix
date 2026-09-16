//! The delivery lane as a type.
//!
//! obix delivers persistent events in two orders — insert order and commit
//! order — and the two differ in what a *position* is. Insert order numbers
//! events by the contiguous, gap-filled [`crate::EventSequence`] the database
//! allocated; commit order numbers them by the dense
//! [`crate::CommitSequence`] the sequencer assigned when it placed their
//! source transaction.
//!
//! A position is therefore a property of a **delivery on a lane**, never of
//! the event row: the same event has a commit position when it arrives on the
//! commit lane and none at all when it arrives on the insert lane, through
//! [`load`](crate::MailboxTables), or from an
//! [`OpCursor`](crate::OpCursor) before it is even written. Carrying it on
//! the event would be provenance masquerading as data.
//!
//! So the lane is a type parameter, defaulting to [`InsertOrder`], and it
//! decides the position type of everything a subscriber touches: the event
//! ([`crate::out::EventDelivery`]), the undecodable stand-in
//! ([`crate::out::UndecodableDelivery`]), the batch
//! flush ([`FlushOp`](crate::FlushOp)) and the durable checkpoint
//! ([`Subscription`]). Insert-lane code sees `EventSequence`, commit-lane
//! code sees `CommitSequence`, and the compiler refuses to run a commit-lane
//! handler on the insert lane.

use serde::{Serialize, de::DeserializeOwned};

use std::time::Duration;

use super::event::{EventDelivery, UndecodableDelivery};
use super::subscription::singleton::{LaneChoice, Ordering, decide_lane};
use super::subscription::{
    StreamPosition, Subscription, SubscriptionError, await_caught_up_commit_lane,
    await_caught_up_insert_lane, read_frontier,
};
use super::{CommitOrderedListener, Outbox, PersistentOutboxListener};
use crate::config::CommitLaneDisabled;
use crate::sequence::{CommitSequence, EventSequence};
use crate::tables::MailboxTables;

pub(crate) mod sealed {
    /// Seals [`Lane`](super::Lane): exactly two lanes exist, and a third
    /// would have to answer questions (what is a flush boundary? what does
    /// the checkpoint mean?) that the runner and the fence answer per lane.
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
        + 'static;

    /// The dynamic name of this lane, as stored state and read-outs report it.
    const ORDERING: Ordering;

    #[doc(hidden)]
    type Listener<P>: futures::Stream<Item = Result<EventDelivery<P, Self>, UndecodableDelivery<Self>>>
        + Send
        + Unpin
    where
        P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin;

    #[doc(hidden)]
    fn require<P, Tables>(outbox: &Outbox<P, Tables>) -> Result<(), CommitLaneDisabled>
    where
        P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
        Tables: MailboxTables;

    #[doc(hidden)]
    fn listen<P, Tables>(
        outbox: &Outbox<P, Tables>,
        start_after: Self::Position,
    ) -> Result<Self::Listener<P>, CommitLaneDisabled>
    where
        P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
        Tables: MailboxTables;

    #[doc(hidden)]
    fn checkpoint(sequence: EventSequence, commit: Option<CommitSequence>) -> Self::Position;

    #[doc(hidden)]
    fn resume_from(
        sequence: EventSequence,
        commit: Option<CommitSequence>,
    ) -> Result<Self::Position, String>;

    #[doc(hidden)]
    fn record(commit_cursor: &mut Option<CommitSequence>, position: Self::Position);

    #[doc(hidden)]
    fn frontier<Tables: MailboxTables>(
        pool: &sqlx::PgPool,
    ) -> impl std::future::Future<Output = Result<Self::Position, sqlx::Error>> + Send;

    #[doc(hidden)]
    fn await_caught_up<'a, P, Tables>(
        subscription: &'a Subscription<P, Tables, Self>,
        timeout: Duration,
    ) -> impl std::future::Future<Output = Result<(), SubscriptionError>> + Send + 'a
    where
        P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
        Tables: MailboxTables;
}

/// Insert order: a contiguous [`EventSequence`], gap-filled. The default lane,
/// and the one every subscriber used before the commit lane existed.
pub struct InsertOrder;

/// Commit order: a dense [`CommitSequence`]. A source transaction's events
/// arrive contiguously and a batch flush never splits one.
///
/// Available only on an outbox whose
/// [`commit_lane`](crate::MailboxConfig::commit_lane) is
/// [`Enabled`](crate::CommitLane::Enabled), and only to singleton
/// subscribers.
pub struct CommitOrder;

impl sealed::Sealed for InsertOrder {}
impl sealed::Sealed for CommitOrder {}

impl Lane for InsertOrder {
    type Position = EventSequence;
    const ORDERING: Ordering = Ordering::Insert;

    type Listener<P>
        = PersistentOutboxListener<P>
    where
        P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin;

    fn require<P, Tables>(_outbox: &Outbox<P, Tables>) -> Result<(), CommitLaneDisabled>
    where
        P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
        Tables: MailboxTables,
    {
        Ok(())
    }

    fn listen<P, Tables>(
        outbox: &Outbox<P, Tables>,
        start_after: EventSequence,
    ) -> Result<Self::Listener<P>, CommitLaneDisabled>
    where
        P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
        Tables: MailboxTables,
    {
        Ok(outbox.listen_persisted(Some(start_after)))
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

    async fn frontier<Tables: MailboxTables>(
        pool: &sqlx::PgPool,
    ) -> Result<EventSequence, sqlx::Error> {
        read_frontier::<Tables>(pool).await
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

    type Listener<P>
        = CommitOrderedListener<P>
    where
        P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin;

    fn require<P, Tables>(outbox: &Outbox<P, Tables>) -> Result<(), CommitLaneDisabled>
    where
        P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
        Tables: MailboxTables,
    {
        outbox.commit_lane()?;
        Ok(())
    }

    fn listen<P, Tables>(
        outbox: &Outbox<P, Tables>,
        start_after: CommitSequence,
    ) -> Result<Self::Listener<P>, CommitLaneDisabled>
    where
        P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
        Tables: MailboxTables,
    {
        outbox.listen_commit_ordered(Some(start_after))
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

    async fn frontier<Tables: MailboxTables>(
        pool: &sqlx::PgPool,
    ) -> Result<CommitSequence, sqlx::Error> {
        Ok(Tables::commit_log_state(pool).await?.0)
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

/// `decide_lane` is total on each `(state, ordering)` pair — it yields that
/// ordering's cursor or refuses — so this is unreachable. Reported rather
/// than panicked: a runner is a poor place to assert.
fn lane_choice_mismatch(ordering: Ordering) -> String {
    format!("lane resolution yielded the other lane's cursor for Ordering::{ordering:?}")
}

impl LaneChoice {
    /// The insert cursor, when the insert lane was chosen.
    pub(crate) fn insert(self) -> Option<EventSequence> {
        match self {
            Self::Insert(sequence) => Some(sequence),
            Self::Commit(_) => None,
        }
    }

    /// The commit cursor, when the commit lane was chosen.
    pub(crate) fn commit(self) -> Option<CommitSequence> {
        match self {
            Self::Commit(commit_sequence) => Some(commit_sequence),
            Self::Insert(_) => None,
        }
    }
}
