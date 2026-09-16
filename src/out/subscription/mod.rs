//! Subscriptions — a consumer's durable relationship to the outbox stream,
//! in its two kinds.
//!
//! A **subscriber** consumes outbox events; a **subscription** is one
//! identity's durable relationship to the stream. The two kinds differ in
//! what brings them into existence:
//!
//! - [`singleton`] — exists because *code* declares it. Exactly one per
//!   type, permanent, registered at startup.
//! - [`keyed`] — exists because *data* creates it. One per key, cancellable,
//!   its subscription an explicit row in the `subscriptions` table (row
//!   absence = cancelled), woken on demand and costing nothing while idle.
//!
//! # The capability split is the semantics
//!
//! | capability | singleton | keyed |
//! |------------|-----------|-------|
//! | ephemeral delivery | yes — by presence | no, statically |
//! | `pause_until`, staged chains | no — by presence | yes |
//! | dormancy / wake | no | yes |
//!
//! The two modes are not one mode with a flag, and the asymmetry is not
//! scheduling mechanics — a resident job could perfectly well sleep. It is
//! the **presence contract**: a singleton subscriber is always on, and that
//! presence is exactly what licenses its ephemeral subscription, since
//! ephemeral events cannot be replayed and only an always-present consumer
//! may hear them. Pausing verbs contradict the property that defines the
//! mode, so they are keyed-only.
//!
//! Consequently, a **single-instance flow that needs to pause or stage is
//! persistent-only by definition** — host it as a keyed subscriber with one
//! static key. That is an intended shape, not a workaround (foreign-system
//! relays with backpressure, single-instance exporters), and it brings
//! dormancy for free. Adding a pause-less staged variant to the singleton
//! would buy a second sealed op type and answer a question this already
//! answers better.
//!
//! This module root holds what both kinds share: [`Subscription`], the
//! public read-back of a subscription's committed checkpoint, plus the
//! caught-up barrier built on it. It is a capability, not a value — it
//! caches nothing, and every read goes to committed state.

pub(crate) mod keyed;
pub(crate) mod singleton;

use serde::{Serialize, de::DeserializeOwned};

use std::{marker::PhantomData, time::Duration};

use crate::out::ctx::OutboxEventJobState;
use crate::out::persistent::SequencerPositions;
use crate::out::subscription::singleton::Ordering;
use crate::{
    sequence::{CommitSequence, EventSequence},
    tables::{DefaultMailboxTables, MailboxTables},
};

/// First poll interval used by [`Subscription::await_caught_up`], doubling
/// up to [`MAX_POLL_INTERVAL`].
const INITIAL_POLL_INTERVAL: Duration = Duration::from_millis(100);
/// Ceiling for the [`Subscription::await_caught_up`] poll interval.
const MAX_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Failure modes of the checkpoint read-back and the caught-up barrier.
#[derive(Debug, thiserror::Error)]
pub enum SubscriptionError {
    /// Reading the stream frontier failed.
    #[error("SubscriptionError - Sqlx: {0}")]
    Sqlx(#[from] sqlx::Error),
    /// Reading the handler job failed — a snapshot load (including the job
    /// never having existed), or a checkpoint point-read whose stored state
    /// did not decode.
    #[error("SubscriptionError - Job: {0}")]
    Job(#[from] ::job::JobError),
    /// The committed execution state did not decode as the handler job's
    /// state type — the checkpoint is unreadable rather than absent.
    #[error("SubscriptionError - StateDecode: {0}")]
    StateDecode(#[from] serde_json::Error),
    /// A keyed member's job could not be resolved from
    /// `(subscriber_type, key)` — no job of that type has ever been spawned
    /// under the key. Distinct from a cancelled subscription, whose job rows
    /// outlive the `subscriptions` row.
    #[error("SubscriptionError - NoSuchJob: no job for ({subscriber_type}, {key})")]
    NoSuchJob {
        subscriber_type: String,
        key: String,
    },
    /// [`Subscription::await_sequence`] — or
    /// [`await_caught_up`](Subscription::await_caught_up), which
    /// delegates to it — hit its deadline. Carries the observed lag so the
    /// caller can alert with real numbers instead of reporting a bare
    /// timeout.
    ///
    /// `target` is the position being awaited: the caller's own for
    /// `await_sequence`, the call-time frontier for `await_caught_up`. Lane
    /// (`Insert`/`Commit`) since a commit-lane subscription's `await_caught_up`
    /// polls its commit cursor, not `EventSequence`.
    #[error(
        "SubscriptionError - CaughtUpTimeout: checkpoint {checkpoint} behind target {target} after {waited:?}"
    )]
    CaughtUpTimeout {
        checkpoint: StreamPosition,
        target: StreamPosition,
        waited: Duration,
    },
}

/// Which lane a position was read from — see
/// [`Ordering`](crate::out::Ordering).
///
/// [`SubscriptionSnapshot::checkpoint`] / [`frontier`](SubscriptionSnapshot::frontier)
/// return this rather than a bare number because the two lanes count
/// different things: an insert-lane position is a contiguous, gap-filled
/// [`EventSequence`]; a commit-lane position is a dense, gap-free
/// [`CommitSequence`]. Comparing across variants is never meaningful — both
/// accessors on one [`SubscriptionSnapshot`] always report the same variant,
/// since the lane is a property of the subscription, not of the individual
/// read.
///
/// Derives [`Ord`] purely for the ergonomics of comparing two positions
/// already known to be on the same lane (`checkpoint >= frontier`, as
/// `stream_status` does below) — a cross-variant comparison type-checks but
/// is meaningless, per the paragraph above, so it is on the caller to only
/// ever compare positions read from the same subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum StreamPosition {
    Insert(EventSequence),
    Commit(CommitSequence),
}

impl StreamPosition {
    fn lane(&self) -> Ordering {
        match self {
            Self::Insert(_) => Ordering::Insert,
            Self::Commit(_) => Ordering::Commit,
        }
    }

    /// Numeric value for lag/comparison math — only ever compared against
    /// another `StreamPosition` sampled from the same lane (see the type's
    /// doc comment), so collapsing to `u64` here loses nothing.
    fn as_u64(&self) -> u64 {
        match self {
            Self::Insert(s) => u64::from(*s),
            Self::Commit(s) => u64::from(*s),
        }
    }
}

impl std::fmt::Display for StreamPosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Insert(s) => write!(f, "insert:{s}"),
            Self::Commit(s) => write!(f, "commit:{s}"),
        }
    }
}

/// Read the lane a subscription's checkpoint is on, and its position on it,
/// from execution state — the one place that decides Insert vs. Commit.
fn lane_position(state: &OutboxEventJobState) -> StreamPosition {
    match state.commit_sequence {
        Some(commit_sequence) => StreamPosition::Commit(commit_sequence),
        None => StreamPosition::Insert(state.sequence),
    }
}

/// A `{ checkpoint, frontier }` pair sampled by
/// [`SubscriptionSnapshot::stream_status`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubscriptionStreamStatus {
    /// Highest position the handler has durably applied.
    pub checkpoint: StreamPosition,
    /// Highest position the outbox has handed out.
    pub frontier: StreamPosition,
}

impl SubscriptionStreamStatus {
    /// How far the handler trails the frontier, saturating at zero.
    ///
    /// Zero does not by itself prove the handler is idle — see
    /// [`is_caught_up`](Self::is_caught_up).
    pub fn lag(&self) -> u64 {
        self.frontier
            .as_u64()
            .saturating_sub(self.checkpoint.as_u64())
    }

    /// Whether the checkpoint has reached the frontier sampled alongside it.
    pub fn is_caught_up(&self) -> bool {
        self.checkpoint >= self.frontier
    }
}

/// A point-in-time view of a registered handler, produced by
/// [`Subscription::load`].
///
/// One `load()` pairs the handler's committed checkpoint with the stream
/// frontier, so every accessor below is synchronous and infallible — a
/// consumer reading several of them pays one round-trip, not one per
/// question. Nothing is cached: a fresh `load()` always reflects the latest
/// committed state.
///
/// The checkpoint is decoded eagerly during `load()` (obix knows the handler
/// job's state type, so there is no reason to defer it to the caller), which
/// is why these accessors cannot fail.
pub struct SubscriptionSnapshot {
    job: ::job::JobSnapshot,
    checkpoint: StreamPosition,
    frontier: StreamPosition,
}

impl SubscriptionSnapshot {
    /// Which lane this snapshot's positions are on. A convenience over
    /// matching [`checkpoint`](Self::checkpoint) or
    /// [`frontier`](Self::frontier) when the caller only needs to know which
    /// arm it is, not the value.
    pub fn ordering(&self) -> Ordering {
        self.checkpoint.lane()
    }

    /// The handler's committed checkpoint: every event at or below this
    /// position, on this subscription's lane, has been handled and its
    /// effects committed (semantics 1). A handler that has never
    /// checkpointed reads as [`EventSequence::BEGIN`] on the insert lane
    /// (semantics 4).
    pub fn checkpoint(&self) -> StreamPosition {
        self.checkpoint
    }

    /// The stream frontier as of this load — the highest position the outbox
    /// had handed out on this subscription's lane (semantics 2).
    pub fn frontier(&self) -> StreamPosition {
        self.frontier
    }

    /// The `{ checkpoint, frontier }` pair.
    pub fn stream_status(&self) -> SubscriptionStreamStatus {
        SubscriptionStreamStatus {
            checkpoint: self.checkpoint,
            frontier: self.frontier,
        }
    }

    /// How far the handler trails the frontier, saturating at zero.
    pub fn lag(&self) -> u64 {
        self.stream_status().lag()
    }

    /// Whether the checkpoint has reached the frontier.
    pub fn is_caught_up(&self) -> bool {
        self.stream_status().is_caught_up()
    }

    /// Runtime status of the job hosting this handler.
    ///
    /// A resident handler job stays `Running`; a terminal status means the
    /// handler is no longer consuming, which is the case
    /// [`Subscription::await_caught_up`] reports as a timeout rather than a
    /// hang.
    pub fn job_status(&self) -> ::job::JobStatus {
        self.job.state()
    }

    /// The handler's most recent failure, if it has ever failed an attempt.
    ///
    /// **This is the wedged-vs-slow signal.** obix registers handlers to
    /// retry indefinitely, so a handler crash-looping on a poison event never
    /// reaches a terminal state: [`job_status`](Self::job_status) keeps
    /// reporting `Pending`/`Running` while the checkpoint sits frozen. A
    /// lagging handler with `Some` here — especially with
    /// [`attempt`](Self::attempt) climbing across successive loads — is stuck
    /// on this error, not merely backlogged.
    ///
    /// `None` means no attempt has ever failed. A stale `Some` from an
    /// earlier, since-recovered failure is possible, which is why the pair
    /// with a frozen checkpoint (or a rising attempt) is what diagnoses.
    pub fn last_error(&self) -> Option<&str> {
        self.job.last_error()
    }

    /// The current attempt number — `Some` only while the job has a live
    /// execution row. Rising across loads means the handler is retrying; see
    /// [`last_error`](Self::last_error).
    pub fn attempt(&self) -> Option<u32> {
        self.job.attempt()
    }

    /// The underlying job snapshot, for callers that want the job's own
    /// accessors (next run, queue id, config, return value).
    pub fn job(&self) -> &::job::JobSnapshot {
        &self.job
    }
}

/// An outbox event handler that has been registered and is running: its
/// committed checkpoint, its position relative to the stream frontier, the
/// runtime status of the job hosting it, and the caught-up barrier.
///
/// Returned by
/// [`Outbox::register_singleton_subscriber`](crate::out::Outbox::register_singleton_subscriber).
/// This does not own the handler — it is a cloneable, cheap-to-hold capability
/// for observing and fencing one, and it caches nothing, so every read
/// reflects the latest committed state.
///
/// # Semantics
///
/// These are the invariants a consumer's correctness rests on.
///
/// 1. **The checkpoint trails applied state, it never leads it.** A batch
///    flush commits the handler's work and its checkpoint in one transaction;
///    skip-only stretches persist the checkpoint lazily (bounded by the
///    handler's `checkpoint_interval`). So `checkpoint >= S` implies
///    everything up to `S` is durably applied. A barrier may therefore wait
///    marginally longer than strictly necessary, but never returns early.
/// 2. **The frontier is the sequence generator's `last_value`**, so it counts
///    sequences already assigned to transactions that have not committed yet
///    (or that aborted). That is what closes the straggler hole for
///    close-books-style fences, and it holds under partition rotation and
///    archival without scanning any table.
/// 3. **Delivery is gapless.** The runner cannot advance past sequence `N`
///    until `N` resolves; sequences belonging to aborted transactions become
///    placeholder deliveries once the gap-fill grace elapses. An aborted
///    sequence sitting at the frontier therefore cannot wedge the barrier.
/// 4. **Missing reads as [`EventSequence::BEGIN`].** A handler with no
///    execution row, or one that has never persisted state, reports honest
///    full lag rather than a spurious "caught up", so a stopped or
///    never-started handler makes the barrier time out with rich data instead
///    of hanging.
/// 5. **Self-publishing handlers anchor per call.** A handler whose flush
///    publishes back onto the *same* outbox leaves a tail behind the frontier
///    that [`await_caught_up`](Self::await_caught_up) sampled, so a
///    successful barrier does **not** imply a subsequent
///    [`load`](Self::load) reports caught up. Each call
///    anchors to its own call-time frontier, and sequential barriers still
///    compose: the first commits its emissions before returning, so the
///    second's snapshot includes them.
pub struct Subscription<P, Tables = DefaultMailboxTables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    anchor: JobAnchor,
    pool: sqlx::PgPool,
    /// The local sequencer's published positions, for the commit-lane
    /// `await_caught_up` fence. `None` for a keyed member: keyed
    /// subscribers never run on the commit lane (no `ordering` on
    /// `register_keyed_subscriber`), so their checkpoint is always
    /// [`StreamPosition::Insert`] and this is never consulted.
    positions: Option<SequencerPositions>,
    _phantom: PhantomData<(P, Tables)>,
}

/// A keyed member's stable identity: `(subscriber_type, key)`, plus the
/// handle onto the job service needed to resolve it.
#[derive(Clone)]
struct KeyedAnchor {
    jobs: ::job::Jobs,
    job_type: ::job::JobType,
    key: String,
}

/// How a [`Subscription`] finds the job it reports on.
///
/// The distinction is load-bearing, not bookkeeping. A `job::JobHandle` is
/// bound to one `JobId` for its whole life — `keyed_handle` resolves the
/// live-or-latest generation *once* and freezes it — and the two kinds of
/// job differ in whether that id stays meaningful.
#[derive(Clone)]
enum JobAnchor {
    /// A resident job: exactly one, forever, for the type's lifetime.
    /// Rescheduling is an in-place `UPDATE` of the same row, so the id never
    /// changes and the execution-state row (keyed on that id) is never
    /// deleted. A handle resolved once stays correct indefinitely.
    Resident(::job::JobHandle),
    /// A keyed job: every wake mints a NEW generation with a NEW `JobId`,
    /// and the spawn that mints it carries the inherited execution state
    /// onto the new id *and deletes every older generation's state row* —
    /// including the one it just copied from.
    ///
    /// So a handle resolved once does not merely go stale after the next
    /// wake: its id no longer has a state row at all, which reads as
    /// `Ok(None)` and decodes to checkpoint 0 — maximal lag, permanently,
    /// for a perfectly healthy subscription. The identity that survives a
    /// wake is `(subscriber_type, key)`, so that is what is stored and
    /// re-resolved per read.
    Keyed(Box<KeyedAnchor>),
}

// Manual `Clone`: this is cloneable regardless of whether `P` is, so
// deriving (which would bound `P: Clone` through `PhantomData`) is wrong.
// Mirrors `Outbox`'s manual impl.
impl<P, Tables> Clone for Subscription<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            anchor: self.anchor.clone(),
            pool: self.pool.clone(),
            positions: self.positions.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<P, Tables> std::fmt::Debug for Subscription<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut out = f.debug_struct("Subscription");
        match &self.anchor {
            JobAnchor::Resident(job) => out.field("job_id", &job.id()),
            JobAnchor::Keyed(anchor) => out
                .field("subscriber_type", &anchor.job_type)
                .field("key", &anchor.key),
        }
        .finish_non_exhaustive()
    }
}

impl<P, Tables> Subscription<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    /// For a resident (singleton-subscriber) job, whose id is stable.
    ///
    /// `positions` is the process's sequencer positions, needed for the
    /// commit-lane `await_caught_up` fence should this subscription turn
    /// out to be on that lane.
    pub(super) fn new(
        job: ::job::JobHandle,
        pool: sqlx::PgPool,
        positions: SequencerPositions,
    ) -> Self {
        Self {
            anchor: JobAnchor::Resident(job),
            pool,
            positions: Some(positions),
            _phantom: PhantomData,
        }
    }

    /// For a keyed member, identified by `(subscriber_type, key)` rather than
    /// by a job id — see [`JobAnchor::Keyed`]. Keyed subscribers never run on
    /// the commit lane, so this carries no sequencer positions.
    pub(super) fn new_keyed(
        jobs: ::job::Jobs,
        job_type: ::job::JobType,
        key: String,
        pool: sqlx::PgPool,
    ) -> Self {
        Self {
            anchor: JobAnchor::Keyed(Box::new(KeyedAnchor {
                jobs,
                job_type,
                key,
            })),
            pool,
            positions: None,
            _phantom: PhantomData,
        }
    }

    /// The id of the job running this handler, when there is a stable one.
    ///
    /// `None` for a keyed member: every wake mints a new generation with a
    /// new id, so there is no id that identifies the subscription over time.
    /// Its stable identity is `(subscriber_type, key)`. The per-run id is
    /// still available from a loaded snapshot via
    /// [`SubscriptionSnapshot::job`].
    pub fn job_id(&self) -> Option<::job::JobId> {
        match &self.anchor {
            JobAnchor::Resident(job) => Some(job.id()),
            JobAnchor::Keyed { .. } => None,
        }
    }

    /// Resolve the job to read. For a keyed member this re-resolves
    /// `(subscriber_type, key)` on every call — see [`JobAnchor::Keyed`] for
    /// why holding the resolved handle is wrong.
    async fn handle(&self) -> Result<::job::JobHandle, SubscriptionError> {
        match &self.anchor {
            JobAnchor::Resident(job) => Ok(job.clone()),
            JobAnchor::Keyed(anchor) => anchor
                .jobs
                .keyed_handle(anchor.job_type.clone(), anchor.key.clone())
                .await?
                .ok_or_else(|| SubscriptionError::NoSuchJob {
                    subscriber_type: anchor.job_type.to_string(),
                    key: anchor.key.clone(),
                }),
        }
    }

    /// Load a point-in-time [`SubscriptionSnapshot`]: the committed checkpoint,
    /// the stream frontier, and the hosting job's runtime status, in one
    /// round-trip pair. Every accessor on the result is synchronous.
    ///
    /// The checkpoint is read **first**, then the frontier, so a concurrent
    /// advance between the two can only overstate the snapshot's lag — never
    /// understate it. A caller acting on
    /// [`is_caught_up`](SubscriptionSnapshot::is_caught_up) therefore never acts
    /// on an optimistic reading.
    #[tracing::instrument(name = "obix.registered_handler.load", skip_all, err)]
    pub async fn load(&self) -> Result<SubscriptionSnapshot, SubscriptionError> {
        let job = self.handle().await?.load().await?;
        let checkpoint = decode_checkpoint(&job)?;
        let frontier = self.frontier_for(checkpoint).await?;
        Ok(SubscriptionSnapshot {
            job,
            checkpoint,
            frontier,
        })
    }

    /// Block until the handler's checkpoint reaches `target` — everything up
    /// to that sequence is handled and its effects committed (semantics 1).
    ///
    /// The checkpoint is polled starting at 100ms and doubling to a 250ms
    /// ceiling, bounded by the deadline. Each poll reads only the checkpoint,
    /// so it costs one round-trip rather than a full [`load`](Self::load).
    ///
    /// Use this when the caller already knows the sequence it cares about —
    /// e.g. one captured from an earlier publish. To fence on "everything
    /// published so far", use [`await_caught_up`](Self::await_caught_up),
    /// which is this method over the call-time frontier.
    ///
    /// A `target` beyond the frontier is not an error, just a wait the
    /// handler cannot satisfy until the stream reaches it; it times out
    /// honestly like any other unmet target.
    ///
    /// The timeout is REQUIRED: the wait is structurally bounded, so a
    /// stopped handler surfaces as an alertable error rather than a silent
    /// hang.
    ///
    /// Insert-lane only: this compares `state.sequence`, which is not
    /// monotone under `Ordering::Commit` (commit order is a permutation of
    /// insert order). A commit-lane subscription should fence with
    /// [`await_caught_up`](Self::await_caught_up), which is lane-aware.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::CaughtUpTimeout`] — carrying the
    /// observed checkpoint, the target and the elapsed wait — if the deadline
    /// passes first.
    #[tracing::instrument(
        name = "obix.registered_handler.await_sequence",
        skip_all,
        // Not `target`: that name collides with `instrument`'s own span-target
        // argument.
        fields(target_seq = %target, timeout_ms = timeout.as_millis()),
        err
    )]
    pub async fn await_sequence(
        &self,
        target: EventSequence,
        timeout: Duration,
    ) -> Result<(), SubscriptionError> {
        self.poll_until(StreamPosition::Insert(target), timeout, |state| {
            StreamPosition::Insert(state.sequence)
        })
        .await
    }

    /// Block until the handler's checkpoint reaches the frontier **sampled at
    /// call time** — the fence for "everything published before this call has
    /// been applied".
    ///
    /// On the insert lane this is a strict special case of
    /// [`await_sequence`](Self::await_sequence) over the call-time frontier,
    /// and inherits its polling and timeout behaviour. Events published
    /// *after* the call are not waited for (semantics 5).
    ///
    /// On the commit lane the checkpoint is a `CommitSequence`, so this
    /// delegates to an internal commit-lane fence instead of
    /// `await_sequence` — see obix-dev/handoff-commit-lane-consumer-gaps.md
    /// §4 for why the originally-specced version hangs.
    ///
    /// The lane is read once at the start of the call (a single-row
    /// execution-state point read, same cost class as one checkpoint poll)
    /// to decide which fence applies — the frontier itself differs by lane,
    /// so this cannot be decided from the frontier read alone the way it
    /// could before the commit lane existed.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::CaughtUpTimeout`] — where `target`
    /// is the sampled frontier — if the deadline passes first.
    #[tracing::instrument(
        name = "obix.registered_handler.await_caught_up",
        skip_all,
        fields(timeout_ms = timeout.as_millis()),
        err
    )]
    pub async fn await_caught_up(&self, timeout: Duration) -> Result<(), SubscriptionError> {
        // Sampled ONCE: the fence is anchored to the stream position at call
        // time, so a handler that publishes as it drains cannot extend its
        // own barrier indefinitely (semantics 5).
        match lane_position(&self.read_state().await?) {
            StreamPosition::Insert(_) => {
                let frontier = read_frontier::<Tables>(&self.pool).await?;
                self.await_sequence(frontier, timeout).await
            }
            StreamPosition::Commit(_) => self.await_caught_up_commit_lane(timeout).await,
        }
    }

    /// The corrected §10 fence for a commit-lane subscription.
    ///
    /// The originally-specced step 2 — poll
    /// `commit_log_state().logged_through_sequence >= H` — hangs: that
    /// watermark only advances when `append_commit_group` runs, so it stalls
    /// behind an open group (an aborted transaction at the head, which is
    /// exactly what inflates `H`) or during a quiet period. See
    /// obix-dev/handoff-commit-lane-consumer-gaps.md §4 (recorded as
    /// CORRECTION 3 in obix-dev/handoff-commit-ordered-delivery.md §10).
    ///
    /// Step 2 below instead polls this process's own sequencer fold
    /// position — an in-process atomic, no query — which advances on every
    /// insert-lane delivery the fold examines, placeholders and
    /// already-logged members included.
    async fn await_caught_up_commit_lane(
        &self,
        timeout: Duration,
    ) -> Result<(), SubscriptionError> {
        let positions = self.positions.as_ref().expect(
            "a commit-lane checkpoint (state.commit_sequence: Some) is only ever written by a \
             resident singleton-subscriber job, which always carries sequencer positions — see \
             Subscription::new_keyed's doc comment",
        );

        let start = tokio::time::Instant::now();
        let deadline = start + timeout;

        let h = read_frontier::<Tables>(&self.pool).await?;

        loop {
            if positions.fold_position() >= h {
                break;
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Err(SubscriptionError::CaughtUpTimeout {
                    checkpoint: StreamPosition::Insert(positions.fold_position()),
                    target: StreamPosition::Insert(h),
                    waited: now.duration_since(start),
                });
            }
            tokio::time::sleep(INITIAL_POLL_INTERVAL.min(deadline - now)).await;
        }

        let (last_commit_seq, _logged_through) = Tables::commit_log_state(&self.pool).await?;
        let now = tokio::time::Instant::now();
        let remaining = if now >= deadline {
            Duration::ZERO
        } else {
            deadline - now
        };
        self.poll_until(
            StreamPosition::Commit(last_commit_seq),
            remaining,
            |state| StreamPosition::Commit(state.commit_sequence.unwrap_or_default()),
        )
        .await
    }

    /// The committed checkpoint alone, via job's point-read: a single-row
    /// `SELECT` on the execution row, with no entity hydration and no
    /// snapshot reconciliation. Backs [`poll_until`](Self::poll_until), which
    /// already holds the target it anchored to and needs nothing else per
    /// tick.
    ///
    /// Staying off [`load`](Self::load) here matters because the entity
    /// hydration it skips grows with the job's event log — that is, with
    /// retries — so a full-snapshot poll would get more expensive exactly
    /// when a handler is wedged and someone is watching a fence time out.
    ///
    /// Safe because this does not serve
    /// [`job_status`](SubscriptionSnapshot::job_status): a missing or
    /// mid-transition row reads `None` ⇒ [`EventSequence::BEGIN`], which can
    /// only under-report progress, and under-reporting preserves the
    /// barrier's never-return-early invariant.
    async fn read_state(&self) -> Result<OutboxEventJobState, SubscriptionError> {
        Ok(self
            .handle()
            .await?
            .execution_state::<OutboxEventJobState>()
            .await?
            .unwrap_or_default())
    }

    /// Shared poll loop behind [`await_sequence`](Self::await_sequence) and
    /// the commit-lane leg of [`await_caught_up`](Self::await_caught_up):
    /// starting at 100ms and doubling to a 250ms ceiling, bounded by
    /// `timeout`, until `extract` of the freshly-read state reaches `target`.
    async fn poll_until(
        &self,
        target: StreamPosition,
        timeout: Duration,
        extract: impl Fn(&OutboxEventJobState) -> StreamPosition,
    ) -> Result<(), SubscriptionError> {
        let start = tokio::time::Instant::now();
        let deadline = start + timeout;

        let mut interval = INITIAL_POLL_INTERVAL;
        loop {
            let checkpoint = extract(&self.read_state().await?);
            if checkpoint >= target {
                return Ok(());
            }

            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Err(SubscriptionError::CaughtUpTimeout {
                    checkpoint,
                    target,
                    waited: now.duration_since(start),
                });
            }

            // Never sleep past the deadline: a long interval must not delay
            // the timeout error beyond what the caller asked for.
            tokio::time::sleep(interval.min(deadline - now)).await;
            interval = (interval * 2).min(MAX_POLL_INTERVAL);
        }
    }

    async fn frontier_for(
        &self,
        checkpoint: StreamPosition,
    ) -> Result<StreamPosition, sqlx::Error> {
        match checkpoint {
            StreamPosition::Insert(_) => Ok(StreamPosition::Insert(
                read_frontier::<Tables>(&self.pool).await?,
            )),
            StreamPosition::Commit(_) => {
                let (last_commit_seq, _logged_through) =
                    Tables::commit_log_state(&self.pool).await?;
                Ok(StreamPosition::Commit(last_commit_seq))
            }
        }
    }
}

/// Read the stream frontier.
///
/// The inner future is boxed deliberately, and removing the box will compile
/// here but break callers.
/// [`MailboxTables::highest_known_persistent_sequence`] returns an opaque
/// `impl Future` that captures the lifetime of its executor argument.
/// Awaiting that opaque type inside a method taking `&self` makes the
/// enclosing future's `Send`-ness higher-ranked over that lifetime, which
/// defeats inference at `tokio::spawn` — "implementation of `Send` is not
/// general enough" (rust-lang/rust#100013). Boxing erases the opaque type and
/// grounds the lifetime, for one allocation per call — nothing next to the
/// round-trip it wraps.
pub(super) async fn read_frontier<Tables: MailboxTables>(
    pool: &sqlx::PgPool,
) -> Result<EventSequence, sqlx::Error> {
    let pool = pool.clone();
    let fut: std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<EventSequence, sqlx::Error>> + Send>,
    > = Box::pin(async move { Tables::highest_known_persistent_sequence(&pool).await });
    fut.await
}

/// Decode a handler job's committed checkpoint. Absent state — no execution
/// row, or a job that has not checkpointed yet — reads as
/// [`EventSequence::BEGIN`] (semantics 4).
fn decode_checkpoint(job: &::job::JobSnapshot) -> Result<StreamPosition, SubscriptionError> {
    Ok(lane_position(
        &job.execution_state::<OutboxEventJobState>()?
            .unwrap_or_default(),
    ))
}
