//! [`RegisteredEventHandler`] — public read-back of a registered outbox event
//! handler's committed checkpoint, plus the caught-up barrier built on it.
//!
//! Returned by
//! [`Outbox::register_event_handler`](super::Outbox::register_event_handler).
//! It is a capability, not a value: it caches nothing, and every read goes
//! to committed state.

use serde::{Serialize, de::DeserializeOwned};

use std::{marker::PhantomData, time::Duration};

use super::ctx::OutboxEventJobState;
use crate::{
    sequence::EventSequence,
    tables::{DefaultMailboxTables, MailboxTables},
};

/// First poll interval used by [`RegisteredEventHandler::await_caught_up`], doubling
/// up to [`MAX_POLL_INTERVAL`].
const INITIAL_POLL_INTERVAL: Duration = Duration::from_millis(100);
/// Ceiling for the [`RegisteredEventHandler::await_caught_up`] poll interval.
const MAX_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Failure modes of the checkpoint read-back and the caught-up barrier.
#[derive(Debug, thiserror::Error)]
pub enum HandlerCheckpointError {
    /// Reading the stream frontier failed.
    #[error("HandlerCheckpointError - Sqlx: {0}")]
    Sqlx(#[from] sqlx::Error),
    /// Loading the handler job's snapshot failed (including the job never
    /// having existed).
    // `job` does not re-export `JobError` at its crate root (only
    // `job::error`), though its public signatures return it.
    #[error("HandlerCheckpointError - Job: {0}")]
    Job(#[from] ::job::error::JobError),
    /// The committed execution state did not decode as the handler job's
    /// state type — the checkpoint is unreadable rather than absent.
    #[error("HandlerCheckpointError - StateDecode: {0}")]
    StateDecode(#[from] serde_json::Error),
    /// [`RegisteredEventHandler::await_caught_up`] hit its deadline. Carries the
    /// observed lag so the caller can alert with real numbers instead of
    /// reporting a bare timeout.
    #[error(
        "HandlerCheckpointError - CaughtUpTimeout: checkpoint {checkpoint} behind frontier {frontier} after {waited:?}"
    )]
    CaughtUpTimeout {
        checkpoint: EventSequence,
        frontier: EventSequence,
        waited: Duration,
    },
}

/// A `{ checkpoint, frontier }` pair sampled by
/// [`HandlerSnapshot::stream_status`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HandlerStreamStatus {
    /// Highest sequence the handler has durably applied.
    pub checkpoint: EventSequence,
    /// Highest sequence the outbox has handed out.
    pub frontier: EventSequence,
}

impl HandlerStreamStatus {
    /// How far the handler trails the frontier, saturating at zero.
    ///
    /// Zero does not by itself prove the handler is idle — see
    /// [`is_caught_up`](Self::is_caught_up).
    pub fn lag(&self) -> u64 {
        u64::from(self.frontier).saturating_sub(u64::from(self.checkpoint))
    }

    /// Whether the checkpoint has reached the frontier sampled alongside it.
    pub fn is_caught_up(&self) -> bool {
        self.checkpoint >= self.frontier
    }
}

/// A point-in-time view of a registered handler, produced by
/// [`RegisteredEventHandler::load`].
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
pub struct HandlerSnapshot {
    job: ::job::JobSnapshot,
    checkpoint: EventSequence,
    frontier: EventSequence,
}

impl HandlerSnapshot {
    /// The handler's committed checkpoint: every persistent event with a
    /// sequence at or below this has been handled and its effects committed
    /// (semantics 1). A handler that has never checkpointed reads as
    /// [`EventSequence::BEGIN`] (semantics 4).
    pub fn checkpoint(&self) -> EventSequence {
        self.checkpoint
    }

    /// The stream frontier as of this load — the highest sequence the outbox
    /// had handed out (semantics 2).
    pub fn frontier(&self) -> EventSequence {
        self.frontier
    }

    /// The `{ checkpoint, frontier }` pair.
    pub fn stream_status(&self) -> HandlerStreamStatus {
        HandlerStreamStatus {
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
    /// [`RegisteredEventHandler::await_caught_up`] reports as a timeout rather than a
    /// hang.
    pub fn job_status(&self) -> ::job::JobStatus {
        self.job.state()
    }

    /// The underlying job snapshot, for callers that want the job's own
    /// accessors (attempt, next run, config, return value).
    pub fn job(&self) -> &::job::JobSnapshot {
        &self.job
    }
}

/// An outbox event handler that has been registered and is running: its
/// committed checkpoint, its position relative to the stream frontier, the
/// runtime status of the job hosting it, and the caught-up barrier.
///
/// Returned by
/// [`Outbox::register_event_handler`](super::Outbox::register_event_handler).
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
pub struct RegisteredEventHandler<P, Tables = DefaultMailboxTables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    job: ::job::JobHandle,
    pool: sqlx::PgPool,
    _phantom: PhantomData<(P, Tables)>,
}

// Manual `Clone`: this is cloneable regardless of whether `P` is, so
// deriving (which would bound `P: Clone` through `PhantomData`) is wrong.
// Mirrors `Outbox`'s manual impl.
impl<P, Tables> Clone for RegisteredEventHandler<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            job: self.job.clone(),
            pool: self.pool.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<P, Tables> std::fmt::Debug for RegisteredEventHandler<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredEventHandler")
            .field("job_id", &self.job.id())
            .finish_non_exhaustive()
    }
}

impl<P, Tables> RegisteredEventHandler<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    pub(super) fn new(job: ::job::JobHandle, pool: sqlx::PgPool) -> Self {
        Self {
            job,
            pool,
            _phantom: PhantomData,
        }
    }

    /// The id of the job running this handler.
    pub fn job_id(&self) -> ::job::JobId {
        self.job.id()
    }

    /// Load a point-in-time [`HandlerSnapshot`]: the committed checkpoint,
    /// the stream frontier, and the hosting job's runtime status, in one
    /// round-trip pair. Every accessor on the result is synchronous.
    ///
    /// The checkpoint is read **first**, then the frontier, so a concurrent
    /// advance between the two can only overstate the snapshot's lag — never
    /// understate it. A caller acting on
    /// [`is_caught_up`](HandlerSnapshot::is_caught_up) therefore never acts
    /// on an optimistic reading.
    #[tracing::instrument(name = "obix.registered_handler.load", skip_all, err)]
    pub async fn load(&self) -> Result<HandlerSnapshot, HandlerCheckpointError> {
        let job = self.job.load().await?;
        let checkpoint = decode_checkpoint(&job)?;
        let frontier = self.frontier().await?;
        Ok(HandlerSnapshot {
            job,
            checkpoint,
            frontier,
        })
    }

    /// Block until the handler's checkpoint reaches the frontier **sampled at
    /// call time** — the fence for "everything published before this call has
    /// been applied".
    ///
    /// The frontier is sampled once, up front; the checkpoint is then polled
    /// starting at 100ms and doubling to a 250ms ceiling, bounded by the
    /// deadline. Events published *after* the call are not waited for
    /// (semantics 5).
    ///
    /// Each poll reads only the checkpoint — it does not re-read the
    /// frontier, so a poll costs one round-trip rather than a full
    /// [`load`](Self::load).
    ///
    /// The timeout is REQUIRED: the wait is structurally bounded, so a
    /// stopped handler surfaces as an alertable error rather than a silent
    /// hang.
    ///
    /// # Errors
    ///
    /// Returns [`HandlerCheckpointError::CaughtUpTimeout`] — carrying the
    /// observed checkpoint, the sampled frontier and the elapsed wait — if
    /// the deadline passes first.
    #[tracing::instrument(
        name = "obix.registered_handler.await_caught_up",
        skip_all,
        fields(timeout_ms = timeout.as_millis()),
        err
    )]
    pub async fn await_caught_up(&self, timeout: Duration) -> Result<(), HandlerCheckpointError> {
        let start = tokio::time::Instant::now();
        let deadline = start + timeout;
        // Sampled ONCE: the fence is anchored to the stream position at call
        // time, so a handler that publishes as it drains cannot extend its
        // own barrier indefinitely (semantics 5).
        let frontier = self.frontier().await?;

        let mut interval = INITIAL_POLL_INTERVAL;
        loop {
            let checkpoint = self.checkpoint().await?;
            if checkpoint >= frontier {
                return Ok(());
            }

            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Err(HandlerCheckpointError::CaughtUpTimeout {
                    checkpoint,
                    frontier,
                    waited: now.duration_since(start),
                });
            }

            // Never sleep past the deadline: a long interval must not delay
            // the timeout error beyond what the caller asked for.
            tokio::time::sleep(interval.min(deadline - now)).await;
            interval = (interval * 2).min(MAX_POLL_INTERVAL);
        }
    }

    /// The committed checkpoint alone — one round-trip, no frontier read.
    /// Backs the [`await_caught_up`](Self::await_caught_up) poll loop, which
    /// already holds the frontier it anchored to.
    async fn checkpoint(&self) -> Result<EventSequence, HandlerCheckpointError> {
        decode_checkpoint(&self.job.load().await?)
    }

    async fn frontier(&self) -> Result<EventSequence, sqlx::Error> {
        read_frontier::<Tables>(&self.pool).await
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
fn decode_checkpoint(job: &::job::JobSnapshot) -> Result<EventSequence, HandlerCheckpointError> {
    Ok(job
        .execution_state::<OutboxEventJobState>()?
        .unwrap_or_default()
        .sequence)
}
