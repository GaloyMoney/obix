//! Handler-controlled transaction scoping for outbox event-handler jobs.
//!
//! Every persistent event delivered to an
//! [`OutboxEventHandler`](super::OutboxEventHandler) comes with an
//! [`EventCtx`] that the handler must resolve into a [`Handled`] token by
//! choosing exactly one of three entry verbs:
//!
//! | verb | meaning | cost |
//! |------|---------|------|
//! | [`EventCtx::skip`] | not my event | zero — no transaction is opened |
//! | [`EventCtx::consume_in_batch`] | do work joined to the pending batch op | shares one transaction (and one checkpoint) with neighboring events |
//! | [`EventCtx::consume_isolated`] | land the pending batch first, then do my work in a fresh op | my event is its own atomic unit, fenced from history |
//!
//! and — when work was done — one of two exit verbs:
//!
//! | verb | meaning |
//! |------|---------|
//! | [`BatchOp::commit`] / [`IsolatedOp::commit`] | land the op (work + checkpoint, atomically) when the invocation returns |
//! | [`BatchOp::defer`] | leave the op open so subsequent events can coalesce into it |
//!
//! A deferred op is only ever open while there is ready backlog: the runner
//! never awaits the stream while a transaction is open, so a pending stream
//! is itself a flush trigger — as is an interleaved ephemeral event (the op
//! never spans the foreign `handle_ephemeral` await either). Batching
//! therefore rides bursts that already happened and adds no latency at low
//! traffic.
//!
//! Every flush — whichever of the triggers fires — persists the checkpoint
//! at the last *fully handled* sequence (skips included), then commits:
//! work and pointer are inseparable.

use serde::{Deserialize, Serialize};

use std::marker::PhantomData;

use job::CurrentJob;

use crate::sequence::EventSequence;

/// Error type shared with the handler trait methods.
pub(crate) type HandlerError = Box<dyn std::error::Error + Send + Sync>;

/// Persisted execution state of an outbox event-handler job: the sequence of
/// the last fully handled persistent event.
#[derive(Default, Clone, Copy, Serialize, Deserialize)]
pub(crate) struct OutboxEventJobState {
    pub(crate) sequence: EventSequence,
}

/// Book-keeping the runner shares with [`EventCtx`].
pub(crate) struct BatchTracker {
    /// Number of events whose work is in the currently open op.
    pub(crate) events_in_op: usize,
    /// Highest sequence whose checkpoint has been persisted to the database.
    pub(crate) persisted_seq: EventSequence,
    /// When the checkpoint was last persisted (any flush or standalone write).
    pub(crate) last_persist: tokio::time::Instant,
}

pub(crate) struct CtxParts<'inv> {
    pub(crate) op_slot: &'inv mut Option<es_entity::DbOp<'static>>,
    pub(crate) current_job: &'inv mut CurrentJob,
    pub(crate) state: &'inv OutboxEventJobState,
    pub(crate) tracker: &'inv mut BatchTracker,
}

/// Proof that a persistent event was resolved in one of the legal ways.
///
/// Only obtainable from [`EventCtx::skip`], [`BatchOp::commit`],
/// [`BatchOp::defer`] or [`IsolatedOp::commit`] — the type system forces
/// every [`handle_persistent`](super::OutboxEventHandler::handle_persistent)
/// invocation to decide the transactional fate of its event.
///
/// The token is branded with the invocation's lifetime, so it cannot leave
/// the invocation that minted it: handlers are `'static`, so stashing a
/// token for a later invocation does not compile —
///
/// ```compile_fail
/// use obix::{EventCtx, Handled};
///
/// struct Evil {
///     stash: std::sync::Mutex<Option<Handled<'static>>>,
/// }
///
/// fn stash_it(ctx: EventCtx<'_>, evil: &Evil) {
///     // error[E0521]: borrowed data escapes outside of function
///     *evil.stash.lock().unwrap() = Some(ctx.skip());
/// }
/// ```
///
/// Combined with the entry verbs consuming the [`EventCtx`] (each invocation
/// can mint exactly one token), the returned token is always *the* token of
/// the current invocation, of the kind that actually happened.
#[must_use = "return the Handled token from handle_persistent"]
pub struct Handled<'inv> {
    pub(crate) outcome: Outcome,
    pub(crate) _invocation: PhantomData<&'inv ()>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    Skip,
    Commit,
    Defer,
}

/// Per-event decision point handed to
/// [`handle_persistent`](super::OutboxEventHandler::handle_persistent).
///
/// See the [module docs](self) for the semantics of the three entry verbs.
#[must_use = "resolve the EventCtx via skip / consume_in_batch / consume_isolated"]
pub struct EventCtx<'inv> {
    pub(crate) parts: CtxParts<'inv>,
}

impl<'inv> EventCtx<'inv> {
    /// This event is not for me — no transaction is opened, an open batch op
    /// is left untouched, and the checkpoint advances lazily (piggybacked on
    /// the next flush, or persisted on the configured checkpoint interval).
    pub fn skip(self) -> Handled<'inv> {
        Handled {
            outcome: Outcome::Skip,
            _invocation: PhantomData,
        }
    }

    /// Do transactional work joined to the pending batch op (opening a fresh
    /// one if none is pending).
    ///
    /// Work in a batch shares fate with its neighbors: a failure anywhere in
    /// the batch rolls back and replays the whole batch from the last
    /// committed checkpoint. Only choose this when the handler's work is
    /// idempotent under such replay (pure in-op DB writes and in-op job
    /// spawns are — they roll back with the op).
    pub async fn consume_in_batch(self) -> Result<BatchOp<'inv>, HandlerError> {
        let parts = self.parts;
        if parts.op_slot.is_none() {
            *parts.op_slot = Some(
                es_entity::DbOp::init_with_clock(
                    parts.current_job.pool(),
                    parts.current_job.clock(),
                )
                .await?,
            );
        }
        parts.tracker.events_in_op += 1;
        Ok(BatchOp { parts })
    }

    /// Land the pending batch first (its work and its checkpoint, at the
    /// last fully handled sequence), then hand back a fresh op: this event
    /// is its own atomic unit, sharing no fate with history — and none with
    /// the future either, since [`IsolatedOp`] only offers
    /// [`commit`](IsolatedOp::commit).
    ///
    /// This is the failure-isolation fence: if this event's work fails, only
    /// this event replays; and it is the exact legacy per-event semantics
    /// when no batch is pending.
    pub async fn consume_isolated(self) -> Result<IsolatedOp<'inv>, HandlerError> {
        let mut parts = self.parts;
        flush_batch(&mut parts, "isolated_entry").await?;
        *parts.op_slot = Some(
            es_entity::DbOp::init_with_clock(parts.current_job.pool(), parts.current_job.clock())
                .await?,
        );
        parts.tracker.events_in_op = 1;
        Ok(IsolatedOp { parts })
    }
}

/// An op joined to the pending batch. Derefs to [`es_entity::DbOp`] — use it
/// exactly like any atomic operation, then exit with [`commit`](Self::commit)
/// or [`defer`](Self::defer).
#[must_use = "exit with .commit() or .defer() to produce the Handled token"]
pub struct BatchOp<'inv> {
    parts: CtxParts<'inv>,
}

impl<'inv> BatchOp<'inv> {
    /// Land the whole batch (my work included) when the invocation returns:
    /// checkpoint at my sequence → commit.
    pub fn commit(self) -> Handled<'inv> {
        Handled {
            outcome: Outcome::Commit,
            _invocation: PhantomData,
        }
    }

    /// Leave the op open so subsequent events can coalesce into it. The
    /// runner lands it when the ready backlog is drained, when the
    /// configured max batch size is reached, when a later event commits or
    /// isolates, when an ephemeral event arrives (the op never spans the
    /// foreign `handle_ephemeral` await), or on shutdown.
    pub fn defer(self) -> Handled<'inv> {
        Handled {
            outcome: Outcome::Defer,
            _invocation: PhantomData,
        }
    }
}

impl std::ops::Deref for BatchOp<'_> {
    type Target = es_entity::DbOp<'static>;

    fn deref(&self) -> &Self::Target {
        self.parts
            .op_slot
            .as_ref()
            .expect("BatchOp always holds a materialized op")
    }
}

impl std::ops::DerefMut for BatchOp<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.parts
            .op_slot
            .as_mut()
            .expect("BatchOp always holds a materialized op")
    }
}

/// Full delegation to the inner [`es_entity::DbOp`] — including the provided
/// methods, which [`es_entity::DbOp`] overrides (`supports_hooks` is `true`,
/// `commit_hook` returns registered hooks, `maybe_now`/`clock` carry the op's
/// cached time and clock). Inheriting the trait defaults instead would
/// silently report `supports_hooks() == false` and lose the op time.
///
/// This lets handlers pass `&mut op` directly to any
/// `fn(&mut impl AtomicOperation)` API (service `*_in_op` methods,
/// `spawn_in_op`, `publish_persisted_in_op`, …) without `&mut *op`.
impl es_entity::AtomicOperation for BatchOp<'_> {
    fn maybe_now(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        (**self).maybe_now()
    }

    fn clock(&self) -> &es_entity::clock::ClockHandle {
        es_entity::AtomicOperation::clock(&**self)
    }

    fn connection(&mut self) -> &mut es_entity::db::Connection {
        (**self).connection()
    }

    fn as_executor(&mut self) -> es_entity::OneTimeExecutor<'_, &mut es_entity::db::Connection> {
        (**self).as_executor()
    }

    fn add_commit_hook<H: es_entity::hooks::CommitHook>(&mut self, hook: H) -> Result<(), H> {
        (**self).add_commit_hook(hook)
    }

    fn commit_hook<H: es_entity::hooks::CommitHook>(&self) -> Option<&H> {
        (**self).commit_hook::<H>()
    }

    fn supports_hooks(&self) -> bool {
        (**self).supports_hooks()
    }
}

/// An op holding exactly this event's work, fenced from the batch history.
/// Derefs to [`es_entity::DbOp`]. The only exit is [`commit`](Self::commit) —
/// isolation from future events is guaranteed by construction.
#[must_use = "exit with .commit() to produce the Handled token"]
pub struct IsolatedOp<'inv> {
    parts: CtxParts<'inv>,
}

impl<'inv> IsolatedOp<'inv> {
    /// Land my work and my checkpoint, atomically, when the invocation
    /// returns.
    pub fn commit(self) -> Handled<'inv> {
        Handled {
            outcome: Outcome::Commit,
            _invocation: PhantomData,
        }
    }
}

impl std::ops::Deref for IsolatedOp<'_> {
    type Target = es_entity::DbOp<'static>;

    fn deref(&self) -> &Self::Target {
        self.parts
            .op_slot
            .as_ref()
            .expect("IsolatedOp always holds a materialized op")
    }
}

impl std::ops::DerefMut for IsolatedOp<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.parts
            .op_slot
            .as_mut()
            .expect("IsolatedOp always holds a materialized op")
    }
}

/// Full delegation to the inner [`es_entity::DbOp`] — see the note on
/// [`BatchOp`]'s impl for why every provided method is delegated too.
impl es_entity::AtomicOperation for IsolatedOp<'_> {
    fn maybe_now(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        (**self).maybe_now()
    }

    fn clock(&self) -> &es_entity::clock::ClockHandle {
        es_entity::AtomicOperation::clock(&**self)
    }

    fn connection(&mut self) -> &mut es_entity::db::Connection {
        (**self).connection()
    }

    fn as_executor(&mut self) -> es_entity::OneTimeExecutor<'_, &mut es_entity::db::Connection> {
        (**self).as_executor()
    }

    fn add_commit_hook<H: es_entity::hooks::CommitHook>(&mut self, hook: H) -> Result<(), H> {
        (**self).add_commit_hook(hook)
    }

    fn commit_hook<H: es_entity::hooks::CommitHook>(&self) -> Option<&H> {
        (**self).commit_hook::<H>()
    }

    fn supports_hooks(&self) -> bool {
        (**self).supports_hooks()
    }
}

/// Land the pending batch op, if any: checkpoint at the last fully handled
/// sequence → commit. No-op when no op is open.
#[tracing::instrument(
    name = "outbox.flush_batch",
    skip_all,
    fields(
        reason = reason,
        batch_size = parts.tracker.events_in_op,
        checkpoint_seq = u64::from(parts.state.sequence),
    ),
    err
)]
pub(crate) async fn flush_batch(
    parts: &mut CtxParts<'_>,
    reason: &'static str,
) -> Result<(), HandlerError> {
    let Some(mut op) = parts.op_slot.take() else {
        return Ok(());
    };
    parts
        .current_job
        .update_execution_state_in_op(&mut op, parts.state)
        .await?;
    op.commit().await?;
    parts.tracker.persisted_seq = parts.state.sequence;
    parts.tracker.last_persist = tokio::time::Instant::now();
    parts.tracker.events_in_op = 0;
    Ok(())
}

/// Persist the checkpoint on its own — used for skip-only stretches where no
/// work op ever materialized, bounded by the configured checkpoint interval.
#[tracing::instrument(
    name = "outbox.persist_checkpoint",
    skip_all,
    fields(checkpoint_seq = u64::from(state.sequence)),
    err
)]
pub(crate) async fn persist_checkpoint(
    current_job: &mut CurrentJob,
    state: &OutboxEventJobState,
) -> Result<(), HandlerError> {
    let mut op = es_entity::DbOp::init_with_clock(current_job.pool(), current_job.clock()).await?;
    current_job
        .update_execution_state_in_op(&mut op, state)
        .await?;
    op.commit().await?;
    Ok(())
}
