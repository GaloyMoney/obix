//! Handler-controlled transaction scoping for outbox event-handler jobs.
//!
//! Every persistent event delivered to an
//! [`SingletonSubscriber`](super::SingletonSubscriber) comes with an
//! [`EventCtx`] that the handler must resolve into a [`Handled`] token by
//! choosing exactly one of four entry verbs — a monotone cost ladder:
//!
//! | verb | meaning | cost |
//! |------|---------|------|
//! | [`EventCtx::skip`] | not my event | zero — no transaction is opened |
//! | [`EventCtx::collect_with`] (and the [`collect`](EventCtx::collect) sugar) | contribute an item to the pending batch's accumulator | zero at collect time — one [`flush`](super::SingletonSubscriber::flush) call per batch applies all items |
//! | [`EventCtx::consume_in_batch`] | do work joined to the pending batch op | shares one transaction (and one checkpoint) with neighboring events |
//! | [`EventCtx::consume_isolated`] | land the pending batch first, then do my work in a fresh op | my event is its own atomic unit, fenced from history |
//!
//! and — when an op was taken — one of two exit verbs:
//!
//! | verb | meaning |
//! |------|---------|
//! | [`BatchOp::commit`] / [`IsolatedOp::commit`] | land the op (work + checkpoint, atomically) when the invocation returns |
//! | [`BatchOp::defer`] | leave the op open so subsequent events can coalesce into it |
//!
//! A pending batch — an open op, collected items, or both — only ever exists
//! while there is ready persistent backlog: the runner never awaits the
//! stream while a batch is pending, so a pending stream is itself a flush
//! trigger. Batching therefore rides bursts that already happened and adds
//! no latency at low traffic. Ephemeral events travel on their own stream
//! and are handled between batches — they never interrupt a batch, a
//! transaction never spans the foreign `handle_ephemeral` await, and between
//! batches the two streams race fairly so neither can starve the other.
//! Handlers that only consume one stream should declare it via
//! [`SUBSCRIPTION`](super::SingletonSubscriber::SUBSCRIPTION) — the other
//! stream is then never subscribed at all.
//!
//! Every flush — whichever of the triggers fires — first hands all collected
//! items to the handler's [`flush`](super::SingletonSubscriber::flush) inside
//! the batch transaction, then persists the checkpoint at the last *fully
//! handled* sequence (skips included), then commits: items, work and pointer
//! are inseparable. A failed flush rolls everything back and replays the
//! whole batch (items are re-collected), so collected work must tolerate
//! wholesale replay — the same contract as [`defer`](BatchOp::defer).

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
    /// Number of events contributing to the pending batch (consumed into the
    /// op or collected into the accumulator) — drives `max_batch_size`.
    pub(crate) events_in_op: usize,
    /// Number of events that collected items since the last flush. Nonzero
    /// means the accumulator is dirty: the batch is open even without an op,
    /// and no checkpoint may be persisted before the items are flushed.
    pub(crate) collected: usize,
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
/// Only obtainable from [`EventCtx::skip`], [`EventCtx::collect_with`] (or
/// its [`collect`](EventCtx::collect) sugar), [`BatchOp::commit`],
/// [`BatchOp::defer`] or [`IsolatedOp::commit`] — the type system forces
/// every [`handle_persistent`](super::SingletonSubscriber::handle_persistent)
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
    Collect,
    Commit,
    Defer,
    /// Keyed-only: my cursor holds *before* this event until the given time.
    /// Only [`KeyedEventCtx::hold_until`] mints this — the singleton runner
    /// never constructs a ctx that can, so this variant is unreachable from
    /// [`EventCtx`].
    Hold(chrono::DateTime<chrono::Utc>),
    /// Keyed-only: commit my isolated work, but the cursor still holds
    /// *before* this event until the given time. Only
    /// [`KeyedIsolatedOp::commit_and_hold`] mints this.
    CommitAndHold(chrono::DateTime<chrono::Utc>),
}

/// Per-event decision point handed to
/// [`handle_persistent`](super::SingletonSubscriber::handle_persistent).
///
/// Generic over the handler's
/// [`Batch`](super::SingletonSubscriber::Batch) accumulator `B` (defaulting
/// to `()` for handlers that never collect). See the [module docs](self)
/// for the semantics of the four entry verbs.
#[must_use = "resolve the EventCtx via skip / collect / consume_in_batch / consume_isolated"]
pub struct EventCtx<'inv, B = ()> {
    pub(crate) parts: CtxParts<'inv>,
    pub(crate) batch: &'inv mut B,
    pub(crate) flusher: &'inv dyn ItemFlush<B>,
}

impl<'inv, B> EventCtx<'inv, B> {
    /// This event is not for me — no transaction is opened, an open batch op
    /// is left untouched, and the checkpoint advances lazily (piggybacked on
    /// the next flush, or persisted on the configured checkpoint interval).
    pub fn skip(self) -> Handled<'inv> {
        Handled {
            outcome: Outcome::Skip,
            _invocation: PhantomData,
        }
    }

    /// Contribute to the pending batch's accumulator — a pure memory write:
    /// no transaction is opened and no statement is executed now. The runner
    /// hands the accumulated batch to the handler's
    /// [`flush`](super::SingletonSubscriber::flush) exactly once per batch
    /// landing, inside the transaction that commits the checkpoint.
    ///
    /// Like [`defer`](BatchOp::defer), collected work shares fate with its
    /// neighbors and must tolerate whole-batch replay: on a failed flush the
    /// events replay and their items are re-collected.
    ///
    /// For `Vec` and `HashMap` accumulators the [`collect`](Self::collect)
    /// sugar is usually more convenient.
    pub fn collect_with(self, f: impl FnOnce(&mut B)) -> Handled<'inv> {
        f(self.batch);
        self.parts.tracker.events_in_op += 1;
        self.parts.tracker.collected += 1;
        Handled {
            outcome: Outcome::Collect,
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
        let op = parts.op_slot.as_mut().expect("just materialized above");
        Ok(BatchOp { op })
    }

    /// Land the pending batch first (its collected items, its work and its
    /// checkpoint, at the last fully handled sequence), then hand back a
    /// fresh op: this event is its own atomic unit, sharing no fate with
    /// history — and none with the future either, since [`IsolatedOp`] only
    /// offers [`commit`](IsolatedOp::commit).
    ///
    /// This is the failure-isolation fence: if this event's work fails, only
    /// this event replays; and it is the exact legacy per-event semantics
    /// when no batch is pending.
    pub async fn consume_isolated(self) -> Result<IsolatedOp<'inv>, HandlerError>
    where
        B: Default,
    {
        let EventCtx {
            mut parts,
            batch,
            flusher,
        } = self;
        flush_batch(&mut parts, batch, flusher, "isolated_entry").await?;
        *parts.op_slot = Some(
            es_entity::DbOp::init_with_clock(parts.current_job.pool(), parts.current_job.clock())
                .await?,
        );
        parts.tracker.events_in_op = 1;
        let op = parts.op_slot.as_mut().expect("just materialized above");
        Ok(IsolatedOp { op })
    }
}

impl<'inv, T> EventCtx<'inv, Vec<T>> {
    /// [`collect_with`](Self::collect_with) sugar for `Vec` accumulators:
    /// append one item to the pending batch.
    pub fn collect(self, item: T) -> Handled<'inv> {
        self.collect_with(|batch| batch.push(item))
    }
}

impl<'inv, K, V, S> EventCtx<'inv, std::collections::HashMap<K, V, S>>
where
    K: std::hash::Hash + Eq,
    S: std::hash::BuildHasher,
{
    /// [`collect_with`](Self::collect_with) sugar for `HashMap` accumulators:
    /// keyed last-write-wins insert. Persistent events arrive in ascending
    /// sequence, so within a batch this naturally keeps the newest item per
    /// key — the coalescing fold (N updates per key → 1 flushed entry).
    pub fn collect(self, key: K, value: V) -> Handled<'inv> {
        self.collect_with(|batch| {
            batch.insert(key, value);
        })
    }
}

/// An op joined to the pending batch. Implements
/// [`AtomicOperation`](es_entity::AtomicOperation) — use it exactly like any
/// atomic operation, then exit with [`commit`](Self::commit) or
/// [`defer`](Self::defer).
///
/// There is no mutable access to the raw [`es_entity::DbOp`] (only a shared
/// [`Deref`](std::ops::Deref) view): committing, rolling back, or swapping
/// out the underlying op is unrepresentable, so work and checkpoint can only
/// land together, through the runner.
#[must_use = "exit with .commit() or .defer() to produce the Handled token"]
pub struct BatchOp<'inv> {
    op: &'inv mut es_entity::DbOp<'static>,
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
    /// runner lands it when the ready persistent backlog is drained, when
    /// the configured max batch size is reached, when a later event commits
    /// or isolates, or on shutdown.
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
        self.op
    }
}

es_entity::delegate_atomic_operation!(BatchOp<'_>, { s => s.op });

/// An op holding exactly this event's work, fenced from the batch history.
/// Implements [`AtomicOperation`](es_entity::AtomicOperation). The only exit
/// is [`commit`](Self::commit) — isolation from future events is guaranteed
/// by construction, and (as with [`BatchOp`]) no mutable access to the raw
/// [`es_entity::DbOp`] exists, so the op can only land through the runner.
#[must_use = "exit with .commit() to produce the Handled token"]
pub struct IsolatedOp<'inv> {
    op: &'inv mut es_entity::DbOp<'static>,
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
        self.op
    }
}

es_entity::delegate_atomic_operation!(IsolatedOp<'_>, { s => s.op });

pub(crate) type BoxFuture<'a, T> =
    std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// Object-safe bridge from the runner (and [`EventCtx::consume_isolated`]'s
/// entry fence) to the handler's typed
/// [`flush`](super::SingletonSubscriber::flush) — erases the handler type so
/// [`EventCtx`] only needs to know the accumulator `B`.
pub(crate) trait ItemFlush<B>: Send + Sync {
    fn flush_items<'a>(
        &'a self,
        op: &'a mut es_entity::DbOp<'static>,
        items: B,
    ) -> BoxFuture<'a, Result<(), HandlerError>>;
}

/// Restricted view of the batch op handed to
/// [`flush`](super::SingletonSubscriber::flush) — everything an
/// [`AtomicOperation`](es_entity::AtomicOperation) can do, and nothing else.
///
/// Committing belongs to the runner: after `flush` returns `Ok`, the
/// checkpoint is written and the transaction commits — items, work and
/// pointer land atomically. There is no access to the raw
/// [`es_entity::DbOp`], mirroring [`BatchOp`]/[`IsolatedOp`]'s sealing.
pub struct FlushOp<'a>(&'a mut es_entity::DbOp<'static>);

impl<'a> FlushOp<'a> {
    pub(crate) fn new(op: &'a mut es_entity::DbOp<'static>) -> Self {
        Self(op)
    }
}

es_entity::delegate_atomic_operation!(FlushOp<'_>, { s => s.0 });

/// A batch flush failed. Carries the sequence range actually at fault, so
/// the failure is not misattributed to the (innocent) event whose verb
/// happened to trigger the landing — e.g. a later event entering
/// [`consume_isolated`](EventCtx::consume_isolated).
///
/// Propagates through `handle_persistent` as a boxed error; downcast to
/// re-attribute in logs or traces.
#[derive(Debug)]
pub struct FlushError {
    /// Which trigger landed the batch (`"backlog_drained"`, `"batch_full"`,
    /// `"commit"`, `"isolated_entry"`, `"shutdown"`, `"stream_closed"`,
    /// `"undecodable_event"`).
    pub reason: &'static str,
    /// The batch covers sequences strictly after this (the last durable
    /// checkpoint)…
    pub after: EventSequence,
    /// …through this (the last fully handled event).
    pub through: EventSequence,
    pub source: HandlerError,
}

impl std::fmt::Display for FlushError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "flush of batch ({}, {}] failed (reason={}): {}",
            self.after, self.through, self.reason, self.source
        )
    }
}

impl std::error::Error for FlushError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

/// Land the pending batch, if any: hand collected items to the handler's
/// flush (inside the batch transaction, opening one if only items are
/// pending) → checkpoint at the last fully handled sequence → commit. No-op
/// when nothing is pending.
#[tracing::instrument(
    name = "outbox.flush_batch",
    skip_all,
    fields(
        reason = reason,
        batch_size = parts.tracker.events_in_op,
        collected = parts.tracker.collected,
        checkpoint_seq = u64::from(parts.state.sequence),
    ),
    err
)]
pub(crate) async fn flush_batch<B: Default>(
    parts: &mut CtxParts<'_>,
    batch: &mut B,
    flusher: &dyn ItemFlush<B>,
    reason: &'static str,
) -> Result<(), HandlerError> {
    if parts.op_slot.is_none() && parts.tracker.collected == 0 {
        return Ok(());
    }
    if parts.tracker.collected > 0 {
        if parts.op_slot.is_none() {
            *parts.op_slot = Some(
                es_entity::DbOp::init_with_clock(
                    parts.current_job.pool(),
                    parts.current_job.clock(),
                )
                .await?,
            );
        }
        // Drain before the call: on error the items are dropped with the op,
        // and the replayed events re-collect them — the accumulator never
        // leaks stale state into a retry.
        let items = std::mem::take(batch);
        parts.tracker.collected = 0;
        let op = parts.op_slot.as_mut().expect("op was materialized above");
        if let Err(source) = flusher.flush_items(op, items).await {
            return Err(Box::new(FlushError {
                reason,
                after: parts.tracker.persisted_seq,
                through: parts.state.sequence,
                source,
            }));
        }
    }
    let mut op = parts
        .op_slot
        .take()
        .expect("a pending batch always has an op by now");
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

// === Keyed subscribers ===
//
// [`KeyedEventCtx`] and [`KeyedIsolatedOp`] below are the keyed counterparts
// of [`EventCtx`] and [`IsolatedOp`] — a facade over the exact same internals
// (`CtxParts`, `Outcome`, `flush_batch`) rather than a fork of them. They add
// exactly one capability past the shared verb set: the hold verbs
// ([`KeyedEventCtx::hold_until`] / [`KeyedIsolatedOp::commit_and_hold`]),
// kept on distinct types so the capability is type-gated — the singleton
// runner never constructs a ctx that can mint `Outcome::Hold` /
// `Outcome::CommitAndHold`. `BatchOp` needs no keyed variant: it carries no
// hold exit in v1, so [`KeyedEventCtx::consume_in_batch`] hands back the
// exact same [`BatchOp`] singleton subscribers use.

/// Per-event decision point handed to
/// [`KeyedSubscriber::handle`](super::keyed_job::KeyedSubscriber::handle) —
/// the keyed counterpart of [`EventCtx`], sharing its verb semantics;
/// [`hold_until`](Self::hold_until) is the one keyed-only addition.
#[must_use = "resolve the KeyedEventCtx via skip / collect / consume_in_batch / consume_isolated / hold_until"]
pub struct KeyedEventCtx<'inv, B = ()> {
    pub(crate) parts: CtxParts<'inv>,
    pub(crate) batch: &'inv mut B,
    pub(crate) flusher: &'inv dyn ItemFlush<B>,
}

impl<'inv, B> KeyedEventCtx<'inv, B> {
    /// Identical to [`EventCtx::skip`].
    pub fn skip(self) -> Handled<'inv> {
        Handled {
            outcome: Outcome::Skip,
            _invocation: PhantomData,
        }
    }

    /// Identical to [`EventCtx::collect_with`].
    pub fn collect_with(self, f: impl FnOnce(&mut B)) -> Handled<'inv> {
        f(self.batch);
        self.parts.tracker.events_in_op += 1;
        self.parts.tracker.collected += 1;
        Handled {
            outcome: Outcome::Collect,
            _invocation: PhantomData,
        }
    }

    /// Identical to [`EventCtx::consume_in_batch`]. Returns the exact same
    /// [`BatchOp`] singleton subscribers use — it carries no hold exit in v1.
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
        let op = parts.op_slot.as_mut().expect("just materialized above");
        Ok(BatchOp { op })
    }

    /// Identical to [`EventCtx::consume_isolated`], but returns a
    /// [`KeyedIsolatedOp`] — the isolated op with the extra
    /// [`commit_and_hold`](KeyedIsolatedOp::commit_and_hold) exit.
    pub async fn consume_isolated(self) -> Result<KeyedIsolatedOp<'inv>, HandlerError>
    where
        B: Default,
    {
        let KeyedEventCtx {
            mut parts,
            batch,
            flusher,
        } = self;
        flush_batch(&mut parts, batch, flusher, "isolated_entry").await?;
        *parts.op_slot = Some(
            es_entity::DbOp::init_with_clock(parts.current_job.pool(), parts.current_job.clock())
                .await?,
        );
        parts.tracker.events_in_op = 1;
        let op = parts.op_slot.as_mut().expect("just materialized above");
        Ok(KeyedIsolatedOp { op })
    }

    /// My cursor holds *before* this event until `at` — entry and exit in
    /// one, nothing to record.
    ///
    /// The runner lands any pending batch first (the same fence as
    /// [`consume_isolated`](Self::consume_isolated)'s entry — checkpoint at
    /// the last fully-handled sequence, which is pre-this-event), persists
    /// the checkpoint if dirty, then ends the run rescheduled at `at`. Does
    /// **not** advance the checkpoint past this event: the next run re-reads
    /// and re-evaluates it, so a hold is retried, not skipped.
    ///
    /// The resume time is domain knowledge (e.g. a retry schedule owned by
    /// the delivery entity) — the one fact obix cannot derive on its own.
    /// Everything else about parking and waking (passivation, generations,
    /// wake) is derivable and stays internal.
    pub fn hold_until(self, at: chrono::DateTime<chrono::Utc>) -> Handled<'inv> {
        Handled {
            outcome: Outcome::Hold(at),
            _invocation: PhantomData,
        }
    }
}

impl<'inv, T> KeyedEventCtx<'inv, Vec<T>> {
    /// [`collect_with`](Self::collect_with) sugar for `Vec` accumulators.
    pub fn collect(self, item: T) -> Handled<'inv> {
        self.collect_with(|batch| batch.push(item))
    }
}

impl<'inv, K, V, S> KeyedEventCtx<'inv, std::collections::HashMap<K, V, S>>
where
    K: std::hash::Hash + Eq,
    S: std::hash::BuildHasher,
{
    /// [`collect_with`](Self::collect_with) sugar for `HashMap` accumulators.
    pub fn collect(self, key: K, value: V) -> Handled<'inv> {
        self.collect_with(|batch| {
            batch.insert(key, value);
        })
    }
}

/// An isolated op for a keyed subscriber: everything [`IsolatedOp`] offers,
/// plus [`commit_and_hold`](Self::commit_and_hold) — commit this event's work
/// while keeping the cursor parked before it.
///
/// A distinct type from [`IsolatedOp`] so the hold capability is type-gated
/// to keyed subscribers: the singleton runner never constructs one, so
/// `Outcome::CommitAndHold` is unreachable from [`IsolatedOp`].
#[must_use = "exit with .commit() or .commit_and_hold() to produce the Handled token"]
pub struct KeyedIsolatedOp<'inv> {
    op: &'inv mut es_entity::DbOp<'static>,
}

impl<'inv> KeyedIsolatedOp<'inv> {
    /// Land my work and my checkpoint, atomically, when the invocation
    /// returns. Identical to [`IsolatedOp::commit`].
    pub fn commit(self) -> Handled<'inv> {
        Handled {
            outcome: Outcome::Commit,
            _invocation: PhantomData,
        }
    }

    /// Commit my work atomically, but the cursor still holds *before* this
    /// event — resume at `at`.
    ///
    /// The lana retry-scheduled-settle shape: attempt record + health fold +
    /// (pre-event) checkpoint, one op, then rescheduled. The checkpoint
    /// written here does **not** advance past this event — the next run
    /// re-reads it and re-evaluates the hold condition.
    pub fn commit_and_hold(self, at: chrono::DateTime<chrono::Utc>) -> Handled<'inv> {
        Handled {
            outcome: Outcome::CommitAndHold(at),
            _invocation: PhantomData,
        }
    }
}

impl std::ops::Deref for KeyedIsolatedOp<'_> {
    type Target = es_entity::DbOp<'static>;

    fn deref(&self) -> &Self::Target {
        self.op
    }
}

es_entity::delegate_atomic_operation!(KeyedIsolatedOp<'_>, { s => s.op });
