//! Handler-controlled transaction scoping for outbox event-handler jobs.
//!
//! Every persistent event delivered to an
//! [`SingletonSubscriber`](super::SingletonSubscriber) comes with an
//! [`EventCtx`] that the handler must resolve into a [`Handled`] token by
//! choosing exactly one of three entry verbs — a monotone cost ladder:
//!
//! | verb | meaning | cost |
//! |------|---------|------|
//! | [`EventCtx::skip`] | not my event | zero — no transaction is opened |
//! | [`EventCtx::collect_with`] (and the [`collect`](EventCtx::collect) sugar) | contribute an item to the pending batch's accumulator | zero at collect time — one [`flush`](super::SingletonSubscriber::flush) call per batch applies all items |
//! | [`EventCtx::consume`] | land the pending batch first, then do my work in a fresh op | my event is its own atomic unit, fenced from history |
//!
//! and — when an op was taken — the single exit verb
//! [`IsolatedOp::commit`], which lands the op (work + checkpoint,
//! atomically) when the invocation returns.
//!
//! A pending batch is **only ever collected items**: no transaction is left
//! open across event invocations. It exists only while there is ready
//! persistent backlog — the runner never awaits the stream while items are
//! pending, so a pending stream is itself a flush trigger. Batching
//! therefore rides bursts that already happened and adds no latency at low
//! traffic. Ephemeral events travel on their own stream
//! and are handled between batches — they never interrupt a batch, a
//! transaction never spans the foreign `handle_ephemeral` await, and between
//! batches the two streams race fairly so neither can starve the other.
//! Handlers that only consume one stream should declare it via
//! [`SUBSCRIPTION`](super::SingletonSubscriber::SUBSCRIPTION) — the other
//! stream is then never subscribed at all.
//!
//! Every flush — whichever of the triggers fires — first hands all collected
//! items to the handler's [`flush`](super::SingletonSubscriber::flush) inside
//! a transaction opened for the landing, then persists the checkpoint at the
//! last *fully handled* sequence (skips included), then commits: items, work
//! and pointer are inseparable. A failed flush rolls everything back and
//! replays the whole batch (items are re-collected), so collected work must
//! tolerate wholesale replay.

use serde::{Deserialize, Serialize};

use std::marker::PhantomData;

use job::CurrentJob;

use crate::sequence::EventSequence;

/// Error type shared with the handler trait methods.
pub(crate) type HandlerError = Box<dyn std::error::Error + Send + Sync>;

/// Persisted execution state of an outbox event-handler job: the sequence of
/// the last fully handled persistent event, plus (keyed subscribers only)
/// the resume token of the event currently being processed.
#[derive(Default, Clone, Serialize, Deserialize)]
pub(crate) struct OutboxEventJobState {
    pub(crate) sequence: EventSequence,
    /// `serde(default)` so execution-state rows written before staged
    /// processing existed still decode; `skip_serializing_if` so a singleton
    /// subscriber's state stays byte-identical to what it always wrote.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) staged: Option<StagedState>,
}

/// The resume-token slot: an opaque JSON value scoped to one event.
///
/// obix owns the slot and its lifetime; the meaning of what is in it belongs
/// entirely to the subscriber. obix never interprets the token.
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct StagedState {
    /// The event the token belongs to. Validity is checked at read time
    /// against the event being processed, which is what makes clearing lazy:
    /// a stale token is unreadable by construction.
    pub(crate) sequence: EventSequence,
    pub(crate) token: serde_json::Value,
}

/// Book-keeping the runner shares with [`EventCtx`].
pub(crate) struct BatchTracker {
    /// Number of events that collected items since the last flush. Nonzero
    /// means the accumulator is dirty: a batch is pending, and no checkpoint
    /// may be persisted before the items are flushed. Also what
    /// `max_batch_size` bounds — with no deferred ops, the pending batch is
    /// exactly its collected events.
    pub(crate) collected: usize,
    /// Highest sequence whose checkpoint has been persisted to the database.
    pub(crate) persisted_seq: EventSequence,
    /// When the checkpoint was last persisted (any flush or standalone write).
    pub(crate) last_persist: tokio::time::Instant,
}

pub(crate) struct CtxParts<'inv> {
    pub(crate) op_slot: &'inv mut Option<es_entity::DbOp<'static>>,
    pub(crate) current_job: &'inv mut CurrentJob,
    /// Mutable so the staged verbs can write the resume token into the state
    /// the runner is about to persist. `sequence` remains the runner's alone.
    pub(crate) state: &'inv mut OutboxEventJobState,
    pub(crate) tracker: &'inv mut BatchTracker,
}

/// Proof that a persistent event was resolved in one of the legal ways.
///
/// Only obtainable from [`EventCtx::skip`], [`EventCtx::collect_with`] (or
/// its [`collect`](EventCtx::collect) sugar) or [`IsolatedOp::commit`] — the
/// type system forces every
/// [`handle_persistent`](super::SingletonSubscriber::handle_persistent)
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
    /// Keyed-only: my cursor holds *before* this event until the given time.
    /// Only [`KeyedEventCtx::hold_until`] mints this — the singleton runner
    /// never constructs a ctx that can, so this variant is unreachable from
    /// [`EventCtx`].
    Hold(chrono::DateTime<chrono::Utc>),
    /// Keyed-only: commit the staged op left in `op_slot`, but the cursor
    /// still holds *before* this event until the given time. Only
    /// [`StagedOp::hold_until`] mints this.
    CommitAndHold(chrono::DateTime<chrono::Utc>),
}

/// Per-event decision point handed to
/// [`handle_persistent`](super::SingletonSubscriber::handle_persistent).
///
/// Generic over the handler's
/// [`Batch`](super::SingletonSubscriber::Batch) accumulator `B` (defaulting
/// to `()` for handlers that never collect). See the [module docs](self)
/// for the semantics of the three entry verbs.
#[must_use = "resolve the EventCtx via skip / collect / consume"]
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
    /// Collected work shares fate with its neighbors and must tolerate
    /// whole-batch replay: on a failed flush the events replay and their
    /// items are re-collected.
    ///
    /// For `Vec` and `HashMap` accumulators the [`collect`](Self::collect)
    /// sugar is usually more convenient.
    pub fn collect_with(self, f: impl FnOnce(&mut B)) -> Handled<'inv> {
        f(self.batch);
        self.parts.tracker.collected += 1;
        Handled {
            outcome: Outcome::Collect,
            _invocation: PhantomData,
        }
    }

    /// Land the pending batch first (its collected items and its checkpoint,
    /// at the last fully handled sequence), then hand back a fresh op: this
    /// event is its own atomic unit, sharing no fate with history — and none
    /// with the future either, since [`IsolatedOp`] only offers
    /// [`commit`](IsolatedOp::commit).
    ///
    /// This is the failure-isolation fence: if this event's work fails, only
    /// this event replays.
    pub async fn consume(self) -> Result<IsolatedOp<'inv>, HandlerError>
    where
        B: Default,
    {
        let EventCtx {
            mut parts,
            batch,
            flusher,
        } = self;
        flush_batch(&mut parts, batch, flusher, "consume_entry").await?;
        *parts.op_slot = Some(
            es_entity::DbOp::init_with_clock(parts.current_job.pool(), parts.current_job.clock())
                .await?,
        );
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

/// An op holding exactly this event's work, fenced from the batch history.
/// Implements [`AtomicOperation`](es_entity::AtomicOperation). The only exit
/// is [`commit`](Self::commit) — isolation from future events is guaranteed
/// by construction, and there is no mutable access to the raw
/// [`es_entity::DbOp`] (only a shared [`Deref`](std::ops::Deref) view):
/// committing, rolling back, or swapping out the underlying op is
/// unrepresentable, so work and checkpoint can only land together, through
/// the runner.
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

/// Object-safe bridge from the runner (and [`EventCtx::consume`]'s
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
/// [`es_entity::DbOp`], mirroring [`IsolatedOp`]'s sealing.
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
/// [`consume`](EventCtx::consume).
///
/// Propagates through `handle_persistent` as a boxed error; downcast to
/// re-attribute in logs or traces.
#[derive(Debug)]
pub struct FlushError {
    /// Which trigger landed the batch (`"backlog_drained"`, `"batch_full"`,
    /// `"commit"`, `"consume_entry"`, `"shutdown"`, `"stream_closed"`,
    /// `"undecodable_event"`, and for keyed subscribers `"hold_entry"` and
    /// `"staged_hold"`).
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
/// flush (inside a transaction opened for the landing) → checkpoint at the
/// last fully handled sequence → commit. Also lands a runner-committed op
/// left in `op_slot` by [`EventCtx::consume`]. No-op when nothing is pending.
#[tracing::instrument(
    name = "outbox.flush_batch",
    skip_all,
    fields(
        reason = reason,
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
// [`KeyedEventCtx`] below is the keyed counterpart of [`EventCtx`] — a facade
// over the exact same internals (`CtxParts`, `Outcome`, `flush_batch`) rather
// than a fork of them. It adds two capabilities past the shared verb set,
// kept on distinct types so both are type-gated (the singleton runner never
// constructs a ctx that can reach them):
//
//   - the hold verb ([`KeyedEventCtx::hold_until`]), which mints
//     `Outcome::Hold`;
//   - staged processing ([`StagedOp`] / [`StagedEvent`]), which lets one
//     event be processed across N committed transactions with external I/O
//     between them, and whose paused exit mints `Outcome::CommitAndHold`.

/// Per-event decision point handed to
/// [`KeyedSubscriber::handle`](super::KeyedSubscriber::handle) — the keyed
/// counterpart of [`EventCtx`], sharing its verb semantics;
/// [`hold_until`](Self::hold_until) and [`consume`](Self::consume)'s staged
/// chain are the keyed-only additions.
#[must_use = "resolve the KeyedEventCtx via skip / collect / consume / hold_until"]
pub struct KeyedEventCtx<'inv, B = ()> {
    pub(crate) parts: CtxParts<'inv>,
    pub(crate) batch: &'inv mut B,
    pub(crate) flusher: &'inv dyn ItemFlush<B>,
    /// Sequence of the event being processed. Distinct from
    /// `parts.state.sequence`, which is the last *fully handled* sequence —
    /// i.e. strictly before this one.
    pub(crate) event_seq: EventSequence,
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
        self.parts.tracker.collected += 1;
        Handled {
            outcome: Outcome::Collect,
            _invocation: PhantomData,
        }
    }

    /// Take this event's work, in one stage or many.
    ///
    /// Same entry fence as [`EventCtx::consume`]: the pending batch (its
    /// collected items and its checkpoint, at the last fully-handled
    /// sequence — strictly before this event) lands first, then a fresh op
    /// is opened. The difference is the exit: a [`StagedOp`] can be
    /// *concluded* (cursor advances, today's isolated-commit semantics), or
    /// it can [`proceed`](StagedOp::proceed) — commit this stage and hand
    /// back a [`StagedEvent`] with **no transaction open**, so the
    /// subscriber can do external I/O before opening the next stage's op.
    ///
    /// The single-transaction case is the one-stage degenerate case: consume,
    /// work, [`conclude`](StagedOp::conclude).
    ///
    /// Interim stages are fenced before the cursor and replayed on crash:
    /// nothing a `proceed` committed is lost, but the event itself is
    /// re-read and re-handled until a `conclude` advances past it. Use
    /// [`resume`](StagedOp::resume) to skip stages already durable.
    pub async fn consume(self) -> Result<StagedOp<'inv>, HandlerError>
    where
        B: Default,
    {
        let KeyedEventCtx {
            mut parts,
            batch,
            flusher,
            event_seq,
        } = self;
        flush_batch(&mut parts, batch, flusher, "consume_entry").await?;
        let op =
            es_entity::DbOp::init_with_clock(parts.current_job.pool(), parts.current_job.clock())
                .await?;
        Ok(StagedOp {
            op,
            parts,
            event_seq,
        })
    }

    /// My cursor holds *before* this event until `at` — entry and exit in
    /// one, nothing to record.
    ///
    /// The runner lands any pending batch first (the same fence as
    /// [`consume`](Self::consume)'s entry — checkpoint at
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

/// One stage of a keyed subscriber's processing of one event: an open op,
/// which every exit commits.
///
/// Implements [`AtomicOperation`](es_entity::AtomicOperation) — use it like
/// any atomic operation, then take exactly one exit:
///
/// | exit | meaning | cursor |
/// |------|---------|--------|
/// | [`proceed`](Self::proceed) | this stage is done, the chain continues | unmoved |
/// | [`hold_until`](Self::hold_until) | this stage is done, processing pauses until `at` | unmoved |
/// | [`conclude`](Self::conclude) | processing of this event is done | advances past the event |
///
/// "Commit" appears in none of the names because all three commit: saying it
/// every time would be mechanics leaking into a semantic surface. What
/// differs is what happens to the *cursor*.
///
/// As with [`IsolatedOp`], there is no mutable access to the raw
/// [`es_entity::DbOp`] — the op can only land through one of the exits, so
/// no stage can commit without the runner knowing what it meant.
///
/// Sealed to its invocation by the same brand [`Handled`] carries, so a
/// staged op cannot be stashed and resumed from a later event:
///
/// ```compile_fail
/// use obix::StagedOp;
///
/// struct Evil {
///     stash: std::sync::Mutex<Option<StagedOp<'static>>>,
/// }
///
/// async fn stash_it(op: StagedOp<'_>, evil: &Evil) {
///     // error[E0521]: borrowed data escapes outside of function
///     *evil.stash.lock().unwrap() = Some(op);
/// }
/// ```
#[must_use = "exit with .proceed() / .hold_until() / .conclude()"]
pub struct StagedOp<'inv> {
    op: es_entity::DbOp<'static>,
    parts: CtxParts<'inv>,
    event_seq: EventSequence,
}

impl<'inv> StagedOp<'inv> {
    /// Commit this stage and continue: the returned [`StagedEvent`] holds
    /// **no open transaction**, so the subscriber can await external I/O
    /// before opening the next stage's op.
    ///
    /// The cursor does not move — this event is still being processed, and a
    /// crash here replays it (with everything this stage committed already
    /// durable).
    pub async fn proceed(self) -> Result<StagedEvent<'inv>, HandlerError> {
        let StagedOp {
            op,
            parts,
            event_seq,
        } = self;
        op.commit().await?;
        Ok(StagedEvent { parts, event_seq })
    }

    /// [`proceed`](Self::proceed), and rewrite the resume token in the same
    /// transaction as this stage's work.
    ///
    /// That atomicity is the point: a crash after this returns leaves the
    /// stage's writes *and* the token that records them durable together, so
    /// the replay skips the stage via [`resume`](Self::resume) rather than
    /// relying on the work being idempotent.
    pub async fn proceed_with<T: Serialize>(
        self,
        token: &T,
    ) -> Result<StagedEvent<'inv>, HandlerError> {
        let StagedOp {
            mut op,
            parts,
            event_seq,
        } = self;
        parts.state.staged = Some(StagedState {
            sequence: event_seq,
            token: serde_json::to_value(token)?,
        });
        parts
            .current_job
            .update_execution_state_in_op(&mut op, &*parts.state)
            .await?;
        op.commit().await?;
        Ok(StagedEvent { parts, event_seq })
    }

    /// The resume token, if one was written while processing *this* event.
    ///
    /// In-memory — the runner already loaded the execution state — so this
    /// does no I/O. `None` when nothing was staged, and also when a token
    /// left over from a different event is still in the slot: validity is
    /// checked against the current event's sequence, which is why stale
    /// tokens never need explicit clearing.
    ///
    /// A deserialization failure surfaces as `Err`; what to do about it is
    /// the subscriber's call. Tokens survive holds, so they can outlive a
    /// deploy — schema-stamping them and treating a mismatch as "start
    /// fresh" is the recommended consumer contract.
    pub fn resume<T: serde::de::DeserializeOwned>(&self) -> Result<Option<T>, serde_json::Error> {
        resume_token(self.parts.state, self.event_seq)
    }

    /// Commit this stage; processing pauses with the cursor still parked
    /// *before* this event, resuming at `at`.
    ///
    /// The op's work lands, but the checkpoint the runner folds in is still
    /// pre-this-event: the next run re-reads the event and re-evaluates. The
    /// resume time is domain knowledge (a retry schedule owned by the
    /// consumer's entities) — the one fact obix cannot derive.
    /// The token survives a hold: a hold is part of processing the event, and
    /// concluded-for-now is not the same as the cursor advancing. It can
    /// therefore live for as long as the backoff does.
    pub fn hold_until(self, at: chrono::DateTime<chrono::Utc>) -> Handled<'inv> {
        let StagedOp { op, parts, .. } = self;
        *parts.op_slot = Some(op);
        Handled {
            outcome: Outcome::CommitAndHold(at),
            _invocation: PhantomData,
        }
    }

    /// [`hold_until`](Self::hold_until), and rewrite the resume token in the
    /// same transaction as this stage's work — the runner folds the state
    /// write into the op it lands.
    pub fn hold_until_with<T: Serialize>(
        self,
        at: chrono::DateTime<chrono::Utc>,
        token: &T,
    ) -> Result<Handled<'inv>, HandlerError> {
        let StagedOp {
            op,
            parts,
            event_seq,
        } = self;
        parts.state.staged = Some(StagedState {
            sequence: event_seq,
            token: serde_json::to_value(token)?,
        });
        *parts.op_slot = Some(op);
        Ok(Handled {
            outcome: Outcome::CommitAndHold(at),
            _invocation: PhantomData,
        })
    }

    /// Commit this stage and conclude processing of this event: the runner
    /// folds the checkpoint at this event's sequence into the same
    /// transaction, so work and cursor advance together.
    ///
    /// There is deliberately no `conclude_with`: the token's lifetime *is*
    /// the event's processing, so concluding clears it — opportunistically,
    /// since this path writes the state anyway.
    pub fn conclude(self) -> Handled<'inv> {
        let StagedOp { op, parts, .. } = self;
        parts.state.staged = None;
        *parts.op_slot = Some(op);
        Handled {
            outcome: Outcome::Commit,
            _invocation: PhantomData,
        }
    }
}

impl std::ops::Deref for StagedOp<'_> {
    type Target = es_entity::DbOp<'static>;

    fn deref(&self) -> &Self::Target {
        &self.op
    }
}

es_entity::delegate_atomic_operation!(StagedOp<'_>, { s => s.op });

/// The gap between two stages of processing one event: **no transaction is
/// open**, which is exactly the point — this is where a subscriber awaits
/// external I/O.
///
/// Open the next stage's op with [`op`](Self::op) (it comes from the job's
/// pool and clock, and is traced like any other, which is why this is
/// ctx-mediated rather than a side-op the consumer opens itself), or pause
/// with [`hold_until`](Self::hold_until).
///
/// Sealed to its invocation, exactly as [`StagedOp`] is:
///
/// ```compile_fail
/// use obix::StagedEvent;
///
/// struct Evil {
///     stash: std::sync::Mutex<Option<StagedEvent<'static>>>,
/// }
///
/// async fn stash_it(staged: StagedEvent<'_>, evil: &Evil) {
///     // error[E0521]: borrowed data escapes outside of function
///     *evil.stash.lock().unwrap() = Some(staged);
/// }
/// ```
#[must_use = "continue with .op() or pause with .hold_until()"]
pub struct StagedEvent<'inv> {
    parts: CtxParts<'inv>,
    event_seq: EventSequence,
}

impl<'inv> StagedEvent<'inv> {
    /// Open the next stage's op.
    pub async fn op(self) -> Result<StagedOp<'inv>, HandlerError> {
        let StagedEvent { parts, event_seq } = self;
        let op =
            es_entity::DbOp::init_with_clock(parts.current_job.pool(), parts.current_job.clock())
                .await?;
        Ok(StagedOp {
            op,
            parts,
            event_seq,
        })
    }

    /// Pause with nothing further to record: the cursor holds *before* this
    /// event until `at`, exactly as [`KeyedEventCtx::hold_until`] does. The
    /// resume token (if any) is preserved.
    pub fn hold_until(self, at: chrono::DateTime<chrono::Utc>) -> Handled<'inv> {
        Handled {
            outcome: Outcome::Hold(at),
            _invocation: PhantomData,
        }
    }

    /// See [`StagedOp::resume`].
    pub fn resume<T: serde::de::DeserializeOwned>(&self) -> Result<Option<T>, serde_json::Error> {
        resume_token(self.parts.state, self.event_seq)
    }
}

/// Shared read path for [`StagedOp::resume`] / [`StagedEvent::resume`]: the
/// token is only visible to the event it was written for.
fn resume_token<T: serde::de::DeserializeOwned>(
    state: &OutboxEventJobState,
    event_seq: EventSequence,
) -> Result<Option<T>, serde_json::Error> {
    match &state.staged {
        Some(staged) if staged.sequence == event_seq => {
            serde_json::from_value(staged.token.clone()).map(Some)
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Execution-state rows written before staged processing existed have no
    /// `staged` field. They must still decode — a keyed subscriber upgrading
    /// into this version resumes from its checkpoint rather than restarting.
    #[test]
    fn pre_staged_execution_state_still_decodes() {
        let state: OutboxEventJobState =
            serde_json::from_str(r#"{"sequence":42}"#).expect("legacy state must decode");
        assert_eq!(u64::from(state.sequence), 42);
        assert!(state.staged.is_none());
    }

    /// And a state with no token serializes back to exactly what a singleton
    /// subscriber has always written — the new field adds no bytes.
    #[test]
    fn state_without_a_token_serializes_unchanged() {
        let state = OutboxEventJobState {
            sequence: EventSequence::from(7u64),
            staged: None,
        };
        assert_eq!(
            serde_json::to_string(&state).expect("serializes"),
            r#"{"sequence":7}"#
        );
    }

    /// The token is only readable by the event it was written for: validity
    /// is a sequence match, which is what makes stale-token clearing lazy.
    #[test]
    fn a_token_is_invisible_to_any_other_event() {
        let state = OutboxEventJobState {
            sequence: EventSequence::from(4u64),
            staged: Some(StagedState {
                sequence: EventSequence::from(5u64),
                token: serde_json::json!({ "stage": 1 }),
            }),
        };

        let mine: Option<serde_json::Value> =
            resume_token(&state, EventSequence::from(5u64)).expect("reads");
        assert_eq!(mine, Some(serde_json::json!({ "stage": 1 })));

        let other: Option<serde_json::Value> =
            resume_token(&state, EventSequence::from(6u64)).expect("reads");
        assert!(other.is_none(), "a stale token must not be readable");
    }
}
