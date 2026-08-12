//! Handler groups: N registered [`OutboxEventHandler`]s multiplexed onto one
//! resident job.
//!
//! A solo handler job costs one permanently-running job slot, one outbox
//! subscription and one checkpoint-write transaction per landing. Those costs
//! are per *handler*, so a consumer with dozens of listeners pays them dozens
//! of times over — enough to starve the job runner's slot budget and to make
//! `job_executions` the busiest table in the system.
//!
//! A group collapses that to one of each. Members keep their own handler, their
//! own [`Batch`](OutboxEventHandler::Batch) accumulator, their own checkpoint
//! and their own failure domain; what they share is the stream read, the
//! landing transaction and the execution-state row.
//!
//! Grouping is a deployment decision made at registration —
//! [`OutboxEventJobConfig::in_group`](super::OutboxEventJobConfig::in_group),
//! one line per call site. The [`OutboxEventHandler`] trait and the
//! [`EventCtx`] consume DSL are unchanged: a handler cannot tell whether it is
//! running solo or grouped, and does not need to.
//!
//! # Delivery
//!
//! One listener per group, positioned at the *minimum* member checkpoint. Each
//! event is dispatched to a member iff `event.sequence > member.checkpoint` —
//! the per-member delivery filter. A lagging member catches up through the
//! shared stream while members already past that point filter cheaply in
//! memory, with no handler call. Once every member has seen an event the filter
//! is a no-op, so convergence is structural rather than a special catch-up
//! phase.
//!
//! Per-member delivery guarantees are exactly the solo ones: gapless, in order,
//! and the checkpoint trails applied state.
//!
//! # Landing
//!
//! The group batch lands under the union of the solo triggers — backlog
//! drained, `max_batch_size` reached, any member committing or isolating, an
//! undecodable event, or shutdown. A landing is ONE transaction: every dirty
//! member's [`flush`](OutboxEventHandler::flush) in registration order, then a
//! single state write carrying every member's checkpoint, then commit.
//!
//! [`consume_isolated`](EventCtx::consume_isolated) keeps its fence semantics
//! for the requesting member: the pending group batch lands first — including
//! its siblings' collected items — then the isolated op runs and commits alone.
//! Other members merely observe a batch boundary, which was never part of the
//! delivery contract.
//!
//! # Failure isolation
//!
//! The unit of failure stays the handler. When a member errors, the landing
//! transaction rolls back and the member is *ejected*: its checkpoint is parked
//! at its last committed sequence (before the poison event) and it is never
//! dispatched to again. The batch then replays without it — replay tolerance is
//! already the DSL's contract — so its siblings advance past an event it could
//! not handle. The parked checkpoint keeps being written verbatim on every
//! subsequent landing, so a restart resumes the member exactly where it stopped
//! and nothing is skipped.
//!
//! Ejection is loud (an `ERROR`-level event) and, in this version, terminal for
//! the process: re-joining happens at the next restart. If every member ejects,
//! the group job fails so the job runner's own retry/backoff applies, which is
//! precisely the solo behaviour at group granularity.

use futures::{FutureExt, StreamExt};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use job::{CurrentJob, Job, JobCompletion, JobInitializer, JobRunner, JobSpawner, JobType};

use super::ctx::*;
use super::job::{EventSubscription, OutboxEventHandler};
use super::{EphemeralOutboxListener, Outbox, event::*};
use crate::{sequence::EventSequence, tables::MailboxTables};

/// Name of a handler group — and the [`JobType`] of the resident job hosting
/// it.
///
/// Takes a `&'static str` because that is all [`JobType`] can be built from: a
/// group name is a deployment constant, exactly like a job type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HandlerGroupName(JobType);

impl HandlerGroupName {
    pub const fn new(name: &'static str) -> Self {
        Self(JobType::new(name))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub(crate) fn job_type(&self) -> JobType {
        self.0.clone()
    }
}

impl std::fmt::Display for HandlerGroupName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A handler could not be registered into its group.
///
/// Every variant is a startup-time programming error, surfaced loudly rather
/// than degraded into a handler that silently receives nothing.
#[derive(Debug, thiserror::Error)]
pub enum HandlerGroupError {
    /// Membership is snapshotted when the group's job starts, so a member
    /// registered afterwards would never be dispatched to. Register every
    /// member before `Jobs::start_poll`.
    #[error(
        "handler group '{group}': member '{member}' registered after the group job started — \
         register all members before starting the job poller"
    )]
    GroupAlreadyStarted {
        group: HandlerGroupName,
        member: JobType,
    },
    /// The member key doubles as checkpoint identity, so it must be unique
    /// within a group.
    #[error("handler group '{group}': duplicate member '{member}'")]
    DuplicateMember {
        group: HandlerGroupName,
        member: JobType,
    },
    /// Batch size and checkpoint interval govern the shared landing, so they
    /// belong to the group. The first registration sets them; a later member
    /// disagreeing is an explicit error rather than a silently folded min/max.
    #[error(
        "handler group '{group}': member '{member}' sets {setting}={member_value}, conflicting \
         with the group's {group_value} (taken from the first registration)"
    )]
    ConflictingGroupSetting {
        group: HandlerGroupName,
        member: JobType,
        setting: &'static str,
        group_value: String,
        member_value: String,
    },
}

/// Persisted execution state of a group job: every member's committed
/// checkpoint, in one row.
///
/// `BTreeMap` so the serialized form is deterministic (stable diffs when
/// inspecting `job_executions` by hand).
#[derive(Default, Clone, Serialize, Deserialize)]
pub(crate) struct HandlerGroupJobState {
    pub(crate) members: BTreeMap<String, EventSequence>,
}

// ── member erasure ──────────────────────────────────────────────────────────

/// Registration-time description of a member: everything needed to build a
/// fresh per-run instance, with the handler type erased.
pub(crate) trait MemberSpec<P>: Send + Sync + 'static
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    fn key(&self) -> &JobType;
    fn subscription(&self) -> EventSubscription;
    fn instantiate(&self) -> Box<dyn MemberInstance<P>>;
}

struct TypedMemberSpec<H, P> {
    key: JobType,
    handler: Arc<H>,
    _payload: std::marker::PhantomData<fn() -> P>,
}

impl<H, P> MemberSpec<P> for TypedMemberSpec<H, P>
where
    H: OutboxEventHandler<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    fn key(&self) -> &JobType {
        &self.key
    }

    fn subscription(&self) -> EventSubscription {
        H::SUBSCRIPTION
    }

    fn instantiate(&self) -> Box<dyn MemberInstance<P>> {
        Box::new(TypedMember::<H, P> {
            key: self.key.clone(),
            handler: self.handler.clone(),
            batch: H::Batch::default(),
            dirty: false,
            flusher: super::job::HandlerFlusher::new(self.handler.clone()),
        })
    }
}

pub(crate) fn member_spec<H, P>(key: JobType, handler: H) -> Arc<dyn MemberSpec<P>>
where
    H: OutboxEventHandler<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    Arc::new(TypedMemberSpec::<H, P> {
        key,
        handler: Arc::new(handler),
        _payload: std::marker::PhantomData,
    })
}

/// One member's live state within a running group: its handler plus its private
/// accumulator. Type-erased over the handler's
/// [`Batch`](OutboxEventHandler::Batch) so the runner can hold a heterogeneous
/// `Vec` of them.
///
/// Deliberately only `Send`, not `Sync`: a handler's
/// [`Batch`](OutboxEventHandler::Batch) accumulator is only required to be
/// `Send`, so a member cannot be shared across threads. That is why the two
/// methods wrapping a foreign await take `&mut self` — a shared borrow held
/// across an await would demand `Sync` and exclude perfectly good accumulators.
pub(crate) trait MemberInstance<P>: Send
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    fn key(&self) -> &JobType;
    fn subscription(&self) -> EventSubscription;

    /// Whether this member holds collected items awaiting a flush.
    fn dirty(&self) -> bool;

    /// Resolve one persistent event through this member's handler, against the
    /// group's shared batch state.
    fn dispatch<'a>(
        &'a mut self,
        parts: CtxParts<'a>,
        event: &'a PersistentOutboxEvent<P>,
    ) -> BoxFuture<'a, Result<Outcome, HandlerError>>;

    /// Apply this member's accumulator onto the landing op. No-op when clean.
    fn flush_into<'a>(
        &'a mut self,
        op: &'a mut es_entity::DbOp<'static>,
    ) -> BoxFuture<'a, Result<(), HandlerError>>;

    /// Drop collected items without applying them — used when a landing rolls
    /// back, so the replay re-collects from scratch rather than double-applying.
    fn discard(&mut self);

    fn handle_undecodable<'a>(
        &'a mut self,
        error: &'a UndecodableEventError,
    ) -> BoxFuture<'a, Result<(), HandlerError>>;

    fn handle_ephemeral<'a>(
        &'a mut self,
        event: &'a EphemeralOutboxEvent<P>,
    ) -> BoxFuture<'a, Result<(), HandlerError>>;
}

struct TypedMember<H, P>
where
    H: OutboxEventHandler<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    key: JobType,
    handler: Arc<H>,
    batch: H::Batch,
    dirty: bool,
    flusher: super::job::HandlerFlusher<H, P>,
}

impl<H, P> MemberInstance<P> for TypedMember<H, P>
where
    H: OutboxEventHandler<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    fn key(&self) -> &JobType {
        &self.key
    }

    fn subscription(&self) -> EventSubscription {
        H::SUBSCRIPTION
    }

    fn dirty(&self) -> bool {
        self.dirty
    }

    fn dispatch<'a>(
        &'a mut self,
        parts: CtxParts<'a>,
        event: &'a PersistentOutboxEvent<P>,
    ) -> BoxFuture<'a, Result<Outcome, HandlerError>> {
        Box::pin(async move {
            let ctx = EventCtx {
                parts,
                batch: &mut self.batch,
                flusher: &self.flusher,
            };
            let outcome = self.handler.handle_persistent(ctx, event).await?.outcome;
            if outcome == Outcome::Collect {
                self.dirty = true;
            }
            Ok(outcome)
        })
    }

    fn flush_into<'a>(
        &'a mut self,
        op: &'a mut es_entity::DbOp<'static>,
    ) -> BoxFuture<'a, Result<(), HandlerError>> {
        Box::pin(async move {
            if !self.dirty {
                return Ok(());
            }
            // Drain before the call, mirroring the solo path: on error the items
            // go with the rolled-back op and the replay re-collects them.
            let items = std::mem::take(&mut self.batch);
            self.dirty = false;
            let mut flush_op = FlushOp::new(op);
            self.handler.flush(&mut flush_op, items).await
        })
    }

    fn discard(&mut self) {
        self.batch = H::Batch::default();
        self.dirty = false;
    }

    fn handle_undecodable<'a>(
        &'a mut self,
        error: &'a UndecodableEventError,
    ) -> BoxFuture<'a, Result<(), HandlerError>> {
        Box::pin(self.handler.handle_undecodable(error))
    }

    fn handle_ephemeral<'a>(
        &'a mut self,
        event: &'a EphemeralOutboxEvent<P>,
    ) -> BoxFuture<'a, Result<(), HandlerError>> {
        Box::pin(self.handler.handle_ephemeral(event))
    }
}

/// The members *other than* the one currently dispatching, split around it so a
/// landing preserves registration order: `left` registered earlier, `right`
/// later. Also used with an empty `right` for runner-initiated landings, where
/// there is no dispatching member and `left` is the whole membership.
struct GroupPeers<'a, P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    left: &'a mut [Box<dyn MemberInstance<P>>],
    right: &'a mut [Box<dyn MemberInstance<P>>],
    /// Absolute membership index of `right[0]`, so a failure in the right half
    /// can be attributed to the right member.
    right_offset: usize,
    /// Set when a peer's flush errors. A flush failure surfaces as a plain
    /// `Err` out of the landing, which on its own says nothing about *whose*
    /// work failed — and ejecting the wrong member would park a healthy handler
    /// while leaving the poisoned one running.
    failed: Option<usize>,
}

impl<P> PeerFlush for GroupPeers<'_, P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    fn any_dirty(&self) -> bool {
        self.left.iter().any(|m| m.dirty()) || self.right.iter().any(|m| m.dirty())
    }

    fn flush_before<'b>(
        &'b mut self,
        op: &'b mut es_entity::DbOp<'static>,
    ) -> BoxFuture<'b, Result<(), HandlerError>> {
        Box::pin(async move {
            for (idx, member) in self.left.iter_mut().enumerate() {
                if let Err(error) = member.flush_into(&mut *op).await {
                    self.failed = Some(idx);
                    return Err(error);
                }
            }
            Ok(())
        })
    }

    fn flush_after<'b>(
        &'b mut self,
        op: &'b mut es_entity::DbOp<'static>,
    ) -> BoxFuture<'b, Result<(), HandlerError>> {
        Box::pin(async move {
            let offset = self.right_offset;
            for (idx, member) in self.right.iter_mut().enumerate() {
                if let Err(error) = member.flush_into(&mut *op).await {
                    self.failed = Some(offset + idx);
                    return Err(error);
                }
            }
            Ok(())
        })
    }
}

/// A no-op [`ItemFlush`] for runner-initiated landings, which have no
/// dispatching member and therefore no accumulator of their own — every
/// member's items travel through [`GroupPeers`].
struct NoItems;

impl ItemFlush<()> for NoItems {
    fn flush_items<'a>(
        &'a self,
        _op: &'a mut es_entity::DbOp<'static>,
        _items: (),
    ) -> BoxFuture<'a, Result<(), HandlerError>> {
        Box::pin(std::future::ready(Ok(())))
    }
}

// ── checkpoint ──────────────────────────────────────────────────────────────

/// Every member's checkpoint, written as one execution-state row.
pub(crate) struct GroupCheckpoint {
    keys: Vec<String>,
    seqs: Vec<EventSequence>,
    /// Keys found in the persisted state but absent from the registered
    /// membership — a handler that was temporarily unregistered. Kept verbatim
    /// so re-registering it later resumes from where it stopped rather than
    /// replaying from the beginning.
    retained: BTreeMap<String, EventSequence>,
    /// The batch's upper bound: the last sequence dispatched to anyone.
    batch_end: EventSequence,
}

impl GroupCheckpoint {
    fn sequence_of(&self, idx: usize) -> EventSequence {
        self.seqs[idx]
    }

    /// Advance one member, monotonically: a member that started ahead of the
    /// shared cursor is never dragged backwards.
    fn advance(&mut self, idx: usize, sequence: EventSequence) {
        if sequence > self.seqs[idx] {
            self.seqs[idx] = sequence;
        }
        if sequence > self.batch_end {
            self.batch_end = sequence;
        }
    }

    /// Roll every member back to its last durable position after a failed
    /// landing.
    fn rewind_to(&mut self, committed: &[EventSequence]) {
        self.seqs.copy_from_slice(committed);
        self.batch_end = committed
            .iter()
            .copied()
            .max()
            .unwrap_or(EventSequence::BEGIN);
    }

    fn snapshot(&self) -> Vec<EventSequence> {
        self.seqs.clone()
    }
}

impl CheckpointState for GroupCheckpoint {
    fn sequence(&self) -> EventSequence {
        self.batch_end
    }

    fn persist<'a>(
        &'a self,
        current_job: &'a mut CurrentJob,
        op: &'a mut es_entity::DbOp<'static>,
    ) -> BoxFuture<'a, Result<(), HandlerError>> {
        Box::pin(async move {
            let mut members = self.retained.clone();
            for (key, sequence) in self.keys.iter().zip(self.seqs.iter()) {
                members.insert(key.clone(), *sequence);
            }
            current_job
                .update_execution_state_in_op(op, &HandlerGroupJobState { members })
                .await?;
            Ok(())
        })
    }
}

// ── adoption ────────────────────────────────────────────────────────────────

/// Take over a legacy solo handler job's checkpoint, if one exists.
///
/// Reads the solo job's persisted sequence and removes its execution row in one
/// statement, so a handler that moves from solo to grouped resumes exactly where
/// it left off instead of replaying the outbox from the beginning.
///
/// Deleting the execution row is what retires the legacy job: the job poller
/// only ever fetches job types that have a registered initializer, and a grouped
/// member no longer registers one — so the row is already inert. Removing it
/// keeps a stale checkpoint from being resurrected if the handler is later moved
/// back out of the group. The `jobs` / `job_events` rows are left as history.
///
/// This reaches into the job crate's schema, which obix already vendors
/// (`migrations/20250904065521_job_setup.sql`). It exists only because the job
/// crate has no public way to read the state of a job this process did not
/// spawn and holds no handle to. Given `Jobs::unique_handle(&JobType) ->
/// Option<JobHandle>`, the whole body collapses to
/// `handle.load().await?.execution_state::<OutboxEventJobState>()?`.
#[tracing::instrument(name = "obix.handler_group.adopt_solo_checkpoint", skip_all, fields(member = %member), err)]
async fn adopt_solo_checkpoint(
    pool: &sqlx::PgPool,
    member: &JobType,
) -> Result<Option<EventSequence>, sqlx::Error> {
    let row = sqlx::query!(
        "DELETE FROM job_executions WHERE job_type = $1 RETURNING execution_state_json",
        member.as_str()
    )
    .fetch_optional(pool)
    .await?;

    Ok(row
        .and_then(|row| row.execution_state_json)
        .and_then(|json| serde_json::from_value::<OutboxEventJobState>(json).ok())
        .map(|state| state.sequence))
}

// ── registry ────────────────────────────────────────────────────────────────

/// One group's registered membership, shared between registration and the
/// group job's initializer.
pub(crate) struct GroupEntry<P> {
    pub(crate) members: Vec<Arc<dyn MemberSpec<P>>>,
    pub(crate) max_batch_size: usize,
    pub(crate) checkpoint_interval: std::time::Duration,
    /// Set when the group job snapshots its membership. Registrations after
    /// this point are rejected rather than silently ignored.
    pub(crate) started: bool,
}

pub(crate) type GroupRegistry<P> =
    Arc<std::sync::RwLock<std::collections::HashMap<HandlerGroupName, GroupEntry<P>>>>;

// ── job wiring ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct HandlerGroupJobData {}

pub(crate) struct HandlerGroupJobInitializer<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    outbox: Outbox<P, Tables>,
    registry: GroupRegistry<P>,
    group: HandlerGroupName,
    retry_settings: job::RetrySettings,
}

impl<P, Tables> HandlerGroupJobInitializer<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    pub(crate) fn new(
        outbox: Outbox<P, Tables>,
        registry: GroupRegistry<P>,
        group: HandlerGroupName,
        retry_settings: job::RetrySettings,
    ) -> Self {
        Self {
            outbox,
            registry,
            group,
            retry_settings,
        }
    }
}

impl<P, Tables> JobInitializer for HandlerGroupJobInitializer<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    type Config = HandlerGroupJobData;

    fn job_type(&self) -> JobType {
        self.group.job_type()
    }

    fn retry_on_error_settings(&self) -> job::RetrySettings {
        self.retry_settings.clone()
    }

    fn init(
        &self,
        _job: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        // Snapshot membership: from here on the group is closed, and a late
        // registration is an error rather than a handler that never runs.
        let (specs, max_batch_size, checkpoint_interval) = {
            let mut registry = self.registry.write().expect("group registry poisoned");
            let entry = registry
                .get_mut(&self.group)
                .expect("group entry exists for a spawned group job");
            entry.started = true;
            (
                entry.members.clone(),
                entry.max_batch_size,
                entry.checkpoint_interval,
            )
        };

        Ok(Box::new(HandlerGroupJobRunner::<P, Tables> {
            outbox: self.outbox.clone(),
            group: self.group.clone(),
            specs,
            max_batch_size,
            checkpoint_interval,
        }))
    }
}

/// The mutable state of one group run: the live members and everything that
/// must move as a unit when a batch lands, rolls back, or ejects a member.
///
/// Held apart from the loop's stream handles so the landing/recovery logic can
/// live in methods instead of being repeated at each of the batch's flush
/// triggers.
struct GroupBatch<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    members: Vec<Box<dyn MemberInstance<P>>>,
    /// An ejected member is never dispatched to again; its parked checkpoint is
    /// still written on every landing so a restart resumes it in place.
    ejected: Vec<bool>,
    persistent_member: Vec<bool>,
    ephemeral_member: Vec<bool>,
    checkpoint: GroupCheckpoint,
    /// Every member's last durable position — where a failed landing rewinds to.
    committed: Vec<EventSequence>,
    tracker: BatchTracker,
    op_slot: Option<es_entity::DbOp<'static>>,
}

/// Outcome of a landing that did not commit.
enum LandFailure {
    /// Attributable to one member's flush: eject it and replay the batch.
    Member(usize, HandlerError),
    /// Not attributable to a member (checkpoint write, commit) — the group job
    /// fails and the runner's own retry re-attaches everyone.
    Fatal(HandlerError),
}

impl<P> GroupBatch<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    fn new(members: Vec<Box<dyn MemberInstance<P>>>, checkpoint: GroupCheckpoint) -> Self {
        let persistent_member = members
            .iter()
            .map(|m| m.subscription() != EventSubscription::EphemeralOnly)
            .collect();
        let ephemeral_member = members
            .iter()
            .map(|m| m.subscription() != EventSubscription::PersistentOnly)
            .collect();
        let committed = checkpoint.snapshot();
        let tracker = BatchTracker {
            events_in_op: 0,
            collected: 0,
            persisted_seq: checkpoint.sequence(),
            last_persist: tokio::time::Instant::now(),
        };
        Self {
            ejected: vec![false; members.len()],
            members,
            persistent_member,
            ephemeral_member,
            checkpoint,
            committed,
            tracker,
            op_slot: None,
        }
    }

    fn pending(&self) -> bool {
        self.op_slot.is_some() || self.tracker.collected > 0
    }

    fn all_ejected(&self) -> bool {
        self.ejected.iter().all(|e| *e)
    }

    fn wants_ephemeral(&self) -> bool {
        self.ephemeral_member.iter().any(|w| *w)
    }

    /// Where the shared listener must sit: the minimum checkpoint across the
    /// members still being delivered to.
    fn cursor(&self) -> EventSequence {
        self.checkpoint
            .seqs
            .iter()
            .enumerate()
            .filter(|(idx, _)| !self.ejected[*idx] && self.persistent_member[*idx])
            .map(|(_, sequence)| *sequence)
            .min()
            .unwrap_or_else(|| self.checkpoint.sequence())
    }

    /// Should this member see this event? False once it is already past it —
    /// the per-member delivery filter, which is what lets one shared stream
    /// serve members at different positions.
    fn deliver_to(&self, idx: usize, sequence: EventSequence) -> bool {
        !self.ejected[idx]
            && self.persistent_member[idx]
            && sequence > self.checkpoint.sequence_of(idx)
    }

    /// Discard the pending batch without applying it: the op rolls back with
    /// the drop, accumulators are cleared so the replay re-collects, and every
    /// member returns to its last durable checkpoint.
    fn roll_back(&mut self) {
        self.op_slot = None;
        for member in self.members.iter_mut() {
            member.discard();
        }
        self.tracker.collected = 0;
        self.tracker.events_in_op = 0;
        self.checkpoint.rewind_to(&self.committed);
    }

    /// Land the pending batch: every dirty member's flush in registration
    /// order, one state write carrying every member's checkpoint, then commit —
    /// one transaction.
    async fn land(
        &mut self,
        current_job: &mut CurrentJob,
        reason: &'static str,
    ) -> Result<(), LandFailure> {
        let mut peers = GroupPeers {
            left: &mut self.members,
            right: &mut [],
            right_offset: 0,
            failed: None,
        };
        // Scoped so the borrows of `peers` and of `self`'s fields end before the
        // failure attribution below reads them back.
        let result = {
            let mut parts = CtxParts {
                op_slot: &mut self.op_slot,
                current_job,
                checkpoint: &self.checkpoint,
                tracker: &mut self.tracker,
                peers: &mut peers,
                own_dirty: false,
            };
            flush_batch(&mut parts, &mut (), &NoItems, reason).await
        };
        let failed = peers.failed;

        match result {
            Ok(()) => {
                self.committed = self.checkpoint.snapshot();
                Ok(())
            }
            Err(error) => Err(match failed {
                Some(idx) => LandFailure::Member(idx, error),
                None => LandFailure::Fatal(error),
            }),
        }
    }

    /// Park a member at its last committed sequence and stop delivering to it.
    fn eject(&mut self, group: &HandlerGroupName, idx: usize, error: &HandlerError, stage: &str) {
        self.ejected[idx] = true;
        record_ejection(group, self.members[idx].key(), stage, error);
    }
}

struct HandlerGroupJobRunner<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    outbox: Outbox<P, Tables>,
    group: HandlerGroupName,
    specs: Vec<Arc<dyn MemberSpec<P>>>,
    max_batch_size: usize,
    checkpoint_interval: std::time::Duration,
}

#[async_trait]
impl<P, Tables> JobRunner for HandlerGroupJobRunner<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    async fn run(
        &self,
        current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        if self
            .specs
            .iter()
            .any(|s| s.subscription() != EventSubscription::EphemeralOnly)
        {
            self.run_with_persistent(current_job).await
        } else {
            self.run_ephemeral_only(current_job).await
        }
    }
}

impl<P, Tables> HandlerGroupJobRunner<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    /// Every member is [`EphemeralOnly`](EventSubscription::EphemeralOnly): a
    /// bare fan-out loop, with none of the checkpoint or batch machinery.
    async fn run_ephemeral_only(
        &self,
        mut current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        let mut members: Vec<Box<dyn MemberInstance<P>>> =
            self.specs.iter().map(|spec| spec.instantiate()).collect();
        let mut ephemeral = self.outbox.listen_ephemeral();
        loop {
            tokio::select! {
                biased;
                _ = current_job.shutdown_requested() => {
                    return Ok(JobCompletion::RescheduleNow);
                }
                event = ephemeral.next() => match event {
                    Some(event) => {
                        for member in members.iter_mut() {
                            member
                                .handle_ephemeral(&event)
                                .await
                                .map_err(|e| e as Box<dyn std::error::Error>)?;
                        }
                    }
                    None => return Ok(JobCompletion::RescheduleNow),
                },
            }
        }
    }

    async fn run_with_persistent(
        &self,
        mut current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        let members: Vec<Box<dyn MemberInstance<P>>> =
            self.specs.iter().map(|spec| spec.instantiate()).collect();
        // Keys are lifted out before the first await: a member is only `Send`,
        // so no borrow of the membership may span one.
        let member_keys: Vec<JobType> = members.iter().map(|m| m.key().clone()).collect();
        let checkpoint = self.init_checkpoint(&member_keys, &current_job).await?;
        let mut batch = GroupBatch::new(members, checkpoint);

        let mut persistent = self.outbox.listen_persisted(Some(batch.cursor()));
        let mut ephemeral = batch
            .wants_ephemeral()
            .then(|| self.outbox.listen_ephemeral());

        loop {
            let item = if batch.pending() {
                // A batch is pending: take only already-buffered events, never
                // holding the batch open across the network.
                match persistent.next().now_or_never() {
                    Some(Some(item)) => item,
                    Some(None) => {
                        self.land(
                            &mut batch,
                            &mut current_job,
                            &mut persistent,
                            "stream_closed",
                        )
                        .await?;
                        return Ok(JobCompletion::RescheduleNow);
                    }
                    None => {
                        self.land(
                            &mut batch,
                            &mut current_job,
                            &mut persistent,
                            "backlog_drained",
                        )
                        .await?;
                        continue;
                    }
                }
            } else {
                let next = tokio::select! {
                    biased;
                    _ = current_job.shutdown_requested() => {
                        if batch.tracker.persisted_seq < batch.checkpoint.sequence() {
                            persist_checkpoint(&mut current_job, &batch.checkpoint)
                                .await
                                .map_err(|e| e as Box<dyn std::error::Error>)?;
                        }
                        return Ok(JobCompletion::RescheduleNow);
                    }
                    _ = tokio::time::sleep_until(
                        batch.tracker.last_persist + self.checkpoint_interval,
                    ), if batch.tracker.persisted_seq < batch.checkpoint.sequence() => {
                        persist_checkpoint(&mut current_job, &batch.checkpoint)
                            .await
                            .map_err(|e| e as Box<dyn std::error::Error>)?;
                        batch.tracker.persisted_seq = batch.checkpoint.sequence();
                        batch.tracker.last_persist = tokio::time::Instant::now();
                        batch.committed = batch.checkpoint.snapshot();
                        continue;
                    }
                    next = async {
                        tokio::select! {
                            Some(event) = next_ephemeral(&mut ephemeral) => {
                                Delivery::Ephemeral(event)
                            }
                            event = persistent.next() => Delivery::Persistent(event),
                        }
                    } => next,
                };
                match next {
                    Delivery::Ephemeral(event) => {
                        // Nothing is pending here by construction, so no
                        // transaction spans these foreign awaits.
                        for idx in 0..batch.members.len() {
                            if batch.ejected[idx] || !batch.ephemeral_member[idx] {
                                continue;
                            }
                            if let Err(error) = batch.members[idx].handle_ephemeral(&event).await {
                                batch.eject(&self.group, idx, &error, "ephemeral");
                            }
                        }
                        if batch.all_ejected() {
                            return Err(all_ejected(&self.group));
                        }
                        continue;
                    }
                    Delivery::Persistent(Some(item)) => item,
                    Delivery::Persistent(None) => {
                        if batch.tracker.persisted_seq < batch.checkpoint.sequence() {
                            persist_checkpoint(&mut current_job, &batch.checkpoint)
                                .await
                                .map_err(|e| e as Box<dyn std::error::Error>)?;
                        }
                        return Ok(JobCompletion::RescheduleNow);
                    }
                }
            };

            // An undecodable payload lands the batch first, then each member
            // decides its own fate: `Ok` advances that member past the event,
            // `Err` parks it before the event while its siblings continue.
            let event = match item {
                Ok(event) => event,
                Err(undecodable) => {
                    self.land(
                        &mut batch,
                        &mut current_job,
                        &mut persistent,
                        "undecodable_event",
                    )
                    .await?;
                    for idx in 0..batch.members.len() {
                        if !batch.deliver_to(idx, undecodable.sequence) {
                            continue;
                        }
                        match batch.members[idx].handle_undecodable(&undecodable).await {
                            Ok(()) => batch.checkpoint.advance(idx, undecodable.sequence),
                            Err(error) => batch.eject(&self.group, idx, &error, "undecodable"),
                        }
                    }
                    if batch.all_ejected() {
                        return Err(all_ejected(&self.group));
                    }
                    continue;
                }
            };

            let mut close_batch = false;
            let mut member_failed = false;
            for idx in 0..batch.members.len() {
                if !batch.deliver_to(idx, event.sequence) {
                    continue;
                }

                let (left, rest) = batch.members.split_at_mut(idx);
                let (member, right) = rest.split_first_mut().expect("idx is in bounds");
                let mut peers = GroupPeers {
                    left,
                    right,
                    right_offset: idx + 1,
                    failed: None,
                };
                let own_dirty = member.dirty();
                let parts = CtxParts {
                    op_slot: &mut batch.op_slot,
                    current_job: &mut current_job,
                    checkpoint: &batch.checkpoint,
                    tracker: &mut batch.tracker,
                    peers: &mut peers,
                    own_dirty,
                };

                let outcome = member.dispatch(parts, &event).await;
                // A peer's flush can fail inside this member's `consume_isolated`
                // fence — then the fault is the peer's, not the dispatcher's.
                let culprit = peers.failed.unwrap_or(idx);

                match outcome {
                    Ok(outcome) => {
                        batch.checkpoint.advance(idx, event.sequence);
                        if outcome == Outcome::Commit {
                            close_batch = true;
                        }
                    }
                    Err(error) => {
                        batch.roll_back();
                        batch.eject(&self.group, culprit, &error, "persistent");
                        member_failed = true;
                        break;
                    }
                }
            }

            if member_failed {
                if batch.all_ejected() {
                    return Err(all_ejected(&self.group));
                }
                // Replay the batch from the last durable position, without the
                // member that could not handle it.
                persistent = self.outbox.listen_persisted(Some(batch.cursor()));
                continue;
            }

            if close_batch {
                self.land(&mut batch, &mut current_job, &mut persistent, "commit")
                    .await?;
            } else if batch.tracker.events_in_op >= self.max_batch_size {
                self.land(&mut batch, &mut current_job, &mut persistent, "batch_full")
                    .await?;
            }
        }
    }

    /// Land the batch, ejecting and replaying instead of failing when the
    /// fault belongs to one member's flush.
    async fn land(
        &self,
        batch: &mut GroupBatch<P>,
        current_job: &mut CurrentJob,
        persistent: &mut super::PersistentOutboxListener<P>,
        reason: &'static str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match batch.land(current_job, reason).await {
            Ok(()) => Ok(()),
            Err(LandFailure::Fatal(error)) => Err(error as Box<dyn std::error::Error>),
            Err(LandFailure::Member(idx, error)) => {
                batch.roll_back();
                batch.eject(&self.group, idx, &error, "flush");
                if batch.all_ejected() {
                    return Err(all_ejected(&self.group));
                }
                *persistent = self.outbox.listen_persisted(Some(batch.cursor()));
                Ok(())
            }
        }
    }

    /// Seed each member's checkpoint: a legacy solo job's position if one
    /// exists, else the group's own persisted state, else the beginning.
    async fn init_checkpoint(
        &self,
        member_keys: &[JobType],
        current_job: &CurrentJob,
    ) -> Result<GroupCheckpoint, Box<dyn std::error::Error>> {
        let persisted = current_job
            .execution_state::<HandlerGroupJobState>()?
            .unwrap_or_default();

        let mut keys = Vec::with_capacity(member_keys.len());
        let mut seqs = Vec::with_capacity(member_keys.len());
        for member_key in member_keys {
            let key = member_key.as_str().to_string();
            let sequence = match adopt_solo_checkpoint(current_job.pool(), member_key).await? {
                Some(adopted) => {
                    tracing::info!(
                        group = %self.group,
                        member = %member_key,
                        sequence = %adopted,
                        "adopted solo handler checkpoint into handler group"
                    );
                    adopted
                }
                None => persisted.members.get(&key).copied().unwrap_or_default(),
            };
            keys.push(key);
            seqs.push(sequence);
        }

        // Anything persisted for a key we no longer register is kept as-is.
        let retained = persisted
            .members
            .iter()
            .filter(|(key, _)| !keys.contains(key))
            .map(|(key, sequence)| (key.clone(), *sequence))
            .collect::<BTreeMap<_, _>>();
        for key in retained.keys() {
            tracing::info!(
                group = %self.group,
                member = %key,
                "retaining checkpoint for a member that is not currently registered"
            );
        }

        let batch_end = seqs.iter().copied().max().unwrap_or(EventSequence::BEGIN);
        Ok(GroupCheckpoint {
            keys,
            seqs,
            retained,
            batch_end,
        })
    }
}

fn all_ejected(group: &HandlerGroupName) -> Box<dyn std::error::Error> {
    format!("handler group '{group}': every member ejected").into()
}

#[tracing::instrument(name = "obix.handler_group.member_ejected", level = "error", skip(error), fields(error = %error))]
fn record_ejection(group: &HandlerGroupName, member: &JobType, stage: &str, error: &HandlerError) {}

enum Delivery<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    Persistent(Option<Result<Arc<PersistentOutboxEvent<P>>, UndecodableEventError>>),
    Ephemeral(Arc<EphemeralOutboxEvent<P>>),
}

async fn next_ephemeral<P>(
    listener: &mut Option<EphemeralOutboxListener<P>>,
) -> Option<Arc<EphemeralOutboxEvent<P>>>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    match listener {
        Some(listener) => listener.next().await,
        None => std::future::pending().await,
    }
}
