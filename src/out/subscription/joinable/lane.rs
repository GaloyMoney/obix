//! The catch-up lane's runner: one `job` execution per live lane.
//!
//! A lane consumes the stream from its birth position through the same handler
//! as the tip, and ends in one join landing under the tip's lock. Unlike a
//! keyed member it never passivates and never pauses, and it attempts to
//! join whenever it has caught up with what is ready, including on skips
//! alone.

use es_entity::errlanes::ResultExt;
use futures::{FutureExt, StreamExt};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{marker::PhantomData, sync::Arc, sync::Mutex};

use job::{CurrentJob, Job, JobType, RetrySettings};

use super::{JoinableConfig, JoinableSubscriber, Scope};
use crate::out::Outbox;
use crate::out::ctx::*;
use crate::out::error::{CouldNotDecodeStored as Undecodable, JoinableFault};
use crate::out::event::EventDelivery;
use crate::out::lane::InsertOrder;
use crate::out::persistent::PersistentOutboxListener;
use crate::out::subscription::StreamPosition;
use crate::sequence::EventSequence;
use crate::tables::MailboxTables;

/// The job crate's keyed-job `Config` for a lane: the lane id, as a string.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(in crate::out) struct LaneMsg {
    pub(super) lane: String,
}

/// One lane's flush bridge and checkpoint mirror in a single struct, so the
/// join sequence the loop sets before the join landing is the one both the
/// flush (as [`Scope`]) and the retirement (as the join record) read.
struct LaneRun<S, P, Tables>
where
    S: JoinableSubscriber<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    subscriber: Arc<S>,
    unit: S::Unit,
    tip_type: JobType,
    lane_type: JobType,
    key: String,
    join: Mutex<Option<EventSequence>>,
    _marker: PhantomData<fn() -> (P, Tables)>,
}

impl<S, P, Tables> LaneRun<S, P, Tables>
where
    S: JoinableSubscriber<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    fn join(&self) -> Option<EventSequence> {
        *self.join.lock().expect("lane join lock poisoned")
    }

    fn set_join(&self, join: Option<EventSequence>) {
        *self.join.lock().expect("lane join lock poisoned") = join;
    }

    /// The scope of the landing in progress: [`Scope::Join`] from the moment
    /// the join landing has fixed its sequence until it has landed, and
    /// [`Scope::CatchUp`] otherwise.
    fn scope(&self) -> Scope<'_, S::Unit> {
        match self.join() {
            Some(join) => Scope::Join {
                unit: &self.unit,
                join,
            },
            None => Scope::CatchUp { unit: &self.unit },
        }
    }
}

impl<S, P, Tables> ItemFlush<S::Batch, FlushOutcome> for LaneRun<S, P, Tables>
where
    S: JoinableSubscriber<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    fn flush_items<'a>(
        &'a self,
        op: &'a mut es_entity::DbOp<'static>,
        items: S::Batch,
        state: &'a OutboxEventJobState,
    ) -> BoxFuture<'a, Result<FlushOutcome, HandlerError>> {
        Box::pin(async move {
            let scope = self.scope();
            let mut op = FlushOp::<InsertOrder>::new(op, state.sequence);
            self.subscriber.flush(&mut op, scope, items).await?;
            Ok(match scope {
                Scope::Join { .. } => FlushOutcome::Finish,
                Scope::CatchUp { .. } | Scope::Tip => FlushOutcome::Continue,
            })
        })
    }

    fn position_of(&self, state: &OutboxEventJobState) -> StreamPosition {
        StreamPosition::Insert(state.sequence)
    }
}

impl<S, P, Tables> CheckpointMirror for LaneRun<S, P, Tables>
where
    S: JoinableSubscriber<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    fn mirror<'a>(
        &'a self,
        op: &'a mut es_entity::DbOp<'static>,
        checkpoint: EventSequence,
    ) -> futures::future::BoxFuture<'a, Result<(), sqlx::Error>> {
        Box::pin(async move {
            Tables::update_subscription_checkpoint_in_op(
                op,
                self.lane_type.as_str(),
                &self.key,
                checkpoint,
            )
            .await
        })
    }

    fn retire<'a>(
        &'a self,
        op: &'a mut es_entity::DbOp<'static>,
    ) -> futures::future::BoxFuture<'a, Result<(), sqlx::Error>> {
        Box::pin(async move {
            Tables::retire_lane_in_op(
                op,
                self.tip_type.as_str(),
                self.lane_type.as_str(),
                &self.key,
                self.join(),
            )
            .await
        })
    }
}

pub(in crate::out) struct LaneJobInitializer<S, P, Tables>
where
    S: JoinableSubscriber<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    outbox: Outbox<P, Tables>,
    subscriber: Arc<S>,
    tip_type: JobType,
    lane_type: JobType,
    retry_settings: RetrySettings,
    max_batch_size: usize,
    max_join_gap: u64,
    checkpoint_interval: std::time::Duration,
    max_concurrent_per_process: Option<usize>,
}

impl<S, P, Tables> LaneJobInitializer<S, P, Tables>
where
    S: JoinableSubscriber<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    pub(in crate::out) fn new(
        outbox: Outbox<P, Tables>,
        subscriber: Arc<S>,
        tip_type: JobType,
        lane_type: JobType,
        config: &JoinableConfig,
    ) -> Self {
        Self {
            outbox,
            subscriber,
            tip_type,
            lane_type,
            retry_settings: config.lane_retry_settings.clone(),
            max_batch_size: config.lane_max_batch_size,
            max_join_gap: config.max_join_gap,
            checkpoint_interval: config.tip.checkpoint_interval,
            max_concurrent_per_process: config.max_concurrent_lanes_per_process,
        }
    }
}

impl<S, P, Tables> job::KeyedJobInitializer for LaneJobInitializer<S, P, Tables>
where
    S: JoinableSubscriber<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    type Config = LaneMsg;

    fn job_type(&self) -> JobType {
        self.lane_type.clone()
    }

    fn retry_on_error_settings(&self) -> RetrySettings {
        self.retry_settings.clone()
    }

    fn max_concurrent_per_process(&self) -> Option<usize> {
        self.max_concurrent_per_process
    }

    /// A respawn resumes where the last generation left off, and a joined
    /// lane's final execution state stays readable.
    fn inherits_state(&self) -> bool {
        true
    }

    fn init(
        &self,
        job: &Job,
        _spawner: job::KeyedJobSpawner<Self::Config>,
    ) -> Result<Box<dyn job::JobRunner>, Box<dyn std::error::Error + Send + Sync>> {
        let LaneMsg { lane } = job.config()?;
        Ok(Box::new(LaneJobRunner::<S, P, Tables> {
            outbox: self.outbox.clone(),
            subscriber: self.subscriber.clone(),
            tip_type: self.tip_type.clone(),
            lane_type: self.lane_type.clone(),
            key: lane,
            max_batch_size: self.max_batch_size,
            max_join_gap: self.max_join_gap,
            checkpoint_interval: self.checkpoint_interval,
        }))
    }
}

struct LaneJobRunner<S, P, Tables>
where
    S: JoinableSubscriber<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    outbox: Outbox<P, Tables>,
    subscriber: Arc<S>,
    tip_type: JobType,
    lane_type: JobType,
    key: String,
    max_batch_size: usize,
    max_join_gap: u64,
    checkpoint_interval: std::time::Duration,
}

/// The lane loop's mutable state, together, so the join landing can take it
/// as one `&mut self`.
struct LaneLoop<'r, S, P, Tables>
where
    S: JoinableSubscriber<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    current_job: CurrentJob,
    state: OutboxEventJobState,
    op_slot: Option<es_entity::DbOp<'static>>,
    tracker: BatchTracker,
    batch: S::Batch,
    persistent: PersistentOutboxListener<P>,
    run: &'r LaneRun<S, P, Tables>,
}

impl<S, P, Tables> LaneLoop<'_, S, P, Tables>
where
    S: JoinableSubscriber<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    fn checkpoint_dirty(&self) -> bool {
        self.tracker.persisted < StreamPosition::Insert(self.state.sequence)
    }

    async fn persist(&mut self) -> Result<(), crate::error::ObixFault> {
        persist_checkpoint(&mut self.current_job, &self.state, Some(self.run)).await
    }

    /// An ordinary landing: the pending batch and the checkpoint, with no
    /// join.
    async fn land_ordinary(&mut self, reason: &'static str) -> Result<Landed, HandlerError> {
        let mut parts = CtxParts {
            op_slot: &mut self.op_slot,
            current_job: &mut self.current_job,
            state: &mut self.state,
            tracker: &mut self.tracker,
            landing: Keyed(self.run),
        };
        flush_batch(&mut parts, &mut self.batch, self.run, reason).await
    }

    /// Resolve one persistent event through the handler; `Skip` and `Collect`
    /// are the only outcomes its ctx can mint.
    async fn handle(
        &mut self,
        event: &EventDelivery<P>,
    ) -> Result<Outcome, Box<dyn std::error::Error + Send + Sync>> {
        let ctx = JoinableEventCtx {
            batch: &mut self.batch,
            tracker: &mut self.tracker,
        };
        let outcome = self
            .run
            .subscriber
            .handle(ctx, self.run.scope(), event)
            .await?
            .outcome;
        self.state.sequence = event.sequence;
        Ok(outcome)
    }

    /// Land the pending batch, as the join landing when the lane has reached
    /// the tip and as an ordinary one otherwise.
    ///
    /// The tip row's lock is the first statement on the landing op, taken
    /// before anything else is read or written on it; there is no pool read
    /// once it is open, and nothing in here is a cancellation point.
    ///
    /// The lane's own row is locked second. A cancel that committed while this
    /// landing waited for the tip's lock is seen here, and the landing ends
    /// the lane without flushing: nothing of a cancelled lane lands, and no
    /// join is claimed. A cancel that arrives later waits for this op to
    /// commit and then finds the row already retired by the join.
    async fn try_land(
        &mut self,
        outbox: &Outbox<P, Tables>,
        tip_type: &JobType,
        max_join_gap: u64,
        reason: &'static str,
    ) -> Result<Landed, HandlerError> {
        let position = self.state.sequence;
        let tip_before = Tables::tip_checkpoint(self.current_job.pool(), tip_type.as_str()).await?;
        if tip_before.is_none_or(|tip| position < tip) {
            return self.land_ordinary(reason).await;
        }

        let mut op = open_landing_op(&self.current_job, Keyed(self.run)).await?;
        let tip = Tables::lock_tip_in_op(&mut op, tip_type.as_str()).await?;
        let live =
            Tables::lock_subscription_in_op(&mut op, self.run.lane_type.as_str(), &self.run.key)
                .await?;
        if !live {
            return Ok(Landed::Finished(op));
        }
        self.op_slot = Some(op);
        let Some(tip) = tip else {
            return self.land_ordinary(reason).await;
        };
        let gap = u64::from(tip).saturating_sub(u64::from(position));
        if gap > max_join_gap || tip > outbox.persistent_cache_handle().head() {
            return self.land_ordinary(reason).await;
        }

        // The join is fixed before the drain, so the events the drain handles
        // are handled under `Scope::Join` — the scope their flush carries.
        self.run.set_join(Some(position.max(tip)));
        let landed = self.land_joining(tip).await;
        self.run.set_join(None);
        landed
    }

    /// The join landing proper, with the tip's lock held and the join fixed:
    /// drain the lane's own stream to `tip`, then land everything with the
    /// join. Every outcome resets the join in [`try_land`](Self::try_land).
    async fn land_joining(&mut self, tip: EventSequence) -> Result<Landed, HandlerError> {
        while self.state.sequence < tip {
            let Some(item) = self.persistent.next().await else {
                Err::<(), _>(JoinableFault::DrainEnded {
                    at: self.state.sequence,
                    through: tip,
                })
                .into_fault()?;
                unreachable!("the drain error was returned above");
            };
            match item {
                Ok(event) => match self.handle(&event).await? {
                    Outcome::Skip | Outcome::Collect => {}
                    Outcome::Commit | Outcome::Pause(_) | Outcome::CommitAndPause(_) => {
                        unreachable!(
                            "only skip and collect_with can be minted from a JoinableEventCtx"
                        )
                    }
                },
                Err(undecodable) => {
                    self.run.subscriber.handle_undecodable(&undecodable).await?;
                    self.state.sequence = undecodable.sequence;
                }
            }
        }

        let mut parts = CtxParts {
            op_slot: &mut self.op_slot,
            current_job: &mut self.current_job,
            state: &mut self.state,
            tracker: &mut self.tracker,
            landing: Keyed(self.run),
        };
        flush_joining(&mut parts, &mut self.batch, self.run, "join").await
    }
}

#[async_trait::async_trait]
impl<S, P, Tables> job::JobRunner for LaneJobRunner<S, P, Tables>
where
    S: JoinableSubscriber<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    async fn run(
        &self,
        current_job: CurrentJob,
    ) -> Result<job::JobCompletion, Box<dyn std::error::Error + Send + Sync>> {
        let Some(row) =
            Tables::find_subscription(current_job.pool(), self.lane_type.as_str(), &self.key)
                .await?
        else {
            return Ok(job::JobCompletion::Complete);
        };
        let unit: S::Unit = serde_json::from_value(row.instance_config)
            .map_err(Undecodable::InstanceConfig)
            .into_fault()?;
        let state = decode_execution_state(&current_job)?.unwrap_or(OutboxEventJobState {
            sequence: row.start_after,
            commit_sequence: None,
            paused: None,
        });

        let run = LaneRun::<S, P, Tables> {
            subscriber: self.subscriber.clone(),
            unit,
            tip_type: self.tip_type.clone(),
            lane_type: self.lane_type.clone(),
            key: self.key.clone(),
            join: Mutex::new(None),
            _marker: PhantomData,
        };
        let persistent = self.outbox.listen_persisted(Some(state.sequence));
        let tracker = BatchTracker {
            collected: 0,
            persisted: StreamPosition::Insert(state.sequence),
            last_persist: tokio::time::Instant::now(),
        };
        let mut lane = LaneLoop {
            current_job,
            state,
            op_slot: None,
            tracker,
            batch: S::Batch::default(),
            persistent,
            run: &run,
        };
        let mut attempt_pending = true;

        loop {
            if lane.tracker.collected == 0 {
                if lane
                    .current_job
                    .shutdown_requested()
                    .now_or_never()
                    .is_some()
                {
                    if lane.checkpoint_dirty() {
                        lane.persist().await?;
                    }
                    return Ok(job::JobCompletion::RescheduleNow);
                }
                if lane.checkpoint_dirty()
                    && tokio::time::Instant::now()
                        >= lane.tracker.last_persist + self.checkpoint_interval
                {
                    lane.persist().await?;
                    lane.tracker.persisted = StreamPosition::Insert(lane.state.sequence);
                    lane.tracker.last_persist = tokio::time::Instant::now();
                }
            }

            let item = if lane.tracker.collected > 0 || attempt_pending {
                match lane.persistent.next().now_or_never() {
                    Some(Some(item)) => item,
                    Some(None) => {
                        let landed = lane.land_ordinary("stream_closed").await?;
                        if let Landed::Finished(op) = landed {
                            return Ok(job::JobCompletion::CompleteWithOp(op));
                        }
                        if lane.checkpoint_dirty() {
                            lane.persist().await?;
                        }
                        return Ok(job::JobCompletion::RescheduleNow);
                    }
                    None => {
                        attempt_pending = false;
                        let reason = if lane.tracker.collected > 0 {
                            "backlog_drained"
                        } else {
                            "caught_up"
                        };
                        let landed = lane
                            .try_land(&self.outbox, &self.tip_type, self.max_join_gap, reason)
                            .await?;
                        if let Landed::Finished(op) = landed {
                            return Ok(job::JobCompletion::CompleteWithOp(op));
                        }
                        continue;
                    }
                }
            } else {
                tokio::select! {
                    biased;
                    _ = lane.current_job.shutdown_requested() => {
                        if lane.checkpoint_dirty() {
                            lane.persist().await?;
                        }
                        return Ok(job::JobCompletion::RescheduleNow);
                    }
                    _ = tokio::time::sleep_until(lane.tracker.last_persist + self.checkpoint_interval),
                        if lane.checkpoint_dirty() => {
                        lane.persist().await?;
                        lane.tracker.persisted = StreamPosition::Insert(lane.state.sequence);
                        lane.tracker.last_persist = tokio::time::Instant::now();
                        continue;
                    }
                    next = lane.persistent.next() => match next {
                        Some(item) => item,
                        None => {
                            if lane.checkpoint_dirty() {
                                lane.persist().await?;
                            }
                            return Ok(job::JobCompletion::RescheduleNow);
                        }
                    }
                }
            };

            let event = match item {
                Ok(event) => event,
                Err(undecodable) => {
                    let landed = lane.land_ordinary("undecodable_event").await?;
                    if let Landed::Finished(op) = landed {
                        return Ok(job::JobCompletion::CompleteWithOp(op));
                    }
                    match self.subscriber.handle_undecodable(&undecodable).await {
                        Ok(()) => {
                            lane.state.sequence = undecodable.sequence;
                            attempt_pending = true;
                            continue;
                        }
                        Err(error) => {
                            if lane.checkpoint_dirty() {
                                lane.persist().await?;
                            }
                            return Err(error);
                        }
                    }
                }
            };

            let outcome = lane.handle(&event).await?;
            attempt_pending = true;
            match outcome {
                Outcome::Skip => {}
                Outcome::Collect => {
                    if lane.tracker.collected >= self.max_batch_size {
                        let landed = lane
                            .try_land(
                                &self.outbox,
                                &self.tip_type,
                                self.max_join_gap,
                                "batch_full",
                            )
                            .await?;
                        if let Landed::Finished(op) = landed {
                            return Ok(job::JobCompletion::CompleteWithOp(op));
                        }
                    }
                }
                Outcome::Commit | Outcome::Pause(_) | Outcome::CommitAndPause(_) => {
                    unreachable!("only skip and collect_with can be minted from a JoinableEventCtx")
                }
            }
        }
    }
}
