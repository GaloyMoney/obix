use async_trait::async_trait;
use futures::{FutureExt, StreamExt};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::sync::Arc;

use job::{
    CurrentJob, Job, JobCompletion, JobInitializer, JobRunner, JobSpawner, JobType, RetrySettings,
};

use super::ctx::*;
use super::{Outbox, event::*};
use crate::tables::MailboxTables;

/// Handles the events of one outbox listener job.
///
/// [`handle_persistent`](Self::handle_persistent) receives an [`EventCtx`]
/// and must resolve it into a [`Handled`] token, deciding the transactional
/// fate of every event:
///
/// - [`skip`](EventCtx::skip) — not my event; costs no transaction at all.
///   The checkpoint advances lazily.
/// - [`consume_in_batch`](EventCtx::consume_in_batch) then
///   [`defer`](BatchOp::defer) — coalesce my work with neighboring events
///   into one transaction and one checkpoint write. The batch lands when the
///   ready backlog is drained (a deferred op is never held open waiting on
///   the network), when `max_batch_size` is reached, when a later event
///   commits or isolates, when an ephemeral event arrives, or on shutdown.
///   Requires the work to tolerate whole-batch replay after a mid-batch
///   failure.
/// - [`consume_in_batch`](EventCtx::consume_in_batch) then
///   [`commit`](BatchOp::commit) — join the batch and close it with me.
/// - [`consume_isolated`](EventCtx::consume_isolated) then
///   [`commit`](IsolatedOp::commit) — my event is its own atomic unit: the
///   pending batch (work + checkpoint) lands before my work starts, and my
///   op commits at return. Exact legacy per-event semantics when nothing is
///   pending. Use for causally significant or risky work.
///
/// [`on_flush`](Self::on_flush) is called immediately before *any* batch op
/// commits — the guaranteed write-back point for handlers that accumulate
/// state across [`consume_in_batch`](EventCtx::consume_in_batch) calls.
pub trait OutboxEventHandler<P>: Send + Sync + 'static
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv>,
        event: &PersistentOutboxEvent<P>,
    ) -> impl std::future::Future<
        Output = Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>>,
    > + Send {
        let _ = event;
        async move { Ok(ctx.skip()) }
    }

    fn handle_ephemeral(
        &self,
        event: &EphemeralOutboxEvent<P>,
    ) -> impl std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send
    {
        let _ = event;
        async { Ok(()) }
    }

    /// Called immediately before a batch op commits (any flush trigger,
    /// including a later event's [`consume_isolated`](EventCtx::consume_isolated)
    /// entry). Handlers that accumulate state across deferred events write
    /// it back here — it lands in the same transaction as the batch and its
    /// checkpoint.
    fn on_flush(
        &self,
        op: &mut es_entity::DbOp<'_>,
    ) -> impl std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send
    {
        let _ = op;
        async { Ok(()) }
    }
}

const DEFAULT_MAX_BATCH_SIZE: usize = 100;
const DEFAULT_CHECKPOINT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Clone)]
pub struct OutboxEventJobConfig {
    pub job_type: JobType,
    pub retry_settings: RetrySettings,
    pub max_batch_size: usize,
    pub checkpoint_interval: std::time::Duration,
}

impl OutboxEventJobConfig {
    pub fn new(job_type: JobType) -> Self {
        Self {
            job_type,
            retry_settings: RetrySettings::repeat_indefinitely(),
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
            checkpoint_interval: DEFAULT_CHECKPOINT_INTERVAL,
        }
    }

    pub fn with_retry_settings(mut self, settings: RetrySettings) -> Self {
        self.retry_settings = settings;
        self
    }

    /// Backstop on how many deferred events may share one op before the
    /// runner force-flushes. Bounds the replay window and transaction size
    /// of handlers that always [`defer`](super::BatchOp::defer); handlers
    /// remain the primary size control by exiting with
    /// [`commit`](super::BatchOp::commit).
    pub fn with_max_batch_size(mut self, max_batch_size: usize) -> Self {
        self.max_batch_size = max_batch_size.max(1);
        self
    }

    /// Maximum staleness of the persisted checkpoint over skip-only
    /// stretches (where no transaction happens at all and the pointer only
    /// advances in memory). Never delays event handling — it only bounds how
    /// many harmless no-op replays a crash can cause.
    pub fn with_checkpoint_interval(mut self, interval: std::time::Duration) -> Self {
        self.checkpoint_interval = interval;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(super) struct OutboxEventJobData {}

pub(super) struct OutboxEventJobInitializer<H, P, Tables>
where
    H: OutboxEventHandler<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    outbox: Outbox<P, Tables>,
    handler: Arc<H>,
    job_type: JobType,
    retry_settings: RetrySettings,
    max_batch_size: usize,
    checkpoint_interval: std::time::Duration,
}

impl<H, P, Tables> OutboxEventJobInitializer<H, P, Tables>
where
    H: OutboxEventHandler<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    pub fn new(outbox: Outbox<P, Tables>, handler: H, config: &OutboxEventJobConfig) -> Self {
        Self {
            outbox,
            handler: Arc::new(handler),
            job_type: config.job_type.clone(),
            retry_settings: config.retry_settings.clone(),
            max_batch_size: config.max_batch_size,
            checkpoint_interval: config.checkpoint_interval,
        }
    }
}

impl<H, P, Tables> JobInitializer for OutboxEventJobInitializer<H, P, Tables>
where
    H: OutboxEventHandler<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    type Config = OutboxEventJobData;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn retry_on_error_settings(&self) -> RetrySettings {
        self.retry_settings.clone()
    }

    fn init(
        &self,
        _job: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(OutboxEventJobRunner::<H, P, Tables> {
            outbox: self.outbox.clone(),
            handler: self.handler.clone(),
            max_batch_size: self.max_batch_size,
            checkpoint_interval: self.checkpoint_interval,
        }))
    }
}

struct OutboxEventJobRunner<H, P, Tables>
where
    H: OutboxEventHandler<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    outbox: Outbox<P, Tables>,
    handler: Arc<H>,
    max_batch_size: usize,
    checkpoint_interval: std::time::Duration,
}

#[async_trait]
impl<H, P, Tables> JobRunner for OutboxEventJobRunner<H, P, Tables>
where
    H: OutboxEventHandler<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    async fn run(
        &self,
        mut current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        let mut state = current_job
            .execution_state::<OutboxEventJobState>()?
            .unwrap_or_default();

        let mut stream = self.outbox.listen_all(Some(state.sequence));

        let mut op_slot: Option<es_entity::DbOp<'static>> = None;
        let mut tracker = BatchTracker {
            events_in_op: 0,
            persisted_seq: state.sequence,
            last_persist: tokio::time::Instant::now(),
        };

        let on_flush: FlushHook = {
            let handler = self.handler.clone();
            Box::new(move |op| {
                let handler = handler.clone();
                Box::pin(async move { handler.on_flush(op).await })
            })
        };

        loop {
            let event = if op_slot.is_some() {
                // A batch op is open: only take events that are already
                // buffered — the transaction is never held open waiting on
                // the network. A pending stream is itself the flush trigger.
                match stream.next().now_or_never() {
                    Some(Some(event)) => event,
                    Some(None) => {
                        let mut parts = CtxParts {
                            op_slot: &mut op_slot,
                            current_job: &mut current_job,
                            state: &state,
                            tracker: &mut tracker,
                            on_flush: &on_flush,
                        };
                        flush_batch(&mut parts, "stream_closed")
                            .await
                            .map_err(|e| e as Box<dyn std::error::Error>)?;
                        return Ok(JobCompletion::RescheduleNow);
                    }
                    None => {
                        let mut parts = CtxParts {
                            op_slot: &mut op_slot,
                            current_job: &mut current_job,
                            state: &state,
                            tracker: &mut tracker,
                            on_flush: &on_flush,
                        };
                        flush_batch(&mut parts, "backlog_drained")
                            .await
                            .map_err(|e| e as Box<dyn std::error::Error>)?;
                        continue;
                    }
                }
            } else {
                tokio::select! {
                    biased;
                    _ = current_job.shutdown_requested() => {
                        if tracker.persisted_seq < state.sequence {
                            persist_checkpoint(&mut current_job, &state)
                                .await
                                .map_err(|e| e as Box<dyn std::error::Error>)?;
                        }
                        return Ok(JobCompletion::RescheduleNow);
                    }
                    _ = tokio::time::sleep_until(tracker.last_persist + self.checkpoint_interval),
                        if tracker.persisted_seq < state.sequence => {
                        persist_checkpoint(&mut current_job, &state)
                            .await
                            .map_err(|e| e as Box<dyn std::error::Error>)?;
                        tracker.persisted_seq = state.sequence;
                        tracker.last_persist = tokio::time::Instant::now();
                        continue;
                    }
                    event = stream.next() => match event {
                        Some(event) => event,
                        None => {
                            if tracker.persisted_seq < state.sequence {
                                persist_checkpoint(&mut current_job, &state)
                                    .await
                                    .map_err(|e| e as Box<dyn std::error::Error>)?;
                            }
                            return Ok(JobCompletion::RescheduleNow);
                        }
                    },
                }
            };

            match event {
                OutboxEvent::Ephemeral(event) => {
                    // Ephemerals are an unordered best-effort broadcast and
                    // never touch the batch op — but an open op must not
                    // span the foreign `handle_ephemeral` await (and steady
                    // ephemeral traffic must not starve the flush), so an
                    // ephemeral arriving mid-batch lands the batch first.
                    if op_slot.is_some() {
                        let mut parts = CtxParts {
                            op_slot: &mut op_slot,
                            current_job: &mut current_job,
                            state: &state,
                            tracker: &mut tracker,
                            on_flush: &on_flush,
                        };
                        flush_batch(&mut parts, "ephemeral")
                            .await
                            .map_err(|e| e as Box<dyn std::error::Error>)?;
                    }
                    self.handler
                        .handle_ephemeral(&event)
                        .await
                        .map_err(|e| e as Box<dyn std::error::Error>)?;
                }
                OutboxEvent::Persistent(event) => {
                    let ctx = EventCtx {
                        parts: CtxParts {
                            op_slot: &mut op_slot,
                            current_job: &mut current_job,
                            state: &state,
                            tracker: &mut tracker,
                            on_flush: &on_flush,
                        },
                    };
                    // The Handled token is branded with the invocation
                    // lifetime (it cannot leave this call) and every path
                    // that mints one consumes the ctx — so the outcome is
                    // authentic by construction. Extract it in the same
                    // statement so the token (and with it the ctx borrows)
                    // ends before the state advance below.
                    let outcome = self
                        .handler
                        .handle_persistent(ctx, &event)
                        .await
                        .map_err(|e| e as Box<dyn std::error::Error>)?
                        .outcome;
                    state.sequence = event.sequence;
                    match outcome {
                        Outcome::Skip => {}
                        Outcome::Commit => {
                            let mut parts = CtxParts {
                                op_slot: &mut op_slot,
                                current_job: &mut current_job,
                                state: &state,
                                tracker: &mut tracker,
                                on_flush: &on_flush,
                            };
                            flush_batch(&mut parts, "commit")
                                .await
                                .map_err(|e| e as Box<dyn std::error::Error>)?;
                        }
                        Outcome::Defer => {
                            if tracker.events_in_op >= self.max_batch_size {
                                let mut parts = CtxParts {
                                    op_slot: &mut op_slot,
                                    current_job: &mut current_job,
                                    state: &state,
                                    tracker: &mut tracker,
                                    on_flush: &on_flush,
                                };
                                flush_batch(&mut parts, "batch_full")
                                    .await
                                    .map_err(|e| e as Box<dyn std::error::Error>)?;
                            }
                        }
                    }
                }
            }
        }
    }
}
