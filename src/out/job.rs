use async_trait::async_trait;
use futures::{FutureExt, StreamExt};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::sync::Arc;

use job::{
    CurrentJob, Job, JobCompletion, JobInitializer, JobRunner, JobSpawner, JobType, RetrySettings,
};

use super::ctx::*;
use super::{EphemeralOutboxListener, Outbox, event::*};
use crate::{sequence::EventSequence, tables::MailboxTables};

/// Which delivery streams an event-handler job subscribes to — see
/// [`OutboxEventHandler::SUBSCRIPTION`].
///
/// - [`PersistentOnly`](Self::PersistentOnly): only the durable, checkpointed
///   stream. The ephemeral stream is never subscribed, so batching is
///   entirely immune to ephemeral traffic.
/// - [`EphemeralOnly`](Self::EphemeralOnly): only the best-effort broadcast.
///   No persistent deliveries, and none of the checkpoint/batch machinery —
///   the job never reads or writes execution state.
/// - [`All`](Self::All): both streams, raced fairly between batches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventSubscription {
    All,
    PersistentOnly,
    EphemeralOnly,
}

/// The error the default
/// [`handle_undecodable`](OutboxEventHandler::handle_undecodable) fails
/// with: a persistent event's stored payload could not be decoded into the
/// handler's event type. Propagates as the handler job's error; the job's
/// checkpoint is parked at the sequence *before* the event, so every retry
/// re-reads it and nothing is ever skipped.
#[derive(Debug, thiserror::Error)]
#[error(
    "undecodable persistent outbox event {id} at sequence {sequence} (checkpoint parked before it): {error}"
)]
pub struct UndecodableEventError {
    pub id: OutboxEventId,
    pub sequence: EventSequence,
    /// The serde error message.
    pub error: String,
}

/// Handles the events of one outbox listener job.
///
/// [`handle_persistent`](Self::handle_persistent) receives an [`EventCtx`]
/// and must resolve it into a [`Handled`] token, deciding the transactional
/// fate of every event:
///
/// - [`skip`](EventCtx::skip) — not my event; costs no transaction at all.
///   The checkpoint advances lazily.
/// - [`collect_with`](EventCtx::collect_with) / the
///   [`collect`](EventCtx::collect) sugar — contribute an item to the
///   pending batch's [`Batch`](Self::Batch) accumulator; a pure memory
///   write. The runner applies the whole accumulator via
///   [`flush`](Self::flush) exactly once per batch landing, inside the
///   transaction that commits the checkpoint — N per-event statements
///   become one batched flush. Same replay contract as `defer`.
/// - [`consume_in_batch`](EventCtx::consume_in_batch) then
///   [`defer`](BatchOp::defer) — coalesce my work with neighboring events
///   into one transaction and one checkpoint write. The batch lands when the
///   ready persistent backlog is drained (a pending batch is never held open
///   waiting on the network), when `max_batch_size` is reached, when a later
///   event commits or isolates, or on shutdown. Requires the work to
///   tolerate whole-batch replay after a mid-batch failure.
/// - [`consume_in_batch`](EventCtx::consume_in_batch) then
///   [`commit`](BatchOp::commit) — join the batch and close it with me.
/// - [`consume_isolated`](EventCtx::consume_isolated) then
///   [`commit`](IsolatedOp::commit) — my event is its own atomic unit: the
///   pending batch (items + work + checkpoint) lands before my work starts,
///   and my op commits at return. Exact legacy per-event semantics when
///   nothing is pending. Use for causally significant or risky work.
///
/// Ephemeral events are delivered on their own stream and are handled
/// between batches: they never interrupt a pending batch, and nothing is
/// pending while [`handle_ephemeral`](Self::handle_ephemeral) runs. Most
/// handlers only consume one of the two streams — declare it via
/// [`SUBSCRIPTION`](Self::SUBSCRIPTION) and the other stream is never even
/// subscribed.
pub trait OutboxEventHandler<P>: Send + Sync + 'static
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    /// Which delivery streams this handler's job subscribes to. Defaults to
    /// [`All`](EventSubscription::All).
    ///
    /// Declaring a single-stream mode is a contract, not a filter: the other
    /// stream is never subscribed, so its handler method is never called —
    /// overriding [`handle_ephemeral`](Self::handle_ephemeral) on a
    /// [`PersistentOnly`](EventSubscription::PersistentOnly) handler (or
    /// [`handle_persistent`](Self::handle_persistent) on an
    /// [`EphemeralOnly`](EventSubscription::EphemeralOnly) one) is dead code.
    const SUBSCRIPTION: EventSubscription = EventSubscription::All;

    /// Accumulator for events resolved via
    /// [`collect_with`](EventCtx::collect_with) — `Vec<T>` for append-style
    /// batching, `HashMap<K, V>` for keyed coalescing folds, or any other
    /// `Default` container. Handlers that never collect use `()`.
    type Batch: Default + Send + 'static;

    fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, Self::Batch>,
        event: &PersistentOutboxEvent<P>,
    ) -> impl std::future::Future<
        Output = Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>>,
    > + Send {
        let _ = event;
        async move { Ok(ctx.skip()) }
    }

    /// Handle a persistent event whose stored payload could not be decoded
    /// into `P` (the event arrives with
    /// [`decode_failure`](PersistentOutboxEvent::decode_failure) set and no
    /// payload). The runner invokes this *instead of*
    /// [`handle_persistent`](Self::handle_persistent) — an undecodable event
    /// never reaches the normal handling path.
    ///
    /// The default fails with [`UndecodableEventError`]: a payload the
    /// consumer cannot decode is a runtime bug (schema drift, a producer
    /// ahead of this consumer, foreign rows in the table) that must surface
    /// loudly, not silently pass by. On any `Err` the runner lands the
    /// pending batch, parks the checkpoint at the sequence *before* the
    /// event and fails the job — every retry re-reads the event from the
    /// database, so deploying a consumer that understands the payload
    /// resumes the pipeline automatically, in order, with nothing skipped.
    ///
    /// Override and return `Ok(())` only when this handler genuinely wants
    /// to move past payloads it cannot decode — an explicit, auditable
    /// decision (consider recording `event.decode_failure` somewhere
    /// durable first). The checkpoint then advances over the event exactly
    /// as for a [`skip`](EventCtx::skip). No transaction is opened on this
    /// path; like [`handle_ephemeral`](Self::handle_ephemeral), any work
    /// done here must tolerate replay.
    fn handle_undecodable(
        &self,
        event: &PersistentOutboxEvent<P>,
    ) -> impl std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send
    {
        let error = UndecodableEventError {
            id: event.id,
            sequence: event.sequence,
            error: event
                .decode_failure
                .as_ref()
                .map(|failure| failure.error.clone())
                .unwrap_or_default(),
        };
        async move { Err(error.into()) }
    }

    /// Apply everything collected since the last flush. Called at most once
    /// per batch landing, inside the batch transaction, before the
    /// checkpoint commits — items and pointer land atomically.
    ///
    /// `op` is the batch op behind a restricted [`FlushOp`] view: execute
    /// statements or register commit hooks on it for work that must share
    /// the checkpoint's fate; ignore it when flushing to a foreign database
    /// (then make the writes idempotent — the checkpoint only advances after
    /// `Ok`, and a failure replays and re-collects the whole batch).
    fn flush(
        &self,
        op: &mut FlushOp<'_>,
        items: Self::Batch,
    ) -> impl std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send
    {
        let _ = (op, items);
        async { Ok(()) }
    }

    fn handle_ephemeral(
        &self,
        event: &EphemeralOutboxEvent<P>,
    ) -> impl std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send
    {
        let _ = event;
        async { Ok(()) }
    }
}

/// Object-safe bridge handing the handler's typed [`flush`] to the runner's
/// flush path (and to [`EventCtx::consume_isolated`]'s entry fence) with the
/// handler type erased.
///
/// [`flush`]: OutboxEventHandler::flush
struct HandlerFlusher<H, P> {
    handler: Arc<H>,
    _payload: std::marker::PhantomData<fn() -> P>,
}

impl<H, P> ItemFlush<H::Batch> for HandlerFlusher<H, P>
where
    H: OutboxEventHandler<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    fn flush_items<'a>(
        &'a self,
        op: &'a mut es_entity::DbOp<'static>,
        items: H::Batch,
    ) -> BoxFuture<'a, Result<(), HandlerError>> {
        Box::pin(async move {
            let mut op = FlushOp::new(op);
            self.handler.flush(&mut op, items).await
        })
    }
}

/// What the fair two-stream race yielded while no batch was pending.
enum NextDelivery<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    Persistent(Option<Arc<PersistentOutboxEvent<P>>>),
    Ephemeral(Arc<EphemeralOutboxEvent<P>>),
}

/// Next ephemeral event — or pend forever when the handler's
/// [`SUBSCRIPTION`](OutboxEventHandler::SUBSCRIPTION) never subscribed the
/// ephemeral stream.
async fn next_if_subscribed<P>(
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

    /// Backstop on how many deferred or collected events may share one
    /// batch before the runner force-flushes. Bounds the replay window, the
    /// transaction size and the flushed accumulator size of handlers that
    /// always [`defer`](super::BatchOp::defer) or
    /// [`collect`](super::EventCtx::collect_with); handlers remain the
    /// primary size control by exiting with
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
        current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        match H::SUBSCRIPTION {
            EventSubscription::EphemeralOnly => self.run_ephemeral_only(current_job).await,
            EventSubscription::All | EventSubscription::PersistentOnly => {
                self.run_with_persistent(current_job).await
            }
        }
    }
}

impl<H, P, Tables> OutboxEventJobRunner<H, P, Tables>
where
    H: OutboxEventHandler<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    /// [`EphemeralOnly`](EventSubscription::EphemeralOnly): a bare dispatch
    /// loop — no persistent subscription, no execution state, no batch or
    /// checkpoint machinery.
    async fn run_ephemeral_only(
        &self,
        mut current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        let mut ephemeral = self.outbox.listen_ephemeral();
        loop {
            tokio::select! {
                biased;
                _ = current_job.shutdown_requested() => {
                    return Ok(JobCompletion::RescheduleNow);
                }
                event = ephemeral.next() => match event {
                    Some(event) => {
                        self.handler
                            .handle_ephemeral(&event)
                            .await
                            .map_err(|e| e as Box<dyn std::error::Error>)?;
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
        let mut state = current_job
            .execution_state::<OutboxEventJobState>()?
            .unwrap_or_default();

        // Two independent streams: the persistent backlog alone governs the
        // batch lifecycle, so ephemeral traffic can never shrink a batch —
        // and ephemerals are only handled while nothing is pending. A
        // `PersistentOnly` handler never subscribes the ephemeral stream at
        // all.
        let mut persistent = self.outbox.listen_persisted(Some(state.sequence));
        let mut ephemeral =
            (H::SUBSCRIPTION == EventSubscription::All).then(|| self.outbox.listen_ephemeral());

        let mut op_slot: Option<es_entity::DbOp<'static>> = None;
        let mut tracker = BatchTracker {
            events_in_op: 0,
            collected: 0,
            persisted_seq: state.sequence,
            last_persist: tokio::time::Instant::now(),
        };
        let mut batch = H::Batch::default();
        let flusher = HandlerFlusher::<H, P> {
            handler: self.handler.clone(),
            _payload: std::marker::PhantomData,
        };

        loop {
            let event = if op_slot.is_some() || tracker.collected > 0 {
                // A batch is pending (an open op, collected items, or both):
                // only take persistent events that are already buffered —
                // the batch is never held open waiting on the network. A
                // pending stream is itself the flush trigger.
                match persistent.next().now_or_never() {
                    Some(Some(event)) => event,
                    Some(None) => {
                        let mut parts = CtxParts {
                            op_slot: &mut op_slot,
                            current_job: &mut current_job,
                            state: &state,
                            tracker: &mut tracker,
                        };
                        flush_batch(&mut parts, &mut batch, &flusher, "stream_closed")
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
                        };
                        flush_batch(&mut parts, &mut batch, &flusher, "backlog_drained")
                            .await
                            .map_err(|e| e as Box<dyn std::error::Error>)?;
                        continue;
                    }
                }
            } else {
                let next = tokio::select! {
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
                    // The two streams race in an UNBIASED inner select: the
                    // poll order is randomized per wait, so when both are
                    // continuously ready each gets a fair share of batch
                    // boundaries — neither channel can starve the other
                    // under any traffic pattern (N consecutive wins against
                    // a ready peer has probability 2^-N). Shutdown and the
                    // checkpoint timer keep deterministic priority above.
                    next = async {
                        tokio::select! {
                            Some(event) = next_if_subscribed(&mut ephemeral) => {
                                NextDelivery::Ephemeral(event)
                            }
                            event = persistent.next() => NextDelivery::Persistent(event),
                        }
                    } => next,
                };
                match next {
                    // Nothing is pending here by construction, so no
                    // transaction spans the foreign `handle_ephemeral` await
                    // and a failure discards no batch work.
                    NextDelivery::Ephemeral(event) => {
                        self.handler
                            .handle_ephemeral(&event)
                            .await
                            .map_err(|e| e as Box<dyn std::error::Error>)?;
                        continue;
                    }
                    NextDelivery::Persistent(Some(event)) => event,
                    NextDelivery::Persistent(None) => {
                        if tracker.persisted_seq < state.sequence {
                            persist_checkpoint(&mut current_job, &state)
                                .await
                                .map_err(|e| e as Box<dyn std::error::Error>)?;
                        }
                        return Ok(JobCompletion::RescheduleNow);
                    }
                }
            };

            // An undecodable payload never reaches the normal handling path:
            // the handler's handle_undecodable decides its fate. `Ok`
            // acknowledges the event (the checkpoint advances over it like a
            // skip); any `Err` — the default — fails the job with the
            // pending batch landed and the checkpoint parked at the sequence
            // *before* the event, so every retry re-reads it and nothing is
            // ever skipped.
            if event.decode_failure.is_some() {
                match self.handler.handle_undecodable(&event).await {
                    Ok(()) => {
                        state.sequence = event.sequence;
                        continue;
                    }
                    Err(error) => {
                        let mut parts = CtxParts {
                            op_slot: &mut op_slot,
                            current_job: &mut current_job,
                            state: &state,
                            tracker: &mut tracker,
                        };
                        flush_batch(&mut parts, &mut batch, &flusher, "undecodable_event")
                            .await
                            .map_err(|e| e as Box<dyn std::error::Error>)?;
                        if tracker.persisted_seq < state.sequence {
                            persist_checkpoint(&mut current_job, &state)
                                .await
                                .map_err(|e| e as Box<dyn std::error::Error>)?;
                        }
                        return Err(error as Box<dyn std::error::Error>);
                    }
                }
            }

            let ctx = EventCtx {
                parts: CtxParts {
                    op_slot: &mut op_slot,
                    current_job: &mut current_job,
                    state: &state,
                    tracker: &mut tracker,
                },
                batch: &mut batch,
                flusher: &flusher,
            };
            // The Handled token is branded with the invocation lifetime (it
            // cannot leave this call) and every path that mints one consumes
            // the ctx — so the outcome is authentic by construction. Extract
            // it in the same statement so the token (and with it the ctx
            // borrows) ends before the state advance below.
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
                    };
                    flush_batch(&mut parts, &mut batch, &flusher, "commit")
                        .await
                        .map_err(|e| e as Box<dyn std::error::Error>)?;
                }
                Outcome::Defer | Outcome::Collect => {
                    if tracker.events_in_op >= self.max_batch_size {
                        let mut parts = CtxParts {
                            op_slot: &mut op_slot,
                            current_job: &mut current_job,
                            state: &state,
                            tracker: &mut tracker,
                        };
                        flush_batch(&mut parts, &mut batch, &flusher, "batch_full")
                            .await
                            .map_err(|e| e as Box<dyn std::error::Error>)?;
                    }
                }
            }
        }
    }
}
