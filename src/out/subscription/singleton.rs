use async_trait::async_trait;
use futures::{FutureExt, StreamExt};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::sync::Arc;

use job::{
    CurrentJob, Job, JobType, ResidentJobCompletion, ResidentJobInitializer, ResidentJobRunner,
    RetrySettings,
};

use crate::out::ctx::*;
use crate::out::lane::{InsertOrder, Lane};
use crate::out::{EphemeralOutboxListener, Outbox, event::*};
use crate::sequence::{CommitSequence, EventSequence};
use crate::tables::MailboxTables;

/// Which delivery streams an event-handler job subscribes to — see
/// [`SingletonSubscriber::SUBSCRIPTION`].
///
/// - [`PersistentOnly`](Self::PersistentOnly): only the durable, checkpointed
///   stream. The ephemeral stream is never subscribed, so batching is
///   entirely immune to ephemeral traffic.
/// - [`EphemeralOnly`](Self::EphemeralOnly): only the best-effort broadcast.
///   No persistent deliveries, and none of the checkpoint/batch machinery —
///   the job never reads or writes execution state.
/// - [`All`](Self::All): both streams, raced fairly between batches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamSelection {
    All,
    PersistentOnly,
    EphemeralOnly,
}

/// Handles the events of one outbox listener job.
///
/// Exactly one instance exists per type, created at registration and
/// **always on**. That presence is a contract, not an implementation detail:
/// it is what licenses the ephemeral subscription below, since ephemeral
/// events cannot be replayed and only an always-present consumer may hear
/// them. It is also why a singleton subscriber has no way to pause — no
/// `pause_until`, no staged chain. For a single-instance
/// flow that does need to pause or stage, see
/// [`KeyedSubscriber`](crate::out::KeyedSubscriber) with one static key.
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
///   become one batched flush. The batch lands when the ready persistent
///   backlog is drained (it is never held open waiting on the network), when
///   `max_batch_size` is reached, when a later event consumes, or on
///   shutdown. Requires the work to tolerate whole-batch replay after a
///   mid-batch failure.
/// - [`consume`](EventCtx::consume) then [`commit`](IsolatedOp::commit) — my
///   event is its own atomic unit: the pending batch (items + checkpoint)
///   lands before my work starts, and my op commits at return. Use for
///   causally significant or risky work.
///
/// Ephemeral events are delivered on their own stream and are handled
/// between batches: they never interrupt a pending batch, and nothing is
/// pending while [`handle_ephemeral`](Self::handle_ephemeral) runs. Most
/// handlers only consume one of the two streams — declare it via
/// [`SUBSCRIPTION`](Self::SUBSCRIPTION) and the other stream is never even
/// subscribed.
///
/// # The lane
///
/// `L` is the delivery lane, and it decides what a position is — for the
/// event, for the batch flush, and for the durable checkpoint. It defaults to
/// [`InsertOrder`](crate::out::InsertOrder), so `impl SingletonSubscriber<P>
/// for X` is an insert-lane handler; `impl SingletonSubscriber<P,
/// CommitOrder> for X` is the same code reading `CommitSequence`s, and the
/// two cannot be confused because the compiler will not let a commit-lane
/// handler run on the insert lane.
///
/// The lane is settled by the impl, not by configuration: it is a semantic
/// contract (what a flush boundary is, what the checkpoint counts), and a
/// subscription already checkpointed on one lane cannot move to the other —
/// registration refuses it.
pub trait SingletonSubscriber<P, L = InsertOrder>: Send + Sync + 'static
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    L: Lane,
{
    /// Which delivery streams this handler's job subscribes to. Defaults to
    /// [`All`](StreamSelection::All).
    ///
    /// Declaring a single-stream mode is a contract, not a filter: the other
    /// stream is never subscribed, so its handler method is never called —
    /// overriding [`handle_ephemeral`](Self::handle_ephemeral) on a
    /// [`PersistentOnly`](StreamSelection::PersistentOnly) handler (or
    /// [`handle_persistent`](Self::handle_persistent) on an
    /// [`EphemeralOnly`](StreamSelection::EphemeralOnly) one) is dead code.
    const SUBSCRIPTION: StreamSelection = StreamSelection::All;

    /// Accumulator for events resolved via
    /// [`collect_with`](EventCtx::collect_with) — `Vec<T>` for append-style
    /// batching, `HashMap<K, V>` for keyed coalescing folds, or any other
    /// `Default` container. Handlers that never collect use `()`.
    type Batch: Default + Send + 'static;

    /// The event, plus where it sits on `L`
    /// ([`position`](crate::out::Delivery::position) — an `EventSequence` on
    /// the insert lane, a `CommitSequence` on the commit lane, readable at
    /// any point in the invocation).
    ///
    /// Reading through the delivery is unchanged: it derefs to the shared
    /// [`Arc`] the outbox decoded once and broadcast to every subscriber,
    /// which in turn derefs to the event — so `event.payload`,
    /// `event.as_event::<E>()` and passing `event` where a
    /// `&PersistentOutboxEvent<P>` is expected all work. A handler that
    /// retains the event past the call — any
    /// [`collect_with`](EventCtx::collect_with) fold — clones the refcount
    /// via [`inner`](crate::out::Delivery::inner), not the payload, so `P`
    /// need not be `Clone`.
    fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, Self::Batch>,
        event: &EventDelivery<P, L>,
    ) -> impl std::future::Future<
        Output = Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>>,
    > + Send {
        let _ = event;
        async move { Ok(ctx.skip()) }
    }

    /// Handle a persistent event whose stored payload could not be decoded
    /// into `P` — delivered as the persistent stream's `Err` arm and never
    /// as an ordinary event, so it cannot reach
    /// [`handle_persistent`](Self::handle_persistent). It arrives with the
    /// same [`position`](crate::out::Delivery::position) an ordinary event
    /// would have had (the slot it occupies on `L`), and derefs to the
    /// [`UndecodableEventError`] carrying the event's identity and the raw
    /// payload + serde error (`error.failure`).
    ///
    /// The runner lands the pending batch *before* invoking this — like
    /// [`handle_ephemeral`](Self::handle_ephemeral), nothing is pending
    /// while it runs and no batch transaction spans the await, and work
    /// completed up to the event is durable regardless of the outcome.
    ///
    /// The default fails with the error as-is: a payload the consumer
    /// cannot decode is a runtime bug (schema drift, a producer ahead of
    /// this consumer, foreign rows in the table) that must surface loudly,
    /// not silently pass by. On `Err` the job fails with its checkpoint
    /// parked at the sequence *before* the event — every retry re-reads the
    /// event from the database, so deploying a consumer that understands
    /// the payload resumes the pipeline automatically, in order, with
    /// nothing skipped.
    ///
    /// Override and return `Ok(())` only when this handler genuinely wants
    /// to move past payloads it cannot decode — an explicit, auditable
    /// decision (consider recording `error.failure` somewhere durable
    /// first). The checkpoint then advances over the event exactly as for a
    /// [`skip`](EventCtx::skip). Any work done here must tolerate replay
    /// (the event is redelivered if the acknowledgement's checkpoint was
    /// not yet persisted at a crash).
    fn handle_undecodable(
        &self,
        error: &UndecodableDelivery<L>,
    ) -> impl std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send
    {
        let error = error.inner().clone();
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
    /// `Ok`, and a failure replays and re-collects the whole batch). It also
    /// carries [`position`](FlushOp::position): exactly where this batch
    /// lands on `L`, which is the watermark to record downstream.
    fn flush(
        &self,
        op: &mut FlushOp<'_, L>,
        items: Self::Batch,
    ) -> impl std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send
    {
        let _ = (op, items);
        async { Ok(()) }
    }

    /// Handed the shared [`Arc`] for the same reason as
    /// [`handle_persistent`](Self::handle_persistent), though an ephemeral
    /// event carries no sequence and belongs to no batch, so there is rarely
    /// anything to retain.
    fn handle_ephemeral(
        &self,
        event: &Arc<EphemeralOutboxEvent<P>>,
    ) -> impl std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send
    {
        let _ = event;
        async { Ok(()) }
    }
}

/// Object-safe bridge handing the handler's typed [`flush`] to the runner's
/// flush path (and to [`EventCtx::consume`]'s entry fence) with the
/// handler type erased.
///
/// [`flush`]: SingletonSubscriber::flush
struct SubscriberFlusher<H, P, L> {
    handler: Arc<H>,
    _payload: std::marker::PhantomData<fn() -> (P, L)>,
}

impl<H, P, L> ItemFlush<H::Batch> for SubscriberFlusher<H, P, L>
where
    H: SingletonSubscriber<P, L>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    L: Lane,
{
    fn flush_items<'a>(
        &'a self,
        op: &'a mut es_entity::DbOp<'static>,
        items: H::Batch,
        state: &'a OutboxEventJobState,
    ) -> BoxFuture<'a, Result<(), HandlerError>> {
        Box::pin(async move {
            let mut op =
                FlushOp::<L>::new(op, L::checkpoint(state.sequence, state.commit_sequence));
            self.handler.flush(&mut op, items).await
        })
    }
}

/// What the fair two-stream race yielded while no batch was pending.
enum NextDelivery<P, L>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
    L: Lane,
{
    Persistent(Option<Result<EventDelivery<P, L>, UndecodableDelivery<L>>>),
    Ephemeral(Arc<EphemeralOutboxEvent<P>>),
}

/// Next ephemeral event — or pend forever when the handler's
/// [`SUBSCRIPTION`](SingletonSubscriber::SUBSCRIPTION) never subscribed the
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

/// What [`decide_lane`] decided, before any I/O happens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LaneChoice {
    /// Stay on the insert lane from the stored insert cursor.
    Insert(EventSequence),
    /// Continue on the commit lane from the stored commit cursor.
    Commit(CommitSequence),
}

/// Decide the lane from stored state and configuration.
///
/// Pure, so the rules — in particular the two refusals — are testable without
/// a job, a pool or a running runner.
pub(crate) fn decide_lane(
    stored_commit: Option<CommitSequence>,
    stored_insert: EventSequence,
    configured: Ordering,
) -> Result<LaneChoice, String> {
    match (stored_commit, configured) {
        (None, Ordering::Insert) => Ok(LaneChoice::Insert(stored_insert)),
        (Some(commit_sequence), Ordering::Commit) => Ok(LaneChoice::Commit(commit_sequence)),
        (None, Ordering::Commit) => {
            if stored_insert == EventSequence::BEGIN {
                Ok(LaneChoice::Commit(CommitSequence::BEGIN))
            } else {
                Err(format!(
                    "subscription is checkpointed on the insert lane at sequence \
                     {stored_insert}; switching lanes is unsupported — register a new job \
                     type instead"
                ))
            }
        }
        (Some(_), Ordering::Insert) => Err(
            "subscription was checkpointed under Ordering::Commit; switching back to \
             Ordering::Insert is unsupported — register a new job type instead"
                .to_string(),
        ),
    }
}

const DEFAULT_MAX_BATCH_SIZE: usize = 100;
const DEFAULT_CHECKPOINT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Which order a subscriber receives persistent events in — the dynamic form
/// of the lane, as stored state and read-outs report it.
///
/// A subscriber declares its lane in the *type* system instead, by which
/// [`Lane`](crate::out::Lane) it implements
/// [`SingletonSubscriber`] for; this is what that
/// choice is called when it has to be a value.
///
/// Name-clashes with [`std::cmp::Ordering`]; import or path-qualify
/// explicitly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Ordering {
    /// Insert order: a contiguous [`EventSequence`](crate::EventSequence),
    /// gap-filled. The default, and unchanged by the commit lane's existence.
    #[default]
    Insert,
    /// Commit order: a dense [`CommitSequence`](crate::CommitSequence). A
    /// source transaction's events are contiguous and a batch flush never
    /// splits one.
    Commit,
}

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

    /// Backstop on how many collected events may share one batch before the
    /// runner force-flushes. Bounds the replay window and the flushed
    /// accumulator size of handlers that always
    /// [`collect`](crate::out::EventCtx::collect_with); handlers remain the
    /// primary size control by entering
    /// [`consume`](crate::out::EventCtx::consume), whose fence lands the
    /// pending batch.
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
pub(in crate::out) struct OutboxEventJobData {}

pub(in crate::out) struct OutboxEventJobInitializer<H, P, Tables, L>
where
    H: SingletonSubscriber<P, L>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
    L: Lane,
{
    outbox: Outbox<P, Tables>,
    handler: Arc<H>,
    job_type: JobType,
    retry_settings: RetrySettings,
    max_batch_size: usize,
    checkpoint_interval: std::time::Duration,
    _lane: std::marker::PhantomData<fn() -> L>,
}

impl<H, P, Tables, L> OutboxEventJobInitializer<H, P, Tables, L>
where
    H: SingletonSubscriber<P, L>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
    L: Lane,
{
    pub fn new(outbox: Outbox<P, Tables>, handler: H, config: &OutboxEventJobConfig) -> Self {
        Self {
            outbox,
            handler: Arc::new(handler),
            job_type: config.job_type.clone(),
            retry_settings: config.retry_settings.clone(),
            max_batch_size: config.max_batch_size,
            checkpoint_interval: config.checkpoint_interval,
            _lane: std::marker::PhantomData,
        }
    }
}

impl<H, P, Tables, L> ResidentJobInitializer for OutboxEventJobInitializer<H, P, Tables, L>
where
    H: SingletonSubscriber<P, L>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
    L: Lane,
{
    type Config = OutboxEventJobData;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn retry_on_error_settings(&self) -> RetrySettings {
        self.retry_settings.clone()
    }

    fn init(&self, _job: &Job) -> Result<Box<dyn ResidentJobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(OutboxEventJobRunner::<H, P, Tables, L> {
            outbox: self.outbox.clone(),
            handler: self.handler.clone(),
            max_batch_size: self.max_batch_size,
            checkpoint_interval: self.checkpoint_interval,
            _lane: std::marker::PhantomData,
        }))
    }
}

struct OutboxEventJobRunner<H, P, Tables, L>
where
    H: SingletonSubscriber<P, L>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
    L: Lane,
{
    outbox: Outbox<P, Tables>,
    handler: Arc<H>,
    max_batch_size: usize,
    checkpoint_interval: std::time::Duration,
    _lane: std::marker::PhantomData<fn() -> L>,
}

#[async_trait]
impl<H, P, Tables, L> ResidentJobRunner for OutboxEventJobRunner<H, P, Tables, L>
where
    H: SingletonSubscriber<P, L>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
    L: Lane,
{
    async fn run(
        &self,
        current_job: CurrentJob,
    ) -> Result<ResidentJobCompletion, Box<dyn std::error::Error>> {
        match H::SUBSCRIPTION {
            StreamSelection::EphemeralOnly => self.run_ephemeral_only(current_job).await,
            StreamSelection::All | StreamSelection::PersistentOnly => {
                self.run_with_persistent(current_job).await
            }
        }
    }
}

impl<H, P, Tables, L> OutboxEventJobRunner<H, P, Tables, L>
where
    H: SingletonSubscriber<P, L>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
    L: Lane,
{
    /// [`EphemeralOnly`](StreamSelection::EphemeralOnly): a bare dispatch
    /// loop — no persistent subscription, no execution state, no batch or
    /// checkpoint machinery.
    async fn run_ephemeral_only(
        &self,
        mut current_job: CurrentJob,
    ) -> Result<ResidentJobCompletion, Box<dyn std::error::Error>> {
        let mut ephemeral = self.outbox.listen_ephemeral();
        loop {
            tokio::select! {
                biased;
                _ = current_job.shutdown_requested() => {
                    return Ok(ResidentJobCompletion::RescheduleNow);
                }
                event = ephemeral.next() => match event {
                    Some(event) => {
                        self.handler
                            .handle_ephemeral(&event)
                            .await
                            .map_err(|e| e as Box<dyn std::error::Error>)?;
                    }
                    None => return Ok(ResidentJobCompletion::RescheduleNow),
                },
            }
        }
    }

    /// Open `L`'s stream at the subscription's stored cursor — refusing, as
    /// [`decide_lane`] does, a subscription established on the other lane.
    fn select_lane(
        &self,
        state: &OutboxEventJobState,
    ) -> Result<L::Listener<P>, Box<dyn std::error::Error>> {
        let start_after = L::resume_from(state.sequence, state.commit_sequence)?;
        Ok(L::listen(&self.outbox, start_after)?)
    }

    async fn run_with_persistent(
        &self,
        mut current_job: CurrentJob,
    ) -> Result<ResidentJobCompletion, Box<dyn std::error::Error>> {
        let mut state = current_job
            .execution_state::<OutboxEventJobState>()?
            .unwrap_or_default();

        // Two independent streams: the persistent backlog alone governs the
        // batch lifecycle, so ephemeral traffic can never shrink a batch —
        // and ephemerals are only handled while nothing is pending. A
        // `PersistentOnly` handler never subscribes the ephemeral stream at
        // all.
        let mut persistent = self.select_lane(&state)?;
        let mut ephemeral =
            (H::SUBSCRIPTION == StreamSelection::All).then(|| self.outbox.listen_ephemeral());

        let mut op_slot: Option<es_entity::DbOp<'static>> = None;
        let mut tracker = BatchTracker {
            collected: 0,
            persisted_seq: state.position(),
            persisted_insert_seq: state.sequence,
            last_persist: tokio::time::Instant::now(),
        };
        let mut in_group = false;
        let mut batch = H::Batch::default();
        let flusher = SubscriberFlusher::<H, P, L> {
            handler: self.handler.clone(),
            _payload: std::marker::PhantomData,
        };

        loop {
            let item = if tracker.collected > 0 {
                // A batch is pending (collected items awaiting a flush):
                // only take persistent events that are already buffered —
                // the batch is never held open waiting on the network. A
                // pending stream is itself the flush trigger.
                match persistent.next().now_or_never() {
                    Some(Some(item)) => item,
                    Some(None) => {
                        let mut parts = CtxParts {
                            op_slot: &mut op_slot,
                            current_job: &mut current_job,
                            state: &mut state,
                            tracker: &mut tracker,
                            mirror: None,
                        };
                        flush_batch(&mut parts, &mut batch, &flusher, "stream_closed")
                            .await
                            .map_err(|e| e as Box<dyn std::error::Error>)?;
                        return Ok(ResidentJobCompletion::RescheduleNow);
                    }
                    None if in_group => match persistent.next().await {
                        Some(item) => item,
                        None => {
                            let mut parts = CtxParts {
                                op_slot: &mut op_slot,
                                current_job: &mut current_job,
                                state: &mut state,
                                tracker: &mut tracker,
                                mirror: None,
                            };
                            flush_batch(&mut parts, &mut batch, &flusher, "stream_closed")
                                .await
                                .map_err(|e| e as Box<dyn std::error::Error>)?;
                            return Ok(ResidentJobCompletion::RescheduleNow);
                        }
                    },
                    None => {
                        let mut parts = CtxParts {
                            op_slot: &mut op_slot,
                            current_job: &mut current_job,
                            state: &mut state,
                            tracker: &mut tracker,
                            mirror: None,
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
                        if tracker.persisted_seq < state.position() {
                            persist_checkpoint(&mut current_job, &state, None)
                                .await
                                .map_err(|e| e as Box<dyn std::error::Error>)?;
                        }
                        return Ok(ResidentJobCompletion::RescheduleNow);
                    }
                    _ = tokio::time::sleep_until(tracker.last_persist + self.checkpoint_interval),
                        if tracker.persisted_seq < state.position() => {
                        persist_checkpoint(&mut current_job, &state, None)
                            .await
                            .map_err(|e| e as Box<dyn std::error::Error>)?;
                        tracker.persisted_seq = state.position();
                        tracker.persisted_insert_seq = state.sequence;
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
                    NextDelivery::Persistent(Some(item)) => item,
                    NextDelivery::Persistent(None) => {
                        if tracker.persisted_seq < state.position() {
                            persist_checkpoint(&mut current_job, &state, None)
                                .await
                                .map_err(|e| e as Box<dyn std::error::Error>)?;
                        }
                        return Ok(ResidentJobCompletion::RescheduleNow);
                    }
                }
            };

            // An undecodable payload arrives as the stream's `Err` arm and
            // never reaches handle_persistent: handle_undecodable decides its
            // fate. The pending batch lands FIRST — completed work and its
            // checkpoint (at the sequence *before* this event) are durable
            // regardless of the outcome, and no batch transaction spans the
            // foreign handle_undecodable await (the same fence as
            // consume's entry). Then `Ok` acknowledges the event
            // (the checkpoint advances over it like a skip); any `Err` — the
            // default — fails the job with the checkpoint parked before the
            // event, so every retry re-reads it and nothing is ever skipped.
            let event = match item {
                Ok(event) => event,
                Err(undecodable) => {
                    let mut parts = CtxParts {
                        op_slot: &mut op_slot,
                        current_job: &mut current_job,
                        state: &mut state,
                        tracker: &mut tracker,
                        mirror: None,
                    };
                    flush_batch(&mut parts, &mut batch, &flusher, "undecodable_event")
                        .await
                        .map_err(|e| e as Box<dyn std::error::Error>)?;
                    match self.handler.handle_undecodable(&undecodable).await {
                        Ok(()) => {
                            // INVARIANT: both cursors advance. Leaving
                            // `commit_sequence` behind redelivers the event
                            // after every restart.
                            state.sequence = undecodable.sequence;
                            L::record(&mut state.commit_sequence, undecodable.position());
                            continue;
                        }
                        Err(error) => {
                            if tracker.persisted_seq < state.position() {
                                persist_checkpoint(&mut current_job, &state, None)
                                    .await
                                    .map_err(|e| e as Box<dyn std::error::Error>)?;
                            }
                            return Err(error as Box<dyn std::error::Error>);
                        }
                    }
                }
            };

            let ctx = EventCtx {
                parts: CtxParts {
                    op_slot: &mut op_slot,
                    current_job: &mut current_job,
                    state: &mut state,
                    tracker: &mut tracker,
                    mirror: None,
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
            L::record(&mut state.commit_sequence, event.position());
            in_group = !event.boundary();
            match outcome {
                Outcome::Skip => {}
                Outcome::Commit => {
                    let mut parts = CtxParts {
                        op_slot: &mut op_slot,
                        current_job: &mut current_job,
                        state: &mut state,
                        tracker: &mut tracker,
                        mirror: None,
                    };
                    flush_batch(&mut parts, &mut batch, &flusher, "commit")
                        .await
                        .map_err(|e| e as Box<dyn std::error::Error>)?;
                }
                Outcome::Collect => {
                    if tracker.collected >= self.max_batch_size && !in_group {
                        let mut parts = CtxParts {
                            op_slot: &mut op_slot,
                            current_job: &mut current_job,
                            state: &mut state,
                            tracker: &mut tracker,
                            mirror: None,
                        };
                        flush_batch(&mut parts, &mut batch, &flusher, "batch_full")
                            .await
                            .map_err(|e| e as Box<dyn std::error::Error>)?;
                    }
                }
                Outcome::Pause(_) | Outcome::CommitAndPause(_) => {
                    // Type-gated unreachable: only KeyedEventCtx, StagedOp
                    // and Suspended can mint these, and a singleton
                    // subscriber's EventCtx reaches none of them.
                    unreachable!(
                        "Outcome::Pause/CommitAndPause cannot be minted from a singleton \
                         subscriber's EventCtx"
                    )
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_lane_stays_on_the_insert_cursor() {
        let choice = decide_lane(None, EventSequence::from(7u64), Ordering::Insert);
        assert_eq!(choice, Ok(LaneChoice::Insert(EventSequence::from(7u64))));
    }

    #[test]
    fn commit_lane_continues_from_the_commit_cursor() {
        let choice = decide_lane(
            Some(CommitSequence::from(4u64)),
            EventSequence::from(9u64),
            Ordering::Commit,
        );
        assert_eq!(choice, Ok(LaneChoice::Commit(CommitSequence::from(4u64))));
    }

    /// A subscription that has never checkpointed is on no lane yet, so it
    /// starts on the configured one from the beginning of that lane.
    #[test]
    fn a_fresh_subscription_starts_at_the_beginning_of_the_commit_lane() {
        let choice = decide_lane(None, EventSequence::BEGIN, Ordering::Commit);
        assert_eq!(choice, Ok(LaneChoice::Commit(CommitSequence::BEGIN)));
    }

    /// The two cursors count different things, so an established
    /// subscription cannot be moved by flipping one enum value.
    #[test]
    fn switching_an_established_subscription_to_commit_is_refused() {
        let error =
            decide_lane(None, EventSequence::from(12u64), Ordering::Commit).expect_err("refuses");
        assert!(error.contains("register a new job type"), "{error}");
    }

    #[test]
    fn switching_back_to_the_insert_lane_is_refused() {
        let error = decide_lane(
            Some(CommitSequence::from(3u64)),
            EventSequence::from(9u64),
            Ordering::Insert,
        )
        .expect_err("refuses");
        assert!(error.contains("register a new job type"), "{error}");
    }
}
