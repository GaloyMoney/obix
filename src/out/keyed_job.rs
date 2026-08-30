//! Keyed subscribers — one consumer instance per domain entity (e.g. one per
//! webhook endpoint), created and destroyed transactionally with the entity,
//! each with its own durable cursor, costing nothing while idle.
//!
//! Vocabulary (see the design handoff, obix-dev library): a **subscriber**
//! consumes outbox events; a **subscription** is one identity's durable
//! relationship to the stream. A **singleton subscriber**
//! ([`SingletonSubscriber`](super::SingletonSubscriber)) exists because code
//! declares it — exactly one per type, permanent. A **keyed subscriber**
//! exists because *data* creates it — one per key, cancellable, its
//! subscription an explicit row in the `subscriptions` table (row absence =
//! cancelled).
//!
//! obix owns identity and terms (this module + the `subscriptions` table);
//! the `job` crate owns execution and progress (liveness, generations,
//! attempts, watermark) via its keyed-job machinery, addressed by
//! `(subscriber_type, key)`. This module must never query job-crate tables
//! for routing, and the runner must never treat job-row existence as
//! subscription existence — the `subscriptions` row is the truth.
//!
//! This is workstream 3 of the handoff: subscriptions table, traits, ctx
//! extensions, per-key runner and the [`Subscriptions`] capability, waking
//! via a periodic sweep only. The wake-plane router (liveness-only,
//! event-driven wakes) is a later, separately-shippable addition.

use futures::{FutureExt, StreamExt};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{marker::PhantomData, sync::Arc, time::Duration};

use job::{CurrentJob, Job, JobType, ResidentJobCompletion, ResidentJobInitializer, RetrySettings};

use super::ctx::*;
use super::{Outbox, Subscription, event::*};
use crate::tables::MailboxTables;

// === Routing key ===

/// A pure classification of an event into a partition of the stream,
/// declared by a subscription at creation. Liveness-only: a false-positive
/// match costs one harmless empty wake, never a correctness gap — routing
/// must never gate delivery.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RoutingKey(pub(crate) String);

impl RoutingKey {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for RoutingKey {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for RoutingKey {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl std::fmt::Display for RoutingKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// === Traits ===

/// Keyed subscribers — persistent-only by construction: there is no
/// `handle_ephemeral` and no `SUBSCRIPTION` selector, because both are
/// statically meaningless for a per-entity consumer (ephemeral events cannot
/// be replayed, and a keyed subscriber for a not-yet-subscribed key does not
/// exist to receive one).
pub trait KeyedSubscriber<P>: Send + Sync + 'static
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    /// Accumulator for events resolved via
    /// [`collect_with`](KeyedEventCtx::collect_with) — see
    /// [`SingletonSubscriber::Batch`](super::SingletonSubscriber::Batch).
    type Batch: Default + Send + 'static;

    fn handle<'inv>(
        &self,
        ctx: KeyedEventCtx<'inv, Self::Batch>,
        event: &PersistentOutboxEvent<P>,
    ) -> impl std::future::Future<
        Output = Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>>,
    > + Send;

    /// Same default and semantics as
    /// [`SingletonSubscriber::handle_undecodable`](super::SingletonSubscriber::handle_undecodable):
    /// fails with the error as-is, parking the cursor before the poison event.
    fn handle_undecodable(
        &self,
        error: &UndecodableEventError,
    ) -> impl std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send
    {
        let error = error.clone();
        async move { Err(error.into()) }
    }

    /// Same contract as
    /// [`SingletonSubscriber::flush`](super::SingletonSubscriber::flush).
    fn flush(
        &self,
        op: &mut FlushOp<'_>,
        items: Self::Batch,
    ) -> impl std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send
    {
        let _ = (op, items);
        async { Ok(()) }
    }
}

/// The definition of one keyed-subscriber type: pure classification plus a
/// factory. NO async, NO DB deps — everything here is cheap and synchronous,
/// called on every wake, hold expiry and retry (see [`KeyedSubscriber`]'s
/// factory contract on [`instantiate`](Self::instantiate)).
pub trait SubscriptionDef<P>: Send + Sync + 'static
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    /// The domain key identifying one subscription instance (e.g. a webhook
    /// endpoint id). `Display`/`FromStr` round-trip it through the
    /// subscriptions table and the job crate's string-keyed storage.
    type Key: Serialize
        + DeserializeOwned
        + std::fmt::Display
        + std::str::FromStr
        + Clone
        + Send
        + Sync
        + 'static;

    /// Immutable per-instance configuration, persisted alongside the key
    /// (e.g. the endpoint's owning partner id).
    type InstanceConfig: Serialize + DeserializeOwned + Send + Sync + 'static;

    type Subscriber: KeyedSubscriber<P>;

    /// Which partition(s) of the stream this event belongs to. Empty = no
    /// interest — there is no separate `interest` prefilter; an empty return
    /// IS no-interest. Must never miss an event a live subscription would act
    /// on: over-approximation is always safe, under-approximation is a
    /// correctness bug.
    fn routing_key(&self, event: &PersistentOutboxEvent<P>)
    -> impl IntoIterator<Item = RoutingKey>;

    /// Build the subscriber instance for one run. Called fresh on every run —
    /// every wake, every hold expiry, every retry, on any node — so this must
    /// be cheap and the subscriber must be stateless between runs: durable
    /// state is the cursor plus whatever the subscriber's own entities record,
    /// never anything held in the instance itself.
    fn instantiate(&self, key: Self::Key, cfg: Self::InstanceConfig) -> Self::Subscriber;
}

// === Configuration ===

const DEFAULT_LINGER: Duration = Duration::from_secs(30);
const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(600);
const DEFAULT_MAX_BATCH_SIZE: usize = 100;
const DEFAULT_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct KeyedSubscriberConfig {
    pub job_type: JobType,
    /// How long an idle (caught-up) member stays Active before passivating to
    /// Dormant. `Duration::MAX` = always-on. Default 30s.
    pub linger: Duration,
    /// How often the wake plane enumerates every subscribed key of this type
    /// and respawns them all (idempotent) — the startup reconcile, the repair
    /// path, and the staleness bound. Default 10 minutes.
    pub sweep_interval: Duration,
    pub checkpoint_interval: Duration,
    pub max_batch_size: usize,
    pub max_concurrent_per_process: Option<usize>,
}

impl KeyedSubscriberConfig {
    pub fn new(job_type: JobType) -> Self {
        Self {
            job_type,
            linger: DEFAULT_LINGER,
            sweep_interval: DEFAULT_SWEEP_INTERVAL,
            checkpoint_interval: DEFAULT_CHECKPOINT_INTERVAL,
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
            max_concurrent_per_process: None,
        }
    }

    pub fn with_linger(mut self, linger: Duration) -> Self {
        self.linger = linger;
        self
    }

    pub fn with_sweep_interval(mut self, sweep_interval: Duration) -> Self {
        self.sweep_interval = sweep_interval;
        self
    }

    pub fn with_checkpoint_interval(mut self, interval: Duration) -> Self {
        self.checkpoint_interval = interval;
        self
    }

    pub fn with_max_batch_size(mut self, max_batch_size: usize) -> Self {
        self.max_batch_size = max_batch_size.max(1);
        self
    }

    pub fn with_max_concurrent_per_process(mut self, n: usize) -> Self {
        self.max_concurrent_per_process = Some(n);
        self
    }
}

/// The job crate's keyed-job `Config`: tiny by design — obix passes only the
/// serialized key. Everything else a run needs lives in the `subscriptions`
/// row, read fresh every run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct KeyMsg {
    key: String,
}

/// Build a `'static` job type from a runtime string, once, at registration.
/// `job::JobType::new` only accepts `&'static str`; leaking a handful of
/// small strings once per process startup (one per registered keyed
/// subscriber type) for identifiers that live for the process's whole
/// lifetime is the standard, safe way to bridge that — not an ongoing leak.
fn derived_job_type(base: &str, suffix: &str) -> JobType {
    let s: &'static str = Box::leak(format!("{base}.{suffix}").into_boxed_str());
    JobType::new(s)
}

// === Object-safe flush bridge ===

struct KeyedSubscriberFlusher<S, P> {
    subscriber: Arc<S>,
    _payload: PhantomData<fn() -> P>,
}

impl<S, P> ItemFlush<S::Batch> for KeyedSubscriberFlusher<S, P>
where
    S: KeyedSubscriber<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    fn flush_items<'a>(
        &'a self,
        op: &'a mut es_entity::DbOp<'static>,
        items: S::Batch,
    ) -> BoxFuture<'a, Result<(), HandlerError>> {
        Box::pin(async move {
            let mut op = FlushOp::new(op);
            self.subscriber.flush(&mut op, items).await
        })
    }
}

// === Per-key runner ===

pub(super) struct KeyedSubscriberJobInitializer<D, P, Tables>
where
    D: SubscriptionDef<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    outbox: Outbox<P, Tables>,
    def: Arc<D>,
    job_type: JobType,
    linger: Duration,
    checkpoint_interval: Duration,
    max_batch_size: usize,
    max_concurrent_per_process: Option<usize>,
}

impl<D, P, Tables> KeyedSubscriberJobInitializer<D, P, Tables>
where
    D: SubscriptionDef<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    pub(super) fn new(
        outbox: Outbox<P, Tables>,
        def: Arc<D>,
        config: &KeyedSubscriberConfig,
    ) -> Self {
        Self {
            outbox,
            def,
            job_type: config.job_type.clone(),
            linger: config.linger,
            checkpoint_interval: config.checkpoint_interval,
            max_batch_size: config.max_batch_size,
            max_concurrent_per_process: config.max_concurrent_per_process,
        }
    }
}

impl<D, P, Tables> job::KeyedJobInitializer for KeyedSubscriberJobInitializer<D, P, Tables>
where
    D: SubscriptionDef<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    type Config = KeyMsg;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn retry_on_error_settings(&self) -> RetrySettings {
        RetrySettings::repeat_indefinitely()
    }

    fn max_concurrent_per_process(&self) -> Option<usize> {
        self.max_concurrent_per_process
    }

    /// A respawn resumes where the last generation left off — the watermark
    /// carries across generations, which is what makes the existing
    /// `Subscription` (checkpoint read-back) surface work per-key for free.
    fn inherits_state(&self) -> bool {
        true
    }

    fn init(
        &self,
        job: &Job,
        _spawner: job::KeyedJobSpawner<Self::Config>,
    ) -> Result<Box<dyn job::JobRunner>, Box<dyn std::error::Error>> {
        let KeyMsg { key } = job.config()?;
        let key: D::Key = key.parse().map_err(|_| -> Box<dyn std::error::Error> {
            "keyed subscriber: could not parse the persisted key".into()
        })?;
        Ok(Box::new(KeyedSubscriberJobRunner {
            outbox: self.outbox.clone(),
            def: self.def.clone(),
            key,
            job_type: self.job_type.clone(),
            linger: self.linger,
            checkpoint_interval: self.checkpoint_interval,
            max_batch_size: self.max_batch_size,
        }))
    }
}

struct KeyedSubscriberJobRunner<D, P, Tables>
where
    D: SubscriptionDef<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    outbox: Outbox<P, Tables>,
    def: Arc<D>,
    key: D::Key,
    job_type: JobType,
    linger: Duration,
    checkpoint_interval: Duration,
    max_batch_size: usize,
}

#[async_trait::async_trait]
impl<D, P, Tables> job::JobRunner for KeyedSubscriberJobRunner<D, P, Tables>
where
    D: SubscriptionDef<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    async fn run(
        &self,
        mut current_job: CurrentJob,
    ) -> Result<job::JobCompletion, Box<dyn std::error::Error>> {
        let key_str = self.key.to_string();

        // Run start: the subscriptions row is the truth. Missing → cancelled;
        // a stray wake respawning a just-cancelled key hits this and dies
        // harmlessly (the router/sweep can't route to it anyway — lookups go
        // through this same table).
        let Some(row) =
            Tables::find_subscription(current_job.pool(), self.job_type.as_str(), &key_str).await?
        else {
            return Ok(job::JobCompletion::Complete);
        };

        // The factory runs fresh every run: every wake, hold expiry, retry,
        // on any node. The subscriber must be cheap to build and stateless
        // between runs — durable state is the cursor plus its own entities.
        let instance_config: D::InstanceConfig = serde_json::from_value(row.instance_config)?;
        let subscriber = Arc::new(self.def.instantiate(self.key.clone(), instance_config));
        let flusher = KeyedSubscriberFlusher::<D::Subscriber, P> {
            subscriber: subscriber.clone(),
            _payload: PhantomData,
        };

        let mut state = current_job
            .execution_state::<OutboxEventJobState>()?
            .unwrap_or(OutboxEventJobState {
                sequence: row.start_after,
            });

        let mut persistent = self.outbox.listen_persisted(Some(state.sequence));

        let mut op_slot: Option<es_entity::DbOp<'static>> = None;
        let mut tracker = BatchTracker {
            events_in_op: 0,
            collected: 0,
            persisted_seq: state.sequence,
            last_persist: tokio::time::Instant::now(),
        };
        let mut batch = <D::Subscriber as KeyedSubscriber<P>>::Batch::default();

        // Armed once the persistent backlog is drained and nothing is
        // pending; disarmed the moment a new event arrives. Firing means
        // "caught up and quiet long enough" — passivate to Dormant.
        let mut linger_deadline: Option<tokio::time::Instant> = None;

        loop {
            let item = if op_slot.is_some() || tracker.collected > 0 {
                match persistent.next().now_or_never() {
                    Some(Some(item)) => item,
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
                        return Ok(job::JobCompletion::RescheduleNow);
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
                if linger_deadline.is_none() {
                    linger_deadline = Some(tokio::time::Instant::now() + self.linger);
                }
                let deadline = linger_deadline.expect("armed above");

                tokio::select! {
                    biased;
                    _ = current_job.shutdown_requested() => {
                        if tracker.persisted_seq < state.sequence {
                            persist_checkpoint(&mut current_job, &state)
                                .await
                                .map_err(|e| e as Box<dyn std::error::Error>)?;
                        }
                        return Ok(job::JobCompletion::RescheduleNow);
                    }
                    _ = tokio::time::sleep_until(deadline) => {
                        if tracker.persisted_seq < state.sequence {
                            let mut op = es_entity::DbOp::init_with_clock(
                                current_job.pool(),
                                current_job.clock(),
                            )
                            .await?;
                            current_job
                                .update_execution_state_in_op(&mut op, &state)
                                .await?;
                            return Ok(job::JobCompletion::CompleteWithOp(op));
                        }
                        return Ok(job::JobCompletion::Complete);
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
                    event = persistent.next() => {
                        linger_deadline = None;
                        match event {
                            Some(item) => item,
                            None => {
                                if tracker.persisted_seq < state.sequence {
                                    persist_checkpoint(&mut current_job, &state)
                                        .await
                                        .map_err(|e| e as Box<dyn std::error::Error>)?;
                                }
                                return Ok(job::JobCompletion::RescheduleNow);
                            }
                        }
                    }
                }
            };

            let event = match item {
                Ok(event) => event,
                Err(undecodable) => {
                    let mut parts = CtxParts {
                        op_slot: &mut op_slot,
                        current_job: &mut current_job,
                        state: &state,
                        tracker: &mut tracker,
                    };
                    flush_batch(&mut parts, &mut batch, &flusher, "undecodable_event")
                        .await
                        .map_err(|e| e as Box<dyn std::error::Error>)?;
                    match subscriber.handle_undecodable(&undecodable).await {
                        Ok(()) => {
                            state.sequence = undecodable.sequence;
                            continue;
                        }
                        Err(error) => {
                            if tracker.persisted_seq < state.sequence {
                                persist_checkpoint(&mut current_job, &state)
                                    .await
                                    .map_err(|e| e as Box<dyn std::error::Error>)?;
                            }
                            return Err(error as Box<dyn std::error::Error>);
                        }
                    }
                }
            };

            let ctx = KeyedEventCtx {
                parts: CtxParts {
                    op_slot: &mut op_slot,
                    current_job: &mut current_job,
                    state: &state,
                    tracker: &mut tracker,
                },
                batch: &mut batch,
                flusher: &flusher,
            };
            let outcome = subscriber
                .handle(ctx, &event)
                .await
                .map_err(|e| e as Box<dyn std::error::Error>)?
                .outcome;

            match outcome {
                Outcome::Hold(at) => {
                    // Does NOT advance state.sequence: the cursor holds
                    // strictly before this event, so the next run re-reads
                    // and re-evaluates it.
                    let mut parts = CtxParts {
                        op_slot: &mut op_slot,
                        current_job: &mut current_job,
                        state: &state,
                        tracker: &mut tracker,
                    };
                    flush_batch(&mut parts, &mut batch, &flusher, "hold_entry")
                        .await
                        .map_err(|e| e as Box<dyn std::error::Error>)?;
                    if tracker.persisted_seq < state.sequence {
                        persist_checkpoint(&mut current_job, &state)
                            .await
                            .map_err(|e| e as Box<dyn std::error::Error>)?;
                    }
                    return Ok(job::JobCompletion::RescheduleAt(at));
                }
                Outcome::CommitAndHold(at) => {
                    // Same non-advance as Hold: the isolated op's work lands,
                    // but the checkpoint it carries is still pre-this-event.
                    let mut parts = CtxParts {
                        op_slot: &mut op_slot,
                        current_job: &mut current_job,
                        state: &state,
                        tracker: &mut tracker,
                    };
                    flush_batch(&mut parts, &mut batch, &flusher, "commit_and_hold")
                        .await
                        .map_err(|e| e as Box<dyn std::error::Error>)?;
                    return Ok(job::JobCompletion::RescheduleAt(at));
                }
                Outcome::Skip => {
                    state.sequence = event.sequence;
                }
                Outcome::Commit => {
                    state.sequence = event.sequence;
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
                    state.sequence = event.sequence;
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

// === Sweep (step-3 wake plane: sweep-only, router deferred) ===
//
// A single per-process-independent resident job per keyed-subscriber type:
// on a timer, enumerate every currently-subscribed key and idempotently
// respawn it. This is the whole wake mechanism until the router lands (a
// later, separately-shippable commit) — a fresh subscription is Active from
// birth (see `Subscriptions::subscribe_in_op`) so it needs no wake until its
// first dormancy, and the sweep bounds how long a Dormant member can stay
// unwoken: the startup reconcile, the repair path, and the staleness bound,
// all in one.

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(super) struct SweepJobData {}

pub(super) struct SweepJobInitializer<Tables> {
    pool: sqlx::PgPool,
    subscriber_type: JobType,
    spawner: job::KeyedJobSpawner<KeyMsg>,
    sweep_interval: Duration,
    _tables: PhantomData<Tables>,
}

impl<Tables> SweepJobInitializer<Tables> {
    pub(super) fn new(
        pool: sqlx::PgPool,
        spawner: job::KeyedJobSpawner<KeyMsg>,
        config: &KeyedSubscriberConfig,
    ) -> Self {
        Self {
            pool,
            subscriber_type: config.job_type.clone(),
            spawner,
            sweep_interval: config.sweep_interval,
            _tables: PhantomData,
        }
    }
}

impl<Tables: MailboxTables> ResidentJobInitializer for SweepJobInitializer<Tables> {
    type Config = SweepJobData;

    fn job_type(&self) -> JobType {
        derived_job_type(self.subscriber_type.as_str(), "sweep")
    }

    fn retry_on_error_settings(&self) -> RetrySettings {
        RetrySettings::repeat_indefinitely()
    }

    fn init(
        &self,
        _job: &Job,
    ) -> Result<Box<dyn job::ResidentJobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(SweepJobRunner::<Tables> {
            pool: self.pool.clone(),
            subscriber_type: self.subscriber_type.clone(),
            spawner: self.spawner.clone(),
            sweep_interval: self.sweep_interval,
            _tables: PhantomData,
        }))
    }
}

struct SweepJobRunner<Tables> {
    pool: sqlx::PgPool,
    subscriber_type: JobType,
    spawner: job::KeyedJobSpawner<KeyMsg>,
    sweep_interval: Duration,
    _tables: PhantomData<Tables>,
}

impl<Tables: MailboxTables> SweepJobRunner<Tables> {
    async fn sweep_once(&self) -> Result<(), Box<dyn std::error::Error>> {
        let keys =
            Tables::list_subscription_keys(&self.pool, self.subscriber_type.as_str()).await?;
        if keys.is_empty() {
            return Ok(());
        }
        let specs = keys
            .into_iter()
            .map(|key| job::KeyedJobSpec::new(key.clone(), KeyMsg { key }))
            .collect();
        self.spawner.spawn_all(specs).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl<Tables: MailboxTables> job::ResidentJobRunner for SweepJobRunner<Tables> {
    async fn run(
        &self,
        mut current_job: CurrentJob,
    ) -> Result<ResidentJobCompletion, Box<dyn std::error::Error>> {
        // Sweep once immediately (the startup reconcile), then on every
        // subsequent interval.
        self.sweep_once().await?;
        loop {
            tokio::select! {
                biased;
                _ = current_job.shutdown_requested() => {
                    return Ok(ResidentJobCompletion::RescheduleNow);
                }
                _ = tokio::time::sleep(self.sweep_interval) => {
                    self.sweep_once().await?;
                }
            }
        }
    }
}

// === Subscriptions capability ===

/// One entry of [`Subscriptions::members`]: a subscribed key paired with its
/// observable [`Subscription`].
pub type SubscriptionMember<D, P, Tables> =
    (<D as SubscriptionDef<P>>::Key, Subscription<P, Tables>);

/// [`Subscriptions::members`]'s return type.
pub type Members<D, P, Tables> =
    Result<Vec<SubscriptionMember<D, P, Tables>>, Box<dyn std::error::Error + Send + Sync>>;

/// Capability returned by
/// [`Outbox::register_keyed_subscriber`](super::Outbox::register_keyed_subscriber):
/// create, cancel and observe individual subscriptions of one keyed
/// subscriber type. Cheap to clone, holds no long-lived borrow.
pub struct Subscriptions<D, P, Tables = crate::tables::DefaultMailboxTables>
where
    D: SubscriptionDef<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    pool: sqlx::PgPool,
    clock: es_entity::clock::ClockHandle,
    job_type: JobType,
    jobs: job::Jobs,
    spawner: job::KeyedJobSpawner<KeyMsg>,
    _phantom: PhantomData<(D, P, Tables)>,
}

// Manual `Clone`: this must be cloneable regardless of whether `D`/`P`/
// `Tables` are — deriving would wrongly bound all three through `PhantomData`.
// Mirrors `Outbox`'s manual impl.
impl<D, P, Tables> Clone for Subscriptions<D, P, Tables>
where
    D: SubscriptionDef<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            clock: self.clock.clone(),
            job_type: self.job_type.clone(),
            jobs: self.jobs.clone(),
            spawner: self.spawner.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<D, P, Tables> Subscriptions<D, P, Tables>
where
    D: SubscriptionDef<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    pub(super) fn new(
        pool: sqlx::PgPool,
        clock: es_entity::clock::ClockHandle,
        job_type: JobType,
        jobs: job::Jobs,
        spawner: job::KeyedJobSpawner<KeyMsg>,
    ) -> Self {
        Self {
            pool,
            clock,
            job_type,
            jobs,
            spawner,
            _phantom: PhantomData,
        }
    }

    /// Create a new subscription, transactionally with whatever entity it
    /// belongs to.
    ///
    /// `start_after` is sampled as the stream frontier *before* the row
    /// insert — deliberately conservative: an event racing this call lands
    /// at-or-before the sampled frontier and is therefore delivered (via the
    /// wake plane observing the row after commit) or not owed, never
    /// owed-and-missed. Born Active: the row is inserted, then the key is
    /// spawned in the same operation, so a fresh subscription drains from
    /// birth without needing any wake until its first dormancy.
    ///
    /// Idempotent: re-subscribing an already-subscribed key resolves to the
    /// existing row (its original `start_after` and `routing_keys` are never
    /// overwritten) and the existing live job.
    #[tracing::instrument(name = "obix.subscriptions.subscribe_in_op", skip_all, err)]
    pub async fn subscribe_in_op(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        key: D::Key,
        cfg: D::InstanceConfig,
        routing: impl IntoIterator<Item = RoutingKey>,
    ) -> Result<Subscription<P, Tables>, Box<dyn std::error::Error + Send + Sync>> {
        let key_str = key.to_string();
        let routing_keys: Vec<String> = routing.into_iter().map(|r| r.0).collect();
        let instance_config = serde_json::to_value(&cfg)?;
        let start_after = Tables::highest_known_persistent_sequence(&self.pool).await?;

        Tables::insert_subscription_in_op(
            op,
            self.job_type.as_str(),
            &key_str,
            &routing_keys,
            instance_config,
            start_after,
        )
        .await?;

        let spawned = self
            .spawner
            .spawn_in_op(op, key_str.clone(), KeyMsg { key: key_str })
            .await?;

        Ok(Subscription::new(spawned.handle, self.pool.clone()))
    }

    /// Cancel a subscription on the caller's own operation — atomic with,
    /// e.g., deletion of the domain entity it belongs to. Row absence is the
    /// tombstone: no job-kill API exists or is needed.
    #[tracing::instrument(name = "obix.subscriptions.cancel_in_op", skip_all, err)]
    pub async fn cancel_in_op(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        key: &D::Key,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Tables::delete_subscription_in_op(op, self.job_type.as_str(), &key.to_string()).await?;
        Ok(())
    }

    /// [`cancel_in_op`](Self::cancel_in_op), standalone.
    #[tracing::instrument(name = "obix.subscriptions.cancel", skip_all, err)]
    pub async fn cancel(
        &self,
        key: &D::Key,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut op = es_entity::DbOp::init_with_clock(&self.pool, &self.clock).await?;
        self.cancel_in_op(&mut op, key).await?;
        op.commit().await?;
        Ok(())
    }

    /// Observe one subscription's checkpoint, frontier and job status —
    /// resolves the live generation, or the latest terminal generation for a
    /// Dormant key (its watermark stays readable there because the runner
    /// registers with `inherits_state = true`).
    #[tracing::instrument(name = "obix.subscriptions.subscription", skip_all, err)]
    pub async fn subscription(
        &self,
        key: &D::Key,
    ) -> Result<Subscription<P, Tables>, Box<dyn std::error::Error + Send + Sync>> {
        let handle = self
            .jobs
            .keyed_handle(self.job_type.clone(), key.to_string())
            .await?
            .ok_or("no subscription has ever existed for this key")?;
        Ok(Subscription::new(handle, self.pool.clone()))
    }

    /// Every currently-subscribed key, paired with its observable
    /// [`Subscription`]. Enumerates the `subscriptions` table (the truth),
    /// not job-crate handles — a job row may outlive a cancelled
    /// subscription.
    #[tracing::instrument(name = "obix.subscriptions.members", skip_all, err)]
    pub async fn members(&self) -> Members<D, P, Tables> {
        let keys = Tables::list_subscription_keys(&self.pool, self.job_type.as_str()).await?;
        let mut members = Vec::with_capacity(keys.len());
        for key_str in keys {
            let Ok(key) = key_str.parse::<D::Key>() else {
                continue;
            };
            let Some(handle) = self
                .jobs
                .keyed_handle(self.job_type.clone(), key_str)
                .await?
            else {
                continue;
            };
            members.push((key, Subscription::new(handle, self.pool.clone())));
        }
        Ok(members)
    }
}
