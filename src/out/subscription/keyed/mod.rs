//! Keyed subscribers — one consumer instance per domain entity (e.g. one per
//! webhook endpoint), created and destroyed transactionally with the entity,
//! each with its own durable cursor, costing nothing while idle.
//!
//! Where a [`singleton`](super::singleton) subscriber exists because code
//! declares it, a keyed subscriber exists because *data* creates it — one
//! per key, cancellable, its subscription an explicit row in the
//! `subscriptions` table (row absence = cancelled).
//!
//! obix owns identity and terms (this module + the `subscriptions` table);
//! the `job` crate owns execution and progress (liveness, generations,
//! attempts, watermark) via its keyed-job machinery, addressed by
//! `(subscriber_type, key)`. This module must never query job-crate tables
//! to decide wakes, and the runner must never treat job-row existence as
//! subscription existence — the `subscriptions` row is the truth.
//!
//! This module root holds what a keyed subscriber *is* — the [`WakeKey`],
//! the [`SubscriptionDef`]/[`KeyedSubscriber`] traits, [`KeyedSubscriberConfig`]
//! and the [`Subscriptions`] capability. The moving parts live beside it:
//! [`runner`] (the per-key job), [`waker`] and [`sweep`] (the two halves of
//! the wake plane — event-driven wakes and the periodic backstop).

mod runner;
mod sweep;
mod waker;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{marker::PhantomData, time::Duration};

use job::JobType;

use crate::out::Subscription;
use crate::out::ctx::{FlushOp, Handled, KeyedEventCtx};
use crate::out::event::{PersistentOutboxEvent, UndecodableEventError};
use crate::tables::MailboxTables;

pub(in crate::out) use runner::KeyedSubscriberJobInitializer;
pub(in crate::out) use sweep::{SweepJobData, SweepJobInitializer};
pub(in crate::out) use waker::{waker_handler, waker_job_type};

// === Wake key ===

/// A pure classification of an event into a partition of the stream,
/// declared by a subscription at creation.
///
/// **A wake key is a liveness signal, not a delivery filter.** A live member
/// reads the whole stream from its own cursor and decides event by event in
/// [`KeyedSubscriber::handle`]; wake keys are consulted only to decide whom
/// to *wake* once a member has passivated. So a false-positive match costs
/// one harmless empty run, while a missed match strands a member until
/// something else wakes it — which is why over-approximating is always the
/// safe direction.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WakeKey(pub(crate) String);

impl WakeKey {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for WakeKey {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for WakeKey {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl std::fmt::Display for WakeKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// === Traits ===

/// Keyed subscribers — persistent-only by construction: there is no
/// `handle_ephemeral` and no `SUBSCRIPTION` selector.
///
/// That is the other side of the presence contract. A keyed subscriber is
/// *not* always present — it passivates when idle, waits out holds, and for
/// a not-yet-subscribed key does not exist at all — so it cannot be offered
/// unreplayable events. What it gets in exchange is everything presence
/// forbids a [`SingletonSubscriber`](crate::out::SingletonSubscriber):
/// [`hold_until`](KeyedEventCtx::hold_until), staged chains across external
/// I/O, and the resume token.
///
/// A single-instance flow that needs those is persistent-only by definition
/// — host it here with one static key rather than reaching for a paused
/// singleton.
pub trait KeyedSubscriber<P>: Send + Sync + 'static
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    /// Accumulator for events resolved via
    /// [`collect_with`](KeyedEventCtx::collect_with) — see
    /// [`SingletonSubscriber::Batch`](crate::out::SingletonSubscriber::Batch).
    type Batch: Default + Send + 'static;

    fn handle<'inv>(
        &self,
        ctx: KeyedEventCtx<'inv, Self::Batch>,
        event: &PersistentOutboxEvent<P>,
    ) -> impl std::future::Future<
        Output = Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>>,
    > + Send;

    /// Same default and semantics as
    /// [`SingletonSubscriber::handle_undecodable`](crate::out::SingletonSubscriber::handle_undecodable):
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
    /// [`SingletonSubscriber::flush`](crate::out::SingletonSubscriber::flush).
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

    /// Which partition(s) of the stream this event belongs to — i.e. which
    /// subscriptions must be *awake* to see it. Empty = this event wakes
    /// nobody of this type; there is no separate `interest` prefilter, an
    /// empty return IS no-interest.
    ///
    /// This decides wakes, never delivery: a woken member reads the whole
    /// stream from its cursor and filters in
    /// [`handle`](KeyedSubscriber::handle). Over-approximating costs an empty
    /// run; under-approximating leaves a passivated member unwoken with its
    /// events unread, so it must never miss an event a live subscription
    /// would act on.
    fn wake_keys(&self, event: &PersistentOutboxEvent<P>) -> impl IntoIterator<Item = WakeKey>;

    /// Build the subscriber instance for one run. Called fresh on every run —
    /// every wake, every hold expiry, every retry, on any node — so this must
    /// be cheap and the subscriber must be stateless between runs: durable
    /// state is the cursor plus whatever the subscriber's own entities record,
    /// never anything held in the instance itself.
    fn instantiate(&self, key: Self::Key, cfg: Self::InstanceConfig) -> Self::Subscriber;
}

// === Errors ===

/// Why [`Subscriptions::subscribe_in_op`] refused to create a subscription.
#[derive(Debug, thiserror::Error)]
pub enum SubscribeError {
    /// The subscription declared no wake keys.
    ///
    /// Matching is set overlap, so an empty set intersects nothing: no event
    /// could ever reach this subscription through the waker, and the first
    /// time it passivated it would stay Dormant forever with its events
    /// unread. It is accepted at the type level (an `IntoIterator` can be
    /// empty) and meaningless at the semantic level, so it is rejected here
    /// rather than stored.
    ///
    /// A subscriber that genuinely wants waking on *every* event says so
    /// explicitly: declare one constant key and return that key from
    /// [`SubscriptionDef::wake_keys`] for every event. That costs a wake per
    /// event by construction, which is the point — the price is visible in
    /// the caller's own code instead of hidden in an empty vector.
    #[error("SubscribeError - EmptyWakeKeys: a subscription must declare at least one wake key")]
    EmptyWakeKeys,
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
pub(in crate::out) struct KeyMsg {
    key: String,
}

/// Enumerate one subscriber type's subscribed keys.
///
/// Boxed for the same reason
/// [`read_frontier`](crate::out::subscription::read_frontier) is, and the box
/// is equally load-bearing: [`MailboxTables`]'s methods return opaque futures
/// that capture their executor argument's lifetime, and awaiting one behind
/// `&self` makes the enclosing future's `Send`-ness higher-ranked — which
/// compiles here and then fails at any caller that `tokio::spawn`s it with
/// "implementation of `Send` is not general enough". Owning the pool and
/// erasing the future behind a box grounds the lifetime.
async fn list_subscription_keys<Tables: MailboxTables>(
    pool: &sqlx::PgPool,
    subscriber_type: &str,
) -> Result<Vec<String>, sqlx::Error> {
    let pool = pool.clone();
    let subscriber_type = subscriber_type.to_string();
    let fut: std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<String>, sqlx::Error>> + Send>,
    > = Box::pin(async move { Tables::list_subscription_keys(&pool, &subscriber_type).await });
    fut.await
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

// === Subscriptions capability ===

/// One entry of [`Subscriptions::members`]: a subscribed key paired with its
/// observable [`Subscription`].
pub type SubscriptionMember<D, P, Tables> =
    (<D as SubscriptionDef<P>>::Key, Subscription<P, Tables>);

/// [`Subscriptions::members`]'s return type.
pub type Members<D, P, Tables> =
    Result<Vec<SubscriptionMember<D, P, Tables>>, Box<dyn std::error::Error + Send + Sync>>;

/// Capability returned by
/// [`Outbox::register_keyed_subscriber`](crate::out::Outbox::register_keyed_subscriber):
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
    pub(in crate::out) fn new(
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
    /// existing row (its original `start_after` and `wake_keys` are never
    /// overwritten) and the existing live job.
    ///
    /// `wake_keys` must be non-empty — see
    /// [`SubscribeError::EmptyWakeKeys`] for why an empty set is a
    /// subscription that could never be woken.
    #[tracing::instrument(name = "obix.subscriptions.subscribe_in_op", skip_all, err)]
    pub async fn subscribe_in_op(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        key: D::Key,
        cfg: D::InstanceConfig,
        wake_keys: impl IntoIterator<Item = WakeKey>,
    ) -> Result<Subscription<P, Tables>, Box<dyn std::error::Error + Send + Sync>> {
        let key_str = key.to_string();
        let wake_keys: Vec<String> = wake_keys.into_iter().map(|r| r.0).collect();
        if wake_keys.is_empty() {
            return Err(SubscribeError::EmptyWakeKeys.into());
        }
        let instance_config = serde_json::to_value(&cfg)?;
        // Boxed, not `Tables::highest_known_persistent_sequence(&self.pool)`
        // directly — see `read_frontier`'s rationale. Awaiting the raw
        // opaque future behind `&self` makes this method's `Send`-ness
        // higher-ranked and breaks `tokio::spawn` at the CALLER, while
        // compiling cleanly here.
        let start_after = crate::out::subscription::read_frontier::<Tables>(&self.pool).await?;

        Tables::insert_subscription_in_op(
            op,
            self.job_type.as_str(),
            &key_str,
            &wake_keys,
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
        let keys = list_subscription_keys::<Tables>(&self.pool, self.job_type.as_str()).await?;
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
