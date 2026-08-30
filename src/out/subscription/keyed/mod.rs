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
//! [`runner`] (the per-key job, one per type) and [`waker`] (the whole wake
//! plane, one per *outbox*, holding one erased route per registered type).
//!
//! There is no periodic reconciler. Every wake is driven by the stream:
//! wake-key matches for arrivals, and cache-pressure catch-up for members
//! drifting toward a cold read. A timer would have to run — and cost a job
//! slot per process — whether or not anything had happened, which on a quiet
//! outbox is exactly when there is nothing for it to find.

mod runner;
mod waker;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{marker::PhantomData, time::Duration};

use job::JobType;

use crate::out::Subscription;
use crate::out::ctx::{FlushOp, Handled, KeyedEventCtx};
use crate::out::event::{PersistentOutboxEvent, UndecodableEventError};
use crate::tables::MailboxTables;

pub(in crate::out) use runner::KeyedSubscriberJobInitializer;
pub(in crate::out) use waker::{WakeRoutes, wake_route, waker_handler, waker_job_type};

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

/// The set of wake keys a subscription declares — **non-empty by
/// construction**.
///
/// Matching is set overlap, and `{} && {anything}` is false, so a
/// subscription declaring nothing could never be reached by the waker: it
/// would work while Active (a live member reads the whole stream regardless)
/// and strand itself the first time it passivated. That is not a
/// configuration anyone wants, so it is not a value anyone can build.
///
/// ```
/// use obix::{WakeKey, WakeKeys};
///
/// // One key — the common case, infallible.
/// let one: WakeKeys = WakeKey::from("endpoint-7").into();
///
/// // Several known at the call site.
/// let many = WakeKeys::new(WakeKey::from("7")).and(WakeKey::from("8"));
/// assert_eq!(many.len(), 2);
/// ```
///
/// There is no way to reach `subscribe_in_op` with nothing — a bare
/// collection is not `Into<WakeKeys>`, so the empty case has no syntax:
///
/// ```compile_fail
/// # use obix::WakeKey;
/// fn takes(_: impl Into<obix::WakeKeys>) {}
/// takes(Vec::<WakeKey>::new());
/// ```
///
/// Keys computed from runtime data are the one case a type cannot decide:
/// build with [`try_from`](Self::try_from) and handle the empty case where
/// the domain context to interpret it actually lives.
///
/// ```
/// use obix::{WakeKey, WakeKeys};
///
/// let configured: Vec<WakeKey> = vec![];
/// assert!(WakeKeys::try_from(configured).is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WakeKeys(Vec<WakeKey>);

impl WakeKeys {
    /// Start from one key; add more with [`and`](Self::and).
    pub fn new(first: WakeKey) -> Self {
        Self(vec![first])
    }

    pub fn and(mut self, key: WakeKey) -> Self {
        self.0.push(key);
        self
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Never true — the type has no empty state. Present because clippy asks
    /// for it beside `len`, and because saying so is cheaper than a reader
    /// wondering.
    pub fn is_empty(&self) -> bool {
        false
    }

    pub fn iter(&self) -> impl Iterator<Item = &WakeKey> {
        self.0.iter()
    }

    pub(crate) fn into_strings(self) -> Vec<String> {
        self.0.into_iter().map(|k| k.0).collect()
    }
}

impl From<WakeKey> for WakeKeys {
    fn from(key: WakeKey) -> Self {
        Self::new(key)
    }
}

impl TryFrom<Vec<WakeKey>> for WakeKeys {
    type Error = SubscribeError;

    fn try_from(keys: Vec<WakeKey>) -> Result<Self, Self::Error> {
        if keys.is_empty() {
            return Err(SubscribeError::EmptyWakeKeys);
        }
        Ok(Self(keys))
    }
}

impl IntoIterator for WakeKeys {
    type Item = WakeKey;
    type IntoIter = std::vec::IntoIter<WakeKey>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
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

/// Why a set of wake keys could not be accepted.
#[derive(Debug, thiserror::Error)]
pub enum SubscribeError {
    /// A runtime-built collection of wake keys turned out to be empty.
    ///
    /// Matching is set overlap, so an empty set intersects nothing: no event
    /// could ever reach such a subscription through the waker, and the first
    /// time it passivated it would stay Dormant forever with its events
    /// unread.
    ///
    /// [`WakeKeys`] makes that unrepresentable, so this is reachable only
    /// through [`WakeKeys::try_from`] — the one place a type cannot decide,
    /// because the keys came from data rather than from the call site. It is
    /// deliberately raised *there* rather than inside
    /// [`Subscriptions::subscribe_in_op`], so the caller handles it where the
    /// domain context to interpret an empty set actually exists.
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
const DEFAULT_MAX_BATCH_SIZE: usize = 100;
const DEFAULT_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(5);

/// Per-type tuning for one keyed-subscriber registration.
///
/// Everything here is an operator dial: it varies per deployment and must be
/// settable without recompiling the subscriber. What the subscriber *is* —
/// its key type, instance config, batch accumulator and classification —
/// lives on [`SubscriptionDef`]/[`KeyedSubscriber`] instead, because the
/// compiler needs it and it cannot vary per deployment.
///
/// Retry settings are deliberately absent: a keyed member retries
/// indefinitely, since giving up would leave a live subscription silently
/// stopped with no cancellation and no error anywhere.
#[derive(Clone)]
pub struct KeyedSubscriberConfig {
    pub job_type: JobType,
    /// How long an idle (caught-up) member stays Active before passivating to
    /// Dormant. `Duration::MAX` = always-on. Default 30s.
    ///
    /// Counts this member's *own* idleness, not stream quiet: a member that
    /// only skips is idle however busy the outbox is.
    pub linger: Duration,
    /// Maximum staleness of the persisted checkpoint over skip-only
    /// stretches, where nothing else would open a transaction. Never delays
    /// handling — it only bounds how many harmless replays a crash costs,
    /// and how stale the mirrored cursor the waker's catch-up scan reads can
    /// be. Default 5s.
    pub checkpoint_interval: Duration,
    /// Backstop on how many `collect_with` events may share one batch before
    /// the runner force-flushes, bounding both the replay window after a
    /// mid-batch failure and the in-memory accumulator. Handlers that
    /// periodically `consume` land the batch at that fence anyway and are
    /// largely unaffected. Default 100.
    pub max_batch_size: usize,
    /// Cap on how many keys *of this type* run concurrently on one process.
    /// `None` defers to the job crate's own default.
    ///
    /// Exists for keyed subscribers and not singletons because cardinality
    /// does: a singleton is one job, whereas a keyed type can be one per
    /// entity, and a burst that wakes thousands at once would otherwise
    /// contend for every job slot and pool connection in the process.
    pub max_concurrent_per_process: Option<usize>,
}

impl KeyedSubscriberConfig {
    pub fn new(job_type: JobType) -> Self {
        Self {
            job_type,
            linger: DEFAULT_LINGER,
            checkpoint_interval: DEFAULT_CHECKPOINT_INTERVAL,
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
            max_concurrent_per_process: None,
        }
    }

    pub fn with_linger(mut self, linger: Duration) -> Self {
        self.linger = linger;
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
    /// `wake_keys` takes anything convertible into [`WakeKeys`], which is
    /// non-empty by construction — a single [`WakeKey`] converts directly.
    /// See [`WakeKeys`] for why an empty set is a subscription that could
    /// never be woken, and how to build one from runtime data.
    #[tracing::instrument(name = "obix.subscriptions.subscribe_in_op", skip_all, err)]
    pub async fn subscribe_in_op(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        key: D::Key,
        cfg: D::InstanceConfig,
        wake_keys: impl Into<WakeKeys>,
    ) -> Result<Subscription<P, Tables>, Box<dyn std::error::Error + Send + Sync>> {
        let key_str = key.to_string();
        // Non-empty by the type, so there is no emptiness check here — see
        // [`WakeKeys`]. The DB's `CHECK (cardinality(wake_keys) > 0)` remains
        // as the backstop against any other writer to the table.
        let wake_keys = wake_keys.into().into_strings();
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

        self.spawner
            .spawn_in_op(
                op,
                key_str.clone(),
                KeyMsg {
                    key: key_str.clone(),
                },
            )
            .await?;

        // Anchored to (subscriber_type, key), NOT to the handle the spawn
        // just returned: that handle names this generation, and the next
        // wake mints another. See `JobAnchor::Keyed`.
        Ok(Subscription::new_keyed(
            self.jobs.clone(),
            self.job_type.clone(),
            key_str,
            self.pool.clone(),
        ))
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

    /// Observe one subscription's checkpoint, frontier and job status.
    ///
    /// The returned [`Subscription`] tracks the key across wakes: each read
    /// resolves the live generation, or the latest terminal generation for a
    /// Dormant key (whose watermark stays readable because the runner
    /// registers with `inherits_state = true`). It is safe to hold — holding
    /// a job handle instead would report checkpoint 0 from the next wake
    /// onward.
    #[tracing::instrument(name = "obix.subscriptions.subscription", skip_all, err)]
    pub async fn subscription(
        &self,
        key: &D::Key,
    ) -> Result<Subscription<P, Tables>, Box<dyn std::error::Error + Send + Sync>> {
        let key_str = key.to_string();
        // Resolved once here only to report "never subscribed" as an error
        // rather than deferring it to the first read.
        self.jobs
            .keyed_handle(self.job_type.clone(), key_str.clone())
            .await?
            .ok_or("no subscription has ever existed for this key")?;
        Ok(Subscription::new_keyed(
            self.jobs.clone(),
            self.job_type.clone(),
            key_str,
            self.pool.clone(),
        ))
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
            if self
                .jobs
                .keyed_handle(self.job_type.clone(), key_str.clone())
                .await?
                .is_none()
            {
                continue;
            }
            members.push((
                key,
                Subscription::new_keyed(
                    self.jobs.clone(),
                    self.job_type.clone(),
                    key_str,
                    self.pool.clone(),
                ),
            ));
        }
        Ok(members)
    }
}
