//! Contracts for keyed subscribers: per-entity outbox consumers with
//! wake-on-demand — the `subscriptions` table,
//! `SubscriptionDef`/`KeyedSubscriber`, the hold verbs, the per-key runner,
//! the `Subscriptions` capability, and both halves of the wake plane (the
//! event-driven router and the periodic sweep that backstops it).

mod helpers;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use obix::{
    EventSequence, Handled, KeyedEventCtx, KeyedSubscriber, KeyedSubscriberConfig, MailboxConfig,
    RoutingKey, SubscriptionDef, out::Outbox,
};
use serde::{Deserialize, Serialize};
use serial_test::file_serial;
use tokio::sync::Mutex;

use helpers::{TestTables, init_pool, wipeout_keyed_subscriber_job_tables, wipeout_subscriptions};

const JOB_TYPE: &str = "test-keyed-subscriber";
const STAGED_JOB_TYPE: &str = "test-keyed-staged";

/// Short enough for dormancy/sweep contracts to land inside a test's
/// patience.
const TEST_LINGER: Duration = Duration::from_millis(150);
const TEST_SWEEP_INTERVAL: Duration = Duration::from_millis(150);
const TEST_CHECKPOINT_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum TestEvent {
    Ping { owner: u64, n: u64 },
}

/// The domain key: an owning entity id. `Display`/`FromStr` round-trip it
/// through the subscriptions table and job's string-keyed storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct OwnerId(u64);

impl std::fmt::Display for OwnerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for OwnerId {
    type Err = std::num::ParseIntError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(OwnerId(s.parse()?))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct InstanceConfig {
    /// Owners this subscription records for *in addition to* its own key.
    /// Empty for every contract where the routing key and the domain key
    /// coincide; non-empty only for the multi-routing-key contract, which
    /// deliberately separates the two so a subscription can watch several
    /// partitions that are none of them its own id.
    #[serde(default)]
    watched: Vec<u64>,
}

/// Shared, cross-run observation point: every instantiated subscriber for a
/// key clones this in, so the test can see what happened across the
/// runner's many fresh `instantiate` calls (one per run/wake/retry).
#[derive(Clone, Default)]
struct Shared {
    received: Arc<Mutex<HashMap<u64, Vec<u64>>>>,
    /// One-shot hold instruction per key: the next delivery for that key
    /// parks at the given time instead of recording, then is consumed.
    hold_once: Arc<Mutex<HashMap<u64, chrono::DateTime<chrono::Utc>>>>,
}

struct TestDef {
    shared: Shared,
}

impl SubscriptionDef<TestEvent> for TestDef {
    type Key = OwnerId;
    type InstanceConfig = InstanceConfig;
    type Subscriber = RecordingSubscriber;

    fn routing_key(
        &self,
        event: &obix::out::PersistentOutboxEvent<TestEvent>,
    ) -> impl IntoIterator<Item = RoutingKey> {
        match &event.payload {
            Some(TestEvent::Ping { owner, .. }) => vec![RoutingKey::from(owner.to_string())],
            None => vec![],
        }
    }

    fn instantiate(&self, key: Self::Key, cfg: Self::InstanceConfig) -> Self::Subscriber {
        RecordingSubscriber {
            key,
            watched: cfg.watched,
            shared: self.shared.clone(),
        }
    }
}

/// Records every `Ping` addressed to its own key. Every subscriber for one
/// keyed subscriber type sees the WHOLE persistent stream (there is no
/// server-side routing yet), so it must filter events belonging to other
/// keys itself — exactly like a real per-entity consumer would.
struct RecordingSubscriber {
    key: OwnerId,
    /// Extra owners to record for beyond `key`, from the instance config.
    watched: Vec<u64>,
    shared: Shared,
}

impl KeyedSubscriber<TestEvent> for RecordingSubscriber {
    type Batch = ();

    async fn handle<'inv>(
        &self,
        ctx: KeyedEventCtx<'inv, ()>,
        event: &obix::out::PersistentOutboxEvent<TestEvent>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        let Some(TestEvent::Ping { owner, n }) = &event.payload else {
            return Ok(ctx.skip());
        };
        if *owner != self.key.0 && !self.watched.contains(owner) {
            return Ok(ctx.skip());
        }

        if let Some(at) = self.shared.hold_once.lock().await.remove(&self.key.0) {
            return Ok(ctx.hold_until(at));
        }

        self.shared
            .received
            .lock()
            .await
            .entry(self.key.0)
            .or_default()
            .push(*n);
        Ok(ctx.skip())
    }
}

async fn init_jobs(pool: &sqlx::PgPool) -> anyhow::Result<job::Jobs> {
    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    Ok(job::Jobs::init(job_config).await?)
}

async fn init_outbox(pool: &sqlx::PgPool) -> anyhow::Result<Outbox<TestEvent, TestTables>> {
    helpers::wipeout_outbox_tables(pool).await?;
    wipeout_keyed_subscriber_job_tables(pool, JOB_TYPE).await?;
    wipeout_subscriptions(pool, JOB_TYPE).await?;

    Ok(Outbox::<TestEvent, TestTables>::init(
        pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?)
}

fn test_config() -> KeyedSubscriberConfig {
    KeyedSubscriberConfig::new(job::JobType::new(JOB_TYPE))
        .with_linger(TEST_LINGER)
        .with_sweep_interval(TEST_SWEEP_INTERVAL)
        .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL)
}

async fn publish_ping(
    outbox: &Outbox<TestEvent, TestTables>,
    owner: u64,
    n: u64,
) -> anyhow::Result<()> {
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping { owner, n })
        .await?;
    op.commit().await?;
    Ok(())
}

/// Poll `f` until it holds or `timeout` elapses.
async fn eventually<F, Fut>(timeout: Duration, mut f: F) -> anyhow::Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<bool>>,
{
    let start = std::time::Instant::now();
    loop {
        if f().await? {
            return Ok(());
        }
        if start.elapsed() >= timeout {
            anyhow::bail!("condition did not hold within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn received_for(shared: &Shared, owner: u64) -> Vec<u64> {
    shared
        .received
        .lock()
        .await
        .get(&owner)
        .cloned()
        .unwrap_or_default()
}

/// Contract 1/2 — per-key ordering and from-birth delivery: a subscription
/// drains its own key's events in order from its birth onward, and never
/// sees events published before it subscribed.
#[tokio::test]
#[file_serial]
async fn subscription_delivers_in_order_from_its_own_birth() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    // Published BEFORE any subscription exists — must never be delivered to
    // a subscription born after it.
    publish_ping(&outbox, 1, 999).await?;

    let shared = Shared::default();
    let def = TestDef {
        shared: shared.clone(),
    };
    let subs = outbox
        .register_keyed_subscriber(&mut jobs, test_config(), def)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut op = outbox.begin_op().await?;
    subs.subscribe_in_op(
        &mut op,
        OwnerId(1),
        InstanceConfig::default(),
        Vec::<RoutingKey>::new(),
    )
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    op.commit().await?;

    jobs.start_poll().await?;

    for n in 1..=5u64 {
        publish_ping(&outbox, 1, n).await?;
    }

    eventually(Duration::from_secs(10), || async {
        Ok(received_for(&shared, 1).await == vec![1, 2, 3, 4, 5])
    })
    .await?;

    // The pre-subscription event never arrives.
    assert_eq!(received_for(&shared, 1).await, vec![1, 2, 3, 4, 5]);

    Ok(())
}

/// Contract — key isolation: two independently-subscribed keys each see only
/// their own events, interleaved on the same stream.
#[tokio::test]
#[file_serial]
async fn independent_keys_do_not_interfere() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let shared = Shared::default();
    let def = TestDef {
        shared: shared.clone(),
    };
    let subs = outbox
        .register_keyed_subscriber(&mut jobs, test_config(), def)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut op = outbox.begin_op().await?;
    subs.subscribe_in_op(
        &mut op,
        OwnerId(1),
        InstanceConfig::default(),
        Vec::<RoutingKey>::new(),
    )
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    subs.subscribe_in_op(
        &mut op,
        OwnerId(2),
        InstanceConfig::default(),
        Vec::<RoutingKey>::new(),
    )
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    op.commit().await?;

    jobs.start_poll().await?;

    publish_ping(&outbox, 1, 10).await?;
    publish_ping(&outbox, 2, 20).await?;
    publish_ping(&outbox, 1, 11).await?;
    publish_ping(&outbox, 2, 21).await?;

    eventually(Duration::from_secs(10), || async {
        Ok(received_for(&shared, 1).await == vec![10, 11]
            && received_for(&shared, 2).await == vec![20, 21])
    })
    .await?;

    Ok(())
}

/// Contract — hold: `hold_until` parks the cursor strictly before the held
/// event (checkpoint does not advance), and the same event is redelivered
/// once the hold expires — never skipped.
#[tokio::test]
#[file_serial]
async fn hold_until_parks_the_cursor_and_redelivers_on_resume() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let shared = Shared::default();
    let def = TestDef {
        shared: shared.clone(),
    };
    let subs = outbox
        .register_keyed_subscriber(&mut jobs, test_config(), def)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut op = outbox.begin_op().await?;
    subs.subscribe_in_op(
        &mut op,
        OwnerId(1),
        InstanceConfig::default(),
        Vec::<RoutingKey>::new(),
    )
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    op.commit().await?;

    // Arm a hold for the very first event this key will see, well past the
    // checkpoint interval so a premature advance would be caught.
    let hold_until = chrono::Utc::now() + chrono::Duration::milliseconds(600);
    shared.hold_once.lock().await.insert(1, hold_until);

    jobs.start_poll().await?;
    publish_ping(&outbox, 1, 7).await?;

    let subscription = subs
        .subscription(&OwnerId(1))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // While parked: the event must not be recorded, and the checkpoint must
    // stay at BEGIN — proving the hold did not advance past it.
    eventually(Duration::from_secs(5), || {
        let subscription = subscription.clone();
        async move { Ok(!subscription.load().await?.job_status().is_terminal()) }
    })
    .await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        received_for(&shared, 1).await,
        Vec::<u64>::new(),
        "event must not be recorded while held"
    );
    assert_eq!(
        subscription.load().await?.checkpoint(),
        EventSequence::BEGIN,
        "checkpoint must not advance past a held event"
    );

    // Once the hold expires, the SAME event resumes and is delivered exactly
    // once — not skipped, not duplicated.
    eventually(Duration::from_secs(10), || async {
        Ok(received_for(&shared, 1).await == vec![7])
    })
    .await?;
    eventually(Duration::from_secs(10), || {
        let subscription = subscription.clone();
        async move { Ok(subscription.load().await?.checkpoint() >= EventSequence::from(1u64)) }
    })
    .await?;

    jobs.shutdown().await?;

    Ok(())
}

/// Contract — cancel: row deletion is the tombstone. A cancelled key stops
/// processing, and the periodic sweep never respawns it (the sweep only
/// enumerates the subscriptions table, which no longer has the row).
#[tokio::test]
#[file_serial]
async fn cancel_stops_delivery_and_the_sweep_never_revives_it() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let shared = Shared::default();
    let def = TestDef {
        shared: shared.clone(),
    };
    let subs = outbox
        .register_keyed_subscriber(&mut jobs, test_config(), def)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut op = outbox.begin_op().await?;
    subs.subscribe_in_op(
        &mut op,
        OwnerId(1),
        InstanceConfig::default(),
        Vec::<RoutingKey>::new(),
    )
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    op.commit().await?;

    jobs.start_poll().await?;
    publish_ping(&outbox, 1, 1).await?;
    eventually(Duration::from_secs(10), || async {
        Ok(received_for(&shared, 1).await == vec![1])
    })
    .await?;

    subs.cancel(&OwnerId(1))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // Give several sweep intervals a chance to fire and (wrongly) respawn.
    tokio::time::sleep(TEST_SWEEP_INTERVAL * 5).await;
    publish_ping(&outbox, 1, 2).await?;
    tokio::time::sleep(TEST_SWEEP_INTERVAL * 5).await;

    assert_eq!(
        received_for(&shared, 1).await,
        vec![1],
        "a cancelled key must never process events published after cancellation"
    );

    let members = subs.members().await.map_err(|e| anyhow::anyhow!("{e}"))?;
    assert!(
        members.is_empty(),
        "members() must not enumerate a cancelled key"
    );

    Ok(())
}

/// Contract — dormancy + sweep-wake: a caught-up member passivates to
/// Dormant after `linger` elapses (no live execution), and the periodic
/// sweep is what revives it to deliver events published while dormant —
/// the backstop path, independent of whether the router ever fires.
#[tokio::test]
#[file_serial]
async fn dormant_member_wakes_via_sweep_and_drains_the_backlog() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let shared = Shared::default();
    let def = TestDef {
        shared: shared.clone(),
    };
    let subs = outbox
        .register_keyed_subscriber(&mut jobs, test_config(), def)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut op = outbox.begin_op().await?;
    subs.subscribe_in_op(
        &mut op,
        OwnerId(1),
        InstanceConfig::default(),
        Vec::<RoutingKey>::new(),
    )
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    op.commit().await?;

    jobs.start_poll().await?;
    publish_ping(&outbox, 1, 1).await?;
    eventually(Duration::from_secs(10), || async {
        Ok(received_for(&shared, 1).await == vec![1])
    })
    .await?;

    // Let the member go idle past `linger`: it passivates to Dormant
    // (terminal generation, watermark retained).
    let subscription = subs
        .subscription(&OwnerId(1))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    eventually(Duration::from_secs(10), || {
        let subscription = subscription.clone();
        async move { Ok(subscription.load().await?.job_status().is_terminal()) }
    })
    .await?;
    // Watermark survives passivation (inherits_state = true).
    assert_eq!(
        subscription.load().await?.checkpoint(),
        EventSequence::from(1u64)
    );

    // Publish while Dormant — nothing is running to observe it directly.
    publish_ping(&outbox, 1, 2).await?;
    publish_ping(&outbox, 1, 3).await?;

    // The next sweep pass idempotently respawns the key and it drains the
    // backlog it missed while dormant.
    eventually(Duration::from_secs(10), || async {
        Ok(received_for(&shared, 1).await == vec![1, 2, 3])
    })
    .await?;

    Ok(())
}

/// Contract — router: a routed event wakes a Dormant member on its own,
/// well inside the sweep interval, proving the event-driven wake is the fast
/// path and not merely a side effect of the sweep also being armed. The
/// subscription registers with a routing key matching its own owner id.
#[tokio::test]
#[file_serial]
async fn router_wakes_a_dormant_member_on_a_matching_event() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let shared = Shared::default();
    let def = TestDef {
        shared: shared.clone(),
    };
    // A deliberately huge sweep interval: if the event is delivered well
    // before this could ever fire, it can only have been the router.
    let config = KeyedSubscriberConfig::new(job::JobType::new(JOB_TYPE))
        .with_linger(TEST_LINGER)
        .with_sweep_interval(Duration::from_secs(3600))
        .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL);
    let subs = outbox
        .register_keyed_subscriber(&mut jobs, config, def)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut op = outbox.begin_op().await?;
    subs.subscribe_in_op(
        &mut op,
        OwnerId(1),
        InstanceConfig::default(),
        vec![RoutingKey::from(OwnerId(1).to_string())],
    )
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    op.commit().await?;

    jobs.start_poll().await?;
    publish_ping(&outbox, 1, 1).await?;
    eventually(Duration::from_secs(10), || async {
        Ok(received_for(&shared, 1).await == vec![1])
    })
    .await?;

    let subscription = subs
        .subscription(&OwnerId(1))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    eventually(Duration::from_secs(10), || {
        let subscription = subscription.clone();
        async move { Ok(subscription.load().await?.job_status().is_terminal()) }
    })
    .await?;

    // Published while Dormant, with a sweep interval far longer than this
    // test's patience — only the router can deliver this in time.
    publish_ping(&outbox, 1, 2).await?;
    eventually(Duration::from_secs(10), || async {
        Ok(received_for(&shared, 1).await == vec![1, 2])
    })
    .await?;

    Ok(())
}

/// Contract — router, multi-routing-key matching: the reason
/// `subscriptions.routing_keys` is a set rather than a scalar.
///
/// Every other contract here registers zero or one routing key, so the
/// array-ness of the column, the `&&` overlap semantics and the
/// `$2::varchar[]` cast are all exercised at cardinality one — which is
/// exactly the cardinality at which the missing cast once compiled clean and
/// failed on every call. This registers a subscription watching TWO
/// partitions, neither of which is its own key, and wakes it through each in
/// turn:
///
/// - woken by an event matching only its SECOND routing key (so a match on
///   the first element, or an equality comparison against a scalar, would
///   not explain the wake),
/// - woken again by an event matching only its FIRST,
/// - and a second subscription watching two entirely different partitions is
///   never woken by either event — asserted on its durable checkpoint, which
///   cannot advance unless the member actually ran.
#[tokio::test]
#[file_serial]
async fn a_subscription_wakes_on_any_of_its_routing_keys_and_only_on_those() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let shared = Shared::default();
    let def = TestDef {
        shared: shared.clone(),
    };
    // As in the router contract above: a sweep interval far beyond this
    // test's patience, so any wake observed here can only be the router's.
    let config = KeyedSubscriberConfig::new(job::JobType::new(JOB_TYPE))
        .with_linger(TEST_LINGER)
        .with_sweep_interval(Duration::from_secs(3600))
        .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL);
    let subs = outbox
        .register_keyed_subscriber(&mut jobs, config, def)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // The watcher's key (10) is deliberately none of the partitions it
    // watches (7 and 8), so nothing about this can pass by conflating the
    // domain key with a routing key.
    let mut op = outbox.begin_op().await?;
    subs.subscribe_in_op(
        &mut op,
        OwnerId(10),
        InstanceConfig {
            watched: vec![7, 8],
        },
        vec![RoutingKey::from("7"), RoutingKey::from("8")],
    )
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    subs.subscribe_in_op(
        &mut op,
        OwnerId(20),
        InstanceConfig {
            watched: vec![21, 22],
        },
        vec![RoutingKey::from("21"), RoutingKey::from("22")],
    )
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    op.commit().await?;

    jobs.start_poll().await?;

    // Both are born Active; let both passivate before anything is published,
    // so every delivery below requires a wake rather than an already-running
    // member happening to see the event.
    let watcher = subs
        .subscription(&OwnerId(10))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let bystander = subs
        .subscription(&OwnerId(20))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let both_dormant = || {
        let watcher = watcher.clone();
        let bystander = bystander.clone();
        async move {
            Ok(watcher.load().await?.job_status().is_terminal()
                && bystander.load().await?.job_status().is_terminal())
        }
    };
    eventually(Duration::from_secs(10), both_dormant).await?;

    // The bystander's durable cursor at rest. It can only move if the member
    // actually runs, so it is the assertion that it was never woken —
    // strictly stronger than re-reading a status that would also read
    // Dormant after a spurious wake had come and gone.
    let bystander_checkpoint = bystander.load().await?.checkpoint();

    // Matches the watcher's SECOND routing key only.
    publish_ping(&outbox, 8, 42).await?;
    eventually(Duration::from_secs(10), || async {
        Ok(received_for(&shared, 10).await == vec![42])
    })
    .await?;

    // Let it passivate again so the next delivery is a fresh wake too.
    eventually(Duration::from_secs(10), || {
        let watcher = watcher.clone();
        async move { Ok(watcher.load().await?.job_status().is_terminal()) }
    })
    .await?;

    // Matches the watcher's FIRST routing key only.
    publish_ping(&outbox, 7, 41).await?;
    eventually(Duration::from_secs(10), || async {
        Ok(received_for(&shared, 10).await == vec![42, 41])
    })
    .await?;

    assert_eq!(
        received_for(&shared, 20).await,
        Vec::<u64>::new(),
        "a subscription must not receive events for partitions it does not watch"
    );
    assert_eq!(
        bystander.load().await?.checkpoint(),
        bystander_checkpoint,
        "a subscription watching neither routing key must never be woken: \
         its durable checkpoint cannot advance without the member running"
    );

    jobs.shutdown().await?;
    Ok(())
}

// === Staged processing: multi-transaction events with external I/O between
// stages, and the opaque resume token that makes the interim stages
// exactly-once on replay. ===

/// The subscriber's own token schema. obix stores it as opaque JSON and never
/// interprets it — this shape exists only so the test can prove round-trip.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct StageToken {
    stage: u8,
    n: u64,
}

#[derive(Clone, Default)]
struct StagedShared {
    /// Ordered trace of everything the subscriber did, across every run.
    trace: Arc<Mutex<Vec<String>>>,
    /// Return `Err` once, immediately after stage 1 committed — standing in
    /// for a crash in the external-I/O gap.
    fail_after_stage_one: Arc<AtomicBool>,
    /// Pause once, in the gap between the stages.
    hold_in_gap: Arc<Mutex<Option<chrono::DateTime<chrono::Utc>>>>,
}

struct StagedDef {
    shared: StagedShared,
}

impl SubscriptionDef<TestEvent> for StagedDef {
    type Key = OwnerId;
    type InstanceConfig = InstanceConfig;
    type Subscriber = StagedSubscriber;

    fn routing_key(
        &self,
        _event: &obix::out::PersistentOutboxEvent<TestEvent>,
    ) -> impl IntoIterator<Item = RoutingKey> {
        Vec::<RoutingKey>::new()
    }

    fn instantiate(&self, key: Self::Key, _cfg: Self::InstanceConfig) -> Self::Subscriber {
        StagedSubscriber {
            key,
            shared: self.shared.clone(),
        }
    }
}

/// Collects small `n` (landing at flush) and processes large `n` as a
/// two-stage chain, so one subscriber exercises both the batch fence and the
/// staged chain.
struct StagedSubscriber {
    key: OwnerId,
    shared: StagedShared,
}

/// Every effect lands here as a labelled row, so the test can assert both
/// *what* happened and *in what order* it committed.
async fn insert_label(
    op: &mut impl es_entity::AtomicOperation,
    label: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO test_staged_effects (label) VALUES ($1)")
        .bind(label)
        .execute(op.as_executor())
        .await?;
    Ok(())
}

impl KeyedSubscriber<TestEvent> for StagedSubscriber {
    type Batch = Vec<String>;

    async fn handle<'inv>(
        &self,
        ctx: KeyedEventCtx<'inv, Self::Batch>,
        event: &obix::out::PersistentOutboxEvent<TestEvent>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        let Some(TestEvent::Ping { owner, n }) = &event.payload else {
            return Ok(ctx.skip());
        };
        if *owner != self.key.0 {
            return Ok(ctx.skip());
        }
        if *n < 10 {
            self.shared.trace.lock().await.push(format!("collect:{n}"));
            return Ok(ctx.collect(format!("collect:{n}")));
        }

        let mut op = ctx.consume().await?;
        let staged = match op.resume::<StageToken>()? {
            // Stage 1 is already durable from an earlier attempt — skip it
            // rather than relying on it being idempotent.
            Some(token) => {
                self.shared
                    .trace
                    .lock()
                    .await
                    .push(format!("resumed:{}", token.n));
                op
            }
            None => {
                insert_label(&mut op, &format!("stage1:{n}")).await?;
                self.shared.trace.lock().await.push(format!("stage1:{n}"));
                let gap = op.proceed_with(&StageToken { stage: 1, n: *n }).await?;

                // The external-I/O gap: no transaction is open here.
                if self
                    .shared
                    .fail_after_stage_one
                    .swap(false, Ordering::SeqCst)
                {
                    self.shared.trace.lock().await.push("crash".to_string());
                    return Err("injected crash in the external-I/O gap".into());
                }
                if let Some(at) = self.shared.hold_in_gap.lock().await.take() {
                    self.shared.trace.lock().await.push("hold".to_string());
                    return Ok(gap.hold_until(at));
                }
                gap.op().await?
            }
        };

        let mut op = staged;
        insert_label(&mut op, &format!("stage2:{n}")).await?;
        self.shared.trace.lock().await.push(format!("stage2:{n}"));
        Ok(op.conclude())
    }

    async fn flush(
        &self,
        op: &mut obix::FlushOp<'_>,
        items: Self::Batch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for label in items {
            insert_label(op, &label).await?;
        }
        Ok(())
    }
}

async fn reset_staged_effects(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    sqlx::query("DROP TABLE IF EXISTS test_staged_effects")
        .execute(pool)
        .await?;
    sqlx::query(
        "CREATE TABLE test_staged_effects (id BIGSERIAL PRIMARY KEY, label VARCHAR NOT NULL)",
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Committed effects in commit order.
async fn staged_effects(pool: &sqlx::PgPool) -> anyhow::Result<Vec<String>> {
    let rows: Vec<(String,)> = sqlx::query_as("SELECT label FROM test_staged_effects ORDER BY id")
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().map(|r| r.0).collect())
}

async fn trace_of(shared: &StagedShared) -> Vec<String> {
    shared.trace.lock().await.clone()
}

async fn init_staged_outbox(pool: &sqlx::PgPool) -> anyhow::Result<Outbox<TestEvent, TestTables>> {
    helpers::wipeout_outbox_tables(pool).await?;
    wipeout_keyed_subscriber_job_tables(pool, STAGED_JOB_TYPE).await?;
    wipeout_subscriptions(pool, STAGED_JOB_TYPE).await?;
    reset_staged_effects(pool).await?;

    Ok(Outbox::<TestEvent, TestTables>::init(
        pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?)
}

fn staged_config() -> KeyedSubscriberConfig {
    KeyedSubscriberConfig::new(job::JobType::new(STAGED_JOB_TYPE))
        .with_linger(TEST_LINGER)
        .with_sweep_interval(TEST_SWEEP_INTERVAL)
        .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL)
}

/// Contract — staged fence: the pending batch (its collected items AND its
/// checkpoint) lands *before* stage 1's op exists, exactly as the
/// single-stage consume fence always did. Asserted on commit order, so a
/// stage-1 write that leaked ahead of the fence would be visible.
#[tokio::test]
#[file_serial]
async fn staged_entry_lands_collected_items_before_stage_one() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_staged_outbox(&pool).await?;

    let shared = StagedShared::default();
    let subs = outbox
        .register_keyed_subscriber(
            &mut jobs,
            staged_config(),
            StagedDef {
                shared: shared.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut op = outbox.begin_op().await?;
    subs.subscribe_in_op(
        &mut op,
        OwnerId(1),
        InstanceConfig::default(),
        Vec::<RoutingKey>::new(),
    )
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    op.commit().await?;

    jobs.start_poll().await?;
    publish_ping(&outbox, 1, 1).await?;
    publish_ping(&outbox, 1, 2).await?;
    publish_ping(&outbox, 1, 11).await?;

    eventually(Duration::from_secs(10), || async {
        Ok(staged_effects(&pool).await?.len() == 4)
    })
    .await?;

    assert_eq!(
        staged_effects(&pool).await?,
        vec!["collect:1", "collect:2", "stage1:11", "stage2:11"],
        "collected items must land before the staged event's first stage"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// Contract — interim durability and chain error: a failure in the gap
/// between stages leaves stage 1's writes committed and the cursor unmoved,
/// so the event is re-read; the resume token is what lets the retry skip the
/// stage instead of redoing it. Stage 1 must appear exactly once.
#[tokio::test]
#[file_serial]
async fn a_crash_between_stages_keeps_stage_one_and_resumes_from_the_token() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_staged_outbox(&pool).await?;

    let shared = StagedShared::default();
    shared.fail_after_stage_one.store(true, Ordering::SeqCst);
    let subs = outbox
        .register_keyed_subscriber(
            &mut jobs,
            staged_config(),
            StagedDef {
                shared: shared.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut op = outbox.begin_op().await?;
    subs.subscribe_in_op(
        &mut op,
        OwnerId(1),
        InstanceConfig::default(),
        Vec::<RoutingKey>::new(),
    )
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    op.commit().await?;

    jobs.start_poll().await?;
    publish_ping(&outbox, 1, 11).await?;

    eventually(Duration::from_secs(20), || async {
        Ok(staged_effects(&pool).await? == vec!["stage1:11", "stage2:11"])
    })
    .await?;

    // Stage 1 ran once and survived the crash; the replay saw the token and
    // went straight to stage 2 rather than re-running stage 1.
    let trace = trace_of(&shared).await;
    assert_eq!(
        trace,
        vec!["stage1:11", "crash", "resumed:11", "stage2:11"],
        "expected stage 1 to commit, crash, then resume from the token"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// Contract — token lifetime: a hold is part of processing the event, so the
/// token survives it (the cursor is still parked before the event). Once
/// `conclude` advances the cursor the token is gone, and the next event
/// starts fresh — proving the slot is scoped to one event, not to the
/// subscription.
#[tokio::test]
#[file_serial]
async fn the_resume_token_survives_a_hold_and_does_not_outlive_its_event() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_staged_outbox(&pool).await?;

    let shared = StagedShared::default();
    *shared.hold_in_gap.lock().await =
        Some(chrono::Utc::now() + chrono::Duration::milliseconds(300));
    let subs = outbox
        .register_keyed_subscriber(
            &mut jobs,
            staged_config(),
            StagedDef {
                shared: shared.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut op = outbox.begin_op().await?;
    subs.subscribe_in_op(
        &mut op,
        OwnerId(1),
        InstanceConfig::default(),
        Vec::<RoutingKey>::new(),
    )
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    op.commit().await?;

    jobs.start_poll().await?;
    publish_ping(&outbox, 1, 11).await?;

    eventually(Duration::from_secs(20), || async {
        Ok(staged_effects(&pool).await? == vec!["stage1:11", "stage2:11"])
    })
    .await?;
    assert_eq!(
        trace_of(&shared).await,
        vec!["stage1:11", "hold", "resumed:11", "stage2:11"],
        "the token must survive the hold and be readable when processing resumes"
    );

    // A second staged event: its own sequence, so the concluded event's
    // token is not visible to it — it must run stage 1 from scratch.
    publish_ping(&outbox, 1, 12).await?;
    eventually(Duration::from_secs(20), || async {
        Ok(staged_effects(&pool).await?.len() == 4)
    })
    .await?;
    assert_eq!(
        staged_effects(&pool).await?,
        vec!["stage1:11", "stage2:11", "stage1:12", "stage2:12"]
    );
    assert_eq!(
        trace_of(&shared).await,
        vec![
            "stage1:11",
            "hold",
            "resumed:11",
            "stage2:11",
            "stage1:12",
            "stage2:12"
        ],
        "a concluded event's token must not be visible to the next event"
    );

    jobs.shutdown().await?;
    Ok(())
}
