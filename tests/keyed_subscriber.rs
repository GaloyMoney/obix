//! Contracts for keyed subscribers: per-entity outbox consumers with
//! wake-on-demand.

mod helpers;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use obix::{
    EventSequence, Handled, KeyedEventCtx, KeyedSubscriber, KeyedSubscriberConfig, MailboxConfig,
    SubscriptionDef, WakeKey, WakeKeys, out::Outbox,
};
use serde::{Deserialize, Serialize};
use serial_test::file_serial;
use tokio::sync::Mutex;

use helpers::{
    KEYED_WAKER_JOB_TYPE, TestTables, init_pool, wipeout_keyed_subscriber_job_tables,
    wipeout_subscriptions,
};

const JOB_TYPE: &str = "test-keyed-subscriber";
/// A second subscriber type on the same outbox, for the shared-waker contract.
const SECOND_JOB_TYPE: &str = "test-keyed-subscriber-2";
const STAGED_JOB_TYPE: &str = "test-keyed-staged";

/// Short enough for dormancy contracts to land inside a test's patience.
const TEST_LINGER: Duration = Duration::from_millis(150);
const TEST_CHECKPOINT_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum TestEvent {
    Ping { owner: u64, n: u64 },
}

/// The domain key: an owning entity id, round-tripped through the subscriptions
/// table by `Display`/`FromStr`.
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
    /// Owners this subscription records for in addition to its own key;
    /// non-empty only where wake keys and the domain key deliberately differ.
    #[serde(default)]
    watched: Vec<u64>,
    /// Work per event, in milliseconds. Non-zero only where the handler must be
    /// slower than its own `linger`.
    #[serde(default)]
    work_ms: u64,
}

/// Shared across the runner's many fresh `instantiate` calls, so a test can see
/// what happened over every run, wake and retry.
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

    fn wake_keys(
        &self,
        event: &obix::out::PersistentOutboxEvent<TestEvent>,
    ) -> impl IntoIterator<Item = WakeKey> {
        match &event.payload {
            Some(TestEvent::Ping { owner, .. }) => vec![WakeKey::from(owner.to_string())],
            None => vec![],
        }
    }

    fn instantiate(&self, key: Self::Key, cfg: Self::InstanceConfig) -> Self::Subscriber {
        RecordingSubscriber {
            key,
            watched: cfg.watched,
            work_ms: cfg.work_ms,
            shared: self.shared.clone(),
        }
    }
}

/// A second subscriber type whose classification is namespaced: the same event
/// yields `p:{owner}` here where [`TestDef`] yields `{owner}`.
struct PrefixedDef {
    shared: Shared,
}

impl SubscriptionDef<TestEvent> for PrefixedDef {
    type Key = OwnerId;
    type InstanceConfig = InstanceConfig;
    type Subscriber = RecordingSubscriber;

    fn wake_keys(
        &self,
        event: &obix::out::PersistentOutboxEvent<TestEvent>,
    ) -> impl IntoIterator<Item = WakeKey> {
        match &event.payload {
            Some(TestEvent::Ping { owner, .. }) => vec![WakeKey::from(format!("p:{owner}"))],
            None => vec![],
        }
    }

    fn instantiate(&self, key: Self::Key, cfg: Self::InstanceConfig) -> Self::Subscriber {
        RecordingSubscriber {
            key,
            watched: cfg.watched,
            work_ms: cfg.work_ms,
            shared: self.shared.clone(),
        }
    }
}

/// Records every `Ping` addressed to its own key. A subscriber sees the WHOLE
/// stream — wake keys decide who runs, not who receives — so it filters.
struct RecordingSubscriber {
    key: OwnerId,
    /// Extra owners to record for beyond `key`, from the instance config.
    watched: Vec<u64>,
    work_ms: u64,
    shared: Shared,
}

impl KeyedSubscriber<TestEvent> for RecordingSubscriber {
    type Batch = ();

    async fn handle<'inv>(
        &self,
        ctx: KeyedEventCtx<'inv, ()>,
        event: &obix::EventDelivery<TestEvent>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        let Some(TestEvent::Ping { owner, n }) = &event.payload else {
            return Ok(ctx.skip());
        };
        if *owner != self.key.0 && !self.watched.contains(owner) {
            return Ok(ctx.skip());
        }

        if let Some(at) = self.shared.hold_once.lock().await.remove(&self.key.0) {
            return Ok(ctx.pause_until(at));
        }

        if self.work_ms > 0 {
            tokio::time::sleep(Duration::from_millis(self.work_ms)).await;
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

/// A small cache, so the waker's catch-up threshold (three quarters of it) is
/// reachable within a test.
async fn init_outbox_with_cache_size(
    pool: &sqlx::PgPool,
    event_cache_size: usize,
) -> anyhow::Result<Outbox<TestEvent, TestTables>> {
    helpers::wipeout_outbox_tables(pool).await?;
    wipeout_keyed_subscriber_job_tables(pool, JOB_TYPE).await?;
    wipeout_subscriptions(pool, JOB_TYPE).await?;

    Ok(Outbox::<TestEvent, TestTables>::init(
        pool,
        MailboxConfig::builder()
            .event_cache_size(event_cache_size)
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?)
}

fn test_config() -> KeyedSubscriberConfig {
    KeyedSubscriberConfig::new(job::JobType::new(JOB_TYPE))
        .with_linger(TEST_LINGER)
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

/// Publish several pings in ONE transaction, so they become visible together
/// and a single waker batch covers all of them.
async fn publish_ping_burst(
    outbox: &Outbox<TestEvent, TestTables>,
    owner: u64,
    ns: std::ops::Range<u64>,
) -> anyhow::Result<()> {
    let mut op = outbox.begin_op().await?;
    for n in ns {
        outbox
            .publish_persisted_in_op(&mut op, TestEvent::Ping { owner, n })
            .await?;
    }
    op.commit().await?;
    Ok(())
}

/// The one partition a subscription's own key names, so traffic addressed to any
/// other owner never wakes it.
fn wake_keys_for(owner: OwnerId) -> WakeKey {
    WakeKey::from(owner.to_string())
}

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

/// An empty wake-key set can never be matched (`{} && {anything}` is false), and
/// only runtime-built keys can reach one — the type rejects the rest.
#[test]
fn an_empty_wake_key_set_cannot_be_built() {
    let from_data: Vec<WakeKey> = vec![];
    let err = WakeKeys::try_from(from_data).expect_err("empty must not convert");
    assert!(matches!(err, obix::SubscribeError::EmptyWakeKeys));

    let ok = WakeKeys::try_from(vec![WakeKey::from("7")]).expect("one key is a valid set");
    assert_eq!(ok.len(), 1);
}

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
        wake_keys_for(OwnerId(1)),
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
        wake_keys_for(OwnerId(1)),
    )
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    subs.subscribe_in_op(
        &mut op,
        OwnerId(2),
        InstanceConfig::default(),
        wake_keys_for(OwnerId(2)),
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

#[tokio::test]
#[file_serial]
async fn pause_until_parks_the_cursor_and_redelivers_on_resume() -> anyhow::Result<()> {
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
        wake_keys_for(OwnerId(1)),
    )
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    op.commit().await?;

    // A hold on the first event this key sees, well past the checkpoint interval
    // so a premature advance would be caught.
    let pause_until = chrono::Utc::now() + chrono::Duration::milliseconds(600);
    shared.hold_once.lock().await.insert(1, pause_until);

    jobs.start_poll().await?;
    publish_ping(&outbox, 1, 7).await?;

    let subscription = subs
        .subscription(&OwnerId(1))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

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

    // Once the hold expires the SAME event resumes, delivered exactly once.
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

/// A pause is cut short by traffic, never by the match for the paused event
/// itself: with the pause an hour out, a later event still wakes the member.
#[tokio::test]
#[file_serial]
async fn traffic_behind_a_paused_event_wakes_the_member_early() -> anyhow::Result<()> {
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
        wake_keys_for(OwnerId(1)),
    )
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    op.commit().await?;

    let pause_until = chrono::Utc::now() + chrono::Duration::hours(1);
    shared.hold_once.lock().await.insert(1, pause_until);

    jobs.start_poll().await?;
    publish_ping(&outbox, 1, 7).await?;

    let subscription = subs
        .subscription(&OwnerId(1))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // The only wake so far is the match for the paused event itself, which must
    // not have cut the pause short.
    eventually(Duration::from_secs(5), || {
        let subscription = subscription.clone();
        async move { Ok(!subscription.load().await?.job_status().is_terminal()) }
    })
    .await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        received_for(&shared, 1).await,
        Vec::<u64>::new(),
        "the match for the paused event must not wake the member"
    );

    // Traffic behind the paused event: wakes the member an hour early.
    publish_ping(&outbox, 1, 8).await?;
    eventually(Duration::from_secs(10), || async {
        Ok(received_for(&shared, 1).await == vec![7, 8])
    })
    .await?;

    jobs.shutdown().await?;

    Ok(())
}

/// Row deletion is the tombstone, and every wake path resolves through the
/// `subscriptions` table. Cancellation takes effect at the end of the current run.
#[tokio::test]
#[file_serial]
async fn cancel_stops_delivery_and_no_wake_revives_it() -> anyhow::Result<()> {
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
        wake_keys_for(OwnerId(1)),
    )
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    subs.subscribe_in_op(
        &mut op,
        OwnerId(2),
        InstanceConfig::default(),
        wake_keys_for(OwnerId(2)),
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

    // Captured before cancelling and deliberately not re-resolved: this
    // generation going terminal is the signal that the run has ended.
    let cancelled = subs
        .subscription(&OwnerId(1))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    subs.cancel(&OwnerId(1))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    eventually(Duration::from_secs(10), || {
        let cancelled = cancelled.clone();
        async move { Ok(cancelled.load().await?.job_status().is_terminal()) }
    })
    .await?;

    // Addressed to the cancelled key's own wake key, then one for the bystander
    // whose arrival proves the waker got past both.
    publish_ping(&outbox, 1, 2).await?;
    publish_ping(&outbox, 2, 3).await?;
    eventually(Duration::from_secs(10), || async {
        Ok(received_for(&shared, 2).await == vec![3])
    })
    .await?;

    assert_eq!(
        received_for(&shared, 1).await,
        vec![1],
        "a cancelled key must never process events published after cancellation"
    );

    // Row absence is the tombstone, so assert on the table itself.
    let live: Vec<String> = sqlx::query_scalar!(
        "SELECT key FROM subscriptions WHERE subscriber_type = $1 ORDER BY key",
        JOB_TYPE
    )
    .fetch_all(&pool)
    .await?;
    assert_eq!(
        live,
        vec![OwnerId(2).to_string()],
        "cancel must delete the subscription row and leave the bystander's"
    );

    Ok(())
}

/// A passivated member retains its watermark, so its next wake drains the whole
/// backlog accumulated while Dormant rather than resuming at the wake point.
#[tokio::test]
#[file_serial]
async fn dormant_member_retains_its_watermark_and_drains_the_backlog() -> anyhow::Result<()> {
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
        wake_keys_for(OwnerId(1)),
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

    // Idle past `linger`, so the member passivates to Dormant.
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

    // Published while Dormant, so nothing is running to observe it directly.
    publish_ping(&outbox, 1, 2).await?;
    publish_ping(&outbox, 1, 3).await?;

    eventually(Duration::from_secs(10), || async {
        Ok(received_for(&shared, 1).await == vec![1, 2, 3])
    })
    .await?;

    Ok(())
}

/// A matching event wakes a Dormant member on its own: the subscription's wake
/// key is its owner id, and the event published while Dormant carries it.
#[tokio::test]
#[file_serial]
async fn the_waker_wakes_a_dormant_member_on_a_matching_event() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let shared = Shared::default();
    let def = TestDef {
        shared: shared.clone(),
    };
    let config = KeyedSubscriberConfig::new(job::JobType::new(JOB_TYPE))
        .with_linger(TEST_LINGER)
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
        WakeKey::from(OwnerId(1).to_string()),
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

    // Published while Dormant, and one event is nowhere near the cache depth the
    // catch-up path triggers on, so only a wake-key match can deliver it.
    publish_ping(&outbox, 1, 2).await?;
    eventually(Duration::from_secs(10), || async {
        Ok(received_for(&shared, 1).await == vec![1, 2])
    })
    .await?;

    Ok(())
}

/// A Dormant member drifting toward the bottom of the cache is woken with no
/// wake key matching. Asserted on the checkpoint: it skips all this traffic.
#[tokio::test]
#[file_serial]
async fn a_member_drifting_out_of_the_cache_is_woken_to_catch_up() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    // Small enough that a handful of events crosses the threshold.
    let outbox = init_outbox_with_cache_size(&pool, 8).await?;

    let shared = Shared::default();
    let def = TestDef {
        shared: shared.clone(),
    };
    let config = KeyedSubscriberConfig::new(job::JobType::new(JOB_TYPE))
        .with_linger(TEST_LINGER)
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
        wake_keys_for(OwnerId(1)),
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
    let dormant_at = subscription.load().await?.checkpoint();

    // Traffic for a partition this member does not watch: no wake key matches,
    // so only the accumulated drift can be the reason to wake.
    for n in 0..12 {
        publish_ping(&outbox, 2, n).await?;
    }

    // Re-resolved each poll: a handle observes the generation it was minted for,
    // and a catch-up wake starts a new one.
    let subs_probe = subs.clone();
    eventually(Duration::from_secs(10), || {
        let subs_probe = subs_probe.clone();
        async move {
            let subscription = subs_probe
                .subscription(&OwnerId(1))
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            Ok(subscription.load().await?.checkpoint() > dormant_at)
        }
    })
    .await?;

    assert_eq!(
        received_for(&shared, 1).await,
        vec![1],
        "a catch-up wake must not deliver another key's events"
    );

    Ok(())
}

/// Idle must mean "deadline due AND nothing ready": a 5ms handler against a 1ms
/// `linger`, and a wake key nothing published here classifies to.
#[tokio::test]
#[file_serial]
async fn a_ready_backlog_defeats_the_linger_deadline() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    // Default cache size, so 24 events cannot reach the catch-up threshold
    // either: the ready backlog is the only path to delivery.
    let outbox = init_outbox(&pool).await?;

    let shared = Shared::default();
    let config = KeyedSubscriberConfig::new(job::JobType::new(JOB_TYPE))
        .with_linger(Duration::from_millis(1))
        .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL);
    let subs = outbox
        .register_keyed_subscriber(
            &mut jobs,
            config,
            TestDef {
                shared: shared.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut op = outbox.begin_op().await?;
    subs.subscribe_in_op(
        &mut op,
        OwnerId(1),
        InstanceConfig {
            work_ms: 5,
            ..Default::default()
        },
        WakeKey::from("a-partition-nothing-publishes-to"),
    )
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    op.commit().await?;

    jobs.start_poll().await?;
    publish_ping_burst(&outbox, 1, 0..24).await?;

    let expected: Vec<u64> = (0..24).collect();
    eventually(Duration::from_secs(10), || {
        let expected = expected.clone();
        let shared = shared.clone();
        async move { Ok(received_for(&shared, 1).await == expected) }
    })
    .await?;

    Ok(())
}

/// Rows of an unregistered subscriber type have no runner, so they sit at their
/// birth frontier and would win every lag-ordered scan if not filtered in SQL.
#[tokio::test]
#[file_serial]
async fn an_unregistered_type_cannot_starve_the_catch_up_scan() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox_with_cache_size(&pool, 8).await?;
    sqlx::query("DELETE FROM subscriptions WHERE subscriber_type = 'retired-type'")
        .execute(&pool)
        .await?;

    // 64 = CATCH_UP_WAKE_LIMIT: exactly enough to fill a pass on their own.
    for i in 0..64 {
        sqlx::query(
            "INSERT INTO subscriptions
               (subscriber_type, key, wake_keys, instance_config, start_after, checkpoint)
             VALUES ('retired-type', $1, ARRAY['x']::varchar[], '{}'::jsonb, 0, 0)",
        )
        .bind(i.to_string())
        .execute(&pool)
        .await?;
    }

    let shared = Shared::default();
    let config = KeyedSubscriberConfig::new(job::JobType::new(JOB_TYPE))
        .with_linger(TEST_LINGER)
        .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL);
    let subs = outbox
        .register_keyed_subscriber(
            &mut jobs,
            config,
            TestDef {
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
        wake_keys_for(OwnerId(1)),
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
    let dormant_at = subscription.load().await?.checkpoint();

    // Traffic for a partition this member does not watch, so only the catch-up
    // scan can revive it — and only if the retired rows do not eat the pass.
    for n in 0..12 {
        publish_ping(&outbox, 2, n).await?;
    }

    let subs_probe = subs.clone();
    eventually(Duration::from_secs(10), || {
        let subs_probe = subs_probe.clone();
        async move {
            let subscription = subs_probe
                .subscription(&OwnerId(1))
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            Ok(subscription.load().await?.checkpoint() > dormant_at)
        }
    })
    .await?;

    Ok(())
}

/// One waker per outbox, not per subscriber type: exactly one job row, and it
/// classifies through every registered type's `wake_keys`.
#[tokio::test]
#[file_serial]
async fn two_subscriber_types_share_one_waker() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;
    wipeout_keyed_subscriber_job_tables(&pool, SECOND_JOB_TYPE).await?;
    wipeout_subscriptions(&pool, SECOND_JOB_TYPE).await?;

    let first_shared = Shared::default();
    let second_shared = Shared::default();
    let first = outbox
        .register_keyed_subscriber(
            &mut jobs,
            test_config(),
            TestDef {
                shared: first_shared.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let second = outbox
        .register_keyed_subscriber(
            &mut jobs,
            KeyedSubscriberConfig::new(job::JobType::new(SECOND_JOB_TYPE))
                .with_linger(TEST_LINGER)
                .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL),
            TestDef {
                shared: second_shared.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut op = outbox.begin_op().await?;
    first
        .subscribe_in_op(
            &mut op,
            OwnerId(1),
            InstanceConfig::default(),
            wake_keys_for(OwnerId(1)),
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    second
        .subscribe_in_op(
            &mut op,
            OwnerId(1),
            InstanceConfig::default(),
            wake_keys_for(OwnerId(1)),
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    op.commit().await?;

    jobs.start_poll().await?;
    publish_ping(&outbox, 1, 1).await?;
    eventually(Duration::from_secs(10), || async {
        Ok(received_for(&first_shared, 1).await == vec![1]
            && received_for(&second_shared, 1).await == vec![1])
    })
    .await?;

    let wakers: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) AS \"count!\" FROM jobs WHERE job_type = $1",
        KEYED_WAKER_JOB_TYPE
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        wakers, 1,
        "two subscriber types on one outbox must share a single waker job"
    );

    // Both passivate, then a single event revives both through the one waker.
    let first_sub = first
        .subscription(&OwnerId(1))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let second_sub = second
        .subscription(&OwnerId(1))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    eventually(Duration::from_secs(10), || {
        let (first_sub, second_sub) = (first_sub.clone(), second_sub.clone());
        async move {
            Ok(first_sub.load().await?.job_status().is_terminal()
                && second_sub.load().await?.job_status().is_terminal())
        }
    })
    .await?;

    publish_ping(&outbox, 1, 2).await?;
    eventually(Duration::from_secs(10), || async {
        Ok(received_for(&first_shared, 1).await == vec![1, 2]
            && received_for(&second_shared, 1).await == vec![1, 2])
    })
    .await?;

    Ok(())
}

/// A wake key belongs to its own subscriber type: the waker's single query must
/// keep `(subscriber_type, wake key)` paired rather than unioning them.
#[tokio::test]
#[file_serial]
async fn a_wake_key_matches_only_within_its_own_subscriber_type() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;
    wipeout_keyed_subscriber_job_tables(&pool, SECOND_JOB_TYPE).await?;
    wipeout_subscriptions(&pool, SECOND_JOB_TYPE).await?;

    let plain_shared = Shared::default();
    let prefixed_shared = Shared::default();
    let plain = outbox
        .register_keyed_subscriber(
            &mut jobs,
            test_config(),
            TestDef {
                shared: plain_shared.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let prefixed = outbox
        .register_keyed_subscriber(
            &mut jobs,
            KeyedSubscriberConfig::new(job::JobType::new(SECOND_JOB_TYPE))
                .with_linger(TEST_LINGER)
                .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL),
            PrefixedDef {
                shared: prefixed_shared.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut op = outbox.begin_op().await?;
    plain
        .subscribe_in_op(
            &mut op,
            OwnerId(1),
            InstanceConfig::default(),
            wake_keys_for(OwnerId(1)),
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    // Deliberately the *other* type's wake key: `PrefixedDef` classifies this
    // event to "p:1" and never to "1".
    prefixed
        .subscribe_in_op(
            &mut op,
            OwnerId(1),
            InstanceConfig::default(),
            WakeKey::from("1"),
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    op.commit().await?;

    // Both are Active on subscribe and a live member reads the whole stream, so
    // the first event lands in both; wake keys only matter from here on.
    jobs.start_poll().await?;
    publish_ping(&outbox, 1, 1).await?;
    eventually(Duration::from_secs(10), || async {
        Ok(received_for(&plain_shared, 1).await == vec![1]
            && received_for(&prefixed_shared, 1).await == vec![1])
    })
    .await?;

    let plain_sub = plain
        .subscription(&OwnerId(1))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let prefixed_sub = prefixed
        .subscription(&OwnerId(1))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    eventually(Duration::from_secs(10), || {
        let (plain_sub, prefixed_sub) = (plain_sub.clone(), prefixed_sub.clone());
        async move {
            Ok(plain_sub.load().await?.job_status().is_terminal()
                && prefixed_sub.load().await?.job_status().is_terminal())
        }
    })
    .await?;

    publish_ping(&outbox, 1, 2).await?;
    eventually(Duration::from_secs(10), || async {
        Ok(received_for(&plain_shared, 1).await == vec![1, 2])
    })
    .await?;

    assert_eq!(
        received_for(&prefixed_shared, 1).await,
        vec![1],
        "a subscription must not be woken by a key its own type never classifies to"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// Multi-wake-key matching: a subscription watching TWO partitions, neither its
/// own key, is woken through each in turn, and a bystander through neither.
#[tokio::test]
#[file_serial]
async fn a_subscription_wakes_on_any_of_its_wake_keys_and_only_on_those() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let shared = Shared::default();
    let def = TestDef {
        shared: shared.clone(),
    };
    let config = KeyedSubscriberConfig::new(job::JobType::new(JOB_TYPE))
        .with_linger(TEST_LINGER)
        .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL);
    let subs = outbox
        .register_keyed_subscriber(&mut jobs, config, def)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // The watcher's key (10) is none of the partitions it watches (7 and 8), so
    // conflating the domain key with a wake key cannot pass.
    let mut op = outbox.begin_op().await?;
    subs.subscribe_in_op(
        &mut op,
        OwnerId(10),
        InstanceConfig {
            watched: vec![7, 8],
            ..Default::default()
        },
        WakeKeys::new(WakeKey::from("7")).and(WakeKey::from("8")),
    )
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    subs.subscribe_in_op(
        &mut op,
        OwnerId(20),
        InstanceConfig {
            watched: vec![21, 22],
            ..Default::default()
        },
        WakeKeys::new(WakeKey::from("21")).and(WakeKey::from("22")),
    )
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    op.commit().await?;

    jobs.start_poll().await?;

    // Both passivate before anything is published, so every delivery below
    // requires a wake rather than an already-running member.
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

    // The bystander's durable cursor at rest: it can only move if the member ran,
    // and it is re-resolved at the end because a wake starts a new generation.
    let bystander_checkpoint = bystander.load().await?.checkpoint();

    // Matches the watcher's SECOND wake key only.
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

    // Matches the watcher's FIRST wake key only.
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
        subs.subscription(&OwnerId(20))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .load()
            .await?
            .checkpoint(),
        bystander_checkpoint,
        "a subscription watching neither wake key must never be woken: \
         its durable checkpoint cannot advance without the member running"
    );

    jobs.shutdown().await?;
    Ok(())
}

// Staged processing: interim stages are durable but replayed, so a crash or hold
// in the gap re-handles the event from its first stage.

#[derive(Clone, Default)]
struct StagedShared {
    /// Ordered trace of everything the subscriber did, across every run.
    trace: Arc<Mutex<Vec<String>>>,
    /// Return `Err` once, right after stage 1 committed: a crash in the gap.
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

    /// Classifies nothing, so a wake cannot disturb the holds and mid-chain
    /// crashes these contracts drive directly.
    fn wake_keys(
        &self,
        _event: &obix::out::PersistentOutboxEvent<TestEvent>,
    ) -> impl IntoIterator<Item = WakeKey> {
        Vec::<WakeKey>::new()
    }

    fn instantiate(&self, key: Self::Key, _cfg: Self::InstanceConfig) -> Self::Subscriber {
        StagedSubscriber {
            key,
            shared: self.shared.clone(),
        }
    }
}

/// Collects small `n` (landing at flush) and processes large `n` as a two-stage
/// chain, so one subscriber exercises both.
struct StagedSubscriber {
    key: OwnerId,
    shared: StagedShared,
}

/// Every effect lands here as a labelled row, so a test can assert what happened
/// and in what order it committed.
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
        event: &obix::EventDelivery<TestEvent>,
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
        insert_label(&mut op, &format!("stage1:{n}")).await?;
        self.shared.trace.lock().await.push(format!("stage1:{n}"));
        let gap = op.suspend().await?;

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
            return Ok(gap.pause_until(at));
        }

        let mut op = gap.resume().await?;
        insert_label(&mut op, &format!("stage2:{n}")).await?;
        self.shared.trace.lock().await.push(format!("stage2:{n}"));
        Ok(op.commit())
    }

    async fn flush(
        &self,
        op: &mut obix::FlushOp<'_, obix::InsertOrder>,
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
        .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL)
}

/// The pending batch, items and checkpoint, lands before stage 1's op exists;
/// asserted on commit order, so a leaked stage-1 write would be visible.
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
        wake_keys_for(OwnerId(1)),
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

/// A failure in the gap leaves stage 1's writes committed and the cursor unmoved,
/// so the event is handled again from its first stage and stage 1 lands twice.
#[tokio::test]
#[file_serial]
async fn a_crash_between_stages_keeps_stage_one_and_replays_the_event() -> anyhow::Result<()> {
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
        wake_keys_for(OwnerId(1)),
    )
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    op.commit().await?;

    jobs.start_poll().await?;
    publish_ping(&outbox, 1, 11).await?;

    eventually(Duration::from_secs(20), || async {
        Ok(staged_effects(&pool).await? == vec!["stage1:11", "stage1:11", "stage2:11"])
    })
    .await?;

    // Stage 1 survived the crash, and the replay landed it again before stage 2.
    let trace = trace_of(&shared).await;
    assert_eq!(
        trace,
        vec!["stage1:11", "crash", "stage1:11", "stage2:11"],
        "expected stage 1 to commit, crash, then replay from the first stage"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// A hold in the gap is part of processing the event: the cursor stays parked, and
/// once the hold ends the event is handled again from its first stage.
#[tokio::test]
#[file_serial]
async fn a_hold_between_stages_replays_the_event_from_its_first_stage() -> anyhow::Result<()> {
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
        wake_keys_for(OwnerId(1)),
    )
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    op.commit().await?;

    jobs.start_poll().await?;
    publish_ping(&outbox, 1, 11).await?;

    eventually(Duration::from_secs(20), || async {
        Ok(staged_effects(&pool).await? == vec!["stage1:11", "stage1:11", "stage2:11"])
    })
    .await?;
    assert_eq!(
        trace_of(&shared).await,
        vec!["stage1:11", "hold", "stage1:11", "stage2:11"],
        "the hold must end with the event handled again from its first stage"
    );

    // A second staged event runs its own chain, unaffected by the first.
    publish_ping(&outbox, 1, 12).await?;
    eventually(Duration::from_secs(20), || async {
        Ok(staged_effects(&pool).await?.len() == 5)
    })
    .await?;
    assert_eq!(
        staged_effects(&pool).await?,
        vec![
            "stage1:11",
            "stage1:11",
            "stage2:11",
            "stage1:12",
            "stage2:12"
        ]
    );
    assert_eq!(
        trace_of(&shared).await,
        vec![
            "stage1:11",
            "hold",
            "stage1:11",
            "stage2:11",
            "stage1:12",
            "stage2:12"
        ],
        "the second event must start its own chain from its first stage"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// Awaiting `MailboxTables`'s opaque futures inside `&self` methods makes `Send`
/// higher-ranked and breaks `tokio::spawn` (rust-lang/rust#100013).
#[tokio::test]
#[file_serial]
async fn the_subscriptions_capability_survives_tokio_spawn() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let shared = Shared::default();
    let subs = outbox
        .register_keyed_subscriber(
            &mut jobs,
            test_config(),
            TestDef {
                shared: shared.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let spawned_outbox = outbox.clone();
    let spawned_subs = subs.clone();
    tokio::spawn(async move {
        let mut op = spawned_outbox.begin_op().await?;
        spawned_subs
            .subscribe_in_op(
                &mut op,
                OwnerId(1),
                InstanceConfig::default(),
                wake_keys_for(OwnerId(1)),
            )
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        op.commit().await?;

        spawned_subs
            .subscription(&OwnerId(1))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        spawned_subs
            .cancel(&OwnerId(1))
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok::<_, anyhow::Error>(())
    })
    .await??;

    jobs.shutdown().await?;
    Ok(())
}

/// Dormancy is about THIS MEMBER's idleness, not the stream's: a deadline that
/// restarted on every arriving event would never passivate a busy outbox.
#[tokio::test]
#[file_serial]
async fn a_member_passivates_while_the_shared_stream_stays_busy() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let shared = Shared::default();
    let config = KeyedSubscriberConfig::new(job::JobType::new(JOB_TYPE))
        .with_linger(TEST_LINGER)
        .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL);
    let subs = outbox
        .register_keyed_subscriber(
            &mut jobs,
            config,
            TestDef {
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
        wake_keys_for(OwnerId(1)),
    )
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    op.commit().await?;

    jobs.start_poll().await?;

    // Traffic for a key this member does not own, faster than `linger` and for
    // longer than `linger`.
    let stop = Arc::new(AtomicBool::new(false));
    let published = Arc::new(AtomicUsize::new(0));
    let publisher = {
        let outbox = outbox.clone();
        let stop = stop.clone();
        let published = published.clone();
        tokio::spawn(async move {
            let mut n = 0u64;
            while !stop.load(Ordering::SeqCst) {
                n += 1;
                if publish_ping(&outbox, 999, n).await.is_err() {
                    break;
                }
                published.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
        })
    };

    let subscription = subs
        .subscription(&OwnerId(1))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let passivated = eventually(Duration::from_secs(10), || {
        let subscription = subscription.clone();
        async move { Ok(subscription.load().await?.job_status().is_terminal()) }
    })
    .await;

    // Captured BEFORE stopping the publisher, so a pass cannot be explained by
    // the traffic having dried up on its own.
    let published_while_waiting = published.load(Ordering::SeqCst);
    stop.store(true, Ordering::SeqCst);
    let _ = publisher.await;
    passivated?;

    assert!(
        published_while_waiting >= 5,
        "the stream must still have been busy when the member passivated, \
         published: {published_while_waiting}"
    );
    assert_eq!(
        received_for(&shared, 1).await,
        Vec::<u64>::new(),
        "the member owns none of this traffic — it should have skipped all of it"
    );

    jobs.shutdown().await?;
    Ok(())
}

/// `linger: Duration::MAX` is always-on: `Instant + Duration::MAX` panics, so an
/// un-addable linger must leave the deadline unarmed instead.
#[tokio::test]
#[file_serial]
async fn always_on_linger_delivers_and_never_passivates() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let shared = Shared::default();
    let config = KeyedSubscriberConfig::new(job::JobType::new(JOB_TYPE))
        .with_linger(Duration::MAX)
        .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL);
    let subs = outbox
        .register_keyed_subscriber(
            &mut jobs,
            config,
            TestDef {
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
        wake_keys_for(OwnerId(1)),
    )
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    op.commit().await?;

    jobs.start_poll().await?;

    // Delivery at all is the panic check: an overflowing deadline would kill the
    // run before it ever read the stream.
    publish_ping(&outbox, 1, 1).await?;
    eventually(Duration::from_secs(10), || async {
        Ok(received_for(&shared, 1).await == vec![1])
    })
    .await?;

    // And it stays resident well past what any finite linger would allow.
    tokio::time::sleep(TEST_LINGER * 10).await;
    let subscription = subs
        .subscription(&OwnerId(1))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    assert!(
        !subscription.load().await?.job_status().is_terminal(),
        "an always-on member must not passivate"
    );

    jobs.shutdown().await?;
    Ok(())
}
