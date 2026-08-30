//! Contracts for keyed subscribers: per-entity outbox consumers with
//! wake-on-demand — the `subscriptions` table,
//! `SubscriptionDef`/`KeyedSubscriber`, the hold verbs, the per-key runner,
//! the `Subscriptions` capability, and both halves of the wake plane (the
//! waker's two paths — wake-key matches and cache-pressure catch-up).

mod helpers;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use obix::{
    EventSequence, Handled, KeyedEventCtx, KeyedSubscriber, KeyedSubscriberConfig, MailboxConfig,
    SubscriptionDef, WakeKey, out::Outbox,
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
    /// Empty for every contract where the wake key and the domain key
    /// coincide; non-empty only for the multi-wake-key contract, which
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
            shared: self.shared.clone(),
        }
    }
}

/// Records every `Ping` addressed to its own key. Every subscriber for one
/// keyed subscriber type sees the WHOLE persistent stream — wake keys
/// decide who runs, never who receives — so it must filter events belonging
/// to other keys itself, exactly like a real per-entity consumer would.
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

/// An outbox whose in-memory cache holds only `event_cache_size` events, so
/// the waker's catch-up threshold (three quarters of it) is reachable within
/// a test rather than after the default 750.
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

/// The wake keys a subscription for `owner` declares: the one partition its
/// own key names, which is exactly the string `TestDef::wake_keys`
/// classifies a `Ping` for that owner to. Traffic addressed to any other
/// owner therefore never wakes it — several contracts below depend on that.
fn wake_keys_for(owner: OwnerId) -> Vec<WakeKey> {
    vec![WakeKey::from(owner.to_string())]
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

/// Contract — an empty wake-key set is refused, not stored.
///
/// Matching is set overlap and `{} && {anything}` is false, so a
/// subscription declaring no wake keys can never be reached by the waker:
/// it works while Active (a live member reads the whole stream regardless)
/// and is stranded the first time it passivates. The type system permits it
/// — `IntoIterator` can yield nothing — so the guard is the only thing
/// standing between a caller and a subscription that silently stops.
///
/// Asserted on the returned error AND on the table: nothing may be written,
/// since a rejected subscribe must not leave a row that the waker would
/// then enumerate.
#[tokio::test]
#[file_serial]
async fn an_empty_wake_key_set_is_refused() -> anyhow::Result<()> {
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
    let err = subs
        .subscribe_in_op(
            &mut op,
            OwnerId(1),
            InstanceConfig::default(),
            Vec::<WakeKey>::new(),
        )
        .await
        .expect_err("an empty wake-key set must be refused");
    assert!(
        err.downcast_ref::<obix::SubscribeError>()
            .is_some_and(|e| matches!(e, obix::SubscribeError::EmptyWakeKeys)),
        "expected SubscribeError::EmptyWakeKeys, got: {err}"
    );
    drop(op);

    let rows: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) AS \"count!\" FROM subscriptions WHERE subscriber_type = $1",
        JOB_TYPE
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(rows, 0, "a refused subscribe must not write a row");

    Ok(())
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
        wake_keys_for(OwnerId(1)),
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
/// processing, and no wake revives it — every wake path resolves through the
/// `subscriptions` table, which no longer has the row.
///
/// Cancellation takes effect at the end of the member's *current* run: a
/// live runner already holding the stream is not killed mid-flight, it
/// re-reads the row at its next run start and completes. So the test waits
/// for that run to end before publishing — the earlier version of this
/// contract published immediately and only passed because a sleep sized to
/// the old sweep interval happened to exceed `linger`.
///
/// The event published after cancellation carries the cancelled key's own
/// wake key, so the waker actively tries to match it. A live bystander
/// subscription supplies the happens-before: once *it* has received an event
/// published after the cancelled one, the waker has demonstrably processed
/// past both, and the cancelled member's silence is a result rather than a
/// race that a longer sleep might have lost.
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

    // The handle is captured before cancelling and deliberately not
    // re-resolved: it observes the generation that is live right now, and
    // that generation reaching a terminal state is the signal that the run
    // cancel had to outlive has actually ended.
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

    // Addressed to the cancelled key's own wake key, so the waker looks it up
    // and finds the tombstone; then one for the bystander, whose arrival
    // proves the waker got past both.
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

    let members = subs.members().await.map_err(|e| anyhow::anyhow!("{e}"))?;
    assert_eq!(
        members.len(),
        1,
        "members() must enumerate the live key and not the cancelled one"
    );
    assert_eq!(members[0].0, OwnerId(2));

    Ok(())
}

/// Contract — dormancy + backlog drain: a caught-up member passivates to
/// Dormant after `linger` elapses (no live execution), retains its
/// watermark across passivation, and on its next wake drains everything
/// published while it was Dormant, in order.
///
/// Distinct from the single-event wake contract below: what is asserted
/// here is that the *watermark survives* passivation and that a multi-event
/// backlog accumulated during dormancy is delivered whole rather than
/// resumed from the wake point.
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

    // Respawned, it resumes from the retained watermark and drains the whole
    // backlog it missed while dormant — not just the event that woke it.
    eventually(Duration::from_secs(10), || async {
        Ok(received_for(&shared, 1).await == vec![1, 2, 3])
    })
    .await?;

    Ok(())
}

/// Contract — waker: a matching event wakes a Dormant member on its own.
/// The subscription registers with a wake key matching its own owner id,
/// and the event published while it is Dormant carries that key.
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
        vec![WakeKey::from(OwnerId(1).to_string())],
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

    // Published while Dormant: nothing is listening for this key, so the
    // wake-key match is the only thing that can deliver it. The catch-up
    // path cannot explain it either — one event is nowhere near the cache
    // depth that path triggers on.
    publish_ping(&outbox, 1, 2).await?;
    eventually(Duration::from_secs(10), || async {
        Ok(received_for(&shared, 1).await == vec![1, 2])
    })
    .await?;

    Ok(())
}

/// Contract — catch-up wake: a Dormant member drifting toward the bottom of
/// the in-memory cache is woken to drain from memory, without any wake key
/// matching and without a timer.
///
/// The member watches partition "1"; every event published after it
/// passivates is addressed to partition "2", so the wake-key path cannot
/// fire. The only mechanism left that can move this member is the catch-up
/// scan, and the only thing that triggers that is the stream advancing past
/// three quarters of the cache depth.
///
/// Asserted on the durable checkpoint rather than on received events,
/// because a woken member correctly *skips* all of this traffic: the
/// checkpoint is the observable that cannot advance unless the member
/// actually ran.
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

    // Traffic for a partition this member does not watch. No wake key can
    // match it, so nothing here is a reason to wake — until the accumulated
    // drift is itself the reason.
    for n in 0..12 {
        publish_ping(&outbox, 2, n).await?;
    }

    // Re-resolved each poll rather than reused: a handle captured before the
    // respawn observes the generation it was minted for, and a catch-up wake
    // starts a new one.
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

/// Contract — one waker per outbox, not per subscriber type.
///
/// Two keyed subscriber types are registered on the same outbox. Asserted:
///
/// - exactly ONE waker job row exists. Per-type wakers would give two, and
///   with them two independent full passes over the persistent stream —
///   every event read, decoded and checkpointed once per registered type.
/// - both types' members are revived from Dormant by the same event, which
///   is what proves the single waker classifies through *every* registered
///   type's `wake_keys` rather than whichever one happened to register
///   first. A waker that consulted only one route would leave the other
///   member Dormant and the assertion below would time out.
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

/// Contract — waker, multi-wake-key matching: the reason
/// `subscriptions.wake_keys` is a set rather than a scalar.
///
/// Every other contract here registers a single wake key, so the
/// array-ness of the column, the `&&` overlap semantics and the
/// `$2::varchar[]` cast are all exercised at cardinality one — which is
/// exactly the cardinality at which the missing cast once compiled clean and
/// failed on every call. This registers a subscription watching TWO
/// partitions, neither of which is its own key, and wakes it through each in
/// turn:
///
/// - woken by an event matching only its SECOND wake key (so a match on
///   the first element, or an equality comparison against a scalar, would
///   not explain the wake),
/// - woken again by an event matching only its FIRST,
/// - and a second subscription watching two entirely different partitions is
///   never woken by either event — asserted on its durable checkpoint, which
///   cannot advance unless the member actually ran.
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

    // The watcher's key (10) is deliberately none of the partitions it
    // watches (7 and 8), so nothing about this can pass by conflating the
    // domain key with a wake key.
    let mut op = outbox.begin_op().await?;
    subs.subscribe_in_op(
        &mut op,
        OwnerId(10),
        InstanceConfig {
            watched: vec![7, 8],
        },
        vec![WakeKey::from("7"), WakeKey::from("8")],
    )
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    subs.subscribe_in_op(
        &mut op,
        OwnerId(20),
        InstanceConfig {
            watched: vec![21, 22],
        },
        vec![WakeKey::from("21"), WakeKey::from("22")],
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
    //
    // Re-resolved at the end rather than read through this handle: a
    // `Subscription` observes the job generation it was minted for, and a
    // wake starts a new one. Comparing two reads of the same handle would
    // hold even if the bystander HAD been woken.
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

    /// Classifies nothing: these contracts drive the runner and the staged
    /// chain directly and must not have holds or mid-chain crashes disturbed
    /// by a wake. Empty here is the *event* side — "this event concerns
    /// nobody" — which is ordinary and unrelated to the empty *subscription*
    /// side that [`SubscribeError::EmptyWakeKeys`] rejects.
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
        wake_keys_for(OwnerId(1)),
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
        wake_keys_for(OwnerId(1)),
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

/// Contract — the `Subscriptions` capability is `tokio::spawn`-able.
///
/// `MailboxTables`'s methods return opaque futures that capture their
/// executor argument's lifetime. Awaiting one directly inside a method taking
/// `&self` makes the enclosing future's `Send`-ness higher-ranked over that
/// lifetime, which defeats inference at `tokio::spawn` with "implementation
/// of `Send` is not general enough" (rust-lang/rust#100013). It compiles
/// fine at the definition site, so only an actual spawn catches it.
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
            .members()
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
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

/// Contract — dormancy is about THIS MEMBER's idleness, not the stream's.
///
/// A keyed member sees the whole shared persistent stream and skips almost
/// all of it. If the linger deadline restarted on every arriving event, no
/// member would ever passivate on an outbox busier than `linger` — idle
/// members would hold a live job forever, and `cancel` could not take effect
/// until the entire stream went quiet. None of the traffic carries this
/// member's wake key, and the burst is far short of the cache depth the
/// catch-up path triggers on, so nothing can revive it once it passivates.
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

    // Traffic for a key this member does not own, faster than `linger`, for
    // longer than `linger` — the member skips every one of these.
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

    // Capture how busy the stream actually was BEFORE stopping the
    // publisher, so a pass cannot be explained by the traffic having dried
    // up on its own.
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

/// Contract — `linger: Duration::MAX` really is always-on.
///
/// The config documents it, and `Instant + Duration::MAX` panics, so the
/// documented value must not be reachable by that arithmetic: an un-addable
/// linger leaves the deadline unarmed and the passivation arm disabled.
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

    // Delivery at all is the panic check: an overflowing deadline kills the
    // run before it ever reads the stream.
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
