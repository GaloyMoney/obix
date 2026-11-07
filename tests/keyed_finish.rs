//! Contracts for keyed members that end themselves from a flush, and for
//! members born below the stream frontier.

mod helpers;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use obix::{
    EventSequence, FinishedAtFence, FlushOutcome, Handled, KeyedEventCtx, KeyedSubscriber,
    KeyedSubscriberConfig, MailboxConfig, SubscriptionDef, WakeKey, out::Outbox,
};
use serde::{Deserialize, Serialize};
use serial_test::file_serial;
use tokio::sync::{Mutex, Notify};

use helpers::{TestTables, init_pool, wipeout_keyed_subscriber_job_tables, wipeout_subscriptions};

const JOB_TYPE: &str = "test-keyed-finish";
/// A second type on the same outbox whose members never passivate.
const ALWAYS_ON_JOB_TYPE: &str = "test-keyed-finish-always-on";

const TEST_LINGER: Duration = Duration::from_millis(150);
const TEST_CHECKPOINT_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum TestEvent {
    Ping { owner: u64, n: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct MemberId(u64);

impl std::fmt::Display for MemberId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for MemberId {
    type Err = std::num::ParseIntError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(MemberId(s.parse()?))
    }
}

/// What one member does, chosen per instance so a test can run a subject and
/// its control side by side.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct InstanceConfig {
    /// The only owner whose pings this member records.
    owner: u64,
    /// Flush returns `Finish` once a batch holds a ping at or past this `n`.
    #[serde(default)]
    finish_at: Option<u64>,
    /// Pause at the ping with this `n` instead of collecting it.
    #[serde(default)]
    pause_at: Option<u64>,
    /// Take the ping with this `n` through `consume()` instead of collecting it.
    #[serde(default)]
    fence_at: Option<u64>,
    /// Park once, before collecting ping 3, until the test releases it.
    #[serde(default)]
    gate: bool,
    /// Milliseconds of work per recorded ping.
    #[serde(default)]
    work_ms: u64,
}

#[derive(Clone, Default)]
struct Shared {
    received: Arc<Mutex<HashMap<u64, Vec<u64>>>>,
    fail_once: Arc<AtomicBool>,
    failed: Arc<AtomicBool>,
    fenced: Arc<AtomicBool>,
    pause_called: Arc<AtomicBool>,
    gate_armed: Arc<AtomicBool>,
    parked: Arc<Notify>,
    release: Arc<Notify>,
}

struct TestDef {
    shared: Shared,
}

impl SubscriptionDef<TestEvent> for TestDef {
    type Key = MemberId;
    type InstanceConfig = InstanceConfig;
    type Subscriber = FinishingSubscriber;

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
        FinishingSubscriber {
            key,
            cfg,
            shared: self.shared.clone(),
        }
    }
}

struct FinishingSubscriber {
    key: MemberId,
    cfg: InstanceConfig,
    shared: Shared,
}

async fn insert_marker(
    op: &mut impl es_entity::AtomicOperation,
    key: u64,
    n: u64,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO test_finish_markers (key, n) VALUES ($1, $2)")
        .bind(key as i64)
        .bind(n as i64)
        .execute(op.as_executor())
        .await?;
    Ok(())
}

impl KeyedSubscriber<TestEvent> for FinishingSubscriber {
    type Batch = Vec<u64>;

    async fn handle<'inv>(
        &self,
        ctx: KeyedEventCtx<'inv, Self::Batch>,
        event: &obix::EventDelivery<TestEvent>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        let Some(TestEvent::Ping { owner, n }) = &event.payload else {
            return Ok(ctx.skip());
        };
        if *owner != self.cfg.owner {
            return Ok(ctx.skip());
        }

        if *n == 3 && self.cfg.gate && self.shared.gate_armed.swap(false, Ordering::SeqCst) {
            self.shared.parked.notify_one();
            self.shared.release.notified().await;
        }

        if Some(*n) == self.cfg.pause_at {
            self.shared.pause_called.store(true, Ordering::SeqCst);
            return Ok(ctx.pause_until(chrono::Utc::now() + chrono::Duration::hours(1)));
        }

        if Some(*n) == self.cfg.fence_at {
            let mut op = match ctx.consume().await {
                Ok(op) => op,
                Err(error) => {
                    if error.is::<FinishedAtFence>() {
                        self.shared.fenced.store(true, Ordering::SeqCst);
                    }
                    return Err(error);
                }
            };
            insert_marker(&mut op, self.key.0, *n).await?;
            return Ok(op.commit());
        }

        if self.cfg.work_ms > 0 {
            tokio::time::sleep(Duration::from_millis(self.cfg.work_ms)).await;
        }

        self.shared
            .received
            .lock()
            .await
            .entry(self.key.0)
            .or_default()
            .push(*n);
        Ok(ctx.collect(*n))
    }

    async fn flush(
        &self,
        op: &mut obix::FlushOp<'_, obix::InsertOrder>,
        items: Self::Batch,
    ) -> Result<FlushOutcome, Box<dyn std::error::Error + Send + Sync>> {
        let finishing = self
            .cfg
            .finish_at
            .is_some_and(|at| items.iter().any(|n| *n >= at));
        for n in &items {
            insert_marker(op, self.key.0, *n).await?;
        }
        if finishing && self.shared.fail_once.swap(false, Ordering::SeqCst) {
            self.shared.failed.store(true, Ordering::SeqCst);
            return Err("injected failure at the finishing landing".into());
        }
        Ok(if finishing {
            FlushOutcome::Finish
        } else {
            FlushOutcome::Continue
        })
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
    init_outbox_with(pool, MailboxConfig::builder().build()).await
}

async fn init_outbox_with(
    pool: &sqlx::PgPool,
    config: MailboxConfig,
) -> anyhow::Result<Outbox<TestEvent, TestTables>> {
    helpers::wipeout_outbox_tables(pool).await?;
    wipeout_keyed_subscriber_job_tables(pool, JOB_TYPE).await?;
    wipeout_keyed_subscriber_job_tables(pool, ALWAYS_ON_JOB_TYPE).await?;
    wipeout_subscriptions(pool, JOB_TYPE).await?;
    wipeout_subscriptions(pool, ALWAYS_ON_JOB_TYPE).await?;
    sqlx::query("DROP TABLE IF EXISTS test_finish_markers")
        .execute(pool)
        .await?;
    sqlx::query(
        "CREATE TABLE test_finish_markers \
         (id BIGSERIAL PRIMARY KEY, key BIGINT NOT NULL, n BIGINT NOT NULL)",
    )
    .execute(pool)
    .await?;

    Ok(Outbox::<TestEvent, TestTables>::init(pool, config).await?)
}

/// Retry fast enough that a failed landing replays inside a test.
fn fast_retry_settings() -> job::RetrySettings {
    job::RetrySettings {
        backoff: job::Backoff::new(
            Duration::from_millis(50),
            Duration::from_millis(100),
            Duration::from_millis(100),
            0,
        ),
        ..job::RetrySettings::default()
    }
}

fn test_config() -> KeyedSubscriberConfig {
    KeyedSubscriberConfig::new(job::JobType::new(JOB_TYPE))
        .with_linger(TEST_LINGER)
        .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL)
        .with_retry_settings(fast_retry_settings())
}

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

async fn received_for(shared: &Shared, key: u64) -> Vec<u64> {
    shared
        .received
        .lock()
        .await
        .get(&key)
        .cloned()
        .unwrap_or_default()
}

async fn markers_of(pool: &sqlx::PgPool, key: u64) -> anyhow::Result<Vec<u64>> {
    let rows: Vec<(i64,)> =
        sqlx::query_as("SELECT n FROM test_finish_markers WHERE key = $1 ORDER BY id")
            .bind(key as i64)
            .fetch_all(pool)
            .await?;
    Ok(rows.into_iter().map(|r| r.0 as u64).collect())
}

async fn row_exists(pool: &sqlx::PgPool, key: u64) -> anyhow::Result<bool> {
    let (count,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM subscriptions WHERE subscriber_type = $1 AND key = $2",
    )
    .bind(JOB_TYPE)
    .bind(key.to_string())
    .fetch_one(pool)
    .await?;
    Ok(count > 0)
}

fn wake_key_for(owner: u64) -> WakeKey {
    WakeKey::from(owner.to_string())
}

type Subs = obix::Subscriptions<TestDef, TestEvent, TestTables>;

async fn is_terminal(subs: &Subs, key: u64) -> anyhow::Result<bool> {
    Ok(subs
        .subscription(&MemberId(key))
        .await?
        .load()
        .await?
        .job_status()
        .is_terminal())
}

/// Park the member before ping 3, publish ping 4 so it is waiting in the
/// stream, then let the member go: its next stream read finds ping 4 ready, so
/// the batch lands at ping 4's verb rather than at a drained backlog.
async fn publish_ping_four_behind_the_gate(
    outbox: &Outbox<TestEvent, TestTables>,
    shared: &Shared,
    owner: u64,
) -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(10), shared.parked.notified()).await?;
    publish_ping_burst(outbox, owner, 4..5).await?;
    tokio::time::sleep(Duration::from_millis(400)).await;
    shared.release.notify_one();
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn finish_lands_work_checkpoint_row_deletion_and_completion_together() -> anyhow::Result<()> {
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
        .await?;

    let finishing = InstanceConfig {
        owner: 7,
        finish_at: Some(3),
        ..Default::default()
    };
    let continuing = InstanceConfig {
        owner: 7,
        ..Default::default()
    };
    let mut op = outbox.begin_op().await?;
    subs.subscribe_in_op(&mut op, MemberId(1), finishing, wake_key_for(7))
        .await?;
    subs.subscribe_in_op(&mut op, MemberId(2), continuing, wake_key_for(7))
        .await?;
    op.commit().await?;

    jobs.start_poll().await?;
    publish_ping_burst(&outbox, 7, 1..4).await?;

    eventually(Duration::from_secs(10), || async {
        is_terminal(&subs, 1).await
    })
    .await?;

    assert_eq!(markers_of(&pool, 1).await?.last(), Some(&3));
    assert!(!row_exists(&pool, 1).await?, "the row is deleted");
    let snapshot = subs.subscription(&MemberId(1)).await?.load().await?;
    assert_eq!(snapshot.checkpoint(), EventSequence::from(3u64));

    // Control: the same stream, a member that returns `Continue`, keeps its
    // row and keeps receiving; the finished member does not.
    publish_ping_burst(&outbox, 7, 4..5).await?;
    eventually(Duration::from_secs(10), || async {
        Ok(received_for(&shared, 2).await == vec![1, 2, 3, 4])
    })
    .await?;
    assert!(
        row_exists(&pool, 2).await?,
        "a continuing member keeps its row"
    );
    assert_eq!(received_for(&shared, 1).await, vec![1, 2, 3]);
    let snapshot = subs.subscription(&MemberId(1)).await?.load().await?;
    assert_eq!(snapshot.checkpoint(), EventSequence::from(3u64));

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn finish_replays_after_a_failed_landing() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let shared = Shared::default();
    shared.fail_once.store(true, Ordering::SeqCst);
    let subs = outbox
        .register_keyed_subscriber(
            &mut jobs,
            test_config(),
            TestDef {
                shared: shared.clone(),
            },
        )
        .await?;

    let cfg = InstanceConfig {
        owner: 7,
        finish_at: Some(3),
        ..Default::default()
    };
    let mut op = outbox.begin_op().await?;
    subs.subscribe_in_op(&mut op, MemberId(1), cfg, wake_key_for(7))
        .await?;
    op.commit().await?;

    jobs.start_poll().await?;
    publish_ping_burst(&outbox, 7, 1..4).await?;

    eventually(Duration::from_secs(15), || async {
        Ok(!row_exists(&pool, 1).await? && is_terminal(&subs, 1).await?)
    })
    .await?;

    assert!(
        shared.failed.load(Ordering::SeqCst),
        "the first finishing landing failed"
    );
    assert_eq!(
        markers_of(&pool, 1).await?,
        vec![1, 2, 3],
        "the failed landing's markers rolled back; the replay wrote them once"
    );

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn finish_at_a_consume_fence_ends_the_member_before_that_event() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let shared = Shared::default();
    shared.gate_armed.store(true, Ordering::SeqCst);
    let subs = outbox
        .register_keyed_subscriber(
            &mut jobs,
            test_config(),
            TestDef {
                shared: shared.clone(),
            },
        )
        .await?;

    let cfg = InstanceConfig {
        owner: 7,
        finish_at: Some(3),
        fence_at: Some(4),
        gate: true,
        ..Default::default()
    };
    let mut op = outbox.begin_op().await?;
    subs.subscribe_in_op(&mut op, MemberId(1), cfg, wake_key_for(7))
        .await?;
    op.commit().await?;

    jobs.start_poll().await?;
    publish_ping_burst(&outbox, 7, 1..4).await?;
    publish_ping_four_behind_the_gate(&outbox, &shared, 7).await?;

    eventually(Duration::from_secs(10), || async {
        is_terminal(&subs, 1).await
    })
    .await?;

    assert!(
        shared.fenced.load(Ordering::SeqCst),
        "consume() returned FinishedAtFence"
    );
    assert_eq!(
        markers_of(&pool, 1).await?,
        vec![1, 2, 3],
        "the batch before the fence landed; ping 4 was never processed"
    );
    assert_eq!(received_for(&shared, 1).await, vec![1, 2, 3]);
    assert!(!row_exists(&pool, 1).await?);
    let snapshot = subs.subscription(&MemberId(1)).await?.load().await?;
    assert_eq!(snapshot.checkpoint(), EventSequence::from(3u64));

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn finish_wins_over_pause_entry() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let shared = Shared::default();
    shared.gate_armed.store(true, Ordering::SeqCst);
    let subs = outbox
        .register_keyed_subscriber(
            &mut jobs,
            test_config(),
            TestDef {
                shared: shared.clone(),
            },
        )
        .await?;

    let finishing = InstanceConfig {
        owner: 7,
        finish_at: Some(3),
        pause_at: Some(4),
        gate: true,
        ..Default::default()
    };
    let pausing = InstanceConfig {
        owner: 9,
        pause_at: Some(4),
        ..Default::default()
    };
    let mut op = outbox.begin_op().await?;
    subs.subscribe_in_op(&mut op, MemberId(1), finishing, wake_key_for(7))
        .await?;
    subs.subscribe_in_op(&mut op, MemberId(2), pausing, wake_key_for(9))
        .await?;
    op.commit().await?;

    jobs.start_poll().await?;
    publish_ping_burst(&outbox, 7, 1..4).await?;
    publish_ping_four_behind_the_gate(&outbox, &shared, 7).await?;

    eventually(Duration::from_secs(10), || async {
        Ok(!row_exists(&pool, 1).await? && is_terminal(&subs, 1).await?)
    })
    .await?;

    assert!(
        shared.pause_called.load(Ordering::SeqCst),
        "the pause verb ran"
    );
    let snapshot = subs.subscription(&MemberId(1)).await?.load().await?;
    let state: serde_json::Value = snapshot
        .job()
        .execution_state()?
        .expect("a finished member retains its execution state");
    assert_eq!(state["sequence"], 3);
    assert!(
        state.get("paused").is_none(),
        "no pause is persisted when the landing finished: {state}"
    );

    // Control: the same pause on a member whose landing continues persists it.
    publish_ping_burst(&outbox, 9, 1..5).await?;
    eventually(Duration::from_secs(10), || async {
        let snapshot = subs.subscription(&MemberId(2)).await?.load().await?;
        let state: Option<serde_json::Value> = snapshot.job().execution_state()?;
        Ok(state.is_some_and(|s| s.get("paused").is_some()))
    })
    .await?;

    Ok(())
}

fn plain_config(owner: u64) -> InstanceConfig {
    InstanceConfig {
        owner,
        ..Default::default()
    }
}

#[tokio::test]
#[file_serial]
async fn member_born_below_the_frontier_reads_history() -> anyhow::Result<()> {
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
        .await?;

    publish_ping_burst(&outbox, 7, 1..6).await?;

    let mut op = outbox.begin_op().await?;
    subs.subscribe_from_in_op(
        &mut op,
        MemberId(1),
        plain_config(7),
        wake_key_for(7),
        EventSequence::BEGIN,
    )
    .await?;
    subs.subscribe_in_op(&mut op, MemberId(2), plain_config(7), wake_key_for(7))
        .await?;
    op.commit().await?;

    jobs.start_poll().await?;

    eventually(Duration::from_secs(10), || async {
        Ok(received_for(&shared, 1).await == vec![1, 2, 3, 4, 5])
    })
    .await?;

    // Control: the member born at the frontier saw none of that history, yet
    // is delivering — it hears the next ping, as does the member born below.
    publish_ping_burst(&outbox, 7, 6..7).await?;
    eventually(Duration::from_secs(10), || async {
        Ok(received_for(&shared, 2).await == vec![6]
            && received_for(&shared, 1).await == vec![1, 2, 3, 4, 5, 6])
    })
    .await?;

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn birth_ahead_of_the_frontier_is_rejected() -> anyhow::Result<()> {
    use es_entity::errlanes::Fail;

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
        .await?;

    publish_ping_burst(&outbox, 7, 1..6).await?;

    let mut op = outbox.begin_op().await?;
    let rejected = subs
        .subscribe_from_in_op(
            &mut op,
            MemberId(1),
            plain_config(7),
            wake_key_for(7),
            EventSequence::from(6u64),
        )
        .await;
    match rejected {
        Err(Fail::Rejected(obix::SubscriptionRejection::BirthAheadOfFrontier {
            start_after,
            frontier,
        })) => {
            assert_eq!(start_after, EventSequence::from(6u64));
            assert_eq!(frontier, EventSequence::from(5u64));
        }
        Ok(_) => panic!("a birth above the frontier must be rejected"),
        Err(other) => panic!("expected BirthAheadOfFrontier, got {other:?}"),
    }
    drop(op);
    assert!(!row_exists(&pool, 1).await?);

    // Control: a birth exactly at the frontier is accepted and behaves as
    // `subscribe_in_op`: no history, then the next ping.
    let mut op = outbox.begin_op().await?;
    subs.subscribe_from_in_op(
        &mut op,
        MemberId(1),
        plain_config(7),
        wake_key_for(7),
        EventSequence::from(5u64),
    )
    .await?;
    op.commit().await?;

    jobs.start_poll().await?;
    publish_ping_burst(&outbox, 7, 6..7).await?;
    eventually(Duration::from_secs(10), || async {
        Ok(received_for(&shared, 1).await == vec![6])
    })
    .await?;

    Ok(())
}

async fn tombstone_exists(pool: &sqlx::PgPool, key: u64) -> anyhow::Result<bool> {
    let (count,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM retired_subscriptions WHERE subscriber_type = $1 AND key = $2",
    )
    .bind(JOB_TYPE)
    .bind(key.to_string())
    .fetch_one(pool)
    .await?;
    Ok(count > 0)
}

async fn assert_key_retired(
    subs: &Subs,
    outbox: &Outbox<TestEvent, TestTables>,
    key: u64,
) -> anyhow::Result<()> {
    use es_entity::errlanes::Fail;

    let mut op = outbox.begin_op().await?;
    let via_frontier = subs
        .subscribe_in_op(&mut op, MemberId(key), plain_config(7), wake_key_for(7))
        .await;
    match via_frontier {
        Err(Fail::Rejected(obix::SubscriptionRejection::KeyRetired {
            subscriber_type,
            key: rejected_key,
        })) => {
            assert_eq!(subscriber_type, JOB_TYPE);
            assert_eq!(rejected_key, key.to_string());
        }
        Ok(_) => panic!("subscribe_in_op must reject retired key {key}"),
        Err(other) => panic!("expected KeyRetired from subscribe_in_op, got {other:?}"),
    }
    let via_birth = subs
        .subscribe_from_in_op(
            &mut op,
            MemberId(key),
            plain_config(7),
            wake_key_for(7),
            EventSequence::BEGIN,
        )
        .await;
    match via_birth {
        Err(Fail::Rejected(obix::SubscriptionRejection::KeyRetired {
            subscriber_type,
            key: rejected_key,
        })) => {
            assert_eq!(subscriber_type, JOB_TYPE);
            assert_eq!(rejected_key, key.to_string());
        }
        Ok(_) => panic!("subscribe_from_in_op must reject retired key {key}"),
        Err(other) => panic!("expected KeyRetired from subscribe_from_in_op, got {other:?}"),
    }
    op.commit().await?;
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn retired_key_is_rejected_by_both_subscribe_calls() -> anyhow::Result<()> {
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
        .await?;

    let finishing = InstanceConfig {
        owner: 7,
        finish_at: Some(1),
        ..Default::default()
    };
    let mut op = outbox.begin_op().await?;
    subs.subscribe_in_op(&mut op, MemberId(1), finishing, wake_key_for(7))
        .await?;
    subs.subscribe_in_op(&mut op, MemberId(2), plain_config(7), wake_key_for(7))
        .await?;
    subs.subscribe_in_op(&mut op, MemberId(3), plain_config(7), wake_key_for(7))
        .await?;
    op.commit().await?;

    jobs.start_poll().await?;
    publish_ping_burst(&outbox, 7, 1..2).await?;
    eventually(Duration::from_secs(10), || async {
        is_terminal(&subs, 1).await
    })
    .await?;
    subs.cancel(&MemberId(2)).await?;

    assert!(
        tombstone_exists(&pool, 1).await?,
        "finish leaves a tombstone"
    );
    assert!(
        tombstone_exists(&pool, 2).await?,
        "cancel leaves a tombstone"
    );

    assert_key_retired(&subs, &outbox, 1).await?;
    assert_key_retired(&subs, &outbox, 2).await?;
    assert!(
        !row_exists(&pool, 1).await?,
        "a rejected subscribe inserts no row"
    );
    assert!(
        !row_exists(&pool, 2).await?,
        "a rejected subscribe inserts no row"
    );

    // Control: a live key is still idempotent through both calls, and a fresh
    // key subscribes and receives.
    assert!(!tombstone_exists(&pool, 3).await?);
    let mut op = outbox.begin_op().await?;
    subs.subscribe_in_op(&mut op, MemberId(3), plain_config(7), wake_key_for(7))
        .await?;
    subs.subscribe_from_in_op(
        &mut op,
        MemberId(3),
        plain_config(7),
        wake_key_for(7),
        EventSequence::BEGIN,
    )
    .await?;
    subs.subscribe_in_op(&mut op, MemberId(4), plain_config(7), wake_key_for(7))
        .await?;
    op.commit().await?;
    assert!(row_exists(&pool, 3).await?);

    publish_ping_burst(&outbox, 7, 2..3).await?;
    eventually(Duration::from_secs(10), || async {
        Ok(received_for(&shared, 3).await == vec![1, 2]
            && received_for(&shared, 4).await == vec![2])
    })
    .await?;
    assert_eq!(received_for(&shared, 1).await, vec![1]);
    assert!(!row_exists(&pool, 1).await?);

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn a_subscribe_racing_a_cancel_cannot_resurrect_the_key() -> anyhow::Result<()> {
    use es_entity::errlanes::Fail;

    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let subs = outbox
        .register_keyed_subscriber(
            &mut jobs,
            test_config(),
            TestDef {
                shared: Shared::default(),
            },
        )
        .await?;

    let mut op = outbox.begin_op().await?;
    subs.subscribe_in_op(&mut op, MemberId(1), plain_config(7), wake_key_for(7))
        .await?;
    subs.subscribe_in_op(&mut op, MemberId(2), plain_config(7), wake_key_for(7))
        .await?;
    op.commit().await?;

    let mut cancelling = outbox.begin_op().await?;
    subs.cancel_in_op(&mut cancelling, &MemberId(1)).await?;

    let racing_outbox = outbox.clone();
    let racing_subs = subs.clone();
    let racing = tokio::spawn(async move {
        let mut op = racing_outbox.begin_op().await?;
        let result = racing_subs
            .subscribe_in_op(&mut op, MemberId(1), plain_config(7), wake_key_for(7))
            .await;
        op.commit().await?;
        Ok::<_, anyhow::Error>(result.map(|_| ()))
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !racing.is_finished(),
        "the subscribe must wait on the row the uncommitted cancel holds"
    );
    cancelling.commit().await?;

    match racing.await?? {
        Err(Fail::Rejected(obix::SubscriptionRejection::KeyRetired { key, .. })) => {
            assert_eq!(key, MemberId(1).to_string());
        }
        Ok(()) => panic!("a subscribe racing a cancel must not resurrect the key"),
        Err(other) => panic!("expected KeyRetired, got {other:?}"),
    }
    assert!(
        !row_exists(&pool, 1).await?,
        "no live row beside the tombstone"
    );
    assert!(tombstone_exists(&pool, 1).await?);

    // Control: with no cancel in flight, the same concurrent shape resolves
    // idempotently against the live row.
    let racing_outbox = outbox.clone();
    let racing_subs = subs.clone();
    tokio::spawn(async move {
        let mut op = racing_outbox.begin_op().await?;
        racing_subs
            .subscribe_in_op(&mut op, MemberId(2), plain_config(7), wake_key_for(7))
            .await?;
        op.commit().await?;
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    assert!(row_exists(&pool, 2).await?);

    Ok(())
}

fn always_on_config() -> KeyedSubscriberConfig {
    KeyedSubscriberConfig::new(job::JobType::new(ALWAYS_ON_JOB_TYPE))
        .with_linger(Duration::MAX)
        .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL)
}

async fn dormant_of(pool: &sqlx::PgPool, job_type: &str, key: u64) -> anyhow::Result<bool> {
    let (dormant,): (bool,) =
        sqlx::query_as("SELECT dormant FROM subscriptions WHERE subscriber_type = $1 AND key = $2")
            .bind(job_type)
            .bind(key.to_string())
            .fetch_one(pool)
            .await?;
    Ok(dormant)
}

async fn generation_of(subs: &Subs, key: u64) -> anyhow::Result<job::JobId> {
    Ok(subs
        .subscription(&MemberId(key))
        .await?
        .load()
        .await?
        .job()
        .job()
        .id)
}

#[tokio::test]
#[file_serial]
async fn always_on_type_members_never_passivate() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let shared = Shared::default();
    let passivating: Subs = outbox
        .register_keyed_subscriber(
            &mut jobs,
            test_config(),
            TestDef {
                shared: shared.clone(),
            },
        )
        .await?;
    let always_on: Subs = outbox
        .register_keyed_subscriber(
            &mut jobs,
            always_on_config(),
            TestDef {
                shared: shared.clone(),
            },
        )
        .await?;

    let mut op = outbox.begin_op().await?;
    passivating
        .subscribe_in_op(&mut op, MemberId(1), plain_config(7), wake_key_for(7))
        .await?;
    always_on
        .subscribe_in_op(&mut op, MemberId(2), plain_config(7), wake_key_for(7))
        .await?;
    op.commit().await?;
    jobs.start_poll().await?;

    let generation = generation_of(&always_on, 2).await?;
    tokio::time::sleep(Duration::from_secs(1)).await;

    assert_eq!(generation_of(&always_on, 2).await?, generation);
    assert!(!is_terminal(&always_on, 2).await?);
    assert!(!dormant_of(&pool, ALWAYS_ON_JOB_TYPE, 2).await?);

    publish_ping_burst(&outbox, 7, 1..2).await?;
    eventually(Duration::from_secs(10), || async {
        Ok(received_for(&shared, 2).await == vec![1])
    })
    .await?;
    assert_eq!(generation_of(&always_on, 2).await?, generation);

    // Control: a member of the type with a bounded linger passivates and its
    // row says so.
    eventually(Duration::from_secs(10), || async {
        dormant_of(&pool, JOB_TYPE, 1).await
    })
    .await?;

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn catch_up_scan_wakes_dormant_members_only() -> anyhow::Result<()> {
    use obix::MailboxTables;

    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox =
        init_outbox_with(&pool, MailboxConfig::builder().event_cache_size(8).build()).await?;

    let shared = Shared::default();
    let passivating: Subs = outbox
        .register_keyed_subscriber(
            &mut jobs,
            test_config(),
            TestDef {
                shared: shared.clone(),
            },
        )
        .await?;
    let always_on: Subs = outbox
        .register_keyed_subscriber(
            &mut jobs,
            always_on_config(),
            TestDef {
                shared: shared.clone(),
            },
        )
        .await?;

    // The dormant member: born before any traffic, passivated at checkpoint 0.
    let mut op = outbox.begin_op().await?;
    passivating
        .subscribe_in_op(&mut op, MemberId(2), plain_config(8), wake_key_for(8))
        .await?;
    op.commit().await?;
    jobs.start_poll().await?;
    eventually(Duration::from_secs(10), || async {
        dormant_of(&pool, JOB_TYPE, 2).await
    })
    .await?;
    let dormant_generation = generation_of(&passivating, 2).await?;

    // History for the live member, born below the frontier with a handler too
    // slow to catch up: legitimately far behind, and not dormant.
    for n in 0..12 {
        publish_ping_burst(&outbox, 5, n..n + 1).await?;
    }
    let mut op = outbox.begin_op().await?;
    always_on
        .subscribe_from_in_op(
            &mut op,
            MemberId(1),
            InstanceConfig {
                owner: 5,
                work_ms: 400,
                ..Default::default()
            },
            wake_key_for(5),
            EventSequence::BEGIN,
        )
        .await?;
    op.commit().await?;
    let live_generation = generation_of(&always_on, 1).await?;

    // The dormant member is woken by the scan: a new generation.
    eventually(Duration::from_secs(10), || async {
        Ok(generation_of(&passivating, 2).await? != dormant_generation)
    })
    .await?;
    assert_eq!(generation_of(&always_on, 1).await?, live_generation);

    // The scan itself, against a bound above every checkpoint: the live
    // member is far behind and is not selected.
    let types = vec![JOB_TYPE.to_string(), ALWAYS_ON_JOB_TYPE.to_string()];
    let scan = |pool: sqlx::PgPool, types: Vec<String>| async move {
        let mut op = es_entity::DbOp::init(&pool).await?;
        let rows =
            TestTables::subscriptions_behind(&mut op, &types, EventSequence::from(1_000u64), 10)
                .await?;
        anyhow::Ok(rows.into_iter().map(|r| r.1).collect::<Vec<_>>())
    };
    assert!(
        !scan(pool.clone(), types.clone())
            .await?
            .contains(&"1".to_string())
    );

    // Control: the same row, flagged dormant, is selected.
    sqlx::query("UPDATE subscriptions SET dormant = TRUE WHERE subscriber_type = $1 AND key = '1'")
        .bind(ALWAYS_ON_JOB_TYPE)
        .execute(&pool)
        .await?;
    assert!(scan(pool.clone(), types).await?.contains(&"1".to_string()));

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn wake_flips_dormant_off_in_the_waker_flush() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let shared = Shared::default();
    let passivating: Subs = outbox
        .register_keyed_subscriber(
            &mut jobs,
            test_config().with_linger(Duration::from_millis(600)),
            TestDef {
                shared: shared.clone(),
            },
        )
        .await?;
    let always_on: Subs = outbox
        .register_keyed_subscriber(
            &mut jobs,
            always_on_config(),
            TestDef {
                shared: shared.clone(),
            },
        )
        .await?;

    let mut op = outbox.begin_op().await?;
    passivating
        .subscribe_in_op(&mut op, MemberId(1), plain_config(7), wake_key_for(7))
        .await?;
    always_on
        .subscribe_in_op(&mut op, MemberId(2), plain_config(7), wake_key_for(7))
        .await?;
    op.commit().await?;
    jobs.start_poll().await?;

    eventually(Duration::from_secs(10), || async {
        dormant_of(&pool, JOB_TYPE, 1).await
    })
    .await?;
    assert!(!dormant_of(&pool, ALWAYS_ON_JOB_TYPE, 2).await?);
    let live_generation = generation_of(&always_on, 2).await?;

    publish_ping_burst(&outbox, 7, 1..2).await?;
    eventually(Duration::from_secs(10), || async {
        Ok(received_for(&shared, 1).await == vec![1] && received_for(&shared, 2).await == vec![1])
    })
    .await?;

    assert!(
        !dormant_of(&pool, JOB_TYPE, 1).await?,
        "the wake cleared the flag"
    );
    // Control: a wake-key match for a live member leaves the flag and its
    // generation alone.
    assert!(!dormant_of(&pool, ALWAYS_ON_JOB_TYPE, 2).await?);
    assert_eq!(generation_of(&always_on, 2).await?, live_generation);

    Ok(())
}
