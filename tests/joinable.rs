//! Contracts for joinable subscriptions: a tip whose every landing takes the
//! tip row's lock at entry, and catch-up lanes that join it at a sequence no tip
//! landing straddles.
//!
//! The fixture is the handler contract: a tip applies an event to a unit only
//! when the unit has joined at a sequence below the event's (read on
//! the landing op through `lanes_in_op`), and a lane collects only its own
//! unit's events, skipping the rest in `handle`. Every application is a row,
//! so "applied by whom, in what order" is a query.

mod helpers;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use obix::{
    CatchUpLane, EventSequence, FlushOp, Handled, InsertOrder, JoinableConfig, JoinableEventCtx,
    JoinableHandle, JoinableSubscriber, MailboxConfig, OutboxEventJobConfig, Scope,
    UndecodableDelivery, out::Outbox,
};
use serde::{Deserialize, Serialize};
use serial_test::file_serial;
use tokio::sync::{Mutex, Notify};

use helpers::{TestTables, init_pool, wipeout_outbox_job_tables};

const TIP: &str = "test-joinable-tip";
const LANE: &str = "test-joinable-tip.lane";
const TEST_CHECKPOINT_INTERVAL: Duration = Duration::from_millis(50);
const WAIT: Duration = Duration::from_secs(15);

type TestOutbox = Outbox<TestEvent, TestTables>;
type TestHandle = JoinableHandle<TestEvent, String, TestTables>;
type TestLane = CatchUpLane<TestEvent, TestTables>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum TestEvent {
    Ping { key: String, n: u64 },
    Noise,
}

/// A payload the typed outbox cannot decode: published through another event
/// enum, as tests sharing the database do.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum ForeignEvent {
    Poison,
}

/// Parks the first caller after `arm`, until `release`.
#[derive(Clone, Default)]
struct Gate {
    armed: Arc<AtomicBool>,
    parked: Arc<AtomicBool>,
    release: Arc<Notify>,
}

impl Gate {
    fn arm(&self) {
        self.parked.store(false, Ordering::SeqCst);
        self.armed.store(true, Ordering::SeqCst);
    }

    async fn pass(&self) {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.parked.store(true, Ordering::SeqCst);
            self.release.notified().await;
            self.parked.store(false, Ordering::SeqCst);
        }
    }

    fn is_parked(&self) -> bool {
        self.parked.load(Ordering::SeqCst)
    }

    fn release(&self) {
        self.release.notify_one();
    }
}

#[derive(Debug, Clone)]
struct TipView {
    since: u64,
    catching_up: Vec<String>,
    joined: Vec<(String, u64)>,
}

#[derive(Clone, Default)]
struct Shared {
    handle: Arc<OnceLock<TestHandle>>,
    /// Parks a tip `flush` after the entry lock is held.
    tip_gate: Gate,
    /// Parks the first `handle` of the event at `handle_gate_seq` after `arm`.
    handle_gate: Gate,
    handle_gate_seq: Arc<AtomicU64>,
    fail_handle_seq: Arc<AtomicU64>,
    fail_handle_armed: Arc<AtomicBool>,
    handle_failures: Arc<AtomicU64>,
    fail_tip_flush: Arc<AtomicBool>,
    fail_tip_flush_once: Arc<AtomicBool>,
    tip_flush_failures: Arc<AtomicU64>,
    undecodable_ok: Arc<AtomicBool>,
    undecodable_calls: Arc<AtomicU64>,
    /// `(sequence, join)` of every event a lane handled under `Scope::Join`.
    drain_joins: Arc<Mutex<Vec<(u64, u64)>>>,
    tip_lock_held: Arc<Mutex<Vec<bool>>>,
    tip_units: Arc<Mutex<HashMap<String, EventSequence>>>,
    tip_prev: Arc<AtomicU64>,
    tip_views: Arc<Mutex<Vec<TipView>>>,
}

struct Applier {
    shared: Shared,
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

impl JoinableSubscriber<TestEvent> for Applier {
    type Unit = String;
    type Batch = Vec<(String, EventSequence)>;

    async fn handle<'inv>(
        &self,
        ctx: JoinableEventCtx<'inv, Self::Batch>,
        scope: Scope<'_, Self::Unit>,
        event: &obix::EventDelivery<TestEvent>,
    ) -> Result<Handled<'inv>, BoxError> {
        let Some(TestEvent::Ping { key, .. }) = &event.payload else {
            return Ok(ctx.skip());
        };
        let sequence = u64::from(event.sequence);
        if let Scope::Join { join, .. } = scope {
            self.shared
                .drain_joins
                .lock()
                .await
                .push((sequence, u64::from(join)));
        }
        if self.shared.handle_gate_seq.load(Ordering::SeqCst) == sequence {
            self.shared.handle_gate.pass().await;
        }
        if self.shared.fail_handle_seq.load(Ordering::SeqCst) == sequence
            && self.shared.fail_handle_armed.swap(false, Ordering::SeqCst)
        {
            self.shared.handle_failures.fetch_add(1, Ordering::SeqCst);
            return Err("injected failure in handle".into());
        }
        // A lane collects its own unit's events only; the tip collects all.
        match scope {
            Scope::Tip => {}
            Scope::CatchUp { unit } | Scope::Join { unit, .. } if unit == key => {}
            Scope::CatchUp { .. } | Scope::Join { .. } => return Ok(ctx.skip()),
        }
        Ok(ctx.collect((key.clone(), event.sequence)))
    }

    async fn handle_undecodable(&self, error: &UndecodableDelivery) -> Result<(), BoxError> {
        self.shared.undecodable_calls.fetch_add(1, Ordering::SeqCst);
        if self.shared.undecodable_ok.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(error.inner().clone().into())
        }
    }

    async fn flush(
        &self,
        op: &mut FlushOp<'_, InsertOrder>,
        scope: Scope<'_, Self::Unit>,
        batch: Self::Batch,
    ) -> Result<(), BoxError> {
        use es_entity::AtomicOperation;

        let lock_held = holds_tip_row_lock(op.as_executor()).await?;
        let items = batch.len() as i64;
        let position = u64::from(op.position());
        match scope {
            Scope::Tip => {
                self.shared.tip_lock_held.lock().await.push(lock_held);
                self.shared.tip_gate.pass().await;
                if self.shared.fail_tip_flush.load(Ordering::SeqCst)
                    || self
                        .shared
                        .fail_tip_flush_once
                        .swap(false, Ordering::SeqCst)
                {
                    self.shared
                        .tip_flush_failures
                        .fetch_add(1, Ordering::SeqCst);
                    return Err("injected failure in tip flush".into());
                }

                let since = EventSequence::from(self.shared.tip_prev.load(Ordering::SeqCst));
                let handle = self
                    .shared
                    .handle
                    .get()
                    .expect("handle is set before spawn");
                let lanes = handle.lanes_in_op(&mut *op, since).await?;
                self.shared.tip_views.lock().await.push(TipView {
                    since: u64::from(since),
                    catching_up: lanes.catching_up.iter().map(|(_, u)| u.clone()).collect(),
                    joined: lanes
                        .joined
                        .iter()
                        .map(|(_, u, f)| (u.clone(), u64::from(*f)))
                        .collect(),
                });
                let mut units = self.shared.tip_units.lock().await;
                for (_, unit, join) in lanes.joined {
                    units.insert(unit, join);
                }
                for (key, sequence) in batch {
                    if units.get(&key).is_some_and(|join| sequence > *join) {
                        insert_applied(op, "tip", &key, sequence).await?;
                    }
                }
                drop(units);
                insert_flush(op, "tip", None, items, lock_held).await?;
                self.shared.tip_prev.store(position, Ordering::SeqCst);
            }
            Scope::CatchUp { unit } | Scope::Join { unit, .. } => {
                let join = match scope {
                    Scope::Join { join, .. } => Some(u64::from(join) as i64),
                    Scope::CatchUp { .. } | Scope::Tip => None,
                };
                for (key, sequence) in batch {
                    assert_eq!(
                        &key, unit,
                        "a lane's batch holds its own unit's events only"
                    );
                    insert_applied(op, "catch_up", &key, sequence).await?;
                }
                insert_flush(op, "catch_up", join, items, lock_held).await?;
            }
        }
        Ok(())
    }
}

async fn insert_applied(
    op: &mut FlushOp<'_, InsertOrder>,
    scope: &str,
    unit: &str,
    sequence: EventSequence,
) -> Result<(), sqlx::Error> {
    use es_entity::AtomicOperation;
    sqlx::query("INSERT INTO test_joinable_applied (scope, unit, sequence) VALUES ($1, $2, $3)")
        .bind(scope)
        .bind(unit)
        .bind(u64::from(sequence) as i64)
        .execute(op.as_executor())
        .await?;
    Ok(())
}

async fn insert_flush(
    op: &mut FlushOp<'_, InsertOrder>,
    scope: &str,
    join: Option<i64>,
    items: i64,
    lock_held: bool,
) -> Result<(), sqlx::Error> {
    use es_entity::AtomicOperation;
    sqlx::query(
        "INSERT INTO test_joinable_flushes (scope, join_sequence, items, lock_held) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(scope)
    .bind(join)
    .bind(items)
    .bind(lock_held)
    .execute(op.as_executor())
    .await?;
    Ok(())
}

/// Whether this backend's open transaction holds the row lock a
/// `SELECT .. FOR UPDATE` on `joinable_tips` takes.
async fn holds_tip_row_lock<'c>(
    executor: impl sqlx::Executor<'c, Database = sqlx::Postgres>,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1 FROM pg_locks
            WHERE pid = pg_backend_pid()
              AND locktype = 'relation'
              AND relation = 'joinable_tips'::regclass
              AND mode = 'RowShareLock'
              AND granted
        )",
    )
    .fetch_one(executor)
    .await
}

/// Whether some backend is waiting on a lock inside the tip row's
/// `SELECT .. FOR UPDATE`.
async fn a_backend_waits_on_the_tip_row(pool: &sqlx::PgPool) -> anyhow::Result<bool> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1 FROM pg_stat_activity
            WHERE datname = current_database()
              AND pid <> pg_backend_pid()
              AND wait_event_type = 'Lock'
              AND query LIKE '%joinable_tips%'
              AND query ILIKE '%for update%'
        )",
    )
    .fetch_one(pool)
    .await?)
}

fn fast_retry_settings() -> job::RetrySettings {
    retry_settings(Duration::from_millis(50), Duration::from_millis(100))
}

fn retry_settings(initial: Duration, max: Duration) -> job::RetrySettings {
    job::RetrySettings {
        backoff: job::Backoff::new(initial, max, max, 0),
        ..job::RetrySettings::default()
    }
}

fn test_config() -> JoinableConfig {
    JoinableConfig::new(
        OutboxEventJobConfig::new(job::JobType::new(TIP))
            .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL)
            .with_retry_settings(fast_retry_settings()),
    )
    .with_lane_retry_settings(fast_retry_settings())
}

struct Harness {
    pool: sqlx::PgPool,
    jobs: job::Jobs,
    outbox: TestOutbox,
    tip: TestHandle,
    shared: Shared,
}

impl Harness {
    async fn start(config: JoinableConfig) -> anyhow::Result<Self> {
        let pool = init_pool().await?;
        let job_config = job::JobSvcConfig::builder()
            .pool(pool.clone())
            .build()
            .unwrap();
        let mut jobs = job::Jobs::init(job_config).await?;

        helpers::wipeout_outbox_tables(&pool).await?;
        wipeout_outbox_job_tables(&pool, TIP).await?;
        wipeout_outbox_job_tables(&pool, LANE).await?;
        helpers::wipeout_subscriptions(&pool, LANE).await?;
        sqlx::query("DELETE FROM joinable_tips WHERE subscriber_type = $1")
            .bind(TIP)
            .execute(&pool)
            .await?;
        sqlx::query("DELETE FROM joinable_lanes WHERE subscriber_type = $1")
            .bind(TIP)
            .execute(&pool)
            .await?;
        sqlx::query("DROP TABLE IF EXISTS test_joinable_applied")
            .execute(&pool)
            .await?;
        sqlx::query(
            "CREATE TABLE test_joinable_applied (id BIGSERIAL PRIMARY KEY, \
             scope TEXT NOT NULL, unit TEXT NOT NULL, sequence BIGINT NOT NULL)",
        )
        .execute(&pool)
        .await?;
        sqlx::query("DROP TABLE IF EXISTS test_joinable_flushes")
            .execute(&pool)
            .await?;
        sqlx::query(
            "CREATE TABLE test_joinable_flushes (id BIGSERIAL PRIMARY KEY, \
             scope TEXT NOT NULL, join_sequence BIGINT, items BIGINT NOT NULL, lock_held BOOLEAN NOT NULL)",
        )
        .execute(&pool)
        .await?;

        let outbox = TestOutbox::init(&pool, MailboxConfig::builder().build()).await?;
        let tip: TestHandle = outbox.joinable(job::JobType::new(TIP));
        let shared = Shared::default();
        shared.undecodable_ok.store(true, Ordering::SeqCst);
        shared
            .handle
            .set(tip.clone())
            .unwrap_or_else(|_| unreachable!("fresh shared"));
        tip.spawn(
            &mut jobs,
            config,
            Applier {
                shared: shared.clone(),
            },
        )
        .await?;
        jobs.start_poll().await?;

        let harness = Self {
            pool,
            jobs,
            outbox,
            tip,
            shared,
        };
        eventually(WAIT, || async {
            Ok(harness.tip_row_checkpoint().await?.is_some())
        })
        .await?;
        Ok(harness)
    }

    async fn publish(&self, events: Vec<TestEvent>) -> anyhow::Result<()> {
        let mut op = self.outbox.begin_op().await?;
        self.outbox.publish_all_persisted(&mut op, events).await?;
        op.commit().await?;
        Ok(())
    }

    async fn publish_pings(&self, key: &str, ns: std::ops::Range<u64>) -> anyhow::Result<()> {
        self.publish(ns.map(|n| ping(key, n)).collect()).await
    }

    async fn publish_poison(&self) -> anyhow::Result<()> {
        let foreign =
            Outbox::<ForeignEvent, TestTables>::init(&self.pool, MailboxConfig::builder().build())
                .await?;
        let mut op = foreign.begin_op().await?;
        foreign
            .publish_persisted_in_op(&mut op, ForeignEvent::Poison)
            .await?;
        op.commit().await?;
        Ok(())
    }

    async fn open_lane(&self, unit: &str, start_after: u64) -> anyhow::Result<TestLane> {
        let mut op = self.outbox.begin_op().await?;
        let lane = self
            .tip
            .open_lane_in_op(&mut op, unit.to_string(), EventSequence::from(start_after))
            .await?;
        op.commit().await?;
        Ok(lane)
    }

    async fn tip_row_checkpoint(&self) -> anyhow::Result<Option<i64>> {
        Ok(
            sqlx::query_scalar("SELECT checkpoint FROM joinable_tips WHERE subscriber_type = $1")
                .bind(TIP)
                .fetch_one(&self.pool)
                .await?,
        )
    }

    async fn applied_by(&self, scope: &str, unit: &str) -> anyhow::Result<Vec<i64>> {
        Ok(sqlx::query_scalar(
            "SELECT sequence FROM test_joinable_applied \
             WHERE scope = $1 AND unit = $2 ORDER BY id",
        )
        .bind(scope)
        .bind(unit)
        .fetch_all(&self.pool)
        .await?)
    }

    async fn applied_count(&self) -> anyhow::Result<i64> {
        Ok(
            sqlx::query_scalar("SELECT COUNT(*) FROM test_joinable_applied")
                .fetch_one(&self.pool)
                .await?,
        )
    }

    /// `(join_sequence, items, lock_held)` of every lane flush, in landing order.
    async fn lane_flushes(&self) -> anyhow::Result<Vec<(Option<i64>, i64, bool)>> {
        Ok(sqlx::query_as(
            "SELECT join_sequence, items, lock_held FROM test_joinable_flushes \
             WHERE scope = 'catch_up' ORDER BY id",
        )
        .fetch_all(&self.pool)
        .await?)
    }

    async fn tip_flush_count(&self) -> anyhow::Result<i64> {
        Ok(
            sqlx::query_scalar("SELECT COUNT(*) FROM test_joinable_flushes WHERE scope = 'tip'")
                .fetch_one(&self.pool)
                .await?,
        )
    }

    /// The lane's record in `joinable_tips`'s sibling, `joinable_lanes`:
    /// `None` while the lane is live, `Some(None)` for a cancelled lane and
    /// `Some(Some(F))` for one that joined at `F`.
    async fn lane_record(&self, lane: &TestLane) -> anyhow::Result<Option<Option<i64>>> {
        Ok(sqlx::query_scalar(
            "SELECT joined_at_sequence FROM joinable_lanes \
             WHERE subscriber_type = $1 AND key = $2",
        )
        .bind(TIP)
        .bind(lane.id().to_string())
        .fetch_optional(&self.pool)
        .await?)
    }

    /// Whether the lane left the ordinary keyed tombstone behind.
    async fn has_tombstone(&self, lane: &TestLane) -> anyhow::Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM retired_subscriptions \
             WHERE subscriber_type = $1 AND key = $2)",
        )
        .bind(LANE)
        .bind(lane.id().to_string())
        .fetch_one(&self.pool)
        .await?)
    }

    async fn lane_row_checkpoint(&self, lane: &TestLane) -> anyhow::Result<Option<i64>> {
        Ok(sqlx::query_scalar(
            "SELECT checkpoint FROM subscriptions WHERE subscriber_type = $1 AND key = $2",
        )
        .bind(LANE)
        .bind(lane.id().to_string())
        .fetch_optional(&self.pool)
        .await?)
    }

    async fn waits_on_the_tip_row(&self) -> anyhow::Result<bool> {
        a_backend_waits_on_the_tip_row(&self.pool).await
    }

    /// Stage the interleaving most tests share: the tip is parked inside a
    /// landing of events 2..=6 of unit `u` (entry lock held, committed
    /// checkpoint 1), and unit `u`'s lane has handled event 1 (another
    /// unit's, skipped) and is released into its first batch, which it can
    /// only land as its join.
    ///
    /// Returns once the lane is waiting on the tip row's lock.
    async fn stage_tip_entered_first(&self) -> anyhow::Result<TestLane> {
        self.publish(vec![ping("x", 0)]).await?;
        eventually(WAIT, || async {
            Ok(self.tip_row_checkpoint().await? == Some(1))
        })
        .await?;

        self.shared.handle_gate_seq.store(1, Ordering::SeqCst);
        self.shared.handle_gate.arm();
        let lane = self.open_lane("u", 0).await?;
        eventually(WAIT, || async { Ok(self.shared.handle_gate.is_parked()) }).await?;

        self.shared.tip_gate.arm();
        self.publish_pings("u", 2..7).await?;
        eventually(WAIT, || async { Ok(self.shared.tip_gate.is_parked()) }).await?;
        assert_eq!(self.tip_row_checkpoint().await?, Some(1));
        assert!(
            !self.waits_on_the_tip_row().await?,
            "control: nobody waits while the tip alone holds the row"
        );

        self.shared.handle_gate.release();
        eventually(WAIT, || async { self.waits_on_the_tip_row().await }).await?;
        assert_eq!(
            self.applied_count().await?,
            0,
            "the lane's first landing is parked behind the tip's"
        );
        Ok(lane)
    }
}

fn ping(key: &str, n: u64) -> TestEvent {
    TestEvent::Ping {
        key: key.to_string(),
        n,
    }
}

fn staged_config() -> JoinableConfig {
    test_config()
        .with_lane_max_batch_size(2)
        .with_max_join_gap(100)
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

// === The tip ===

#[tokio::test]
#[file_serial]
async fn the_tip_lands_under_scope_tip_and_mirrors_its_checkpoint() -> anyhow::Result<()> {
    let h = Harness::start(test_config()).await?;
    let unspawned: TestHandle = h
        .outbox
        .joinable(job::JobType::new("test-joinable-unspawned"));
    assert!(
        unspawned.load().await.is_err(),
        "an address used before spawn has no registration to read"
    );

    h.publish_pings("a", 1..4).await?;
    eventually(WAIT, || async {
        Ok(h.tip_row_checkpoint().await? == Some(3))
    })
    .await?;

    assert!(h.tip_flush_count().await? >= 1);
    assert!(
        h.lane_flushes().await?.is_empty(),
        "no lane exists, so every landing is the tip's"
    );
    assert_eq!(h.tip.load().await?.checkpoint(), EventSequence::from(3u64));
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn a_tip_landing_holds_the_tip_row_lock_before_flush_runs() -> anyhow::Result<()> {
    let h = Harness::start(test_config()).await?;

    h.publish_pings("a", 1..2).await?;
    eventually(WAIT, || async {
        Ok(!h.shared.tip_lock_held.lock().await.is_empty())
    })
    .await?;
    assert!(
        h.shared.tip_lock_held.lock().await.iter().all(|held| *held),
        "the lock is already held when flush starts"
    );

    // Control: the predicate is false on a transaction that never took the lock.
    let mut tx = h.pool.begin().await?;
    assert!(!holds_tip_row_lock(&mut *tx).await?);
    sqlx::query("SELECT checkpoint FROM joinable_tips WHERE subscriber_type = $1 FOR UPDATE")
        .bind(TIP)
        .fetch_one(&mut *tx)
        .await?;
    assert!(holds_tip_row_lock(&mut *tx).await?);
    tx.rollback().await?;
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn a_tip_landing_blocks_at_entry_while_the_tip_row_is_locked() -> anyhow::Result<()> {
    let h = Harness::start(test_config()).await?;
    assert!(
        !h.waits_on_the_tip_row().await?,
        "control: nothing waits while nobody holds the row"
    );

    let mut holder = h.pool.begin().await?;
    sqlx::query("SELECT checkpoint FROM joinable_tips WHERE subscriber_type = $1 FOR UPDATE")
        .bind(TIP)
        .fetch_one(&mut *holder)
        .await?;

    h.publish_pings("a", 1..2).await?;
    eventually(WAIT, || async { h.waits_on_the_tip_row().await }).await?;
    assert!(
        h.shared.tip_lock_held.lock().await.is_empty() && h.tip_flush_count().await? == 0,
        "the landing is parked at its first statement: flush has not run"
    );

    holder.rollback().await?;
    eventually(WAIT, || async { Ok(h.tip_flush_count().await? == 1) }).await?;
    assert_eq!(
        h.shared.tip_lock_held.lock().await.as_slice(),
        &[true],
        "once released the landing runs, lock held"
    );
    Ok(())
}

// === Catch-up lanes: the §7 contract ===

#[tokio::test]
#[file_serial]
async fn lane_joins_quiet_tip() -> anyhow::Result<()> {
    let h = Harness::start(test_config()).await?;
    h.publish(vec![ping("u", 1), ping("v", 2), ping("u", 3), ping("v", 4)])
        .await?;
    eventually(WAIT, || async {
        Ok(h.tip_row_checkpoint().await? == Some(4))
    })
    .await?;

    let lane = h.open_lane("u", 0).await?;
    let join = lane.await_join(WAIT).await?;

    assert!(u64::from(join) >= 4);
    assert_eq!(h.applied_by("catch_up", "u").await?, vec![1, 3]);
    assert!(
        h.applied_by("tip", "u").await?.is_empty(),
        "no event at or below the join is applied by the tip"
    );
    assert!(h.tip_row_checkpoint().await?.unwrap() <= u64::from(join) as i64);
    assert_eq!(h.lane_record(&lane).await?, Some(Some(4)));
    let flushes = h.lane_flushes().await?;
    assert_eq!(flushes.len(), 1);
    assert_eq!(
        flushes[0],
        (Some(u64::from(join) as i64), 2, true),
        "one join landing, over the unit's own events only, lock held"
    );

    // Control: past the join the tip applies to the unit, and the lane does not.
    h.publish(vec![ping("u", 5)]).await?;
    eventually(WAIT, || async {
        Ok(h.applied_by("tip", "u").await? == vec![5])
    })
    .await?;
    assert_eq!(h.applied_by("catch_up", "u").await?, vec![1, 3]);
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn tip_entered_first() -> anyhow::Result<()> {
    let h = Harness::start(staged_config()).await?;
    let lane = h.stage_tip_entered_first().await?;

    h.shared.tip_gate.release();
    let join = lane.await_join(WAIT).await?;

    assert_eq!(
        u64::from(join),
        6,
        "the join is at the end of the landing the tip held"
    );
    assert_eq!(
        h.applied_by("catch_up", "u").await?,
        vec![2, 3, 4, 5, 6],
        "what the tip skipped for the unit was applied by the lane, once"
    );
    assert!(h.applied_by("tip", "u").await?.is_empty());
    let flushes = h.lane_flushes().await?;
    assert_eq!(
        flushes,
        vec![(Some(6), 5, true)],
        "one landing, carrying the join, over the lane's whole batch"
    );
    assert_eq!(
        h.shared.drain_joins.lock().await.as_slice(),
        &[(4, 6), (5, 6), (6, 6)],
        "the drain handled what the tip had landed under the join's scope"
    );
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn join_entered_first() -> anyhow::Result<()> {
    let h = Harness::start(staged_config()).await?;
    let lane = h.stage_tip_entered_first().await?;

    // The lane will hold the lock in its drain, parked at event 4.
    h.shared.handle_gate_seq.store(4, Ordering::SeqCst);
    h.shared.handle_gate.arm();
    h.shared.tip_gate.release();
    eventually(WAIT, || async { Ok(h.shared.handle_gate.is_parked()) }).await?;
    assert!(
        !h.waits_on_the_tip_row().await?,
        "control: the lane got the row, so nobody waits yet"
    );
    assert_eq!(
        h.shared.drain_joins.lock().await.as_slice(),
        &[(4, 6)],
        "inside the drain, handle already runs under the join it will land"
    );

    h.publish(vec![ping("u", 7)]).await?;
    eventually(WAIT, || async { h.waits_on_the_tip_row().await }).await?;
    assert!(
        h.applied_by("tip", "u").await?.is_empty(),
        "the tip's next landing is parked at entry behind the join"
    );
    assert_eq!(h.lane_record(&lane).await?, None);

    h.shared.handle_gate.release();
    let join = lane.await_join(WAIT).await?;
    assert_eq!(u64::from(join), 6);
    eventually(WAIT, || async {
        Ok(h.applied_by("tip", "u").await? == vec![7])
    })
    .await?;
    assert_eq!(
        h.applied_by("catch_up", "u").await?,
        vec![2, 3, 4, 5, 6],
        "the lane applied up to the join and the tip only past it"
    );
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn lane_ahead_joins_at_own_position() -> anyhow::Result<()> {
    let slow_tip = JoinableConfig::new(
        OutboxEventJobConfig::new(job::JobType::new(TIP))
            .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL)
            .with_retry_settings(retry_settings(
                Duration::from_millis(1000),
                Duration::from_millis(1500),
            )),
    )
    .with_lane_retry_settings(fast_retry_settings());
    let h = Harness::start(slow_tip).await?;

    h.publish(vec![ping("x", 0)]).await?;
    eventually(WAIT, || async {
        Ok(h.tip_row_checkpoint().await? == Some(1))
    })
    .await?;

    // The lane is held at an event the tip has already passed.
    h.shared.handle_gate_seq.store(1, Ordering::SeqCst);
    h.shared.handle_gate.arm();
    let lane = h.open_lane("u", 0).await?;
    eventually(WAIT, || async { Ok(h.shared.handle_gate.is_parked()) }).await?;

    // The tip is stalled: every landing fails, and the retry backoff keeps it
    // out of the way, holding no lock.
    h.shared.fail_tip_flush.store(true, Ordering::SeqCst);
    h.publish_pings("u", 2..5).await?;
    eventually(WAIT, || async {
        Ok(h.shared.tip_flush_failures.load(Ordering::SeqCst) >= 1)
    })
    .await?;

    h.shared.handle_gate.release();
    let join = lane.await_join(WAIT).await?;

    assert_eq!(u64::from(join), 4, "the lane joined at its own position");
    assert_eq!(
        h.tip_row_checkpoint().await?,
        Some(1),
        "the tip is behind the join"
    );
    assert_eq!(h.applied_by("catch_up", "u").await?, vec![2, 3, 4]);

    // The tip recovers and replays from its own checkpoint, past the join.
    h.shared.fail_tip_flush.store(false, Ordering::SeqCst);
    h.publish(vec![ping("u", 5)]).await?;
    eventually(WAIT, || async {
        Ok(h.applied_by("tip", "u").await? == vec![5])
    })
    .await?;
    assert_eq!(
        h.applied_by("tip", "u").await?,
        vec![5],
        "the tip applies nothing at or below the join, though it replays it"
    );
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn birth_is_serialised_with_tip_landings() -> anyhow::Result<()> {
    let h = Harness::start(test_config()).await?;

    h.shared.tip_gate.arm();
    h.publish(vec![ping("u", 1)]).await?;
    eventually(WAIT, || async { Ok(h.shared.tip_gate.is_parked()) }).await?;
    assert!(
        !h.waits_on_the_tip_row().await?,
        "control: nobody waits while the tip alone holds the row"
    );

    let birth = {
        let outbox = h.outbox.clone();
        let tip = h.tip.clone();
        tokio::spawn(async move {
            let mut op = outbox.begin_op().await?;
            let lane = tip
                .open_lane_in_op(&mut op, "u".to_string(), EventSequence::BEGIN)
                .await
                .map_err(|e| anyhow::anyhow!("{e:?}"))?;
            op.commit().await?;
            anyhow::Ok(lane)
        })
    };
    eventually(WAIT, || async { h.waits_on_the_tip_row().await }).await?;
    assert!(
        !birth.is_finished(),
        "the birth waits for the landing in flight"
    );

    // Hold the lane back once it exists, so the tip's next landing meets it live.
    h.shared.handle_gate_seq.store(1, Ordering::SeqCst);
    h.shared.handle_gate.arm();
    h.shared.tip_gate.release();
    let lane = tokio::time::timeout(WAIT, birth).await???;
    eventually(WAIT, || async { Ok(h.shared.handle_gate.is_parked()) }).await?;

    h.publish(vec![ping("u", 2)]).await?;
    eventually(WAIT, || async {
        Ok(h.shared
            .tip_views
            .lock()
            .await
            .iter()
            .any(|view| view.catching_up == vec!["u".to_string()]))
    })
    .await?;
    let views = h.shared.tip_views.lock().await.clone();
    assert!(
        views
            .first()
            .is_some_and(|view| view.catching_up.is_empty()),
        "the landing that was in flight at birth did not see the lane: {views:?}"
    );

    h.shared.handle_gate.release();
    let join = lane.await_join(WAIT).await?;
    assert_eq!(u64::from(join), 2);
    assert_eq!(h.applied_by("catch_up", "u").await?, vec![1, 2]);
    assert!(h.applied_by("tip", "u").await?.is_empty());
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn join_drain_failure_replays() -> anyhow::Result<()> {
    let slow_lane_retry = staged_config().with_lane_retry_settings(retry_settings(
        Duration::from_secs(1),
        Duration::from_secs(2),
    ));
    let h = Harness::start(slow_lane_retry).await?;
    let lane = h.stage_tip_entered_first().await?;
    let checkpoint_before = h.lane_row_checkpoint(&lane).await?;

    // The first handle of event 4 happens inside the join landing's drain.
    h.shared.fail_handle_seq.store(4, Ordering::SeqCst);
    h.shared.fail_handle_armed.store(true, Ordering::SeqCst);
    h.shared.tip_gate.release();
    eventually(WAIT, || async {
        Ok(h.shared.handle_failures.load(Ordering::SeqCst) == 1)
    })
    .await?;

    assert_eq!(
        h.lane_record(&lane).await?,
        None,
        "a failed join landing leaves no join record"
    );
    assert_eq!(h.lane_row_checkpoint(&lane).await?, checkpoint_before);
    assert_eq!(h.applied_count().await?, 0, "nothing of it committed");

    let join = lane.await_join(WAIT).await?;
    assert!(u64::from(join) >= 6);
    assert_eq!(
        h.applied_by("catch_up", "u").await?,
        vec![2, 3, 4, 5, 6],
        "the retry applied everything up to the join exactly once"
    );
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn empty_batch_join_landing() -> anyhow::Result<()> {
    let h = Harness::start(test_config()).await?;
    h.publish(vec![TestEvent::Noise, TestEvent::Noise, TestEvent::Noise])
        .await?;
    eventually(WAIT, || async {
        Ok(h.tip_row_checkpoint().await? == Some(3))
    })
    .await?;

    let lane = h.open_lane("u", 0).await?;
    let join = lane.await_join(WAIT).await?;

    assert_eq!(u64::from(join), 3);
    assert_eq!(
        h.lane_flushes().await?,
        vec![(Some(3), 0, true)],
        "flush ran once, over an empty batch, carrying the join"
    );
    assert_eq!(h.lane_record(&lane).await?, Some(Some(3)));
    assert_eq!(h.applied_count().await?, 0);
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn max_join_gap_declines() -> anyhow::Result<()> {
    let h = Harness::start(staged_config().with_max_join_gap(1)).await?;
    let lane = h.stage_tip_entered_first().await?;

    h.shared.tip_gate.release();
    let join = lane.await_join(WAIT).await?;

    assert_eq!(u64::from(join), 6);
    assert_eq!(h.applied_by("catch_up", "u").await?, vec![2, 3, 4, 5, 6]);
    let flushes = h.lane_flushes().await?;
    assert_eq!(
        flushes.first(),
        Some(&(None, 2, true)),
        "the first landing declined the join and was ordinary, under the lock: {flushes:?}"
    );
    assert_eq!(
        flushes.last(),
        Some(&(Some(6), 1, true)),
        "and the lane kept catching up until it joined: {flushes:?}"
    );
    assert!(
        flushes[..flushes.len() - 1]
            .iter()
            .all(|(f, ..)| f.is_none()),
        "only the last landing carries a join: {flushes:?}"
    );
    assert!(
        flushes.iter().any(|(f, _, held)| f.is_none() && !held),
        "an ordinary landing short of the tip does not take the lock: {flushes:?}"
    );
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn tip_replay_after_join() -> anyhow::Result<()> {
    let h = Harness::start(test_config()).await?;
    h.publish_pings("u", 1..3).await?;
    eventually(WAIT, || async {
        Ok(h.tip_row_checkpoint().await? == Some(2))
    })
    .await?;
    let lane = h.open_lane("u", 0).await?;
    let join = lane.await_join(WAIT).await?;
    assert_eq!(u64::from(join), 2);

    h.shared.fail_tip_flush_once.store(true, Ordering::SeqCst);
    h.publish_pings("u", 3..4).await?;
    eventually(WAIT, || async {
        Ok(h.applied_by("tip", "u").await? == vec![3])
    })
    .await?;

    assert_eq!(h.shared.tip_flush_failures.load(Ordering::SeqCst), 1);
    assert_eq!(
        h.applied_by("tip", "u").await?,
        vec![3],
        "the retry applied past the join only, once"
    );
    assert_eq!(h.applied_by("catch_up", "u").await?, vec![1, 2]);
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn lanes_in_op_is_bounded() -> anyhow::Result<()> {
    let h = Harness::start(test_config()).await?;
    h.publish_pings("u", 1..4).await?;
    eventually(WAIT, || async {
        Ok(h.tip_row_checkpoint().await? == Some(3))
    })
    .await?;
    let lane = h.open_lane("u", 0).await?;
    assert_eq!(u64::from(lane.await_join(WAIT).await?), 3);

    h.publish_pings("u", 4..5).await?;
    eventually(WAIT, || async {
        Ok(h.tip_row_checkpoint().await? == Some(4))
    })
    .await?;
    h.publish_pings("u", 5..6).await?;
    eventually(WAIT, || async {
        Ok(h.tip_row_checkpoint().await? == Some(5))
    })
    .await?;

    let views = h.shared.tip_views.lock().await.clone();
    let listed = |since: u64| {
        views
            .iter()
            .find(|view| view.since == since)
            .map(|view| view.joined.len())
    };
    assert_eq!(
        listed(3),
        Some(1),
        "a landing whose previous checkpoint is the join sequence still lists the join: {views:?}"
    );
    assert_eq!(
        listed(4),
        Some(0),
        "once the tip's checkpoint is past the join sequence the join is no longer listed: {views:?}"
    );
    assert_eq!(
        h.applied_by("tip", "u").await?,
        vec![4, 5],
        "and the tip still applies to the unit, from its own seam state"
    );

    let mut op = h.outbox.begin_op().await?;
    let at_join = h
        .tip
        .lanes_in_op(&mut op, EventSequence::from(3u64))
        .await?;
    assert_eq!(at_join.joined.len(), 1);
    assert_eq!(at_join.joined[0].0, lane.id());
    assert_eq!(at_join.joined[0].2, EventSequence::from(3u64));
    let past_join = h
        .tip
        .lanes_in_op(&mut op, EventSequence::from(4u64))
        .await?;
    assert!(past_join.joined.is_empty());
    assert!(past_join.catching_up.is_empty());
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn undecodable_inside_drain() -> anyhow::Result<()> {
    let config = JoinableConfig::new(
        OutboxEventJobConfig::new(job::JobType::new(TIP))
            .with_checkpoint_interval(Duration::from_secs(3600))
            .with_retry_settings(fast_retry_settings()),
    )
    .with_lane_retry_settings(fast_retry_settings())
    .with_lane_max_batch_size(1)
    .with_max_join_gap(100);
    let h = Harness::start(config).await?;

    // The unit's own event at 1, so the lane's first batch is full at 1 and
    // its first landing attempt is the join that meets the poison row.
    h.publish(vec![ping("u", 0)]).await?;
    eventually(WAIT, || async {
        Ok(h.tip_row_checkpoint().await? == Some(1))
    })
    .await?;

    h.shared.handle_gate_seq.store(1, Ordering::SeqCst);
    h.shared.handle_gate.arm();
    let lane = h.open_lane("u", 0).await?;
    eventually(WAIT, || async { Ok(h.shared.handle_gate.is_parked()) }).await?;

    // The tip passes the poison row (event 2) in memory only: its committed
    // checkpoint stays 1 until the landing it is about to hold open.
    h.publish_poison().await?;
    eventually(WAIT, || async {
        Ok(h.shared.undecodable_calls.load(Ordering::SeqCst) == 1)
    })
    .await?;
    h.shared.tip_gate.arm();
    h.publish_pings("u", 3..5).await?;
    eventually(WAIT, || async { Ok(h.shared.tip_gate.is_parked()) }).await?;

    // From here the poison row is a failure for whoever meets it next.
    h.shared.undecodable_ok.store(false, Ordering::SeqCst);
    h.shared.handle_gate.release();
    eventually(WAIT, || async { h.waits_on_the_tip_row().await }).await?;
    h.shared.tip_gate.release();

    eventually(WAIT, || async {
        Ok(h.shared.undecodable_calls.load(Ordering::SeqCst) >= 2)
    })
    .await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        h.lane_record(&lane).await?,
        None,
        "the default handle_undecodable fails the join landing"
    );
    assert!(
        h.lane_row_checkpoint(&lane).await?.unwrap() < 2,
        "and the lane parks before the poison row"
    );
    assert_eq!(h.applied_count().await?, 0);

    h.shared.undecodable_ok.store(true, Ordering::SeqCst);
    let join = lane.await_join(WAIT).await?;
    assert_eq!(u64::from(join), 4);
    assert_eq!(
        h.applied_by("catch_up", "u").await?,
        vec![1, 3, 4],
        "an overriding Ok lets the join complete past the poison row, once"
    );
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn shutdown_between_landings_only() -> anyhow::Result<()> {
    let h = Harness::start(staged_config()).await?;
    let lane = h.stage_tip_entered_first().await?;
    let checkpoint_before = h.lane_row_checkpoint(&lane).await?;

    // Park the lane inside its drain, then ask the process to shut down.
    h.shared.handle_gate_seq.store(4, Ordering::SeqCst);
    h.shared.handle_gate.arm();
    h.shared.tip_gate.release();
    eventually(WAIT, || async { Ok(h.shared.handle_gate.is_parked()) }).await?;
    let shutdown = {
        let jobs = h.jobs.clone();
        tokio::spawn(async move { jobs.shutdown().await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    h.shared.handle_gate.release();
    tokio::time::timeout(WAIT, shutdown).await???;

    let record = h.lane_record(&lane).await?;
    match record {
        Some(join) => {
            assert_eq!(join, Some(6), "the landing completed whole, at the join");
            assert_eq!(h.applied_by("catch_up", "u").await?, vec![2, 3, 4, 5, 6]);
        }
        None => {
            assert_eq!(
                h.lane_row_checkpoint(&lane).await?,
                checkpoint_before,
                "or it rolled back whole: no checkpoint strictly between P and the join"
            );
            assert!(h.applied_by("catch_up", "u").await?.is_empty());
        }
    }
    Ok(())
}

// === The rest of the lane surface ===

#[tokio::test]
#[file_serial]
async fn a_birth_ahead_of_the_frontier_is_rejected() -> anyhow::Result<()> {
    let h = Harness::start(test_config()).await?;
    h.publish_pings("u", 1..3).await?;

    let mut op = h.outbox.begin_op().await?;
    let rejected = h
        .tip
        .open_lane_in_op(&mut op, "u".to_string(), EventSequence::from(99u64))
        .await;
    assert!(
        matches!(
            rejected,
            Err(es_entity::errlanes::Fail::Rejected(
                obix::SubscriptionRejection::BirthAheadOfFrontier { .. }
            ))
        ),
        "a birth above the frontier is a rejection"
    );
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn cancel_in_op_ends_a_lane_without_a_join() -> anyhow::Result<()> {
    let h = Harness::start(staged_config()).await?;
    let lane = h.stage_tip_entered_first().await?;

    let mut op = h.outbox.begin_op().await?;
    lane.cancel_in_op(&mut op).await?;
    op.commit().await?;
    assert_eq!(
        h.lane_record(&lane).await?,
        Some(None),
        "a cancelled lane is recorded with no join sequence"
    );
    assert!(
        h.has_tombstone(&lane).await?,
        "the ordinary keyed tombstone is left behind"
    );

    // The lane is parked on the tip's lock inside its join landing. Once the
    // tip commits, that landing must see the cancel, not claim a join.
    h.shared.tip_gate.release();
    eventually(WAIT, || async {
        Ok(h.tip_row_checkpoint().await? == Some(6))
    })
    .await?;
    let timeout = lane.await_join(Duration::from_millis(600)).await;
    assert!(
        matches!(
            timeout,
            Err(es_entity::errlanes::Fail::Rejected(
                obix::SubscriptionRejection::LaneJoinTimeout { .. }
            ))
        ),
        "a cancelled lane never joins"
    );
    assert_eq!(
        h.lane_record(&lane).await?,
        Some(None),
        "the cancel record is not overwritten by a join"
    );
    assert!(
        h.lane_flushes().await?.is_empty(),
        "nothing of a cancelled lane lands: no join flush ran"
    );
    assert!(
        h.applied_by("catch_up", "u").await?.is_empty(),
        "no catch-up write for the cancelled unit committed"
    );
    assert!(
        !h.waits_on_the_tip_row().await?,
        "the landing released the tip's lock"
    );
    let mut op = h.outbox.begin_op().await?;
    let lanes = h.tip.lanes_in_op(&mut op, EventSequence::BEGIN).await?;
    assert!(lanes.catching_up.is_empty() && lanes.joined.is_empty());
    Ok(())
}
