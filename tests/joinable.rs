//! Contracts for joinable subscriptions: a tip whose every landing takes the
//! tip row's lock at entry.

mod helpers;

use std::sync::Arc;
use std::time::Duration;

use obix::{
    EventSequence, FlushOp, Handled, InsertOrder, JoinableConfig, JoinableEventCtx, JoinableHandle,
    JoinableSubscriber, MailboxConfig, OutboxEventJobConfig, Scope, out::Outbox,
};
use serde::{Deserialize, Serialize};
use serial_test::file_serial;
use tokio::sync::Mutex;

use helpers::{TestTables, init_pool, wipeout_outbox_job_tables};

const TIP: &str = "test-joinable-tip";
const TEST_CHECKPOINT_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum TestEvent {
    Ping { key: String, n: u64 },
}

#[derive(Clone, Default)]
struct Shared {
    lock_held_in_flush: Arc<Mutex<Vec<bool>>>,
}

struct Applier {
    shared: Shared,
}

impl JoinableSubscriber<TestEvent> for Applier {
    type Unit = String;
    type Batch = Vec<(String, EventSequence)>;

    async fn handle<'inv>(
        &self,
        ctx: JoinableEventCtx<'inv, Self::Batch>,
        event: &obix::EventDelivery<TestEvent>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        let Some(TestEvent::Ping { key, .. }) = &event.payload else {
            return Ok(ctx.skip());
        };
        Ok(ctx.collect((key.clone(), event.sequence)))
    }

    async fn flush(
        &self,
        op: &mut FlushOp<'_, InsertOrder>,
        scope: Scope<'_, Self::Unit>,
        batch: Self::Batch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use es_entity::AtomicOperation;

        let held = holds_tip_row_lock(op.as_executor()).await?;
        self.shared.lock_held_in_flush.lock().await.push(held);

        let scope_name = match scope {
            Scope::Tip => "tip",
            Scope::CatchUp { .. } => "catch_up",
        };
        for (key, sequence) in batch {
            sqlx::query(
                "INSERT INTO test_joinable_applied (scope, unit, sequence) VALUES ($1, $2, $3)",
            )
            .bind(scope_name)
            .bind(key)
            .bind(u64::from(sequence) as i64)
            .execute(op.as_executor())
            .await?;
        }
        Ok(())
    }
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

async fn init_jobs(pool: &sqlx::PgPool) -> anyhow::Result<job::Jobs> {
    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    Ok(job::Jobs::init(job_config).await?)
}

async fn init_outbox(pool: &sqlx::PgPool) -> anyhow::Result<Outbox<TestEvent, TestTables>> {
    helpers::wipeout_outbox_tables(pool).await?;
    wipeout_outbox_job_tables(pool, TIP).await?;
    sqlx::query("DELETE FROM joinable_tips WHERE subscriber_type = $1")
        .bind(TIP)
        .execute(pool)
        .await?;
    sqlx::query("DROP TABLE IF EXISTS test_joinable_applied")
        .execute(pool)
        .await?;
    sqlx::query(
        "CREATE TABLE test_joinable_applied \
         (id BIGSERIAL PRIMARY KEY, scope TEXT NOT NULL, unit TEXT NOT NULL, sequence BIGINT NOT NULL)",
    )
    .execute(pool)
    .await?;
    Ok(Outbox::<TestEvent, TestTables>::init(pool, MailboxConfig::builder().build()).await?)
}

fn test_config() -> JoinableConfig {
    JoinableConfig::new(
        OutboxEventJobConfig::new(job::JobType::new(TIP))
            .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL),
    )
}

async fn publish_pings(
    outbox: &Outbox<TestEvent, TestTables>,
    key: &str,
    ns: std::ops::Range<u64>,
) -> anyhow::Result<()> {
    let mut op = outbox.begin_op().await?;
    for n in ns {
        outbox
            .publish_persisted_in_op(
                &mut op,
                TestEvent::Ping {
                    key: key.to_string(),
                    n,
                },
            )
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

async fn applied(pool: &sqlx::PgPool) -> anyhow::Result<Vec<(String, String, i64)>> {
    Ok(
        sqlx::query_as("SELECT scope, unit, sequence FROM test_joinable_applied ORDER BY id")
            .fetch_all(pool)
            .await?,
    )
}

async fn tip_row_checkpoint(pool: &sqlx::PgPool) -> anyhow::Result<Option<i64>> {
    Ok(
        sqlx::query_scalar("SELECT checkpoint FROM joinable_tips WHERE subscriber_type = $1")
            .bind(TIP)
            .fetch_one(pool)
            .await?,
    )
}

#[tokio::test]
#[file_serial]
async fn the_tip_lands_under_scope_tip_and_mirrors_its_checkpoint() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let tip: JoinableHandle<TestEvent, String, TestTables> =
        outbox.joinable(job::JobType::new(TIP));
    assert!(
        tip.load().await.is_err(),
        "an address used before spawn has no registration to read"
    );

    tip.spawn(
        &mut jobs,
        test_config(),
        Applier {
            shared: Shared::default(),
        },
    )
    .await?;
    jobs.start_poll().await?;

    publish_pings(&outbox, "a", 1..4).await?;
    eventually(Duration::from_secs(10), || async {
        Ok(applied(&pool).await?.len() == 3)
    })
    .await?;

    assert!(
        applied(&pool)
            .await?
            .iter()
            .all(|(scope, ..)| scope == "tip"),
        "every landing of the tip is Scope::Tip"
    );
    eventually(Duration::from_secs(10), || async {
        Ok(tip_row_checkpoint(&pool).await? == Some(3))
    })
    .await?;
    let snapshot = tip.load().await?;
    assert_eq!(snapshot.checkpoint(), EventSequence::from(3u64));
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn a_tip_landing_holds_the_tip_row_lock_before_flush_runs() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let shared = Shared::default();
    let tip: JoinableHandle<TestEvent, String, TestTables> =
        outbox.joinable(job::JobType::new(TIP));
    tip.spawn(
        &mut jobs,
        test_config(),
        Applier {
            shared: shared.clone(),
        },
    )
    .await?;
    jobs.start_poll().await?;

    publish_pings(&outbox, "a", 1..2).await?;
    eventually(Duration::from_secs(10), || async {
        Ok(!shared.lock_held_in_flush.lock().await.is_empty())
    })
    .await?;
    assert!(
        shared
            .lock_held_in_flush
            .lock()
            .await
            .iter()
            .all(|held| *held),
        "the lock is already held when flush starts"
    );

    // Control: the predicate is false on a transaction that never took the lock.
    let mut tx = pool.begin().await?;
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
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let shared = Shared::default();
    let tip: JoinableHandle<TestEvent, String, TestTables> =
        outbox.joinable(job::JobType::new(TIP));
    tip.spawn(
        &mut jobs,
        test_config(),
        Applier {
            shared: shared.clone(),
        },
    )
    .await?;
    jobs.start_poll().await?;

    // Let the tip report its checkpoint so its run-start write is out of the way.
    eventually(Duration::from_secs(10), || async {
        Ok(tip_row_checkpoint(&pool).await?.is_some())
    })
    .await?;
    assert!(
        !a_backend_waits_on_the_tip_row(&pool).await?,
        "control: nothing waits while nobody holds the row"
    );

    let mut holder = pool.begin().await?;
    sqlx::query("SELECT checkpoint FROM joinable_tips WHERE subscriber_type = $1 FOR UPDATE")
        .bind(TIP)
        .fetch_one(&mut *holder)
        .await?;

    publish_pings(&outbox, "a", 1..2).await?;
    eventually(Duration::from_secs(10), || async {
        a_backend_waits_on_the_tip_row(&pool).await
    })
    .await?;
    assert!(
        shared.lock_held_in_flush.lock().await.is_empty() && applied(&pool).await?.is_empty(),
        "the landing is parked at its first statement: flush has not run"
    );

    holder.rollback().await?;
    eventually(Duration::from_secs(10), || async {
        Ok(applied(&pool).await?.len() == 1)
    })
    .await?;
    assert_eq!(
        shared.lock_held_in_flush.lock().await.as_slice(),
        &[true],
        "once released the landing runs, lock held"
    );
    Ok(())
}
