mod helpers;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use obix::{
    EventCtx, Handled, MailboxConfig, OutboxEventJobConfig, SingletonSubscriber, StreamSelection,
    out::Outbox,
};
use serde::{Deserialize, Serialize};
use serial_test::file_serial;
use tokio::sync::Mutex;

use helpers::{TestTables, init_pool, wipeout_outbox_job_tables, wipeout_outbox_tables};

const JOB_TYPE: &str = "test-outbox-handler";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum TestEvent {
    Ping(u64),
}

/// Records deliveries and skips: no transaction is ever opened on its behalf.
struct SkippingObserver {
    received: Arc<Mutex<Vec<u64>>>,
}

impl SingletonSubscriber<TestEvent> for SkippingObserver {
    type Batch = ();

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv>,
        event: &obix::EventDelivery<TestEvent>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(TestEvent::Ping(n)) = &event.payload {
            self.received.lock().await.push(*n);
        }
        Ok(ctx.skip())
    }
}

/// A different event enum on the same table: publishing through it plants a row
/// the `TestEvent` consumers cannot decode.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "module")]
enum ForeignEvent {
    CoreParty { id: u64 },
}

async fn publish_foreign_poison(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    let foreign = Outbox::<ForeignEvent, TestTables>::init(
        pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;
    let mut op = pool.begin().await?;
    foreign
        .publish_persisted_in_op(&mut op, ForeignEvent::CoreParty { id: 1 })
        .await?;
    op.commit().await?;
    Ok(())
}

/// Acknowledges undecodable events (`Ok` instead of failing) and records their
/// sequences.
struct AckingObserver {
    received: Arc<Mutex<Vec<u64>>>,
    acked: Arc<Mutex<Vec<u64>>>,
}

impl SingletonSubscriber<TestEvent> for AckingObserver {
    type Batch = ();

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv>,
        event: &obix::EventDelivery<TestEvent>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(TestEvent::Ping(n)) = &event.payload {
            self.received.lock().await.push(*n);
        }
        Ok(ctx.skip())
    }

    async fn handle_undecodable(
        &self,
        error: &obix::UndecodableDelivery,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.acked.lock().await.push(u64::from(error.position()));
        Ok(())
    }
}

/// One isolated op + checkpoint per event.
struct CheckpointingObserver {
    received: Arc<Mutex<Vec<u64>>>,
}

impl SingletonSubscriber<TestEvent> for CheckpointingObserver {
    type Batch = ();

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv>,
        event: &obix::EventDelivery<TestEvent>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(TestEvent::Ping(n)) = &event.payload {
            self.received.lock().await.push(*n);
        }
        let op = ctx.consume().await?;
        Ok(op.commit())
    }
}

struct TestEphemeralHandler {
    received: Arc<Mutex<Vec<u64>>>,
}

impl SingletonSubscriber<TestEvent> for TestEphemeralHandler {
    type Batch = ();

    async fn handle_ephemeral(
        &self,
        event: &Arc<obix::out::EphemeralOutboxEvent<TestEvent>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let TestEvent::Ping(n) = &event.payload;
        self.received.lock().await.push(*n);
        Ok(())
    }
}

struct TestBothHandler {
    persistent_received: Arc<Mutex<Vec<u64>>>,
    ephemeral_received: Arc<Mutex<Vec<u64>>>,
}

impl SingletonSubscriber<TestEvent> for TestBothHandler {
    type Batch = ();

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv>,
        event: &obix::EventDelivery<TestEvent>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(TestEvent::Ping(n)) = &event.payload {
            self.persistent_received.lock().await.push(*n);
        }
        Ok(ctx.skip())
    }

    async fn handle_ephemeral(
        &self,
        event: &Arc<obix::out::EphemeralOutboxEvent<TestEvent>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let TestEvent::Ping(n) = &event.payload;
        self.ephemeral_received.lock().await.push(*n);
        Ok(())
    }
}

/// Takes any `AtomicOperation`, so handlers can pass `&mut op` directly and
/// exercise the direct impls rather than the `&mut *op` deref.
async fn insert_effect_in_op(
    op: &mut impl es_entity::AtomicOperation,
    n: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO test_batch_effects (n) VALUES ($1)")
        .bind(n)
        .execute(op.as_executor())
        .await?;
    Ok(())
}

/// Collects one item per event and inserts the accumulator in `flush`; sleeps
/// per event so the backlog stays ahead and batch composition is deterministic.
struct CollectingEffectHandler {
    deliveries: Arc<Mutex<Vec<u64>>>,
    fail_on_first: Option<u64>,
    failed: Arc<AtomicBool>,
}

impl SingletonSubscriber<TestEvent> for CollectingEffectHandler {
    type Batch = Vec<i64>;

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, Self::Batch>,
        event: &obix::EventDelivery<TestEvent>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        let Some(TestEvent::Ping(n)) = &event.payload else {
            return Ok(ctx.skip());
        };
        self.deliveries.lock().await.push(*n);
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        if self.fail_on_first == Some(*n) && !self.failed.swap(true, Ordering::SeqCst) {
            return Err("injected mid-batch failure".into());
        }
        Ok(ctx.collect(*n as i64))
    }

    async fn flush(
        &self,
        op: &mut obix::FlushOp<'_, obix::InsertOrder>,
        items: Self::Batch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use es_entity::AtomicOperation;
        // Inheriting the trait default would report false here; DbOp overrides
        // it to true.
        if !op.supports_hooks() {
            return Err("FlushOp must delegate supports_hooks to the inner DbOp".into());
        }
        for n in items {
            insert_effect_in_op(op, n).await?;
        }
        Ok(())
    }
}

/// Records ephemerals and snapshots the committed effect rows when one runs, so
/// a test can see whether an ephemeral interrupted an open batch.
struct SlowCollectingHandler {
    pool: sqlx::PgPool,
    ephemeral_received: Arc<Mutex<Vec<u64>>>,
    rows_at_ephemeral: Arc<Mutex<usize>>,
}

impl SingletonSubscriber<TestEvent> for SlowCollectingHandler {
    type Batch = Vec<i64>;

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, Self::Batch>,
        event: &obix::EventDelivery<TestEvent>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        let Some(TestEvent::Ping(n)) = &event.payload else {
            return Ok(ctx.skip());
        };
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        Ok(ctx.collect(*n as i64))
    }

    async fn flush(
        &self,
        op: &mut obix::FlushOp<'_, obix::InsertOrder>,
        items: Self::Batch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for n in items {
            insert_effect_in_op(op, n).await?;
        }
        Ok(())
    }

    async fn handle_ephemeral(
        &self,
        event: &Arc<obix::out::EphemeralOutboxEvent<TestEvent>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let TestEvent::Ping(n) = &event.payload;
        self.ephemeral_received.lock().await.push(*n);
        let rows = batch_effect_rows(&self.pool)
            .await
            .expect("read effect rows")
            .len();
        *self.rows_at_ephemeral.lock().await = rows;
        Ok(())
    }
}

/// Collects into a `Vec` with no per-event statement and applies the batch in
/// one `flush`; optionally fails the first flush so replay re-collects.
struct CollectingHandler {
    flush_sizes: Arc<Mutex<Vec<usize>>>,
    fail_first_flush: Arc<AtomicBool>,
}

impl SingletonSubscriber<TestEvent> for CollectingHandler {
    type Batch = Vec<i64>;

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, Vec<i64>>,
        event: &obix::EventDelivery<TestEvent>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        let Some(TestEvent::Ping(n)) = &event.payload else {
            return Ok(ctx.skip());
        };
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        Ok(ctx.collect(*n as i64))
    }

    async fn flush(
        &self,
        op: &mut obix::FlushOp<'_, obix::InsertOrder>,
        items: Vec<i64>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use es_entity::AtomicOperation;
        // The same delegation, pinned on FlushOp.
        if !op.supports_hooks() {
            return Err("FlushOp must delegate supports_hooks to the inner DbOp".into());
        }
        if self.fail_first_flush.swap(false, Ordering::SeqCst) {
            return Err("injected flush failure".into());
        }
        self.flush_sizes.lock().await.push(items.len());
        for n in items {
            insert_effect_in_op(op, n).await?;
        }
        Ok(())
    }
}

/// Coalesces events into a `HashMap` by key (`n % 2`), so only the last value
/// per key reaches the flush.
struct CoalescingHandler {
    flush_sizes: Arc<Mutex<Vec<usize>>>,
}

impl SingletonSubscriber<TestEvent> for CoalescingHandler {
    type Batch = std::collections::HashMap<i64, i64>;

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, Self::Batch>,
        event: &obix::EventDelivery<TestEvent>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        let Some(TestEvent::Ping(n)) = &event.payload else {
            return Ok(ctx.skip());
        };
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        let n = *n as i64;
        Ok(ctx.collect(n % 2, n))
    }

    async fn flush(
        &self,
        op: &mut obix::FlushOp<'_, obix::InsertOrder>,
        items: Self::Batch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.flush_sizes.lock().await.push(items.len());
        for (_key, latest) in items {
            insert_effect_in_op(op, latest).await?;
        }
        Ok(())
    }
}

/// Collects everything except one event value, which it handles isolated, so
/// the isolation fence must land the collected items first.
struct CollectThenIsolateHandler {
    deliveries: Arc<Mutex<Vec<u64>>>,
    isolate_on: u64,
    fail_isolated_once: Arc<AtomicBool>,
}

impl SingletonSubscriber<TestEvent> for CollectThenIsolateHandler {
    type Batch = Vec<i64>;

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, Vec<i64>>,
        event: &obix::EventDelivery<TestEvent>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        let Some(TestEvent::Ping(n)) = &event.payload else {
            return Ok(ctx.skip());
        };
        self.deliveries.lock().await.push(*n);
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        if *n == self.isolate_on {
            let mut op = ctx.consume().await?;
            insert_effect_in_op(&mut op, *n as i64).await?;
            if !self.fail_isolated_once.swap(true, Ordering::SeqCst) {
                return Err("injected isolated failure".into());
            }
            Ok(op.commit())
        } else {
            Ok(ctx.collect(*n as i64))
        }
    }

    async fn flush(
        &self,
        op: &mut obix::FlushOp<'_, obix::InsertOrder>,
        items: Vec<i64>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for n in items {
            insert_effect_in_op(op, n).await?;
        }
        Ok(())
    }
}

/// An `All` subscription with a slow ephemeral path: under a saturating flood
/// the ephemeral stream is always ready, so static priority would starve.
struct FairnessProbeHandler {
    persistent_received: Arc<Mutex<Vec<u64>>>,
}

impl SingletonSubscriber<TestEvent> for FairnessProbeHandler {
    type Batch = ();

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv>,
        event: &obix::EventDelivery<TestEvent>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(TestEvent::Ping(n)) = &event.payload {
            self.persistent_received.lock().await.push(*n);
        }
        Ok(ctx.skip())
    }

    async fn handle_ephemeral(
        &self,
        _event: &Arc<obix::out::EphemeralOutboxEvent<TestEvent>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Slower than the flood's inter-arrival time, so the ephemeral channel
        // never goes empty.
        tokio::time::sleep(std::time::Duration::from_millis(15)).await;
        Ok(())
    }
}

/// Declares `PersistentOnly`, so its `handle_ephemeral` override is dead code
/// that must never run.
struct PersistentOnlyHandler {
    persistent_received: Arc<Mutex<Vec<u64>>>,
    ephemeral_received: Arc<Mutex<Vec<u64>>>,
}

impl SingletonSubscriber<TestEvent> for PersistentOnlyHandler {
    const SUBSCRIPTION: StreamSelection = StreamSelection::PersistentOnly;
    type Batch = ();

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv>,
        event: &obix::EventDelivery<TestEvent>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(TestEvent::Ping(n)) = &event.payload {
            self.persistent_received.lock().await.push(*n);
        }
        Ok(ctx.skip())
    }

    async fn handle_ephemeral(
        &self,
        event: &Arc<obix::out::EphemeralOutboxEvent<TestEvent>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let TestEvent::Ping(n) = &event.payload;
        self.ephemeral_received.lock().await.push(*n);
        Ok(())
    }
}

/// Declares `EphemeralOnly`: no persistent deliveries, and the job must never
/// write execution state.
struct EphemeralOnlyHandler {
    persistent_received: Arc<Mutex<Vec<u64>>>,
    ephemeral_received: Arc<Mutex<Vec<u64>>>,
}

impl SingletonSubscriber<TestEvent> for EphemeralOnlyHandler {
    const SUBSCRIPTION: StreamSelection = StreamSelection::EphemeralOnly;
    type Batch = ();

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv>,
        event: &obix::EventDelivery<TestEvent>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(TestEvent::Ping(n)) = &event.payload {
            self.persistent_received.lock().await.push(*n);
        }
        Ok(ctx.skip())
    }

    async fn handle_ephemeral(
        &self,
        event: &Arc<obix::out::EphemeralOutboxEvent<TestEvent>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let TestEvent::Ping(n) = &event.payload;
        self.ephemeral_received.lock().await.push(*n);
        Ok(())
    }
}

async fn init_outbox_with_handler<H: SingletonSubscriber<TestEvent>>(
    pool: &sqlx::PgPool,
    jobs: &mut job::Jobs,
    handler: H,
) -> anyhow::Result<Outbox<TestEvent, TestTables>> {
    init_outbox_with_handler_config(
        pool,
        jobs,
        OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE)),
        handler,
    )
    .await
}

async fn init_outbox_with_handler_config<H: SingletonSubscriber<TestEvent>>(
    pool: &sqlx::PgPool,
    jobs: &mut job::Jobs,
    config: OutboxEventJobConfig,
    handler: H,
) -> anyhow::Result<Outbox<TestEvent, TestTables>> {
    wipeout_outbox_tables(pool).await?;
    wipeout_outbox_job_tables(pool, JOB_TYPE).await?;

    let outbox = Outbox::<TestEvent, TestTables>::init(
        pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    outbox
        .register_singleton_subscriber(jobs, config, handler)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    Ok(outbox)
}

fn fast_retry_settings() -> job::RetrySettings {
    let mut settings = job::RetrySettings::repeat_indefinitely();
    settings.min_backoff = std::time::Duration::from_millis(50);
    settings.max_backoff = std::time::Duration::from_millis(100);
    settings.backoff_jitter_pct = 0;
    settings
}

async fn wait_for_n_deliveries(
    received: &Mutex<Vec<u64>>,
    n: usize,
    timeout: std::time::Duration,
) -> anyhow::Result<()> {
    let start = std::time::Instant::now();
    loop {
        if received.lock().await.len() >= n {
            return Ok(());
        }
        if start.elapsed() > timeout {
            anyhow::bail!("Timeout waiting for {n} deliveries");
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// Mirrors the one field of obix's crate-private `OutboxEventJobState` these
/// tests read.
#[derive(Deserialize)]
struct CheckpointState {
    sequence: i64,
}

/// Reads the checkpoint through the `job` crate's public API rather than its
/// storage tables.
async fn checkpoint_sequence(jobs: &job::Jobs) -> anyhow::Result<Option<i64>> {
    let Some(handle) = jobs.resident_handle(job::JobType::new(JOB_TYPE)).await? else {
        return Ok(None);
    };
    Ok(handle
        .execution_state::<CheckpointState>()
        .await?
        .map(|s| s.sequence))
}

async fn wait_for_checkpoint(jobs: &job::Jobs, expected: i64) -> anyhow::Result<()> {
    let start = std::time::Instant::now();
    loop {
        if checkpoint_sequence(jobs).await? == Some(expected) {
            return Ok(());
        }
        if start.elapsed() > std::time::Duration::from_secs(5) {
            anyhow::bail!(
                "Timeout waiting for checkpoint to reach {expected}, at {:?}",
                checkpoint_sequence(jobs).await?
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

async fn reset_batch_effects_table(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    sqlx::query("DROP TABLE IF EXISTS test_batch_effects")
        .execute(pool)
        .await?;
    sqlx::query("CREATE TABLE test_batch_effects (n BIGINT PRIMARY KEY)")
        .execute(pool)
        .await?;
    Ok(())
}

async fn batch_effect_rows(pool: &sqlx::PgPool) -> anyhow::Result<Vec<i64>> {
    let rows: Vec<(i64,)> = sqlx::query_as("SELECT n FROM test_batch_effects ORDER BY n")
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().map(|(n,)| n).collect())
}

async fn wait_for_effect_rows(pool: &sqlx::PgPool, n: usize) -> anyhow::Result<()> {
    let start = std::time::Instant::now();
    loop {
        if batch_effect_rows(pool).await?.len() >= n {
            return Ok(());
        }
        if start.elapsed() > std::time::Duration::from_secs(10) {
            anyhow::bail!(
                "Timeout waiting for {n} effect rows, at {:?}",
                batch_effect_rows(pool).await?
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

#[tokio::test]
#[file_serial]
async fn handler_receives_persistent_events() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let received = Arc::new(Mutex::new(Vec::new()));
    let outbox = init_outbox_with_handler(
        &pool,
        &mut jobs,
        SkippingObserver {
            received: received.clone(),
        },
    )
    .await?;

    jobs.start_poll().await?;

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(1))
        .await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(2))
        .await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(3))
        .await?;
    op.commit().await?;

    wait_for_n_deliveries(&received, 3, std::time::Duration::from_secs(5)).await?;
    assert_eq!(*received.lock().await, vec![1, 2, 3]);

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn handler_receives_ephemeral_events() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let received = Arc::new(Mutex::new(Vec::new()));
    let outbox = init_outbox_with_handler(
        &pool,
        &mut jobs,
        TestEphemeralHandler {
            received: received.clone(),
        },
    )
    .await?;

    jobs.start_poll().await?;

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let event_type = obix::out::EphemeralEventType::new("test_type");
    outbox
        .publish_ephemeral(event_type.clone(), TestEvent::Ping(42))
        .await?;

    let start = std::time::Instant::now();
    loop {
        let events = received.lock().await;
        if !events.is_empty() {
            assert!(events.iter().all(|&v| v == 42));
            break;
        }
        drop(events);
        if start.elapsed() > std::time::Duration::from_secs(5) {
            anyhow::bail!("Timeout waiting for ephemeral events");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn handler_resumes_from_last_sequence_on_restart() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    // First run.
    let received_first = Arc::new(Mutex::new(Vec::new()));
    {
        let job_config = job::JobSvcConfig::builder()
            .pool(pool.clone())
            .build()
            .unwrap();
        let mut jobs = job::Jobs::init(job_config).await?;

        let outbox = init_outbox_with_handler(
            &pool,
            &mut jobs,
            CheckpointingObserver {
                received: received_first.clone(),
            },
        )
        .await?;

        jobs.start_poll().await?;

        let mut op = outbox.begin_op().await?;
        outbox
            .publish_persisted_in_op(&mut op, TestEvent::Ping(10))
            .await?;
        outbox
            .publish_persisted_in_op(&mut op, TestEvent::Ping(20))
            .await?;
        op.commit().await?;

        wait_for_n_deliveries(&received_first, 2, std::time::Duration::from_secs(5)).await?;

        jobs.shutdown().await?;
    }

    // Second run: 10 and 20 must not come back.
    let received_second = Arc::new(Mutex::new(Vec::new()));
    {
        let job_config = job::JobSvcConfig::builder()
            .pool(pool.clone())
            .build()
            .unwrap();
        let mut jobs = job::Jobs::init(job_config).await?;

        // Re-init without wiping: the sequence state must survive.
        let outbox = Outbox::<TestEvent, TestTables>::init(
            &pool,
            MailboxConfig::builder()
                .build()
                .expect("Couldn't build MailboxConfig"),
        )
        .await?;

        outbox
            .register_singleton_subscriber(
                &mut jobs,
                OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE)),
                CheckpointingObserver {
                    received: received_second.clone(),
                },
            )
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        jobs.start_poll().await?;

        let mut op = outbox.begin_op().await?;
        outbox
            .publish_persisted_in_op(&mut op, TestEvent::Ping(30))
            .await?;
        op.commit().await?;

        let start = std::time::Instant::now();
        loop {
            let events = received_second.lock().await;
            if !events.is_empty() {
                assert_eq!(*events, vec![30]);
                break;
            }
            drop(events);
            if start.elapsed() > std::time::Duration::from_secs(5) {
                anyhow::bail!("Timeout waiting for second-run events");
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        // Settle, so a stale event would have time to arrive.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let events = received_second.lock().await;
        assert_eq!(*events, vec![30]);
    }

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn handler_receives_both_persistent_and_ephemeral() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let persistent_received = Arc::new(Mutex::new(Vec::new()));
    let ephemeral_received = Arc::new(Mutex::new(Vec::new()));

    let outbox = init_outbox_with_handler(
        &pool,
        &mut jobs,
        TestBothHandler {
            persistent_received: persistent_received.clone(),
            ephemeral_received: ephemeral_received.clone(),
        },
    )
    .await?;

    jobs.start_poll().await?;

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(100))
        .await?;
    op.commit().await?;

    let event_type = obix::out::EphemeralEventType::new("both_test");
    outbox
        .publish_ephemeral(event_type, TestEvent::Ping(200))
        .await?;

    let start = std::time::Instant::now();
    loop {
        let p = persistent_received.lock().await;
        let e = ephemeral_received.lock().await;
        if !p.is_empty() && !e.is_empty() {
            assert_eq!(*p, vec![100]);
            assert!(e.iter().all(|&v| v == 200));
            break;
        }
        drop(p);
        drop(e);
        if start.elapsed() > std::time::Duration::from_secs(5) {
            anyhow::bail!("Timeout waiting for both event types");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn collected_batch_replays_wholesale_on_mid_batch_failure() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    reset_batch_effects_table(&pool).await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let deliveries = Arc::new(Mutex::new(Vec::new()));
    let config = OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
        .with_retry_settings(fast_retry_settings());
    let outbox = init_outbox_with_handler_config(
        &pool,
        &mut jobs,
        config,
        CollectingEffectHandler {
            deliveries: deliveries.clone(),
            fail_on_first: Some(2),
            failed: Arc::new(AtomicBool::new(false)),
        },
    )
    .await?;

    // Publish before the job starts so the events arrive as ready backlog.
    const N: u64 = 5;
    let mut op = outbox.begin_op().await?;
    for n in 1..=N {
        outbox
            .publish_persisted_in_op(&mut op, TestEvent::Ping(n))
            .await?;
    }
    op.commit().await?;

    jobs.start_poll().await?;

    wait_for_effect_rows(&pool, N as usize).await?;

    // Exactly-once DB effects despite the replay.
    assert_eq!(batch_effect_rows(&pool).await?, vec![1, 2, 3, 4, 5]);

    // Event 1 shared the batch event 2 poisoned, so it was delivered again on
    // replay: per-batch fate sharing, not per-event.
    let deliveries = deliveries.lock().await;
    let event_1_deliveries = deliveries.iter().filter(|&&n| n == 1).count();
    assert!(
        event_1_deliveries >= 2,
        "expected event 1 to replay with the poisoned batch, deliveries: {deliveries:?}"
    );

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn skipped_events_advance_checkpoint_lazily() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let received = Arc::new(Mutex::new(Vec::new()));
    let config = OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
        .with_checkpoint_interval(std::time::Duration::from_millis(100));
    let outbox = init_outbox_with_handler_config(
        &pool,
        &mut jobs,
        config,
        SkippingObserver {
            received: received.clone(),
        },
    )
    .await?;

    jobs.start_poll().await?;

    let mut op = outbox.begin_op().await?;
    for n in 1..=3u64 {
        outbox
            .publish_persisted_in_op(&mut op, TestEvent::Ping(n))
            .await?;
    }
    op.commit().await?;

    wait_for_n_deliveries(&received, 3, std::time::Duration::from_secs(5)).await?;

    // No transaction ever ran for these events, yet the checkpoint catches up
    // via the standalone pointer write.
    wait_for_checkpoint(&jobs, 3).await?;

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn ephemeral_never_interrupts_an_open_batch() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    reset_batch_effects_table(&pool).await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let ephemeral_received = Arc::new(Mutex::new(Vec::new()));
    let rows_at_ephemeral = Arc::new(Mutex::new(0usize));
    let outbox = init_outbox_with_handler(
        &pool,
        &mut jobs,
        SlowCollectingHandler {
            pool: pool.clone(),
            ephemeral_received: ephemeral_received.clone(),
            rows_at_ephemeral: rows_at_ephemeral.clone(),
        },
    )
    .await?;

    // A backlog of slow (100ms each) events, so the ephemeral below lands while
    // the batch is guaranteed to still be open.
    const N: u64 = 5;
    let mut op = outbox.begin_op().await?;
    for n in 1..=N {
        outbox
            .publish_persisted_in_op(&mut op, TestEvent::Ping(n))
            .await?;
    }
    op.commit().await?;

    jobs.start_poll().await?;

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    outbox
        .publish_ephemeral(
            obix::out::EphemeralEventType::new("mid_batch"),
            TestEvent::Ping(9),
        )
        .await?;

    wait_for_effect_rows(&pool, N as usize).await?;
    wait_for_n_deliveries(&ephemeral_received, 1, std::time::Duration::from_secs(5)).await?;

    // Ephemerals are handled between batches, so the mid-drain arrival must not
    // have truncated the batch.
    let rows_at_ephemeral = *rows_at_ephemeral.lock().await;
    assert_eq!(
        rows_at_ephemeral, N as usize,
        "expected the ephemeral to run at the batch boundary, after the whole batch landed"
    );
    assert_eq!(batch_effect_rows(&pool).await?, vec![1, 2, 3, 4, 5]);

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn single_collected_event_commits_promptly_at_low_traffic() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    reset_batch_effects_table(&pool).await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let deliveries = Arc::new(Mutex::new(Vec::new()));
    let outbox = init_outbox_with_handler(
        &pool,
        &mut jobs,
        CollectingEffectHandler {
            deliveries: deliveries.clone(),
            fail_on_first: None,
            failed: Arc::new(AtomicBool::new(false)),
        },
    )
    .await?;

    jobs.start_poll().await?;

    // Let the job start and go idle.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let published_at = std::time::Instant::now();
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(1))
        .await?;
    op.commit().await?;

    // A pending batch is never held open waiting for future events: draining the
    // backlog lands the batch of one, work and checkpoint together.
    wait_for_effect_rows(&pool, 1).await?;
    wait_for_checkpoint(&jobs, 1).await?;
    let latency = published_at.elapsed();
    assert!(
        latency < std::time::Duration::from_secs(2),
        "single collected event took {latency:?} to land"
    );

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn collected_events_flush_once_per_batch() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    reset_batch_effects_table(&pool).await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let flush_sizes = Arc::new(Mutex::new(Vec::new()));
    let outbox = init_outbox_with_handler(
        &pool,
        &mut jobs,
        CollectingHandler {
            flush_sizes: flush_sizes.clone(),
            fail_first_flush: Arc::new(AtomicBool::new(false)),
        },
    )
    .await?;

    // Publish before the job starts so the events arrive as ready backlog.
    const N: u64 = 5;
    let mut op = outbox.begin_op().await?;
    for n in 1..=N {
        outbox
            .publish_persisted_in_op(&mut op, TestEvent::Ping(n))
            .await?;
    }
    op.commit().await?;

    jobs.start_poll().await?;

    wait_for_effect_rows(&pool, N as usize).await?;
    wait_for_checkpoint(&jobs, N as i64).await?;

    // One flush applied the whole burst, inside the checkpoint's transaction.
    assert_eq!(batch_effect_rows(&pool).await?, vec![1, 2, 3, 4, 5]);
    assert_eq!(*flush_sizes.lock().await, vec![N as usize]);

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn batch_full_bounds_collected_batches() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    reset_batch_effects_table(&pool).await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let flush_sizes = Arc::new(Mutex::new(Vec::new()));
    let config = OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE)).with_max_batch_size(3);
    let outbox = init_outbox_with_handler_config(
        &pool,
        &mut jobs,
        config,
        CollectingHandler {
            flush_sizes: flush_sizes.clone(),
            fail_first_flush: Arc::new(AtomicBool::new(false)),
        },
    )
    .await?;

    const N: u64 = 5;
    let mut op = outbox.begin_op().await?;
    for n in 1..=N {
        outbox
            .publish_persisted_in_op(&mut op, TestEvent::Ping(n))
            .await?;
    }
    op.commit().await?;

    jobs.start_poll().await?;

    wait_for_effect_rows(&pool, N as usize).await?;

    // Collected events count toward max_batch_size, so the burst of 5 was
    // force-flushed at 3.
    assert_eq!(batch_effect_rows(&pool).await?, vec![1, 2, 3, 4, 5]);
    assert_eq!(*flush_sizes.lock().await, vec![3, 2]);

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn failed_flush_replays_and_recollects() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    reset_batch_effects_table(&pool).await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let flush_sizes = Arc::new(Mutex::new(Vec::new()));
    let config = OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
        .with_retry_settings(fast_retry_settings());
    let outbox = init_outbox_with_handler_config(
        &pool,
        &mut jobs,
        config,
        CollectingHandler {
            flush_sizes: flush_sizes.clone(),
            fail_first_flush: Arc::new(AtomicBool::new(true)),
        },
    )
    .await?;

    const N: u64 = 5;
    let mut op = outbox.begin_op().await?;
    for n in 1..=N {
        outbox
            .publish_persisted_in_op(&mut op, TestEvent::Ping(n))
            .await?;
    }
    op.commit().await?;

    jobs.start_poll().await?;

    wait_for_effect_rows(&pool, N as usize).await?;
    wait_for_checkpoint(&jobs, N as i64).await?;

    // The failed flush dropped its items with the op, so the retry re-collected
    // from scratch; the primary key proves nothing was applied twice.
    assert_eq!(batch_effect_rows(&pool).await?, vec![1, 2, 3, 4, 5]);
    assert_eq!(*flush_sizes.lock().await, vec![N as usize]);

    Ok(())
}
#[tokio::test]
#[file_serial]
async fn consume_entry_flushes_collected_items_first() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    reset_batch_effects_table(&pool).await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let deliveries = Arc::new(Mutex::new(Vec::new()));
    let config = OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
        .with_retry_settings(fast_retry_settings());
    let outbox = init_outbox_with_handler_config(
        &pool,
        &mut jobs,
        config,
        CollectThenIsolateHandler {
            deliveries: deliveries.clone(),
            isolate_on: 3,
            fail_isolated_once: Arc::new(AtomicBool::new(false)),
        },
    )
    .await?;

    const N: u64 = 3;
    let mut op = outbox.begin_op().await?;
    for n in 1..=N {
        outbox
            .publish_persisted_in_op(&mut op, TestEvent::Ping(n))
            .await?;
    }
    op.commit().await?;

    jobs.start_poll().await?;

    wait_for_effect_rows(&pool, N as usize).await?;
    assert_eq!(batch_effect_rows(&pool).await?, vec![1, 2, 3]);

    // The fence flushed items 1 and 2 before event 3's op existed, so the
    // injected failure replayed event 3 alone.
    let deliveries = deliveries.lock().await;
    let count = |v: u64| deliveries.iter().filter(|&&n| n == v).count();
    assert_eq!(
        count(1),
        1,
        "event 1 must not replay with the isolated failure, deliveries: {deliveries:?}"
    );
    assert_eq!(
        count(2),
        1,
        "event 2 must not replay with the isolated failure, deliveries: {deliveries:?}"
    );
    assert!(
        count(3) >= 2,
        "event 3 must replay alone, deliveries: {deliveries:?}"
    );

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn hashmap_collect_coalesces_by_key() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    reset_batch_effects_table(&pool).await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let flush_sizes = Arc::new(Mutex::new(Vec::new()));
    let outbox = init_outbox_with_handler(
        &pool,
        &mut jobs,
        CoalescingHandler {
            flush_sizes: flush_sizes.clone(),
        },
    )
    .await?;

    // Five events over two keys (n % 2), in ascending sequence, so last-write
    // wins keeps 4 (key 0) and 5 (key 1).
    const N: u64 = 5;
    let mut op = outbox.begin_op().await?;
    for n in 1..=N {
        outbox
            .publish_persisted_in_op(&mut op, TestEvent::Ping(n))
            .await?;
    }
    op.commit().await?;

    jobs.start_poll().await?;

    wait_for_effect_rows(&pool, 2).await?;
    wait_for_checkpoint(&jobs, N as i64).await?;

    assert_eq!(batch_effect_rows(&pool).await?, vec![4, 5]);
    assert_eq!(*flush_sizes.lock().await, vec![2]);

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn single_collected_event_flushes_promptly_at_low_traffic() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    reset_batch_effects_table(&pool).await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let flush_sizes = Arc::new(Mutex::new(Vec::new()));
    let outbox = init_outbox_with_handler(
        &pool,
        &mut jobs,
        CollectingHandler {
            flush_sizes: flush_sizes.clone(),
            fail_first_flush: Arc::new(AtomicBool::new(false)),
        },
    )
    .await?;

    jobs.start_poll().await?;

    // Let the job start and go idle.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let published_at = std::time::Instant::now();
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(1))
        .await?;
    op.commit().await?;

    // Collected items are not held for future events either, and no transaction
    // existed before the flush instant.
    wait_for_effect_rows(&pool, 1).await?;
    wait_for_checkpoint(&jobs, 1).await?;
    let latency = published_at.elapsed();
    assert!(
        latency < std::time::Duration::from_secs(2),
        "single collected event took {latency:?} to land"
    );
    assert_eq!(*flush_sizes.lock().await, vec![1]);

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn continuous_ephemeral_traffic_does_not_starve_persistent_events() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let persistent_received = Arc::new(Mutex::new(Vec::new()));
    let outbox = init_outbox_with_handler(
        &pool,
        &mut jobs,
        FairnessProbeHandler {
            persistent_received: persistent_received.clone(),
        },
    )
    .await?;

    jobs.start_poll().await?;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Inter-arrival (~5ms) is shorter than the handler's ephemeral handling time
    // (15ms), so the ephemeral stream is always ready.
    let flood_outbox = outbox.clone();
    let flood = tokio::spawn(async move {
        let event_type = obix::out::EphemeralEventType::new("flood");
        loop {
            if flood_outbox
                .publish_ephemeral(event_type.clone(), TestEvent::Ping(0))
                .await
                .is_err()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    });

    // Let the flood saturate the channel before the persistent event lands.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(1))
        .await?;
    op.commit().await?;

    let delivered =
        wait_for_n_deliveries(&persistent_received, 1, std::time::Duration::from_secs(5)).await;
    flood.abort();
    delivered?;

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn persistent_only_handler_never_subscribes_ephemerals() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let persistent_received = Arc::new(Mutex::new(Vec::new()));
    let ephemeral_received = Arc::new(Mutex::new(Vec::new()));
    let outbox = init_outbox_with_handler(
        &pool,
        &mut jobs,
        PersistentOnlyHandler {
            persistent_received: persistent_received.clone(),
            ephemeral_received: ephemeral_received.clone(),
        },
    )
    .await?;

    jobs.start_poll().await?;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let event_type = obix::out::EphemeralEventType::new("unsubscribed");
    outbox
        .publish_ephemeral(event_type.clone(), TestEvent::Ping(99))
        .await?;

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(1))
        .await?;
    op.commit().await?;

    wait_for_n_deliveries(&persistent_received, 1, std::time::Duration::from_secs(5)).await?;

    // Settle, so the never-subscribed ephemeral stream would have had time to
    // run the override.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(*persistent_received.lock().await, vec![1]);
    assert!(
        ephemeral_received.lock().await.is_empty(),
        "PersistentOnly handler must never receive ephemeral events"
    );

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn ephemeral_only_handler_skips_checkpoint_machinery() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let persistent_received = Arc::new(Mutex::new(Vec::new()));
    let ephemeral_received = Arc::new(Mutex::new(Vec::new()));
    let outbox = init_outbox_with_handler(
        &pool,
        &mut jobs,
        EphemeralOnlyHandler {
            persistent_received: persistent_received.clone(),
            ephemeral_received: ephemeral_received.clone(),
        },
    )
    .await?;

    jobs.start_poll().await?;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(1))
        .await?;
    op.commit().await?;

    let event_type = obix::out::EphemeralEventType::new("only");
    outbox
        .publish_ephemeral(event_type, TestEvent::Ping(7))
        .await?;

    wait_for_n_deliveries(&ephemeral_received, 1, std::time::Duration::from_secs(5)).await?;

    // Settle: neither half of the contract may be violated in the meantime.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(
        persistent_received.lock().await.is_empty(),
        "EphemeralOnly handler must never receive persistent events"
    );
    assert_eq!(
        checkpoint_sequence(&jobs).await?,
        None,
        "EphemeralOnly job must never write execution state"
    );

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn undecodable_event_fails_job_and_resumes_after_fix() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    // Phase 1 — Ping(1) at seq 1, an undecodable foreign event at seq 2, Ping(2)
    // at seq 3.
    let received = Arc::new(Mutex::new(Vec::new()));
    {
        let job_config = job::JobSvcConfig::builder()
            .pool(pool.clone())
            .build()
            .unwrap();
        let mut jobs = job::Jobs::init(job_config).await?;

        let config = OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
            .with_retry_settings(fast_retry_settings())
            .with_checkpoint_interval(std::time::Duration::from_millis(100));
        let outbox = init_outbox_with_handler_config(
            &pool,
            &mut jobs,
            config,
            SkippingObserver {
                received: received.clone(),
            },
        )
        .await?;

        let mut op = outbox.begin_op().await?;
        outbox
            .publish_persisted_in_op(&mut op, TestEvent::Ping(1))
            .await?;
        op.commit().await?;

        publish_foreign_poison(&pool).await?;

        let mut op = outbox.begin_op().await?;
        outbox
            .publish_persisted_in_op(&mut op, TestEvent::Ping(2))
            .await?;
        op.commit().await?;

        jobs.start_poll().await?;

        // Every retry re-reads the poison event and fails again, so the
        // checkpoint stays parked at seq 1.
        wait_for_checkpoint(&jobs, 1).await?;
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        assert_eq!(*received.lock().await, vec![1]);
        assert_eq!(checkpoint_sequence(&jobs).await?, Some(1));

        jobs.shutdown().await?;
    }

    // Phase 2 — remediation: NULLing the payload turns the row into an ordinary
    // placeholder, and the restarted consumer resumes where it parked.
    sqlx::query("UPDATE persistent_outbox_events SET payload = NULL WHERE sequence = 2")
        .execute(&pool)
        .await?;

    let received_after = Arc::new(Mutex::new(Vec::new()));
    {
        let job_config = job::JobSvcConfig::builder()
            .pool(pool.clone())
            .build()
            .unwrap();
        let mut jobs = job::Jobs::init(job_config).await?;

        let outbox = Outbox::<TestEvent, TestTables>::init(
            &pool,
            MailboxConfig::builder()
                .build()
                .expect("Couldn't build MailboxConfig"),
        )
        .await?;
        outbox
            .register_singleton_subscriber(
                &mut jobs,
                OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
                    .with_retry_settings(fast_retry_settings())
                    .with_checkpoint_interval(std::time::Duration::from_millis(100)),
                SkippingObserver {
                    received: received_after.clone(),
                },
            )
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        jobs.start_poll().await?;

        wait_for_n_deliveries(&received_after, 1, std::time::Duration::from_secs(5)).await?;
        assert_eq!(*received_after.lock().await, vec![2]);
        wait_for_checkpoint(&jobs, 3).await?;

        jobs.shutdown().await?;
    }

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn handle_undecodable_ok_moves_past_poison_event() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let received = Arc::new(Mutex::new(Vec::new()));
    let acked = Arc::new(Mutex::new(Vec::new()));
    let config = OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
        .with_checkpoint_interval(std::time::Duration::from_millis(100));
    let outbox = init_outbox_with_handler_config(
        &pool,
        &mut jobs,
        config,
        AckingObserver {
            received: received.clone(),
            acked: acked.clone(),
        },
    )
    .await?;

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(1))
        .await?;
    op.commit().await?;

    publish_foreign_poison(&pool).await?;

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(2))
        .await?;
    op.commit().await?;

    jobs.start_poll().await?;

    // `handle_undecodable` runs instead, and its `Ok` moves the pipeline past the
    // poison event without failing the job.
    wait_for_n_deliveries(&received, 2, std::time::Duration::from_secs(5)).await?;
    assert_eq!(*received.lock().await, vec![1, 2]);
    assert_eq!(*acked.lock().await, vec![2]);
    wait_for_checkpoint(&jobs, 3).await?;

    jobs.shutdown().await?;

    Ok(())
}
