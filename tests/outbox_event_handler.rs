mod helpers;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use obix::{
    EventCtx, Handled, MailboxConfig, OutboxEventHandler, OutboxEventJobConfig, out::Outbox,
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

/// Pure observer: records deliveries and skips — no transaction is ever
/// opened on its behalf.
struct SkippingObserver {
    received: Arc<Mutex<Vec<u64>>>,
}

impl OutboxEventHandler<TestEvent> for SkippingObserver {
    async fn handle_persistent(
        &self,
        ctx: EventCtx<'_>,
        event: &obix::out::PersistentOutboxEvent<TestEvent>,
    ) -> Result<Handled, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(TestEvent::Ping(n)) = &event.payload {
            self.received.lock().await.push(*n);
        }
        Ok(ctx.skip())
    }
}

/// Legacy-style observer: one isolated op + checkpoint per event.
struct CheckpointingObserver {
    received: Arc<Mutex<Vec<u64>>>,
}

impl OutboxEventHandler<TestEvent> for CheckpointingObserver {
    async fn handle_persistent(
        &self,
        ctx: EventCtx<'_>,
        event: &obix::out::PersistentOutboxEvent<TestEvent>,
    ) -> Result<Handled, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(TestEvent::Ping(n)) = &event.payload {
            self.received.lock().await.push(*n);
        }
        let op = ctx.consume_isolated().await?;
        Ok(op.commit())
    }
}

struct TestEphemeralHandler {
    received: Arc<Mutex<Vec<u64>>>,
}

impl OutboxEventHandler<TestEvent> for TestEphemeralHandler {
    async fn handle_ephemeral(
        &self,
        event: &obix::out::EphemeralOutboxEvent<TestEvent>,
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

impl OutboxEventHandler<TestEvent> for TestBothHandler {
    async fn handle_persistent(
        &self,
        ctx: EventCtx<'_>,
        event: &obix::out::PersistentOutboxEvent<TestEvent>,
    ) -> Result<Handled, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(TestEvent::Ping(n)) = &event.payload {
            self.persistent_received.lock().await.push(*n);
        }
        Ok(ctx.skip())
    }

    async fn handle_ephemeral(
        &self,
        event: &obix::out::EphemeralOutboxEvent<TestEvent>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let TestEvent::Ping(n) = &event.payload;
        self.ephemeral_received.lock().await.push(*n);
        Ok(())
    }
}

/// Batch-safe worker: inserts one row per event inside the shared batch op
/// and defers. Optionally fails the first time a given event value is seen.
/// Sleeps briefly per event so the backfill keeps the ready backlog ahead of
/// the handler (making batch composition deterministic in tests).
struct DeferringEffectHandler {
    deliveries: Arc<Mutex<Vec<u64>>>,
    fail_on_first: Option<u64>,
    failed: Arc<AtomicBool>,
}

impl OutboxEventHandler<TestEvent> for DeferringEffectHandler {
    async fn handle_persistent(
        &self,
        ctx: EventCtx<'_>,
        event: &obix::out::PersistentOutboxEvent<TestEvent>,
    ) -> Result<Handled, Box<dyn std::error::Error + Send + Sync>> {
        use es_entity::AtomicOperation;
        let Some(TestEvent::Ping(n)) = &event.payload else {
            return Ok(ctx.skip());
        };
        self.deliveries.lock().await.push(*n);
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        let mut op = ctx.consume_in_batch().await?;
        if self.fail_on_first == Some(*n) && !self.failed.swap(true, Ordering::SeqCst) {
            return Err("injected mid-batch failure".into());
        }
        sqlx::query("INSERT INTO test_batch_effects (n) VALUES ($1)")
            .bind(*n as i64)
            .execute(op.as_executor())
            .await?;
        Ok(op.defer())
    }
}

/// Defers everything except one event value, which it handles isolated —
/// fencing the pending batch — and fails on its first attempt.
struct IsolatingEffectHandler {
    deliveries: Arc<Mutex<Vec<u64>>>,
    isolate_on: u64,
    fail_isolated_once: Arc<AtomicBool>,
}

impl OutboxEventHandler<TestEvent> for IsolatingEffectHandler {
    async fn handle_persistent(
        &self,
        ctx: EventCtx<'_>,
        event: &obix::out::PersistentOutboxEvent<TestEvent>,
    ) -> Result<Handled, Box<dyn std::error::Error + Send + Sync>> {
        use es_entity::AtomicOperation;
        let Some(TestEvent::Ping(n)) = &event.payload else {
            return Ok(ctx.skip());
        };
        self.deliveries.lock().await.push(*n);
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        if *n == self.isolate_on {
            let mut op = ctx.consume_isolated().await?;
            sqlx::query("INSERT INTO test_batch_effects (n) VALUES ($1)")
                .bind(*n as i64)
                .execute(op.as_executor())
                .await?;
            if !self.fail_isolated_once.swap(true, Ordering::SeqCst) {
                return Err("injected isolated failure".into());
            }
            Ok(op.commit())
        } else {
            let mut op = ctx.consume_in_batch().await?;
            sqlx::query("INSERT INTO test_batch_effects (n) VALUES ($1)")
                .bind(*n as i64)
                .execute(op.as_executor())
                .await?;
            Ok(op.defer())
        }
    }
}

/// Slow deferring worker that also records ephemerals and counts flushes —
/// used to prove an ephemeral arriving mid-batch lands the open batch first.
struct SlowDeferringHandler {
    ephemeral_received: Arc<Mutex<Vec<u64>>>,
    flush_count: Arc<AtomicUsize>,
}

impl OutboxEventHandler<TestEvent> for SlowDeferringHandler {
    async fn handle_persistent(
        &self,
        ctx: EventCtx<'_>,
        event: &obix::out::PersistentOutboxEvent<TestEvent>,
    ) -> Result<Handled, Box<dyn std::error::Error + Send + Sync>> {
        use es_entity::AtomicOperation;
        let Some(TestEvent::Ping(n)) = &event.payload else {
            return Ok(ctx.skip());
        };
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let mut op = ctx.consume_in_batch().await?;
        sqlx::query("INSERT INTO test_batch_effects (n) VALUES ($1)")
            .bind(*n as i64)
            .execute(op.as_executor())
            .await?;
        Ok(op.defer())
    }

    async fn handle_ephemeral(
        &self,
        event: &obix::out::EphemeralOutboxEvent<TestEvent>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let TestEvent::Ping(n) = &event.payload;
        self.ephemeral_received.lock().await.push(*n);
        Ok(())
    }

    async fn on_flush(
        &self,
        _op: &mut es_entity::DbOp<'_>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.flush_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Aggregator: accumulates event values in memory across deferred events and
/// writes them back in `on_flush` — one write-back per batch, not per event.
struct AccumulatingHandler {
    pending: Arc<std::sync::Mutex<Vec<i64>>>,
    flush_count: Arc<AtomicUsize>,
}

impl OutboxEventHandler<TestEvent> for AccumulatingHandler {
    async fn handle_persistent(
        &self,
        ctx: EventCtx<'_>,
        event: &obix::out::PersistentOutboxEvent<TestEvent>,
    ) -> Result<Handled, Box<dyn std::error::Error + Send + Sync>> {
        let Some(TestEvent::Ping(n)) = &event.payload else {
            return Ok(ctx.skip());
        };
        self.pending.lock().expect("lock").push(*n as i64);
        let op = ctx.consume_in_batch().await?;
        Ok(op.defer())
    }

    async fn on_flush(
        &self,
        op: &mut es_entity::DbOp<'_>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use es_entity::AtomicOperation;
        let drained: Vec<i64> = std::mem::take(&mut *self.pending.lock().expect("lock"));
        for n in drained {
            sqlx::query("INSERT INTO test_batch_effects (n) VALUES ($1)")
                .bind(n)
                .execute(op.as_executor())
                .await?;
        }
        self.flush_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

async fn init_outbox_with_handler<H: OutboxEventHandler<TestEvent>>(
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

async fn init_outbox_with_handler_config<H: OutboxEventHandler<TestEvent>>(
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
        .register_event_handler(jobs, config, handler)
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

async fn checkpoint_sequence(pool: &sqlx::PgPool) -> anyhow::Result<Option<i64>> {
    let row: Option<(Option<serde_json::Value>,)> = sqlx::query_as(
        "SELECT je.execution_state_json FROM job_executions je \
         JOIN jobs j ON j.id = je.id WHERE j.job_type = $1",
    )
    .bind(JOB_TYPE)
    .fetch_optional(pool)
    .await?;
    Ok(row
        .and_then(|(json,)| json)
        .and_then(|json| json.get("sequence").and_then(|s| s.as_i64())))
}

async fn wait_for_checkpoint(pool: &sqlx::PgPool, expected: i64) -> anyhow::Result<()> {
    let start = std::time::Instant::now();
    loop {
        if checkpoint_sequence(pool).await? == Some(expected) {
            return Ok(());
        }
        if start.elapsed() > std::time::Duration::from_secs(5) {
            anyhow::bail!(
                "Timeout waiting for checkpoint to reach {expected}, at {:?}",
                checkpoint_sequence(pool).await?
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

    // Give the job time to start and begin listening
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

    // First run: process some events
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

    // Second run: publish more events, handler should NOT receive events 10,20 again
    let received_second = Arc::new(Mutex::new(Vec::new()));
    {
        let job_config = job::JobSvcConfig::builder()
            .pool(pool.clone())
            .build()
            .unwrap();
        let mut jobs = job::Jobs::init(job_config).await?;

        // Re-init outbox (don't wipe tables — we want to keep the sequence state)
        let outbox = Outbox::<TestEvent, TestTables>::init(
            &pool,
            MailboxConfig::builder()
                .build()
                .expect("Couldn't build MailboxConfig"),
        )
        .await?;

        outbox
            .register_event_handler(
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
                // Should only have 30, not 10 or 20
                assert_eq!(*events, vec![30]);
                break;
            }
            drop(events);
            if start.elapsed() > std::time::Duration::from_secs(5) {
                anyhow::bail!("Timeout waiting for second-run events");
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        // Wait a bit to make sure no stale events arrive
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

    // Give the job time to start
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Publish persistent event
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(100))
        .await?;
    op.commit().await?;

    // Publish ephemeral event
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
async fn deferred_batch_replays_wholesale_on_mid_batch_failure() -> anyhow::Result<()> {
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
        DeferringEffectHandler {
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

    // Event 1 was deferred into the batch that event 2 poisoned, so the
    // whole batch rolled back and event 1 was delivered again on replay —
    // per-batch fate sharing, not per-event.
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
async fn isolated_event_fences_prior_batch_from_its_failure() -> anyhow::Result<()> {
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
        IsolatingEffectHandler {
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

    // Events 1 and 2 were landed (batch flush at the isolation fence or
    // earlier) before event 3's isolated op failed — so only event 3
    // replays. With PR-#84-style runner batching, the whole batch would
    // have rolled back and events 1 and 2 would replay too.
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

    // No transaction ever ran for these events, yet the checkpoint catches
    // up within ~checkpoint_interval via the standalone pointer write.
    // (Sequences are deterministic: the wipeout restarts identity at 1.)
    wait_for_checkpoint(&pool, 3).await?;

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn on_flush_coalesces_accumulated_writes() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    reset_batch_effects_table(&pool).await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let flush_count = Arc::new(AtomicUsize::new(0));
    let outbox = init_outbox_with_handler(
        &pool,
        &mut jobs,
        AccumulatingHandler {
            pending: Arc::new(std::sync::Mutex::new(Vec::new())),
            flush_count: flush_count.clone(),
        },
    )
    .await?;

    // Publish before the job starts so the events arrive as ready backlog.
    const N: u64 = 50;
    let mut op = outbox.begin_op().await?;
    for n in 1..=N {
        outbox
            .publish_persisted_in_op(&mut op, TestEvent::Ping(n))
            .await?;
    }
    op.commit().await?;

    jobs.start_poll().await?;

    wait_for_effect_rows(&pool, N as usize).await?;
    assert_eq!(
        batch_effect_rows(&pool).await?,
        (1..=N as i64).collect::<Vec<_>>()
    );

    // The write-backs were coalesced: strictly fewer flushes than events.
    let flushes = flush_count.load(Ordering::SeqCst);
    assert!(
        flushes < N as usize,
        "expected fewer flushes than events, got {flushes}"
    );

    // The checkpoint lands with the last batch. (Sequences are
    // deterministic: the wipeout restarts identity at 1.)
    wait_for_checkpoint(&pool, N as i64).await?;

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn ephemeral_arriving_mid_batch_lands_the_open_batch_first() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    reset_batch_effects_table(&pool).await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let ephemeral_received = Arc::new(Mutex::new(Vec::new()));
    let flush_count = Arc::new(AtomicUsize::new(0));
    let outbox = init_outbox_with_handler(
        &pool,
        &mut jobs,
        SlowDeferringHandler {
            ephemeral_received: ephemeral_received.clone(),
            flush_count: flush_count.clone(),
        },
    )
    .await?;

    // Publish a backlog of slow (100ms each) deferring events, then land an
    // ephemeral while the batch is guaranteed to still be open mid-drain.
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

    // The ephemeral truncated the open batch (flush reason "ephemeral"), so
    // the drain took at least two flushes instead of one — and the open
    // transaction was never held across the ephemeral handler's await.
    let flushes = flush_count.load(Ordering::SeqCst);
    assert!(
        flushes >= 2,
        "expected the mid-batch ephemeral to land the open batch, flushes: {flushes}"
    );
    assert_eq!(batch_effect_rows(&pool).await?, vec![1, 2, 3, 4, 5]);

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn single_deferred_event_commits_promptly_at_low_traffic() -> anyhow::Result<()> {
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
        DeferringEffectHandler {
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

    // A deferred op is never held open waiting for future events: the
    // moment the backlog is drained the batch (of one) lands — work AND
    // checkpoint — with no configured wait.
    wait_for_effect_rows(&pool, 1).await?;
    wait_for_checkpoint(&pool, 1).await?;
    let latency = published_at.elapsed();
    assert!(
        latency < std::time::Duration::from_secs(2),
        "single deferred event took {latency:?} to land"
    );

    Ok(())
}
