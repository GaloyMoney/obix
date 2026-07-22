mod helpers;

use std::sync::Arc;

use obix::{MailboxConfig, OutboxEventHandler, OutboxEventJobConfig, out::Outbox};
use serde::{Deserialize, Serialize};
use serial_test::file_serial;
use tokio::sync::Mutex;

use helpers::{TestTables, init_pool, wipeout_outbox_job_tables, wipeout_outbox_tables};

const JOB_TYPE: &str = "test-outbox-handler";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum TestEvent {
    Ping(u64),
}

struct TestPersistentHandler {
    received: Arc<Mutex<Vec<u64>>>,
}

impl OutboxEventHandler<TestEvent> for TestPersistentHandler {
    async fn handle_persistent(
        &self,
        _op: &mut es_entity::DbOp<'_>,
        event: &obix::out::PersistentOutboxEvent<TestEvent>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(TestEvent::Ping(n)) = &event.payload {
            self.received.lock().await.push(*n);
        }
        Ok(())
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
        _op: &mut es_entity::DbOp<'_>,
        event: &obix::out::PersistentOutboxEvent<TestEvent>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(TestEvent::Ping(n)) = &event.payload {
            self.persistent_received.lock().await.push(*n);
        }
        Ok(())
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
    let row: Option<(serde_json::Value,)> = sqlx::query_as(
        "SELECT je.execution_state_json FROM job_executions je \
         JOIN jobs j ON j.id = je.id WHERE j.job_type = $1",
    )
    .bind(JOB_TYPE)
    .fetch_optional(pool)
    .await?;
    Ok(row.and_then(|(json,)| json.get("sequence").and_then(|s| s.as_i64())))
}

struct BatchEffectHandler {
    deliveries: Arc<Mutex<Vec<u64>>>,
    fail_on_first: Option<u64>,
    failed: Arc<std::sync::atomic::AtomicBool>,
}

impl OutboxEventHandler<TestEvent> for BatchEffectHandler {
    async fn handle_persistent(
        &self,
        op: &mut es_entity::DbOp<'_>,
        event: &obix::out::PersistentOutboxEvent<TestEvent>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use es_entity::AtomicOperation;
        if let Some(TestEvent::Ping(n)) = &event.payload {
            self.deliveries.lock().await.push(*n);
            if self.fail_on_first == Some(*n)
                && !self.failed.swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                return Err("injected mid-batch failure".into());
            }
            sqlx::query("INSERT INTO test_batch_effects (n) VALUES ($1)")
                .bind(*n as i64)
                .execute(op.as_executor())
                .await?;
        }
        Ok(())
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
        TestPersistentHandler {
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

    let start = std::time::Instant::now();
    loop {
        let events = received.lock().await;
        if events.len() >= 3 {
            assert_eq!(*events, vec![1, 2, 3]);
            break;
        }
        drop(events);
        if start.elapsed() > std::time::Duration::from_secs(5) {
            anyhow::bail!("Timeout waiting for persistent events");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

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
            TestPersistentHandler {
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

        let start = std::time::Instant::now();
        loop {
            let events = received_first.lock().await;
            if events.len() >= 2 {
                break;
            }
            drop(events);
            if start.elapsed() > std::time::Duration::from_secs(5) {
                anyhow::bail!("Timeout waiting for first-run events");
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

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
                TestPersistentHandler {
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
async fn batched_handler_processes_all_events_and_checkpoints_last_sequence() -> anyhow::Result<()>
{
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
        TestPersistentHandler {
            received: received.clone(),
        },
    )
    .await?;

    // Publish all events before the job starts so they are buffered and
    // drained into batches.
    const N: u64 = 50;
    let mut op = outbox.begin_op().await?;
    for n in 1..=N {
        outbox
            .publish_persisted_in_op(&mut op, TestEvent::Ping(n))
            .await?;
    }
    op.commit().await?;

    jobs.start_poll().await?;

    wait_for_n_deliveries(&received, N as usize, std::time::Duration::from_secs(10)).await?;

    let events = received.lock().await;
    assert_eq!(*events, (1..=N).collect::<Vec<_>>());
    drop(events);

    // The checkpoint eventually reflects the last event's sequence.
    let last_sequence: i64 =
        sqlx::query_scalar("SELECT MAX(sequence) FROM persistent_outbox_events")
            .fetch_one(&pool)
            .await?;
    wait_for_checkpoint(&pool, last_sequence).await?;

    Ok(())
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

#[tokio::test]
#[file_serial]
async fn mid_batch_handler_error_replays_batch_exactly_once() -> anyhow::Result<()> {
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
        BatchEffectHandler {
            deliveries: deliveries.clone(),
            fail_on_first: Some(2),
            failed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        },
    )
    .await?;

    // Publish before the job starts so events land in a single batch.
    const N: u64 = 5;
    let mut op = outbox.begin_op().await?;
    for n in 1..=N {
        outbox
            .publish_persisted_in_op(&mut op, TestEvent::Ping(n))
            .await?;
    }
    op.commit().await?;

    jobs.start_poll().await?;

    // First batch attempt: event 1 handled, event 2 fails -> whole batch
    // rolls back. Retry replays the entire batch; this time it succeeds.
    let start = std::time::Instant::now();
    loop {
        if batch_effect_rows(&pool).await?.len() == N as usize {
            break;
        }
        if start.elapsed() > std::time::Duration::from_secs(10) {
            anyhow::bail!("Timeout waiting for all batch effects to commit");
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    // Exactly-once DB effects despite the replay.
    assert_eq!(batch_effect_rows(&pool).await?, vec![1, 2, 3, 4, 5]);

    // Event 1 was delivered again on replay — proof the checkpoint is
    // per-batch, not per-event (legacy per-event checkpointing would never
    // redeliver event 1).
    let deliveries = deliveries.lock().await;
    let event_1_deliveries = deliveries.iter().filter(|&&n| n == 1).count();
    assert!(
        event_1_deliveries >= 2,
        "expected event 1 to be replayed with the batch, deliveries: {deliveries:?}"
    );

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn single_event_flushed_promptly_at_low_traffic() -> anyhow::Result<()> {
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
        TestPersistentHandler {
            received: received.clone(),
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

    // An idle listener must not wait for a full batch: the event is
    // handled within ~batch_flush_timeout (default 100ms).
    wait_for_n_deliveries(&received, 1, std::time::Duration::from_secs(2)).await?;
    let latency = published_at.elapsed();
    assert!(
        latency < std::time::Duration::from_secs(2),
        "single event took {latency:?} to be handled"
    );

    Ok(())
}

#[derive(Debug, PartialEq)]
enum Handled {
    Persistent(u64),
    Ephemeral(u64),
}

struct OrderRecordingHandler {
    log: Arc<Mutex<Vec<Handled>>>,
}

impl OutboxEventHandler<TestEvent> for OrderRecordingHandler {
    async fn handle_persistent(
        &self,
        _op: &mut es_entity::DbOp<'_>,
        event: &obix::out::PersistentOutboxEvent<TestEvent>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(TestEvent::Ping(n)) = &event.payload {
            self.log.lock().await.push(Handled::Persistent(*n));
        }
        Ok(())
    }

    async fn handle_ephemeral(
        &self,
        event: &obix::out::EphemeralOutboxEvent<TestEvent>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let TestEvent::Ping(n) = &event.payload;
        self.log.lock().await.push(Handled::Ephemeral(*n));
        Ok(())
    }
}

#[tokio::test]
#[file_serial]
async fn ephemeral_arriving_mid_batch_is_handled_after_buffered_persistents() -> anyhow::Result<()>
{
    let pool = init_pool().await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let log = Arc::new(Mutex::new(Vec::new()));
    let outbox =
        init_outbox_with_handler(&pool, &mut jobs, OrderRecordingHandler { log: log.clone() })
            .await?;

    jobs.start_poll().await?;

    // Let the job start and go idle.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Publish persistent, then ephemeral, then persistent in quick
    // succession so the ephemeral arrives while the first persistent's
    // batch is still open.
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(1))
        .await?;
    op.commit().await?;

    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    outbox
        .publish_ephemeral(
            obix::out::EphemeralEventType::new("ordering_test"),
            TestEvent::Ping(9),
        )
        .await?;

    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(2))
        .await?;
    op.commit().await?;

    // Ephemeral delivery is at-least-once; wait until the second
    // persistent has been handled.
    let start = std::time::Instant::now();
    loop {
        if log.lock().await.contains(&Handled::Persistent(2)) {
            break;
        }
        if start.elapsed() > std::time::Duration::from_secs(5) {
            anyhow::bail!("Timeout waiting for all events");
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    // Stream order is preserved: the first delivery of the ephemeral
    // happens only after the persistent buffered ahead of it has been
    // handled, and before the persistent published after it.
    let log = log.lock().await;
    let pos = |h: &Handled| log.iter().position(|e| e == h).unwrap();
    assert!(pos(&Handled::Persistent(1)) < pos(&Handled::Ephemeral(9)));
    assert!(pos(&Handled::Ephemeral(9)) < pos(&Handled::Persistent(2)));

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn batch_size_one_preserves_legacy_per_event_semantics() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    reset_batch_effects_table(&pool).await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let deliveries = Arc::new(Mutex::new(Vec::new()));
    let config = OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
        .with_retry_settings(fast_retry_settings())
        .with_batch_size(1);
    let outbox = init_outbox_with_handler_config(
        &pool,
        &mut jobs,
        config,
        BatchEffectHandler {
            deliveries: deliveries.clone(),
            fail_on_first: Some(2),
            failed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
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

    let start = std::time::Instant::now();
    loop {
        if batch_effect_rows(&pool).await?.len() == N as usize {
            break;
        }
        if start.elapsed() > std::time::Duration::from_secs(10) {
            anyhow::bail!("Timeout waiting for all effects to commit");
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    // Legacy semantics: event 1 committed in its own transaction before the
    // failure on event 2, so it is never redelivered.
    let deliveries = deliveries.lock().await;
    let event_1_deliveries = deliveries.iter().filter(|&&n| n == 1).count();
    assert_eq!(
        event_1_deliveries, 1,
        "with batch_size(1) event 1 must not be replayed, deliveries: {deliveries:?}"
    );
    assert_eq!(batch_effect_rows(&pool).await?, vec![1, 2, 3]);

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn zero_flush_timeout_still_batches_ready_events() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    reset_batch_effects_table(&pool).await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let deliveries = Arc::new(Mutex::new(Vec::new()));
    let config = OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
        .with_retry_settings(fast_retry_settings())
        .with_batch_flush_timeout(std::time::Duration::ZERO);
    let outbox = init_outbox_with_handler_config(
        &pool,
        &mut jobs,
        config,
        BatchEffectHandler {
            deliveries: deliveries.clone(),
            fail_on_first: Some(2),
            failed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        },
    )
    .await?;

    // Publish before the job starts so all events are buffered and must be
    // drained into one batch without any coalescing wait.
    const N: u64 = 5;
    let mut op = outbox.begin_op().await?;
    for n in 1..=N {
        outbox
            .publish_persisted_in_op(&mut op, TestEvent::Ping(n))
            .await?;
    }
    op.commit().await?;

    jobs.start_poll().await?;

    let start = std::time::Instant::now();
    loop {
        if batch_effect_rows(&pool).await?.len() == N as usize {
            break;
        }
        if start.elapsed() > std::time::Duration::from_secs(10) {
            anyhow::bail!("Timeout waiting for all batch effects to commit");
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    // Event 1 replayed => the zero-timeout drain still formed a multi-event
    // batch (single-event batches would never redeliver event 1).
    let deliveries = deliveries.lock().await;
    let event_1_deliveries = deliveries.iter().filter(|&&n| n == 1).count();
    assert!(
        event_1_deliveries >= 2,
        "expected ready events to be coalesced with zero timeout, deliveries: {deliveries:?}"
    );
    assert_eq!(batch_effect_rows(&pool).await?, vec![1, 2, 3, 4, 5]);

    Ok(())
}

struct AlwaysFailPersistentWithEphemeralHandler {
    ephemeral_received: Arc<Mutex<Vec<u64>>>,
}

impl OutboxEventHandler<TestEvent> for AlwaysFailPersistentWithEphemeralHandler {
    async fn handle_persistent(
        &self,
        _op: &mut es_entity::DbOp<'_>,
        _event: &obix::out::PersistentOutboxEvent<TestEvent>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Err("persistent always fails".into())
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

#[tokio::test]
#[file_serial]
async fn ephemeral_is_handled_even_when_batch_fails() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let ephemeral_received = Arc::new(Mutex::new(Vec::new()));
    let config = OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
        .with_retry_settings(fast_retry_settings());
    let outbox = init_outbox_with_handler_config(
        &pool,
        &mut jobs,
        config,
        AlwaysFailPersistentWithEphemeralHandler {
            ephemeral_received: ephemeral_received.clone(),
        },
    )
    .await?;

    // Both buffered before the job starts: the ephemeral is drained into
    // pending_ephemeral while the persistent batch is open, and the batch
    // then fails.
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(1))
        .await?;
    op.commit().await?;
    outbox
        .publish_ephemeral(
            obix::out::EphemeralEventType::new("batch_failure_test"),
            TestEvent::Ping(9),
        )
        .await?;

    jobs.start_poll().await?;

    // The ephemeral must be handled even though the persistent batch never
    // commits.
    wait_for_n_deliveries(&ephemeral_received, 1, std::time::Duration::from_secs(5)).await?;

    Ok(())
}
