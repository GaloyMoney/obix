mod helpers;

use std::sync::Arc;

use async_trait::async_trait;
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
        .register_event_handler(
            jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE)),
            handler,
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    Ok(outbox)
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

const SPAWNER_HANDLER_JOB_TYPE: &str = "test-spawner-handler";
const NOTIFICATION_JOB_TYPE: &str = "send-notification";
const AUDIT_LOG_JOB_TYPE: &str = "send-audit-log";
const MULTI_SPAWNER_HANDLER_JOB_TYPE: &str = "test-multi-spawner-handler";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SendNotificationConfig {
    value: u64,
}

struct SendNotificationInitializer {
    results: Arc<Mutex<Vec<u64>>>,
}

impl job::JobInitializer for SendNotificationInitializer {
    type Config = SendNotificationConfig;

    fn job_type(&self) -> job::JobType {
        job::JobType::new(NOTIFICATION_JOB_TYPE)
    }

    fn init(
        &self,
        job: &job::Job,
        _: job::JobSpawner<Self::Config>,
    ) -> Result<Box<dyn job::JobRunner>, Box<dyn std::error::Error>> {
        let config: SendNotificationConfig = job.config()?;
        Ok(Box::new(SendNotificationRunner {
            value: config.value,
            results: self.results.clone(),
        }))
    }
}

struct SendNotificationRunner {
    value: u64,
    results: Arc<Mutex<Vec<u64>>>,
}

#[async_trait]
impl job::JobRunner for SendNotificationRunner {
    async fn run(
        &self,
        _current_job: job::CurrentJob,
    ) -> Result<job::JobCompletion, Box<dyn std::error::Error>> {
        self.results.lock().await.push(self.value);
        Ok(job::JobCompletion::Complete)
    }
}

struct SpawnerHandler {
    spawner: job::JobSpawner<SendNotificationConfig>,
}

impl OutboxEventHandler<TestEvent> for SpawnerHandler {
    async fn handle_persistent(
        &self,
        op: &mut es_entity::DbOp<'_>,
        event: &obix::out::PersistentOutboxEvent<TestEvent>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(TestEvent::Ping(n)) = &event.payload {
            self.spawner
                .spawn_in_op(op, job::JobId::new(), SendNotificationConfig { value: *n })
                .await?;
        }
        Ok(())
    }
}

#[tokio::test]
#[file_serial]
async fn handler_spawns_downstream_job() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    wipeout_outbox_tables(&pool).await?;
    wipeout_outbox_job_tables(&pool, SPAWNER_HANDLER_JOB_TYPE).await?;
    wipeout_outbox_job_tables(&pool, NOTIFICATION_JOB_TYPE).await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let results = Arc::new(Mutex::new(Vec::new()));

    let outbox = Outbox::<TestEvent, TestTables>::init(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let results_clone = results.clone();
    outbox
        .register_event_handler_with::<SpawnerHandler, _>(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(SPAWNER_HANDLER_JOB_TYPE)),
            |ctx| {
                let spawner = ctx.add_initializer(SendNotificationInitializer {
                    results: results_clone,
                });
                SpawnerHandler { spawner }
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    jobs.start_poll().await?;

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(99))
        .await?;
    op.commit().await?;

    let start = std::time::Instant::now();
    loop {
        let values = results.lock().await;
        if !values.is_empty() {
            assert_eq!(*values, vec![99]);
            break;
        }
        drop(values);
        if start.elapsed() > std::time::Duration::from_secs(10) {
            anyhow::bail!("Timeout waiting for downstream job to execute");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    Ok(())
}

// -- Second downstream job type for multi-spawner test --

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SendAuditLogConfig {
    value: u64,
}

struct SendAuditLogInitializer {
    results: Arc<Mutex<Vec<u64>>>,
}

impl job::JobInitializer for SendAuditLogInitializer {
    type Config = SendAuditLogConfig;

    fn job_type(&self) -> job::JobType {
        job::JobType::new(AUDIT_LOG_JOB_TYPE)
    }

    fn init(
        &self,
        job: &job::Job,
        _: job::JobSpawner<Self::Config>,
    ) -> Result<Box<dyn job::JobRunner>, Box<dyn std::error::Error>> {
        let config: SendAuditLogConfig = job.config()?;
        Ok(Box::new(SendAuditLogRunner {
            value: config.value,
            results: self.results.clone(),
        }))
    }
}

struct SendAuditLogRunner {
    value: u64,
    results: Arc<Mutex<Vec<u64>>>,
}

#[async_trait]
impl job::JobRunner for SendAuditLogRunner {
    async fn run(
        &self,
        _current_job: job::CurrentJob,
    ) -> Result<job::JobCompletion, Box<dyn std::error::Error>> {
        self.results.lock().await.push(self.value);
        Ok(job::JobCompletion::Complete)
    }
}

struct MultiSpawnerHandler {
    notification_spawner: job::JobSpawner<SendNotificationConfig>,
    audit_log_spawner: job::JobSpawner<SendAuditLogConfig>,
}

impl OutboxEventHandler<TestEvent> for MultiSpawnerHandler {
    async fn handle_persistent(
        &self,
        op: &mut es_entity::DbOp<'_>,
        event: &obix::out::PersistentOutboxEvent<TestEvent>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(TestEvent::Ping(n)) = &event.payload {
            self.notification_spawner
                .spawn_in_op(op, job::JobId::new(), SendNotificationConfig { value: *n })
                .await?;
            self.audit_log_spawner
                .spawn_in_op(op, job::JobId::new(), SendAuditLogConfig { value: *n * 10 })
                .await?;
        }
        Ok(())
    }
}

#[tokio::test]
#[file_serial]
async fn handler_spawns_multiple_downstream_jobs() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    wipeout_outbox_tables(&pool).await?;
    wipeout_outbox_job_tables(&pool, MULTI_SPAWNER_HANDLER_JOB_TYPE).await?;
    wipeout_outbox_job_tables(&pool, NOTIFICATION_JOB_TYPE).await?;
    wipeout_outbox_job_tables(&pool, AUDIT_LOG_JOB_TYPE).await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let notification_results = Arc::new(Mutex::new(Vec::new()));
    let audit_log_results = Arc::new(Mutex::new(Vec::new()));

    let outbox = Outbox::<TestEvent, TestTables>::init(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let notif_results = notification_results.clone();
    let audit_results = audit_log_results.clone();
    outbox
        .register_event_handler_with::<MultiSpawnerHandler, _>(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(MULTI_SPAWNER_HANDLER_JOB_TYPE)),
            |ctx| {
                let notification_spawner = ctx.add_initializer(SendNotificationInitializer {
                    results: notif_results,
                });
                let audit_log_spawner = ctx.add_initializer(SendAuditLogInitializer {
                    results: audit_results,
                });
                MultiSpawnerHandler {
                    notification_spawner,
                    audit_log_spawner,
                }
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    jobs.start_poll().await?;

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(7))
        .await?;
    op.commit().await?;

    let start = std::time::Instant::now();
    loop {
        let notifs = notification_results.lock().await;
        let audits = audit_log_results.lock().await;
        if !notifs.is_empty() && !audits.is_empty() {
            assert_eq!(*notifs, vec![7]);
            assert_eq!(*audits, vec![70]);
            break;
        }
        drop(notifs);
        drop(audits);
        if start.elapsed() > std::time::Duration::from_secs(10) {
            anyhow::bail!("Timeout waiting for both downstream jobs to execute");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    Ok(())
}
