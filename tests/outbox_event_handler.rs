mod helpers;

use std::sync::Arc;

use async_trait::async_trait;
use obix::{
    CommandJob, CommandJobSpawner, EventHandlerJobConfig, MailboxConfig, OutboxEventHandler,
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
    CustomerCreated(u64),
    WelcomeEmailSent(u64),
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
        let n = match &event.payload {
            Some(TestEvent::Ping(n))
            | Some(TestEvent::CustomerCreated(n))
            | Some(TestEvent::WelcomeEmailSent(n)) => *n,
            None => return Ok(()),
        };
        self.received.lock().await.push(n);
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
        if let TestEvent::Ping(n) = &event.payload {
            self.received.lock().await.push(*n);
        }
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
        if let TestEvent::Ping(n) = &event.payload {
            self.ephemeral_received.lock().await.push(*n);
        }
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
            EventHandlerJobConfig::new(job::JobType::new(JOB_TYPE)),
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
                EventHandlerJobConfig::new(job::JobType::new(JOB_TYPE)),
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

// ---------------------------------------------------------------------------
// Command Job round-trip test
// ---------------------------------------------------------------------------

const CUSTOMER_CREATED_HANDLER_JOB_TYPE: &str = "customer-created-handler";
const SEND_WELCOME_EMAIL_JOB_TYPE: &str = "send-welcome-email";
const WELCOME_EMAIL_OBSERVER_JOB_TYPE: &str = "welcome-email-observer";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SendWelcomeEmailCommand {
    customer_id: String,
    value: u64,
}

struct SendWelcomeEmailCommandJob {
    outbox: Outbox<TestEvent, TestTables>,
}

#[async_trait]
impl CommandJob for SendWelcomeEmailCommandJob {
    type Command = SendWelcomeEmailCommand;

    fn job_type() -> job::JobType {
        job::JobType::new(SEND_WELCOME_EMAIL_JOB_TYPE)
    }

    fn entity_id(command: &Self::Command) -> &str {
        &command.customer_id
    }

    async fn run(
        &self,
        op: &mut es_entity::DbOp<'_>,
        command: &SendWelcomeEmailCommand,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.outbox
            .publish_persisted_in_op(op, TestEvent::WelcomeEmailSent(command.value * 10))
            .await?;
        Ok(())
    }
}

struct CustomerCreatedEventHandler {
    spawner: CommandJobSpawner<SendWelcomeEmailCommand>,
}

impl OutboxEventHandler<TestEvent> for CustomerCreatedEventHandler {
    async fn handle_persistent(
        &self,
        op: &mut es_entity::DbOp<'_>,
        event: &obix::out::PersistentOutboxEvent<TestEvent>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(TestEvent::CustomerCreated(n)) = &event.payload {
            let command = SendWelcomeEmailCommand {
                customer_id: format!("customer-{n}"),
                value: *n,
            };
            self.spawner.spawn(op, command).await?;
        }
        Ok(())
    }
}

#[tokio::test]
#[file_serial]
async fn command_job_round_trip() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    wipeout_outbox_tables(&pool).await?;
    wipeout_outbox_job_tables(&pool, CUSTOMER_CREATED_HANDLER_JOB_TYPE).await?;
    wipeout_outbox_job_tables(&pool, SEND_WELCOME_EMAIL_JOB_TYPE).await?;
    wipeout_outbox_job_tables(&pool, WELCOME_EMAIL_OBSERVER_JOB_TYPE).await?;

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

    // Register the command-spawning handler
    outbox
        .register_event_handler_with_context(
            &mut jobs,
            EventHandlerJobConfig::new(job::JobType::new(CUSTOMER_CREATED_HANDLER_JOB_TYPE)),
            |ctx| {
                let spawner = ctx.add_command_job(SendWelcomeEmailCommandJob {
                    outbox: ctx.outbox().clone(),
                });
                CustomerCreatedEventHandler { spawner }
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // Register an observer handler to capture all persistent events
    let observed = Arc::new(Mutex::new(Vec::new()));
    outbox
        .register_event_handler(
            &mut jobs,
            EventHandlerJobConfig::new(job::JobType::new(WELCOME_EMAIL_OBSERVER_JOB_TYPE)),
            TestPersistentHandler {
                received: observed.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    jobs.start_poll().await?;

    // Publish trigger event: CustomerCreated(42)
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::CustomerCreated(42))
        .await?;
    op.commit().await?;

    // Wait for the downstream event WelcomeEmailSent(420) published by the command job
    let start = std::time::Instant::now();
    loop {
        let values = observed.lock().await;
        if values.contains(&420) {
            break;
        }
        drop(values);
        if start.elapsed() > std::time::Duration::from_secs(10) {
            let values = observed.lock().await;
            anyhow::bail!(
                "Timeout waiting for command job downstream event. Observed: {:?}",
                *values
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // Verify: observer should have seen both CustomerCreated(42) and WelcomeEmailSent(420)
    let values = observed.lock().await;
    assert!(
        values.contains(&42),
        "Should have observed trigger event (CustomerCreated)"
    );
    assert!(
        values.contains(&420),
        "Should have observed downstream event from command job (WelcomeEmailSent)"
    );

    Ok(())
}
