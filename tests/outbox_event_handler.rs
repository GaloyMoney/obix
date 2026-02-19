mod helpers;

use std::sync::Arc;

use async_trait::async_trait;
use obix::{
    CommandJob, CommandJobSpawner, MailboxConfig, OutboxEventHandler, OutboxEventJobConfig,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum OrderEvent {
    OrderPlaced { order_id: u64 },
    OrderShipped { order_id: u64 },
}

const FULFILLMENT_HANDLER_JOB_TYPE: &str = "fulfillment-handler";
const FULFILL_ORDER_JOB_TYPE: &str = "fulfill-order";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FulfillOrderConfig {
    order_id: u64,
}

struct FulfillOrderInitializer {
    fulfilled: Arc<Mutex<Vec<u64>>>,
}

impl job::JobInitializer for FulfillOrderInitializer {
    type Config = FulfillOrderConfig;

    fn job_type(&self) -> job::JobType {
        job::JobType::new(FULFILL_ORDER_JOB_TYPE)
    }

    fn init(
        &self,
        job: &job::Job,
        _: job::JobSpawner<Self::Config>,
    ) -> Result<Box<dyn job::JobRunner>, Box<dyn std::error::Error>> {
        let config: FulfillOrderConfig = job.config()?;
        Ok(Box::new(FulfillOrderRunner {
            order_id: config.order_id,
            fulfilled: self.fulfilled.clone(),
        }))
    }
}

struct FulfillOrderRunner {
    order_id: u64,
    fulfilled: Arc<Mutex<Vec<u64>>>,
}

#[async_trait]
impl job::JobRunner for FulfillOrderRunner {
    async fn run(
        &self,
        _current_job: job::CurrentJob,
    ) -> Result<job::JobCompletion, Box<dyn std::error::Error>> {
        self.fulfilled.lock().await.push(self.order_id);
        Ok(job::JobCompletion::Complete)
    }
}

struct OrderFulfillmentHandler {
    spawner: job::JobSpawner<FulfillOrderConfig>,
}

impl OutboxEventHandler<OrderEvent> for OrderFulfillmentHandler {
    async fn handle_persistent(
        &self,
        op: &mut es_entity::DbOp<'_>,
        event: &obix::out::PersistentOutboxEvent<OrderEvent>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(OrderEvent::OrderPlaced { order_id }) = &event.payload {
            self.spawner
                .spawn_in_op(
                    op,
                    job::JobId::new(),
                    FulfillOrderConfig {
                        order_id: *order_id,
                    },
                )
                .await?;
        }
        Ok(())
    }
}

#[tokio::test]
#[file_serial]
async fn fulfillment_handler_spawns_downstream_job() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    wipeout_outbox_tables(&pool).await?;
    wipeout_outbox_job_tables(&pool, FULFILLMENT_HANDLER_JOB_TYPE).await?;
    wipeout_outbox_job_tables(&pool, FULFILL_ORDER_JOB_TYPE).await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let fulfilled = Arc::new(Mutex::new(Vec::new()));

    let outbox = Outbox::<OrderEvent, TestTables>::init(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let fulfilled_clone = fulfilled.clone();
    outbox
        .register_event_handler_with_context(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(FULFILLMENT_HANDLER_JOB_TYPE)),
            |ctx| {
                let spawner = ctx.add_initializer(FulfillOrderInitializer {
                    fulfilled: fulfilled_clone,
                });
                OrderFulfillmentHandler { spawner }
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    jobs.start_poll().await?;

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, OrderEvent::OrderPlaced { order_id: 99 })
        .await?;
    op.commit().await?;

    let start = std::time::Instant::now();
    loop {
        let values = fulfilled.lock().await;
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

// -- Chain test: EventHandler1 → CommandJob → EventHandler2 --
//
// OrderPlaced → ShippingHandler spawns ShipOrder job
//            → ShipOrderRunner publishes OrderShipped
//            → DeliveryConfirmationHandler records the shipment

const SHIPPING_HANDLER_JOB_TYPE: &str = "shipping-handler";
const SHIP_ORDER_JOB_TYPE: &str = "ship-order";
const DELIVERY_HANDLER_JOB_TYPE: &str = "delivery-confirmation-handler";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ShipOrderConfig {
    order_id: u64,
}

struct ShipOrderInitializer {
    outbox: Outbox<OrderEvent, TestTables>,
}

impl job::JobInitializer for ShipOrderInitializer {
    type Config = ShipOrderConfig;

    fn job_type(&self) -> job::JobType {
        job::JobType::new(SHIP_ORDER_JOB_TYPE)
    }

    fn init(
        &self,
        job: &job::Job,
        _: job::JobSpawner<Self::Config>,
    ) -> Result<Box<dyn job::JobRunner>, Box<dyn std::error::Error>> {
        let config: ShipOrderConfig = job.config()?;
        Ok(Box::new(ShipOrderRunner {
            order_id: config.order_id,
            outbox: self.outbox.clone(),
        }))
    }
}

struct ShipOrderRunner {
    order_id: u64,
    outbox: Outbox<OrderEvent, TestTables>,
}

#[async_trait]
impl job::JobRunner for ShipOrderRunner {
    async fn run(
        &self,
        _current_job: job::CurrentJob,
    ) -> Result<job::JobCompletion, Box<dyn std::error::Error>> {
        let mut op = self.outbox.begin_op().await?;
        self.outbox
            .publish_persisted_in_op(
                &mut op,
                OrderEvent::OrderShipped {
                    order_id: self.order_id,
                },
            )
            .await?;
        op.commit().await?;
        Ok(job::JobCompletion::Complete)
    }
}

struct ShippingHandler {
    spawner: job::JobSpawner<ShipOrderConfig>,
}

impl OutboxEventHandler<OrderEvent> for ShippingHandler {
    async fn handle_persistent(
        &self,
        op: &mut es_entity::DbOp<'_>,
        event: &obix::out::PersistentOutboxEvent<OrderEvent>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(OrderEvent::OrderPlaced { order_id }) = &event.payload {
            self.spawner
                .spawn_in_op(
                    op,
                    job::JobId::new(),
                    ShipOrderConfig {
                        order_id: *order_id,
                    },
                )
                .await?;
        }
        Ok(())
    }
}

struct DeliveryConfirmationHandler {
    confirmed: Arc<Mutex<Vec<u64>>>,
}

impl OutboxEventHandler<OrderEvent> for DeliveryConfirmationHandler {
    async fn handle_persistent(
        &self,
        _op: &mut es_entity::DbOp<'_>,
        event: &obix::out::PersistentOutboxEvent<OrderEvent>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(OrderEvent::OrderShipped { order_id }) = &event.payload {
            self.confirmed.lock().await.push(*order_id);
        }
        Ok(())
    }
}

#[tokio::test]
#[file_serial]
async fn event_chain_handler_to_job_to_handler() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    wipeout_outbox_tables(&pool).await?;
    wipeout_outbox_job_tables(&pool, SHIPPING_HANDLER_JOB_TYPE).await?;
    wipeout_outbox_job_tables(&pool, SHIP_ORDER_JOB_TYPE).await?;
    wipeout_outbox_job_tables(&pool, DELIVERY_HANDLER_JOB_TYPE).await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let outbox = Outbox::<OrderEvent, TestTables>::init(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    // Handler1: ShippingHandler spawns ShipOrder job on OrderPlaced
    let outbox_for_job = outbox.clone();
    outbox
        .register_event_handler_with_context(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(SHIPPING_HANDLER_JOB_TYPE)),
            |ctx| {
                let spawner = ctx.add_initializer(ShipOrderInitializer {
                    outbox: outbox_for_job,
                });
                ShippingHandler { spawner }
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // Handler2: DeliveryConfirmationHandler records OrderShipped
    let confirmed = Arc::new(Mutex::new(Vec::new()));
    outbox
        .register_event_handler(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(DELIVERY_HANDLER_JOB_TYPE)),
            DeliveryConfirmationHandler {
                confirmed: confirmed.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    jobs.start_poll().await?;

    // Publish OrderPlaced — triggers the chain:
    // ShippingHandler → ShipOrder job → publishes OrderShipped → DeliveryConfirmationHandler
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, OrderEvent::OrderPlaced { order_id: 42 })
        .await?;
    op.commit().await?;

    let start = std::time::Instant::now();
    loop {
        let values = confirmed.lock().await;
        if !values.is_empty() {
            assert_eq!(*values, vec![42]);
            break;
        }
        drop(values);
        if start.elapsed() > std::time::Duration::from_secs(15) {
            anyhow::bail!(
                "Timeout waiting for chain: OrderPlaced → ShipOrder → OrderShipped confirmation"
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Command Job round-trip test
// ---------------------------------------------------------------------------

const CMD_HANDLER_JOB_TYPE: &str = "test-cmd-handler";
const PROCESS_CMD_JOB_TYPE: &str = "process-command";
const CMD_OBSERVER_JOB_TYPE: &str = "test-cmd-observer";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProcessCommandConfig {
    value: u64,
}

struct ProcessCommand {
    outbox: Outbox<TestEvent, TestTables>,
}

#[async_trait]
impl CommandJob for ProcessCommand {
    type Config = ProcessCommandConfig;

    fn job_type() -> job::JobType {
        job::JobType::new(PROCESS_CMD_JOB_TYPE)
    }

    async fn run(
        &self,
        op: &mut es_entity::DbOp<'_>,
        config: &ProcessCommandConfig,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.outbox
            .publish_persisted_in_op(op, TestEvent::Ping(config.value * 10))
            .await?;
        Ok(())
    }
}

struct CommandSpawnerHandler {
    spawner: CommandJobSpawner<ProcessCommandConfig>,
}

impl OutboxEventHandler<TestEvent> for CommandSpawnerHandler {
    async fn handle_persistent(
        &self,
        _op: &mut es_entity::DbOp<'_>,
        event: &obix::out::PersistentOutboxEvent<TestEvent>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(TestEvent::Ping(n)) = &event.payload {
            // Only spawn command jobs for "trigger" events (< 100) to avoid infinite chain
            if *n < 100 {
                self.spawner
                    .spawn_for_event(event, ProcessCommandConfig { value: *n })
                    .await?;
                // Call again to verify idempotency — should be a no-op (DuplicateId)
                self.spawner
                    .spawn_for_event(event, ProcessCommandConfig { value: *n })
                    .await?;
            }
        }
        Ok(())
    }
}

#[tokio::test]
#[file_serial]
async fn command_job_round_trip() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    wipeout_outbox_tables(&pool).await?;
    wipeout_outbox_job_tables(&pool, CMD_HANDLER_JOB_TYPE).await?;
    wipeout_outbox_job_tables(&pool, PROCESS_CMD_JOB_TYPE).await?;
    wipeout_outbox_job_tables(&pool, CMD_OBSERVER_JOB_TYPE).await?;

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
        .register_event_handler_with::<CommandSpawnerHandler>(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(CMD_HANDLER_JOB_TYPE)),
            |ctx| {
                let spawner = ctx.add_command_job_from(|outbox| ProcessCommand { outbox });
                CommandSpawnerHandler { spawner }
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // Register an observer handler to capture all persistent events
    let observed = Arc::new(Mutex::new(Vec::new()));
    outbox
        .register_event_handler(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(CMD_OBSERVER_JOB_TYPE)),
            TestPersistentHandler {
                received: observed.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    jobs.start_poll().await?;

    // Publish trigger event: Ping(42)
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(42))
        .await?;
    op.commit().await?;

    // Wait for the downstream event (420 = 42 * 10) published by the command job
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

    // Verify: observer should have seen both the trigger (42) and downstream (420)
    let values = observed.lock().await;
    assert!(
        values.contains(&42),
        "Should have observed trigger event"
    );
    assert!(
        values.contains(&420),
        "Should have observed downstream event from command job"
    );
    // Verify no duplicate downstream events (dedup via deterministic job ID)
    assert_eq!(
        values.iter().filter(|&&v| v == 420).count(),
        1,
        "Downstream event should appear exactly once (dedup test)"
    );

    Ok(())
}
