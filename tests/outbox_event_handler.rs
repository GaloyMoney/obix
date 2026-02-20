mod helpers;

use std::sync::Arc;

use async_trait::async_trait;
use obix::{MailboxConfig, OutboxEventHandler, OutboxEventJobConfig, out::Outbox};
use serde::{Deserialize, Serialize};
use serial_test::file_serial;
use tokio::sync::Mutex;

use helpers::{TestTables, init_pool, wipeout_outbox_job_tables, wipeout_outbox_tables};

const ORDER_HANDLER_JOB_TYPE: &str = "order-event-handler";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum OrderEvent {
    OrderPlaced { order_id: u64 },
    OrderShipped { order_id: u64 },
}

struct OrderLedgerHandler {
    recorded: Arc<Mutex<Vec<u64>>>,
}

impl OutboxEventHandler<OrderEvent> for OrderLedgerHandler {
    async fn handle_persistent(
        &self,
        _op: &mut es_entity::DbOp<'_>,
        event: &obix::out::PersistentOutboxEvent<OrderEvent>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(OrderEvent::OrderPlaced { order_id }) = &event.payload {
            self.recorded.lock().await.push(*order_id);
        }
        Ok(())
    }
}

struct OrderDashboardHandler {
    displayed: Arc<Mutex<Vec<u64>>>,
}

impl OutboxEventHandler<OrderEvent> for OrderDashboardHandler {
    async fn handle_ephemeral(
        &self,
        event: &obix::out::EphemeralOutboxEvent<OrderEvent>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let OrderEvent::OrderPlaced { order_id } = &event.payload {
            self.displayed.lock().await.push(*order_id);
        }
        Ok(())
    }
}

struct OrderActivityHandler {
    persistent_recorded: Arc<Mutex<Vec<u64>>>,
    ephemeral_displayed: Arc<Mutex<Vec<u64>>>,
}

impl OutboxEventHandler<OrderEvent> for OrderActivityHandler {
    async fn handle_persistent(
        &self,
        _op: &mut es_entity::DbOp<'_>,
        event: &obix::out::PersistentOutboxEvent<OrderEvent>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(OrderEvent::OrderPlaced { order_id }) = &event.payload {
            self.persistent_recorded.lock().await.push(*order_id);
        }
        Ok(())
    }

    async fn handle_ephemeral(
        &self,
        event: &obix::out::EphemeralOutboxEvent<OrderEvent>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let OrderEvent::OrderPlaced { order_id } = &event.payload {
            self.ephemeral_displayed.lock().await.push(*order_id);
        }
        Ok(())
    }
}

async fn init_outbox_with_handler<H: OutboxEventHandler<OrderEvent>>(
    pool: &sqlx::PgPool,
    jobs: &mut job::Jobs,
    handler: H,
) -> anyhow::Result<Outbox<OrderEvent, TestTables>> {
    wipeout_outbox_tables(pool).await?;
    wipeout_outbox_job_tables(pool, ORDER_HANDLER_JOB_TYPE).await?;

    let outbox = Outbox::<OrderEvent, TestTables>::init(
        pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    outbox
        .register_event_handler(
            jobs,
            OutboxEventJobConfig::new(job::JobType::new(ORDER_HANDLER_JOB_TYPE)),
            handler,
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    Ok(outbox)
}

#[tokio::test]
#[file_serial]
async fn order_ledger_receives_persistent_events() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let recorded = Arc::new(Mutex::new(Vec::new()));
    let outbox = init_outbox_with_handler(
        &pool,
        &mut jobs,
        OrderLedgerHandler {
            recorded: recorded.clone(),
        },
    )
    .await?;

    jobs.start_poll().await?;

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, OrderEvent::OrderPlaced { order_id: 1 })
        .await?;
    outbox
        .publish_persisted_in_op(&mut op, OrderEvent::OrderPlaced { order_id: 2 })
        .await?;
    outbox
        .publish_persisted_in_op(&mut op, OrderEvent::OrderPlaced { order_id: 3 })
        .await?;
    op.commit().await?;

    let start = std::time::Instant::now();
    loop {
        let events = recorded.lock().await;
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
async fn order_dashboard_receives_ephemeral_events() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let displayed = Arc::new(Mutex::new(Vec::new()));
    let outbox = init_outbox_with_handler(
        &pool,
        &mut jobs,
        OrderDashboardHandler {
            displayed: displayed.clone(),
        },
    )
    .await?;

    jobs.start_poll().await?;

    // Give the job time to start and begin listening
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let event_type = obix::out::EphemeralEventType::new("order_update");
    outbox
        .publish_ephemeral(event_type, OrderEvent::OrderPlaced { order_id: 42 })
        .await?;

    let start = std::time::Instant::now();
    loop {
        let events = displayed.lock().await;
        if !events.is_empty() {
            assert!(events.iter().all(|&v| v == 42));
            break;
        }
        drop(events);
        if start.elapsed() > std::time::Duration::from_secs(5) {
            anyhow::bail!("Timeout waiting for ephemeral event");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn order_ledger_resumes_after_restart() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    // First run: process two events
    let recorded_first = Arc::new(Mutex::new(Vec::new()));
    {
        let job_config = job::JobSvcConfig::builder()
            .pool(pool.clone())
            .build()
            .unwrap();
        let mut jobs = job::Jobs::init(job_config).await?;

        let outbox = init_outbox_with_handler(
            &pool,
            &mut jobs,
            OrderLedgerHandler {
                recorded: recorded_first.clone(),
            },
        )
        .await?;

        jobs.start_poll().await?;

        let mut op = outbox.begin_op().await?;
        outbox
            .publish_persisted_in_op(&mut op, OrderEvent::OrderPlaced { order_id: 10 })
            .await?;
        outbox
            .publish_persisted_in_op(&mut op, OrderEvent::OrderPlaced { order_id: 11 })
            .await?;
        op.commit().await?;

        let start = std::time::Instant::now();
        loop {
            let events = recorded_first.lock().await;
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

    // Second run: handler should NOT receive event with order_id 10 again
    let recorded_second = Arc::new(Mutex::new(Vec::new()));
    {
        let job_config = job::JobSvcConfig::builder()
            .pool(pool.clone())
            .build()
            .unwrap();
        let mut jobs = job::Jobs::init(job_config).await?;

        // Re-init outbox (don't wipe tables — we want to keep the sequence state)
        let outbox = Outbox::<OrderEvent, TestTables>::init(
            &pool,
            MailboxConfig::builder()
                .build()
                .expect("Couldn't build MailboxConfig"),
        )
        .await?;

        outbox
            .register_event_handler(
                &mut jobs,
                OutboxEventJobConfig::new(job::JobType::new(ORDER_HANDLER_JOB_TYPE)),
                OrderLedgerHandler {
                    recorded: recorded_second.clone(),
                },
            )
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        jobs.start_poll().await?;

        let mut op = outbox.begin_op().await?;
        outbox
            .publish_persisted_in_op(&mut op, OrderEvent::OrderPlaced { order_id: 20 })
            .await?;
        op.commit().await?;

        let start = std::time::Instant::now();
        loop {
            let events = recorded_second.lock().await;
            if !events.is_empty() {
                // Should only have 20, not 10
                assert_eq!(*events, vec![20]);
                break;
            }
            drop(events);
            if start.elapsed() > std::time::Duration::from_secs(5) {
                anyhow::bail!("Timeout waiting for second-run event");
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        // Wait a bit to make sure no stale events arrive
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let events = recorded_second.lock().await;
        assert_eq!(*events, vec![20]);
    }

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn order_activity_handler_receives_both_event_types() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let persistent_recorded = Arc::new(Mutex::new(Vec::new()));
    let ephemeral_displayed = Arc::new(Mutex::new(Vec::new()));

    let outbox = init_outbox_with_handler(
        &pool,
        &mut jobs,
        OrderActivityHandler {
            persistent_recorded: persistent_recorded.clone(),
            ephemeral_displayed: ephemeral_displayed.clone(),
        },
    )
    .await?;

    jobs.start_poll().await?;

    // Give the job time to start
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Publish persistent event
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, OrderEvent::OrderPlaced { order_id: 100 })
        .await?;
    op.commit().await?;

    // Publish ephemeral event
    let event_type = obix::out::EphemeralEventType::new("order_update");
    outbox
        .publish_ephemeral(event_type, OrderEvent::OrderPlaced { order_id: 200 })
        .await?;

    let start = std::time::Instant::now();
    loop {
        let p = persistent_recorded.lock().await;
        let e = ephemeral_displayed.lock().await;
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

// -- Fulfillment handler: spawns a downstream job from an event --

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
        .register_event_handler_with(
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
        .register_event_handler_with(
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
