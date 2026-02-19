mod helpers;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use obix::{MailboxConfig, OutboxEventHandler, OutboxEventJobConfig, out::Outbox};
use serde::{Deserialize, Serialize};
use serial_test::file_serial;
use tokio::sync::Mutex;

use helpers::{
    TestTables, init_jobs, init_outbox, init_pool, poll_until, wipeout_outbox_job_tables,
};

const JOB_TYPE: &str = "test-outbox-handler";
const TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Ping(u64);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, obix::OutboxEvent)]
enum TestEvent {
    Ping(Ping),
}

struct TestHandler {
    received: Arc<Mutex<Vec<u64>>>,
}

impl OutboxEventHandler<Ping> for TestHandler {
    async fn handle(
        &self,
        _op: &mut es_entity::DbOp<'_>,
        event: &Ping,
        _meta: obix::out::OutboxEventMeta,
        _spawner: &obix::CommandJobSpawner,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.received.lock().await.push(event.0);
        Ok(())
    }
}

#[tokio::test]
#[file_serial]
async fn handler_receives_persistent_events() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipeout_outbox_job_tables(&pool, JOB_TYPE).await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox: Outbox<TestEvent, TestTables> =
        init_outbox(&pool, MailboxConfig::builder().build().unwrap()).await?;

    let received = Arc::new(Mutex::new(Vec::new()));
    outbox
        .register_event_handler(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE)),
            TestHandler {
                received: received.clone(),
            },
        )
        .with_event::<Ping>()
        .register()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    jobs.start_poll().await?;

    let mut op = outbox.begin_op().await?;
    outbox.publish_persisted_in_op(&mut op, Ping(1)).await?;
    outbox.publish_persisted_in_op(&mut op, Ping(2)).await?;
    outbox.publish_persisted_in_op(&mut op, Ping(3)).await?;
    op.commit().await?;

    poll_until(TIMEOUT, || {
        let received = received.clone();
        async move {
            let events = received.lock().await;
            if events.len() >= 3 {
                assert_eq!(*events, vec![1, 2, 3]);
                Some(())
            } else {
                None
            }
        }
    })
    .await?;

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct EphemeralPing(u64);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, obix::OutboxEvent)]
enum EphemeralTestEvent {
    Ping(EphemeralPing),
}

const EPHEMERAL_JOB_TYPE: &str = "test-ephemeral-handler";

struct EphemeralTestHandler {
    received: Arc<Mutex<Vec<u64>>>,
}

impl OutboxEventHandler<EphemeralPing> for EphemeralTestHandler {
    async fn handle(
        &self,
        _op: &mut es_entity::DbOp<'_>,
        event: &EphemeralPing,
        _meta: obix::out::OutboxEventMeta,
        _spawner: &obix::CommandJobSpawner,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.received.lock().await.push(event.0);
        Ok(())
    }
}

#[tokio::test]
#[file_serial]
async fn handler_receives_ephemeral_events() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipeout_outbox_job_tables(&pool, EPHEMERAL_JOB_TYPE).await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox: Outbox<EphemeralTestEvent, TestTables> =
        init_outbox(&pool, MailboxConfig::builder().build().unwrap()).await?;

    let received = Arc::new(Mutex::new(Vec::new()));
    outbox
        .register_event_handler(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(EPHEMERAL_JOB_TYPE)),
            EphemeralTestHandler {
                received: received.clone(),
            },
        )
        .with_event::<EphemeralPing>()
        .register()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    jobs.start_poll().await?;

    // Give the job time to start and begin listening
    tokio::time::sleep(Duration::from_millis(200)).await;

    let event_type = obix::out::EphemeralEventType::new("test_type");
    outbox
        .publish_ephemeral(event_type.clone(), EphemeralPing(42))
        .await?;

    poll_until(TIMEOUT, || {
        let received = received.clone();
        async move {
            let events = received.lock().await;
            if !events.is_empty() {
                assert!(events.iter().all(|&v| v == 42));
                Some(())
            } else {
                None
            }
        }
    })
    .await?;

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn handler_resumes_from_last_sequence_on_restart() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipeout_outbox_job_tables(&pool, JOB_TYPE).await?;

    // First run: process some events
    let received_first = Arc::new(Mutex::new(Vec::new()));
    {
        let mut jobs = init_jobs(&pool).await?;
        let outbox: Outbox<TestEvent, TestTables> =
            init_outbox(&pool, MailboxConfig::builder().build().unwrap()).await?;

        outbox
            .register_event_handler(
                &mut jobs,
                OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE)),
                TestHandler {
                    received: received_first.clone(),
                },
            )
            .with_event::<Ping>()
            .register()
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        jobs.start_poll().await?;

        let mut op = outbox.begin_op().await?;
        outbox.publish_persisted_in_op(&mut op, Ping(10)).await?;
        outbox.publish_persisted_in_op(&mut op, Ping(20)).await?;
        op.commit().await?;

        poll_until(TIMEOUT, || {
            let received_first = received_first.clone();
            async move {
                let events = received_first.lock().await;
                if events.len() >= 2 { Some(()) } else { None }
            }
        })
        .await?;

        jobs.shutdown().await?;
    }

    // Second run: publish more events, handler should NOT receive events 10,20 again
    let received_second = Arc::new(Mutex::new(Vec::new()));
    {
        let mut jobs = init_jobs(&pool).await?;

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
                TestHandler {
                    received: received_second.clone(),
                },
            )
            .with_event::<Ping>()
            .register()
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        jobs.start_poll().await?;

        let mut op = outbox.begin_op().await?;
        outbox.publish_persisted_in_op(&mut op, Ping(30)).await?;
        op.commit().await?;

        poll_until(TIMEOUT, || {
            let received_second = received_second.clone();
            async move {
                let events = received_second.lock().await;
                if !events.is_empty() {
                    // Should only have 30, not 10 or 20
                    assert_eq!(*events, vec![30]);
                    Some(())
                } else {
                    None
                }
            }
        })
        .await?;

        // Wait a bit to make sure no stale events arrive
        tokio::time::sleep(Duration::from_millis(200)).await;
        let events = received_second.lock().await;
        assert_eq!(*events, vec![30]);
    }

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct MixedPersist(u64);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct MixedEphemeral(u64);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, obix::OutboxEvent)]
enum MixedEvent {
    Persist(MixedPersist),
    Ephemeral(MixedEphemeral),
}

const MIXED_PERSIST_JOB: &str = "test-mixed-persist";
const MIXED_EPHEMERAL_JOB: &str = "test-mixed-ephemeral";

struct MixedPersistHandler {
    received: Arc<Mutex<Vec<u64>>>,
}

impl OutboxEventHandler<MixedPersist> for MixedPersistHandler {
    async fn handle(
        &self,
        _op: &mut es_entity::DbOp<'_>,
        event: &MixedPersist,
        _meta: obix::out::OutboxEventMeta,
        _spawner: &obix::CommandJobSpawner,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.received.lock().await.push(event.0);
        Ok(())
    }
}

struct MixedEphemeralHandler {
    received: Arc<Mutex<Vec<u64>>>,
}

impl OutboxEventHandler<MixedEphemeral> for MixedEphemeralHandler {
    async fn handle(
        &self,
        _op: &mut es_entity::DbOp<'_>,
        event: &MixedEphemeral,
        _meta: obix::out::OutboxEventMeta,
        _spawner: &obix::CommandJobSpawner,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.received.lock().await.push(event.0);
        Ok(())
    }
}

#[tokio::test]
#[file_serial]
async fn handler_receives_both_persistent_and_ephemeral() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipeout_outbox_job_tables(&pool, MIXED_PERSIST_JOB).await?;
    wipeout_outbox_job_tables(&pool, MIXED_EPHEMERAL_JOB).await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox: Outbox<MixedEvent, TestTables> =
        init_outbox(&pool, MailboxConfig::builder().build().unwrap()).await?;

    let persist_received = Arc::new(Mutex::new(Vec::new()));
    outbox
        .register_event_handler(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(MIXED_PERSIST_JOB)),
            MixedPersistHandler {
                received: persist_received.clone(),
            },
        )
        .with_event::<MixedPersist>()
        .register()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let ephemeral_received = Arc::new(Mutex::new(Vec::new()));
    outbox
        .register_event_handler(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(MIXED_EPHEMERAL_JOB)),
            MixedEphemeralHandler {
                received: ephemeral_received.clone(),
            },
        )
        .with_event::<MixedEphemeral>()
        .register()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    jobs.start_poll().await?;

    // Give the job time to start
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Publish persistent event
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, MixedPersist(100))
        .await?;
    op.commit().await?;

    // Publish ephemeral event
    let event_type = obix::out::EphemeralEventType::new("both_test");
    outbox
        .publish_ephemeral(event_type, MixedEphemeral(200))
        .await?;

    poll_until(TIMEOUT, || {
        let persist_received = persist_received.clone();
        let ephemeral_received = ephemeral_received.clone();
        async move {
            let persists = persist_received.lock().await;
            let ephemerals = ephemeral_received.lock().await;
            if !persists.is_empty() && !ephemerals.is_empty() {
                assert_eq!(*persists, vec![100]);
                assert!(ephemerals.iter().all(|&v| v == 200));
                Some(())
            } else {
                None
            }
        }
    })
    .await?;

    Ok(())
}

// --- Multi-variant event tests ---

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct PingEvent(u64);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct PongEvent(String);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, obix::OutboxEvent)]
enum MultiEvent {
    Ping(PingEvent),
    Pong(PongEvent),
}

const PING_JOB_TYPE: &str = "test-ping-handler";
const PONG_JOB_TYPE: &str = "test-pong-handler";

struct PingHandler {
    received: Arc<Mutex<Vec<u64>>>,
}

impl OutboxEventHandler<PingEvent> for PingHandler {
    async fn handle(
        &self,
        _op: &mut es_entity::DbOp<'_>,
        event: &PingEvent,
        _meta: obix::out::OutboxEventMeta,
        _spawner: &obix::CommandJobSpawner,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.received.lock().await.push(event.0);
        Ok(())
    }
}

struct PongHandler {
    received: Arc<Mutex<Vec<String>>>,
}

impl OutboxEventHandler<PongEvent> for PongHandler {
    async fn handle(
        &self,
        _op: &mut es_entity::DbOp<'_>,
        event: &PongEvent,
        _meta: obix::out::OutboxEventMeta,
        _spawner: &obix::CommandJobSpawner,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.received.lock().await.push(event.0.clone());
        Ok(())
    }
}

#[tokio::test]
#[file_serial]
async fn handler_only_receives_matching_event_variant() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipeout_outbox_job_tables(&pool, PING_JOB_TYPE).await?;
    wipeout_outbox_job_tables(&pool, PONG_JOB_TYPE).await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox: Outbox<MultiEvent, TestTables> =
        init_outbox(&pool, MailboxConfig::builder().build().unwrap()).await?;

    let ping_received = Arc::new(Mutex::new(Vec::new()));
    outbox
        .register_event_handler(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(PING_JOB_TYPE)),
            PingHandler {
                received: ping_received.clone(),
            },
        )
        .with_event::<PingEvent>()
        .register()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let pong_received = Arc::new(Mutex::new(Vec::new()));
    outbox
        .register_event_handler(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(PONG_JOB_TYPE)),
            PongHandler {
                received: pong_received.clone(),
            },
        )
        .with_event::<PongEvent>()
        .register()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    jobs.start_poll().await?;

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, PingEvent(1))
        .await?;
    outbox
        .publish_persisted_in_op(&mut op, PongEvent("hello".into()))
        .await?;
    outbox
        .publish_persisted_in_op(&mut op, PingEvent(2))
        .await?;
    outbox
        .publish_persisted_in_op(&mut op, PongEvent("world".into()))
        .await?;
    op.commit().await?;

    poll_until(TIMEOUT, || {
        let ping_received = ping_received.clone();
        let pong_received = pong_received.clone();
        async move {
            let pings = ping_received.lock().await;
            let pongs = pong_received.lock().await;
            if pings.len() >= 2 && pongs.len() >= 2 {
                assert_eq!(*pings, vec![1, 2]);
                assert_eq!(*pongs, vec!["hello", "world"]);
                Some(())
            } else {
                None
            }
        }
    })
    .await?;

    Ok(())
}

// --- Multi-event handler test (single handler struct, multiple event types, one job) ---

const MULTI_HANDLER_JOB_TYPE: &str = "test-multi-handler";

struct MultiHandler {
    pings: Arc<Mutex<Vec<u64>>>,
    pongs: Arc<Mutex<Vec<String>>>,
}

impl OutboxEventHandler<PingEvent> for MultiHandler {
    async fn handle(
        &self,
        _op: &mut es_entity::DbOp<'_>,
        event: &PingEvent,
        _meta: obix::out::OutboxEventMeta,
        _spawner: &obix::CommandJobSpawner,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.pings.lock().await.push(event.0);
        Ok(())
    }
}

impl OutboxEventHandler<PongEvent> for MultiHandler {
    async fn handle(
        &self,
        _op: &mut es_entity::DbOp<'_>,
        event: &PongEvent,
        _meta: obix::out::OutboxEventMeta,
        _spawner: &obix::CommandJobSpawner,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.pongs.lock().await.push(event.0.clone());
        Ok(())
    }
}

#[tokio::test]
#[file_serial]
async fn multi_event_handler_receives_all_matching_events() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipeout_outbox_job_tables(&pool, MULTI_HANDLER_JOB_TYPE).await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox: Outbox<MultiEvent, TestTables> =
        init_outbox(&pool, MailboxConfig::builder().build().unwrap()).await?;

    let pings = Arc::new(Mutex::new(Vec::new()));
    let pongs = Arc::new(Mutex::new(Vec::new()));

    let handler = MultiHandler {
        pings: pings.clone(),
        pongs: pongs.clone(),
    };

    outbox
        .register_event_handler(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(MULTI_HANDLER_JOB_TYPE)),
            handler,
        )
        .with_event::<PingEvent>()
        .with_event::<PongEvent>()
        .register()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    jobs.start_poll().await?;

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, PingEvent(1))
        .await?;
    outbox
        .publish_persisted_in_op(&mut op, PongEvent("hello".into()))
        .await?;
    outbox
        .publish_persisted_in_op(&mut op, PingEvent(2))
        .await?;
    outbox
        .publish_persisted_in_op(&mut op, PongEvent("world".into()))
        .await?;
    op.commit().await?;

    poll_until(TIMEOUT, || {
        let pings = pings.clone();
        let pongs = pongs.clone();
        async move {
            let p = pings.lock().await;
            let q = pongs.lock().await;
            if p.len() >= 2 && q.len() >= 2 {
                assert_eq!(*p, vec![1, 2]);
                assert_eq!(*q, vec!["hello", "world"]);
                Some(())
            } else {
                None
            }
        }
    })
    .await?;

    Ok(())
}

// --- Cross-domain handler test: single handler, two unrelated event types ---

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct UserCreated {
    user_id: u64,
    email: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct InvoicePaid {
    invoice_id: u64,
    amount_cents: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, obix::OutboxEvent)]
enum CrossDomainEvent {
    UserCreated(UserCreated),
    InvoicePaid(InvoicePaid),
}

const CROSS_DOMAIN_JOB_TYPE: &str = "test-cross-domain-handler";

/// A single handler that handles two conceptually unrelated events from different
/// domains (user management and billing). This demonstrates that handlers have no
/// coupling between the event types they handle — the `with_event` registration
/// compiles only because the handler implements `OutboxEventHandler<E>` for each
/// event type and the enum implements `OutboxEventMarker<E>` for each variant.
struct CrossDomainHandler {
    users: Arc<Mutex<Vec<(u64, String)>>>,
    invoices: Arc<Mutex<Vec<(u64, u64)>>>,
}

impl OutboxEventHandler<UserCreated> for CrossDomainHandler {
    async fn handle(
        &self,
        _op: &mut es_entity::DbOp<'_>,
        event: &UserCreated,
        _meta: obix::out::OutboxEventMeta,
        _spawner: &obix::CommandJobSpawner,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.users
            .lock()
            .await
            .push((event.user_id, event.email.clone()));
        Ok(())
    }
}

impl OutboxEventHandler<InvoicePaid> for CrossDomainHandler {
    async fn handle(
        &self,
        _op: &mut es_entity::DbOp<'_>,
        event: &InvoicePaid,
        _meta: obix::out::OutboxEventMeta,
        _spawner: &obix::CommandJobSpawner,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.invoices
            .lock()
            .await
            .push((event.invoice_id, event.amount_cents));
        Ok(())
    }
}

#[tokio::test]
#[file_serial]
async fn cross_domain_handler_receives_unrelated_events() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipeout_outbox_job_tables(&pool, CROSS_DOMAIN_JOB_TYPE).await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox: Outbox<CrossDomainEvent, TestTables> =
        init_outbox(&pool, MailboxConfig::builder().build().unwrap()).await?;

    let users = Arc::new(Mutex::new(Vec::new()));
    let invoices = Arc::new(Mutex::new(Vec::new()));

    let handler = CrossDomainHandler {
        users: users.clone(),
        invoices: invoices.clone(),
    };

    outbox
        .register_event_handler(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(CROSS_DOMAIN_JOB_TYPE)),
            handler,
        )
        .with_event::<UserCreated>()
        .with_event::<InvoicePaid>()
        .register()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    jobs.start_poll().await?;

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(
            &mut op,
            UserCreated {
                user_id: 42,
                email: "alice@example.com".into(),
            },
        )
        .await?;
    outbox
        .publish_persisted_in_op(
            &mut op,
            InvoicePaid {
                invoice_id: 1001,
                amount_cents: 9999,
            },
        )
        .await?;
    outbox
        .publish_persisted_in_op(
            &mut op,
            UserCreated {
                user_id: 43,
                email: "bob@example.com".into(),
            },
        )
        .await?;
    outbox
        .publish_persisted_in_op(
            &mut op,
            InvoicePaid {
                invoice_id: 1002,
                amount_cents: 4500,
            },
        )
        .await?;
    op.commit().await?;

    poll_until(TIMEOUT, || {
        let users = users.clone();
        let invoices = invoices.clone();
        async move {
            let u = users.lock().await;
            let i = invoices.lock().await;
            if u.len() >= 2 && i.len() >= 2 {
                assert_eq!(
                    *u,
                    vec![
                        (42, "alice@example.com".to_string()),
                        (43, "bob@example.com".to_string()),
                    ]
                );
                assert_eq!(*i, vec![(1001, 9999), (1002, 4500)]);
                Some(())
            } else {
                None
            }
        }
    })
    .await?;

    Ok(())
}

const MULTI_HANDLER_EPHEMERAL_JOB_TYPE: &str = "test-multi-handler-ephemeral";

#[tokio::test]
#[file_serial]
async fn multi_event_handler_receives_ephemeral_events() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipeout_outbox_job_tables(&pool, MULTI_HANDLER_EPHEMERAL_JOB_TYPE).await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox: Outbox<MultiEvent, TestTables> =
        init_outbox(&pool, MailboxConfig::builder().build().unwrap()).await?;

    let pings = Arc::new(Mutex::new(Vec::new()));
    let pongs = Arc::new(Mutex::new(Vec::new()));

    let handler = MultiHandler {
        pings: pings.clone(),
        pongs: pongs.clone(),
    };

    outbox
        .register_event_handler(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(MULTI_HANDLER_EPHEMERAL_JOB_TYPE)),
            handler,
        )
        .with_event::<PingEvent>()
        .with_event::<PongEvent>()
        .register()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    jobs.start_poll().await?;

    // Give the job time to start and begin listening for ephemeral events
    tokio::time::sleep(Duration::from_millis(200)).await;

    let event_type = obix::out::EphemeralEventType::new("multi_ephemeral_test");
    outbox
        .publish_ephemeral(event_type.clone(), PingEvent(10))
        .await?;
    outbox
        .publish_ephemeral(event_type.clone(), PongEvent("ephemeral_hello".into()))
        .await?;
    outbox
        .publish_ephemeral(event_type.clone(), PingEvent(20))
        .await?;
    outbox
        .publish_ephemeral(event_type.clone(), PongEvent("ephemeral_world".into()))
        .await?;

    poll_until(TIMEOUT, || {
        let pings = pings.clone();
        let pongs = pongs.clone();
        async move {
            let p = pings.lock().await;
            let q = pongs.lock().await;
            // Ephemeral events may be delivered more than once (PG NOTIFY + cache_fill
            // paths can both fire), so check that all expected values are present
            // rather than asserting exact counts.
            if p.contains(&10)
                && p.contains(&20)
                && q.iter().any(|s| s == "ephemeral_hello")
                && q.iter().any(|s| s == "ephemeral_world")
            {
                assert!(p.iter().all(|v| *v == 10 || *v == 20));
                assert!(
                    q.iter()
                        .all(|s| s == "ephemeral_hello" || s == "ephemeral_world")
                );
                Some(())
            } else {
                None
            }
        }
    })
    .await?;

    Ok(())
}

// --- True cross-domain test: one handler struct, two separate outbox enums, two outboxes ---

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct AccountCreated {
    account_id: u64,
    name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct PaymentReceived {
    payment_id: u64,
    amount_cents: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, obix::OutboxEvent)]
enum UserDomainEvent {
    AccountCreated(AccountCreated),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, obix::OutboxEvent)]
enum BillingDomainEvent {
    PaymentReceived(PaymentReceived),
}

const USER_DOMAIN_JOB_TYPE: &str = "test-user-domain-handler";
const BILLING_DOMAIN_JOB_TYPE: &str = "test-billing-domain-handler";

/// A single handler struct that spans two completely separate outbox enums.
/// It implements `OutboxEventHandler<AccountCreated>` and `OutboxEventHandler<PaymentReceived>`,
/// and is registered on two different outboxes — proving that handler structs are not
/// coupled to a single enum and can span domain boundaries via multiple registrations.
struct TrueCrossDomainHandler {
    accounts: Arc<Mutex<Vec<(u64, String)>>>,
    payments: Arc<Mutex<Vec<(u64, u64)>>>,
}

impl OutboxEventHandler<AccountCreated> for TrueCrossDomainHandler {
    async fn handle(
        &self,
        _op: &mut es_entity::DbOp<'_>,
        event: &AccountCreated,
        _meta: obix::out::OutboxEventMeta,
        _spawner: &obix::CommandJobSpawner,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.accounts
            .lock()
            .await
            .push((event.account_id, event.name.clone()));
        Ok(())
    }
}

impl OutboxEventHandler<PaymentReceived> for TrueCrossDomainHandler {
    async fn handle(
        &self,
        _op: &mut es_entity::DbOp<'_>,
        event: &PaymentReceived,
        _meta: obix::out::OutboxEventMeta,
        _spawner: &obix::CommandJobSpawner,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.payments
            .lock()
            .await
            .push((event.payment_id, event.amount_cents));
        Ok(())
    }
}

#[tokio::test]
#[file_serial]
async fn handler_spans_two_separate_outbox_enums() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipeout_outbox_job_tables(&pool, USER_DOMAIN_JOB_TYPE).await?;
    wipeout_outbox_job_tables(&pool, BILLING_DOMAIN_JOB_TYPE).await?;
    let mut jobs = init_jobs(&pool).await?;

    // This test needs two separate outboxes with different enum types.
    // init_outbox wipes shared outbox tables, so call it once for the first,
    // then init the second directly (tables are already clean).
    let user_outbox: Outbox<UserDomainEvent, TestTables> =
        init_outbox(&pool, MailboxConfig::builder().build().unwrap()).await?;
    let billing_outbox = Outbox::<BillingDomainEvent, TestTables>::init(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let accounts = Arc::new(Mutex::new(Vec::new()));
    let payments = Arc::new(Mutex::new(Vec::new()));

    // Two handler instances sharing the same state — the same struct type is
    // registered on two different outboxes with different enum parameters.
    // Register on the user-domain outbox
    user_outbox
        .register_event_handler(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(USER_DOMAIN_JOB_TYPE)),
            TrueCrossDomainHandler {
                accounts: accounts.clone(),
                payments: payments.clone(),
            },
        )
        .with_event::<AccountCreated>()
        .register()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // Register on the billing-domain outbox
    billing_outbox
        .register_event_handler(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(BILLING_DOMAIN_JOB_TYPE)),
            TrueCrossDomainHandler {
                accounts: accounts.clone(),
                payments: payments.clone(),
            },
        )
        .with_event::<PaymentReceived>()
        .register()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    jobs.start_poll().await?;

    // Publish on the user-domain outbox
    let mut op = user_outbox.begin_op().await?;
    user_outbox
        .publish_persisted_in_op(
            &mut op,
            AccountCreated {
                account_id: 1,
                name: "alice".into(),
            },
        )
        .await?;
    user_outbox
        .publish_persisted_in_op(
            &mut op,
            AccountCreated {
                account_id: 2,
                name: "bob".into(),
            },
        )
        .await?;
    op.commit().await?;

    // Publish on the billing-domain outbox
    let mut op = billing_outbox.begin_op().await?;
    billing_outbox
        .publish_persisted_in_op(
            &mut op,
            PaymentReceived {
                payment_id: 100,
                amount_cents: 5000,
            },
        )
        .await?;
    billing_outbox
        .publish_persisted_in_op(
            &mut op,
            PaymentReceived {
                payment_id: 101,
                amount_cents: 7500,
            },
        )
        .await?;
    op.commit().await?;

    poll_until(TIMEOUT, || {
        let accounts = accounts.clone();
        let payments = payments.clone();
        async move {
            let a = accounts.lock().await;
            let p = payments.lock().await;
            if a.len() >= 2 && p.len() >= 2 {
                assert_eq!(*a, vec![(1, "alice".to_string()), (2, "bob".to_string())]);
                assert_eq!(*p, vec![(100, 5000), (101, 7500)]);
                Some(())
            } else {
                None
            }
        }
    })
    .await?;

    Ok(())
}

// --- Subset-of-variants test: handler does NOT need to handle all enum variants ---

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Registered {
    user_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct ProfileUpdated {
    user_id: u64,
    field: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Deactivated {
    user_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, obix::OutboxEvent)]
enum UserLifecycleEvent {
    Registered(Registered),
    ProfileUpdated(ProfileUpdated),
    Deactivated(Deactivated),
}

const SUBSET_JOB_TYPE: &str = "test-subset-handler";

/// A handler that only handles `Registered` and `Deactivated` events,
/// deliberately ignoring `ProfileUpdated`. This proves that handlers do NOT
/// need to handle all variants of an enum — only the subset they register for.
struct SubsetHandler {
    registered: Arc<Mutex<Vec<u64>>>,
    deactivated: Arc<Mutex<Vec<u64>>>,
}

impl OutboxEventHandler<Registered> for SubsetHandler {
    async fn handle(
        &self,
        _op: &mut es_entity::DbOp<'_>,
        event: &Registered,
        _meta: obix::out::OutboxEventMeta,
        _spawner: &obix::CommandJobSpawner,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.registered.lock().await.push(event.user_id);
        Ok(())
    }
}

impl OutboxEventHandler<Deactivated> for SubsetHandler {
    async fn handle(
        &self,
        _op: &mut es_entity::DbOp<'_>,
        event: &Deactivated,
        _meta: obix::out::OutboxEventMeta,
        _spawner: &obix::CommandJobSpawner,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.deactivated.lock().await.push(event.user_id);
        Ok(())
    }
}

#[tokio::test]
#[file_serial]
async fn handler_subscribes_to_subset_of_enum_variants() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipeout_outbox_job_tables(&pool, SUBSET_JOB_TYPE).await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox: Outbox<UserLifecycleEvent, TestTables> =
        init_outbox(&pool, MailboxConfig::builder().build().unwrap()).await?;

    let registered = Arc::new(Mutex::new(Vec::new()));
    let deactivated = Arc::new(Mutex::new(Vec::new()));

    let handler = SubsetHandler {
        registered: registered.clone(),
        deactivated: deactivated.clone(),
    };

    // Only register for Registered and Deactivated — NOT ProfileUpdated
    outbox
        .register_event_handler(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(SUBSET_JOB_TYPE)),
            handler,
        )
        .with_event::<Registered>()
        .with_event::<Deactivated>()
        .register()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    jobs.start_poll().await?;

    // Publish all three variant types — the handler should only receive two
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, Registered { user_id: 1 })
        .await?;
    outbox
        .publish_persisted_in_op(
            &mut op,
            ProfileUpdated {
                user_id: 1,
                field: "email".into(),
            },
        )
        .await?;
    outbox
        .publish_persisted_in_op(&mut op, Deactivated { user_id: 2 })
        .await?;
    outbox
        .publish_persisted_in_op(
            &mut op,
            ProfileUpdated {
                user_id: 2,
                field: "name".into(),
            },
        )
        .await?;
    outbox
        .publish_persisted_in_op(&mut op, Registered { user_id: 3 })
        .await?;
    op.commit().await?;

    // Wait for the handler to receive the Registered and Deactivated events
    poll_until(TIMEOUT, || {
        let registered = registered.clone();
        let deactivated = deactivated.clone();
        async move {
            let r = registered.lock().await;
            let d = deactivated.lock().await;
            if r.len() >= 2 && !d.is_empty() {
                assert_eq!(*r, vec![1, 3]);
                assert_eq!(*d, vec![2]);
                Some(())
            } else {
                None
            }
        }
    })
    .await?;

    // Extra wait to ensure ProfileUpdated events are NOT delivered
    tokio::time::sleep(Duration::from_millis(300)).await;
    let r = registered.lock().await;
    let d = deactivated.lock().await;
    assert_eq!(*r, vec![1, 3], "Should only contain Registered events");
    assert_eq!(*d, vec![2], "Should only contain Deactivated events");

    Ok(())
}

// --- Duplicate with_event detection test ---

#[tokio::test]
#[file_serial]
#[should_panic(expected = "duplicate with_event")]
async fn duplicate_with_event_panics() {
    let pool = init_pool().await.unwrap();
    wipeout_outbox_job_tables(&pool, "test-dup-detection")
        .await
        .unwrap();
    let mut jobs = init_jobs(&pool).await.unwrap();
    let outbox: Outbox<MultiEvent, TestTables> =
        init_outbox(&pool, MailboxConfig::builder().build().unwrap())
            .await
            .unwrap();

    let handler = MultiHandler {
        pings: Arc::new(Mutex::new(Vec::new())),
        pongs: Arc::new(Mutex::new(Vec::new())),
    };

    // This should panic — PingEvent is registered twice
    outbox
        .register_event_handler(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new("test-dup-detection")),
            handler,
        )
        .with_event::<PingEvent>()
        .with_event::<PingEvent>(); // duplicate!
}

// --- Handler spawns downstream job via CommandJobSpawner ---

const SPAWNING_HANDLER_JOB_TYPE: &str = "test-spawning-handler";
const DOWNSTREAM_JOB_TYPE: &str = "test-downstream-command";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SendNotificationConfig {
    user_id: u64,
    message: String,
}

struct SendNotificationInitializer {
    sent: Arc<Mutex<Vec<(u64, String)>>>,
}

impl job::JobInitializer for SendNotificationInitializer {
    type Config = SendNotificationConfig;

    fn job_type(&self) -> job::JobType {
        job::JobType::new(DOWNSTREAM_JOB_TYPE)
    }

    fn init(
        &self,
        job: &job::Job,
        _spawner: job::JobSpawner<Self::Config>,
    ) -> Result<Box<dyn job::JobRunner>, Box<dyn std::error::Error>> {
        let config: SendNotificationConfig = job.config()?;
        Ok(Box::new(SendNotificationRunner {
            config,
            sent: self.sent.clone(),
        }))
    }
}

struct SendNotificationRunner {
    config: SendNotificationConfig,
    sent: Arc<Mutex<Vec<(u64, String)>>>,
}

#[async_trait]
impl job::JobRunner for SendNotificationRunner {
    async fn run(
        &self,
        _current_job: job::CurrentJob,
    ) -> Result<job::JobCompletion, Box<dyn std::error::Error>> {
        self.sent
            .lock()
            .await
            .push((self.config.user_id, self.config.message.clone()));
        Ok(job::JobCompletion::Complete)
    }
}

struct SpawningHandler;

impl OutboxEventHandler<Ping> for SpawningHandler {
    async fn handle(
        &self,
        op: &mut es_entity::DbOp<'_>,
        event: &Ping,
        _meta: obix::out::OutboxEventMeta,
        spawner: &obix::CommandJobSpawner,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        spawner
            .spawn_in_op(
                op,
                job::JobId::new(),
                SendNotificationConfig {
                    user_id: event.0,
                    message: format!("ping received: {}", event.0),
                },
            )
            .await?;
        Ok(())
    }
}

#[tokio::test]
#[file_serial]
async fn handler_spawns_downstream_job_via_spawner() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipeout_outbox_job_tables(&pool, SPAWNING_HANDLER_JOB_TYPE).await?;
    wipeout_outbox_job_tables(&pool, DOWNSTREAM_JOB_TYPE).await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox: Outbox<TestEvent, TestTables> =
        init_outbox(&pool, MailboxConfig::builder().build().unwrap()).await?;

    let sent = Arc::new(Mutex::new(Vec::new()));

    outbox
        .register_event_handler(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(SPAWNING_HANDLER_JOB_TYPE)),
            SpawningHandler,
        )
        .with_job_initializer(SendNotificationInitializer { sent: sent.clone() })
        .with_event::<Ping>()
        .register()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    jobs.start_poll().await?;

    let mut op = outbox.begin_op().await?;
    outbox.publish_persisted_in_op(&mut op, Ping(42)).await?;
    op.commit().await?;

    poll_until(TIMEOUT, || {
        let sent = sent.clone();
        async move {
            let notifications = sent.lock().await;
            if !notifications.is_empty() {
                assert_eq!(notifications.len(), 1);
                assert_eq!(notifications[0].0, 42);
                assert_eq!(notifications[0].1, "ping received: 42");
                Some(())
            } else {
                None
            }
        }
    })
    .await?;

    Ok(())
}
