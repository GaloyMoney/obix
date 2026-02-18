mod helpers;

use std::sync::Arc;

use obix::{
    MailboxConfig, OutboxEventHandler, OutboxEventJobConfig, OutboxMultiEventHandler, out::Outbox,
};
use serde::{Deserialize, Serialize};
use serial_test::file_serial;
use tokio::sync::Mutex;

use helpers::{TestTables, init_pool, wipeout_outbox_job_tables, wipeout_outbox_tables};

const JOB_TYPE: &str = "test-outbox-handler";

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
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.received.lock().await.push(event.0);
        Ok(())
    }
}

#[tokio::test]
#[file_serial]
async fn handler_receives_persistent_events() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    wipeout_outbox_tables(&pool).await?;
    wipeout_outbox_job_tables(&pool, JOB_TYPE).await?;

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

    let received = Arc::new(Mutex::new(Vec::new()));
    outbox
        .register_event_handler(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE)),
            TestHandler {
                received: received.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    jobs.start_poll().await?;

    let mut op = outbox.begin_op().await?;
    outbox.publish_persisted_in_op(&mut op, Ping(1)).await?;
    outbox.publish_persisted_in_op(&mut op, Ping(2)).await?;
    outbox.publish_persisted_in_op(&mut op, Ping(3)).await?;
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
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.received.lock().await.push(event.0);
        Ok(())
    }
}

#[tokio::test]
#[file_serial]
async fn handler_receives_ephemeral_events() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    wipeout_outbox_tables(&pool).await?;
    wipeout_outbox_job_tables(&pool, EPHEMERAL_JOB_TYPE).await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let outbox = Outbox::<EphemeralTestEvent, TestTables>::init(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let received = Arc::new(Mutex::new(Vec::new()));
    outbox
        .register_event_handler(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(EPHEMERAL_JOB_TYPE)),
            EphemeralTestHandler {
                received: received.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    jobs.start_poll().await?;

    // Give the job time to start and begin listening
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let event_type = obix::out::EphemeralEventType::new("test_type");
    outbox
        .publish_ephemeral(event_type.clone(), EphemeralPing(42))
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

    wipeout_outbox_tables(&pool).await?;
    wipeout_outbox_job_tables(&pool, JOB_TYPE).await?;

    // First run: process some events
    let received_first = Arc::new(Mutex::new(Vec::new()));
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
            .register_event_handler(
                &mut jobs,
                OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE)),
                TestHandler {
                    received: received_first.clone(),
                },
            )
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        jobs.start_poll().await?;

        let mut op = outbox.begin_op().await?;
        outbox.publish_persisted_in_op(&mut op, Ping(10)).await?;
        outbox.publish_persisted_in_op(&mut op, Ping(20)).await?;
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
                TestHandler {
                    received: received_second.clone(),
                },
            )
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        jobs.start_poll().await?;

        let mut op = outbox.begin_op().await?;
        outbox.publish_persisted_in_op(&mut op, Ping(30)).await?;
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
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.received.lock().await.push(event.0);
        Ok(())
    }
}

#[tokio::test]
#[file_serial]
async fn handler_receives_both_persistent_and_ephemeral() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    wipeout_outbox_tables(&pool).await?;
    wipeout_outbox_job_tables(&pool, MIXED_PERSIST_JOB).await?;
    wipeout_outbox_job_tables(&pool, MIXED_EPHEMERAL_JOB).await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let outbox = Outbox::<MixedEvent, TestTables>::init(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let persist_received = Arc::new(Mutex::new(Vec::new()));
    outbox
        .register_event_handler(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(MIXED_PERSIST_JOB)),
            MixedPersistHandler {
                received: persist_received.clone(),
            },
        )
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
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    jobs.start_poll().await?;

    // Give the job time to start
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

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

    let start = std::time::Instant::now();
    loop {
        let persists = persist_received.lock().await;
        let ephemerals = ephemeral_received.lock().await;
        if !persists.is_empty() && !ephemerals.is_empty() {
            assert_eq!(*persists, vec![100]);
            assert!(ephemerals.iter().all(|&v| v == 200));
            break;
        }
        drop(persists);
        drop(ephemerals);
        if start.elapsed() > std::time::Duration::from_secs(5) {
            anyhow::bail!("Timeout waiting for both event types");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

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
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.received.lock().await.push(event.0.clone());
        Ok(())
    }
}

#[tokio::test]
#[file_serial]
async fn handler_only_receives_matching_event_variant() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    wipeout_outbox_tables(&pool).await?;
    wipeout_outbox_job_tables(&pool, PING_JOB_TYPE).await?;
    wipeout_outbox_job_tables(&pool, PONG_JOB_TYPE).await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let outbox = Outbox::<MultiEvent, TestTables>::init(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let ping_received = Arc::new(Mutex::new(Vec::new()));
    outbox
        .register_event_handler(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(PING_JOB_TYPE)),
            PingHandler {
                received: ping_received.clone(),
            },
        )
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

    let start = std::time::Instant::now();
    loop {
        let pings = ping_received.lock().await;
        let pongs = pong_received.lock().await;
        if pings.len() >= 2 && pongs.len() >= 2 {
            assert_eq!(*pings, vec![1, 2]);
            assert_eq!(*pongs, vec!["hello", "world"]);
            break;
        }
        drop(pings);
        drop(pongs);
        if start.elapsed() > std::time::Duration::from_secs(5) {
            let pings = ping_received.lock().await;
            let pongs = pong_received.lock().await;
            anyhow::bail!(
                "Timeout: ping_received={:?}, pong_received={:?}",
                *pings,
                *pongs
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

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
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.pongs.lock().await.push(event.0.clone());
        Ok(())
    }
}

#[tokio::test]
#[file_serial]
async fn multi_event_handler_receives_all_matching_events() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    wipeout_outbox_tables(&pool).await?;
    wipeout_outbox_job_tables(&pool, MULTI_HANDLER_JOB_TYPE).await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let outbox = Outbox::<MultiEvent, TestTables>::init(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let pings = Arc::new(Mutex::new(Vec::new()));
    let pongs = Arc::new(Mutex::new(Vec::new()));

    let handler = MultiHandler {
        pings: pings.clone(),
        pongs: pongs.clone(),
    };

    let multi = OutboxMultiEventHandler::new(handler)
        .with_event::<PingEvent>()
        .with_event::<PongEvent>();

    outbox
        .register_multi_event_handler(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(MULTI_HANDLER_JOB_TYPE)),
            multi,
        )
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

    let start = std::time::Instant::now();
    loop {
        let p = pings.lock().await;
        let q = pongs.lock().await;
        if p.len() >= 2 && q.len() >= 2 {
            assert_eq!(*p, vec![1, 2]);
            assert_eq!(*q, vec!["hello", "world"]);
            break;
        }
        drop(p);
        drop(q);
        if start.elapsed() > std::time::Duration::from_secs(5) {
            let p = pings.lock().await;
            let q = pongs.lock().await;
            anyhow::bail!("Timeout: pings={:?}, pongs={:?}", *p, *q);
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    Ok(())
}

const MULTI_HANDLER_EPHEMERAL_JOB_TYPE: &str = "test-multi-handler-ephemeral";

#[tokio::test]
#[file_serial]
async fn multi_event_handler_receives_ephemeral_events() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    wipeout_outbox_tables(&pool).await?;
    wipeout_outbox_job_tables(&pool, MULTI_HANDLER_EPHEMERAL_JOB_TYPE).await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let outbox = Outbox::<MultiEvent, TestTables>::init(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let pings = Arc::new(Mutex::new(Vec::new()));
    let pongs = Arc::new(Mutex::new(Vec::new()));

    let handler = MultiHandler {
        pings: pings.clone(),
        pongs: pongs.clone(),
    };

    let multi = OutboxMultiEventHandler::new(handler)
        .with_event::<PingEvent>()
        .with_event::<PongEvent>();

    outbox
        .register_multi_event_handler(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(MULTI_HANDLER_EPHEMERAL_JOB_TYPE)),
            multi,
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    jobs.start_poll().await?;

    // Give the job time to start and begin listening for ephemeral events
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

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

    let start = std::time::Instant::now();
    loop {
        let p = pings.lock().await;
        let q = pongs.lock().await;
        if p.len() >= 2 && q.len() >= 2 {
            assert_eq!(*p, vec![10, 20]);
            assert_eq!(*q, vec!["ephemeral_hello", "ephemeral_world"]);
            break;
        }
        drop(p);
        drop(q);
        if start.elapsed() > std::time::Duration::from_secs(5) {
            let p = pings.lock().await;
            let q = pongs.lock().await;
            anyhow::bail!(
                "Timeout waiting for ephemeral multi-events: pings={:?}, pongs={:?}",
                *p,
                *q
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    Ok(())
}
