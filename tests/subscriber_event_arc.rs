//! Subscribers are handed the shared `Arc` the outbox decoded once, not a
//! borrow of it — so a handler can retain an event past the call for the
//! price of a refcount, and the payload type need not be `Clone`.
//!
//! Every subscriber in this file is generic over [`NonCloneEvent`], which
//! deliberately does **not** derive `Clone`: a batch holding whole events
//! could not be written at all if retaining one required copying it.

mod helpers;

use std::sync::Arc;

use obix::{
    EventCtx, EventDelivery, FlushOp, Handled, InsertOrder, KeyedEventCtx, KeyedSubscriber,
    KeyedSubscriberConfig, MailboxConfig, OutboxEventJobConfig, SingletonSubscriber,
    SubscriptionDef, WakeKey,
    out::{Outbox, OutboxEventMarker, PersistentOutboxEvent},
};
use serde::{Deserialize, Serialize};
use serial_test::file_serial;
use tokio::sync::Mutex;

use helpers::{
    TestTables, init_pool, wipeout_keyed_subscriber_job_tables, wipeout_outbox_job_tables,
    wipeout_outbox_tables, wipeout_subscriptions,
};

const JOB_TYPE: &str = "test-subscriber-event-arc";
const KEYED_JOB_TYPE: &str = "test-subscriber-event-arc-keyed";
const CLASSIFY_JOB_TYPE: &str = "test-subscriber-event-arc-classify";

/// No `Clone`. That is the point of this file.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
enum NonCloneEvent {
    Ping { owner: u64, n: u64 },
}

impl NonCloneEvent {
    fn n(&self) -> u64 {
        let NonCloneEvent::Ping { n, .. } = self;
        *n
    }
}

type Retained = Arc<PersistentOutboxEvent<NonCloneEvent>>;

/// What the batch carried into `flush`, flattened for assertion.
#[derive(Debug, PartialEq)]
struct Flushed {
    sequence: u64,
    n: u64,
    identity: usize,
}

/// The allocation a given `Arc` points at. Comparing this between the
/// invocation and the flush is what distinguishes "kept the event" from
/// "kept a copy of the event".
fn identity(event: &Retained) -> usize {
    Arc::as_ptr(event) as usize
}

#[derive(Clone, Default)]
struct Observed {
    /// Identities in the order `handle_persistent` / `handle` saw them.
    handled: Arc<Mutex<Vec<usize>>>,
    /// What each flush was handed.
    flushed: Arc<Mutex<Vec<Flushed>>>,
}

impl Observed {
    async fn record_flush(&self, items: Vec<Retained>) {
        let mut flushed = self.flushed.lock().await;
        for event in items {
            flushed.push(Flushed {
                sequence: u64::from(event.sequence),
                n: event.payload.as_ref().expect("payload").n(),
                identity: identity(&event),
            });
        }
    }
}

// === Singleton ===

struct RetainingSubscriber {
    observed: Observed,
}

impl SingletonSubscriber<NonCloneEvent> for RetainingSubscriber {
    /// A batch of whole events. Unrepresentable when the handler is handed a
    /// borrow, and uncopyable when the payload is not `Clone`.
    type Batch = Vec<Retained>;

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, Self::Batch>,
        event: &EventDelivery<NonCloneEvent>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        assert_eq!(event.position(), event.sequence);
        self.observed
            .handled
            .lock()
            .await
            .push(identity(event.inner()));
        Ok(ctx.collect(Arc::clone(event.inner())))
    }

    async fn flush(
        &self,
        _op: &mut FlushOp<'_, InsertOrder>,
        items: Self::Batch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.observed.record_flush(items).await;
        Ok(())
    }
}

/// Reads the event through the delivery without retaining it — the deref path
/// every existing handler body takes unchanged, now through two hops.
struct EphemeralReader {
    received: Arc<Mutex<Vec<u64>>>,
}

impl SingletonSubscriber<NonCloneEvent> for EphemeralReader {
    type Batch = ();

    async fn handle_ephemeral(
        &self,
        event: &Arc<obix::out::EphemeralOutboxEvent<NonCloneEvent>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.received.lock().await.push(event.payload.n());
        Ok(())
    }
}

// === Classifying through the Arc ===

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct PingPayload {
    owner: u64,
    n: u64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct PongPayload;

#[derive(Debug, Serialize, Deserialize, PartialEq, obix::OutboxEvent)]
#[serde(tag = "type")]
enum ClassifiedEvent {
    Ping(PingPayload),
    Pong(PongPayload),
}

type ClassifiedRetained = Arc<PersistentOutboxEvent<ClassifiedEvent>>;

/// Classifies through the delivery (`event.as_event::<PingPayload>()`, which
/// must keep resolving through `Delivery` → `Arc` → event) and retains the
/// same `Arc` in the batch, in the same `handle_persistent` body.
struct ClassifyingSubscriber {
    /// `n` of every `Ping` the handler classified and retained.
    classified: Arc<Mutex<Vec<u64>>>,
}

impl SingletonSubscriber<ClassifiedEvent> for ClassifyingSubscriber {
    type Batch = Vec<ClassifiedRetained>;

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, Self::Batch>,
        event: &EventDelivery<ClassifiedEvent>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        let Some(ping) = event.as_event::<PingPayload>() else {
            return Ok(ctx.skip());
        };
        self.classified.lock().await.push(ping.n);
        Ok(ctx.collect(Arc::clone(event.inner())))
    }

    async fn flush(
        &self,
        _op: &mut FlushOp<'_, InsertOrder>,
        items: Self::Batch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for event in items {
            let payload = event.payload.as_ref().expect("payload");
            assert_eq!(
                <ClassifiedEvent as OutboxEventMarker<PingPayload>>::as_event(payload),
                event.as_event::<PingPayload>()
            );
        }
        Ok(())
    }
}

// === Keyed ===

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct OwnerId(u64);

impl std::fmt::Display for OwnerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for OwnerId {
    type Err = std::num::ParseIntError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(OwnerId(s.parse()?))
    }
}

struct RetainingDef {
    observed: Observed,
}

impl SubscriptionDef<NonCloneEvent> for RetainingDef {
    type Key = OwnerId;
    type InstanceConfig = ();
    type Subscriber = RetainingKeyedSubscriber;

    fn wake_keys(
        &self,
        event: &PersistentOutboxEvent<NonCloneEvent>,
    ) -> impl IntoIterator<Item = WakeKey> {
        match &event.payload {
            Some(NonCloneEvent::Ping { owner, .. }) => vec![WakeKey::from(owner.to_string())],
            None => vec![],
        }
    }

    fn instantiate(&self, _key: Self::Key, _cfg: Self::InstanceConfig) -> Self::Subscriber {
        RetainingKeyedSubscriber {
            observed: self.observed.clone(),
        }
    }
}

struct RetainingKeyedSubscriber {
    observed: Observed,
}

impl KeyedSubscriber<NonCloneEvent> for RetainingKeyedSubscriber {
    type Batch = Vec<Retained>;

    async fn handle<'inv>(
        &self,
        ctx: KeyedEventCtx<'inv, Self::Batch>,
        event: &EventDelivery<NonCloneEvent>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        self.observed
            .handled
            .lock()
            .await
            .push(identity(event.inner()));
        Ok(ctx.collect(Arc::clone(event.inner())))
    }

    async fn flush(
        &self,
        _op: &mut FlushOp<'_, InsertOrder>,
        items: Self::Batch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.observed.record_flush(items).await;
        Ok(())
    }
}

// === Harness ===

async fn init_jobs(pool: &sqlx::PgPool) -> anyhow::Result<job::Jobs> {
    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    Ok(job::Jobs::init(job_config).await?)
}

async fn init_outbox(pool: &sqlx::PgPool) -> anyhow::Result<Outbox<NonCloneEvent, TestTables>> {
    wipeout_outbox_tables(pool).await?;
    wipeout_outbox_job_tables(pool, JOB_TYPE).await?;
    wipeout_keyed_subscriber_job_tables(pool, KEYED_JOB_TYPE).await?;
    wipeout_subscriptions(pool, KEYED_JOB_TYPE).await?;

    Ok(Outbox::<NonCloneEvent, TestTables>::init(
        pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?)
}

async fn init_classify_outbox(
    pool: &sqlx::PgPool,
) -> anyhow::Result<Outbox<ClassifiedEvent, TestTables>> {
    wipeout_outbox_tables(pool).await?;
    wipeout_outbox_job_tables(pool, CLASSIFY_JOB_TYPE).await?;

    Ok(Outbox::<ClassifiedEvent, TestTables>::init(
        pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?)
}

async fn publish_pings(
    outbox: &Outbox<NonCloneEvent, TestTables>,
    owner: u64,
    ns: std::ops::RangeInclusive<u64>,
) -> anyhow::Result<()> {
    let mut op = outbox.begin_op().await?;
    for n in ns {
        outbox
            .publish_persisted_in_op(&mut op, NonCloneEvent::Ping { owner, n })
            .await?;
    }
    op.commit().await?;
    Ok(())
}

async fn eventually<F, Fut>(timeout: std::time::Duration, mut f: F) -> anyhow::Result<()>
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
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// The `flush` saw the very allocations the invocations were handed, in the
/// same order — the batch shares the events rather than copying them.
async fn assert_batch_retained_what_it_was_handed(observed: &Observed) {
    let handled = observed.handled.lock().await;
    let flushed = observed.flushed.lock().await;

    assert_eq!(
        flushed.iter().map(|f| f.identity).collect::<Vec<_>>(),
        *handled,
        "batch items are not the events the handler was handed"
    );
    assert!(
        flushed.windows(2).all(|w| w[0].sequence < w[1].sequence),
        "sequences not strictly ascending: {flushed:?}"
    );
}

// === Contracts ===

/// A singleton subscriber batches whole events of a payload type that is not
/// `Clone`, and `flush` receives exactly the allocations the invocations
/// were handed.
#[tokio::test]
#[file_serial]
async fn a_singleton_batch_retains_whole_events() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let observed = Observed::default();
    outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE)),
            RetainingSubscriber {
                observed: observed.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    jobs.start_poll().await?;

    publish_pings(&outbox, 1, 1..=3).await?;

    eventually(std::time::Duration::from_secs(10), || async {
        Ok(observed.flushed.lock().await.len() >= 3)
    })
    .await?;

    assert_eq!(
        observed
            .flushed
            .lock()
            .await
            .iter()
            .map(|f| f.n)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_batch_retained_what_it_was_handed(&observed).await;

    let _ = jobs.shutdown().await;
    Ok(())
}

/// The keyed equivalent: same batch shape, same identity guarantee, on the
/// per-key runner.
#[tokio::test]
#[file_serial]
async fn a_keyed_batch_retains_whole_events() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let observed = Observed::default();
    let subs = outbox
        .register_keyed_subscriber(
            &mut jobs,
            KeyedSubscriberConfig::new(job::JobType::new(KEYED_JOB_TYPE))
                .with_linger(std::time::Duration::from_millis(150))
                .with_checkpoint_interval(std::time::Duration::from_millis(50)),
            RetainingDef {
                observed: observed.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut op = outbox.begin_op().await?;
    subs.subscribe_in_op(&mut op, OwnerId(1), (), WakeKey::from("1"))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    op.commit().await?;

    jobs.start_poll().await?;

    publish_pings(&outbox, 1, 1..=3).await?;

    eventually(std::time::Duration::from_secs(10), || async {
        Ok(observed.flushed.lock().await.len() >= 3)
    })
    .await?;

    assert_eq!(
        observed
            .flushed
            .lock()
            .await
            .iter()
            .map(|f| f.n)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_batch_retained_what_it_was_handed(&observed).await;

    let _ = jobs.shutdown().await;
    Ok(())
}

/// `ClassifyingSubscriber` classifies through the `Arc` and retains it, in
/// the same `handle_persistent` body.
#[tokio::test]
#[file_serial]
async fn a_singleton_classifies_through_the_arc_while_retaining() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_classify_outbox(&pool).await?;

    let classified = Arc::new(Mutex::new(Vec::new()));
    outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(CLASSIFY_JOB_TYPE)),
            ClassifyingSubscriber {
                classified: classified.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    jobs.start_poll().await?;

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(
            &mut op,
            ClassifiedEvent::Ping(PingPayload { owner: 1, n: 1 }),
        )
        .await?;
    outbox
        .publish_persisted_in_op(&mut op, ClassifiedEvent::Pong(PongPayload))
        .await?;
    outbox
        .publish_persisted_in_op(
            &mut op,
            ClassifiedEvent::Ping(PingPayload { owner: 1, n: 2 }),
        )
        .await?;
    op.commit().await?;

    eventually(std::time::Duration::from_secs(10), || async {
        Ok(classified.lock().await.len() >= 2)
    })
    .await?;

    assert_eq!(*classified.lock().await, vec![1, 2]);

    let _ = jobs.shutdown().await;
    Ok(())
}

/// Ephemeral deliveries arrive as the shared `Arc` too, and read through it
/// unchanged.
#[tokio::test]
#[file_serial]
async fn an_ephemeral_handler_reads_through_the_arc() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let received = Arc::new(Mutex::new(Vec::new()));
    outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE)),
            EphemeralReader {
                received: received.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    jobs.start_poll().await?;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    outbox
        .publish_ephemeral(
            obix::out::EphemeralEventType::new("arc_test"),
            NonCloneEvent::Ping { owner: 1, n: 42 },
        )
        .await?;

    eventually(std::time::Duration::from_secs(10), || async {
        Ok(!received.lock().await.is_empty())
    })
    .await?;
    assert_eq!(*received.lock().await, vec![42]);

    let _ = jobs.shutdown().await;
    Ok(())
}
