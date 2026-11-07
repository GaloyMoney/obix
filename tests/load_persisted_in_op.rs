//! `Outbox::load_persisted_in_op`: a bounded read of committed persistent
//! events on the caller's own operation.

mod helpers;

use std::sync::Arc;
use std::time::Duration;

use obix::{
    EventSequence, FlushOutcome, Handled, KeyedEventCtx, KeyedSubscriber, KeyedSubscriberConfig,
    MailboxConfig, SubscriptionDef, WakeKey, out::Outbox,
};
use serde::{Deserialize, Serialize};
use serial_test::file_serial;
use tokio::sync::Mutex;

use helpers::{TestTables, init_pool, wipeout_keyed_subscriber_job_tables, wipeout_subscriptions};

const JOB_TYPE: &str = "test-load-persisted-in-op";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum TestEvent {
    Ping { owner: u64, n: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "module")]
enum ForeignEvent {
    CoreParty { id: u64 },
}

fn ping(n: u64) -> TestEvent {
    TestEvent::Ping { owner: 7, n }
}

fn seq(n: u64) -> EventSequence {
    EventSequence::from(n)
}

async fn init_outbox(pool: &sqlx::PgPool) -> anyhow::Result<Outbox<TestEvent, TestTables>> {
    helpers::wipeout_outbox_tables(pool).await?;
    Ok(Outbox::<TestEvent, TestTables>::init(pool, MailboxConfig::builder().build()).await?)
}

fn sequences(
    rows: &[Result<
        obix::out::PersistentOutboxEvent<TestEvent>,
        obix::out::UndecodableEventError,
    >],
) -> Vec<u64> {
    rows.iter()
        .map(|row| match row {
            Ok(event) => u64::from(event.sequence),
            Err(undecodable) => u64::from(undecodable.sequence),
        })
        .collect()
}

#[tokio::test]
#[file_serial]
async fn range_is_exclusive_inclusive_and_ordered() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = init_outbox(&pool).await?;

    let mut op = outbox.begin_op().await?;
    for n in 1..=5 {
        outbox.publish_persisted_in_op(&mut op, ping(n)).await?;
    }
    op.commit().await?;

    let mut op = outbox.begin_op().await?;
    let rows = outbox.load_persisted_in_op(&mut op, seq(1), seq(4)).await?;
    assert_eq!(sequences(&rows), vec![2, 3, 4]);
    let payloads: Vec<_> = rows
        .iter()
        .map(|row| row.as_ref().expect("decodable").payload.clone())
        .collect();
    assert_eq!(payloads, vec![Some(ping(2)), Some(ping(3)), Some(ping(4))]);

    let rows = outbox.load_persisted_in_op(&mut op, seq(5), seq(5)).await?;
    assert!(rows.is_empty());
    let rows = outbox.load_persisted_in_op(&mut op, seq(4), seq(2)).await?;
    assert!(rows.is_empty());

    let rows = outbox.load_persisted_in_op(&pool, seq(0), seq(5)).await?;
    assert_eq!(sequences(&rows), vec![1, 2, 3, 4, 5]);
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn reads_the_callers_snapshot() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = init_outbox(&pool).await?;

    let mut seed = outbox.begin_op().await?;
    outbox.publish_persisted_in_op(&mut seed, ping(1)).await?;
    seed.commit().await?;
    let prev = outbox.highest_known_persistent_sequence().await?;

    let mut a = pool.begin().await?;
    sqlx::query("INSERT INTO persistent_outbox_events (payload) VALUES ($1::jsonb)")
        .bind(serde_json::to_string(&ping(2))?)
        .execute(&mut *a)
        .await?;
    let in_flight = outbox.highest_known_persistent_sequence().await?;
    assert!(in_flight > prev);

    let mut b = outbox.begin_op().await?;
    let rows = outbox.load_persisted_in_op(&mut b, prev, in_flight).await?;
    assert!(rows.is_empty(), "an uncommitted event is not visible");
    b.commit().await?;

    a.commit().await?;

    let mut b = outbox.begin_op().await?;
    let rows = outbox.load_persisted_in_op(&mut b, prev, in_flight).await?;
    assert_eq!(sequences(&rows), vec![u64::from(in_flight)]);
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn undecodable_row_is_an_err_item() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = init_outbox(&pool).await?;
    let foreign =
        Outbox::<ForeignEvent, TestTables>::init(&pool, MailboxConfig::builder().build()).await?;

    let mut op = outbox.begin_op().await?;
    outbox.publish_persisted_in_op(&mut op, ping(1)).await?;
    op.commit().await?;
    let mut op = foreign.begin_op().await?;
    foreign
        .publish_persisted_in_op(&mut op, ForeignEvent::CoreParty { id: 1 })
        .await?;
    op.commit().await?;
    let mut op = outbox.begin_op().await?;
    outbox.publish_persisted_in_op(&mut op, ping(3)).await?;
    op.commit().await?;

    let mut op = outbox.begin_op().await?;
    let rows = outbox.load_persisted_in_op(&mut op, seq(0), seq(3)).await?;
    assert_eq!(rows.len(), 3);
    assert!(rows[0].is_ok());
    let undecodable = rows[1]
        .as_ref()
        .expect_err("the foreign row is the Err item");
    assert_eq!(u64::from(undecodable.sequence), 2);
    assert!(rows[2].is_ok());
    Ok(())
}

#[derive(Clone, Default)]
struct Observed {
    /// `(rows read in the flush op, items collected in the batch)` per flush.
    flushes: Arc<Mutex<Vec<(usize, usize)>>>,
}

struct Def {
    outbox: Outbox<TestEvent, TestTables>,
    observed: Observed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct MemberId(u64);

impl std::fmt::Display for MemberId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for MemberId {
    type Err = std::num::ParseIntError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(MemberId(s.parse()?))
    }
}

impl SubscriptionDef<TestEvent> for Def {
    type Key = MemberId;
    type InstanceConfig = ();
    type Subscriber = Member;

    fn wake_keys(
        &self,
        event: &obix::out::PersistentOutboxEvent<TestEvent>,
    ) -> impl IntoIterator<Item = WakeKey> {
        match &event.payload {
            Some(TestEvent::Ping { owner, .. }) => vec![WakeKey::from(owner.to_string())],
            None => vec![],
        }
    }

    fn instantiate(&self, _key: Self::Key, _cfg: Self::InstanceConfig) -> Self::Subscriber {
        Member {
            outbox: self.outbox.clone(),
            observed: self.observed.clone(),
        }
    }
}

struct Member {
    outbox: Outbox<TestEvent, TestTables>,
    observed: Observed,
}

impl KeyedSubscriber<TestEvent> for Member {
    type Batch = Vec<u64>;

    async fn handle<'inv>(
        &self,
        ctx: KeyedEventCtx<'inv, Self::Batch>,
        event: &obix::EventDelivery<TestEvent>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        let Some(TestEvent::Ping { n, .. }) = &event.payload else {
            return Ok(ctx.skip());
        };
        Ok(ctx.collect(*n))
    }

    async fn flush(
        &self,
        op: &mut obix::FlushOp<'_, obix::InsertOrder>,
        items: Self::Batch,
    ) -> Result<FlushOutcome, Box<dyn std::error::Error + Send + Sync>> {
        let through = op.position();
        let rows = self
            .outbox
            .load_persisted_in_op(&mut *op, EventSequence::BEGIN, through)
            .await?;
        self.observed
            .flushes
            .lock()
            .await
            .push((rows.len(), items.len()));
        Ok(FlushOutcome::Continue)
    }
}

#[tokio::test]
#[file_serial]
async fn works_on_a_keyed_flush_op() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = init_outbox(&pool).await?;
    wipeout_keyed_subscriber_job_tables(&pool, JOB_TYPE).await?;
    wipeout_subscriptions(&pool, JOB_TYPE).await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let observed = Observed::default();
    let subs = outbox
        .register_keyed_subscriber(
            &mut jobs,
            KeyedSubscriberConfig::new(job::JobType::new(JOB_TYPE))
                .with_linger(Duration::from_millis(150))
                .with_checkpoint_interval(Duration::from_millis(50)),
            Def {
                outbox: outbox.clone(),
                observed: observed.clone(),
            },
        )
        .await?;

    let mut op = outbox.begin_op().await?;
    subs.subscribe_in_op(&mut op, MemberId(1), (), WakeKey::from("7"))
        .await?;
    op.commit().await?;

    jobs.start_poll().await?;

    let mut op = outbox.begin_op().await?;
    for n in 1..=3 {
        outbox.publish_persisted_in_op(&mut op, ping(n)).await?;
    }
    op.commit().await?;

    let start = std::time::Instant::now();
    loop {
        let flushes = observed.flushes.lock().await.clone();
        let collected: usize = flushes.iter().map(|(_, items)| items).sum();
        if collected == 3 {
            let read: usize = flushes.last().map(|(rows, _)| *rows).unwrap();
            assert_eq!(
                read, 3,
                "the flush op read every event the member collected"
            );
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "member did not collect the events: {flushes:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(())
}
