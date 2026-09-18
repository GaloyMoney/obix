//! Lane-typed delivery: one handler API for both lanes.

mod helpers;

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering as AtomicOrd},
};
use std::time::Duration;

use futures::{StreamExt, TryStreamExt};
use obix::{
    CommitLane, CommitLaneDisabled, CommitOrder, CommitSequence, EventCtx, EventDelivery,
    EventSequence, FlushOp, Handled, InsertOrder, MailboxConfig, Ordering, OutboxEventJobConfig,
    SingletonSubscriber, StreamPosition, SubscriptionError, UndecodableDelivery, out::Outbox,
};
use serde::{Deserialize, Serialize};
use serial_test::file_serial;
use tokio::sync::{Mutex, Notify};

use helpers::{TestTables, init_pool, wipeout_outbox_job_tables, wipeout_outbox_tables};

const JOB_TYPE: &str = "test-lane-typed-delivery";
/// The two lanes' cursors count different things, so handles for both on one
/// outbox cannot share a subscription.
const INSERT_JOB_TYPE: &str = "test-lane-typed-delivery-insert";

const TEST_CHECKPOINT_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum TestEvent {
    Ping(u64),
}

async fn init_jobs(pool: &sqlx::PgPool) -> anyhow::Result<job::Jobs> {
    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    Ok(job::Jobs::init(job_config).await?)
}

fn config(commit_lane: CommitLane) -> MailboxConfig {
    MailboxConfig::builder()
        .commit_lane(commit_lane)
        .build()
        .expect("Couldn't build MailboxConfig")
}

fn config_checkpointing_every_group() -> MailboxConfig {
    MailboxConfig::builder()
        .commit_lane(CommitLane::Enabled)
        .commit_checkpoint_every(1)
        .build()
        .expect("Couldn't build MailboxConfig")
}

async fn wipe(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    wipeout_outbox_tables(pool).await?;
    wipeout_outbox_job_tables(pool, JOB_TYPE).await?;
    Ok(())
}

async fn init_outbox(
    pool: &sqlx::PgPool,
    commit_lane: CommitLane,
) -> anyhow::Result<Outbox<TestEvent, TestTables>> {
    wipe(pool).await?;
    Ok(Outbox::<TestEvent, TestTables>::init(pool, config(commit_lane)).await?)
}

/// One seeded event: its insert sequence, the transaction (group) it belongs
/// to, and its stored payload.
struct Seeded {
    sequence: i64,
    xid: i64,
    payload: serde_json::Value,
}

fn ev(sequence: i64, xid: i64, payload: u64) -> Seeded {
    Seeded {
        sequence,
        xid,
        payload: serde_json::json!({ "Ping": payload }),
    }
}

fn undecodable(sequence: i64, xid: i64) -> Seeded {
    Seeded {
        sequence,
        xid,
        payload: serde_json::json!({ "NotAVariant": true }),
    }
}

/// Write events at chosen insert sequences and groups, an interleaving the
/// write path cannot produce; the `setval` is what moves the insert frontier.
async fn seed(pool: &sqlx::PgPool, rows: &[Seeded]) -> anyhow::Result<()> {
    for row in rows {
        sqlx::query(
            "INSERT INTO persistent_outbox_events (sequence, payload, commit_xid)
             VALUES ($1, $2, $3)",
        )
        .bind(row.sequence)
        .bind(&row.payload)
        .bind(row.xid)
        .execute(pool)
        .await?;
    }
    let highest = rows.iter().map(|r| r.sequence).max().unwrap_or(0);
    sqlx::query("SELECT setval('persistent_outbox_events_sequence_seq', $1)")
        .bind(highest)
        .execute(pool)
        .await?;
    Ok(())
}

/// How many sparse checkpoints the commit-lane fold has written, and the
/// newest one's `(sequence, commit_seq)`.
async fn checkpoint_state(pool: &sqlx::PgPool) -> anyhow::Result<(i64, Option<(i64, i64)>)> {
    let count: (i64,) = sqlx::query_as("SELECT count(*) FROM persistent_outbox_commit_checkpoints")
        .fetch_one(pool)
        .await?;
    let newest: Option<(i64, i64)> = sqlx::query_as(
        "SELECT sequence, commit_seq FROM persistent_outbox_commit_checkpoints
         ORDER BY sequence DESC LIMIT 1",
    )
    .fetch_optional(pool)
    .await?;
    Ok((count.0, newest))
}

async fn publish_group(
    outbox: &Outbox<TestEvent, TestTables>,
    payloads: impl IntoIterator<Item = u64>,
) -> anyhow::Result<()> {
    let mut op = outbox.begin_op().await?;
    for n in payloads {
        outbox
            .publish_persisted_in_op(&mut op, TestEvent::Ping(n))
            .await?;
    }
    op.commit().await?;
    Ok(())
}

async fn until<F>(mut f: F, what: &str) -> anyhow::Result<()>
where
    F: AsyncFnMut() -> bool,
{
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while std::time::Instant::now() < deadline {
        if f().await {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    anyhow::bail!("timed out waiting for {what}")
}

struct InsertPositionRecorder {
    seen: Arc<Mutex<Vec<(EventSequence, EventSequence)>>>,
    last_handled: Arc<Mutex<Option<EventSequence>>>,
    flushes: Arc<Mutex<Vec<(EventSequence, Option<EventSequence>, Vec<u64>)>>>,
}

impl SingletonSubscriber<TestEvent> for InsertPositionRecorder {
    type Batch = Vec<u64>;

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, Vec<u64>>,
        event: &EventDelivery<TestEvent>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        self.seen
            .lock()
            .await
            .push((event.position(), event.sequence));
        *self.last_handled.lock().await = Some(event.position());
        match &event.payload {
            Some(TestEvent::Ping(n)) if *n == 1 => {
                let n = *n;
                Ok(ctx.collect_with(move |batch| batch.push(n)))
            }
            _ => Ok(ctx.skip()),
        }
    }

    async fn flush(
        &self,
        op: &mut FlushOp<'_, InsertOrder>,
        items: Vec<u64>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let last = *self.last_handled.lock().await;
        self.flushes.lock().await.push((op.position(), last, items));
        Ok(())
    }
}

#[tokio::test]
#[file_serial]
async fn insert_lane_position_is_the_sequence() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool, CommitLane::Disabled).await?;

    let seen = Arc::new(Mutex::new(Vec::new()));
    let flushes = Arc::new(Mutex::new(Vec::new()));
    outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
                .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL),
            InsertPositionRecorder {
                seen: seen.clone(),
                last_handled: Arc::new(Mutex::new(None)),
                flushes: flushes.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    jobs.start_poll().await?;

    // One source transaction: the insert lane promises no group atomicity, so
    // a batch may only span events the runner finds already buffered — which a
    // single commit's events are, and two separate commits' never are.
    publish_group(&outbox, [1, 2, 3]).await?;

    until(
        async || seen.lock().await.len() >= 3,
        "all three events delivered",
    )
    .await?;
    until(
        async || !flushes.lock().await.is_empty(),
        "the batch landed",
    )
    .await?;

    for (position, sequence) in seen.lock().await.iter() {
        assert_eq!(
            position, sequence,
            "on the insert lane position() is the event's sequence",
        );
    }

    let flushes = flushes.lock().await.clone();
    for (position, last_handled, _) in &flushes {
        assert_eq!(
            Some(*position),
            *last_handled,
            "FlushOp::position() must be the last fully handled event, skips included",
        );
    }
    let discriminating = flushes
        .iter()
        .find(|(_, _, items)| items.contains(&1))
        .expect("the flush carrying Ping(1) must have been recorded");
    assert_eq!(
        discriminating.0,
        EventSequence::from(3u64),
        "the batch ended on two skipped events, so its watermark is 3 — a `max` over the \
         collected rows would understate it as 1",
    );

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Recorded {
    payload: u64,
    position: CommitSequence,
    boundary: bool,
}

struct CommitPositionRecorder {
    recorded: Arc<Mutex<Vec<Recorded>>>,
}

impl SingletonSubscriber<TestEvent, CommitOrder> for CommitPositionRecorder {
    type Batch = ();

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, ()>,
        event: &EventDelivery<TestEvent, CommitOrder>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(TestEvent::Ping(n)) = &event.payload {
            self.recorded.lock().await.push(Recorded {
                payload: *n,
                position: event.position(),
                boundary: event.is_commit_boundary(),
            });
        }
        Ok(ctx.skip())
    }
}

/// Three groups round-robin over insert sequences: A at 1/4/7, B at 2/5/8, C
/// at 3/6/9 — so insert order reads A,B,C,A,B,C,A,B,C.
fn round_robin_groups() -> Vec<Seeded> {
    let mut rows = Vec::new();
    for (idx, payloads) in [[11u64, 12, 13], [21, 22, 23], [31, 32, 33]]
        .into_iter()
        .enumerate()
    {
        for (round, payload) in payloads.into_iter().enumerate() {
            rows.push(ev((round * 3 + idx + 1) as i64, 8001 + idx as i64, payload));
        }
    }
    rows
}

#[tokio::test]
#[file_serial]
async fn commit_lane_position_is_dense_and_groups_are_contiguous() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;

    wipe(&pool).await?;
    seed(&pool, &round_robin_groups()).await?;
    let outbox = Outbox::<TestEvent, TestTables>::init(&pool, config(CommitLane::Enabled)).await?;

    let recorded = Arc::new(Mutex::new(Vec::new()));
    outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
                .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL),
            CommitPositionRecorder {
                recorded: recorded.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    jobs.start_poll().await?;

    until(
        async || recorded.lock().await.len() >= 9,
        "all nine seeded events delivered on the commit lane",
    )
    .await?;

    let recorded = recorded.lock().await.clone();
    assert_eq!(
        recorded
            .iter()
            .map(|r| u64::from(r.position))
            .collect::<Vec<_>>(),
        (1..=9).collect::<Vec<u64>>(),
        "commit positions must be dense and ascending, with no gap or repeat",
    );
    assert_eq!(
        recorded.iter().map(|r| r.payload).collect::<Vec<_>>(),
        vec![11, 12, 13, 21, 22, 23, 31, 32, 33],
        "each group's members must arrive contiguously, in first-sight order",
    );
    assert_eq!(
        recorded.iter().map(|r| r.boundary).collect::<Vec<_>>(),
        vec![false, false, true, false, false, true, false, false, true],
        "exactly the last member of each group is a commit boundary",
    );

    Ok(())
}

struct GroupFlusher {
    seen: Arc<Mutex<Vec<(u64, CommitSequence, bool)>>>,
    flushes: Arc<Mutex<Vec<(CommitSequence, Vec<u64>)>>>,
}

impl SingletonSubscriber<TestEvent, CommitOrder> for GroupFlusher {
    type Batch = Vec<u64>;

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, Vec<u64>>,
        event: &EventDelivery<TestEvent, CommitOrder>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        let Some(TestEvent::Ping(n)) = &event.payload else {
            return Ok(ctx.skip());
        };
        let n = *n;
        self.seen
            .lock()
            .await
            .push((n, event.position(), event.is_commit_boundary()));
        if event.is_commit_boundary() {
            return Ok(ctx.skip());
        }
        Ok(ctx.collect_with(move |batch| batch.push(n)))
    }

    async fn flush(
        &self,
        op: &mut FlushOp<'_, CommitOrder>,
        items: Vec<u64>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if !items.is_empty() {
            self.flushes.lock().await.push((op.position(), items));
        }
        Ok(())
    }
}

#[tokio::test]
#[file_serial]
async fn commit_lane_flush_position_is_the_boundary_not_the_max() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;

    wipe(&pool).await?;
    seed(&pool, &[ev(1, 9101, 11), ev(2, 9101, 12), ev(3, 9101, 13)]).await?;
    let outbox = Outbox::<TestEvent, TestTables>::init(&pool, config(CommitLane::Enabled)).await?;

    let seen = Arc::new(Mutex::new(Vec::new()));
    let flushes = Arc::new(Mutex::new(Vec::new()));
    outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
                .with_max_batch_size(2)
                .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL),
            GroupFlusher {
                seen: seen.clone(),
                flushes: flushes.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    jobs.start_poll().await?;

    until(
        async || !flushes.lock().await.is_empty(),
        "the group's batch landed",
    )
    .await?;

    let seen = seen.lock().await.clone();
    assert_eq!(seen.len(), 3, "the whole group was delivered: {seen:?}");
    let boundary = seen
        .iter()
        .find(|(_, _, boundary)| *boundary)
        .expect("the group has a boundary member");
    assert_eq!(
        boundary.1,
        CommitSequence::from(3u64),
        "the group's last member closes it",
    );

    let flushes = flushes.lock().await.clone();
    assert_eq!(
        flushes,
        vec![(CommitSequence::from(3u64), vec![11, 12])],
        "the flush must land at the boundary's position — `max` over the collected rows would \
         report 2, understating the watermark by the skipped boundary member",
    );

    Ok(())
}

struct UndecodableRecorder<L> {
    seen: Arc<Mutex<Vec<u64>>>,
    undecodable_at: Arc<Mutex<Vec<StreamPosition>>>,
    _lane: std::marker::PhantomData<fn() -> L>,
}

impl<L> UndecodableRecorder<L> {
    fn new(seen: Arc<Mutex<Vec<u64>>>, undecodable_at: Arc<Mutex<Vec<StreamPosition>>>) -> Self {
        Self {
            seen,
            undecodable_at,
            _lane: std::marker::PhantomData,
        }
    }
}

impl SingletonSubscriber<TestEvent, CommitOrder> for UndecodableRecorder<CommitOrder> {
    type Batch = ();

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, ()>,
        event: &EventDelivery<TestEvent, CommitOrder>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(TestEvent::Ping(n)) = &event.payload {
            self.seen.lock().await.push(*n);
        }
        Ok(ctx.skip())
    }

    async fn handle_undecodable(
        &self,
        error: &UndecodableDelivery<CommitOrder>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.undecodable_at
            .lock()
            .await
            .push(error.position().into());
        Ok(())
    }
}

impl SingletonSubscriber<TestEvent> for UndecodableRecorder<InsertOrder> {
    type Batch = ();

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, ()>,
        event: &EventDelivery<TestEvent>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(TestEvent::Ping(n)) = &event.payload {
            self.seen.lock().await.push(*n);
        }
        Ok(ctx.skip())
    }

    async fn handle_undecodable(
        &self,
        error: &UndecodableDelivery,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        assert_eq!(
            error.position(),
            error.sequence,
            "on the insert lane an undecodable delivery's position is its sequence",
        );
        self.undecodable_at
            .lock()
            .await
            .push(error.position().into());
        Ok(())
    }
}

#[tokio::test]
#[file_serial]
async fn undecodable_delivery_carries_the_lane_position() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    // Group X holds sequences 1 and 4, group Y 2 and 3, so commit order is
    // [1, 4, 2, 3] and the undecodable row at sequence 2 sits in commit slot 3.
    let rows = || {
        vec![
            ev(1, 9201, 11),
            undecodable(2, 9202),
            ev(3, 9202, 21),
            ev(4, 9201, 12),
        ]
    };

    // --- commit lane ---
    let mut jobs = init_jobs(&pool).await?;
    wipe(&pool).await?;
    seed(&pool, &rows()).await?;
    let outbox = Outbox::<TestEvent, TestTables>::init(&pool, config(CommitLane::Enabled)).await?;

    let seen = Arc::new(Mutex::new(Vec::new()));
    let at = Arc::new(Mutex::new(Vec::new()));
    let subscription = outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
                .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL),
            UndecodableRecorder::<CommitOrder>::new(seen.clone(), at.clone()),
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    jobs.start_poll().await?;

    until(
        async || seen.lock().await.len() >= 3,
        "every decodable member delivered",
    )
    .await?;
    assert_eq!(
        *at.lock().await,
        vec![StreamPosition::Commit(CommitSequence::from(3u64))],
        "the undecodable row sits at commit slot 3 — its insert sequence is 2, so reporting \
         that would be reporting the other lane's number",
    );
    until(
        async || {
            subscription
                .load()
                .await
                .map(|s| s.checkpoint() >= CommitSequence::from(4u64))
                .unwrap_or(false)
        },
        "the commit cursor advanced past every delivery",
    )
    .await?;
    let _ = jobs.shutdown().await;
    drop(jobs);
    drop(outbox);

    // Both cursors advanced, so a fresh run does not see it again.
    let mut jobs = init_jobs(&pool).await?;
    let outbox = Outbox::<TestEvent, TestTables>::init(&pool, config(CommitLane::Enabled)).await?;
    let seen_again = Arc::new(Mutex::new(Vec::new()));
    let at_again = Arc::new(Mutex::new(Vec::new()));
    outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
                .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL),
            UndecodableRecorder::<CommitOrder>::new(seen_again.clone(), at_again.clone()),
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    jobs.start_poll().await?;
    publish_group(&outbox, [99]).await?;
    until(
        async || seen_again.lock().await.contains(&99),
        "the restarted run reached the new event",
    )
    .await?;
    assert!(
        at_again.lock().await.is_empty(),
        "an acknowledged undecodable event must not be redelivered after a restart",
    );
    let _ = jobs.shutdown().await;
    drop(jobs);
    drop(outbox);

    // --- insert lane: the same row, reported as its sequence ---
    let mut jobs = init_jobs(&pool).await?;
    wipe(&pool).await?;
    seed(&pool, &rows()).await?;
    let outbox = Outbox::<TestEvent, TestTables>::init(&pool, config(CommitLane::Disabled)).await?;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let at = Arc::new(Mutex::new(Vec::new()));
    outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
                .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL),
            UndecodableRecorder::<InsertOrder>::new(seen.clone(), at.clone()),
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    jobs.start_poll().await?;
    until(
        async || seen.lock().await.len() >= 3,
        "every decodable member delivered on the insert lane",
    )
    .await?;
    assert_eq!(
        *at.lock().await,
        vec![StreamPosition::Insert(EventSequence::from(2u64))],
        "on the insert lane the undecodable delivery reports its sequence",
    );

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn raw_listener_try_next_fails_with_position() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipe(&pool).await?;
    // The undecodable row's commit slot (3) and insert sequence (2) differ.
    seed(
        &pool,
        &[
            ev(1, 9301, 11),
            undecodable(2, 9302),
            ev(3, 9302, 21),
            ev(4, 9301, 12),
        ],
    )
    .await?;
    let outbox = Outbox::<TestEvent, TestTables>::init(&pool, config(CommitLane::Enabled)).await?;

    let mut commit = outbox.listen_commit_ordered(CommitSequence::BEGIN)?;
    for expected in [1u64, 2] {
        let item = commit
            .try_next()
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .expect("a commit-lane item");
        assert_eq!(item.position(), CommitSequence::from(expected));
    }
    let failure = commit
        .try_next()
        .await
        .expect_err("an undecodable payload must fail try_next");
    assert_eq!(
        failure.position(),
        CommitSequence::from(3u64),
        "the commit-lane error carries the slot it occupied, not the row's sequence",
    );

    let mut insert = outbox.listen_persisted(EventSequence::BEGIN);
    let first = insert
        .try_next()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    assert_eq!(
        first.expect("first insert-lane item").position(),
        EventSequence::from(1u64),
    );
    let failure = insert
        .try_next()
        .await
        .expect_err("an undecodable payload must fail try_next");
    assert_eq!(failure.position(), EventSequence::from(2u64));

    Ok(())
}

/// The `commit_at`'th delivery commits in its own op: without a durably
/// persisted commit checkpoint the fence silently takes the insert path.
struct CommitThenBlock {
    commit_at: usize,
    block_at: usize,
    blocked: Arc<AtomicBool>,
    release: Arc<Notify>,
    received: Arc<Mutex<Vec<u64>>>,
}

impl SingletonSubscriber<TestEvent, CommitOrder> for CommitThenBlock {
    type Batch = ();

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, ()>,
        event: &EventDelivery<TestEvent, CommitOrder>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        let handled = {
            let mut received = self.received.lock().await;
            if let Some(TestEvent::Ping(n)) = &event.payload {
                received.push(*n);
            }
            received.len()
        };
        if handled == self.block_at && !self.blocked.swap(true, AtomicOrd::SeqCst) {
            self.release.notified().await;
            return Ok(ctx.skip());
        }
        if handled == self.commit_at {
            return Ok(ctx.consume().await?.commit());
        }
        Ok(ctx.skip())
    }
}

/// Group A holds insert sequences 1 and 4, group B 2 and 3: after two
/// deliveries the insert cursor is at the frontier, the commit cursor 2 of 4.
#[tokio::test]
#[file_serial]
async fn caught_up_fence_on_the_commit_lane_does_not_return_early() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;

    wipe(&pool).await?;
    seed(
        &pool,
        &[
            ev(1, 9001, 101),
            ev(2, 9002, 201),
            ev(3, 9002, 202),
            ev(4, 9001, 102),
        ],
    )
    .await?;
    let outbox = Outbox::<TestEvent, TestTables>::init(&pool, config(CommitLane::Enabled)).await?;

    let blocked = Arc::new(AtomicBool::new(false));
    let release = Arc::new(Notify::new());
    let received = Arc::new(Mutex::new(Vec::new()));

    let subscription = outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
                .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL),
            CommitThenBlock {
                commit_at: 2,
                block_at: 3,
                blocked: blocked.clone(),
                release: release.clone(),
                received: received.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    jobs.start_poll().await?;

    until(
        async || blocked.load(AtomicOrd::SeqCst),
        "handler blocked on the third delivery",
    )
    .await?;
    assert_eq!(
        *received.lock().await,
        vec![101, 102, 201],
        "commit order must deliver group A (sequences 1 and 4) first, contiguously",
    );

    let snapshot = subscription.load().await?;
    assert_eq!(
        snapshot.ordering(),
        Ordering::Commit,
        "the fence under test is the commit-lane one, which needs a committed commit cursor",
    );
    assert_eq!(
        snapshot.checkpoint(),
        CommitSequence::from(2u64),
        "precondition: the commit cursor is only 2 of 4",
    );
    assert_eq!(
        outbox.highest_known_persistent_sequence().await?,
        EventSequence::from(4u64),
        "precondition: the insert frontier is 4 — the same value the insert cursor now holds, \
         which is exactly what an insert-cursor fence would wrongly accept as caught up",
    );

    let mut barrier = tokio::spawn({
        let subscription = subscription.clone();
        async move { subscription.await_caught_up(Duration::from_secs(20)).await }
    });

    assert!(
        tokio::time::timeout(Duration::from_millis(500), &mut barrier)
            .await
            .is_err(),
        "await_caught_up returned while commit positions 3 and 4 were still undelivered",
    );

    release.notify_one();
    barrier
        .await?
        .map_err(|e| anyhow::anyhow!("await_caught_up: {e}"))?;

    assert_eq!(
        *received.lock().await,
        vec![101, 102, 201, 202],
        "every event must be delivered, in commit order",
    );

    Ok(())
}

struct Observer {
    received: Arc<Mutex<Vec<u64>>>,
}

impl SingletonSubscriber<TestEvent, CommitOrder> for Observer {
    type Batch = ();

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, ()>,
        event: &EventDelivery<TestEvent, CommitOrder>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(TestEvent::Ping(n)) = &event.payload {
            self.received.lock().await.push(*n);
        }
        Ok(ctx.skip())
    }
}

#[tokio::test]
#[file_serial]
async fn caught_up_fence_is_not_wedged_by_an_aborted_tail() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    wipe(&pool).await?;
    let outbox = Outbox::<TestEvent, TestTables>::init(
        &pool,
        MailboxConfig::builder()
            .commit_lane(CommitLane::Enabled)
            .gap_fill_grace(Duration::from_millis(100))
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let received = Arc::new(Mutex::new(Vec::new()));
    let subscription = outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
                .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL),
            Observer {
                received: received.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    jobs.start_poll().await?;

    publish_group(&outbox, [1, 2]).await?;
    until(
        async || received.lock().await.len() >= 2,
        "the two real events delivered",
    )
    .await?;
    // Without this the test can race onto the insert-lane fence and prove
    // nothing: that one is taken until a commit cursor is checkpointed.
    until(
        async || {
            subscription
                .load()
                .await
                .map(|s| s.ordering() == Ordering::Commit)
                .unwrap_or(false)
        },
        "the subscription checkpoints on the commit lane",
    )
    .await?;

    // Raw SQL: the INSERT happens in the op's pre-commit, so a dropped op
    // never burns a sequence at all.
    let before_abort = outbox.highest_known_persistent_sequence().await?;
    let mut aborted = pool.begin().await?;
    sqlx::query("INSERT INTO persistent_outbox_events (payload) VALUES ($1::jsonb)")
        .bind(r#"{"Ping": 999}"#)
        .execute(&mut *aborted)
        .await?;
    aborted.rollback().await?;

    let after_abort = outbox.highest_known_persistent_sequence().await?;
    assert!(
        after_abort > before_abort,
        "precondition: the aborted transaction must have raised the insert head: \
         {before_abort} -> {after_abort}",
    );

    subscription
        .await_caught_up(Duration::from_secs(15))
        .await
        .map_err(|e| {
            anyhow::anyhow!("await_caught_up must not be wedged by an aborted tail: {e}")
        })?;

    Ok(())
}

struct Skipper;
impl SingletonSubscriber<TestEvent, CommitOrder> for Skipper {
    type Batch = ();
}
struct InsertSkipper;
impl SingletonSubscriber<TestEvent> for InsertSkipper {
    type Batch = ();
}

#[tokio::test]
#[file_serial]
async fn snapshot_reports_lane_typed_positions() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;

    // Seeded at 11-13 so the two lanes' frontiers are different numbers
    // (insert 13, commit 3).
    wipe(&pool).await?;
    seed(&pool, &[ev(11, 9401, 1), ev(12, 9402, 2), ev(13, 9403, 3)]).await?;
    let outbox = Outbox::<TestEvent, TestTables>::init(&pool, config(CommitLane::Enabled)).await?;

    let subscription = outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
                .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL),
            Skipper,
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    jobs.start_poll().await?;

    // `is_caught_up()` alone is trivially true before anything is sequenced.
    let expected = CommitSequence::from(3u64);
    until(
        async || {
            subscription
                .load()
                .await
                .map(|s| s.checkpoint() == expected)
                .unwrap_or(false)
        },
        "the commit-lane subscription reaches position 3",
    )
    .await?;

    assert_eq!(
        outbox.frontier::<CommitOrder>().await?,
        expected,
        "the outbox's commit-lane frontier agrees with what the subscription reports",
    );
    let insert_frontier = outbox.highest_known_persistent_sequence().await?;
    assert_eq!(
        insert_frontier,
        EventSequence::from(13u64),
        "precondition: the insert frontier is a different number from the commit head",
    );
    let snapshot = subscription.load().await?;
    assert_eq!(snapshot.ordering(), Ordering::Commit);
    assert_eq!(
        snapshot.frontier(),
        expected,
        "the commit-lane frontier is the fold's head, not the sequence generator's last value",
    );
    assert_eq!(snapshot.checkpoint(), expected);
    assert_eq!(snapshot.lag(), 0);
    assert!(snapshot.is_caught_up());
    assert_eq!(
        snapshot.stream_status().frontier,
        StreamPosition::Commit(expected),
        "the dynamic read-out names the lane",
    );

    let mut insert_jobs = init_jobs(&pool).await?;
    wipeout_outbox_job_tables(&pool, INSERT_JOB_TYPE).await?;
    let insert = outbox
        .register_singleton_subscriber(
            &mut insert_jobs,
            OutboxEventJobConfig::new(job::JobType::new(INSERT_JOB_TYPE))
                .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL),
            InsertSkipper,
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let snapshot = insert.load().await?;
    assert_eq!(snapshot.ordering(), Ordering::Insert);
    assert_eq!(
        snapshot.frontier(),
        outbox.highest_known_persistent_sequence().await?,
        "the insert lane's frontier is the sequence generator's last value",
    );

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn registration_infers_the_lane_and_refuses_a_switch() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool, CommitLane::Enabled).await?;

    let insert = outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
                .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL),
            InsertSkipper,
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    jobs.start_poll().await?;

    publish_group(&outbox, [1, 2, 3]).await?;
    until(
        async || {
            insert
                .load()
                .await
                .map(|s| s.checkpoint() > EventSequence::BEGIN)
                .unwrap_or(false)
        },
        "the insert-lane subscription checkpoints",
    )
    .await?;

    let mut other_jobs = init_jobs(&pool).await?;
    let commit = outbox
        .register_singleton_subscriber(
            &mut other_jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE)),
            Skipper,
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    match commit.load().await {
        Err(SubscriptionError::LaneMismatch(message)) => {
            assert!(
                message.contains("register a new job type"),
                "the refusal must say what to do instead: {message}",
            );
        }
        other => anyhow::bail!("expected LaneMismatch, got {other:?}", other = other.err()),
    }

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn await_position_on_each_lane() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool, CommitLane::Enabled).await?;

    let commit = outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE)),
            Skipper,
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let target = CommitSequence::from(999u64);
    match commit.await_position(target, Duration::ZERO).await {
        Err(SubscriptionError::CaughtUpTimeout {
            checkpoint,
            target: reported,
            ..
        }) => {
            assert_eq!(reported, StreamPosition::Commit(target));
            assert!(matches!(checkpoint, StreamPosition::Commit(_)));
        }
        other => anyhow::bail!("expected CaughtUpTimeout, got {other:?}"),
    }

    let mut insert_jobs = init_jobs(&pool).await?;
    wipeout_outbox_job_tables(&pool, INSERT_JOB_TYPE).await?;
    let insert = outbox
        .register_singleton_subscriber(
            &mut insert_jobs,
            OutboxEventJobConfig::new(job::JobType::new(INSERT_JOB_TYPE)),
            InsertSkipper,
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let target = EventSequence::from(999u64);
    match insert.await_position(target, Duration::ZERO).await {
        Err(SubscriptionError::CaughtUpTimeout {
            checkpoint,
            target: reported,
            ..
        }) => {
            assert_eq!(reported, StreamPosition::Insert(target));
            assert!(matches!(checkpoint, StreamPosition::Insert(_)));
        }
        other => anyhow::bail!("expected CaughtUpTimeout, got {other:?}"),
    }

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn disabled_lane_refuses_commit_order_at_registration() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool, CommitLane::Disabled).await?;

    assert_eq!(
        outbox.listen_commit_ordered(CommitSequence::BEGIN).err(),
        Some(CommitLaneDisabled),
        "the raw listener refuses",
    );

    let refused = outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE)),
            Skipper,
        )
        .await;
    let error = refused.err().expect("registration must be refused");
    assert!(
        error.downcast_ref::<CommitLaneDisabled>().is_some(),
        "registration must fail with CommitLaneDisabled, got: {error}",
    );
    let jobs_rows: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM jobs WHERE job_type = $1")
        .bind(JOB_TYPE)
        .fetch_one(&pool)
        .await?;
    assert_eq!(
        jobs_rows.0, 0,
        "the refusal must come before the job is spawned",
    );

    // The insert lane is untouched, and no sequencer runs.
    let received = Arc::new(Mutex::new(Vec::new()));
    struct Recorder {
        received: Arc<Mutex<Vec<u64>>>,
    }
    impl SingletonSubscriber<TestEvent> for Recorder {
        type Batch = ();
        async fn handle_persistent<'inv>(
            &self,
            ctx: EventCtx<'inv, ()>,
            event: &EventDelivery<TestEvent>,
        ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
            if let Some(TestEvent::Ping(n)) = &event.payload {
                self.received.lock().await.push(*n);
            }
            Ok(ctx.skip())
        }
    }
    wipeout_outbox_job_tables(&pool, INSERT_JOB_TYPE).await?;
    outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(INSERT_JOB_TYPE))
                .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL),
            Recorder {
                received: received.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    jobs.start_poll().await?;

    publish_group(&outbox, [1, 2, 3]).await?;
    until(
        async || received.lock().await.len() >= 3,
        "the insert lane still delivers with the commit lane off",
    )
    .await?;

    assert_eq!(
        checkpoint_state(&pool).await?.0,
        0,
        "no sequencer ran: nothing folded, so nothing checkpointed",
    );

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn enabling_the_lane_later_sequences_the_full_history_in_the_same_order() -> anyhow::Result<()>
{
    let pool = init_pool().await?;

    // Phase 1 — lane off; the write path still stamps `commit_group`.
    {
        let outbox = init_outbox(&pool, CommitLane::Disabled).await?;
        publish_group(&outbox, [1, 2]).await?;
    }
    seed(
        &pool,
        &[
            ev(3, 7001, 11),
            ev(4, 7002, 21),
            ev(5, 7003, 31),
            ev(6, 7001, 12),
            ev(7, 7002, 22),
            ev(8, 7003, 32),
        ],
    )
    .await?;
    assert_eq!(
        checkpoint_state(&pool).await?.0,
        0,
        "precondition: nothing was sequenced while the lane was disabled — without this the \
         test is merely the always-on case again",
    );

    // Phase 2 — enable it: with no checkpoint to seed from, the fold starts at
    // the beginning of the stream.
    let mut jobs = init_jobs(&pool).await?;
    let outbox = Outbox::<TestEvent, TestTables>::init(&pool, config(CommitLane::Enabled)).await?;
    let recorded = Arc::new(Mutex::new(Vec::new()));
    outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
                .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL),
            CommitPositionRecorder {
                recorded: recorded.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    jobs.start_poll().await?;

    until(
        async || recorded.lock().await.len() >= 8,
        "the full history was sequenced and delivered",
    )
    .await?;

    let history = recorded.lock().await.clone();
    assert_eq!(
        history
            .iter()
            .map(|r| u64::from(r.position))
            .collect::<Vec<_>>(),
        (1..=8).collect::<Vec<u64>>(),
        "the replayed log is dense from 1",
    );
    assert_eq!(
        history.iter().map(|r| r.payload).collect::<Vec<_>>(),
        vec![1, 2, 11, 12, 21, 22, 31, 32],
        "a late enable must produce the order a live sequencer would have: groups placed at \
         first sight of their lowest member, each pulled contiguous",
    );
    assert_eq!(
        history.iter().map(|r| r.boundary).collect::<Vec<_>>(),
        vec![false, true, false, true, false, true, false, true],
    );

    publish_group(&outbox, [41, 42]).await?;
    until(
        async || recorded.lock().await.len() >= 10,
        "live events continue the lane",
    )
    .await?;
    let all = recorded.lock().await.clone();
    assert_eq!(
        all[8..]
            .iter()
            .map(|r| u64::from(r.position))
            .collect::<Vec<_>>(),
        vec![9, 10],
        "live groups continue from the head the backfill reached",
    );

    Ok(())
}

struct Consumer {
    received: Arc<Mutex<Vec<u64>>>,
}

impl SingletonSubscriber<TestEvent, CommitOrder> for Consumer {
    type Batch = ();

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, ()>,
        event: &EventDelivery<TestEvent, CommitOrder>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(TestEvent::Ping(n)) = &event.payload {
            self.received.lock().await.push(*n);
        }
        // Its own op per event, so the checkpoint is durable immediately.
        Ok(ctx.consume().await?.commit())
    }
}

#[tokio::test]
#[file_serial]
async fn toggling_off_then_on_resumes_from_stored_state() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    // Phase 1 — lane on: three groups, checkpointed at commit position 3.
    {
        let mut jobs = init_jobs(&pool).await?;
        wipe(&pool).await?;
        let outbox =
            Outbox::<TestEvent, TestTables>::init(&pool, config_checkpointing_every_group())
                .await?;
        let received = Arc::new(Mutex::new(Vec::new()));
        let subscription = outbox
            .register_singleton_subscriber(
                &mut jobs,
                OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
                    .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL),
                Consumer {
                    received: received.clone(),
                },
            )
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        jobs.start_poll().await?;

        for n in 1..=3u64 {
            publish_group(&outbox, [n]).await?;
        }
        until(
            async || {
                subscription
                    .load()
                    .await
                    .map(|s| s.checkpoint() == CommitSequence::from(3u64))
                    .unwrap_or(false)
            },
            "the subscriber checkpoints at commit position 3",
        )
        .await?;
        assert_eq!(*received.lock().await, vec![1, 2, 3]);
        let _ = jobs.shutdown().await;
    }

    // Phase 2 — lane off: three more groups reach the table, nothing folds.
    seed(&pool, &[ev(4, 7101, 4), ev(5, 7102, 5), ev(6, 7103, 6)]).await?;
    assert_eq!(
        checkpoint_state(&pool)
            .await?
            .1
            .map(|(_, commit_seq)| commit_seq),
        Some(3),
        "precondition: with the lane off nothing folds, so the newest checkpoint stays where \
         phase 1 left it",
    );

    // Phase 3 — lane on again.
    let mut jobs = init_jobs(&pool).await?;
    let outbox = Outbox::<TestEvent, TestTables>::init(&pool, config(CommitLane::Enabled)).await?;
    let received = Arc::new(Mutex::new(Vec::new()));
    let subscription = outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
                .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL),
            Consumer {
                received: received.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    jobs.start_poll().await?;

    until(
        async || received.lock().await.len() >= 3,
        "the three groups written while the lane was off are delivered",
    )
    .await?;
    assert_eq!(
        *received.lock().await,
        vec![4, 5, 6],
        "exactly the new events, and nothing already acknowledged",
    );
    assert_eq!(
        subscription.load().await?.checkpoint(),
        CommitSequence::from(6u64),
        "the log continued from 3 rather than restarting",
    );

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn lagged_listener_recovers_without_a_new_broadcast() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipe(&pool).await?;
    // A four-deep broadcast against twenty events: the listener cannot stay
    // inside the window.
    let outbox = Outbox::<TestEvent, TestTables>::init(
        &pool,
        MailboxConfig::builder()
            .event_buffer_size(4)
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let mut listener = outbox.listen_persisted(EventSequence::BEGIN);

    for n in 1..=20u64 {
        publish_group(&outbox, [n]).await?;
    }

    // Nothing is published from here on: everything below must come from a
    // backfill.
    let mut received = Vec::new();
    while received.len() < 20 {
        let item = tokio::time::timeout(Duration::from_secs(20), listener.try_next())
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "a lagged listener stalled after {} of 20 events, with no further \
                     publish to wake it",
                    received.len()
                )
            })?
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .expect("the stream stays open");
        received.push(u64::from(item.position()));
    }
    assert_eq!(
        received,
        (1..=20).collect::<Vec<u64>>(),
        "every event, in order, after lagging out of the window",
    );

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn both_lanes_share_one_listener_state_machine() -> anyhow::Result<()> {
    async fn first_position<L: obix::Lane>(
        listener: &mut obix::out::LaneListener<L, TestEvent>,
    ) -> anyhow::Result<L::Position> {
        let item = listener
            .try_next()
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .expect("an item");
        Ok(item.position())
    }

    let pool = init_pool().await?;
    wipe(&pool).await?;
    seed(&pool, &[ev(1, 9501, 11), ev(2, 9501, 12)]).await?;
    let outbox = Outbox::<TestEvent, TestTables>::init(&pool, config(CommitLane::Enabled)).await?;

    let mut insert = outbox.listen_persisted(EventSequence::BEGIN);
    let mut commit = outbox.listen_commit_ordered(CommitSequence::BEGIN)?;

    assert_eq!(
        first_position(&mut insert).await?,
        EventSequence::from(1u64),
    );
    assert_eq!(
        first_position(&mut commit).await?,
        CommitSequence::from(1u64),
    );

    Ok(())
}

struct BoundaryFlusher<L> {
    flushes: Arc<Mutex<Vec<Vec<u64>>>>,
    _lane: std::marker::PhantomData<fn() -> L>,
}

impl<L> BoundaryFlusher<L> {
    fn new(flushes: Arc<Mutex<Vec<Vec<u64>>>>) -> Self {
        Self {
            flushes,
            _lane: std::marker::PhantomData,
        }
    }
}

impl SingletonSubscriber<TestEvent> for BoundaryFlusher<InsertOrder> {
    type Batch = Vec<u64>;

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, Vec<u64>>,
        event: &EventDelivery<TestEvent>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        match &event.payload {
            Some(TestEvent::Ping(n)) => {
                let n = *n;
                Ok(ctx.collect_with(move |batch| batch.push(n)))
            }
            None => Ok(ctx.skip()),
        }
    }

    async fn flush(
        &self,
        _op: &mut FlushOp<'_, InsertOrder>,
        items: Vec<u64>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.flushes.lock().await.push(items);
        Ok(())
    }
}

impl SingletonSubscriber<TestEvent, CommitOrder> for BoundaryFlusher<CommitOrder> {
    type Batch = Vec<u64>;

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, Vec<u64>>,
        event: &EventDelivery<TestEvent, CommitOrder>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        match &event.payload {
            Some(TestEvent::Ping(n)) => {
                let n = *n;
                Ok(ctx.collect_with(move |batch| batch.push(n)))
            }
            None => Ok(ctx.skip()),
        }
    }

    async fn flush(
        &self,
        _op: &mut FlushOp<'_, CommitOrder>,
        items: Vec<u64>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.flushes.lock().await.push(items);
        Ok(())
    }
}

#[tokio::test]
#[file_serial]
async fn insert_lane_delivery_is_always_a_boundary() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    // Insert lane: three events in one transaction, one flush each.
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool, CommitLane::Disabled).await?;
    let flushes = Arc::new(Mutex::new(Vec::new()));
    outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
                .with_max_batch_size(1)
                .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL),
            BoundaryFlusher::<InsertOrder>::new(flushes.clone()),
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    jobs.start_poll().await?;

    publish_group(&outbox, [1, 2, 3]).await?;
    until(
        async || flushes.lock().await.iter().flatten().count() >= 3,
        "all three insert-lane events flushed",
    )
    .await?;
    assert_eq!(
        *flushes.lock().await,
        vec![vec![1], vec![2], vec![3]],
        "every insert-lane delivery is a legal flush point, so `max_batch_size = 1` lands one \
         event per flush",
    );
    let _ = jobs.shutdown().await;
    drop(jobs);
    drop(outbox);

    // Commit lane: one group of three, held open past the soft limit.
    let mut jobs = init_jobs(&pool).await?;
    wipe(&pool).await?;
    seed(&pool, &[ev(1, 9601, 11), ev(2, 9601, 12), ev(3, 9601, 13)]).await?;
    let outbox = Outbox::<TestEvent, TestTables>::init(&pool, config(CommitLane::Enabled)).await?;
    let flushes = Arc::new(Mutex::new(Vec::new()));
    outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
                .with_max_batch_size(1)
                .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL),
            BoundaryFlusher::<CommitOrder>::new(flushes.clone()),
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    jobs.start_poll().await?;

    until(
        async || flushes.lock().await.iter().flatten().count() >= 3,
        "the commit-lane group flushed",
    )
    .await?;
    assert_eq!(
        *flushes.lock().await,
        vec![vec![11, 12, 13]],
        "the soft limit gives way to the group boundary: one flush, the whole transaction",
    );

    Ok(())
}

struct FailingFlusher<L> {
    _lane: std::marker::PhantomData<fn() -> L>,
}

impl<L> FailingFlusher<L> {
    fn new() -> Self {
        Self {
            _lane: std::marker::PhantomData,
        }
    }
}

impl SingletonSubscriber<TestEvent> for FailingFlusher<InsertOrder> {
    type Batch = Vec<u64>;

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, Vec<u64>>,
        event: &EventDelivery<TestEvent>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        let _ = event;
        Ok(ctx.collect_with(|batch| batch.push(1)))
    }

    async fn flush(
        &self,
        _op: &mut FlushOp<'_, InsertOrder>,
        _items: Vec<u64>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Err("flush refused".into())
    }
}

impl SingletonSubscriber<TestEvent, CommitOrder> for FailingFlusher<CommitOrder> {
    type Batch = Vec<u64>;

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, Vec<u64>>,
        event: &EventDelivery<TestEvent, CommitOrder>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        let _ = event;
        Ok(ctx.collect_with(|batch| batch.push(1)))
    }

    async fn flush(
        &self,
        _op: &mut FlushOp<'_, CommitOrder>,
        _items: Vec<u64>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Err("flush refused".into())
    }
}

/// The error surfaces through the job's `last_error`, which renders
/// `FlushError`'s `after`/`through` fields.
#[tokio::test]
#[file_serial]
async fn flush_error_reports_lane_positions() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    // Insert lane.
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool, CommitLane::Disabled).await?;
    let subscription = outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
                .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL),
            FailingFlusher::<InsertOrder>::new(),
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    jobs.start_poll().await?;

    publish_group(&outbox, [1]).await?;
    let mut reported = String::new();
    until(
        async || {
            if let Ok(snapshot) = subscription.load().await
                && let Some(error) = snapshot.last_error()
            {
                reported = error.to_string();
                return true;
            }
            false
        },
        "the insert-lane flush failure reached the job",
    )
    .await?;
    assert!(
        reported.contains("(insert:0, insert:1]"),
        "the insert lane must report insert positions: {reported}",
    );
    let _ = jobs.shutdown().await;
    drop(jobs);
    drop(outbox);

    // Commit lane, over an interleaving where the two numberings differ: group
    // X at sequences 1 and 4, group Y at 2 and 3.
    let mut jobs = init_jobs(&pool).await?;
    wipe(&pool).await?;
    seed(
        &pool,
        &[
            ev(1, 9701, 11),
            ev(2, 9702, 21),
            ev(3, 9702, 22),
            ev(4, 9701, 12),
        ],
    )
    .await?;
    let outbox = Outbox::<TestEvent, TestTables>::init(&pool, config(CommitLane::Enabled)).await?;
    let subscription = outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
                .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL),
            FailingFlusher::<CommitOrder>::new(),
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    jobs.start_poll().await?;

    let mut reported = String::new();
    until(
        async || {
            if let Ok(snapshot) = subscription.load().await
                && let Some(error) = snapshot.last_error()
            {
                reported = error.to_string();
                return true;
            }
            false
        },
        "the commit-lane flush failure reached the job",
    )
    .await?;
    assert!(
        reported.contains("commit:") && !reported.contains("insert:"),
        "the commit lane must report commit positions, and only those: {reported}",
    );

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn positions_equal_the_first_sight_numbering() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;

    // Five interleaved groups, including a straddler: 8005 is first sighted at
    // 3 and has a member at 14.
    let layout: Vec<(i64, i64, u64)> = vec![
        (1, 8001, 11),
        (2, 8002, 21),
        (3, 8005, 51),
        (4, 8001, 12),
        (5, 8003, 31),
        (6, 8002, 22),
        (7, 8003, 32),
        (8, 8004, 41),
        (9, 8004, 42),
        (10, 8001, 13),
        (11, 8002, 23),
        (12, 8003, 33),
        (13, 8004, 43),
        (14, 8005, 52),
    ];
    wipe(&pool).await?;
    seed(
        &pool,
        &layout
            .iter()
            .map(|(seq, xid, payload)| ev(*seq, *xid, *payload))
            .collect::<Vec<_>>(),
    )
    .await?;

    // The expected numbering, computed independently of the implementation: at
    // each group's first sight emit its whole membership in sequence order.
    let mut expected: Vec<(u64, u64)> = Vec::new(); // (position, payload)
    let mut emitted: std::collections::HashSet<i64> = std::collections::HashSet::new();
    for (_, xid, _) in &layout {
        if !emitted.insert(*xid) {
            continue;
        }
        let mut members: Vec<(i64, u64)> = layout
            .iter()
            .filter(|(_, x, _)| x == xid)
            .map(|(seq, _, payload)| (*seq, *payload))
            .collect();
        members.sort_unstable();
        for (_, payload) in members {
            expected.push((expected.len() as u64 + 1, payload));
        }
    }

    let outbox = Outbox::<TestEvent, TestTables>::init(&pool, config(CommitLane::Enabled)).await?;
    let recorded = Arc::new(Mutex::new(Vec::new()));
    outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
                .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL),
            CommitPositionRecorder {
                recorded: recorded.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    jobs.start_poll().await?;

    until(
        async || recorded.lock().await.len() >= layout.len(),
        "every seeded event delivered",
    )
    .await?;

    let delivered: Vec<(u64, u64)> = recorded
        .lock()
        .await
        .iter()
        .map(|r| (u64::from(r.position), r.payload))
        .collect();
    assert_eq!(
        delivered, expected,
        "the computed numbering must equal the first-sight rule, member for member",
    );

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn restart_resumes_from_a_checkpoint_without_renumbering() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    // Group 8101 straddles: first sighted at 1, last member at 5, so the fold
    // checkpoints while it is still open.
    wipe(&pool).await?;
    seed(
        &pool,
        &[
            ev(1, 8101, 11),
            ev(2, 8102, 21),
            ev(3, 8102, 22),
            ev(4, 8103, 31),
            ev(5, 8101, 12),
        ],
    )
    .await?;

    let first: Vec<(u64, u64)> = {
        let outbox =
            Outbox::<TestEvent, TestTables>::init(&pool, config_checkpointing_every_group())
                .await?;
        let mut listener = outbox.listen_commit_ordered(CommitSequence::BEGIN)?;
        let mut seen = Vec::new();
        for _ in 0..5 {
            let item = listener.next().await.expect("event")?;
            seen.push((u64::from(item.position()), item.sequence.into()));
        }
        // A checkpoint must exist, or the restart below proves nothing.
        until(
            async || {
                checkpoint_state(&pool)
                    .await
                    .map(|(count, _)| count > 0)
                    .unwrap_or(false)
            },
            "the fold wrote a checkpoint",
        )
        .await?;
        seen
    };
    assert_eq!(
        first.iter().map(|(p, _)| *p).collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5],
    );

    // A fresh process, seeded from the checkpoint rather than from zero.
    seed(&pool, &[ev(6, 8104, 41)]).await?;
    let outbox =
        Outbox::<TestEvent, TestTables>::init(&pool, config_checkpointing_every_group()).await?;
    let mut listener = outbox.listen_commit_ordered(CommitSequence::from(5u64))?;
    let item = tokio::time::timeout(Duration::from_secs(20), listener.next())
        .await
        .map_err(|_| anyhow::anyhow!("the resumed fold never reached the new group"))?
        .expect("event")?;
    assert_eq!(
        (u64::from(item.position()), u64::from(item.sequence)),
        (6, 6),
        "the numbering must continue at 6 — a fold that re-emitted the straddling group would \
         renumber, and one that restarted from zero would repeat",
    );

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn no_per_group_writes() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = init_outbox(&pool, CommitLane::Enabled).await?;

    // Twenty single-event groups against the default cadence of 1,000.
    for n in 1..=20u64 {
        publish_group(&outbox, [n]).await?;
    }
    until(
        async || {
            outbox
                .frontier::<CommitOrder>()
                .await
                .map(|head| head == CommitSequence::from(20u64))
                .unwrap_or(false)
        },
        "the fold emitted all twenty groups",
    )
    .await?;

    let (checkpoints, _) = checkpoint_state(&pool).await?;
    assert!(
        checkpoints <= 1,
        "twenty groups under a 1,000-group cadence must not write twenty rows: {checkpoints}",
    );

    for table in [
        "persistent_outbox_commit_log",
        "persistent_outbox_commit_log_state",
    ] {
        let exists: (Option<String>,) = sqlx::query_as("SELECT to_regclass($1)::text")
            .bind(table)
            .fetch_one(&pool)
            .await?;
        assert_eq!(exists.0, None, "{table} must not exist any more");
    }

    Ok(())
}
