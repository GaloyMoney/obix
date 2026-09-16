//! Lane-typed delivery: one handler API for both lanes.
//!
//! A position is a property of a delivery on a lane, so every delivered thing
//! — event, undecodable stand-in, batch flush, checkpoint — is typed by its
//! lane and reports that lane's position. These tests pin the positions
//! themselves (1, 2, 3, 4, 5, 8, 11), the exact flush watermark (1, 3), the
//! corrected commit-lane fence (6, 7), lane inference and refusal (9), and
//! the commit lane's opt-in switch with its late-enable guarantee (12, 13,
//! 14).

mod helpers;

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering as AtomicOrd},
};
use std::time::Duration;

use futures::TryStreamExt;
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
/// A second job type, for the tests that put both lanes' handles on one
/// outbox: the two cursors count different things, so they cannot share a
/// subscription.
const INSERT_JOB_TYPE: &str = "test-lane-typed-delivery-insert";

/// Short enough that a skip-only handler's lazy checkpoint lands inside a
/// test's patience, rather than at the 5s production default.
const TEST_CHECKPOINT_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum TestEvent {
    Ping(u64),
}

// === Harness ===

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

async fn wipe(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    wipeout_outbox_tables(pool).await?;
    wipeout_outbox_job_tables(pool, JOB_TYPE).await?;
    Ok(())
}

/// Wipe, then open an outbox with the lane in the given state.
async fn init_outbox(
    pool: &sqlx::PgPool,
    commit_lane: CommitLane,
) -> anyhow::Result<Outbox<TestEvent, TestTables>> {
    wipe(pool).await?;
    Ok(Outbox::<TestEvent, TestTables>::init(pool, config(commit_lane)).await?)
}

/// One seeded event: its insert sequence, the transaction (group) it belongs
/// to, and the payload exactly as stored.
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

/// A row whose stored payload cannot decode into [`TestEvent`].
fn undecodable(sequence: i64, xid: i64) -> Seeded {
    Seeded {
        sequence,
        xid,
        payload: serde_json::json!({ "NotAVariant": true }),
    }
}

/// Write events at exact insert sequences and groups, then advance the
/// generator past them.
///
/// Direct SQL rather than `publish`: insert sequences are allocated in
/// `PersistEvents::pre_commit`, so the write path cannot produce a chosen
/// interleaving of two transactions' sequences — a suite whose transactions
/// commit in the order they opened cannot tell the two lanes apart at all.
/// The `setval` is load-bearing for anything fencing on `await_caught_up`:
/// the frontier is the generator's `last_value`, which explicit-sequence
/// inserts do not move, so without it the frontier reads 0 and every fence
/// passes trivially.
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

/// `(last_commit_seq, logged_through_sequence)` straight from the sequencer's
/// state row — an independent reading of the lane's head.
async fn commit_log_state(pool: &sqlx::PgPool) -> anyhow::Result<(i64, i64)> {
    let row: (i64, i64) = sqlx::query_as(
        "SELECT last_commit_seq, logged_through_sequence
         FROM persistent_outbox_commit_log_state WHERE singleton",
    )
    .fetch_one(pool)
    .await?;
    Ok(row)
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

/// Poll until `f` holds, without sleeping on a fixed budget: the condition
/// itself is the synchronisation.
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

// === 1. Insert-lane positions and the exact flush watermark ===

/// Collects the first payload, skips the rest, and records what every flush
/// reported against what the handler had actually last handled.
///
/// `gate` blocks the very first invocation, which is what makes the batch
/// deterministic: the test publishes the remaining events while the runner is
/// parked inside this handler, so they are already broadcast (and buffered)
/// by the time the runner asks for more — the `now_or_never` path that keeps
/// a batch open then cannot miss them.
struct InsertPositionRecorder {
    gate: Arc<Notify>,
    gated: Arc<AtomicBool>,
    /// `(position(), sequence)` per delivery — equal on this lane, always.
    seen: Arc<Mutex<Vec<(EventSequence, EventSequence)>>>,
    /// The last sequence the handler fully handled, skips included.
    last_handled: Arc<Mutex<Option<EventSequence>>>,
    /// `(FlushOp::position(), last_handled at that moment, items)`.
    flushes: Arc<Mutex<Vec<(EventSequence, Option<EventSequence>, Vec<u64>)>>>,
}

impl SingletonSubscriber<TestEvent> for InsertPositionRecorder {
    type Batch = Vec<u64>;

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, Vec<u64>>,
        event: &EventDelivery<TestEvent>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        if !self.gated.swap(true, AtomicOrd::SeqCst) {
            self.gate.notified().await;
        }
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

/// Test 1 — on the insert lane a delivery's position IS its sequence, and a
/// flush lands at the last *fully handled* event, not at the highest event it
/// happened to collect.
#[tokio::test]
#[file_serial]
async fn insert_lane_position_is_the_sequence() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool, CommitLane::Disabled).await?;

    let gate = Arc::new(Notify::new());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let flushes = Arc::new(Mutex::new(Vec::new()));
    outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
                .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL),
            InsertPositionRecorder {
                gate: gate.clone(),
                gated: Arc::new(AtomicBool::new(false)),
                seen: seen.clone(),
                last_handled: Arc::new(Mutex::new(None)),
                flushes: flushes.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    jobs.start_poll().await?;

    // Ping(1) is collected; Ping(2) and Ping(3) are skipped after it, so the
    // batch's last fully handled event is 3 while its only collected row is
    // 1. Published while the handler is parked on the first delivery, so all
    // three are buffered before the runner resumes.
    publish_group(&outbox, [1]).await?;
    publish_group(&outbox, [2, 3]).await?;
    // Released only after every event is broadcast, so the runner's
    // `now_or_never` poll — the one that keeps a batch open — cannot miss
    // them and split the batch on arrival timing.
    gate.notify_one();

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

// === 2. Commit-lane positions are dense and groups are contiguous ===

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

/// The three-group round-robin interleaving: A at insert sequences 1/4/7, B
/// at 2/5/8, C at 3/6/9. Commit order places each group at first sight of its
/// lowest member and pulls it contiguous, so the lane reads A,A,A,B,B,B,C,C,C
/// while insert order reads A,B,C,A,B,C,A,B,C.
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

/// Test 2 — the commit lane numbers deliveries densely from 1, a source
/// transaction's members arrive contiguously, and exactly the last member of
/// each is a commit boundary.
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

// === 3. The commit-lane flush watermark is the boundary, not the max ===

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
        // The group's last member is skipped, so the batch's highest
        // collected row is strictly below the position it lands at.
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

/// Test 3 — with `max_batch_size` below the group size the runner holds the
/// batch open to the group boundary, and the flush reports that boundary's
/// position: the exact watermark, strictly above every row it carries.
#[tokio::test]
#[file_serial]
async fn commit_lane_flush_position_is_the_boundary_not_the_max() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;

    wipe(&pool).await?;
    // One transaction of three events: commit positions 1, 2, 3.
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

// === 4. An undecodable delivery carries its lane position ===

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

/// Test 4 — `handle_undecodable` is handed the slot the event occupies on the
/// handler's lane, and acknowledging it advances both cursors so a restart
/// does not redeliver it.
#[tokio::test]
#[file_serial]
async fn undecodable_delivery_carries_the_lane_position() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    // Two interleaved groups, so the undecodable row's commit slot and its
    // insert sequence are DIFFERENT numbers — otherwise this test cannot tell
    // a lane position from a sequence at all. Group X holds sequences 1 and
    // 4, group Y sequences 2 and 3, so commit order is [1, 4, 2, 3] and the
    // undecodable row at sequence 2 occupies commit slot 3.
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

// === 5. The raw listeners put the Result on the outside, on both lanes ===

/// Test 5 — `try_next()?` fails loudly on an undecodable payload identically
/// on both lanes, and the error carries the position it occupied.
#[tokio::test]
#[file_serial]
async fn raw_listener_try_next_fails_with_position() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipe(&pool).await?;
    // Interleaved again, so the undecodable row's commit slot (3) and its
    // insert sequence (2) are different numbers.
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

// === 6. The commit-lane fence does not return early ===

/// Commits the `commit_at`'th event in its own op — which persists the
/// checkpoint durably and immediately, the state the fence reads — then
/// blocks on the `block_at`'th until released.
///
/// A handler that merely blocks on its first delivery never persists a
/// checkpoint at all, which leaves the stored state lane-less and makes the
/// fence silently take the insert path.
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

/// Test 6 — the fence must not return early on the commit lane.
///
/// Group A occupies insert sequences 1 and 4, group B sequences 2 and 3, so
/// commit order is `[1, 4, 2, 3]`. After the subscriber handles the first two
/// deliveries its INSERT cursor is already 4 — the insert frontier — while
/// its COMMIT cursor is only 2 of 4. A fence comparing the insert cursor
/// against the insert frontier therefore returns with half the stream
/// undelivered.
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

    // Must NOT return while blocked — a bounded assertion window, not a
    // synchronisation sleep: `timeout` resolves the instant the barrier
    // completes and otherwise expires deterministically.
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

// === 7. The fence is not wedged by an aborted tail ===

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

/// Test 7 — an aborted transaction at the head must not wedge
/// `await_caught_up`: the fence waits on the sequencer's own fold position,
/// which advances over a placeholder, rather than on
/// `logged_through_sequence`, which only moves when a real group is appended
/// and therefore never reaches past an aborted tail.
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
    // The fence under test is the COMMIT-lane one, which is only taken once a
    // commit cursor has been checkpointed. Without this the test can race onto
    // the insert-lane fence and pass while proving nothing.
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

    // A writer that allocates the highest sequence and aborts. Raw SQL,
    // because `publish_persisted_in_op` only registers the event: the INSERT
    // (and with it the `nextval`) happens in the op's pre-commit, so an op
    // that is dropped never burns a sequence at all.
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

// === 8. The snapshot reports lane-typed positions ===

struct Skipper;
impl SingletonSubscriber<TestEvent, CommitOrder> for Skipper {
    type Batch = ();
}
struct InsertSkipper;
impl SingletonSubscriber<TestEvent> for InsertSkipper {
    type Batch = ();
}

/// Test 8 — `load()` on a `CommitOrder` subscription reports
/// `CommitSequence`s, with the frontier read from the commit log's own head;
/// the insert-lane handle reports `EventSequence`s against the generator.
#[tokio::test]
#[file_serial]
async fn snapshot_reports_lane_typed_positions() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;

    // Seeded at sequences 11-13, not 1-3: the two lanes' frontiers must be
    // DIFFERENT numbers (insert 13, commit 3), or this test cannot tell which
    // one the commit-lane snapshot read.
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

    // Against the position the three groups must reach — `is_caught_up()`
    // alone is trivially true before anything is sequenced, so waiting on it
    // would let this test read an empty lane and assert nothing.
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

    let (last_commit_seq, _) = commit_log_state(&pool).await?;
    assert_eq!(
        last_commit_seq, 3,
        "the log's own head agrees with what the subscription reports",
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
        "the commit-lane frontier is the log's head, not the sequence generator's last value",
    );
    assert_eq!(snapshot.checkpoint(), expected);
    assert_eq!(snapshot.lag(), 0);
    assert!(snapshot.is_caught_up());
    assert_eq!(
        snapshot.stream_status().frontier,
        StreamPosition::Commit(expected),
        "the dynamic read-out names the lane",
    );

    // The insert-lane handle over the same stream counts the other thing.
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

// === 9. Lane inference, and the refusal to switch ===

/// Test 9 — a handler with a single `SingletonSubscriber` impl registers with
/// no turbofish (every commit-lane registration in this file is that proof),
/// and a handle typed for the other lane than the one the subscription is
/// checkpointed on refuses to read rather than reporting a number that counts
/// something else.
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

    // The same job type, now claimed by a commit-lane handler: `decide_lane`'s
    // refusal, surfaced on the typed handle.
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

// === 11. await_position on each lane ===

/// Test 11 — `await_position` takes the lane's own position type, and its
/// timeout names the lane on both sides.
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

// === 12. The lane is off by default, and says so ===

/// Test 12 — with the lane disabled (the default) nothing folds, nothing is
/// appended, and a commit-lane consumer is refused at startup rather than
/// stalling on a stream that would never advance.
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
        commit_log_state(&pool).await?,
        (0, 0),
        "no sequencer ran: the commit log and its state row are untouched",
    );

    Ok(())
}

// === 13. Enabling the lane later sequences the full history ===

/// Test 13 — a database whose commit lane has never run is sequenced from the
/// beginning when the lane is enabled, into the same order a sequencer
/// running from day one would have produced: placement is a pure function of
/// the persisted table, not of when the fold happened to run.
#[tokio::test]
#[file_serial]
async fn enabling_the_lane_later_sequences_the_full_history_in_the_same_order() -> anyhow::Result<()>
{
    let pool = init_pool().await?;

    // Phase 1 — the lane is off. The write path still stamps `commit_group`,
    // which is what makes a later enable possible.
    {
        let outbox = init_outbox(&pool, CommitLane::Disabled).await?;
        publish_group(&outbox, [1, 2]).await?;
    }
    // …plus three groups interleaved in insert order, which the write path
    // cannot produce on its own.
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
        commit_log_state(&pool).await?,
        (0, 0),
        "precondition: nothing was sequenced while the lane was disabled — without this the \
         test is merely the always-on case again",
    );

    // Phase 2 — enable it. The fold resumes from `logged_through_sequence`
    // (0 here), so the whole history is placed, page by page.
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
    // First sight by group MIN: the published pair at sequences 1-2, then the
    // seeded groups in the order their lowest members appear (3, 4, 5).
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

    // And the lane continues live from where the backfill left off.
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

// === 14. Toggling off and on resumes from stored state ===

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
        // Its own op per event, so the checkpoint is durable immediately —
        // what a restart must resume from.
        Ok(ctx.consume().await?.commit())
    }
}

/// Test 14 — turning the lane off and on again resumes the fold and the
/// subscriber from their stored cursors: the events written while it was off
/// are sequenced on re-enable, continuing the log, and nothing already
/// delivered comes back.
#[tokio::test]
#[file_serial]
async fn toggling_off_then_on_resumes_from_stored_state() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    // Phase 1 — lane on: three single-event groups, delivered and durably
    // checkpointed at commit position 3.
    {
        let mut jobs = init_jobs(&pool).await?;
        let outbox = init_outbox(&pool, CommitLane::Enabled).await?;
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
        commit_log_state(&pool).await?.0,
        3,
        "precondition: with the lane off the log stays where phase 1 left it",
    );

    // Phase 3 — lane on again: the fold resumes from its stored cursor and
    // the subscriber from its own.
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
