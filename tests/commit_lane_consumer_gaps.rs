//! Tests from obix-dev/handoff-commit-lane-consumer-gaps.md §6: the two
//! consumer-side gaps that blocked commit-lane adoption.
//!
//! Gap A — a commit-lane subscriber can read its own position
//! (`EventCtx::commit_position`, `FlushOp::commit_position`). Gap B (§10) —
//! `Subscription`'s snapshot and `await_caught_up` fence are lane-aware
//! (`StreamPosition`), with the corrected fence from §4 (polls the
//! sequencer's own fold position, not `commit_log_state().logged_through_sequence`,
//! which stalls behind an open group or an aborted tail).

mod helpers;

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering as AtomicOrd},
};
use std::time::Duration;

use obix::{
    CommitSequence, EventCtx, FlushOp, Handled, MailboxConfig, Ordering, OutboxEventJobConfig,
    SingletonSubscriber, StreamPosition, out::Outbox,
};
use serde::{Deserialize, Serialize};
use serial_test::file_serial;
use tokio::sync::{Mutex, Notify};

use helpers::{TestTables, init_pool, wipeout_outbox_job_tables, wipeout_outbox_tables};

const JOB_TYPE: &str = "test-commit-lane-consumer-gaps";

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

async fn init_outbox(
    pool: &sqlx::PgPool,
    config: MailboxConfig,
) -> anyhow::Result<Outbox<TestEvent, TestTables>> {
    wipe(pool).await?;
    Ok(Outbox::<TestEvent, TestTables>::init(pool, config).await?)
}

async fn wipe(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    wipeout_outbox_tables(pool).await?;
    wipeout_outbox_job_tables(pool, JOB_TYPE).await?;
    Ok(())
}

/// One seeded event: its insert sequence, the transaction (group) it
/// belongs to, and its `Ping` payload.
struct Seeded {
    sequence: i64,
    xid: i64,
    payload: u64,
}

/// Write events at exact insert sequences and groups, then advance the
/// generator past them.
///
/// Direct SQL rather than `publish`, for the reason `tests/commit_ordered.rs`
/// gives: insert sequences are allocated in `PersistEvents::pre_commit`, so
/// the write path cannot produce a chosen interleaving of two transactions'
/// sequences. The `setval` is load-bearing for anything fencing on
/// `await_caught_up`: the frontier is the generator's `last_value`, which
/// explicit-sequence inserts do not move.
async fn seed(pool: &sqlx::PgPool, rows: &[Seeded]) -> anyhow::Result<()> {
    for row in rows {
        sqlx::query(
            "INSERT INTO persistent_outbox_events (sequence, payload, commit_xid)
             VALUES ($1, $2, $3)",
        )
        .bind(row.sequence)
        .bind(serde_json::json!({ "Ping": row.payload }))
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

/// Short checkpoint interval so a skip-only handler's lazy checkpoint lands
/// inside a test's patience.
const TEST_CHECKPOINT_INTERVAL: Duration = Duration::from_millis(50);

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

// === Test 1 & 2: EventCtx::commit_position ===

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Recorded {
    payload: u64,
    commit_seq: u64,
    boundary: bool,
}

struct PositionRecorder {
    recorded: Arc<Mutex<Vec<Recorded>>>,
}

impl SingletonSubscriber<TestEvent> for PositionRecorder {
    type Batch = ();

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, ()>,
        event: &Arc<obix::out::PersistentOutboxEvent<TestEvent>>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(TestEvent::Ping(n)) = &event.payload {
            let (commit_seq, boundary) = ctx
                .commit_position()
                .expect("commit lane must report a position");
            self.recorded.lock().await.push(Recorded {
                payload: *n,
                commit_seq: u64::from(commit_seq),
                boundary,
            });
        }
        Ok(ctx.skip())
    }
}

/// Test 1 — three transactions of three events each, maximally interleaved
/// in insert order (round-robin: A at 1/4/7, B at 2/5/8, C at 3/6/9), and
/// assert `EventCtx::commit_position()` reports a dense 1..=9 numbering in
/// which every group's members are contiguous and only the group's last
/// member is a commit boundary.
#[tokio::test]
#[file_serial]
async fn commit_position_is_visible_to_a_commit_lane_subscriber() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;

    // Groups A/B/C, round-robin interleaved in insert order — a shape the
    // write path cannot produce, since a transaction's sequences are
    // allocated contiguously in its own pre-commit.
    let group_a = [11u64, 12, 13];
    let group_b = [21u64, 22, 23];
    let group_c = [31u64, 32, 33];
    wipe(&pool).await?;
    seed(
        &pool,
        &[
            Seeded {
                sequence: 1,
                xid: 8001,
                payload: group_a[0],
            },
            Seeded {
                sequence: 2,
                xid: 8002,
                payload: group_b[0],
            },
            Seeded {
                sequence: 3,
                xid: 8003,
                payload: group_c[0],
            },
            Seeded {
                sequence: 4,
                xid: 8001,
                payload: group_a[1],
            },
            Seeded {
                sequence: 5,
                xid: 8002,
                payload: group_b[1],
            },
            Seeded {
                sequence: 6,
                xid: 8003,
                payload: group_c[1],
            },
            Seeded {
                sequence: 7,
                xid: 8001,
                payload: group_a[2],
            },
            Seeded {
                sequence: 8,
                xid: 8002,
                payload: group_b[2],
            },
            Seeded {
                sequence: 9,
                xid: 8003,
                payload: group_c[2],
            },
        ],
    )
    .await?;

    let outbox = Outbox::<TestEvent, TestTables>::init(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let recorded = Arc::new(Mutex::new(Vec::new()));
    outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE)).ordering(Ordering::Commit),
            PositionRecorder {
                recorded: recorded.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    jobs.start_poll().await?;

    until(
        async || recorded.lock().await.len() >= 9,
        "all 9 events delivered with a commit position",
    )
    .await?;

    let recorded = recorded.lock().await.clone();
    assert_eq!(recorded.len(), 9, "every event delivered exactly once");

    let mut all_seqs: Vec<u64> = recorded.iter().map(|r| r.commit_seq).collect();
    all_seqs.sort_unstable();
    assert_eq!(
        all_seqs,
        (1..=9).collect::<Vec<_>>(),
        "commit positions must be a dense 1..=9 numbering with no gap or repeat: {all_seqs:?}"
    );

    for group in [group_a, group_b, group_c] {
        let seqs: Vec<u64> = group
            .iter()
            .map(|p| {
                recorded
                    .iter()
                    .find(|r| r.payload == *p)
                    .unwrap_or_else(|| panic!("payload {p} was never recorded"))
                    .commit_seq
            })
            .collect();
        let min = *seqs.iter().min().unwrap();
        let max = *seqs.iter().max().unwrap();
        assert_eq!(
            max - min + 1,
            3,
            "group {group:?} must be contiguous in commit order, got {seqs:?}"
        );
        let mut sorted = seqs.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, vec![min, min + 1, min + 2]);

        for (i, payload) in group.iter().enumerate() {
            let entry = recorded.iter().find(|r| r.payload == *payload).unwrap();
            let is_last_inserted = i == group.len() - 1;
            assert_eq!(
                entry.boundary, is_last_inserted,
                "payload {payload} commit_boundary must be true iff it is the group's last \
                 member, got {entry:?}"
            );
        }
    }

    Ok(())
}

/// Test 2 — on the insert lane (default `Ordering`), `commit_position()` is
/// always `None`.
#[tokio::test]
#[file_serial]
async fn commit_position_is_none_on_the_insert_lane() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    struct AssertNoCommitPosition {
        seen: Arc<Mutex<usize>>,
    }

    impl SingletonSubscriber<TestEvent> for AssertNoCommitPosition {
        type Batch = ();

        async fn handle_persistent<'inv>(
            &self,
            ctx: EventCtx<'inv, ()>,
            event: &Arc<obix::out::PersistentOutboxEvent<TestEvent>>,
        ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
            if event.payload.is_some() {
                assert_eq!(
                    ctx.commit_position(),
                    None,
                    "the insert lane must never report a commit position"
                );
                *self.seen.lock().await += 1;
            }
            Ok(ctx.skip())
        }
    }

    let seen = Arc::new(Mutex::new(0));
    outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE)),
            AssertNoCommitPosition { seen: seen.clone() },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    jobs.start_poll().await?;

    let mut op = outbox.begin_op().await?;
    for n in 0..3u64 {
        outbox
            .publish_persisted_in_op(&mut op, TestEvent::Ping(n))
            .await?;
    }
    op.commit().await?;

    until(async || *seen.lock().await >= 3, "all events observed").await?;
    Ok(())
}

// === Test 3: FlushOp::commit_position ===

/// `commit_position()` read for every event, keyed by payload — lets the
/// test compare a flush's reported position against the boundary event's
/// own, even when that event was skipped rather than collected.
type PerEventPositions = Arc<Mutex<Vec<(u64, Option<(CommitSequence, bool)>)>>>;

struct FlushPositionRecorder {
    per_event: PerEventPositions,
    collected_max: Arc<Mutex<Option<u64>>>,
    flush_positions: Arc<Mutex<Vec<Option<CommitSequence>>>>,
}

impl SingletonSubscriber<TestEvent> for FlushPositionRecorder {
    type Batch = Vec<u64>;

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, Vec<u64>>,
        event: &Arc<obix::out::PersistentOutboxEvent<TestEvent>>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        let Some(TestEvent::Ping(n)) = &event.payload else {
            return Ok(ctx.skip());
        };
        let n = *n;
        self.per_event.lock().await.push((n, ctx.commit_position()));
        // Skip the group's trailing member (n % 10 == 3) so the flush's
        // position must come from the runner's own tracked checkpoint, not
        // from a max over collected rows.
        if n % 10 == 3 {
            Ok(ctx.skip())
        } else {
            let mut max = self.collected_max.lock().await;
            *max = Some(max.map_or(n, |m| m.max(n)));
            Ok(ctx.collect(n))
        }
    }

    async fn flush(
        &self,
        op: &mut FlushOp<'_>,
        items: Vec<u64>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if !items.is_empty() {
            self.flush_positions.lock().await.push(op.commit_position());
        }
        Ok(())
    }
}

/// Test 3 — with `max_batch_size` smaller than the group, the flush's
/// `commit_position()` is the last *handled* event's position (a commit
/// boundary), not the max over the collected (and possibly
/// trailing-skipped) rows.
#[tokio::test]
#[file_serial]
async fn flush_position_is_the_batch_boundary() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let per_event = Arc::new(Mutex::new(Vec::new()));
    let collected_max = Arc::new(Mutex::new(None));
    let flush_positions = Arc::new(Mutex::new(Vec::new()));

    outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
                .ordering(Ordering::Commit)
                .with_max_batch_size(2),
            FlushPositionRecorder {
                per_event: per_event.clone(),
                collected_max: collected_max.clone(),
                flush_positions: flush_positions.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    jobs.start_poll().await?;

    // One group of three: 1 and 2 are collected, 3 (n % 10 == 3) is skipped
    // and is the group's boundary.
    let mut op = outbox.begin_op().await?;
    for n in [1u64, 2, 3] {
        outbox
            .publish_persisted_in_op(&mut op, TestEvent::Ping(n))
            .await?;
    }
    op.commit().await?;

    until(
        async || per_event.lock().await.len() >= 3,
        "all 3 group members observed",
    )
    .await?;
    until(
        async || !flush_positions.lock().await.is_empty(),
        "the batch flushed",
    )
    .await?;

    let per_event = per_event.lock().await.clone();
    let boundary_position = per_event
        .iter()
        .find(|(n, _)| *n == 3)
        .and_then(|(_, cp)| *cp)
        .map(|(seq, boundary)| {
            assert!(boundary, "payload 3 must be the group's commit boundary");
            seq
        })
        .expect("payload 3 must have a commit position");

    let collected_max = collected_max
        .lock()
        .await
        .expect("payloads 1 and 2 were collected");
    let collected_max_position = per_event
        .iter()
        .find(|(n, _)| *n == collected_max)
        .and_then(|(_, cp)| *cp)
        .expect("collected max payload must have a commit position")
        .0;
    assert!(
        boundary_position > collected_max_position,
        "the boundary (skipped) event must have a strictly higher commit position than any \
         collected row: boundary={boundary_position} collected_max={collected_max_position}"
    );

    let flush_positions = flush_positions.lock().await.clone();
    assert_eq!(
        flush_positions,
        vec![Some(boundary_position)],
        "the flush's commit_position() must equal the boundary event's position, not the max \
         over collected rows"
    );

    Ok(())
}

// === Test 4: the fence does not return early ===

/// Commits the `commit_at`'th event in its own op — which persists the
/// checkpoint durably and immediately, the state the fence reads — then
/// blocks on the `block_at`'th until released.
struct CommitThenBlock {
    commit_at: usize,
    block_at: usize,
    blocked: Arc<AtomicBool>,
    release: Arc<Notify>,
    received: Arc<Mutex<Vec<u64>>>,
}

impl SingletonSubscriber<TestEvent> for CommitThenBlock {
    type Batch = ();

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, ()>,
        event: &Arc<obix::out::PersistentOutboxEvent<TestEvent>>,
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

/// Test 4 — the fence must not return early on the commit lane.
///
/// The shape that makes the bug reachable: group A occupies insert
/// sequences 1 and 4, group B sequences 2 and 3, so commit order is
/// `[1, 4, 2, 3]` — A is first-sighted at sequence 1 and pulled contiguous.
/// After the subscriber handles the first two deliveries its INSERT cursor
/// is already 4, the insert frontier, while its COMMIT cursor is only 2 of
/// 4. A fence that compares the insert cursor against the insert frontier —
/// which is what `Subscription` did before this change, since
/// `singleton.rs` assigns `state.sequence = event.sequence` on this lane
/// too — therefore returns with half the stream undelivered.
///
/// A suite whose transactions commit in the order they opened cannot
/// distinguish the two cursors at all, which is why this interleaving is
/// seeded rather than published.
#[tokio::test]
#[file_serial]
async fn caught_up_fence_on_the_commit_lane_does_not_return_early() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;

    wipe(&pool).await?;
    seed(
        &pool,
        &[
            Seeded {
                sequence: 1,
                xid: 9001,
                payload: 101,
            },
            Seeded {
                sequence: 2,
                xid: 9002,
                payload: 201,
            },
            Seeded {
                sequence: 3,
                xid: 9002,
                payload: 202,
            },
            Seeded {
                sequence: 4,
                xid: 9001,
                payload: 102,
            },
        ],
    )
    .await?;

    let outbox = Outbox::<TestEvent, TestTables>::init(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let blocked = Arc::new(AtomicBool::new(false));
    let release = Arc::new(Notify::new());
    let received = Arc::new(Mutex::new(Vec::new()));

    let subscription = outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
                .ordering(Ordering::Commit)
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

    // Commit positions 1 and 2 (insert sequences 1 and 4) are handled and
    // durably checkpointed; the handler is then blocked on position 3. The
    // persisted INSERT cursor is now 4 — the insert frontier — while the
    // persisted COMMIT cursor is 2 of 4.
    until(
        async || blocked.load(AtomicOrd::SeqCst),
        "handler blocked on the third delivery",
    )
    .await?;
    assert_eq!(
        *received.lock().await,
        vec![101, 102, 201],
        "commit order must deliver group A (sequences 1 and 4) first, contiguously"
    );

    let snapshot = subscription.load().await?;
    assert_eq!(
        snapshot.ordering(),
        Ordering::Commit,
        "the fence under test is the commit-lane one, which needs a committed commit cursor"
    );
    assert_eq!(
        snapshot.checkpoint(),
        StreamPosition::Commit(CommitSequence::from(2u64)),
        "precondition: the commit cursor is only 2 of 4"
    );
    assert_eq!(
        outbox.highest_known_persistent_sequence().await?,
        obix::EventSequence::from(4u64),
        "precondition: the insert frontier is 4 — the same value the insert cursor now holds, \
         which is exactly what an insert-cursor fence would wrongly accept as caught up"
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
        "await_caught_up returned while commit positions 3 and 4 were still undelivered"
    );

    release.notify_one();

    barrier
        .await?
        .map_err(|e| anyhow::anyhow!("await_caught_up: {e}"))?;

    assert_eq!(
        *received.lock().await,
        vec![101, 102, 201, 202],
        "every event must be delivered, in commit order"
    );

    Ok(())
}

// === Test 5: the fence is not wedged by an aborted tail ===

/// Test 5 — an aborted transaction at the head (the highest allocated
/// sequences) must not wedge `await_caught_up`: the corrected fence (§4)
/// waits on the sequencer's own fold position, which advances past a
/// placeholder, rather than `commit_log_state().logged_through_sequence`,
/// which only advances when a real group is appended and therefore never
/// reaches past an aborted tail.
#[tokio::test]
#[file_serial]
async fn caught_up_fence_is_not_wedged_by_an_aborted_tail() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(
        &pool,
        MailboxConfig::builder()
            .gap_fill_grace(Duration::from_millis(100))
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let received = Arc::new(Mutex::new(Vec::new()));
    struct Observer {
        received: Arc<Mutex<Vec<u64>>>,
    }
    impl SingletonSubscriber<TestEvent> for Observer {
        type Batch = ();
        async fn handle_persistent<'inv>(
            &self,
            ctx: EventCtx<'inv, ()>,
            event: &Arc<obix::out::PersistentOutboxEvent<TestEvent>>,
        ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
            if let Some(TestEvent::Ping(n)) = &event.payload {
                self.received.lock().await.push(*n);
            }
            Ok(ctx.skip())
        }
    }

    let subscription = outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
                .ordering(Ordering::Commit)
                .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL),
            Observer {
                received: received.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    jobs.start_poll().await?;

    // Two real, committed events…
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(1))
        .await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(2))
        .await?;
    op.commit().await?;

    until(
        async || received.lock().await.len() >= 2,
        "the two real events delivered",
    )
    .await?;
    // The fence under test is the COMMIT-lane one, which is only taken once
    // a commit cursor has been checkpointed. Without this the test can race
    // onto the insert-lane fence and pass while proving nothing.
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

    // …then a writer that allocates the highest sequence and aborts. Raw
    // SQL, because `publish_persisted_in_op` only registers the event: the
    // INSERT (and with it the `nextval`) happens in the op's pre-commit, so
    // an op that is dropped never burns a sequence at all.
    let before_abort = outbox.highest_known_persistent_sequence().await?;
    let mut aborted = pool.begin().await?;
    sqlx::query("INSERT INTO persistent_outbox_events (payload) VALUES ($1::jsonb)")
        .bind(r#"{"Ping": 999}"#)
        .execute(&mut *aborted)
        .await?;
    aborted.rollback().await?;

    // The premise of the test: the burned allocation really did push the
    // insert head (which `await_caught_up` anchors H on) past everything
    // that will ever reach the commit log.
    let after_abort = outbox.highest_known_persistent_sequence().await?;
    assert!(
        after_abort > before_abort,
        "the aborted transaction must have raised the insert head: {before_abort} -> \
         {after_abort}"
    );

    // Must return, not time out — this is the revert-to-red case: reverting
    // the corrected fence to poll `logged_through_sequence` instead of the
    // sequencer's fold position hangs here, since nothing ever appends the
    // aborted sequence to the commit log.
    subscription
        .await_caught_up(Duration::from_secs(15))
        .await
        .map_err(|e| {
            anyhow::anyhow!("await_caught_up must not be wedged by an aborted tail: {e}")
        })?;

    Ok(())
}

// === Test 6: SubscriptionSnapshot is lane-aware ===

/// Test 6 — `load()` on an `Ordering::Commit` subscription reports
/// `StreamPosition::Commit`, and `lag()`/`is_caught_up()` are correct
/// against the commit log's own head.
#[tokio::test]
#[file_serial]
async fn snapshot_reports_commit_lane_positions() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    struct Skip;
    impl SingletonSubscriber<TestEvent> for Skip {
        type Batch = ();
    }

    let subscription = outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
                .ordering(Ordering::Commit)
                .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL),
            Skip,
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    jobs.start_poll().await?;

    // Three single-member groups: commit order equals insert order, so the
    // commit log's head is exactly 3 — an independent value this test reads
    // straight off `persistent_outbox_commit_log_state` rather than trusting
    // the subscriber's own report of it.
    let mut op = outbox.begin_op().await?;
    for n in 0..3u64 {
        outbox
            .publish_persisted_in_op(&mut op, TestEvent::Ping(n))
            .await?;
    }
    op.commit().await?;

    until(
        async || {
            subscription
                .load()
                .await
                .map(|s| s.is_caught_up())
                .unwrap_or(false)
        },
        "the commit-lane subscription catches up",
    )
    .await?;

    let (last_commit_seq,): (i64,) = sqlx::query_as(
        "SELECT last_commit_seq FROM persistent_outbox_commit_log_state WHERE singleton",
    )
    .fetch_one(&pool)
    .await?;
    let expected = StreamPosition::Commit(CommitSequence::from(last_commit_seq as u64));

    let snapshot = subscription.load().await?;
    assert_eq!(snapshot.ordering(), Ordering::Commit);
    assert!(matches!(snapshot.checkpoint(), StreamPosition::Commit(_)));
    assert!(matches!(snapshot.frontier(), StreamPosition::Commit(_)));
    assert_eq!(snapshot.frontier(), expected);
    assert_eq!(snapshot.checkpoint(), expected);
    assert_eq!(snapshot.lag(), 0);
    assert!(snapshot.is_caught_up());

    Ok(())
}
