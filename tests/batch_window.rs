//! Opt-in persistent windows use the real stream, runner and SQL landing path.
mod helpers;

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

use obix::{
    CommitLane, CommitOrder, EventCtx, EventDelivery, FlushOp, Handled, InsertOrder, Lane,
    MailboxConfig, OutboxEventJobConfig, SingletonSubscriber, out::Outbox,
};
use serde::{Deserialize, Serialize};
use serial_test::file_serial;
use tokio::sync::Mutex;

use helpers::{TestTables, init_pool, wipeout_outbox_job_tables, wipeout_outbox_tables};

const JOB_TYPE: &str = "test-batch-window";
const WINDOW: Duration = Duration::from_millis(400);
const WAIT: Duration = Duration::from_secs(10);

#[derive(Clone, Serialize, Deserialize)]
enum TestEvent {
    Ping(u64),
    Skip,
}

#[derive(Default)]
struct Observed {
    collected: Mutex<Vec<(u64, Instant)>>,
    attempts: Mutex<Vec<(Vec<i64>, Instant)>>,
}

struct Recorder {
    observed: Arc<Observed>,
    delay: Duration,
    isolate_on: Option<u64>,
    fail_flush_once: AtomicBool,
}

impl Recorder {
    fn new(observed: Arc<Observed>) -> Self {
        Self {
            observed,
            delay: Duration::ZERO,
            isolate_on: None,
            fail_flush_once: AtomicBool::new(false),
        }
    }
}

impl<L: Lane> SingletonSubscriber<TestEvent, L> for Recorder {
    type Batch = Vec<i64>;

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, Self::Batch>,
        event: &EventDelivery<TestEvent, L>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        let Some(TestEvent::Ping(n)) = event.payload.as_ref() else {
            return Ok(ctx.skip());
        };
        if self.isolate_on == Some(*n) {
            let mut op = ctx.consume().await?;
            use es_entity::AtomicOperation;
            sqlx::query("INSERT INTO batch_window_effects (n) VALUES ($1)")
                .bind(*n as i64)
                .execute(op.as_executor())
                .await?;
            return Ok(op.commit());
        }
        let collected_at = Instant::now();
        let handled = ctx.collect(*n as i64);
        self.observed
            .collected
            .lock()
            .await
            .push((*n, collected_at));
        Ok(handled)
    }

    async fn flush(
        &self,
        op: &mut FlushOp<'_, L>,
        items: Self::Batch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use es_entity::AtomicOperation;
        self.observed
            .attempts
            .lock()
            .await
            .push((items.clone(), Instant::now()));
        sqlx::query("INSERT INTO batch_window_effects (n) SELECT UNNEST($1::bigint[])")
            .bind(items)
            .execute(op.as_executor())
            .await?;
        if self.fail_flush_once.swap(false, Ordering::SeqCst) {
            return Err("injected windowed flush failure after writes".into());
        }
        Ok(())
    }
}

struct Harness {
    pool: sqlx::PgPool,
    jobs: job::Jobs,
    outbox: Outbox<TestEvent, TestTables>,
    observed: Arc<Observed>,
}

/// Catch-up options for [`Harness::with_catch_up`].
#[derive(Default)]
struct CatchUp {
    /// Turn on `with_full_batches_during_catch_up`.
    full_batches: bool,
    /// Override the outbox's backfill page size.
    page: Option<usize>,
    /// Published one per source transaction by another outbox before the
    /// harness's outbox boots: a backlog a fresh process starts behind.
    before_boot: Vec<TestEvent>,
}

impl Harness {
    async fn new<L: Lane>(
        window: Duration,
        max: usize,
        recorder: Recorder,
    ) -> anyhow::Result<Self> {
        Self::with_catch_up::<L>(window, max, recorder, CatchUp::default()).await
    }

    async fn with_catch_up<L: Lane>(
        window: Duration,
        max: usize,
        recorder: Recorder,
        catch_up: CatchUp,
    ) -> anyhow::Result<Self> {
        let pool = init_pool().await?;
        wipeout_outbox_tables(&pool).await?;
        wipeout_outbox_job_tables(&pool, JOB_TYPE).await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS batch_window_effects (n BIGINT PRIMARY KEY)")
            .execute(&pool)
            .await?;
        sqlx::query("TRUNCATE batch_window_effects")
            .execute(&pool)
            .await?;
        if !catch_up.before_boot.is_empty() {
            let earlier =
                Outbox::<TestEvent, TestTables>::init(&pool, MailboxConfig::builder().build())
                    .await?;
            for event in catch_up.before_boot {
                let mut op = earlier.begin_op().await?;
                earlier.publish_persisted_in_op(&mut op, event).await?;
                op.commit().await?;
            }
        }
        let outbox = Outbox::init(
            &pool,
            MailboxConfig::builder()
                .commit_lane(CommitLane::Enabled)
                .maybe_backfill_page_size(catch_up.page)
                .build(),
        )
        .await?;
        let mut jobs = job::Jobs::init(
            job::JobSvcConfig::builder()
                .pool(pool.clone())
                .build()
                .unwrap(),
        )
        .await?;
        let mut config = OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE));
        if catch_up.full_batches {
            config = config.with_full_batches_during_catch_up();
        }
        let config = config
            .with_batch_window(window)
            .with_max_batch_size(max)
            .with_checkpoint_interval(Duration::from_millis(10))
            .with_retry_settings(job::RetrySettings {
                backoff: job::Backoff::new(
                    Duration::from_millis(50),
                    Duration::from_millis(100),
                    Duration::from_millis(100),
                    0,
                ),
                ..job::RetrySettings::default()
            });
        let observed = recorder.observed.clone();
        outbox
            .register_singleton_subscriber::<_, L>(&mut jobs, config, recorder)
            .await?;
        Ok(Self {
            pool,
            jobs,
            outbox,
            observed,
        })
    }

    async fn publish(&self, events: impl IntoIterator<Item = TestEvent>) -> anyhow::Result<()> {
        let mut op = self.outbox.begin_op().await?;
        for event in events {
            self.outbox.publish_persisted_in_op(&mut op, event).await?;
        }
        op.commit().await?;
        Ok(())
    }

    async fn checkpoint(&self) -> anyhow::Result<Option<u64>> {
        #[derive(Deserialize)]
        struct State {
            sequence: u64,
        }
        let handle = self
            .jobs
            .resident_handle(job::JobType::new(JOB_TYPE))
            .await?
            .unwrap();
        Ok(handle.execution_state::<State>().await?.map(|s| s.sequence))
    }

    async fn wait_checkpoint(&self, n: u64) -> anyhow::Result<()> {
        let end = Instant::now() + WAIT;
        while self.checkpoint().await? != Some(n) {
            anyhow::ensure!(Instant::now() < end, "checkpoint never reached {n}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    }

    async fn wait_collected(&self, n: usize) -> anyhow::Result<()> {
        let end = Instant::now() + WAIT;
        while self.observed.collected.lock().await.len() < n {
            anyhow::ensure!(Instant::now() < end, "never collected {n} events");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Ok(())
    }

    async fn assert_unlanded(&self) -> anyhow::Result<()> {
        assert!(
            self.checkpoint().await?.is_none(),
            "no acknowledgement before landing"
        );
        let (effects,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM batch_window_effects")
            .fetch_one(&self.pool)
            .await?;
        assert_eq!(effects, 0);
        // A DbOp opened before sleeping would sit idle in transaction here.
        let (open_ops,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database()
             AND pid <> pg_backend_pid() AND state LIKE 'idle in transaction%'",
        )
        .fetch_one(&self.pool)
        .await?;
        assert_eq!(open_ops, 0, "no landing transaction during the window");
        Ok(())
    }
}

#[tokio::test]
#[file_serial]
async fn skip_only_checkpoint_interval_is_unchanged_with_a_window() -> anyhow::Result<()> {
    let observed = Arc::new(Observed::default());
    let window = Duration::from_secs(60);
    let mut h = Harness::new::<InsertOrder>(window, 100, Recorder::new(observed)).await?;
    h.publish([TestEvent::Skip]).await?;
    h.jobs.start_poll().await?;
    h.wait_checkpoint(1).await?;
    assert!(h.observed.collected.lock().await.is_empty());
    assert!(h.observed.attempts.lock().await.is_empty());
    // The checkpoint timeout is shorter than the window: skip-only progress
    // cannot have waited for a nonempty-batch timer or called the flusher.
    h.jobs.shutdown().await?;
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn sparse_arrivals_coalesce_without_resetting_the_deadline() -> anyhow::Result<()> {
    let observed = Arc::new(Observed::default());
    let mut h = Harness::new::<InsertOrder>(WINDOW, 100, Recorder::new(observed)).await?;
    h.jobs.start_poll().await?;
    h.publish([TestEvent::Ping(1)]).await?;
    h.wait_collected(1).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    h.assert_unlanded().await?;
    h.publish([TestEvent::Ping(2)]).await?;
    h.wait_collected(2).await?;
    h.assert_unlanded().await?;
    h.wait_checkpoint(2).await?;
    let attempts = h.observed.attempts.lock().await;
    let collected = h.observed.collected.lock().await;
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].0, vec![1, 2]);
    assert!(attempts[0].1.duration_since(collected[0].1) >= WINDOW);
    // Exact fixed-deadline timing is covered with a paused clock in ctx tests.
    drop(collected);
    drop(attempts);
    h.jobs.shutdown().await?;
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn full_batches_land_without_waiting_out_the_window() -> anyhow::Result<()> {
    let observed = Arc::new(Observed::default());
    let mut h = Harness::new::<InsertOrder>(WINDOW, 2, Recorder::new(observed)).await?;
    h.publish((1..=4).map(TestEvent::Ping)).await?;
    h.jobs.start_poll().await?;
    h.wait_checkpoint(4).await?;
    let attempts = h.observed.attempts.lock().await;
    let collected = h.observed.collected.lock().await;
    assert_eq!(
        attempts.iter().map(|a| a.0.clone()).collect::<Vec<_>>(),
        vec![vec![1, 2], vec![3, 4]]
    );
    // A batch at the ceiling cannot coalesce another event: it lands at its
    // next commit-group boundary instead of sitting out the window.
    assert!(
        attempts[0].1.duration_since(collected[0].1) < WINDOW,
        "a full batch does not wait out the window"
    );
    assert!(
        attempts[1].1.duration_since(collected[2].1) < WINDOW,
        "the next full batch does not wait either"
    );
    drop(collected);
    drop(attempts);
    h.jobs.shutdown().await?;
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn a_partial_tail_batch_still_waits_out_the_window() -> anyhow::Result<()> {
    let observed = Arc::new(Observed::default());
    let mut h = Harness::new::<InsertOrder>(WINDOW, 3, Recorder::new(observed)).await?;
    h.publish((1..=4).map(TestEvent::Ping)).await?;
    h.jobs.start_poll().await?;
    h.wait_checkpoint(4).await?;
    let attempts = h.observed.attempts.lock().await;
    let collected = h.observed.collected.lock().await;
    assert_eq!(
        attempts.iter().map(|a| a.0.clone()).collect::<Vec<_>>(),
        vec![vec![1, 2, 3], vec![4]]
    );
    assert!(
        attempts[0].1.duration_since(collected[0].1) < WINDOW,
        "the full batch landed at its boundary"
    );
    assert!(
        attempts[1].1.duration_since(collected[3].1) >= WINDOW,
        "the partial tail coalesced for the whole window"
    );
    drop(collected);
    drop(attempts);
    h.jobs.shutdown().await?;
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn continuously_ready_input_cannot_starve_the_deadline() -> anyhow::Result<()> {
    let observed = Arc::new(Observed::default());
    let mut recorder = Recorder::new(observed);
    recorder.delay = Duration::from_millis(20);
    let mut h = Harness::new::<InsertOrder>(Duration::from_millis(100), 100, recorder).await?;
    h.publish((1..=20).map(TestEvent::Ping)).await?;
    h.jobs.start_poll().await?;
    h.wait_checkpoint(20).await?;
    let attempts = h.observed.attempts.lock().await;
    assert!(
        attempts.len() > 1,
        "must land before the ready backlog drains"
    );
    assert!(attempts[0].0.len() < 20);
    assert_eq!(
        attempts
            .iter()
            .flat_map(|a| a.0.iter().copied())
            .collect::<Vec<_>>(),
        (1..=20).collect::<Vec<_>>()
    );
    drop(attempts);
    h.jobs.shutdown().await?;
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn deadline_and_size_wait_for_a_commit_boundary_even_when_it_skips() -> anyhow::Result<()> {
    let observed = Arc::new(Observed::default());
    let mut recorder = Recorder::new(observed);
    recorder.delay = Duration::from_millis(30);
    let mut h = Harness::new::<CommitOrder>(Duration::from_millis(40), 2, recorder).await?;
    h.publish([
        TestEvent::Ping(1),
        TestEvent::Ping(2),
        TestEvent::Ping(3),
        TestEvent::Skip,
    ])
    .await?;
    h.publish([TestEvent::Ping(5)]).await?;
    h.jobs.start_poll().await?;
    h.wait_checkpoint(5).await?;
    assert_eq!(
        h.observed
            .attempts
            .lock()
            .await
            .iter()
            .map(|a| a.0.clone())
            .collect::<Vec<_>>(),
        vec![vec![1, 2, 3], vec![5]],
        "deadline/full cannot split the first group or fetch the next one"
    );
    h.jobs.shutdown().await?;
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn shutdown_does_not_bypass_a_nonempty_window() -> anyhow::Result<()> {
    let observed = Arc::new(Observed::default());
    let mut h = Harness::new::<InsertOrder>(WINDOW, 100, Recorder::new(observed)).await?;
    h.publish([TestEvent::Ping(1)]).await?;
    h.jobs.start_poll().await?;
    h.wait_collected(1).await?;
    let jobs = h.jobs.clone();
    let shutdown = tokio::spawn(async move { jobs.shutdown().await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!shutdown.is_finished());
    h.assert_unlanded().await?;
    tokio::time::timeout(WAIT, shutdown).await???;
    assert_eq!(h.checkpoint().await?, Some(1));
    let attempts = h.observed.attempts.lock().await;
    assert_eq!(attempts.len(), 1);
    assert!(
        attempts[0]
            .1
            .duration_since(h.observed.collected.lock().await[0].1)
            >= WINDOW
    );
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn consume_entry_lands_the_prior_batch_without_waiting() -> anyhow::Result<()> {
    let observed = Arc::new(Observed::default());
    let mut recorder = Recorder::new(observed);
    recorder.isolate_on = Some(2);
    let mut h = Harness::new::<InsertOrder>(WINDOW, 100, recorder).await?;
    h.publish([TestEvent::Ping(1), TestEvent::Ping(2)]).await?;
    h.jobs.start_poll().await?;
    h.wait_checkpoint(2).await?;
    let attempts = h.observed.attempts.lock().await;
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].0, vec![1]);
    // The runner reads nothing while the fence lands, so the window could
    // not coalesce another event: the fenced batch lands at once.
    assert!(
        attempts[0]
            .1
            .duration_since(h.observed.collected.lock().await[0].1)
            < WINDOW,
        "a consume fence does not wait out the window"
    );
    drop(attempts);
    h.jobs.shutdown().await?;
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn undecodable_entry_waits_then_checkpoints_only_the_completed_batch() -> anyhow::Result<()> {
    let observed = Arc::new(Observed::default());
    let mut h = Harness::new::<InsertOrder>(WINDOW, 100, Recorder::new(observed)).await?;
    h.publish([TestEvent::Ping(1)]).await?;
    // Publish foreign bytes rather than updating an already cached valid row.
    let foreign = Outbox::<serde_json::Value, TestTables>::init(
        &h.pool,
        MailboxConfig::builder()
            .commit_lane(CommitLane::Enabled)
            .build(),
    )
    .await?;
    let mut op = foreign.begin_op().await?;
    foreign
        .publish_persisted_in_op(&mut op, serde_json::json!({"Unknown": true}))
        .await?;
    op.commit().await?;
    h.jobs.start_poll().await?;
    h.wait_collected(1).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    h.assert_unlanded().await?;
    h.wait_checkpoint(1).await?;
    h.jobs.shutdown().await?;
    assert_eq!(h.checkpoint().await?, Some(1));
    let attempts = h.observed.attempts.lock().await;
    assert_eq!(attempts.len(), 1);
    assert!(
        attempts[0]
            .1
            .duration_since(h.observed.collected.lock().await[0].1)
            >= WINDOW
    );
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn failed_landing_replays_the_whole_batch_with_a_fresh_window() -> anyhow::Result<()> {
    let observed = Arc::new(Observed::default());
    let recorder = Recorder::new(observed);
    recorder.fail_flush_once.store(true, Ordering::SeqCst);
    // A partial batch: the window (not the ceiling) is what delays the
    // landing, so the retry's own fresh window stays under test.
    let mut h = Harness::new::<InsertOrder>(WINDOW, 100, recorder).await?;
    h.publish([TestEvent::Ping(1), TestEvent::Ping(2)]).await?;
    h.jobs.start_poll().await?;
    h.wait_collected(4).await?;
    // The first flush wrote then failed; retry re-collects both events without
    // acknowledging or leaking the rolled-back effects.
    h.assert_unlanded().await?;
    h.wait_checkpoint(2).await?;
    let rows: Vec<(i64,)> = sqlx::query_as("SELECT n FROM batch_window_effects ORDER BY n")
        .fetch_all(&h.pool)
        .await?;
    assert_eq!(rows, vec![(1,), (2,)]);
    let attempts = h.observed.attempts.lock().await;
    let collected = h.observed.collected.lock().await;
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].0, vec![1, 2]);
    assert_eq!(attempts[1].0, vec![1, 2]);
    assert!(attempts[0].1.duration_since(collected[0].1) >= WINDOW);
    assert!(attempts[1].1.duration_since(collected[2].1) >= WINDOW);
    drop(collected);
    drop(attempts);
    h.jobs.shutdown().await?;
    Ok(())
}

async fn attempt_sizes(h: &Harness) -> Vec<usize> {
    h.observed
        .attempts
        .lock()
        .await
        .iter()
        .map(|a| a.0.len())
        .collect()
}

#[tokio::test]
#[file_serial]
async fn catch_up_lands_full_batches_past_the_window() -> anyhow::Result<()> {
    let observed = Arc::new(Observed::default());
    let mut recorder = Recorder::new(observed);
    // Filling the batch takes about 600 ms, four windows.
    recorder.delay = Duration::from_millis(30);
    let mut h = Harness::with_catch_up::<InsertOrder>(
        Duration::from_millis(150),
        20,
        recorder,
        CatchUp {
            full_batches: true,
            before_boot: (1..=20).map(TestEvent::Ping).collect(),
            ..CatchUp::default()
        },
    )
    .await?;
    h.jobs.start_poll().await?;
    h.wait_checkpoint(20).await?;
    let attempts = h.observed.attempts.lock().await;
    assert_eq!(
        attempts.len(),
        1,
        "the window did not cut the catch-up batch"
    );
    assert_eq!(attempts[0].0, (1..=20).collect::<Vec<_>>());
    drop(attempts);
    h.jobs.shutdown().await?;
    Ok(())
}

/// 175 events behind on a fresh boot, read in pages of 10, batches of 50.
async fn replay_from_a_fresh_boot<L: Lane>(full_batches: bool) -> anyhow::Result<Vec<usize>> {
    let observed = Arc::new(Observed::default());
    let mut h = Harness::with_catch_up::<L>(
        Duration::ZERO,
        50,
        Recorder::new(observed),
        CatchUp {
            full_batches,
            page: Some(10),
            before_boot: (1..=175).map(TestEvent::Ping).collect(),
        },
    )
    .await?;
    h.jobs.start_poll().await?;
    h.wait_checkpoint(175).await?;
    let sizes = attempt_sizes(&h).await;
    h.jobs.shutdown().await?;
    Ok(sizes)
}

fn assert_full_batches_while_the_lane_could_fill_them(sizes: &[usize]) {
    assert_eq!(sizes.iter().sum::<usize>(), 175);
    // Once 25 remain the lane cannot fill a batch of 50, so the real-time
    // rules land the tail as reads pause, in one landing or several.
    assert!(
        sizes.len() >= 4 && sizes[..3] == [50, 50, 50],
        "full batches while catching up, got {sizes:?}"
    );
}

#[tokio::test]
#[file_serial]
async fn catch_up_lands_full_batches_with_no_window() -> anyhow::Result<()> {
    let sizes = replay_from_a_fresh_boot::<InsertOrder>(true).await?;
    assert_full_batches_while_the_lane_could_fill_them(&sizes);
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn commit_lane_catch_up_lands_full_batches_from_a_fresh_boot() -> anyhow::Result<()> {
    // The commit lane's own head starts at zero here (no commit checkpoint)
    // and climbs with the fold: only the not-yet-folded insert rows tell the
    // runner the backlog is there.
    let sizes = replay_from_a_fresh_boot::<CommitOrder>(true).await?;
    assert_full_batches_while_the_lane_could_fill_them(&sizes);
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn without_the_switch_a_replay_lands_at_read_pauses() -> anyhow::Result<()> {
    // Control: the real-time default lands partial batches mid-replay.
    let sizes = replay_from_a_fresh_boot::<InsertOrder>(false).await?;
    assert_eq!(sizes.iter().sum::<usize>(), 175);
    assert!(
        sizes[..sizes.len() - 1].iter().any(|&size| size < 50),
        "expected a partial batch before the last, got {sizes:?}"
    );
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn real_time_is_unchanged_when_the_lane_cannot_fill_the_batch() -> anyhow::Result<()> {
    let observed = Arc::new(Observed::default());
    let mut h = Harness::with_catch_up::<InsertOrder>(
        WINDOW,
        100,
        Recorder::new(observed),
        CatchUp {
            full_batches: true,
            ..CatchUp::default()
        },
    )
    .await?;
    h.jobs.start_poll().await?;
    for n in 1..=3 {
        h.publish([TestEvent::Ping(n)]).await?;
    }
    h.wait_checkpoint(3).await?;
    let attempts = h.observed.attempts.lock().await;
    let collected = h.observed.collected.lock().await;
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].0, vec![1, 2, 3]);
    let waited = attempts[0].1.duration_since(collected[0].1);
    assert!(waited >= WINDOW, "the window still applies");
    assert!(waited < WAIT);
    drop(collected);
    drop(attempts);
    h.jobs.shutdown().await?;
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn a_skipping_subscriber_is_not_held_past_a_batch_of_deliveries() -> anyhow::Result<()> {
    let observed = Arc::new(Observed::default());
    let mut before_boot = vec![TestEvent::Ping(1)];
    before_boot.extend(std::iter::repeat_n(TestEvent::Skip, 20));
    let mut h = Harness::with_catch_up::<InsertOrder>(
        Duration::ZERO,
        5,
        Recorder::new(observed),
        CatchUp {
            full_batches: true,
            before_boot,
            ..CatchUp::default()
        },
    )
    .await?;
    h.jobs.start_poll().await?;
    h.wait_checkpoint(21).await?;
    let attempts = h.observed.attempts.lock().await;
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].0, vec![1]);
    drop(attempts);
    h.jobs.shutdown().await?;
    Ok(())
}
