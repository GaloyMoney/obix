//! `SingletonSubscriber::resume_from`: the handler chooses where each run
//! resumes. Every scenario runs on both lanes.

mod helpers;

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use obix::{
    CommitLane, CommitOrder, EventCtx, EventDelivery, FlushOp, Handled, InsertOrder, Lane,
    MailboxConfig, OutboxEventJobConfig, SingletonSubscriber, Subscription, out::Outbox,
};
use serde::{Deserialize, Serialize};
use serial_test::file_serial;
use tokio::sync::Mutex;

use helpers::{TestTables, init_pool, wipeout_outbox_job_tables, wipeout_outbox_tables};

const JOB_TYPE: &str = "test-singleton-resume-from";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum TestEvent {
    Ping(u64),
}

/// What the test handler's `resume_from` answers.
#[derive(Clone, Copy)]
enum Answer {
    Stored,
    Begin,
    At(u64),
    /// `stored` plus this many positions.
    Ahead(u64),
    Refuse,
}

/// Shared between a test and its handler.
struct Control {
    /// Consumed one per call; `fallback` answers once it is empty.
    answers: Mutex<VecDeque<Answer>>,
    fallback: Mutex<Answer>,
    /// The `stored` value of every call, as a bare number.
    stored_log: Mutex<Vec<u64>>,
    /// `(position, n)` of every delivery, in delivery order.
    delivered: Mutex<Vec<(u64, u64)>>,
    /// How many upcoming deliveries fail.
    failures: Mutex<u32>,
}

impl Control {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            answers: Mutex::new(VecDeque::new()),
            fallback: Mutex::new(Answer::Stored),
            stored_log: Mutex::new(Vec::new()),
            delivered: Mutex::new(Vec::new()),
            failures: Mutex::new(0),
        })
    }

    async fn answer_next(&self, answer: Answer) {
        self.answers.lock().await.push_back(answer);
    }

    async fn delivered_numbers(&self) -> Vec<u64> {
        self.delivered.lock().await.iter().map(|d| d.1).collect()
    }

    async fn position_of(&self, n: u64) -> u64 {
        self.delivered
            .lock()
            .await
            .iter()
            .find(|d| d.1 == n)
            .map(|d| d.0)
            .expect("event was delivered")
    }
}

struct Rewinder<L: Lane> {
    control: Arc<Control>,
    _lane: std::marker::PhantomData<L>,
}

impl<L: Lane> SingletonSubscriber<TestEvent, L> for Rewinder<L> {
    type Batch = Vec<u64>;

    async fn resume_from(
        &self,
        stored: L::Position,
    ) -> Result<L::Position, Box<dyn std::error::Error + Send + Sync>> {
        let stored_value = L::position_to_u64(stored);
        self.control.stored_log.lock().await.push(stored_value);
        let answer = match self.control.answers.lock().await.pop_front() {
            Some(answer) => answer,
            None => *self.control.fallback.lock().await,
        };
        match answer {
            Answer::Stored => Ok(stored),
            Answer::Begin => Ok(L::begin()),
            Answer::At(n) => Ok(L::position_from_u64(n)),
            Answer::Ahead(by) => Ok(L::position_from_u64(stored_value + by)),
            Answer::Refuse => Err("resume-from-refused".into()),
        }
    }

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, Vec<u64>>,
        event: &EventDelivery<TestEvent, L>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        let Some(TestEvent::Ping(n)) = &event.payload else {
            return Ok(ctx.skip());
        };
        {
            let mut failures = self.control.failures.lock().await;
            if *failures > 0 {
                *failures -= 1;
                return Err("delivery-failed".into());
            }
        }
        let n = *n;
        self.control
            .delivered
            .lock()
            .await
            .push((L::position_to_u64(event.position()), n));
        Ok(ctx.collect_with(move |batch| batch.push(n)))
    }

    async fn flush(
        &self,
        _op: &mut FlushOp<'_, L>,
        _items: Vec<u64>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

async fn init_jobs(pool: &sqlx::PgPool) -> anyhow::Result<job::Jobs> {
    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    Ok(job::Jobs::init(job_config).await?)
}

async fn init_outbox(pool: &sqlx::PgPool) -> anyhow::Result<Outbox<TestEvent, TestTables>> {
    wipeout_outbox_tables(pool).await?;
    wipeout_outbox_job_tables(pool, JOB_TYPE).await?;
    Ok(Outbox::<TestEvent, TestTables>::init(
        pool,
        MailboxConfig::builder()
            .commit_lane(CommitLane::Enabled)
            .build(),
    )
    .await?)
}

fn config() -> OutboxEventJobConfig {
    OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE)).with_retry_settings(job::RetrySettings {
        backoff: job::Backoff::new(
            Duration::from_millis(50),
            Duration::from_millis(100),
            Duration::from_millis(100),
            0,
        ),
        ..job::RetrySettings::default()
    })
}

/// One job instance: start it, and shut it down before the next registers, or a
/// restart is not a restart.
struct Run<L: Lane> {
    jobs: job::Jobs,
    subscription: Subscription<TestEvent, L, TestTables>,
}

impl<L: Lane> Run<L> {
    async fn start(
        pool: &sqlx::PgPool,
        outbox: &Outbox<TestEvent, TestTables>,
        control: &Arc<Control>,
    ) -> anyhow::Result<Self> {
        let mut jobs = init_jobs(pool).await?;
        let subscription = outbox
            .register_singleton_subscriber(
                &mut jobs,
                config(),
                Rewinder::<L> {
                    control: control.clone(),
                    _lane: std::marker::PhantomData,
                },
            )
            .await?;
        jobs.start_poll().await?;
        Ok(Self {
            jobs,
            subscription,
        })
    }

    async fn checkpoint(&self) -> anyhow::Result<u64> {
        Ok(L::position_to_u64(self.subscription.load().await?.checkpoint()))
    }

    async fn checkpoint_reaches(&self, position: u64) -> anyhow::Result<()> {
        until(
            async || self.checkpoint().await.is_ok_and(|c| c >= position),
            "the checkpoint to reach the position",
        )
        .await
    }

    async fn stop(mut self) {
        let _ = self.jobs.shutdown().await;
    }
}

/// One transaction per event, so every event has a position of its own.
async fn publish(
    outbox: &Outbox<TestEvent, TestTables>,
    range: std::ops::RangeInclusive<u64>,
) -> anyhow::Result<()> {
    for n in range {
        let mut op = outbox.begin_op().await?;
        outbox
            .publish_persisted_in_op(&mut op, TestEvent::Ping(n))
            .await?;
        op.commit().await?;
    }
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

/// Process events 1..=5 on a first run and stop, leaving a checkpoint at 5.
async fn first_run_of_five<L: Lane>(
    pool: &sqlx::PgPool,
    outbox: &Outbox<TestEvent, TestTables>,
    control: &Arc<Control>,
) -> anyhow::Result<u64> {
    let run = Run::<L>::start(pool, outbox, control).await?;
    publish(outbox, 1..=5).await?;
    until(
        async || control.delivered.lock().await.len() >= 5,
        "five deliveries",
    )
    .await?;
    let last = control.position_of(5).await;
    run.checkpoint_reaches(last).await?;
    run.stop().await;
    control.delivered.lock().await.clear();
    control.stored_log.lock().await.clear();
    Ok(last)
}

/// T2 / T3: rewinding to the beginning redelivers everything, in order, and the
/// lane accepts the rewound state on the next start.
async fn rewind_to_beginning<L: Lane>() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = init_outbox(&pool).await?;
    let control = Control::new();
    let last = first_run_of_five::<L>(&pool, &outbox, &control).await?;

    control.answer_next(Answer::Begin).await;
    let run = Run::<L>::start(&pool, &outbox, &control).await?;
    until(
        async || control.delivered.lock().await.len() >= 5,
        "redelivery",
    )
    .await?;
    assert_eq!(control.delivered_numbers().await, vec![1, 2, 3, 4, 5]);
    assert_eq!(control.stored_log.lock().await[0], last);
    run.checkpoint_reaches(last).await?;
    run.stop().await;

    // The next start is on the same lane, with nothing left to redeliver.
    control.delivered.lock().await.clear();
    let run = Run::<L>::start(&pool, &outbox, &control).await?;
    publish(&outbox, 6..=6).await?;
    until(
        async || control.delivered.lock().await.len() >= 1,
        "the new event",
    )
    .await?;
    assert_eq!(control.delivered_numbers().await, vec![6]);
    run.stop().await;
    Ok(())
}

/// T4: rewinding to a middle position redelivers exactly what follows it.
async fn rewind_to_the_middle<L: Lane>() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = init_outbox(&pool).await?;
    let control = Control::new();

    let run = Run::<L>::start(&pool, &outbox, &control).await?;
    publish(&outbox, 1..=5).await?;
    until(
        async || control.delivered.lock().await.len() >= 5,
        "five deliveries",
    )
    .await?;
    let second = control.position_of(2).await;
    run.checkpoint_reaches(control.position_of(5).await).await?;
    run.stop().await;
    control.delivered.lock().await.clear();

    control.answer_next(Answer::At(second)).await;
    let run = Run::<L>::start(&pool, &outbox, &control).await?;
    until(
        async || control.delivered.lock().await.len() >= 3,
        "redelivery",
    )
    .await?;
    // Settle: nothing beyond the three may follow.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(control.delivered_numbers().await, vec![3, 4, 5]);
    run.stop().await;
    Ok(())
}

/// T5: a position after the checkpoint fails the run and moves nothing.
async fn forward_is_refused<L: Lane>() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = init_outbox(&pool).await?;
    let control = Control::new();
    let last = first_run_of_five::<L>(&pool, &outbox, &control).await?;

    *control.fallback.lock().await = Answer::Ahead(10);
    let run = Run::<L>::start(&pool, &outbox, &control).await?;
    publish(&outbox, 6..=6).await?;
    until(
        async || {
            run.subscription
                .load()
                .await
                .is_ok_and(|s| s.last_error().is_some())
        },
        "the run to fail",
    )
    .await?;

    let snapshot = run.subscription.load().await?;
    assert!(
        snapshot
            .last_error()
            .is_some_and(|e| e.contains("resume_from may only rewind")),
        "unexpected error: {:?}",
        snapshot.last_error()
    );
    assert_eq!(L::position_to_u64(snapshot.checkpoint()), last);
    assert!(control.delivered.lock().await.is_empty());
    run.stop().await;
    Ok(())
}

/// T6: an `Err` fails the run with the checkpoint unchanged; the retry
/// proceeds and redelivers nothing.
async fn hook_error_is_retried<L: Lane>() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = init_outbox(&pool).await?;
    let control = Control::new();
    let last = first_run_of_five::<L>(&pool, &outbox, &control).await?;

    control.answer_next(Answer::Refuse).await;
    let run = Run::<L>::start(&pool, &outbox, &control).await?;
    publish(&outbox, 6..=6).await?;
    until(
        async || control.delivered.lock().await.len() >= 1,
        "the retry to deliver",
    )
    .await?;
    assert_eq!(control.delivered_numbers().await, vec![6]);
    let log = control.stored_log.lock().await.clone();
    assert_eq!(log[..2], [last, last], "the checkpoint was untouched");
    run.stop().await;
    Ok(())
}

/// T7: the rewind is committed before the first delivery, so a run that fails
/// on its first event asks the hook again from the beginning.
async fn rewind_is_committed_before_delivery<L: Lane>() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = init_outbox(&pool).await?;
    let control = Control::new();
    let last = first_run_of_five::<L>(&pool, &outbox, &control).await?;

    control.answer_next(Answer::Begin).await;
    *control.failures.lock().await = 1;
    let run = Run::<L>::start(&pool, &outbox, &control).await?;
    until(
        async || control.delivered.lock().await.len() >= 5,
        "redelivery after the retry",
    )
    .await?;
    let log = control.stored_log.lock().await.clone();
    assert_eq!(log[0], last);
    assert_eq!(
        log[1],
        L::position_to_u64(L::begin()),
        "the retry must see the rewound checkpoint"
    );
    run.stop().await;
    Ok(())
}

/// T8: the public read reflects the rewind before any event is flushed.
async fn rewind_is_visible_to_load<L: Lane>() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = init_outbox(&pool).await?;
    let control = Control::new();

    let run = Run::<L>::start(&pool, &outbox, &control).await?;
    publish(&outbox, 1..=5).await?;
    until(
        async || control.delivered.lock().await.len() >= 5,
        "five deliveries",
    )
    .await?;
    let second = control.position_of(2).await;
    run.checkpoint_reaches(control.position_of(5).await).await?;
    run.stop().await;

    control.answer_next(Answer::At(second)).await;
    *control.failures.lock().await = u32::MAX;
    let run = Run::<L>::start(&pool, &outbox, &control).await?;
    until(
        async || run.checkpoint().await.is_ok_and(|c| c == second),
        "the checkpoint to read the rewound position",
    )
    .await?;
    run.stop().await;
    Ok(())
}

/// T9: a subscriber that never checkpointed is handed the beginning.
async fn fresh_subscriber_gets_the_beginning<L: Lane>() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = init_outbox(&pool).await?;
    let control = Control::new();

    control.answer_next(Answer::Begin).await;
    let run = Run::<L>::start(&pool, &outbox, &control).await?;
    publish(&outbox, 1..=3).await?;
    until(
        async || control.delivered.lock().await.len() >= 3,
        "three deliveries",
    )
    .await?;
    assert_eq!(control.delivered_numbers().await, vec![1, 2, 3]);
    assert_eq!(
        control.stored_log.lock().await[0],
        L::position_to_u64(L::begin())
    );
    run.stop().await;
    Ok(())
}

/// T10: the hook runs once per run, not once per process.
async fn hook_runs_once_per_run<L: Lane>() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = init_outbox(&pool).await?;
    let control = Control::new();
    let last = first_run_of_five::<L>(&pool, &outbox, &control).await?;

    let run = Run::<L>::start(&pool, &outbox, &control).await?;
    publish(&outbox, 6..=6).await?;
    until(
        async || control.delivered.lock().await.len() >= 1,
        "the new event",
    )
    .await?;
    assert_eq!(*control.stored_log.lock().await, vec![last]);
    run.stop().await;
    Ok(())
}

macro_rules! on_both_lanes {
    ($($scenario:ident => $insert:ident, $commit:ident;)*) => {$(
        #[tokio::test]
        #[file_serial]
        async fn $insert() -> anyhow::Result<()> {
            $scenario::<InsertOrder>().await
        }

        #[tokio::test]
        #[file_serial]
        async fn $commit() -> anyhow::Result<()> {
            $scenario::<CommitOrder>().await
        }
    )*};
}

on_both_lanes! {
    rewind_to_beginning => insert_lane_rewinds_to_the_beginning, commit_lane_rewinds_to_the_beginning;
    rewind_to_the_middle => insert_lane_rewinds_to_the_middle, commit_lane_rewinds_to_the_middle;
    forward_is_refused => insert_lane_refuses_a_forward_resume, commit_lane_refuses_a_forward_resume;
    hook_error_is_retried => insert_lane_retries_a_failed_hook, commit_lane_retries_a_failed_hook;
    rewind_is_committed_before_delivery => insert_lane_commits_the_rewind_first, commit_lane_commits_the_rewind_first;
    rewind_is_visible_to_load => insert_lane_rewind_is_visible_to_load, commit_lane_rewind_is_visible_to_load;
    fresh_subscriber_gets_the_beginning => insert_lane_fresh_subscriber_gets_the_beginning, commit_lane_fresh_subscriber_gets_the_beginning;
    hook_runs_once_per_run => insert_lane_calls_the_hook_once_per_run, commit_lane_calls_the_hook_once_per_run;
}
