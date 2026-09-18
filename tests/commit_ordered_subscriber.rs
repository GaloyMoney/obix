//! The singleton runner on the commit-ordered lane: group-safe flushing and
//! cursor handling across restarts.

mod helpers;

use std::sync::Arc;
use std::time::Duration;

use obix::{
    CommitLane, CommitOrder, CommitSequence, EventCtx, EventDelivery, FlushOp, Handled,
    MailboxConfig, OutboxEventJobConfig, SingletonSubscriber, UndecodableDelivery, out::Outbox,
};
use serde::{Deserialize, Serialize};
use serial_test::file_serial;
use tokio::sync::Mutex;

use helpers::{TestTables, init_pool, wipeout_outbox_job_tables, wipeout_outbox_tables};

const JOB_TYPE: &str = "test-commit-ordered-subscriber";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum TestEvent {
    Ping(u64),
}

struct FlushRecorder {
    flushes: Arc<Mutex<Vec<Vec<u64>>>>,
    flush_positions: Arc<Mutex<Vec<CommitSequence>>>,
}

impl FlushRecorder {
    fn new(flushes: Arc<Mutex<Vec<Vec<u64>>>>) -> Self {
        Self {
            flushes,
            flush_positions: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl SingletonSubscriber<TestEvent, CommitOrder> for FlushRecorder {
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
        op: &mut FlushOp<'_, CommitOrder>,
        items: Vec<u64>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if !items.is_empty() {
            self.flush_positions.lock().await.push(op.position());
            self.flushes.lock().await.push(items);
        }
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
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?)
}

/// Publish `count` events inside ONE transaction, so they form one group.
async fn publish_group(
    outbox: &Outbox<TestEvent, TestTables>,
    start: u64,
    count: u64,
) -> anyhow::Result<()> {
    let mut op = outbox.begin_op().await?;
    for n in start..start + count {
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

#[tokio::test]
#[file_serial]
async fn commit_ordering_never_splits_a_group() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = init_outbox(&pool).await?;
    let mut jobs = init_jobs(&pool).await?;

    let flushes = Arc::new(Mutex::new(Vec::new()));

    // Groups of three against a max batch size of two.
    outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE)).with_max_batch_size(2),
            FlushRecorder::new(flushes.clone()),
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    jobs.start_poll().await?;

    for group in 0..4u64 {
        publish_group(&outbox, group * 3, 3).await?;
    }

    until(
        async || flushes.lock().await.iter().flatten().count() >= 12,
        "all 12 events flushed",
    )
    .await?;

    let flushes = flushes.lock().await.clone();
    let delivered: Vec<u64> = flushes.iter().flatten().copied().collect();
    assert_eq!(delivered.len(), 12, "every event delivered exactly once");

    for group in 0..4u64 {
        let members: Vec<u64> = (group * 3..group * 3 + 3).collect();
        let containing: Vec<&Vec<u64>> = flushes
            .iter()
            .filter(|f| members.iter().any(|m| f.contains(m)))
            .collect();
        assert_eq!(
            containing.len(),
            1,
            "group {members:?} was split across flushes {containing:?}",
        );
        for m in &members {
            assert!(
                containing[0].contains(m),
                "group {members:?} incomplete in flush {:?}",
                containing[0],
            );
        }
    }
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn commit_lane_checkpoint_resumes_across_runs() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = init_outbox(&pool).await?;

    let first = Arc::new(Mutex::new(Vec::new()));
    {
        let mut jobs = init_jobs(&pool).await?;
        outbox
            .register_singleton_subscriber(
                &mut jobs,
                OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE)),
                FlushRecorder::new(first.clone()),
            )
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        jobs.start_poll().await?;

        publish_group(&outbox, 0, 3).await?;
        until(
            async || first.lock().await.iter().flatten().count() >= 3,
            "first run delivered its events",
        )
        .await?;
        // The first instance must stop competing for the job before the second
        // registers, or the restart is not a restart.
        let _ = jobs.shutdown().await;
    }

    let second = Arc::new(Mutex::new(Vec::new()));
    let mut jobs = init_jobs(&pool).await?;
    outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE)),
            FlushRecorder::new(second.clone()),
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    jobs.start_poll().await?;

    publish_group(&outbox, 100, 3).await?;
    until(
        async || second.lock().await.iter().flatten().count() >= 3,
        "second run delivered the new group",
    )
    .await?;

    let replayed: Vec<u64> = second.lock().await.iter().flatten().copied().collect();
    assert!(
        replayed.iter().all(|n| *n >= 100),
        "the second run must resume from its commit cursor, not replay: {replayed:?}",
    );
    Ok(())
}

struct UndecodableAcker {
    seen: Arc<Mutex<Vec<u64>>>,
    undecodable: Arc<Mutex<usize>>,
    undecodable_at: Arc<Mutex<Vec<CommitSequence>>>,
}

impl SingletonSubscriber<TestEvent, CommitOrder> for UndecodableAcker {
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
        self.undecodable_at.lock().await.push(error.position());
        *self.undecodable.lock().await += 1;
        Ok(())
    }
}

#[tokio::test]
#[file_serial]
async fn acknowledged_undecodable_advances_the_commit_cursor() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = init_outbox(&pool).await?;

    // A group whose middle member cannot decode into TestEvent.
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(1))
        .await?;
    op.commit().await?;
    sqlx::query(
        "UPDATE persistent_outbox_events SET payload = '{\"NotAVariant\":true}'::jsonb
         WHERE sequence = 1",
    )
    .execute(&pool)
    .await?;

    let undecodable = Arc::new(Mutex::new(0usize));
    let undecodable_at = Arc::new(Mutex::new(Vec::new()));
    {
        let mut jobs = init_jobs(&pool).await?;
        outbox
            .register_singleton_subscriber(
                &mut jobs,
                OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE)),
                UndecodableAcker {
                    seen: Arc::new(Mutex::new(Vec::new())),
                    undecodable: undecodable.clone(),
                    undecodable_at: undecodable_at.clone(),
                },
            )
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        jobs.start_poll().await?;

        until(
            async || *undecodable.lock().await >= 1,
            "the undecodable event was acknowledged",
        )
        .await?;
        let _ = jobs.shutdown().await;
    }

    assert_eq!(
        *undecodable_at.lock().await,
        vec![CommitSequence::from(1u64)],
        "an undecodable delivery must report the position it occupies on its lane",
    );

    // A fresh run against the stored checkpoint must not see it again.
    let after_restart = Arc::new(Mutex::new(0usize));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut jobs = init_jobs(&pool).await?;
    outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE)),
            UndecodableAcker {
                seen: seen.clone(),
                undecodable: after_restart.clone(),
                undecodable_at: Arc::new(Mutex::new(Vec::new())),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    jobs.start_poll().await?;

    // Publish past it, so the restarted run has demonstrably reached the
    // stream rather than merely not started.
    publish_group(&outbox, 100, 1).await?;
    until(
        async || seen.lock().await.contains(&100),
        "the restarted run reached the new event",
    )
    .await?;

    assert_eq!(
        *after_restart.lock().await,
        0,
        "an acknowledged undecodable event must not be redelivered after a restart",
    );
    Ok(())
}
