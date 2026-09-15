//! The singleton runner on the commit-ordered lane: group-safe flushing and
//! the guarded lane switch.

mod helpers;

use std::sync::Arc;
use std::time::Duration;

use obix::{
    EventCtx, FlushOp, Handled, MailboxConfig, Ordering, OutboxEventJobConfig, SingletonSubscriber,
    out::Outbox,
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

/// Collects every event and records each flush as the group of payloads it
/// landed, so a test can assert on batch composition rather than just on
/// delivery.
struct FlushRecorder {
    flushes: Arc<Mutex<Vec<Vec<u64>>>>,
}

impl SingletonSubscriber<TestEvent> for FlushRecorder {
    type Batch = Vec<u64>;

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, Vec<u64>>,
        event: &obix::out::PersistentOutboxEvent<TestEvent>,
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
        _op: &mut FlushOp<'_>,
        items: Vec<u64>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if !items.is_empty() {
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

/// A flush on the commit lane never lands a partial source transaction, even
/// when the group is larger than `max_batch_size`.
#[tokio::test]
#[file_serial]
async fn commit_ordering_never_splits_a_group() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = init_outbox(&pool).await?;
    let mut jobs = init_jobs(&pool).await?;

    let flushes = Arc::new(Mutex::new(Vec::new()));

    // Groups of three against a max batch size of two: the soft limit must
    // give way to the group boundary.
    outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
                .ordering(Ordering::Commit)
                .with_max_batch_size(2),
            FlushRecorder {
                flushes: flushes.clone(),
            },
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

    // Every group of three landed inside a single flush: no flush boundary
    // falls strictly inside {3k, 3k+1, 3k+2}.
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

/// Delivery on the commit lane survives a restart: the second run resumes
/// from the stored commit cursor rather than replaying from the beginning.
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
                OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE)).ordering(Ordering::Commit),
                FlushRecorder {
                    flushes: first.clone(),
                },
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
    }

    // A fresh Jobs instance re-runs the same job type against the stored
    // checkpoint.
    let second = Arc::new(Mutex::new(Vec::new()));
    let mut jobs = init_jobs(&pool).await?;
    outbox
        .register_singleton_subscriber(
            &mut jobs,
            OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE)).ordering(Ordering::Commit),
            FlushRecorder {
                flushes: second.clone(),
            },
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
