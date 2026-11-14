use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use es_entity::{DbOp, clock::ClockHandle};
use obix::{
    CommitLane, CommitOrder, EventCtx, EventDelivery, FlushOp, Handled, MailboxConfig,
    OutboxEventJobConfig, SingletonSubscriber, out::Outbox,
};
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use tokio::sync::{Mutex, Semaphore};

#[derive(Debug, Serialize, Deserialize)]
struct Event {
    number: u64,
}
#[derive(Clone, Copy)]
enum FirstEvent {
    Collect,
    Skip,
    Consume,
}

struct Recorder {
    first: FirstEvent,
    flushes: Arc<Mutex<Vec<Vec<u64>>>>,
    entered: Arc<AtomicBool>,
    release: Option<Arc<Semaphore>>,
}
impl SingletonSubscriber<Event, CommitOrder> for Recorder {
    type Batch = Vec<u64>;
    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, Self::Batch>,
        event: &EventDelivery<Event, CommitOrder>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        let Some(event) = &event.payload else {
            return Ok(ctx.skip());
        };
        if event.number == 1 {
            self.entered.store(true, Ordering::SeqCst);
            if let Some(release) = &self.release {
                release.acquire().await?.forget();
            }
        }
        if event.number < 4 {
            // Let the pause future become ready while an admitted handler
            // is still inside a source group with an empty accumulator.
            if !matches!(self.first, FirstEvent::Collect) {
                tokio::time::sleep(Duration::from_millis(350)).await;
            }
            match self.first {
                FirstEvent::Skip => return Ok(ctx.skip()),
                FirstEvent::Consume => return Ok(ctx.consume().await?.commit()),
                FirstEvent::Collect => {}
            }
        }
        let number = event.number;
        Ok(ctx.collect_with(move |batch| batch.push(number)))
    }
    async fn flush(
        &self,
        _: &mut FlushOp<'_, CommitOrder>,
        items: Self::Batch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if !items.is_empty() {
            self.flushes.lock().await.push(items);
        }
        Ok(())
    }
}

async fn publish(outbox: &Outbox<Event>, start: u64) -> anyhow::Result<()> {
    let mut op = outbox.begin_op().await?;
    for number in start..start + 4 {
        outbox
            .publish_persisted_in_op(&mut op, Event { number })
            .await?;
    }
    op.commit().await?;
    Ok(())
}

async fn pause_scenario(first: FirstEvent) -> anyhow::Result<()> {
    let options: PgConnectOptions = std::env::var("PG_CON")?.parse()?;
    let admin = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options.clone())
        .await?;
    let database = format!(
        "obix_quiescence_{}",
        job::JobId::new().to_string().replace('-', "")
    );
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE {database}")))
        .execute(&admin)
        .await?;
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect_with(options.database(&database))
        .await?;
    {
        let (clock, _controller) = ClockHandle::manual();
        let mut jobs = job::Jobs::init(
            job::JobSvcConfig::builder()
                .pool(pool.clone())
                .clock(clock.clone())
                .exec_migrations(true)
                .execution_control(true)
                .build()?,
        )
        .await?;
        sqlx::raw_sql(include_str!("../migrations/20251204130225_obix_setup.sql"))
            .execute(&pool)
            .await?;
        let outbox = Outbox::<Event>::init(
            &pool,
            MailboxConfig::builder()
                .clock(clock.clone())
                .commit_lane(CommitLane::Enabled)
                .build(),
        )
        .await?;
        let flushes = Arc::new(Mutex::new(Vec::new()));
        let entered = Arc::new(AtomicBool::new(false));
        let release = Arc::new(Semaphore::new(0));
        let business = outbox
            .register_singleton_subscriber(
                &mut jobs,
                OutboxEventJobConfig::new(job::JobType::new("business")).with_max_batch_size(1),
                Recorder {
                    first,
                    flushes: flushes.clone(),
                    entered: entered.clone(),
                    release: Some(release.clone()),
                },
            )
            .await?;
        let relay = outbox
            .register_singleton_subscriber(
                &mut jobs,
                OutboxEventJobConfig::new(job::JobType::new("relay")).with_max_batch_size(1),
                Recorder {
                    first: FirstEvent::Collect,
                    flushes: Arc::new(Mutex::new(Vec::new())),
                    entered: Arc::new(AtomicBool::new(false)),
                    release: None,
                },
            )
            .await?;
        jobs.start_poll().await?;
        publish(&outbox, 1).await?;
        tokio::time::timeout(Duration::from_secs(10), async {
            while !entered.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        let control = jobs.execution_control().unwrap();
        let mut op = DbOp::init_with_clock(&pool, &clock).await?;
        control
            .pause_in_op(&mut op, &[job::JobType::new("relay")])
            .await?;
        op.commit().await?;
        assert!(!control.status().await?.is_quiesced());
        release.add_permits(1);
        tokio::time::timeout(Duration::from_secs(10), async {
            while !control.status().await.unwrap().is_quiesced() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await?;
        let first_batch = match first {
            FirstEvent::Collect => vec![1, 2, 3, 4],
            FirstEvent::Skip | FirstEvent::Consume => vec![4],
        };
        assert_eq!(*flushes.lock().await, vec![first_batch.clone()]);
        assert_eq!(u64::from(business.load().await?.checkpoint()), 4);
        assert!(matches!(
            business.load().await?.job_status(),
            job::JobStatus::Pending {
                attempt: 1,
                last_error: None,
                ..
            }
        ));

        publish(&outbox, 5).await?;
        relay.await_caught_up(Duration::from_secs(10)).await?;
        assert_eq!(u64::from(relay.load().await?.checkpoint()), 8);
        assert_eq!(u64::from(business.load().await?.checkpoint()), 4);
        assert!(control.status().await?.is_quiesced());
        let mut op = DbOp::init_with_clock(&pool, &clock).await?;
        control.resume_in_op(&mut op).await?;
        op.commit().await?;
        business.await_caught_up(Duration::from_secs(10)).await?;
        assert_eq!(*flushes.lock().await, vec![first_batch, vec![5, 6, 7, 8]]);

        // An idle stream must also yield; no new event is needed to wake it.
        let mut op = DbOp::init_with_clock(&pool, &clock).await?;
        control
            .pause_in_op(&mut op, &[job::JobType::new("relay")])
            .await?;
        op.commit().await?;
        tokio::time::timeout(Duration::from_secs(10), async {
            while !control.status().await.unwrap().is_quiesced() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await?;
        let mut op = DbOp::init_with_clock(&pool, &clock).await?;
        control.resume_in_op(&mut op).await?;
        op.commit().await?;
        jobs.shutdown().await?;
    }
    pool.close().await;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP DATABASE {database} WITH (FORCE)"
    )))
    .execute(&admin)
    .await?;
    admin.close().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_finishes_a_whole_group_checkpoints_and_keeps_the_relay_running() -> anyhow::Result<()>
{
    pause_scenario(FirstEvent::Collect).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_finishes_an_open_group_after_a_skipped_event() -> anyhow::Result<()> {
    pause_scenario(FirstEvent::Skip).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_finishes_an_open_group_after_a_consumed_event() -> anyhow::Result<()> {
    pause_scenario(FirstEvent::Consume).await
}
