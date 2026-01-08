#![allow(dead_code)]

use obix::{
    Inbox, InboxEventStatus, MailboxConfig,
    inbox::{InboxConfig, InboxEventId},
    out::Outbox,
};

#[derive(obix::MailboxTables)]
pub struct TestTables;

pub async fn init_pool() -> anyhow::Result<sqlx::PgPool> {
    let pg_host = std::env::var("PG_HOST").unwrap_or("localhost".to_string());
    let pg_con = format!("postgres://user:password@{pg_host}:5432/pg");
    let pool = sqlx::PgPool::connect(&pg_con).await?;
    Ok(pool)
}

pub async fn wipeout_inbox_tables(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    sqlx::query!("TRUNCATE inbox_events").execute(pool).await?;

    // Delete child tables first due to foreign key constraints
    // job_events and job_executions reference jobs(id)
    sqlx::query!(
        r#"
        DELETE FROM job_events 
        WHERE id IN (SELECT id FROM jobs WHERE job_type = 'test-inbox')
        "#
    )
    .execute(pool)
    .await?;

    sqlx::query!(
        r#"
        DELETE FROM job_executions 
        WHERE id IN (SELECT id FROM jobs WHERE job_type = 'test-inbox')
        "#
    )
    .execute(pool)
    .await?;

    sqlx::query!("DELETE FROM jobs WHERE job_type = 'test-inbox'")
        .execute(pool)
        .await?;

    Ok(())
}

pub async fn wipeout_outbox_tables(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    sqlx::query!("TRUNCATE persistent_outbox_events RESTART IDENTITY")
        .execute(pool)
        .await?;
    sqlx::query!("TRUNCATE ephemeral_outbox_events")
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn init_inbox<H>(
    pool: &sqlx::PgPool,
    jobs: &mut job::Jobs,
    handler: H,
) -> anyhow::Result<Inbox<TestTables>>
where
    H: obix::inbox::InboxHandler,
{
    wipeout_inbox_tables(pool).await?;
    let inbox = Inbox::<TestTables>::new(
        pool,
        jobs,
        InboxConfig::new(job::JobType::new("test-inbox")),
        handler,
    );
    Ok(inbox)
}

pub async fn init_inbox_with_clock<H>(
    pool: &sqlx::PgPool,
    jobs: &mut job::Jobs,
    handler: H,
    clock: es_entity::clock::ClockHandle,
) -> anyhow::Result<Inbox<TestTables>>
where
    H: obix::inbox::InboxHandler,
{
    wipeout_inbox_tables(pool).await?;
    let inbox_config = InboxConfig::new(job::JobType::new("test-inbox")).with_clock(clock);
    let inbox = Inbox::<TestTables>::new(pool, jobs, inbox_config, handler);
    Ok(inbox)
}

pub async fn init_outbox<P>(
    pool: &sqlx::PgPool,
    config: MailboxConfig,
) -> anyhow::Result<Outbox<P, TestTables>>
where
    P: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + Unpin + 'static,
{
    wipeout_outbox_tables(pool).await?;
    let outbox = Outbox::<P, TestTables>::init(pool, config).await?;
    Ok(outbox)
}

pub async fn wait_for_inbox_status(
    inbox: &Inbox<TestTables>,
    event_id: InboxEventId,
    expected_status: InboxEventStatus,
    timeout: std::time::Duration,
) -> anyhow::Result<()> {
    let start = std::time::Instant::now();
    let poll_interval = std::time::Duration::from_millis(50);

    loop {
        let event = inbox.find_event_by_id(event_id).await?;
        if event.status == expected_status {
            return Ok(());
        }

        if start.elapsed() >= timeout {
            anyhow::bail!(
                "Timeout waiting for event {:?} to reach status {:?}, current status: {:?}",
                event_id,
                expected_status,
                event.status
            );
        }

        tokio::time::sleep(poll_interval).await;
    }
}
