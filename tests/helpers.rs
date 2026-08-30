#![allow(dead_code)]

use obix::{
    Inbox, InboxEventStatus, MailboxConfig,
    inbox::{InboxConfig, InboxEventId},
    out::Outbox,
};

#[derive(obix::MailboxTables)]
pub struct TestTables;

pub async fn init_pool() -> anyhow::Result<sqlx::PgPool> {
    let pg_con = std::env::var("PG_CON").unwrap();
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

/// Wipe every subscription row for one keyed subscriber type.
pub async fn wipeout_subscriptions(
    pool: &sqlx::PgPool,
    subscriber_type: &str,
) -> anyhow::Result<()> {
    sqlx::query("DELETE FROM subscriptions WHERE subscriber_type = $1")
        .bind(subscriber_type)
        .execute(pool)
        .await?;
    Ok(())
}

/// The waker's job type: one per outbox rather than one per subscriber
/// type, derived from the persistent table name.
pub const KEYED_WAKER_JOB_TYPE: &str = "persistent_outbox_events.keyed-waker";

/// [`wipeout_outbox_job_tables`] for every job type a keyed subscriber
/// registration touches: the per-key job type itself, its derived
/// `{job_type}.sweep`, and the outbox-wide waker.
///
/// The waker must be wiped along with the outbox tables it tracks — it is
/// shared across subscriber types and holds a durable checkpoint, so a
/// surviving one would sit past the sequences a truncated stream reissues
/// and silently stop waking anything.
pub async fn wipeout_keyed_subscriber_job_tables(
    pool: &sqlx::PgPool,
    job_type: &str,
) -> anyhow::Result<()> {
    wipeout_outbox_job_tables(pool, job_type).await?;
    wipeout_outbox_job_tables(pool, &format!("{job_type}.sweep")).await?;
    wipeout_outbox_job_tables(pool, KEYED_WAKER_JOB_TYPE).await?;
    Ok(())
}

pub async fn wipeout_outbox_job_tables(pool: &sqlx::PgPool, job_type: &str) -> anyhow::Result<()> {
    sqlx::query(&format!(
        "DELETE FROM job_events WHERE id IN (SELECT id FROM jobs WHERE job_type = '{job_type}')"
    ))
    .execute(pool)
    .await?;

    sqlx::query(&format!(
        "DELETE FROM job_executions WHERE id IN (SELECT id FROM jobs WHERE job_type = '{job_type}')"
    ))
    .execute(pool)
    .await?;

    sqlx::query(&format!("DELETE FROM jobs WHERE job_type = '{job_type}'"))
        .execute(pool)
        .await?;

    Ok(())
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
