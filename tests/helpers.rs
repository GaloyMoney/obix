#![allow(dead_code)]

use obix::{Inbox, MailboxConfig, inbox::InboxConfig, out::Outbox};

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

pub async fn init_inbox<P, H>(
    pool: &sqlx::PgPool,
    jobs: &mut job::Jobs,
    handler: H,
) -> anyhow::Result<Inbox<P, TestTables>>
where
    P: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + Unpin + 'static,
    H: obix::inbox::InboxHandler<P>,
{
    wipeout_inbox_tables(pool).await?;
    let inbox = Inbox::<P, TestTables>::new(
        pool,
        jobs,
        InboxConfig::new(job::JobType::new("test-inbox")),
        handler,
    );
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
