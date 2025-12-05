use serde::{Deserialize, Serialize};

use obix::out::Outbox;

#[derive(obix::MailboxTables)]
struct TestTables;

#[derive(Serialize, Deserialize)]
enum TestEvent {
    Ping,
}

pub async fn init_pool() -> anyhow::Result<sqlx::PgPool> {
    let pg_host = std::env::var("PG_HOST").unwrap_or("localhost".to_string());
    let pg_con = format!("postgres://user:password@{pg_host}:5432/pg");
    let pool = sqlx::PgPool::connect(&pg_con).await?;
    Ok(pool)
}

#[tokio::test]
async fn mailbox() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let _outbox = Outbox::<TestEvent, TestTables>::init(&pool, Default::default()).await?;
    Ok(())
}
