use futures::stream::StreamExt;
use serde::{Deserialize, Serialize};

use obix::out::Outbox;

#[derive(Serialize, Deserialize)]
enum TestEvent {
    Ping(u64),
}

pub async fn init_pool() -> anyhow::Result<sqlx::PgPool> {
    let pg_host = std::env::var("PG_HOST").unwrap_or("localhost".to_string());
    let pg_con = format!("postgres://user:password@{pg_host}:5432/pg");
    let pool = sqlx::PgPool::connect(&pg_con).await?;
    wipeout_table(&pool).await?;
    Ok(pool)
}

#[tokio::test]
async fn events_are_short_circuited() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let outbox = Outbox::<TestEvent>::init(&pool, Default::default()).await?;
    let mut listener = outbox.listen_persisted(None);

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(0))
        .await?;
    op.commit().await?;

    let Some(event) = listener.next().await else {
        anyhow::bail!("expected event from listener");
    };
    assert!(matches!(event.payload, Some(TestEvent::Ping(0))));
    Ok(())
}

// #[tokio::test]
async fn events_are_listened() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let outbox = Outbox::<TestEvent>::init(&pool, Default::default()).await?;
    let mut listener = outbox.listen_persisted(None);

    let mut op = pool.begin().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(0))
        .await?;
    op.commit().await?;

    let Some(event) = listener.next().await else {
        anyhow::bail!("expected event from listener");
    };
    assert!(matches!(event.payload, Some(TestEvent::Ping(0))));
    Ok(())
}

async fn wipeout_table(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    sqlx::query!("TRUNCATE persistent_outbox_events RESTART IDENTITY")
        .execute(pool)
        .await?;
    Ok(())
}
