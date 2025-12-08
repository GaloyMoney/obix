use futures::stream::StreamExt;
use obix::MailboxConfig;
use serde::{Deserialize, Serialize};
use serial_test::file_serial;

use obix::{EventSequence, out::Outbox};

#[derive(Debug, Serialize, Deserialize)]
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
#[file_serial]
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

#[tokio::test]
#[file_serial]
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

#[tokio::test]
#[file_serial]
async fn events_are_cached() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let outbox = Outbox::<TestEvent>::init(&pool, Default::default()).await?;

    let mut op = pool.begin().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(0))
        .await?;
    op.commit().await?;

    let mut listener = outbox.listen_persisted(EventSequence::BEGIN);

    let Some(event) = listener.next().await else {
        anyhow::bail!("expected event from listener");
    };
    assert!(matches!(event.payload, Some(TestEvent::Ping(0))));
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn events_not_in_cache_fails() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let config = MailboxConfig {
        event_cache_size: 2,
        event_cache_trim_percent: 50,
        ..Default::default()
    };
    let outbox = Outbox::<TestEvent>::init(&pool, config).await?;

    // Create listener before publish to track when all events are processed
    let mut pre_listener = outbox.listen_persisted(None);

    let mut op = pool.begin().await?;
    outbox
        .publish_all_persisted(&mut op, (0..10).map(TestEvent::Ping))
        .await?;
    op.commit().await?;

    // Wait for all 10 events
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        (&mut pre_listener).take(5).for_each(|_| async {}),
    )
    .await?;

    let mut listener = outbox.listen_persisted(EventSequence::BEGIN);

    // This should fail/timeout because event 1 is no longer in cache
    let result = tokio::time::timeout(std::time::Duration::from_secs(1), listener.next()).await;
    assert!(
        result.is_err(),
        "expected timeout because early events were evicted from cache"
    );

    Ok(())
}

async fn wipeout_table(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    sqlx::query!("TRUNCATE persistent_outbox_events RESTART IDENTITY")
        .execute(pool)
        .await?;
    Ok(())
}
