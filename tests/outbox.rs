use futures::stream::StreamExt;
use obix::MailboxConfig;
use serde::{Deserialize, Serialize};
use serial_test::file_serial;

use obix::{EventSequence, out::Outbox};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum TestEvent {
    Ping(u64),
    LargePayload(String),
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
async fn events_via_short_circuit() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let outbox = Outbox::<TestEvent>::init(&pool, MailboxConfig::default()).await?;
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
async fn events_via_pg_notify() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let outbox = Outbox::<TestEvent>::init(&pool, MailboxConfig::default()).await?;
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
async fn events_via_cache() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let outbox = Outbox::<TestEvent>::init(&pool, MailboxConfig::default()).await?;
    let mut pre_listener = outbox.listen_persisted(None);

    let mut op = pool.begin().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(0))
        .await?;
    op.commit().await?;
    pre_listener.next().await.expect("event was cached");

    let mut listener = outbox.listen_persisted(EventSequence::BEGIN);

    let Some(event) =
        tokio::time::timeout(std::time::Duration::from_secs(1), listener.next()).await?
    else {
        anyhow::bail!("expected event from listener");
    };
    assert!(matches!(event.payload, Some(TestEvent::Ping(0))));

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn events_not_in_cache_backfilled_from_pg() -> anyhow::Result<()> {
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

    // This should now work because backfill will fetch from PG even if events are not in cache
    let mut events = Vec::new();
    for _ in 0..10 {
        let event = tokio::time::timeout(std::time::Duration::from_secs(1), listener.next())
            .await
            .expect("should receive event via PG backfill")
            .expect("should have event");
        events.push(event);
    }

    // Verify we got all 10 events in order
    for (i, event) in events.iter().enumerate() {
        assert!(matches!(event.payload, Some(TestEvent::Ping(n)) if n == i as u64));
    }

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn large_payload_via_pg_notify_fetches_from_db() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = Outbox::<TestEvent>::init(&pool, MailboxConfig::default()).await?;
    let mut listener = outbox.listen_persisted(None);

    let large_string = "x".repeat(10_000);

    let expected_events = vec![
        TestEvent::Ping(0),
        TestEvent::LargePayload(large_string.clone()),
        TestEvent::Ping(1),
        TestEvent::Ping(2),
        TestEvent::LargePayload(format!("y{}", "y".repeat(9_999))),
        TestEvent::Ping(3),
        TestEvent::LargePayload(large_string.clone()),
        TestEvent::Ping(4),
    ];

    let mut op = pool.begin().await?;
    for event in &expected_events {
        outbox
            .publish_persisted_in_op(&mut op, event.clone())
            .await?;
    }
    op.commit().await?;

    let mut received_events = Vec::new();
    for i in 0..expected_events.len() {
        let event = tokio::time::timeout(std::time::Duration::from_secs(2), listener.next())
            .await
            .unwrap_or_else(|_| panic!("timeout waiting for event {}", i))
            .unwrap_or_else(|| panic!("expected event {} but got None", i));
        received_events.push(event);
    }

    for (i, (received, expected)) in received_events.iter().zip(&expected_events).enumerate() {
        let payload = received
            .payload
            .as_ref()
            .unwrap_or_else(|| panic!("event {} payload should not be None", i));

        assert_eq!(
            payload, expected,
            "event {} should match expected payload",
            i
        );
        if let TestEvent::LargePayload(s) = payload {
            assert!(
                s.len() >= 10_000,
                "event {} large payload should be complete, got {} bytes",
                i,
                s.len()
            );
        }
    }

    Ok(())
}

async fn wipeout_table(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    sqlx::query!("TRUNCATE persistent_outbox_events RESTART IDENTITY")
        .execute(pool)
        .await?;
    Ok(())
}
