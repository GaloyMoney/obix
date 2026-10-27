mod helpers;

use std::str::FromStr;
use std::time::Duration;

use futures::stream::StreamExt;
use obix::{EphemeralConfig, EphemeralEventType, MailboxConfig};
use serde::{Deserialize, Serialize};
use serial_test::file_serial;

use helpers::{init_ephemeral, init_outbox, init_pool};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum DurableEvent {
    Saved(u64),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum FleetingEvent {
    Tick(u64),
}

/// A pool whose connections are attributable to this test run alone, so
/// LISTEN connections can be counted without other tests perturbing the number.
async fn init_named_pool() -> anyhow::Result<(sqlx::PgPool, String)> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let name = format!("obix-ephemeral-test-{}-{nanos}", std::process::id());
    let options = sqlx::postgres::PgConnectOptions::from_str(&std::env::var("PG_CON")?)?
        .application_name(&name);
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_with(options)
        .await?;
    Ok((pool, name))
}

async fn listen_connections(
    observer: &sqlx::PgPool,
    application_name: &str,
) -> anyhow::Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1 AND query LIKE 'LISTEN%'",
    )
    .bind(application_name)
    .fetch_one(observer)
    .await?)
}

async fn row_count(pool: &sqlx::PgPool, table: &str) -> anyhow::Result<i64> {
    Ok(sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
        .fetch_one(pool)
        .await?)
}

#[tokio::test]
#[file_serial]
async fn the_two_handles_deliver_their_own_payload_types_independently() -> anyhow::Result<()> {
    let observer = init_pool().await?;
    let (pool, application_name) = init_named_pool().await?;

    let outbox = init_outbox::<DurableEvent>(&pool, MailboxConfig::builder().build()).await?;
    assert_eq!(
        listen_connections(&observer, &application_name).await?,
        1,
        "a persistent-only process opens exactly one LISTEN connection"
    );

    let ephemeral =
        init_ephemeral::<FleetingEvent>(&pool, EphemeralConfig::builder().build()).await?;
    assert_eq!(
        listen_connections(&observer, &application_name).await?,
        2,
        "a process holding both handles opens two LISTEN connections"
    );

    let mut persistent_listener = outbox.listen_persisted(None);
    let mut ephemeral_listener = ephemeral.listen();

    let event_type = EphemeralEventType::new("tick");
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, DurableEvent::Saved(1))
        .await?;
    op.commit().await?;
    ephemeral
        .publish(event_type.clone(), FleetingEvent::Tick(2))
        .await?;

    let persisted = tokio::time::timeout(Duration::from_secs(5), persistent_listener.next())
        .await?
        .expect("persistent stream is open")?;
    assert_eq!(persisted.payload, Some(DurableEvent::Saved(1)));

    let ephemeral_event = tokio::time::timeout(Duration::from_secs(5), ephemeral_listener.next())
        .await?
        .expect("ephemeral stream is open");
    assert_eq!(ephemeral_event.event_type, event_type);
    assert_eq!(ephemeral_event.payload, FleetingEvent::Tick(2));

    assert!(
        tokio::time::timeout(Duration::from_millis(500), persistent_listener.next())
            .await
            .is_err(),
        "the persistent listener must not see the ephemeral event"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(500), ephemeral_listener.next())
            .await
            .is_err(),
        "the ephemeral listener must not see the persistent event"
    );

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn publish_in_op_shares_the_persistent_ops_fate() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let outbox = init_outbox::<DurableEvent>(&pool, MailboxConfig::builder().build()).await?;
    let ephemeral =
        init_ephemeral::<FleetingEvent>(&pool, EphemeralConfig::builder().build()).await?;

    let mut persistent_listener = outbox.listen_persisted(None);
    let mut ephemeral_listener = ephemeral.listen();

    let first = EphemeralEventType::new("first");
    let second = EphemeralEventType::new("second");

    let mut rolled_back = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut rolled_back, DurableEvent::Saved(1))
        .await?;
    ephemeral
        .publish_in_op(&mut rolled_back, first.clone(), FleetingEvent::Tick(1))
        .await?;
    ephemeral
        .publish_in_op(&mut rolled_back, second.clone(), FleetingEvent::Tick(2))
        .await?;
    drop(rolled_back);

    assert!(
        tokio::time::timeout(Duration::from_millis(500), persistent_listener.next())
            .await
            .is_err(),
        "a rolled-back op must publish no persistent event"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(500), ephemeral_listener.next())
            .await
            .is_err(),
        "a rolled-back op must publish no ephemeral event"
    );
    assert_eq!(row_count(&pool, "persistent_outbox_events").await?, 0);
    assert_eq!(row_count(&pool, "ephemeral_outbox_events").await?, 0);

    // Two ephemeral publishes in one op: the second merges into the first's
    // pending commit hook.
    let mut committed = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut committed, DurableEvent::Saved(3))
        .await?;
    ephemeral
        .publish_in_op(&mut committed, first.clone(), FleetingEvent::Tick(10))
        .await?;
    ephemeral
        .publish_in_op(&mut committed, second.clone(), FleetingEvent::Tick(20))
        .await?;
    committed.commit().await?;

    let persisted = tokio::time::timeout(Duration::from_secs(5), persistent_listener.next())
        .await?
        .expect("persistent stream is open")?;
    assert_eq!(persisted.payload, Some(DurableEvent::Saved(3)));

    let mut received = Vec::new();
    for _ in 0..2 {
        let event = tokio::time::timeout(Duration::from_secs(5), ephemeral_listener.next())
            .await?
            .expect("ephemeral stream is open");
        received.push((event.event_type.clone(), event.payload.clone()));
    }
    received.sort_by_key(|(event_type, _)| event_type.to_string());
    assert_eq!(
        received,
        vec![
            (first, FleetingEvent::Tick(10)),
            (second, FleetingEvent::Tick(20)),
        ]
    );
    assert_eq!(row_count(&pool, "persistent_outbox_events").await?, 1);
    assert_eq!(row_count(&pool, "ephemeral_outbox_events").await?, 2);

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn two_ephemeral_handles_each_hear_an_externally_written_row() -> anyhow::Result<()> {
    let observer = init_pool().await?;
    let (pool, application_name) = init_named_pool().await?;

    let first = init_ephemeral::<FleetingEvent>(&pool, EphemeralConfig::builder().build()).await?;
    assert_eq!(listen_connections(&observer, &application_name).await?, 1);
    let second = init_ephemeral::<FleetingEvent>(&pool, EphemeralConfig::builder().build()).await?;
    assert_eq!(
        listen_connections(&observer, &application_name).await?,
        2,
        "each EphemeralOutbox holds its own LISTEN connection"
    );

    let mut first_listener = first.listen();
    let mut second_listener = second.listen();

    sqlx::query("INSERT INTO ephemeral_outbox_events (event_type, payload) VALUES ($1, $2)")
        .bind("external")
        .bind(serde_json::json!({"Tick": 5}))
        .execute(&observer)
        .await?;

    for listener in [&mut first_listener, &mut second_listener] {
        let event = tokio::time::timeout(Duration::from_secs(5), listener.next())
            .await?
            .expect("ephemeral stream is open");
        assert_eq!(event.event_type.as_str(), "external");
        assert_eq!(event.payload, FleetingEvent::Tick(5));
    }

    Ok(())
}
