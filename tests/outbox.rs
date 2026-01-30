mod helpers;

use futures::stream::StreamExt;
use obix::{EventSequence, MailboxConfig, OutboxEvent, out::OutboxEventMarker};
use serde::{Deserialize, Serialize};
use serial_test::file_serial;

use helpers::{init_outbox, init_pool};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum TestEvent {
    Ping(u64),
    LargePayload(String),
}

// Test the OutboxEvent derive macro
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct PingEvent(u64);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct PongEvent(String);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, OutboxEvent)]
#[serde(tag = "type")]
enum DerivedEvent {
    Ping(PingEvent),
    Pong(PongEvent),
    #[serde(other)]
    Unknown,
}

#[test]
fn outbox_event_derive_generates_marker_impls() {
    // Test From impls
    let ping = PingEvent(42);
    let event: DerivedEvent = ping.clone().into();
    assert_eq!(event, DerivedEvent::Ping(PingEvent(42)));

    let pong = PongEvent("hello".to_string());
    let event: DerivedEvent = pong.clone().into();
    assert_eq!(event, DerivedEvent::Pong(PongEvent("hello".to_string())));

    // Test OutboxEventMarker::as_event
    let event = DerivedEvent::Ping(PingEvent(42));
    assert_eq!(
        <DerivedEvent as OutboxEventMarker<PingEvent>>::as_event(&event),
        Some(&PingEvent(42))
    );
    assert_eq!(
        <DerivedEvent as OutboxEventMarker<PongEvent>>::as_event(&event),
        None
    );

    let event = DerivedEvent::Pong(PongEvent("test".to_string()));
    assert_eq!(
        <DerivedEvent as OutboxEventMarker<PongEvent>>::as_event(&event),
        Some(&PongEvent("test".to_string()))
    );
    assert_eq!(
        <DerivedEvent as OutboxEventMarker<PingEvent>>::as_event(&event),
        None
    );

    // Unknown variant returns None for all
    let event = DerivedEvent::Unknown;
    assert_eq!(
        <DerivedEvent as OutboxEventMarker<PingEvent>>::as_event(&event),
        None
    );
    assert_eq!(
        <DerivedEvent as OutboxEventMarker<PongEvent>>::as_event(&event),
        None
    );
}

#[tokio::test]
#[file_serial]
async fn events_via_short_circuit() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

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

    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

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

    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

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
    let config = MailboxConfig::builder()
        .event_cache_trim_percent(50)
        .event_cache_size(2)
        .build()
        .expect("Couldn't build MailboxConfig");
    let outbox = init_outbox::<TestEvent>(&pool, config).await?;

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
    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;
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

#[tokio::test]
#[file_serial]
async fn ephemeral_events_via_cache() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let mut listener = outbox.listen_ephemeral();

    // Publish an ephemeral event
    let event_type = obix::out::EphemeralEventType::new("test_type");
    outbox
        .publish_ephemeral(event_type.clone(), TestEvent::Ping(42))
        .await?;

    // Should receive the event from the cache
    let Some(event) =
        tokio::time::timeout(std::time::Duration::from_secs(1), listener.next()).await?
    else {
        anyhow::bail!("expected event from listener");
    };
    assert_eq!(event.event_type, event_type);
    assert!(matches!(event.payload, TestEvent::Ping(42)));

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn ephemeral_events_multiple_types() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    // Publish events before creating listener
    let type1 = obix::out::EphemeralEventType::new("type1");
    let type2 = obix::out::EphemeralEventType::new("type2");

    outbox
        .publish_ephemeral(type1.clone(), TestEvent::Ping(1))
        .await?;
    outbox
        .publish_ephemeral(type2.clone(), TestEvent::Ping(2))
        .await?;

    // Give the cache time to process
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Create listener - should receive backfill of current events
    let mut listener = outbox.listen_ephemeral();

    let mut received_events = Vec::new();
    for _ in 0..2 {
        let event = tokio::time::timeout(std::time::Duration::from_secs(1), listener.next())
            .await?
            .expect("should have event");
        received_events.push(event);
    }

    // Should have received both events (order may vary since it's a HashMap)
    assert_eq!(received_events.len(), 2);
    let has_type1 = received_events.iter().any(|e| e.event_type == type1);
    let has_type2 = received_events.iter().any(|e| e.event_type == type2);
    assert!(has_type1, "should have received type1 event");
    assert!(has_type2, "should have received type2 event");

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn ephemeral_events_replace_same_type() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    // Publish events of the same type - later should replace earlier
    let event_type = obix::out::EphemeralEventType::new("replaceable");

    outbox
        .publish_ephemeral(event_type.clone(), TestEvent::Ping(1))
        .await?;
    outbox
        .publish_ephemeral(event_type.clone(), TestEvent::Ping(2))
        .await?;
    outbox
        .publish_ephemeral(event_type.clone(), TestEvent::Ping(3))
        .await?;

    // Give the cache time to process
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Create listener - should only receive the latest event for this type
    let mut listener = outbox.listen_ephemeral();

    let event = tokio::time::timeout(std::time::Duration::from_secs(1), listener.next())
        .await?
        .expect("should have event");

    assert_eq!(event.event_type, event_type);
    assert!(matches!(event.payload, TestEvent::Ping(3)));

    // Should not receive any more events from backfill
    let timeout_result =
        tokio::time::timeout(std::time::Duration::from_millis(200), listener.next()).await;

    assert!(
        timeout_result.is_err(),
        "should not have received additional events from backfill"
    );

    Ok(())
}
