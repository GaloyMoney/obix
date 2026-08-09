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
    let event = event?;
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
    let event = event?;
    assert!(matches!(event.payload, Some(TestEvent::Ping(0))));
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn event_batch_via_pg_notify() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let mut listener = outbox.listen_persisted(None);

    // A bare transaction has no commit hooks, so nothing reaches the cache
    // via the in-process broadcast: delivery depends entirely on the single
    // {min_sequence, max_sequence} NOTIFY emitted by the insert statement
    // and the SELECT-only range fetch it triggers.
    let mut op = pool.begin().await?;
    outbox
        .publish_all_persisted(&mut op, (0..5).map(TestEvent::Ping))
        .await?;
    op.commit().await?;

    for i in 0..5 {
        let Some(event) =
            tokio::time::timeout(std::time::Duration::from_secs(5), listener.next()).await?
        else {
            anyhow::bail!("expected event {i} from listener");
        };
        let event = event?;
        assert!(matches!(event.payload, Some(TestEvent::Ping(n)) if n == i));
    }
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
    pre_listener
        .next()
        .await
        .expect("event was cached")
        .expect("undecodable event");

    let mut listener = outbox.listen_persisted(EventSequence::BEGIN);

    let Some(event) =
        tokio::time::timeout(std::time::Duration::from_secs(1), listener.next()).await?
    else {
        anyhow::bail!("expected event from listener");
    };
    let event = event?;
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
            .expect("should have event")?;
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
            .unwrap_or_else(|| panic!("expected event {} but got None", i))
            .unwrap_or_else(|e| panic!("undecodable event {}: {}", i, e));
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
async fn large_batch_persisted_in_bounded_chunks() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let batch_size = 5;
    let total = 23;

    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .persist_events_batch_size(batch_size)
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_all_persisted(&mut op, (0..10).map(TestEvent::Ping))
        .await?;
    outbox
        .publish_all_persisted(&mut op, (10..total).map(TestEvent::Ping))
        .await?;
    op.commit().await?;

    let mut listener = outbox.listen_persisted(EventSequence::BEGIN);

    let mut events = Vec::new();
    for i in 0..total {
        let event = tokio::time::timeout(std::time::Duration::from_secs(2), listener.next())
            .await
            .unwrap_or_else(|_| panic!("timeout waiting for event {i}"))
            .unwrap_or_else(|| panic!("expected event {i} but got None"))
            .unwrap_or_else(|e| panic!("undecodable event {i}: {e}"));
        events.push(event);
    }

    assert_eq!(events.len() as u64, total, "all events should be persisted");

    let mut last_sequence: Option<EventSequence> = None;
    for (i, event) in events.iter().enumerate() {
        assert!(
            matches!(event.payload, Some(TestEvent::Ping(n)) if n == i as u64),
            "event {i} payload should match publish order",
        );
        if let Some(prev) = last_sequence {
            assert!(
                event.sequence > prev,
                "sequences must be strictly increasing across chunk boundaries",
            );
        }
        last_sequence = Some(event.sequence);
    }

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn sequence_gap_from_rolled_back_transaction() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    // Small grace so the fill episode starts quickly; a raw nextval burn
    // has no owning transaction left, so the abandonment proof passes on
    // the episode's first check and the placeholder follows immediately.
    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .gap_fill_grace(std::time::Duration::from_millis(100))
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let mut listener = outbox.listen_persisted(None);

    // Publish an event (seq N)
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(0))
        .await?;
    op.commit().await?;

    let event = tokio::time::timeout(std::time::Duration::from_secs(2), listener.next())
        .await?
        .expect("should receive first event")?;
    assert!(matches!(event.payload, Some(TestEvent::Ping(0))));

    // Create a gap by consuming a sequence number without inserting a row
    sqlx::query!("SELECT nextval('persistent_outbox_events_sequence_seq')")
        .fetch_one(&pool)
        .await?;

    // Publish another event (seq N+2, skipping the consumed N+1)
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(1))
        .await?;
    op.commit().await?;

    // Should receive the gap-filled placeholder (None payload) followed by the real event
    let gap_event = tokio::time::timeout(std::time::Duration::from_secs(5), listener.next())
        .await?
        .expect("should receive gap-filled placeholder")?;
    assert!(
        gap_event.payload.is_none(),
        "gap-filled event should have None payload"
    );

    let real_event = tokio::time::timeout(std::time::Duration::from_secs(2), listener.next())
        .await?
        .expect("should receive real event after gap")?;
    assert!(matches!(real_event.payload, Some(TestEvent::Ping(1))));

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn gap_fill_waits_for_grace_period() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .gap_fill_grace(std::time::Duration::from_secs(2))
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let mut listener = outbox.listen_persisted(None);

    // Publish an event (seq N)
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(0))
        .await?;
    op.commit().await?;

    let event = tokio::time::timeout(std::time::Duration::from_secs(2), listener.next())
        .await?
        .expect("should receive first event")?;
    assert!(matches!(event.payload, Some(TestEvent::Ping(0))));

    // Burn a sequence number (gap at N+1), then publish seq N+2
    sqlx::query!("SELECT nextval('persistent_outbox_events_sequence_seq')")
        .fetch_one(&pool)
        .await?;

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(1))
        .await?;
    op.commit().await?;

    // Within the grace period nothing may be yielded: no premature
    // placeholder for the gap, and the real event is contiguous-blocked
    // behind it.
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(500), listener.next())
            .await
            .is_err(),
        "no gap-fill placeholder before the grace period elapses"
    );

    // After the grace period the placeholder arrives, then the real event.
    let gap_event = tokio::time::timeout(std::time::Duration::from_secs(5), listener.next())
        .await?
        .expect("should receive gap-filled placeholder after grace period")?;
    assert!(
        gap_event.payload.is_none(),
        "gap-filled event should have None payload"
    );

    let real_event = tokio::time::timeout(std::time::Duration::from_secs(2), listener.next())
        .await?
        .expect("should receive real event after gap")?;
    assert!(matches!(real_event.payload, Some(TestEvent::Ping(1))));

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn in_flight_transaction_gap_resolves_without_placeholder() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    // Default grace (2s) — the in-flight transaction below commits well
    // within it (and while it lives its sequence is never provably
    // abandoned), so the gap must resolve with the real row, never a
    // placeholder.
    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let mut listener = outbox.listen_persisted(None);

    // Publish an event (seq N)
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(0))
        .await?;
    op.commit().await?;

    let event = tokio::time::timeout(std::time::Duration::from_secs(2), listener.next())
        .await?
        .expect("should receive first event")?;
    assert!(matches!(event.payload, Some(TestEvent::Ping(0))));

    // Insert seq N+1 directly in a transaction that stays open…
    let mut tx = pool.begin().await?;
    sqlx::query("INSERT INTO persistent_outbox_events (payload) VALUES ($1::jsonb)")
        .bind(r#"{"Ping": 7}"#)
        .execute(&mut *tx)
        .await?;

    // …while a later event (seq N+2) commits first, creating the classic
    // allocation-order vs commit-order gap.
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(1))
        .await?;
    op.commit().await?;

    // Nothing is yielded while the gap is unresolved (contiguous broadcast).
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), listener.next())
            .await
            .is_err(),
        "no event may be yielded while the gap sequence is uncommitted"
    );

    // Commit the in-flight transaction within the grace period: the gap
    // resolves with the real event, not a placeholder.
    tx.commit().await?;

    let gap_event = tokio::time::timeout(std::time::Duration::from_secs(5), listener.next())
        .await?
        .expect("should receive the committed gap event")?;
    assert!(
        matches!(gap_event.payload, Some(TestEvent::Ping(7))),
        "gap must resolve with the real committed event, got {:?}",
        gap_event.payload
    );

    let real_event = tokio::time::timeout(std::time::Duration::from_secs(2), listener.next())
        .await?
        .expect("should receive real event after gap")?;
    assert!(matches!(real_event.payload, Some(TestEvent::Ping(1))));

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn gap_fill_defers_to_in_flight_writer() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    // Grace far below the writer's lifetime: fill episodes start early but
    // must stay read-only while a transaction that could own the gap is
    // still running — the abandonment proof (xmin horizon) cannot pass
    // until that writer ends.
    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .gap_fill_grace(std::time::Duration::from_millis(100))
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

    let event = tokio::time::timeout(std::time::Duration::from_secs(2), listener.next())
        .await?
        .expect("should receive first event")?;
    assert!(matches!(event.payload, Some(TestEvent::Ping(0))));

    // A writer allocates seq N+1 and stays in flight…
    let mut tx = pool.begin().await?;
    sqlx::query("INSERT INTO persistent_outbox_events (payload) VALUES ($1::jsonb)")
        .bind(r#"{"Ping": 7}"#)
        .execute(&mut *tx)
        .await?;

    // …while seq N+2 commits, stalling the broadcast on the gap.
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(1))
        .await?;
    op.commit().await?;

    // Well past the grace period: fill episodes have been running, but no
    // placeholder may appear while the writer lives (and the episode must
    // not block on the writer's speculative-insertion lock either — it
    // never touches an unproven sequence).
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(1500), listener.next())
            .await
            .is_err(),
        "no placeholder may be written while the gap's writer is still in flight"
    );

    // The writer aborts: its sequence is now provably abandoned and the
    // running episode fills it on its next proof check.
    tx.rollback().await?;

    let gap_event = tokio::time::timeout(std::time::Duration::from_secs(5), listener.next())
        .await?
        .expect("should receive gap-filled placeholder after the writer aborts")?;
    assert!(
        gap_event.payload.is_none(),
        "gap-filled event should have None payload"
    );

    let real_event = tokio::time::timeout(std::time::Duration::from_secs(2), listener.next())
        .await?
        .expect("should receive real event after gap")?;
    assert!(matches!(real_event.payload, Some(TestEvent::Ping(1))));

    Ok(())
}

/// The reactive tier: a transaction that fails *after* its outbox persist
/// ran (here: a post-persist hook veto) reports its allocated sequences to
/// the in-process compensator, which placeholder-fills them immediately —
/// downstream listeners resume in milliseconds, well before the
/// grace-gated backstop (2s default here) would even take its first look.
#[tokio::test]
#[file_serial]
async fn failed_commit_compensates_placeholders_reactively() -> anyhow::Result<()> {
    use es_entity::hooks::{BoxFuture, HookOperation};
    use obix::out::{PersistentOutboxEvent, PostPersistHook};
    use std::sync::atomic::{AtomicBool, Ordering};

    struct FailOnceHook {
        failed: AtomicBool,
    }

    impl PostPersistHook<TestEvent> for FailOnceHook {
        fn on_persisted<'a>(
            &'a self,
            _op: &'a mut HookOperation<'_>,
            _events: &'a [PersistentOutboxEvent<TestEvent>],
        ) -> BoxFuture<'a, Result<(), sqlx::Error>> {
            Box::pin(async move {
                if !self.failed.swap(true, Ordering::SeqCst) {
                    Err(sqlx::Error::Protocol("post-persist hook veto".into()))
                } else {
                    Ok(())
                }
            })
        }
    }

    let pool = init_pool().await?;
    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;
    outbox.add_post_persist_hook(FailOnceHook {
        failed: AtomicBool::new(false),
    });

    let mut listener = outbox.listen_persisted(None);

    // The hook vetoes the first commit — after the persist allocated its
    // sequence. The rollback burns the sequence.
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(0))
        .await?;
    assert!(
        op.commit().await.is_err(),
        "the hook veto must fail the commit"
    );

    // Reactive compensation: the placeholder must arrive well before the
    // 2s grace period would allow the backstop's first fill attempt.
    let gap_event = tokio::time::timeout(std::time::Duration::from_millis(1500), listener.next())
        .await
        .map_err(|_| anyhow::anyhow!("compensation placeholder did not arrive reactively"))?
        .expect("stream open")?;
    assert!(
        gap_event.payload.is_none(),
        "the compensated sequence must be a placeholder"
    );

    // The stream continues normally past the compensated sequence.
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(1))
        .await?;
    op.commit().await?;

    let real_event = tokio::time::timeout(std::time::Duration::from_secs(2), listener.next())
        .await?
        .expect("should receive the event after the compensated gap")?;
    assert!(matches!(real_event.payload, Some(TestEvent::Ping(1))));
    assert!(u64::from(real_event.sequence) > u64::from(gap_event.sequence));

    Ok(())
}

/// The `on_rollback` tier: a LATER commit hook on the same operation fails
/// after `PersistEvents::pre_commit` already persisted its batch — the
/// whole transaction rolls back, and es-entity fires `on_rollback` on the
/// persist hook (after the rollback), which reports the burned sequences
/// for immediate compensation. Regression test for the review question
/// "does compensation survive a later hook's failure?": the placeholder
/// must arrive reactively, well before the 2s grace period would let the
/// backstop act.
#[tokio::test]
#[file_serial]
async fn later_hook_failure_compensates_placeholders_reactively() -> anyhow::Result<()> {
    use es_entity::AtomicOperation as _;
    use es_entity::hooks::{CommitHook, HookOperation, PreCommitRet};

    struct VetoHook;

    impl CommitHook for VetoHook {
        async fn pre_commit(
            self,
            _op: HookOperation<'_>,
        ) -> Result<PreCommitRet<'_, Self>, sqlx::Error> {
            Err(sqlx::Error::Protocol("later hook veto".into()))
        }
    }

    let pool = init_pool().await?;
    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let mut listener = outbox.listen_persisted(None);

    // Publish first (registers PersistEvents), then register the vetoing
    // hook — hooks run in registration order, so the veto fires AFTER the
    // outbox persist has allocated its sequence.
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(0))
        .await?;
    op.add_commit_hook(VetoHook)
        .unwrap_or_else(|_| panic!("DbOp supports commit hooks"));
    assert!(
        op.commit().await.is_err(),
        "the later hook's veto must fail the commit"
    );

    // Reactive compensation via on_rollback: the placeholder must arrive
    // well before the 2s grace period would allow a backstop fill.
    let gap_event = tokio::time::timeout(std::time::Duration::from_millis(1500), listener.next())
        .await
        .map_err(|_| anyhow::anyhow!("compensation placeholder did not arrive reactively"))?
        .expect("stream open")?;
    assert!(
        gap_event.payload.is_none(),
        "the compensated sequence must be a placeholder"
    );

    // The stream continues normally past the compensated sequence.
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(1))
        .await?;
    op.commit().await?;

    let real_event = tokio::time::timeout(std::time::Duration::from_secs(2), listener.next())
        .await?
        .expect("should receive the event after the compensated gap")?;
    assert!(matches!(real_event.payload, Some(TestEvent::Ping(1))));
    assert!(u64::from(real_event.sequence) > u64::from(gap_event.sequence));

    Ok(())
}

/// One backfill request serves its whole range, parking on gaps it cannot
/// yet serve instead of terminating: a replaying listener that runs into a
/// young frontier gap (in-flight writer) must receive the writer's REAL
/// event once it commits — through the same request, with no placeholder
/// and no listener-side re-request logic.
#[tokio::test]
#[file_serial]
async fn backfill_parks_across_in_flight_frontier_gap() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    // seq 1 committed; seq 2 allocated by a writer that stays in flight;
    // seq 3 committed — all after outbox init, so seq 2 is a young gap the
    // backfill task must never placeholder-fill.
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(0))
        .await?;
    op.commit().await?;

    let mut tx = pool.begin().await?;
    sqlx::query("INSERT INTO persistent_outbox_events (payload) VALUES ($1::jsonb)")
        .bind(r#"{"Ping": 7}"#)
        .execute(&mut *tx)
        .await?;

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(1))
        .await?;
    op.commit().await?;

    // Replay from the beginning: backfill serves seq 1, then parks at the
    // in-flight seq 2.
    let mut listener = outbox.listen_persisted(Some(EventSequence::from(0)));

    let first = tokio::time::timeout(std::time::Duration::from_secs(5), listener.next())
        .await?
        .expect("should replay first event")?;
    assert!(matches!(first.payload, Some(TestEvent::Ping(0))));

    // While the writer lives nothing may be delivered past the gap — and
    // no placeholder may be written for it.
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(500), listener.next())
            .await
            .is_err(),
        "the parked backfill must not deliver past (or placeholder) an in-flight gap"
    );

    // The writer commits: the parked request resumes and delivers the real
    // event, then the rest of the range — same request, no re-request
    // needed.
    tx.commit().await?;

    let gap_event = tokio::time::timeout(std::time::Duration::from_secs(5), listener.next())
        .await?
        .expect("should receive the committed gap event")?;
    assert!(
        matches!(gap_event.payload, Some(TestEvent::Ping(7))),
        "the gap must resolve with the real committed event, got {:?}",
        gap_event.payload
    );

    let third = tokio::time::timeout(std::time::Duration::from_secs(5), listener.next())
        .await?
        .expect("should receive the event after the gap")?;
    assert!(matches!(third.payload, Some(TestEvent::Ping(1))));

    Ok(())
}

/// Concurrent replays over the same burned range: both listeners' backfills
/// report the same historical gap; the GapFiller merges the overlapping
/// requests into one proof + one locked fill, and both replays complete
/// with the placeholder in position.
#[tokio::test]
#[file_serial]
async fn overlapping_backfills_share_one_historical_fill() -> anyhow::Result<()> {
    use obix::out::Outbox;

    let pool = init_pool().await?;
    helpers::wipeout_outbox_tables(&pool).await?;

    // Pre-init history: seq 1 committed, seq 2 burned, seq 3 committed.
    sqlx::query("INSERT INTO persistent_outbox_events (payload) VALUES ($1::jsonb)")
        .bind(r#"{"Ping": 0}"#)
        .execute(&pool)
        .await?;
    sqlx::query!("SELECT nextval('persistent_outbox_events_sequence_seq')")
        .fetch_one(&pool)
        .await?;
    sqlx::query("INSERT INTO persistent_outbox_events (payload) VALUES ($1::jsonb)")
        .bind(r#"{"Ping": 1}"#)
        .execute(&pool)
        .await?;

    let outbox = Outbox::<TestEvent, helpers::TestTables>::init(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let mut listeners = [
        outbox.listen_persisted(Some(EventSequence::from(0))),
        outbox.listen_persisted(Some(EventSequence::from(0))),
    ];

    for listener in &mut listeners {
        let first = tokio::time::timeout(std::time::Duration::from_secs(5), listener.next())
            .await?
            .expect("should replay first event")?;
        assert!(matches!(first.payload, Some(TestEvent::Ping(0))));

        let gap_event = tokio::time::timeout(std::time::Duration::from_secs(5), listener.next())
            .await?
            .expect("should receive placeholder for the burned sequence")?;
        assert!(gap_event.payload.is_none());
        assert_eq!(u64::from(gap_event.sequence), 2);

        let third = tokio::time::timeout(std::time::Duration::from_secs(5), listener.next())
            .await?
            .expect("should replay the event after the gap")?;
        assert!(matches!(third.payload, Some(TestEvent::Ping(1))));
    }

    Ok(())
}

/// The cluster-wide fill lock: a backstop fill skips entirely (inserting
/// nothing) while another connection holds the lock, and proceeds once it
/// is released.
#[tokio::test]
#[file_serial]
async fn fill_gaps_deduped_skips_while_fill_lock_held() -> anyhow::Result<()> {
    use obix::MailboxTables as _;

    let pool = init_pool().await?;
    helpers::wipeout_outbox_tables(&pool).await?;

    sqlx::query!("SELECT nextval('persistent_outbox_events_sequence_seq')")
        .fetch_one(&pool)
        .await?;

    // Another session holds the fill lock (same key the generated query
    // derives from the table name).
    let mut tx = pool.begin().await?;
    sqlx::query(
        "SELECT pg_advisory_xact_lock(hashtextextended('persistent_outbox_events_gap_fill', 0))",
    )
    .execute(&mut *tx)
    .await?;

    let skipped =
        helpers::TestTables::fill_gaps_deduped::<TestEvent>(&pool, vec![EventSequence::from(1)])
            .await?;
    assert!(
        skipped.is_none(),
        "fill must skip while another connection holds the fill lock"
    );
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM persistent_outbox_events")
        .fetch_one(&pool)
        .await?;
    assert_eq!(rows, 0, "a skipped fill must not insert anything");

    tx.rollback().await?;

    let filled =
        helpers::TestTables::fill_gaps_deduped::<TestEvent>(&pool, vec![EventSequence::from(1)])
            .await?
            .expect("lock released — the fill must proceed");
    assert_eq!(filled.len(), 1);

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn gap_fill_batch_limit_fills_across_attempts() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    // Batch limit of 2 with 5 lost sequences: no single fill may insert
    // more than 2 placeholders, and successive attempts must recover the
    // remainder — capped, never skipped.
    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .gap_fill_grace(std::time::Duration::from_millis(100))
            .gap_fill_batch_limit(2)
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

    let event = tokio::time::timeout(std::time::Duration::from_secs(2), listener.next())
        .await?
        .expect("should receive first event")?;
    assert!(matches!(event.payload, Some(TestEvent::Ping(0))));

    // Burn five sequence numbers (gaps at N+1..=N+5), then publish N+6.
    for _ in 0..5 {
        sqlx::query!("SELECT nextval('persistent_outbox_events_sequence_seq')")
            .fetch_one(&pool)
            .await?;
    }

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(1))
        .await?;
    op.commit().await?;

    // All five placeholders arrive, in order, across at least three
    // batch-capped fill attempts.
    let first_gap = u64::from(event.sequence) + 1;
    for expected_sequence in first_gap..first_gap + 5 {
        let gap_event = tokio::time::timeout(std::time::Duration::from_secs(5), listener.next())
            .await?
            .expect("should receive gap-filled placeholder")?;
        assert!(
            gap_event.payload.is_none(),
            "gap-filled event should have None payload"
        );
        assert_eq!(u64::from(gap_event.sequence), expected_sequence);
    }

    let real_event = tokio::time::timeout(std::time::Duration::from_secs(5), listener.next())
        .await?
        .expect("should receive real event after gaps")?;
    assert!(matches!(real_event.payload, Some(TestEvent::Ping(1))));

    Ok(())
}

/// A batch-capped fill episode stays alive across batches: the remainder
/// of the window recovers at the 1s loop cadence, not one full grace
/// period per batch (delivering a batch advances the broadcast cursor,
/// which discards the gap state — a returning episode would restart grace
/// from scratch for every batch).
#[tokio::test]
#[file_serial]
async fn batch_capped_fill_does_not_pay_grace_per_batch() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    // Grace (2.5s) far above the episode's 1s loop cadence: if each batch
    // paid a fresh grace, consecutive placeholders would arrive ~2.5s
    // apart; within one episode they arrive ~1s apart.
    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .gap_fill_grace(std::time::Duration::from_millis(2500))
            .gap_fill_batch_limit(1)
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

    let event = tokio::time::timeout(std::time::Duration::from_secs(2), listener.next())
        .await?
        .expect("should receive first event")?;
    assert!(matches!(event.payload, Some(TestEvent::Ping(0))));

    // Two burned sequences -> two single-placeholder batches.
    for _ in 0..2 {
        sqlx::query!("SELECT nextval('persistent_outbox_events_sequence_seq')")
            .fetch_one(&pool)
            .await?;
    }
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(1))
        .await?;
    op.commit().await?;

    let first_gap = tokio::time::timeout(std::time::Duration::from_secs(6), listener.next())
        .await?
        .expect("should receive first placeholder")?;
    assert!(first_gap.payload.is_none());
    let first_at = std::time::Instant::now();

    let second_gap = tokio::time::timeout(std::time::Duration::from_secs(6), listener.next())
        .await?
        .expect("should receive second placeholder")?;
    assert!(second_gap.payload.is_none());
    assert!(
        first_at.elapsed() < std::time::Duration::from_secs(2),
        "second batch must follow at the episode's loop cadence (~1s), \
         not after a fresh grace period (2.5s); took {:?}",
        first_at.elapsed()
    );

    let real_event = tokio::time::timeout(std::time::Duration::from_secs(2), listener.next())
        .await?
        .expect("should receive real event after gaps")?;
    assert!(matches!(real_event.payload, Some(TestEvent::Ping(1))));

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn fill_gaps_leaves_committed_rows_untouched() -> anyhow::Result<()> {
    use obix::MailboxTables as _;

    let pool = init_pool().await?;
    helpers::wipeout_outbox_tables(&pool).await?;

    sqlx::query("INSERT INTO persistent_outbox_events (payload) VALUES ($1::jsonb), ($2::jsonb)")
        .bind(r#"{"Ping": 1}"#)
        .bind(r#"{"Ping": 2}"#)
        .execute(&pool)
        .await?;

    let tuples_before: Vec<(String, i64)> = sqlx::query_as(
        "SELECT xmin::text, sequence FROM persistent_outbox_events ORDER BY sequence",
    )
    .fetch_all(&pool)
    .await?;

    // Filling over already-committed sequences must insert nothing and —
    // unlike the previous DO UPDATE upsert — rewrite nothing (no new row
    // versions, no dead tuples).
    let inserted = helpers::TestTables::fill_gaps::<TestEvent>(
        &pool,
        vec![EventSequence::from(1), EventSequence::from(2)],
    )
    .await?;
    assert!(
        inserted.is_empty(),
        "fill over committed sequences must return no inserted rows"
    );

    let tuples_after: Vec<(String, i64)> = sqlx::query_as(
        "SELECT xmin::text, sequence FROM persistent_outbox_events ORDER BY sequence",
    )
    .fetch_all(&pool)
    .await?;
    assert_eq!(
        tuples_before, tuples_after,
        "committed rows must keep their xmin — a rewrite would create dead tuples"
    );

    // And the payloads survive untouched.
    let events =
        helpers::TestTables::load_next_page::<TestEvent>(&pool, EventSequence::from(0), 10).await?;
    let payloads = events
        .into_iter()
        .map(|item| item.expect("decodable row").payload)
        .collect::<Vec<_>>();
    assert_eq!(
        payloads,
        vec![Some(TestEvent::Ping(1)), Some(TestEvent::Ping(2))]
    );

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn backfill_fills_historical_gap_for_replaying_listener() -> anyhow::Result<()> {
    use obix::out::Outbox;

    let pool = init_pool().await?;
    helpers::wipeout_outbox_tables(&pool).await?;

    // History written with NO cache loop running: seq 1 committed, seq 2
    // burned (a rolled-back writer nobody observed as a frontier stall),
    // seq 3 committed.
    sqlx::query("INSERT INTO persistent_outbox_events (payload) VALUES ($1::jsonb)")
        .bind(r#"{"Ping": 0}"#)
        .execute(&pool)
        .await?;
    sqlx::query!("SELECT nextval('persistent_outbox_events_sequence_seq')")
        .fetch_one(&pool)
        .await?;
    sqlx::query("INSERT INTO persistent_outbox_events (payload) VALUES ($1::jsonb)")
        .bind(r#"{"Ping": 1}"#)
        .execute(&pool)
        .await?;

    // Init AFTER the gap exists: the broadcast cursor starts at the head
    // and never visits the historical gap — only backfill can serve a
    // replaying listener, and it must placeholder-fill the lost sequence
    // (provably abandoned: allocated before the cache loop started, and
    // its writer is long gone, so the abandonment proof passes at once).
    let outbox = Outbox::<TestEvent, helpers::TestTables>::init(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;
    let mut listener = outbox.listen_persisted(Some(EventSequence::from(0)));

    let first = tokio::time::timeout(std::time::Duration::from_secs(5), listener.next())
        .await?
        .expect("should replay first event")?;
    assert!(matches!(first.payload, Some(TestEvent::Ping(0))));

    let gap_event = tokio::time::timeout(std::time::Duration::from_secs(5), listener.next())
        .await?
        .expect("should receive placeholder for the historical gap")?;
    assert!(
        gap_event.payload.is_none(),
        "historical gap must resolve as a placeholder"
    );
    assert_eq!(u64::from(gap_event.sequence), 2);

    let third = tokio::time::timeout(std::time::Duration::from_secs(5), listener.next())
        .await?
        .expect("should replay the event after the gap")?;
    assert!(matches!(third.payload, Some(TestEvent::Ping(1))));

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn backfill_fills_burned_tail_for_replaying_listener() -> anyhow::Result<()> {
    use obix::out::Outbox;

    let pool = init_pool().await?;
    helpers::wipeout_outbox_tables(&pool).await?;

    // History ends in burned sequences: seq 1 committed, seqs 2 and 3
    // allocated but never committed, no process running at the time.
    sqlx::query("INSERT INTO persistent_outbox_events (payload) VALUES ($1::jsonb)")
        .bind(r#"{"Ping": 0}"#)
        .execute(&pool)
        .await?;
    for _ in 0..2 {
        sqlx::query!("SELECT nextval('persistent_outbox_events_sequence_seq')")
            .fetch_one(&pool)
            .await?;
    }

    // The cursor initializes at the allocation head (3); a replaying
    // listener must not stall forever on the burned tail — backfill
    // placeholder-fills it once the abandonment proof passes (immediately:
    // the burning connections are gone).
    let outbox = Outbox::<TestEvent, helpers::TestTables>::init(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;
    let mut listener = outbox.listen_persisted(Some(EventSequence::from(0)));

    let first = tokio::time::timeout(std::time::Duration::from_secs(5), listener.next())
        .await?
        .expect("should replay first event")?;
    assert!(matches!(first.payload, Some(TestEvent::Ping(0))));

    for expected_sequence in 2..=3u64 {
        let gap_event = tokio::time::timeout(std::time::Duration::from_secs(5), listener.next())
            .await?
            .expect("should receive placeholder for the burned tail")?;
        assert!(
            gap_event.payload.is_none(),
            "burned tail must resolve as placeholders"
        );
        assert_eq!(u64::from(gap_event.sequence), expected_sequence);
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

// SECURITY regression: a forged pg_notify on the ephemeral channel must not
// inject events. LISTEN/NOTIFY has no per-channel authorization in
// PostgreSQL — any role able to connect can signal any channel — so the
// notification is only a hint; the event must come from the table (and a
// forged hint with no matching row must yield nothing).
#[tokio::test]
#[file_serial]
async fn forged_ephemeral_notification_is_not_delivered() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let mut listener = outbox.listen_ephemeral();

    // Fully-formed forged event, as the pre-fix listener would have accepted
    // and broadcast directly from the notification body.
    sqlx::query("SELECT pg_notify('ephemeral_outbox_events', $1)")
        .bind(
            serde_json::json!({
                "event_type": "forged_type",
                "payload": {"Ping": 999},
                "tracing_context": null,
                "recorded_at": chrono::Utc::now(),
            })
            .to_string(),
        )
        .execute(&pool)
        .await?;

    // Nothing may arrive: the hint references no row in the table.
    let forged = tokio::time::timeout(std::time::Duration::from_millis(500), listener.next()).await;
    assert!(
        forged.is_err(),
        "forged ephemeral notification must not be delivered, got {forged:?}"
    );

    // The listener is still alive: a legitimately published event arrives.
    let event_type = obix::out::EphemeralEventType::new("legit_type");
    outbox
        .publish_ephemeral(event_type.clone(), TestEvent::Ping(1))
        .await?;
    let event = tokio::time::timeout(std::time::Duration::from_secs(5), listener.next())
        .await?
        .expect("listener must still deliver legitimate events");
    assert_eq!(event.event_type, event_type);
    assert!(matches!(event.payload, TestEvent::Ping(1)));

    Ok(())
}

// An ephemeral event written by another process (straight SQL insert,
// bypassing this process's in-process publish broadcast) must still reach
// listeners: the trigger's {event_type, recorded_at} hint triggers a
// fetch from the table with the listener's own credentials.
#[tokio::test]
#[file_serial]
async fn ephemeral_event_written_externally_is_fetched_from_db() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let mut listener = outbox.listen_ephemeral();

    sqlx::query("INSERT INTO ephemeral_outbox_events (event_type, payload) VALUES ($1, $2)")
        .bind("external_type")
        .bind(serde_json::json!({"Ping": 7}))
        .execute(&pool)
        .await?;

    let event = tokio::time::timeout(std::time::Duration::from_secs(5), listener.next())
        .await?
        .expect("externally written ephemeral event must be delivered via the hint + fetch path");
    assert_eq!(event.event_type.as_str(), "external_type");
    assert!(matches!(event.payload, TestEvent::Ping(7)));

    Ok(())
}

// SECURITY regression: a forged {min_sequence, max_sequence} notification
// on the persistent channel must not (a) advance the cache head past the
// database's actual sequence, (b) synthesize phantom events, (c) drive an
// unbounded range scan over a populated table, or (d) stall real delivery.
// Unlike the ephemeral forgery test this runs against a POPULATED table, so
// the range-fetch amplification (fetch_notified_range(0, i64::MAX) streaming
// the whole tail) is actually exercised and the clamp is tested.
#[tokio::test]
#[file_serial]
async fn forged_persistent_notification_does_not_stall_listener() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    // Populate the table so the forged range fetch has real rows to amplify
    // over if the clamp were absent.
    let mut op = pool.begin().await?;
    outbox
        .publish_all_persisted(&mut op, (0..5).map(TestEvent::Ping))
        .await?;
    op.commit().await?;

    let mut listener = outbox.listen_persisted(None);

    sqlx::query("SELECT pg_notify('persistent_outbox_events', $1)")
        .bind(
            serde_json::json!({
                "min_sequence": 1,
                "max_sequence": i64::MAX,
            })
            .to_string(),
        )
        .execute(&pool)
        .await?;

    // The forged claim is clamped to the real head, so no phantom events
    // beyond the committed 5 are synthesized. (The 5 real events may arrive
    // from the clamped fetch — that's correct, not a forgery.)
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    while let Ok(Some(item)) =
        tokio::time::timeout(std::time::Duration::from_millis(300), listener.next()).await
    {
        let event = item?;
        let n = event.payload.as_ref().and_then(|p| match p {
            TestEvent::Ping(n) => Some(*n),
            _ => None,
        });
        assert!(
            n.is_some_and(|n| n < 5),
            "forged persistent notification must not deliver events beyond the real head, got {event:?}"
        );
    }

    // Real delivery still works, promptly (the forged head must not wedge
    // the contiguity machinery).
    let mut op = pool.begin().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(99))
        .await?;
    op.commit().await?;

    let event = tokio::time::timeout(std::time::Duration::from_secs(5), listener.next())
        .await?
        .expect("listener must still deliver after a forged notification")?;
    assert!(matches!(event.payload, Some(TestEvent::Ping(99))));

    Ok(())
}

/// Regression: an event whose NOTIFY fires while the LISTEN connection is
/// down must still reach consumers.
///
/// Publishing through a plain transaction delivers exclusively via pg_notify
/// (no same-process short-circuit hook), so the event below travels the wire
/// path or not at all. The LISTEN backends are terminated *inside* the
/// publishing transaction: they die at statement time and the NOTIFY fires at
/// COMMIT moments later, guaranteeing it is sent while no listener connection
/// exists — that notification is unrecoverably lost.
///
/// Before the fix the pump ran on `PgListener::recv()`, which silently
/// swallows the reconnect marker, so a notification lost in the gap was never
/// resynced and the consumer stalled forever (and when the internal reconnect
/// itself failed, the pump task exited, dropping the notification senders and
/// killing the cache loops outright — the sb-realtime staging wedge, where
/// every outbox consumer froze mid-sequence while its listener job kept
/// heartbeating). With the fix the pump surfaces the gap (`try_recv` +
/// `NotifyMessage::Resync`) and the caches re-read the tables.
#[tokio::test]
#[file_serial]
async fn delivers_events_notified_while_listen_connection_down() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let mut listener = outbox.listen_persisted(None);

    // Baseline: the pg_notify path works.
    let mut op = pool.begin().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(1))
        .await?;
    op.commit().await?;

    let event = tokio::time::timeout(std::time::Duration::from_secs(10), listener.next())
        .await
        .map_err(|_| anyhow::anyhow!("baseline pg_notify delivery timed out"))?
        .ok_or_else(|| anyhow::anyhow!("listener stream ended"))??;
    assert!(matches!(event.payload, Some(TestEvent::Ping(1))));

    // Terminate every LISTEN backend and insert the event in ONE autocommit
    // statement: the backends die mid-statement and the insert trigger's
    // NOTIFY fires at that same statement's commit, before any client-side
    // reconnect round trip can complete — so the notification is
    // deterministically sent while no LISTEN connection exists. (Publishing
    // in a separate statement is racy: sqlx's eager reconnect re-subscribes off
    // a warm pool connection faster than a second round trip.)
    sqlx::query(
        r#"
        WITH kill AS (
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE pid <> pg_backend_pid() AND query LIKE 'LISTEN%'
        )
        INSERT INTO persistent_outbox_events (payload)
        SELECT $1::jsonb FROM (SELECT count(*) FROM kill) _forced
        "#,
    )
    .bind(r#"{"Ping": 2}"#)
    .execute(&pool)
    .await?;

    let event = tokio::time::timeout(std::time::Duration::from_secs(10), listener.next())
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "event published while the LISTEN connection was down was never delivered \
                 (missed notification not resynced)"
            )
        })?
        .ok_or_else(|| anyhow::anyhow!("listener stream ended"))??;
    assert!(matches!(event.payload, Some(TestEvent::Ping(2))));

    Ok(())
}

// A hook-supporting op carries no pg_notify; a second Outbox instance can
// only learn about the event through the out-of-band debounced hint —
// receipt well under the 10s idle-resync default proves that path works.
#[tokio::test]
#[file_serial]
async fn cross_instance_delivery_via_debounced_notify() -> anyhow::Result<()> {
    use obix::out::Outbox;

    let pool = init_pool().await?;
    let config = MailboxConfig::builder()
        .build()
        .expect("Couldn't build MailboxConfig");

    let outbox_a = init_outbox::<TestEvent>(&pool, config.clone()).await?;
    let outbox_b = Outbox::<TestEvent, helpers::TestTables>::init(&pool, config).await?;

    let mut listener_b = outbox_b.listen_persisted(None);

    // DbOp supports commit hooks: the persist statement is the silent
    // variant, so cross-process delivery depends on the debounced notify.
    let mut op = outbox_a.begin_op().await?;
    outbox_a
        .publish_persisted_in_op(&mut op, TestEvent::Ping(0))
        .await?;
    op.commit().await?;

    let event = tokio::time::timeout(std::time::Duration::from_secs(2), listener_b.next())
        .await
        .map_err(|_| anyhow::anyhow!("debounced notify never reached the other instance"))?
        .ok_or_else(|| anyhow::anyhow!("listener stream ended"))??;
    assert!(matches!(event.payload, Some(TestEvent::Ping(0))));

    Ok(())
}

// A burst of commits must coalesce into fewer NOTIFYs, and the final
// hint's max_sequence must still cover the last committed batch.
#[tokio::test]
#[file_serial]
async fn debounced_notifier_coalesces_bursts() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .notify_debounce(std::time::Duration::from_millis(100))
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let mut raw_listener = sqlx::postgres::PgListener::connect_with(&pool).await?;
    raw_listener.listen("persistent_outbox_events").await?;

    let n: u64 = 10;
    for i in 0..n {
        let mut op = outbox.begin_op().await?;
        outbox
            .publish_persisted_in_op(&mut op, TestEvent::Ping(i))
            .await?;
        op.commit().await?;
    }

    // Collect notifications until the channel goes quiet for well over a
    // debounce interval.
    let mut payloads = Vec::new();
    while let Ok(notification) =
        tokio::time::timeout(std::time::Duration::from_millis(500), raw_listener.recv()).await
    {
        payloads.push(notification?.payload().to_string());
    }

    assert!(
        !payloads.is_empty(),
        "the debounced notifier must emit at least one notification"
    );
    assert!(
        (payloads.len() as u64) < n,
        "a burst of {n} commits must coalesce into fewer notifications, got {}",
        payloads.len()
    );

    #[derive(Deserialize)]
    struct Header {
        max_sequence: u64,
    }
    let last: Header = serde_json::from_str(payloads.last().expect("non-empty"))?;
    assert_eq!(
        last.max_sequence, n,
        "the final hint's max_sequence must cover the last committed batch"
    );

    Ok(())
}

// Crash-window backstop: rows that never get any notification (writer died
// between commit and notify, or external raw-SQL insert) must still be
// delivered via the idle head-poll.
#[tokio::test]
#[file_serial]
async fn unnotified_events_delivered_via_idle_resync() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .idle_resync_interval(std::time::Duration::from_millis(500))
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let mut listener = outbox.listen_persisted(None);

    // Raw insert: no obix publish, no post_commit report, no NOTIFY at all.
    sqlx::query("INSERT INTO persistent_outbox_events (payload) VALUES ($1::jsonb)")
        .bind(r#"{"Ping": 42}"#)
        .execute(&pool)
        .await?;

    let event = tokio::time::timeout(std::time::Duration::from_secs(5), listener.next())
        .await
        .map_err(|_| anyhow::anyhow!("idle head-poll never delivered the unnotified event"))?
        .ok_or_else(|| anyhow::anyhow!("listener stream ended"))??;
    assert!(matches!(event.payload, Some(TestEvent::Ping(42))));

    Ok(())
}

// SECURITY regression: garbage traffic on the notify channel must not
// count as pipeline activity — the idle head-poll timer only resets on
// authoritative progress (a new committed row or a confirmed head read),
// so junk spam cannot suppress the backstop and starve unnotified events.
#[tokio::test]
#[file_serial]
async fn junk_notifications_do_not_suppress_idle_resync() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .idle_resync_interval(std::time::Duration::from_millis(500))
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let mut listener = outbox.listen_persisted(None);

    // Unnotified row, as in unnotified_events_delivered_via_idle_resync…
    sqlx::query("INSERT INTO persistent_outbox_events (payload) VALUES ($1::jsonb)")
        .bind(r#"{"Ping": 42}"#)
        .execute(&pool)
        .await?;

    // …but under continuous unparsable spam on the channel.
    let spam_pool = pool.clone();
    let spam = tokio::spawn(async move {
        loop {
            let _ = sqlx::query("SELECT pg_notify('persistent_outbox_events', 'junk')")
                .execute(&spam_pool)
                .await;
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    });

    let result = tokio::time::timeout(std::time::Duration::from_secs(5), listener.next()).await;
    spam.abort();
    let event = result
        .map_err(|_| anyhow::anyhow!("junk notifications suppressed the idle head-poll"))?
        .ok_or_else(|| anyhow::anyhow!("listener stream ended"))??;
    assert!(matches!(event.payload, Some(TestEvent::Ping(42))));

    Ok(())
}

// The bare-transaction path keeps the legacy in-tx NOTIFY: prompt
// cross-instance delivery proves the notifying variant is wired on the
// force-execute path.
#[tokio::test]
#[file_serial]
async fn bare_transaction_publish_delivers_promptly_cross_instance() -> anyhow::Result<()> {
    use obix::out::Outbox;

    let pool = init_pool().await?;
    let config = MailboxConfig::builder()
        .build()
        .expect("Couldn't build MailboxConfig");

    let outbox_a = init_outbox::<TestEvent>(&pool, config.clone()).await?;
    let outbox_b = Outbox::<TestEvent, helpers::TestTables>::init(&pool, config).await?;

    let mut listener_b = outbox_b.listen_persisted(None);

    let mut op = pool.begin().await?;
    outbox_a
        .publish_persisted_in_op(&mut op, TestEvent::Ping(0))
        .await?;
    op.commit().await?;

    let event = tokio::time::timeout(std::time::Duration::from_secs(2), listener_b.next())
        .await
        .map_err(|_| anyhow::anyhow!("in-tx notify of a bare-transaction publish never arrived"))?
        .ok_or_else(|| anyhow::anyhow!("listener stream ended"))??;
    assert!(matches!(event.payload, Some(TestEvent::Ping(0))));

    Ok(())
}

// Rows written into a shared database by a different event enum (e.g.
// test-only module tags) must neither crash the reader nor be silently
// dropped: the event is still delivered — in order, as the `Err` item
// carrying the raw payload and the serde error — and later events flow.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "module")]
enum ForeignEvent {
    CoreParty { id: u64 },
}

#[tokio::test]
#[file_serial]
async fn undecodable_payload_is_delivered_as_err_item() -> anyhow::Result<()> {
    use obix::{MailboxTables as _, out::Outbox};

    let pool = init_pool().await?;
    helpers::wipeout_outbox_tables(&pool).await?;

    let config = MailboxConfig::builder()
        .build()
        .expect("Couldn't build MailboxConfig");

    // Publish via a foreign event enum, like tests sharing the database do.
    let foreign = Outbox::<ForeignEvent, helpers::TestTables>::init(&pool, config.clone()).await?;
    let mut op = pool.begin().await?;
    foreign
        .publish_persisted_in_op(&mut op, ForeignEvent::CoreParty { id: 1 })
        .await?;
    op.commit().await?;

    let outbox = Outbox::<TestEvent, helpers::TestTables>::init(&pool, config).await?;
    let mut listener = outbox.listen_persisted(Some(EventSequence::from(0)));

    let mut op = pool.begin().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(0))
        .await?;
    op.commit().await?;

    // The load path delivers the poison row as its Err item — raw JSON and
    // serde error attached, sequence position occupied — and the valid row
    // intact.
    let events =
        helpers::TestTables::load_next_page::<TestEvent>(&pool, EventSequence::from(0), 10).await?;
    assert_eq!(events.len(), 2);
    let poison = events[0]
        .as_ref()
        .expect_err("poison row must be the Err item");
    assert_eq!(u64::from(poison.sequence), 1);
    assert_eq!(
        poison.failure.raw,
        serde_json::json!({"module": "CoreParty", "id": 1})
    );
    assert!(
        !poison.failure.error.is_empty(),
        "the Err item must carry the serde error"
    );
    let decoded = events[1].as_ref().expect("valid row must be the Ok item");
    assert!(matches!(decoded.payload, Some(TestEvent::Ping(0))));

    // The raw stream delivers the poison event as its Err arm, in sequence
    // position — the type forces every raw consumer to decide (`?` would
    // fail loudly here) — and later events still arrive, in order.
    let first = tokio::time::timeout(std::time::Duration::from_secs(5), listener.next())
        .await?
        .ok_or_else(|| anyhow::anyhow!("listener stream closed"))?;
    let undecodable = first.expect_err("poison event must surface as the stream's Err arm");
    assert_eq!(u64::from(undecodable.sequence), 1);
    assert_eq!(
        undecodable.failure.raw,
        serde_json::json!({"module": "CoreParty", "id": 1})
    );

    let second = tokio::time::timeout(std::time::Duration::from_secs(5), listener.next())
        .await?
        .ok_or_else(|| anyhow::anyhow!("listener stream closed"))??;
    assert!(matches!(second.payload, Some(TestEvent::Ping(0))));

    Ok(())
}
