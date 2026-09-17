mod helpers;

use futures::stream::StreamExt;
use obix::{EventSequence, MailboxConfig, out::Outbox};
use serde::{Deserialize, Serialize};
use serial_test::file_serial;
use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

use helpers::{init_outbox, init_pool};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum TestEvent {
    Ping(u64),
}

/// Counts `tracing` spans created under a given name, without pulling in
/// `tracing-subscriber` (not a workspace dependency): a minimal
/// `tracing::Subscriber` that only counts `new_span` calls, installed as the
/// thread-local default for the test's duration. `#[tokio::test]` defaults
/// to the current-thread runtime, so every task spawned by the outbox
/// (including the catch-up task) runs on the same thread as the guard and
/// is captured.
struct SpanCounter {
    name: &'static str,
    count: Arc<AtomicUsize>,
    next_id: AtomicU64,
}

impl SpanCounter {
    fn install(name: &'static str) -> (tracing::subscriber::DefaultGuard, Arc<AtomicUsize>) {
        let count = Arc::new(AtomicUsize::new(0));
        let subscriber = SpanCounter {
            name,
            count: count.clone(),
            next_id: AtomicU64::new(1),
        };
        (tracing::subscriber::set_default(subscriber), count)
    }
}

impl tracing::Subscriber for SpanCounter {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        if span.metadata().name() == self.name {
            self.count.fetch_add(1, Ordering::SeqCst);
        }
        tracing::span::Id::from_u64(self.next_id.fetch_add(1, Ordering::SeqCst))
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
    fn event(&self, _event: &tracing::Event<'_>) {}
    fn enter(&self, _span: &tracing::span::Id) {}
    fn exit(&self, _span: &tracing::span::Id) {}
}

/// The headline case: a single commit larger than the cache-fill broadcast
/// must drain at DB speed, not at the gap-fill-grace cadence
/// (`backfill_page_size / gap_fill_grace`, ~500/s on `main`). On unfixed
/// code this test does not merely run slowly — it fails outright, because
/// 200,000 / 500/s = 400s blows the 60s budget by 6x.
#[tokio::test]
#[file_serial]
async fn bulk_commit_larger_than_buffer_drains_at_page_speed() -> anyhow::Result<()> {
    const TOTAL: u64 = 200_000;

    let (_tracing_guard, catch_up_spans) = SpanCounter::install("obix.persistent_cache.catch_up");

    let pool = init_pool().await?;
    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .event_buffer_size(1_000)
            .event_cache_size(1_000)
            .backfill_page_size(1_000)
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let mut listener = outbox.listen_persisted(None);

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_all_persisted(&mut op, (0..TOTAL).map(TestEvent::Ping))
        .await?;
    op.commit().await?;

    let start = std::time::Instant::now();
    let received = tokio::time::timeout(std::time::Duration::from_secs(60), async {
        let mut events = Vec::with_capacity(TOTAL as usize);
        for _ in 0..TOTAL {
            let event = listener
                .next()
                .await
                .expect("stream closed early")
                .expect("undecodable event");
            events.push(event);
        }
        events
    })
    .await
    .map_err(|_| anyhow::anyhow!("did not drain {TOTAL} events within 60s"))?;
    let elapsed = start.elapsed();
    eprintln!("bulk_commit_larger_than_buffer_drains_at_page_speed: {TOTAL} events in {elapsed:?}");

    assert_eq!(received.len(), TOTAL as usize);
    let mut last_sequence = None;
    for (i, event) in received.iter().enumerate() {
        assert_eq!(event.payload, Some(TestEvent::Ping(i as u64)));
        if let Some(last) = last_sequence {
            assert!(
                u64::from(event.sequence) > last,
                "sequences must be strictly increasing"
            );
        }
        last_sequence = Some(u64::from(event.sequence));
    }

    // Proves the drain above actually went through the catch-up path (not
    // some other mechanism quietly delivering everything) and that it never
    // ran more than one catch-up task at a time for this single contiguous
    // backlog: one continuous task pages from the cursor to the head with
    // nothing in between to end it early.
    assert_eq!(
        catch_up_spans.load(Ordering::SeqCst),
        1,
        "expected exactly one catch-up task for one uninterrupted backlog"
    );

    Ok(())
}

/// Counted observation, not just correctness: repeated truncated commits are
/// a *standing* trigger condition (the cache loop re-evaluates "behind by at
/// least a page" on every cache-fill drain, dozens of times over this test),
/// which is exactly the shape that let a one-shot waiter accumulate
/// unboundedly elsewhere. `catch_up: Option<OwnedTaskHandle>` gates spawning
/// on a single loop-local slot, so this must stay low regardless of how many
/// times the trigger condition re-fires — never one task per commit, and
/// never one per re-evaluation.
#[tokio::test]
#[file_serial]
async fn catch_up_does_not_accumulate_across_repeated_truncated_commits() -> anyhow::Result<()> {
    const COMMITS: u64 = 20;
    const PER_COMMIT: u64 = 2_000;
    const TOTAL: u64 = COMMITS * PER_COMMIT;

    let (_tracing_guard, catch_up_spans) = SpanCounter::install("obix.persistent_cache.catch_up");

    let pool = init_pool().await?;
    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .event_buffer_size(200)
            .backfill_page_size(100)
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let mut listener = outbox.listen_persisted(None);

    // Every commit here (2,000 events against a 200-wide buffer) truncates
    // on its own — twenty separate re-arms of the same trigger condition,
    // not one big backlog.
    for commit in 0..COMMITS {
        let mut op = outbox.begin_op().await?;
        outbox
            .publish_all_persisted(
                &mut op,
                (0..PER_COMMIT).map(|n| TestEvent::Ping(commit * PER_COMMIT + n)),
            )
            .await?;
        op.commit().await?;
    }

    let received = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        let mut events = Vec::with_capacity(TOTAL as usize);
        for _ in 0..TOTAL {
            let event = listener
                .next()
                .await
                .expect("stream closed early")
                .expect("undecodable event");
            events.push(event);
        }
        events
    })
    .await
    .map_err(|_| anyhow::anyhow!("did not drain {TOTAL} events within 30s"))?;

    assert_eq!(received.len(), TOTAL as usize);
    let mut last_sequence = None;
    for (i, event) in received.iter().enumerate() {
        assert_eq!(event.payload, Some(TestEvent::Ping(i as u64)));
        if let Some(last) = last_sequence {
            assert!(
                u64::from(event.sequence) > last,
                "sequences must be strictly increasing (no duplicate, no reorder)"
            );
        }
        last_sequence = Some(u64::from(event.sequence));
    }

    let spawned = catch_up_spans.load(Ordering::SeqCst);
    assert!(
        spawned >= 1,
        "catch-up never engaged — this test would pass vacuously without it"
    );
    assert!(
        spawned <= 5,
        "catch-up spawned {spawned} times for {COMMITS} truncated commits — \
         it must coalesce repeated triggers into a bounded few tasks, not \
         one per commit (would be {COMMITS}) or one per re-evaluation \
         (would be in the hundreds)"
    );

    Ok(())
}

/// The notified path: a bulk commit on one outbox instance must be caught up
/// by a second instance listening on the same table without an unbounded
/// `fetch_notified_range` materialising the whole range.
#[tokio::test]
#[file_serial]
async fn remote_bulk_commit_is_caught_up_without_whole_range_fetch() -> anyhow::Result<()> {
    const TOTAL: u64 = 20_000;

    let pool = init_pool().await?;
    let config = MailboxConfig::builder()
        .event_buffer_size(1_000)
        .event_cache_size(1_000)
        .backfill_page_size(1_000)
        .build()
        .expect("Couldn't build MailboxConfig");

    let outbox_a = init_outbox::<TestEvent>(&pool, config.clone()).await?;
    let outbox_b = Outbox::<TestEvent, helpers::TestTables>::init(&pool, config).await?;

    let mut listener_b = outbox_b.listen_persisted(None);

    let mut op = outbox_a.begin_op().await?;
    outbox_a
        .publish_all_persisted(&mut op, (0..TOTAL).map(TestEvent::Ping))
        .await?;
    op.commit().await?;

    let received = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        let mut events = Vec::with_capacity(TOTAL as usize);
        for _ in 0..TOTAL {
            let event = listener_b
                .next()
                .await
                .expect("stream closed early")
                .expect("undecodable event");
            events.push(event);
        }
        events
    })
    .await
    .map_err(|_| anyhow::anyhow!("did not catch up {TOTAL} events within 20s"))?;

    assert_eq!(received.len(), TOTAL as usize);
    for (i, event) in received.iter().enumerate() {
        assert_eq!(event.payload, Some(TestEvent::Ping(i as u64)));
    }

    Ok(())
}

/// The budget: a `post_commit` truncated to `broadcast_budget` events must
/// still deliver every event — the remainder is the catch-up task's job,
/// not a loss.
#[tokio::test]
#[file_serial]
async fn truncated_post_commit_still_delivers_everything() -> anyhow::Result<()> {
    const TOTAL: u64 = 5_000;

    let pool = init_pool().await?;
    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .event_buffer_size(200)
            .backfill_page_size(100)
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let mut listener = outbox.listen_persisted(None);

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_all_persisted(&mut op, (0..TOTAL).map(TestEvent::Ping))
        .await?;
    op.commit().await?;

    let received = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let mut events = Vec::with_capacity(TOTAL as usize);
        for _ in 0..TOTAL {
            let event = listener
                .next()
                .await
                .expect("stream closed early")
                .expect("undecodable event");
            events.push(event);
        }
        events
    })
    .await
    .map_err(|_| anyhow::anyhow!("did not deliver {TOTAL} events within 10s"))?;

    assert_eq!(received.len(), TOTAL as usize);
    for (i, event) in received.iter().enumerate() {
        assert_eq!(event.payload, Some(TestEvent::Ping(i as u64)));
    }

    // Independently confirm nothing is missing from persistent storage: a
    // fresh listener from the very beginning must also see everything, in
    // order — the backfill path, unaffected by this change.
    let mut replay = outbox.listen_persisted(Some(EventSequence::BEGIN));
    let replayed = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let mut events = Vec::with_capacity(TOTAL as usize);
        for _ in 0..TOTAL {
            let event = replay
                .next()
                .await
                .expect("stream closed early")
                .expect("undecodable event");
            events.push(event);
        }
        events
    })
    .await
    .map_err(|_| anyhow::anyhow!("replay from BEGIN did not see {TOTAL} events within 10s"))?;
    assert_eq!(replayed.len(), TOTAL as usize);

    Ok(())
}

/// Trim safety: a catch-up page delivered in order must be fully consumed
/// by the contiguity walk (and broadcast) before the cache trim can evict
/// it, even when the configured cache is much smaller than one page.
#[tokio::test]
#[file_serial]
async fn catch_up_page_survives_a_cache_smaller_than_the_page() -> anyhow::Result<()> {
    const TOTAL: u64 = 20_000;

    let pool = init_pool().await?;
    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .event_buffer_size(1_000)
            .event_cache_size(50)
            .event_cache_trim_percent(50)
            .backfill_page_size(500)
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let mut listener = outbox.listen_persisted(None);

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_all_persisted(&mut op, (0..TOTAL).map(TestEvent::Ping))
        .await?;
    op.commit().await?;

    let received = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        let mut events = Vec::with_capacity(TOTAL as usize);
        for _ in 0..TOTAL {
            let event = listener
                .next()
                .await
                .expect("stream closed early")
                .expect("undecodable event");
            events.push(event);
        }
        events
    })
    .await
    .map_err(|_| anyhow::anyhow!("did not deliver {TOTAL} events within 20s"))?;

    assert_eq!(received.len(), TOTAL as usize);
    for (i, event) in received.iter().enumerate() {
        assert_eq!(event.payload, Some(TestEvent::Ping(i as u64)));
    }

    Ok(())
}

/// A bulk insert still in flight is a hole, not a catch-up loop: the
/// catch-up's empty first read must hand over to the existing grace-gated
/// stall path rather than spinning, and no placeholder may be written while
/// the writer that owns the gap is still live.
#[tokio::test]
#[file_serial]
async fn in_flight_bulk_insert_costs_one_probe_then_waits() -> anyhow::Result<()> {
    const BURNED: u64 = 3_000;

    let pool = init_pool().await?;
    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let mut listener = outbox.listen_persisted(None);

    // A raw writer burns BURNED sequences and stays in flight.
    let mut tx = pool.begin().await?;
    for n in 0..BURNED {
        sqlx::query("INSERT INTO persistent_outbox_events (payload) VALUES ($1::jsonb)")
            .bind(format!(r#"{{"Ping": {n}}}"#))
            .execute(&mut *tx)
            .await?;
    }

    // …while sequence BURNED + 1 commits through the outbox, landing well
    // past the catch-up threshold.
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(BURNED))
        .await?;
    op.commit().await?;

    // Nothing may be delivered while the writer holding the gap is live —
    // neither a real event (contiguity-gated behind the gap) nor a
    // placeholder (the gap is unproven while the writer runs).
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(500), listener.next())
            .await
            .is_err(),
        "no delivery may occur while the in-flight writer still holds the gap"
    );

    // The writer commits for real: its rows are now visible, and the
    // existing grace-gated stall path (or, if re-armed in time, the
    // catch-up) drains them without ever needing an abandonment proof.
    tx.commit().await?;

    let received = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        let mut events = Vec::with_capacity((BURNED + 1) as usize);
        for _ in 0..=BURNED {
            let event = listener
                .next()
                .await
                .expect("stream closed early")
                .expect("undecodable event");
            events.push(event);
        }
        events
    })
    .await
    .map_err(|_| anyhow::anyhow!("did not deliver the burned range after the writer committed"))?;

    assert_eq!(received.len(), (BURNED + 1) as usize);
    for (i, event) in received.iter().take(BURNED as usize).enumerate() {
        assert_eq!(event.payload, Some(TestEvent::Ping(i as u64)));
        assert_eq!(u64::from(event.sequence), (i + 1) as u64);
    }
    let last = received.last().expect("checked len above");
    assert_eq!(last.payload, Some(TestEvent::Ping(BURNED)));
    assert_eq!(u64::from(last.sequence), BURNED + 1);

    Ok(())
}

/// Regression (Bugbot on this PR): a catch-up that merely re-discovers a
/// hole a GapFiller episode is already working must never restart that
/// episode's grace clock. The original code sent `GapFillRequest::StallCleared`
/// unconditionally whenever `StartCatchUp` fired — including a catch-up
/// re-armed by a notification unrelated to the hole (e.g. a wide, distant
/// commit, or even a debounced echo of the process's own earlier commits)
/// — so every such interference pushed the abandoned sequence's fill
/// further out. Under sustained concurrent write load this never
/// terminates: "an abandoned sequence never becomes fillable, and every
/// listener stays blocked."
///
/// This is a timing proof, not a count: the fix moved `StallCleared` off
/// catch-up *start* and onto catch-up *outcome*, sent only when the
/// outcome actually differs from what is already reported (see the
/// `catch_up_done_rx` arm in `spawn_cache_loop`) — so a catch-up that lands
/// back on the exact position already reported must leave that episode's
/// grace clock untouched, no matter how many times it re-fires. Proving
/// that requires firing it repeatedly against a wall-clock deadline, which
/// is why this test measures elapsed time rather than counting spans.
#[tokio::test]
#[file_serial]
async fn repeated_unrelated_notifications_do_not_delay_an_unresolved_holes_grace()
-> anyhow::Result<()> {
    let grace = std::time::Duration::from_secs(2);

    let pool = init_pool().await?;
    let config = MailboxConfig::builder()
        .backfill_page_size(1)
        .gap_fill_grace(grace)
        .build()
        .expect("Couldn't build MailboxConfig");
    let outbox = init_outbox::<TestEvent>(&pool, config.clone()).await?;

    let mut listener = outbox.listen_persisted(None);

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(0))
        .await?;
    op.commit().await?;
    let event = tokio::time::timeout(std::time::Duration::from_secs(2), listener.next())
        .await?
        .expect("first event")?;
    assert!(matches!(event.payload, Some(TestEvent::Ping(0))));

    // Burn seq 2 permanently — no transaction ever holds it, so it is
    // abandoned from the moment it is allocated (matching
    // `gap_fill_waits_for_grace_period`'s approach in tests/outbox.rs).
    let stall_reported_at = tokio::time::Instant::now();
    sqlx::query!("SELECT nextval('persistent_outbox_events_sequence_seq')")
        .fetch_one(&pool)
        .await?;

    // seq 3 commits through the outbox, stalling the cursor at 1. With
    // backfill_page_size(1), distance (2) reaches the threshold
    // immediately, so catch-up engages right away, discovers the hole, and
    // hands it to the GapFiller's grace-gated episode.
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(1))
        .await?;
    op.commit().await?;

    // Nothing before the grace period, matching the unmodified stall path
    // (`gap_fill_waits_for_grace_period`).
    assert!(
        tokio::time::timeout(grace / 2, listener.next())
            .await
            .is_err(),
        "no premature placeholder before the grace period elapses"
    );

    // While the episode's grace clock is running, repeatedly insert real
    // rows well past the hole and manually emit the same `pg_notify` a
    // second instance's own debounced notifier would send for them —
    // exactly what an unrelated concurrent bulk commit looks like from
    // this outbox's perspective, without spinning up a second `Outbox`
    // (whose own auto-registered sequencer would independently discover
    // and no-grace Historical-fill this same shared-table hole, racing the
    // very episode this test observes). Raw SQL, not `publish_all_persisted`,
    // for the same reason. Each notified range is wide enough to bypass
    // `fetch_notified_range` and reach the `catch_up_exhausted_at` re-arm
    // paths, re-triggering `StartCatchUp` roughly every 150ms for nearly
    // the whole grace window. If starting a catch-up ever cleared the
    // episode, every one of these would push the eventual delivery further
    // out.
    let mut inserted_up_to = 3u64;
    while stall_reported_at.elapsed() < grace.mul_f32(0.9) {
        let batch_start = inserted_up_to + 1;
        let batch_end = inserted_up_to + 10;
        for n in batch_start..=batch_end {
            sqlx::query("INSERT INTO persistent_outbox_events (payload) VALUES ($1::jsonb)")
                .bind(format!(r#"{{"Ping": {n}}}"#))
                .execute(&pool)
                .await?;
        }
        inserted_up_to = batch_end;
        sqlx::query(&format!(
            "SELECT pg_notify('persistent_outbox_events', \
             '{{\"min_sequence\": {batch_start}, \"max_sequence\": {batch_end}}}')"
        ))
        .execute(&pool)
        .await?;
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    }

    // A generous safety net so a genuinely broken build fails outright
    // rather than hanging; the real assertion is the elapsed-time check
    // below, which is tight enough to tell "one grace period" from
    // "restarted on the last interference".
    let gap_event = tokio::time::timeout(std::time::Duration::from_secs(10), listener.next())
        .await
        .map_err(|_| anyhow::anyhow!("gap-filled placeholder never arrived"))?
        .expect("gap-filled placeholder")?;
    let elapsed = stall_reported_at.elapsed();
    assert!(
        gap_event.payload.is_none(),
        "gap-filled event should have None payload"
    );
    assert!(
        elapsed < grace + std::time::Duration::from_millis(800),
        "placeholder arrived after {elapsed:?} (grace was {grace:?}) — well past one \
         grace period from the original stall report, meaning an interfering \
         notification reset the episode's grace clock"
    );

    let real_event = tokio::time::timeout(std::time::Duration::from_secs(2), listener.next())
        .await?
        .expect("real event after the gap")?;
    assert!(matches!(real_event.payload, Some(TestEvent::Ping(1))));

    Ok(())
}
