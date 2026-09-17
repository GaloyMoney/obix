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

/// A handle to one watched span name's counted observations: how many
/// spans of that name were created, and (when a field was requested) the
/// sum of a named `u64` field across every one of them.
#[derive(Clone)]
struct SpanWatch {
    count: Arc<AtomicUsize>,
    field_sum: Arc<AtomicU64>,
}

impl SpanWatch {
    fn count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    fn field_sum(&self) -> u64 {
        self.field_sum.load(Ordering::SeqCst)
    }
}

struct WatchedSpan {
    name: &'static str,
    field: Option<&'static str>,
    handle: SpanWatch,
}

/// Counts spans by name without `tracing-subscriber` (not a workspace
/// dependency). One instance must watch *every* name a test cares about:
/// `set_default` replaces the thread-local dispatcher rather than layering.
struct SpanCounter {
    watched: Vec<WatchedSpan>,
    /// span id -> index into `watched`, for spans matching a name whose
    /// field is being tracked (so `record()` knows which sum to add to).
    tracked_ids: std::sync::Mutex<std::collections::HashMap<u64, usize>>,
    next_id: AtomicU64,
}

/// Sums a field across matching spans, from both `Attributes` (set at
/// creation, e.g. `memory_feed`'s `rows`) and `Record` (set later, e.g.
/// `catch_up`'s `rows`).
struct FieldSum<'a> {
    field_name: &'static str,
    sum: &'a Arc<AtomicU64>,
}

impl tracing::field::Visit for FieldSum<'_> {
    fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if field.name() == self.field_name {
            self.sum.fetch_add(value, Ordering::SeqCst);
        }
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        if field.name() == self.field_name && value >= 0 {
            self.sum.fetch_add(value as u64, Ordering::SeqCst);
        }
    }
}

impl SpanCounter {
    /// Watch a single span name, count only.
    fn install(name: &'static str) -> (tracing::subscriber::DefaultGuard, SpanWatch) {
        let (guard, mut handles) = Self::install_many(&[(name, None)]);
        (guard, handles.remove(0))
    }

    /// Watch several `(name, field)` pairs at once — `field: None` when
    /// only the count matters for that name. Returns one handle per pair,
    /// in the same order.
    fn install_many(
        watch: &[(&'static str, Option<&'static str>)],
    ) -> (tracing::subscriber::DefaultGuard, Vec<SpanWatch>) {
        let watched: Vec<WatchedSpan> = watch
            .iter()
            .map(|&(name, field)| WatchedSpan {
                name,
                field,
                handle: SpanWatch {
                    count: Arc::new(AtomicUsize::new(0)),
                    field_sum: Arc::new(AtomicU64::new(0)),
                },
            })
            .collect();
        let handles = watched.iter().map(|w| w.handle.clone()).collect();
        let subscriber = SpanCounter {
            watched,
            tracked_ids: std::sync::Mutex::new(std::collections::HashMap::new()),
            next_id: AtomicU64::new(1),
        };
        (tracing::subscriber::set_default(subscriber), handles)
    }
}

impl tracing::Subscriber for SpanCounter {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        let id_num = self.next_id.fetch_add(1, Ordering::SeqCst);
        let name = span.metadata().name();
        if let Some((idx, watched)) = self
            .watched
            .iter()
            .enumerate()
            .find(|(_, w)| w.name == name)
        {
            watched.handle.count.fetch_add(1, Ordering::SeqCst);
            if let Some(field_name) = watched.field {
                self.tracked_ids
                    .lock()
                    .expect("lock poisoned")
                    .insert(id_num, idx);
                let mut visitor = FieldSum {
                    field_name,
                    sum: &watched.handle.field_sum,
                };
                span.record(&mut visitor);
            }
        }
        tracing::span::Id::from_u64(id_num)
    }

    fn record(&self, span: &tracing::span::Id, values: &tracing::span::Record<'_>) {
        let idx = self
            .tracked_ids
            .lock()
            .expect("lock poisoned")
            .get(&span.into_u64())
            .copied();
        if let Some(idx) = idx
            && let Some(field_name) = self.watched[idx].field
        {
            let mut visitor = FieldSum {
                field_name,
                sum: &self.watched[idx].handle.field_sum,
            };
            values.record(&mut visitor);
        }
    }
    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
    fn event(&self, _event: &tracing::Event<'_>) {}
    fn enter(&self, _span: &tracing::span::Id) {}
    fn exit(&self, _span: &tracing::span::Id) {}
}

/// The headline case: one commit larger than the cache-fill broadcast drains
/// at page speed, not the ~500/s gap-fill-grace cadence `main` manages (which
/// needs 400s and blows the 60s budget 6x). Local, so memory feeds it.
#[tokio::test]
#[file_serial]
async fn bulk_commit_larger_than_buffer_drains_at_page_speed() -> anyhow::Result<()> {
    const TOTAL: u64 = 200_000;

    let (_tracing_guard, spans) = SpanCounter::install_many(&[
        ("obix.persistent_cache.catch_up", None),
        ("obix.persistent_cache.memory_feed", None),
    ]);
    let catch_up_spans = spans[0].clone();
    let memory_feed_spans = spans[1].clone();

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

    // Pins *which* path drained it, so the timing above cannot pass via some
    // other mechanism quietly delivering everything.
    assert_eq!(
        catch_up_spans.count(),
        0,
        "a local commit already held in memory must never be re-read from the database"
    );
    assert_eq!(
        memory_feed_spans.count(),
        1,
        "expected exactly one memory-feed span for one uninterrupted local batch"
    );

    Ok(())
}

/// Counted observation against a *standing* trigger: twenty commits, each
/// re-arming the condition, must produce exactly one `memory_feed` span each —
/// never one per page or per loop iteration.
#[tokio::test]
#[file_serial]
async fn local_commits_are_fed_from_memory_one_span_per_batch() -> anyhow::Result<()> {
    const COMMITS: u64 = 20;
    const PER_COMMIT: u64 = 2_000;
    const TOTAL: u64 = COMMITS * PER_COMMIT;

    let (_tracing_guard, spans) = SpanCounter::install_many(&[
        ("obix.persistent_cache.catch_up", None),
        ("obix.persistent_cache.memory_feed", None),
    ]);
    let catch_up_spans = spans[0].clone();
    let memory_feed_spans = spans[1].clone();

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

    assert_eq!(
        catch_up_spans.count(),
        0,
        "every commit here is a local batch already in memory — the database \
         catch-up branch must never engage"
    );
    assert_eq!(
        memory_feed_spans.count(),
        COMMITS as usize,
        "expected exactly one memory-feed span per commit ({COMMITS} commits) — \
         never one per page and never one per loop re-evaluation"
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

/// A commit far larger than a small `event_buffer_size` must still deliver
/// every event — fed page by page from memory, never truncated or dropped.
#[tokio::test]
#[file_serial]
async fn small_buffer_large_local_commit_is_fed_from_memory() -> anyhow::Result<()> {
    const TOTAL: u64 = 5_000;

    let (_tracing_guard, spans) = SpanCounter::install_many(&[
        ("obix.persistent_cache.catch_up", None),
        ("obix.persistent_cache.memory_feed", None),
    ]);
    let catch_up_spans = spans[0].clone();
    let memory_feed_spans = spans[1].clone();

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

    assert_eq!(
        catch_up_spans.count(),
        0,
        "the whole batch is already in memory — the database catch-up branch \
         must never engage"
    );
    assert_eq!(
        memory_feed_spans.count(),
        1,
        "expected exactly one memory-feed span for one uninterrupted local batch, \
         however many pages it took to drain"
    );

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

/// Trim safety: a cache far smaller than one page must not lose a fed page's
/// tail. If trim ever does evict one, the feeder's re-derive-from-the-real-
/// cursor rule re-feeds it from memory, never from the database.
#[tokio::test]
#[file_serial]
async fn catch_up_page_survives_a_cache_smaller_than_the_page() -> anyhow::Result<()> {
    const TOTAL: u64 = 20_000;

    let (_tracing_guard, catch_up_spans) = SpanCounter::install("obix.persistent_cache.catch_up");

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

    assert_eq!(
        catch_up_spans.count(),
        0,
        "a small cache trimming an unconsumed tail must be healed by re-feeding \
         from memory — not by falling back to a database read"
    );

    Ok(())
}

/// A bulk insert still in flight is a hole, not a backlog: the empty first
/// read must hand over to the grace-gated stall path rather than spinning,
/// and no placeholder may be written while that writer is still live.
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

/// Regression: a catch-up that merely re-discovers a hole an episode is already
/// working must not restart its grace clock, or under sustained write load an
/// abandoned sequence never becomes fillable. Hence a timing proof.
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

    // seq 3 commits, stalling the cursor at 1. With backfill_page_size(1) the
    // distance reaches the threshold at once, so catch-up engages, finds the
    // hole, and hands it to the grace-gated episode.
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

    // Interfere every 150ms for most of the grace window, each notified range
    // wide enough to re-arm the exhaustion and re-fire a catch-up. Raw SQL,
    // since a second `Outbox` would Historical-fill this hole and race us.
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

    // A safety net so a broken build fails rather than hangs; the real
    // assertion is the elapsed-time check below.
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

/// Interleaved concurrent commits: B steals a sequence from the middle of A's
/// 20,000 and commits ~3s later. Considering only the oldest pending batch's
/// front, A parks behind B's hole and B queues behind A until the 10s grace.
#[tokio::test]
#[file_serial]
async fn interleaved_local_commit_below_a_parked_batch_is_fed_from_memory() -> anyhow::Result<()> {
    use es_entity::hooks::{BoxFuture, HookOperation};
    use obix::out::{PersistentOutboxEvent, PostPersistHook};

    const A_TOTAL: u64 = 20_000;
    const GRACE: std::time::Duration = std::time::Duration::from_secs(10);
    const B_HOLD: std::time::Duration = std::time::Duration::from_secs(3);

    /// Holds a transaction open for `B_HOLD` whenever it sees exactly one
    /// event — `publish_persisted_in_op`'s single-event commit, never one
    /// of `publish_all_persisted`'s multi-thousand-event chunks.
    struct SleepOnSingleton;

    impl PostPersistHook<TestEvent> for SleepOnSingleton {
        fn on_persisted<'a>(
            &'a self,
            _op: &'a mut HookOperation<'_>,
            events: &'a [PersistentOutboxEvent<TestEvent>],
        ) -> BoxFuture<'a, Result<(), sqlx::Error>> {
            Box::pin(async move {
                if events.len() == 1 {
                    tokio::time::sleep(B_HOLD).await;
                }
                Ok(())
            })
        }
    }

    let (_tracing_guard, catch_up_spans) = SpanCounter::install("obix.persistent_cache.catch_up");

    let pool = init_pool().await?;
    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .event_buffer_size(1_000)
            .event_cache_size(1_000)
            .backfill_page_size(1_000)
            .gap_fill_grace(GRACE)
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;
    outbox.add_post_persist_hook(SleepOnSingleton);

    let mut listener = outbox.listen_persisted(None);

    let outbox_a = outbox.clone();
    let a_handle = tokio::spawn(async move {
        let mut op = outbox_a.begin_op().await.expect("begin op for A");
        outbox_a
            .publish_all_persisted(&mut op, (0..A_TOTAL).map(TestEvent::Ping))
            .await
            .expect("publish A");
        op.commit().await.expect("commit A");
    });

    // A head start, so B steals from the middle of A's range: a hole *inside*
    // an already-pending batch, not merely one ahead of it.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let outbox_b = outbox.clone();
    let b_start = tokio::time::Instant::now();
    let b_handle = tokio::spawn(async move {
        let mut op = outbox_b.begin_op().await.expect("begin op for B");
        outbox_b
            .publish_persisted_in_op(&mut op, TestEvent::Ping(A_TOTAL))
            .await
            .expect("publish B");
        op.commit().await.expect("commit B");
    });

    let received = tokio::time::timeout(GRACE, async {
        let mut events = Vec::with_capacity((A_TOTAL + 1) as usize);
        for _ in 0..=A_TOTAL {
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
    .map_err(|_| {
        anyhow::anyhow!(
            "did not deliver {} events within the grace period",
            A_TOTAL + 1
        )
    })?;
    let elapsed = b_start.elapsed();

    a_handle.await?;
    b_handle.await?;

    assert_eq!(received.len(), (A_TOTAL + 1) as usize);
    let mut last_sequence: Option<u64> = None;
    let mut seen = std::collections::HashSet::with_capacity(received.len());
    for event in &received {
        if let Some(last) = last_sequence {
            assert!(
                u64::from(event.sequence) > last,
                "sequences must be strictly increasing"
            );
        }
        last_sequence = Some(u64::from(event.sequence));
        let TestEvent::Ping(n) = event.payload.clone().expect("no placeholders expected");
        assert!(
            seen.insert(n),
            "payload {n} delivered more than once — not exactly-once"
        );
    }
    assert_eq!(
        seen.len(),
        (A_TOTAL + 1) as usize,
        "every payload 0..={A_TOTAL} must be delivered exactly once"
    );

    // A small multiple of B's hold, nowhere near the grace — on the FIFO bug
    // the grace-gated fill dominates and this fails outright.
    assert!(
        elapsed < B_HOLD + std::time::Duration::from_secs(3),
        "took {elapsed:?} after B's commit started (hold was {B_HOLD:?}) — memory should \
         feed B the instant it arrives, not wait anywhere near the {GRACE:?} grace period"
    );

    assert_eq!(
        catch_up_spans.count(),
        0,
        "the single stolen sequence is a hole memory resolves the moment B commits — \
         the database catch-up branch, gated on a full page of distance, must never engage"
    );

    Ok(())
}

/// The database branch is *bounded*: 1,500 remote rows below a pending 20,000
/// local batch are read only up to where that batch begins. Counted on the
/// span's `rows`, so an unbounded read fails rather than reading twice.
#[tokio::test]
#[file_serial]
async fn remote_gap_below_a_parked_batch_is_read_bounded_then_memory_resumes() -> anyhow::Result<()>
{
    const BURNED: u64 = 1_500;
    const A_TOTAL: u64 = 20_000;
    let grace = std::time::Duration::from_secs(2);

    let (_tracing_guard, spans) = SpanCounter::install_many(&[
        ("obix.persistent_cache.catch_up", Some("rows")),
        ("obix.persistent_cache.memory_feed", None),
    ]);
    let catch_up_spans = spans[0].clone();
    let memory_feed_spans = spans[1].clone();

    let pool = init_pool().await?;
    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .event_buffer_size(1_000)
            .event_cache_size(1_000)
            .backfill_page_size(1_000)
            .gap_fill_grace(grace)
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let mut listener = outbox.listen_persisted(None);

    // A remote writer's transaction: BURNED sequences allocated, stays
    // open (uncommitted) — same shape as T6.
    let mut tx = pool.begin().await?;
    for n in 0..BURNED {
        sqlx::query("INSERT INTO persistent_outbox_events (payload) VALUES ($1::jsonb)")
            .bind(format!(r#"{{"Ping": {n}}}"#))
            .execute(&mut *tx)
            .await?;
    }

    // A local batch commits through the outbox, landing entirely above the
    // still-open gap — straight into the feeder's `pending`.
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_all_persisted(&mut op, (0..A_TOTAL).map(|n| TestEvent::Ping(BURNED + n)))
        .await?;
    op.commit().await?;

    // Nothing may be delivered while the gap below the pending batch is
    // still an unproven, live writer's hole.
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(500), listener.next())
            .await
            .is_err(),
        "no delivery may occur while the remote writer still holds the gap"
    );

    // The remote writer commits for real, and fires the same `pg_notify` a
    // second instance's own debounced notifier would send for it.
    tx.commit().await?;
    sqlx::query(&format!(
        "SELECT pg_notify('persistent_outbox_events', \
         '{{\"min_sequence\": 1, \"max_sequence\": {BURNED}}}')"
    ))
    .execute(&pool)
    .await?;

    let total = BURNED + A_TOTAL;
    let received = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let mut events = Vec::with_capacity(total as usize);
        for _ in 0..total {
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
    .map_err(|_| anyhow::anyhow!("did not deliver {total} events within 10s"))?;

    assert_eq!(received.len(), total as usize);
    let mut last_sequence: Option<u64> = None;
    for event in &received {
        if let Some(last) = last_sequence {
            assert!(
                u64::from(event.sequence) > last,
                "sequences must be strictly increasing"
            );
        }
        last_sequence = Some(u64::from(event.sequence));
    }
    for (i, event) in received.iter().skip(BURNED as usize).enumerate() {
        assert_eq!(event.payload, Some(TestEvent::Ping(BURNED + i as u64)));
    }

    // Exactly the rows memory could not supply — counting the batch's rows
    // above them would mean the bound was never applied.
    assert_eq!(
        catch_up_spans.field_sum(),
        BURNED,
        "the database catch-up must read exactly the rows memory could not supply, \
         not the local batch's rows above them"
    );
    assert!(
        catch_up_spans.count() >= 1,
        "the gap must be discovered by at least one database probe — a vacuous pass \
         here would mean the gap was never actually exercised"
    );
    assert!(
        memory_feed_spans.count() >= 1,
        "the local batch above the gap must still be fed from memory once the gap clears"
    );

    Ok(())
}
