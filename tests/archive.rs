mod helpers;

use std::sync::Arc;

use chrono::{DateTime, NaiveDate, Utc};
use es_entity::clock::ClockHandle;
use futures::stream::StreamExt;
use obix::{
    ArchiveConfig, Compression, DEFAULT_PARTITION_WIDTH, DailyRetentionBoundary,
    EventArchiveStorage, EventArchiver, EventSequence, InMemoryArchiveStorage, MailboxConfig,
    out::Outbox,
};
use serde::{Deserialize, Serialize};
use serial_test::file_serial;

use helpers::{TestTables, init_pool, wipeout_outbox_job_tables, wipeout_outbox_tables};

async fn relation_exists(pool: &sqlx::PgPool, name: &str) -> anyhow::Result<bool> {
    let row = sqlx::query("SELECT to_regclass($1) IS NOT NULL AS present")
        .bind(name)
        .fetch_one(pool)
        .await?;
    use sqlx::Row;
    Ok(row.get::<bool, _>("present"))
}

const DAY: std::time::Duration = std::time::Duration::from_secs(24 * 60 * 60);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum TestEvent {
    Ping(u64),
}

fn day(n: u32) -> DateTime<Utc> {
    // 2026-07-20 is "day 0"; return noon of day n to stay clear of
    // midnight boundaries.
    NaiveDate::from_ymd_opt(2026, 7, 20)
        .unwrap()
        .checked_add_days(chrono::Days::new(n as u64))
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap()
        .and_utc()
}

async fn publish(outbox: &Outbox<TestEvent, TestTables>, values: &[u64]) -> anyhow::Result<()> {
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_all_persisted(&mut op, values.iter().map(|v| TestEvent::Ping(*v)))
        .await?;
    op.commit().await?;
    Ok(())
}

struct Setup {
    pool: sqlx::PgPool,
    outbox: Outbox<TestEvent, TestTables>,
    storage: Arc<InMemoryArchiveStorage>,
    archive_config: ArchiveConfig,
    controller: es_entity::clock::ClockController,
}

async fn init_setup(retention_days: i64, boundaries_per_run: usize) -> anyhow::Result<Setup> {
    let pool = init_pool().await?;
    wipeout_outbox_tables(&pool).await?;

    let (clock, controller) = ClockHandle::manual_at(day(0));

    let storage = Arc::new(InMemoryArchiveStorage::new());
    let boundary = Arc::new(DailyRetentionBoundary::<TestTables>::new(
        &pool,
        chrono::Duration::days(retention_days),
        clock.clone(),
    ));
    let archive_config = ArchiveConfig::new(storage.clone(), boundary)
        .with_boundaries_per_run(boundaries_per_run)
        .with_clock(clock.clone());

    let config = MailboxConfig::builder()
        .clock(clock.clone())
        .archive(Some(archive_config.clone()))
        .build()?;
    let outbox = Outbox::<TestEvent, TestTables>::init(&pool, config).await?;

    Ok(Setup {
        pool,
        outbox,
        storage,
        archive_config,
        controller,
    })
}

// (live_sequences removed: archived rows are no longer row-level deleted,
//  so checking the live table after archiving is no longer meaningful.)

async fn manifest_rows(pool: &sqlx::PgPool) -> anyhow::Result<Vec<(String, i64, i64)>> {
    let rows = sqlx::query!(
        "SELECT path, min_sequence, max_sequence FROM persistent_outbox_archive_chunks ORDER BY min_sequence"
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| (r.path, r.min_sequence, r.max_sequence))
        .collect())
}

/// Collect the payloads of a full-history replay, asserting contiguity:
/// `Some(v)` per `Ping(v)`, `None` per placeholder.
async fn replay_from_beginning(
    outbox: &Outbox<TestEvent, TestTables>,
    expected_len: usize,
) -> Vec<Option<u64>> {
    let mut listener = outbox.listen_persisted(Some(EventSequence::BEGIN));
    let mut received = Vec::new();
    let mut expected_sequence = 1u64;
    while received.len() < expected_len {
        let event = tokio::time::timeout(std::time::Duration::from_secs(10), listener.next())
            .await
            .expect("timed out waiting for replayed event")
            .expect("stream ended during replay")
            .expect("undecodable event during replay");
        assert_eq!(
            u64::from(event.sequence),
            expected_sequence,
            "replay must be contiguous"
        );
        received.push(event.payload.as_ref().map(|TestEvent::Ping(v)| *v));
        expected_sequence += 1;
    }
    received
}

#[tokio::test]
#[file_serial]
async fn archives_settled_spans_to_object_storage() -> anyhow::Result<()> {
    let setup = init_setup(1, 1).await?;

    // Dates 0..2 in the archive window, date 3 = today.
    publish(&setup.outbox, &[1, 2]).await?;
    setup.controller.advance(DAY).await;
    publish(&setup.outbox, &[3]).await?;
    setup.controller.advance(DAY).await;
    publish(&setup.outbox, &[4, 5]).await?;
    setup.controller.advance(DAY).await;
    publish(&setup.outbox, &[6]).await?;

    let archiver = EventArchiver::<TestTables>::new(&setup.pool, setup.archive_config.clone());

    // Retention 1 day, today = date 3 → dates 0 and 1 eligible.
    // boundaries_per_run = 1: one span per run.
    let report = archiver.run_once().await?;
    assert_eq!(report.spans_archived, 1);
    assert_eq!(report.chunks_written, 1);
    assert_eq!(u64::from(report.watermark), 2);

    let chunks = manifest_rows(&setup.pool).await?;
    assert_eq!(chunks.len(), 1);
    assert_eq!((chunks[0].1, chunks[0].2), (1, 2));
    // The logical day lives in the path, not the manifest.
    assert_eq!(
        chunks[0].0,
        "2026-07-20/events-00000000000000000001-00000000000000000002.jsonl"
    );

    // Next run sweeps the second date span (seq 3).
    let report = archiver.run_once().await?;
    assert_eq!(report.spans_archived, 1);
    assert_eq!(u64::from(report.watermark), 3);

    // Nothing eligible left: day 2 and day 3 are inside the retention window.
    let report = archiver.run_once().await?;
    assert_eq!(report.spans_archived, 0);
    assert_eq!(report.chunks_written, 0);

    // The exported file holds exactly the day's events as JSONL.
    let data = setup
        .storage
        .get_sync("2026-07-20/events-00000000000000000001-00000000000000000002.jsonl")
        .expect("chunk file exists");
    let lines: Vec<&[u8]> = data
        .split(|&b| b == b'\n')
        .filter(|l| !l.is_empty())
        .collect();
    assert_eq!(lines.len(), 2);
    let first: serde_json::Value = serde_json::from_slice(lines[0])?;
    assert_eq!(first["sequence"], 1);
    assert_eq!(first["payload"]["Ping"], 1);

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn archive_chunks_can_be_gzip_compressed() -> anyhow::Result<()> {
    let mut setup = init_setup(2, 1).await?;
    setup.archive_config = setup.archive_config.with_compression(Compression::Gzip);

    publish(&setup.outbox, &[1, 2]).await?;
    setup.controller.advance(DAY).await;
    publish(&setup.outbox, &[3]).await?;
    setup.controller.advance(DAY).await;
    setup.controller.advance(DAY).await;

    let archiver = EventArchiver::<TestTables>::new(&setup.pool, setup.archive_config.clone());
    let report = archiver.run_once().await?;
    assert_eq!(report.chunks_written, 1);

    let chunks = manifest_rows(&setup.pool).await?;
    assert_eq!(
        chunks[0].0,
        "2026-07-20/events-00000000000000000001-00000000000000000002.jsonl.gz"
    );

    // Stored bytes are gzip...
    let data = setup
        .storage
        .get_sync(&chunks[0].0)
        .expect("chunk file exists");
    assert_eq!(&data[..2], &[0x1f, 0x8b], "gzip magic bytes");
    // ...decompressing to the day's JSONL.
    let mut plain = Vec::new();
    std::io::Read::read_to_end(&mut flate2::read::GzDecoder::new(&data[..]), &mut plain)?;
    let lines: Vec<&[u8]> = plain
        .split(|&b| b == b'\n')
        .filter(|l| !l.is_empty())
        .collect();
    assert_eq!(lines.len(), 2);
    let first: serde_json::Value = serde_json::from_slice(lines[0])?;
    assert_eq!(first["sequence"], 1);
    assert_eq!(first["payload"]["Ping"], 1);

    // Reads sniff the path, not the config: replay crosses the
    // archive → postgres seam.
    let replayed = replay_from_beginning(&setup.outbox, 3).await;
    assert_eq!(replayed, vec![Some(1), Some(2), Some(3)]);

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn replay_reads_mixed_plain_and_compressed_chunks() -> anyhow::Result<()> {
    let mut setup = init_setup(1, 1).await?;

    publish(&setup.outbox, &[1]).await?;
    setup.controller.advance(DAY).await;
    publish(&setup.outbox, &[2]).await?;
    setup.controller.advance(DAY).await;
    publish(&setup.outbox, &[3]).await?;
    setup.controller.advance(DAY).await;

    // First span archived plain...
    let archiver = EventArchiver::<TestTables>::new(&setup.pool, setup.archive_config.clone());
    archiver.run_once().await?;
    // ...second span compressed. Flipping the write config must not
    // affect readability of the plain chunk.
    setup.archive_config = setup.archive_config.with_compression(Compression::Gzip);
    let archiver = EventArchiver::<TestTables>::new(&setup.pool, setup.archive_config.clone());
    archiver.run_once().await?;

    let chunks = manifest_rows(&setup.pool).await?;
    assert!(chunks[0].0.ends_with(".jsonl"));
    assert!(chunks[1].0.ends_with(".jsonl.gz"));

    let replayed = replay_from_beginning(&setup.outbox, 3).await;
    assert_eq!(replayed, vec![Some(1), Some(2), Some(3)]);

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn listener_replays_across_archive_postgres_seam() -> anyhow::Result<()> {
    let setup = init_setup(1, 10).await?;

    publish(&setup.outbox, &[1, 2]).await?;
    setup.controller.advance(DAY).await;
    publish(&setup.outbox, &[3, 4]).await?;
    setup.controller.advance(DAY).await;
    publish(&setup.outbox, &[5]).await?;
    setup.controller.advance(DAY).await;
    publish(&setup.outbox, &[6, 7]).await?;

    let archiver = EventArchiver::<TestTables>::new(&setup.pool, setup.archive_config.clone());
    let report = archiver.run_once().await?;
    // Days 0 and 1 eligible (retention 1, today = day 3), swept in one run.
    assert_eq!(report.spans_archived, 2);
    assert_eq!(u64::from(report.watermark), 4);

    // A NEW outbox instance — the "new module replaying from day 0" case —
    // reads the archive first, then crosses into postgres mid-stream.
    let replay_outbox = Outbox::<TestEvent, TestTables>::init(
        &setup.pool,
        MailboxConfig::builder()
            .archive(Some(setup.archive_config.clone()))
            .build()?,
    )
    .await?;

    let received = replay_from_beginning(&replay_outbox, 7).await;
    assert_eq!(
        received,
        vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            Some(7)
        ]
    );

    // Live publish still lands on the same listener seamlessly.
    let mut listener = replay_outbox.listen_persisted(Some(EventSequence::BEGIN));
    let mut seen = 0;
    while seen < 7 {
        listener
            .next()
            .await
            .expect("stream ended")
            .expect("undecodable");
        seen += 1;
    }
    publish(&setup.outbox, &[8]).await?;
    let event = tokio::time::timeout(std::time::Duration::from_secs(10), listener.next())
        .await?
        .expect("stream ended")
        .expect("undecodable");
    assert_eq!(event.payload, Some(TestEvent::Ping(8)));

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn archive_export_materializes_sequence_gaps() -> anyhow::Result<()> {
    let setup = init_setup(2, 10).await?;

    publish(&setup.outbox, &[1]).await?;
    // Sequence 2: allocated by an INSERT in a transaction that rolls
    // back — a real gap in the sequence. (Publishing onto a dropped op
    // would not leave a gap: obix inserts at commit time.)
    {
        let mut tx = setup.pool.begin().await?;
        sqlx::query!("INSERT INTO persistent_outbox_events (payload) VALUES ('null')")
            .execute(&mut *tx)
            .await?;
        tx.rollback().await?;
    }
    publish(&setup.outbox, &[2]).await?;
    setup.controller.advance(DAY * 3).await;
    publish(&setup.outbox, &[3]).await?;

    let archiver = EventArchiver::<TestTables>::new(&setup.pool, setup.archive_config.clone());
    let report = archiver.run_once().await?;
    // Day 0 (seqs 1-3) eligible; the gap at sequence 2 is exported as a
    // placeholder line. Rows are not row-level deleted (pruning is
    // partition-level); only the watermark advances.
    assert_eq!(u64::from(report.watermark), 3);

    let received = replay_from_beginning(&setup.outbox, 4).await;
    assert_eq!(received, vec![Some(1), None, Some(2), Some(3)]);

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn archive_rerun_is_idempotent() -> anyhow::Result<()> {
    let setup = init_setup(2, 10).await?;

    publish(&setup.outbox, &[1, 2, 3]).await?;
    setup.controller.advance(DAY * 3).await;

    let archiver = EventArchiver::<TestTables>::new(&setup.pool, setup.archive_config.clone());
    let first = archiver.run_once().await?;
    assert_eq!(first.spans_archived, 1);
    assert_eq!(u64::from(first.watermark), 3);

    let storage_listing = setup.storage.list();

    // Re-running with no new eligible days changes nothing — same files,
    // same manifest, no re-export.
    let second = archiver.run_once().await?;
    assert_eq!(second.spans_archived, 0);
    assert_eq!(second.chunks_written, 0);
    assert_eq!(setup.storage.list(), storage_listing);
    assert_eq!(manifest_rows(&setup.pool).await?.len(), 1);

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn archiver_job_sweeps_settled_days() -> anyhow::Result<()> {
    let setup = init_setup(2, 1).await?;
    wipeout_outbox_job_tables(&setup.pool, "test-archiver-job").await?;

    publish(&setup.outbox, &[1, 2, 3]).await?;
    setup.controller.advance(DAY * 3).await;
    publish(&setup.outbox, &[4]).await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(setup.pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;
    setup
        .outbox
        .register_event_archiver(
            &mut jobs,
            obix::OutboxArchiverJobConfig::new(job::JobType::new("test-archiver-job")),
        )
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    jobs.start_poll().await?;

    // The job sweeps the one eligible day (seqs 1-3). Poll for the
    // manifest entry (not live_sequences): archived rows are not
    // row-level deleted, so only the watermark advancing proves the
    // sweep happened.
    let start = std::time::Instant::now();
    while manifest_rows(&setup.pool).await?.is_empty() {
        if start.elapsed() > std::time::Duration::from_secs(30) {
            panic!("archiver job did not sweep within timeout");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let received = replay_from_beginning(&setup.outbox, 4).await;
    assert_eq!(received, vec![Some(1), Some(2), Some(3), Some(4)]);

    jobs.shutdown()
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn register_event_archiver_requires_archive_config() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipeout_outbox_tables(&pool).await?;

    let outbox =
        Outbox::<TestEvent, TestTables>::init(&pool, MailboxConfig::builder().build()?).await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let result = outbox
        .register_event_archiver(
            &mut jobs,
            obix::OutboxArchiverJobConfig::new(job::JobType::new("test-archiver")),
        )
        .await;
    assert!(matches!(
        result.unwrap_err().downcast_ref::<obix::ArchiveError>(),
        Some(obix::ArchiveError::NotConfigured)
    ));

    Ok(())
}

/// Storage wrapper that lets `succeed_puts` puts through and then fails
/// the next `fail_puts` puts, to exercise mid-span storage outages.
struct FlakyStorage {
    inner: InMemoryArchiveStorage,
    calls: std::sync::atomic::AtomicUsize,
    succeed_puts: usize,
    fail_puts: usize,
}

impl FlakyStorage {
    fn new(succeed_puts: usize, fail_puts: usize) -> Self {
        Self {
            inner: InMemoryArchiveStorage::new(),
            calls: std::sync::atomic::AtomicUsize::new(0),
            succeed_puts,
            fail_puts,
        }
    }
}

#[async_trait::async_trait]
impl obix::EventArchiveStorage for FlakyStorage {
    async fn put(&self, path: &str, data: bytes::Bytes) -> Result<(), obix::ArchiveError> {
        let call = self
            .calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if call >= self.succeed_puts && call < self.succeed_puts + self.fail_puts {
            return Err(obix::ArchiveError::storage("injected storage outage"));
        }
        self.inner.put(path, data).await
    }

    async fn get(&self, path: &str) -> Result<bytes::Bytes, obix::ArchiveError> {
        self.inner.get(path).await
    }
}

#[tokio::test]
#[file_serial]
async fn storage_failure_mid_span_prunes_nothing_and_retries_cleanly() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipeout_outbox_tables(&pool).await?;

    let (clock, controller) = ClockHandle::manual_at(day(0));
    // The first span's chunk lands; the second span's put fails.
    let storage = Arc::new(FlakyStorage::new(1, 1));
    let boundary = Arc::new(DailyRetentionBoundary::<TestTables>::new(
        &pool,
        chrono::Duration::days(1),
        clock.clone(),
    ));
    let archive_config = ArchiveConfig::new(storage.clone(), boundary)
        .with_boundaries_per_run(10)
        .with_clock(clock.clone());
    let outbox = Outbox::<TestEvent, TestTables>::init(
        &pool,
        MailboxConfig::builder()
            .clock(clock.clone())
            .archive(Some(archive_config.clone()))
            .build()?,
    )
    .await?;

    publish(&outbox, &[1]).await?;
    controller.advance(DAY).await;
    publish(&outbox, &[2]).await?;
    controller.advance(DAY).await;
    publish(&outbox, &[3]).await?;
    controller.advance(DAY).await;

    // Days 0 and 1 eligible; the second chunk's put fails → the run
    // errors after the first span, having pruned only that span.
    let archiver = EventArchiver::<TestTables>::new(&pool, archive_config.clone());
    let result = archiver.run_once().await;
    assert!(result.is_err(), "run must fail on the storage outage");
    let chunks = manifest_rows(&pool).await?;
    assert_eq!(chunks.len(), 1);
    assert_eq!((chunks[0].1, chunks[0].2), (1, 1));

    // Retry with storage healthy: the remaining span archives
    // contiguously off the existing watermark.
    let report = archiver.run_once().await?;
    assert_eq!(report.spans_archived, 1);
    assert_eq!(u64::from(report.watermark), 2);

    let replayed = replay_from_beginning(&outbox, 3).await;
    assert_eq!(replayed, vec![Some(1), Some(2), Some(3)]);

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn backfill_rechecks_watermark_when_archiver_prunes_mid_walk() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipeout_outbox_tables(&pool).await?;

    let (clock, controller) = ClockHandle::manual_at(day(0));
    let storage = Arc::new(InMemoryArchiveStorage::new());
    let boundary = Arc::new(DailyRetentionBoundary::<TestTables>::new(
        &pool,
        chrono::Duration::days(1),
        clock.clone(),
    ));
    let archive_config = ArchiveConfig::new(storage.clone(), boundary)
        .with_boundaries_per_run(10)
        .with_clock(clock.clone());
    let mailbox_config = MailboxConfig::builder()
        .clock(clock.clone())
        // Backfill page size 3, channel capacity 1: the walk loads
        // events 1-3, delivers 1 and stalls mid-page on the full
        // channel — provably mid-walk when the archiver prunes, since
        // the next page load only happens after page 1 is fully sent.
        .event_buffer_size(1)
        .event_cache_size(3)
        .archive(Some(archive_config.clone()))
        .build()?;
    let outbox = Outbox::<TestEvent, TestTables>::init(&pool, mailbox_config.clone()).await?;

    // Day 0 settles and is archived; day 3 events stay live — the prune
    // removes a *middle* span with committed rows above it. One event
    // per commit so the size-1 live channels never overflow.
    for v in [1, 2, 3, 4] {
        publish(&outbox, &[v]).await?;
    }
    controller.advance(DAY * 3).await;
    for v in [5, 6] {
        publish(&outbox, &[v]).await?;
    }

    // A second, cold outbox: its cache is empty, so its backfill must
    // actually walk postgres (a warm snapshot would short-circuit the
    // walk and mask the race).
    let walk_outbox = Outbox::<TestEvent, TestTables>::init(&pool, mailbox_config).await?;

    let mut listener = walk_outbox.listen_persisted(Some(EventSequence::BEGIN));
    // Streams are lazy: the backfill only starts on the first poll.
    // Consume event 1 to start it, then let it run ahead and stall on
    // the full channel (mid page 1) before the sweep starts.
    let first = tokio::time::timeout(std::time::Duration::from_secs(10), listener.next())
        .await?
        .expect("stream ended during replay")
        .expect("undecodable event during replay");
    assert_eq!(u64::from(first.sequence), 1);
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let archiver = EventArchiver::<TestTables>::new(&pool, archive_config.clone());
    let report = archiver.run_once().await?;
    assert_eq!(u64::from(report.watermark), 4);

    // The walk resumes into a page covering the just-pruned sequence 4:
    // the per-page watermark re-check must route it through the archive —
    // it arrives real, not as a placeholder.
    let mut received = vec![first.payload.as_ref().map(|TestEvent::Ping(v)| *v)];
    for expected_sequence in 2..=6u64 {
        let event = tokio::time::timeout(std::time::Duration::from_secs(10), listener.next())
            .await?
            .expect("stream ended during replay")
            .expect("undecodable event during replay");
        assert_eq!(u64::from(event.sequence), expected_sequence);
        received.push(event.payload.as_ref().map(|TestEvent::Ping(v)| *v));
    }
    assert_eq!(
        received,
        vec![Some(1), Some(2), Some(3), Some(4), Some(5), Some(6)]
    );

    Ok(())
}

/// A backfill whose archive stream ends early (listener dropped) below
/// the watermark must not fall through to `load_next_page`: it
/// gap-fills by inserting placeholder rows, which below the watermark
/// would be written for archived (partition-dropped) sequences and
/// delivered as rolled-back events. The continuation stays SELECT-only.
#[tokio::test]
#[file_serial]
async fn backfill_listener_drop_below_watermark_writes_no_placeholders() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipeout_outbox_tables(&pool).await?;

    let width = DEFAULT_PARTITION_WIDTH;

    // Ensure p0 exists (a prior test run may have dropped it) and p1 for
    // the live event above the watermark.
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS persistent_outbox_events_p0 \
         PARTITION OF persistent_outbox_events \
         FOR VALUES FROM (0) TO ({width})"
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS persistent_outbox_events_p1 \
         PARTITION OF persistent_outbox_events \
         FOR VALUES FROM ({width}) TO ({width} * 2)"
    ))
    .execute(&pool)
    .await?;

    // Live rows in p0 (to be archived + pruned) and one in p1.
    for (seq, ping) in [(1i64, 1u64), (2, 2), (3, 3)] {
        sqlx::query(
            "INSERT INTO persistent_outbox_events (sequence, payload) \
             VALUES ($1, $2::jsonb)",
        )
        .bind(seq)
        .bind(serde_json::json!({ "Ping": ping }).to_string())
        .execute(&pool)
        .await?;
    }
    sqlx::query(
        "INSERT INTO persistent_outbox_events (sequence, payload) \
         VALUES ($1, $2::jsonb)",
    )
    .bind((width + 1) as i64)
    .bind(serde_json::json!({ "Ping": 4 }).to_string())
    .execute(&pool)
    .await?;

    // The archive: a manifest chunk covering p0's range (watermark =
    // width) whose storage file holds only the first three lines.
    let storage = Arc::new(InMemoryArchiveStorage::new());
    let mut file = Vec::new();
    for (seq, ping) in [(1u64, 1u64), (2, 2), (3, 3)] {
        let line = serde_json::json!({
            "id": format!("00000000-0000-0000-0000-{:012}", seq),
            "sequence": seq,
            "payload": { "Ping": ping },
            "recorded_at": "2026-07-20T00:00:00Z",
        });
        serde_json::to_writer(&mut file, &line)?;
        file.push(b'\n');
    }
    storage.put("test/early-exit", file.into()).await?;
    sqlx::query(
        "INSERT INTO persistent_outbox_archive_chunks (path, min_sequence, max_sequence) \
         VALUES ('test/early-exit', 1, $1)",
    )
    .bind(width as i64)
    .execute(&pool)
    .await?;

    // Drop p0 — the archived range is gone from the live table.
    obix::out::Partitions::<TestTables>::new(&pool, 1)
        .prune_archived()
        .await?;
    assert!(!relation_exists(&pool, "persistent_outbox_events_p0").await?);

    // Head the sequence so the cold outbox's cache starts above BEGIN
    // and its backfill actually walks.
    sqlx::query("SELECT setval('persistent_outbox_events_sequence_seq', $1)")
        .bind((width + 5) as i64)
        .execute(&pool)
        .await?;

    let (clock, _controller) = ClockHandle::manual_at(day(0));
    let boundary = Arc::new(DailyRetentionBoundary::<TestTables>::new(
        &pool,
        chrono::Duration::days(1),
        clock.clone(),
    ));
    let archive_config = ArchiveConfig::new(storage.clone(), boundary)
        .with_boundaries_per_run(10)
        .with_clock(clock.clone());
    let mailbox_config = MailboxConfig::builder()
        .clock(clock.clone())
        // Channel capacity 1: the archive stream blocks on the second
        // delivery until the listener drops — provably mid-stream.
        .event_buffer_size(1)
        .archive(Some(archive_config))
        .build()?;
    let walk_outbox = Outbox::<TestEvent, TestTables>::init(&pool, mailbox_config).await?;

    let mut listener = walk_outbox.listen_persisted(Some(EventSequence::BEGIN));
    // Consume the first archived event (the backfill is streaming from
    // the archive), then drop the listener mid-stream.
    let first = tokio::time::timeout(std::time::Duration::from_secs(10), listener.next())
        .await?
        .expect("stream ended during replay")
        .expect("undecodable event during replay");
    assert_eq!(u64::from(first.sequence), 1);
    drop(listener);
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // No placeholder rows were written below the watermark.
    let remaining: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM persistent_outbox_events WHERE sequence <= $1")
            .bind(width as i64)
            .fetch_one(&pool)
            .await?;
    assert_eq!(remaining, 0);

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn backfill_below_watermark_without_reader_degrades_to_select_only() -> anyhow::Result<()> {
    let setup = init_setup(1, 10).await?;

    publish(&setup.outbox, &[1, 2]).await?;
    setup.controller.advance(DAY * 3).await;
    publish(&setup.outbox, &[3]).await?;

    let archiver = EventArchiver::<TestTables>::new(&setup.pool, setup.archive_config.clone());
    let report = archiver.run_once().await?;
    assert_eq!(u64::from(report.watermark), 2);

    // A second outbox on the same tables WITHOUT archive configuration:
    // the misconfigured-deployment case. The sub-watermark range
    // degrades to placeholders — loudly — but must be served SELECT-only.
    let plain_outbox =
        Outbox::<TestEvent, TestTables>::init(&setup.pool, MailboxConfig::builder().build()?)
            .await?;

    let received = replay_from_beginning(&plain_outbox, 3).await;
    // Archived rows remain in the live table until partition drop, so
    // the degraded SELECT-only walk finds the real events rather than
    // serving placeholders.
    assert_eq!(received, vec![Some(1), Some(2), Some(3)]);

    // The degraded walk must not have written placeholder rows into the
    // live table: only the 2 original archived rows should be present
    // below the watermark.
    let remaining: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM persistent_outbox_events WHERE sequence <= 2")
            .fetch_one(&setup.pool)
            .await?;
    assert_eq!(remaining, 2);

    Ok(())
}

/// Archived partitions are dropped by the partition maintainer once the
/// archive watermark has passed their upper bound — an O(1) metadata
/// operation instead of an O(n) row-level DELETE. Rows remain in the live
/// table (invisible to readers via watermark gating) until the partition is
/// dropped.
#[tokio::test]
#[file_serial]
async fn prune_drops_fully_archived_partitions() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipeout_outbox_tables(&pool).await?;

    let width = DEFAULT_PARTITION_WIDTH;

    // Ensure p0 exists (a prior test run may have dropped it — the
    // suite's wipeout helper truncates but does not recreate partitions).
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS persistent_outbox_events_p0 \
         PARTITION OF persistent_outbox_events \
         FOR VALUES FROM (0) TO ({width})"
    ))
    .execute(&pool)
    .await?;

    // Create p1 (p0 ships in the migration).
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS persistent_outbox_events_p1 \
         PARTITION OF persistent_outbox_events \
         FOR VALUES FROM ({width}) TO ({width} * 2)"
    ))
    .execute(&pool)
    .await?;

    // A row in p0 and one in p1.
    sqlx::query("INSERT INTO persistent_outbox_events (sequence, payload) VALUES (1, '{}'::jsonb)")
        .execute(&pool)
        .await?;
    sqlx::query(&format!(
        "INSERT INTO persistent_outbox_events (sequence, payload) VALUES ({width} + 1, '{{}}'::jsonb)"
    ))
    .execute(&pool)
        .await?;

    let partitions = obix::out::Partitions::<TestTables>::new(&pool, 1);

    // No watermark (archiving not active) → prune is a no-op.
    partitions.prune_archived().await?;
    assert!(relation_exists(&pool, "persistent_outbox_events_p0").await?);

    // Watermark past p0's upper bound → prune drops p0, keeps p1.
    sqlx::query(&format!(
        "INSERT INTO persistent_outbox_archive_chunks (path, min_sequence, max_sequence) \
         VALUES ('test/whole-p0', 1, {width})"
    ))
    .execute(&pool)
    .await?;

    partitions.prune_archived().await?;
    assert!(
        !relation_exists(&pool, "persistent_outbox_events_p0").await?,
        "p0 dropped after prune"
    );
    assert!(
        relation_exists(&pool, "persistent_outbox_events_p1").await?,
        "p1 survives (watermark below its upper bound)"
    );

    Ok(())
}
