mod helpers;

use futures::stream::StreamExt;
use obix::{EventSequence, MailboxConfig, PartitionMaintainerConfig, out::Outbox};
use serde::{Deserialize, Serialize};
use serial_test::file_serial;
use sqlx::Row;

use helpers::{
    TestTables, init_outbox, init_pool, wipeout_outbox_job_tables, wipeout_outbox_tables,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum TestEvent {
    Ping(u64),
}

const WIDTH: u64 = obix::DEFAULT_PARTITION_WIDTH; // 2_000_000 — the p0/p1 boundary
const PREMAKE: u64 = obix::DEFAULT_PARTITION_PREMAKE;
const BOUNDARY: i64 = WIDTH as i64;

// ── Test-only partition helpers ──────────────────────────────────────────
//
// The shipped migration gives every test `p0` ([0, WIDTH)) plus a DEFAULT
// backstop. Driving the sequence up to (or past) the p0 boundary cheaply means
// positioning the shared sequence just below WIDTH with `setval` (O(1)) rather
// than inserting millions of rows. Because partitions the maintainer/recovery
// create persist across serial tests, each test first resets to the migration
// baseline (p0 + DEFAULT only).

async fn reset_partitions_to_baseline(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    // A prior recovery may have left a detached DEFAULT copy.
    sqlx::query("DROP TABLE IF EXISTS persistent_outbox_events_default_old")
        .execute(pool)
        .await?;
    // Drop every partition the maintainer / recovery created, keeping only the
    // migration baseline (p0 + DEFAULT). Their count varies with `premake`, so
    // enumerate them from the catalog rather than hard-coding names.
    let children = sqlx::query(
        "SELECT c.relname FROM pg_inherits i \
         JOIN pg_class c ON c.oid = i.inhrelid \
         WHERE i.inhparent = 'persistent_outbox_events'::regclass \
           AND c.relname NOT IN ('persistent_outbox_events_p0', 'persistent_outbox_events_default')",
    )
    .fetch_all(pool)
    .await?;
    for row in children {
        let name: String = row.get("relname");
        sqlx::query(&format!("DROP TABLE IF EXISTS {name}"))
            .execute(pool)
            .await?;
    }
    // Recovery recreates DEFAULT, but guard against a half-run leaving it gone.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS persistent_outbox_events_default \
         PARTITION OF persistent_outbox_events DEFAULT",
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn set_sequence(pool: &sqlx::PgPool, value: i64) -> anyhow::Result<()> {
    sqlx::query("SELECT setval('persistent_outbox_events_sequence_seq', $1)")
        .bind(value)
        .execute(pool)
        .await?;
    Ok(())
}

async fn relation_exists(pool: &sqlx::PgPool, name: &str) -> anyhow::Result<bool> {
    let row = sqlx::query("SELECT to_regclass($1) IS NOT NULL AS present")
        .bind(name)
        .fetch_one(pool)
        .await?;
    Ok(row.get::<bool, _>("present"))
}

async fn default_row_count(pool: &sqlx::PgPool) -> anyhow::Result<i64> {
    let row = sqlx::query("SELECT COUNT(*) AS n FROM persistent_outbox_events_default")
        .fetch_one(pool)
        .await?;
    Ok(row.get::<i64, _>("n"))
}

async fn max_sequence(pool: &sqlx::PgPool) -> anyhow::Result<Option<i64>> {
    let row = sqlx::query("SELECT MAX(sequence) AS m FROM persistent_outbox_events")
        .fetch_one(pool)
        .await?;
    Ok(row.get::<Option<i64>, _>("m"))
}

async fn reloptions(pool: &sqlx::PgPool, relname: &str) -> anyhow::Result<String> {
    let row = sqlx::query(
        "SELECT COALESCE(array_to_string(reloptions, ','), '') AS opts \
         FROM pg_class WHERE relname = $1",
    )
    .bind(relname)
    .fetch_one(pool)
    .await?;
    Ok(row.get::<String, _>("opts"))
}

/// Wipe the outbox, reset to baseline partitions (p0 + DEFAULT, no explicit
/// partition beyond p0), position the sequence at `head`, then build a fresh
/// outbox whose cache starts at `head` (so a `None` listener never tries to
/// backfill from 0).
async fn prepare_outbox(
    pool: &sqlx::PgPool,
    head: i64,
) -> anyhow::Result<Outbox<TestEvent, TestTables>> {
    wipeout_outbox_tables(pool).await?;
    reset_partitions_to_baseline(pool).await?;
    set_sequence(pool, head).await?;
    let outbox = Outbox::<TestEvent, TestTables>::init(
        pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;
    Ok(outbox)
}

// ── Tests ────────────────────────────────────────────────────────────────

/// The maintainer, run synchronously at registration, pre-creates a runway of
/// partitions ahead of the head (with their storage params) before the head
/// reaches them.
#[tokio::test]
#[file_serial]
async fn maintainer_premakes_partitions_ahead() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let job_type = "test-partition-maintainer";
    wipeout_outbox_tables(&pool).await?;
    wipeout_outbox_job_tables(&pool, job_type).await?;
    reset_partitions_to_baseline(&pool).await?;

    // Head approaching the p0 boundary, with no explicit partition beyond p0.
    set_sequence(&pool, BOUNDARY - 10).await?;
    assert!(
        !relation_exists(&pool, "persistent_outbox_events_p1").await?,
        "p1 must not exist before registration"
    );

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let outbox = Outbox::<TestEvent, TestTables>::init(
        &pool,
        MailboxConfig::builder().build().expect("config"),
    )
    .await?;

    // Registration runs one premake pass synchronously before returning.
    outbox
        .register_partition_maintainer(
            &mut jobs,
            PartitionMaintainerConfig::new(job::JobType::new(job_type)),
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // Both the next partition AND the full premake runway exist.
    assert!(
        relation_exists(&pool, "persistent_outbox_events_p1").await?,
        "p1 pre-created before the head reaches the boundary"
    );
    let deepest = format!("persistent_outbox_events_p{PREMAKE}");
    assert!(
        relation_exists(&pool, &deepest).await?,
        "premake keeps {PREMAKE} partitions ahead ({deepest})"
    );

    // Per-partition storage params present on a maintainer-created partition.
    let opts = reloptions(&pool, "persistent_outbox_events_p1").await?;
    assert!(
        opts.contains("autovacuum_freeze_min_age=0"),
        "freeze param present: {opts}"
    );
    assert!(
        opts.contains("autovacuum_vacuum_insert_scale_factor=0"),
        "insert-vacuum param present: {opts}"
    );

    let _ = jobs.shutdown().await;
    Ok(())
}

/// Rows stranded in DEFAULT (maintainer behind) are recovered into explicit
/// partitions in one transaction: DEFAULT empties, the rows survive intact and
/// readable, and `MAX(sequence)` never regresses.
#[tokio::test]
#[file_serial]
async fn default_fill_then_recover() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    // No explicit partition beyond p0, so rows past the boundary land in DEFAULT.
    let outbox = prepare_outbox(&pool, BOUNDARY - 2).await?;

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_all_persisted(
            &mut op,
            [
                TestEvent::Ping(0), // BOUNDARY-1, p0
                TestEvent::Ping(1), // BOUNDARY,   DEFAULT
                TestEvent::Ping(2), // BOUNDARY+1, DEFAULT
            ],
        )
        .await?;
    op.commit().await?;

    assert_eq!(
        default_row_count(&pool).await?,
        2,
        "two rows stranded in DEFAULT"
    );
    let max_before = max_sequence(&pool).await?;
    assert_eq!(max_before, Some(BOUNDARY + 1));

    // The stranded rows are readable before recovery.
    assert_replayable(&pool).await?;

    // Recover.
    obix::out::Partitions::<TestTables>::new(&pool, PREMAKE)
        .recover_default()
        .await?;

    assert_eq!(default_row_count(&pool).await?, 0, "DEFAULT drained");
    assert!(
        relation_exists(&pool, "persistent_outbox_events_p1").await?,
        "stranded rows moved into explicit p1"
    );
    assert_eq!(
        max_sequence(&pool).await?,
        max_before,
        "MAX(sequence) never regressed"
    );
    // Recovery left no artifact behind.
    assert!(!relation_exists(&pool, "persistent_outbox_events_default_old").await?);

    // The recovered rows are still readable, in order, with their payloads.
    assert_replayable(&pool).await?;
    Ok(())
}

/// Replay the three events published around the boundary from a fresh outbox
/// (empty cache → DB backfill) and assert order + payloads survive.
async fn assert_replayable(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    let replay = Outbox::<TestEvent, TestTables>::init(
        pool,
        MailboxConfig::builder().build().expect("config"),
    )
    .await?;
    let mut listener = replay.listen_persisted(Some(EventSequence::from((BOUNDARY - 2) as u64)));
    for (i, expected_seq) in [BOUNDARY - 1, BOUNDARY, BOUNDARY + 1]
        .into_iter()
        .enumerate()
    {
        let ev = tokio::time::timeout(std::time::Duration::from_secs(3), listener.next())
            .await?
            .expect("replayed event")?;
        assert_eq!(ev.sequence, EventSequence::from(expected_seq as u64));
        assert_eq!(ev.payload, Some(TestEvent::Ping(i as u64)));
    }
    Ok(())
}

/// Concurrent `ensure` calls (multi-instance startup, a maintainer tick
/// overlapping an operator repair) must not fail with a creation race: the
/// advisory lock serializes creators so every caller succeeds.
#[tokio::test]
#[file_serial]
async fn concurrent_ensure_does_not_race() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipeout_outbox_tables(&pool).await?;
    reset_partitions_to_baseline(&pool).await?;
    set_sequence(&pool, BOUNDARY - 10).await?;

    let results = futures::future::join_all((0..8).map(|_| {
        let pool = pool.clone();
        async move {
            obix::out::Partitions::<TestTables>::new(&pool, PREMAKE)
                .ensure()
                .await
        }
    }))
    .await;
    for r in results {
        r?;
    }
    let deepest = format!("persistent_outbox_events_p{PREMAKE}");
    assert!(relation_exists(&pool, &deepest).await?);
    Ok(())
}

/// Replay-from-zero contract is unchanged by partitioning: a `BEGIN` listener
/// returns the full contiguous stream. `TRUNCATE ... RESTART IDENTITY` on the
/// partitioned parent cascades to every partition and resets the sequence.
#[tokio::test]
#[file_serial]
async fn replay_from_begin_still_contiguous() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox =
        init_outbox::<TestEvent>(&pool, MailboxConfig::builder().build().expect("config")).await?;
    reset_partitions_to_baseline(&pool).await?;

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_all_persisted(
            &mut op,
            [TestEvent::Ping(0), TestEvent::Ping(1), TestEvent::Ping(2)],
        )
        .await?;
    op.commit().await?;

    let replay = Outbox::<TestEvent, TestTables>::init(
        &pool,
        MailboxConfig::builder().build().expect("config"),
    )
    .await?;
    let mut listener = replay.listen_persisted(EventSequence::BEGIN);
    for i in 0..3u64 {
        let ev = tokio::time::timeout(std::time::Duration::from_secs(3), listener.next())
            .await?
            .expect("replayed event")?;
        assert_eq!(ev.sequence, EventSequence::from(i + 1));
        assert_eq!(ev.payload, Some(TestEvent::Ping(i)));
    }
    Ok(())
}
