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

const WIDTH: u64 = obix::DEFAULT_PARTITION_WIDTH; // 10_000_000 — the p0/p1 boundary
const PREMAKE: u64 = obix::DEFAULT_PARTITION_PREMAKE;
const BOUNDARY: i64 = WIDTH as i64;

// ── Test-only partition helpers ──────────────────────────────────────────
//
// The shipped migration gives every test a wide `p0` ([0, 10_000_000)) plus a
// DEFAULT backstop. Crossing a partition boundary cheaply means positioning
// the shared sequence just below 10_000_000 with `setval` (O(1)) rather than
// inserting ten million rows. Because partitions the maintainer/recovery
// create persist across serial tests, each test first resets to the migration
// baseline (p0 + DEFAULT only).

async fn reset_partitions_to_baseline(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    for name in [
        "persistent_outbox_events_default_old",
        "persistent_outbox_events_p1",
        "persistent_outbox_events_p2",
        "persistent_outbox_events_p3",
        "persistent_outbox_events_p4",
    ] {
        sqlx::query(&format!("DROP TABLE IF EXISTS {name}"))
            .execute(pool)
            .await?;
    }
    // Recovery tests recreate DEFAULT, but guard against a half-run leaving it
    // detached.
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

/// Wipe the outbox, reset to baseline partitions, optionally premake the p1/p2
/// partitions, position the sequence at `head`, then build a fresh outbox whose
/// cache starts at `head` (so a `None` listener never tries to backfill from 0).
async fn prepare_outbox(
    pool: &sqlx::PgPool,
    head: i64,
    premake_p1: bool,
) -> anyhow::Result<Outbox<TestEvent, TestTables>> {
    wipeout_outbox_tables(pool).await?;
    reset_partitions_to_baseline(pool).await?;
    if premake_p1 {
        obix::out::Partitions::<TestTables>::new(pool, WIDTH, PREMAKE)
            .ensure()
            .await?;
    }
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

/// §9.1 — an aborted-transaction gap that lands on the far side of a partition
/// boundary is still filled with a contiguous placeholder.
#[tokio::test]
#[file_serial]
async fn gap_fill_across_partition_boundary() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = prepare_outbox(&pool, BOUNDARY - 2, true).await?;

    let mut listener = outbox.listen_persisted(None);

    // seq BOUNDARY-1, in p0
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(0))
        .await?;
    op.commit().await?;
    let first = tokio::time::timeout(std::time::Duration::from_secs(2), listener.next())
        .await?
        .expect("first event")?;
    assert_eq!(first.sequence, EventSequence::from((BOUNDARY - 1) as u64));
    assert!(matches!(first.payload, Some(TestEvent::Ping(0))));

    // Burn BOUNDARY (the first sequence in p1) → a gap that straddles the
    // p0/p1 boundary.
    sqlx::query("SELECT nextval('persistent_outbox_events_sequence_seq')")
        .execute(&pool)
        .await?;

    // seq BOUNDARY+1, in p1
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Ping(1))
        .await?;
    op.commit().await?;

    // Placeholder for BOUNDARY (routed into p1), then the real event.
    let gap = tokio::time::timeout(std::time::Duration::from_secs(5), listener.next())
        .await?
        .expect("gap placeholder")?;
    assert_eq!(gap.sequence, EventSequence::from(BOUNDARY as u64));
    assert!(gap.payload.is_none(), "gap event has None payload");

    let real = tokio::time::timeout(std::time::Duration::from_secs(2), listener.next())
        .await?
        .expect("real event after gap")?;
    assert_eq!(real.sequence, EventSequence::from((BOUNDARY + 1) as u64));
    assert!(matches!(real.payload, Some(TestEvent::Ping(1))));

    // The placeholder physically landed in p1, not DEFAULT.
    assert_eq!(default_row_count(&pool).await?, 0, "DEFAULT stays empty");
    Ok(())
}

/// §9.2 — a single publish batch whose sequences straddle two partitions
/// persists (routing each row to its partition) and delivers in order.
#[tokio::test]
#[file_serial]
async fn batch_straddles_partition_boundary() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = prepare_outbox(&pool, BOUNDARY - 2, true).await?;

    let mut listener = outbox.listen_persisted(None);

    // One op, one INSERT statement: seqs BOUNDARY-1 (p0), BOUNDARY (p1),
    // BOUNDARY+1 (p1).
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_all_persisted(
            &mut op,
            [TestEvent::Ping(0), TestEvent::Ping(1), TestEvent::Ping(2)],
        )
        .await?;
    op.commit().await?;

    for (i, expected_seq) in [BOUNDARY - 1, BOUNDARY, BOUNDARY + 1]
        .into_iter()
        .enumerate()
    {
        let ev = tokio::time::timeout(std::time::Duration::from_secs(3), listener.next())
            .await?
            .expect("batched event")?;
        assert_eq!(ev.sequence, EventSequence::from(expected_seq as u64));
        assert_eq!(ev.payload, Some(TestEvent::Ping(i as u64)));
    }

    assert_eq!(default_row_count(&pool).await?, 0, "DEFAULT stays empty");
    Ok(())
}

/// §9.3 — a listener replaying from just below the boundary reads across it
/// contiguously via the backfill (`load_next_page`) path.
#[tokio::test]
#[file_serial]
async fn read_across_partition_boundary() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = prepare_outbox(&pool, BOUNDARY - 2, true).await?;

    // Persist three contiguous events straddling the boundary.
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_all_persisted(
            &mut op,
            [TestEvent::Ping(0), TestEvent::Ping(1), TestEvent::Ping(2)],
        )
        .await?;
    op.commit().await?;

    // A fresh outbox has an empty cache, so its listener backfills from the DB
    // (crossing the boundary) rather than reading from the in-memory broadcast.
    let replay = Outbox::<TestEvent, TestTables>::init(
        &pool,
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

/// §9.4 — the maintainer, run at registration, pre-creates the next partition
/// (with its storage params) before the head reaches it.
#[tokio::test]
#[file_serial]
async fn maintainer_premakes_next_partition() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let job_type = "test-partition-maintainer";
    wipeout_outbox_tables(&pool).await?;
    wipeout_outbox_job_tables(&pool, job_type).await?;
    reset_partitions_to_baseline(&pool).await?;

    // Head approaching the p0/p1 boundary, with p1 not yet created.
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

    assert!(
        relation_exists(&pool, "persistent_outbox_events_p1").await?,
        "p1 pre-created before the head reaches the boundary"
    );
    // premake keeps two partitions ahead.
    assert!(relation_exists(&pool, "persistent_outbox_events_p2").await?);

    // Per-partition storage params present on the maintainer-created partition.
    let opts = reloptions(&pool, "persistent_outbox_events_p1").await?;
    assert!(
        opts.contains("autovacuum_freeze_min_age=0"),
        "freeze param present: {opts}"
    );
    assert!(
        opts.contains("autovacuum_vacuum_insert_scale_factor=0"),
        "insert-vacuum param present: {opts}"
    );
    Ok(())
}

/// §9.5 — rows stranded in DEFAULT (maintainer behind) are recovered into
/// explicit partitions in one transaction: DEFAULT empties, rows stay
/// contiguous, and `MAX(sequence)` never regresses.
#[tokio::test]
#[file_serial]
async fn default_fill_then_recover() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    // No p1: rows above the boundary land in DEFAULT.
    let outbox = prepare_outbox(&pool, BOUNDARY - 2, false).await?;

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

    // Reads see the stranded rows before recovery (contiguous across DEFAULT).
    assert_contiguous_replay(&pool).await?;

    // Recover.
    obix::out::Partitions::<TestTables>::new(&pool, WIDTH, PREMAKE)
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

    // Stream still contiguous with the original payloads after recovery.
    assert_contiguous_replay(&pool).await?;
    Ok(())
}

/// Replay the three straddling events from just below the boundary and assert
/// contiguity + payloads. A fresh outbox forces the DB backfill path.
async fn assert_contiguous_replay(pool: &sqlx::PgPool) -> anyhow::Result<()> {
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
