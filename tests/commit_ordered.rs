//! The commit-ordered lane's placement rules, exercised through the lane
//! itself.
//!
//! The order is computed, not stored: there is no log table to read back, so
//! these assert on what the lane *delivers*. The tuples are the same ones the
//! materialised log used to hold — `(position, sequence, boundary)` — because
//! the numbering is unchanged by the fact that nothing writes it down.

mod helpers;

use futures::stream::StreamExt;
use obix::{CommitLane, CommitSequence, MailboxConfig, out::Outbox};

use helpers::{TestTables, init_outbox, init_pool, wipeout_outbox_tables};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum TestEvent {
    Marker { group: String },
}

/// One seeded row: its insert sequence, the transaction it belongs to, and
/// whether it carries a payload (`false` writes a gap-fill placeholder).
struct Row {
    sequence: i64,
    xid: i64,
    payload: bool,
}

const fn ev(sequence: i64, xid: i64) -> Row {
    Row {
        sequence,
        xid,
        payload: true,
    }
}

const fn placeholder(sequence: i64, xid: i64) -> Row {
    Row {
        sequence,
        xid,
        payload: false,
    }
}

/// Write rows at exact sequences and groups. Direct SQL rather than
/// `publish`, because the point of these tests is to pin placement against
/// interleavings that are awkward to provoke through the write path.
async fn seed(pool: &sqlx::PgPool, rows: &[Row]) -> anyhow::Result<()> {
    for row in rows {
        let payload = row
            .payload
            .then(|| serde_json::json!({ "type": "marker", "group": row.xid.to_string() }));
        sqlx::query(
            "INSERT INTO persistent_outbox_events (sequence, payload, commit_xid)
             VALUES ($1, $2, $3)",
        )
        .bind(row.sequence)
        .bind(payload)
        .bind(row.xid)
        .execute(pool)
        .await?;
    }
    let highest = rows.iter().map(|r| r.sequence).max().unwrap_or(0);
    sqlx::query("SELECT setval('persistent_outbox_events_sequence_seq', $1)")
        .bind(highest)
        .execute(pool)
        .await?;
    Ok(())
}

/// What the lane delivers, as `(position, sequence, boundary)` — the shape
/// the commit log used to store.
///
/// The outbox is opened here rather than by the caller so the fold starts
/// after the seeding, which is what makes these assertions about placement
/// rather than about timing.
async fn lane_rows(pool: &sqlx::PgPool, count: usize) -> anyhow::Result<Vec<(i64, i64, bool)>> {
    let outbox = Outbox::<TestEvent, TestTables>::init(
        pool,
        MailboxConfig::builder()
            .commit_lane(CommitLane::Enabled)
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;
    let mut listener = outbox.listen_commit_ordered(CommitSequence::BEGIN)?;

    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        let item = tokio::time::timeout(std::time::Duration::from_secs(20), listener.next())
            .await
            .map_err(|_| anyhow::anyhow!("the lane stopped after {} of {count} rows", rows.len()))?
            .expect("the stream stays open")
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        rows.push((
            i64::from(item.position()),
            u64::from(item.sequence) as i64,
            item.is_commit_boundary(),
        ));
    }
    Ok(rows)
}

/// Checkpoint rows, `(sequence, commit_seq)`, oldest first.
async fn checkpoints(pool: &sqlx::PgPool) -> anyhow::Result<Vec<(i64, i64)>> {
    let rows = sqlx::query_as::<_, (i64, i64)>(
        "SELECT sequence, commit_seq FROM persistent_outbox_commit_checkpoints
         ORDER BY sequence",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// A transaction's events are contiguous in the commit lane even when
/// another transaction's inserts interleave with them, and groups are
/// ordered by the sequence at which they are first seen.
#[tokio::test]
#[serial_test::file_serial]
async fn interleaved_groups_are_contiguous_and_first_sight_ordered() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipeout_outbox_tables(&pool).await?;

    // A inserts a1, B inserts b1+b2 and commits, A inserts a2 and commits.
    // Insert order is a1, b1, b2, a2.
    seed(&pool, &[ev(1, 7001), ev(2, 7002), ev(3, 7002), ev(4, 7001)]).await?;

    // A is first-sighted at sequence 1 and B at 2, so A is ordered first;
    // each group's members are adjacent. Byte-for-byte what the log held.
    assert_eq!(
        lane_rows(&pool, 4).await?,
        vec![(1, 1, false), (2, 4, true), (3, 2, false), (4, 3, true)],
    );
    Ok(())
}

/// Gap-fill placeholders occupy an insert sequence but are not events, so
/// they never reach the commit lane and never take a position.
#[tokio::test]
#[serial_test::file_serial]
async fn placeholders_are_never_emitted() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipeout_outbox_tables(&pool).await?;

    seed(&pool, &[ev(1, 7010), placeholder(2, 7010), ev(3, 7011)]).await?;

    // Dense despite the placeholder sharing group 7010: the lane numbers
    // events, not sequences.
    assert_eq!(lane_rows(&pool, 2).await?, vec![(1, 1, true), (2, 3, true)],);
    Ok(())
}

/// Every group is contiguous in the lane, and groups are ordered by the
/// sequence of their lowest member, under a randomised interleaving.
#[tokio::test]
#[serial_test::file_serial]
async fn randomised_interleavings_keep_groups_contiguous() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipeout_outbox_tables(&pool).await?;

    // Deterministic pseudo-random assignment of sequences to groups: a
    // fixed multiplier interleaves groups without a rng dependency.
    let assignments: Vec<(i64, i64)> = (1..=300).map(|n: i64| (n, 7200 + (n * 17) % 23)).collect();
    let rows: Vec<Row> = assignments.iter().map(|(n, x)| ev(*n, *x)).collect();
    seed(&pool, &rows).await?;

    let delivered = lane_rows(&pool, 300).await?;
    assert_eq!(delivered.len(), 300);
    for (index, (position, _, _)) in delivered.iter().enumerate() {
        assert_eq!(*position, index as i64 + 1, "positions must be dense");
    }

    let group_of: std::collections::HashMap<i64, i64> = assignments.into_iter().collect();

    // Each group occupies one unbroken run, the boundary marks its end, and
    // the runs are ordered by each group's lowest sequence.
    let mut seen_groups = std::collections::HashSet::new();
    let mut previous_run_min = 0;
    let mut index = 0;
    while index < delivered.len() {
        let group = group_of[&delivered[index].1];
        assert!(
            seen_groups.insert(group),
            "group {group} appears in more than one run — it was split",
        );
        let mut members = Vec::new();
        while index < delivered.len() && group_of[&delivered[index].1] == group {
            members.push(delivered[index].1);
            let is_last = delivered[index].2;
            index += 1;
            let run_ended = index == delivered.len() || group_of[&delivered[index].1] != group;
            assert_eq!(
                is_last, run_ended,
                "the boundary must mark exactly the run's end"
            );
        }
        // Within a group, members stay in insert order.
        let mut sorted = members.clone();
        sorted.sort_unstable();
        assert_eq!(members, sorted, "group members must stay in insert order");

        let run_min = members[0];
        assert!(
            run_min > previous_run_min,
            "runs must be ordered by their lowest member: {run_min} after {previous_run_min}",
        );
        previous_run_min = run_min;
    }

    Ok(())
}

/// End to end through a real outbox: publish, then receive on the commit
/// lane with lane metadata populated and a dense cursor.
#[tokio::test]
#[serial_test::file_serial]
async fn commit_lane_delivers_published_events_end_to_end() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .commit_lane(CommitLane::Enabled)
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let mut listener = outbox.listen_commit_ordered(CommitSequence::BEGIN)?;

    // Two events in one transaction: one group, so the second closes it.
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Marker { group: "a1".into() })
        .await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Marker { group: "a2".into() })
        .await?;
    op.commit().await?;

    let first = listener.next().await.expect("first event")?;
    let second = listener.next().await.expect("second event")?;

    assert_eq!(first.position(), CommitSequence::from(1u64));
    assert_eq!(second.position(), CommitSequence::from(2u64));
    assert!(
        !first.is_commit_boundary(),
        "the first of two events in a transaction does not close its group",
    );
    assert!(
        second.is_commit_boundary(),
        "the last event of a transaction closes its group",
    );
    assert_eq!(
        first.commit_group, second.commit_group,
        "events of one transaction share a group",
    );
    Ok(())
}

/// A commit-ordered listener created against a populated stream reads it
/// back from the beginning — by re-folding, since there is no log to page.
#[tokio::test]
#[serial_test::file_serial]
async fn commit_lane_backfills_by_refolding() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .commit_lane(CommitLane::Enabled)
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    for n in 0..8 {
        let mut op = outbox.begin_op().await?;
        outbox
            .publish_persisted_in_op(
                &mut op,
                TestEvent::Marker {
                    group: format!("e{n}"),
                },
            )
            .await?;
        op.commit().await?;
    }

    // A listener starting at BEGIN must replay the whole stream, not just
    // receive what is published after it subscribes.
    let mut listener = outbox.listen_commit_ordered(CommitSequence::BEGIN)?;
    let mut received = Vec::new();
    for _ in 0..8 {
        let item = listener.next().await.expect("event")?;
        received.push(u64::from(item.position()));
    }
    assert_eq!(received, (1..=8).collect::<Vec<u64>>());
    Ok(())
}

/// The sequencer runs in every `Enabled` process, whether or not that
/// process has a commit-ordered listener: the fold must not wait for a
/// subscriber, and its checkpoints must land regardless.
#[tokio::test]
#[serial_test::file_serial]
async fn sequencer_runs_without_a_commit_listener() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipeout_outbox_tables(&pool).await?;
    let outbox = Outbox::<TestEvent, TestTables>::init(
        &pool,
        MailboxConfig::builder()
            .commit_lane(CommitLane::Enabled)
            // Every group, so the assertion needs no timing slack.
            .commit_checkpoint_every(1)
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    for n in 0..4 {
        let mut op = outbox.begin_op().await?;
        outbox
            .publish_persisted_in_op(
                &mut op,
                TestEvent::Marker {
                    group: format!("e{n}"),
                },
            )
            .await?;
        op.commit().await?;
    }

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        if checkpoints(&pool)
            .await?
            .last()
            .is_some_and(|(_, commit_seq)| *commit_seq == 4)
        {
            break;
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "the fold never checkpointed up to the published head",
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    Ok(())
}

/// Two processes sharing a pool derive the same commit order — now without
/// any shared state to derive it from: each computes it independently.
#[tokio::test]
#[serial_test::file_serial]
async fn two_processes_deliver_identical_commit_order() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let config = MailboxConfig::builder()
        .commit_lane(CommitLane::Enabled)
        .build()
        .expect("Couldn't build MailboxConfig");
    let first = init_outbox::<TestEvent>(&pool, config.clone()).await?;
    let second = Outbox::<TestEvent, TestTables>::init(&pool, config).await?;

    let mut first_listener = first.listen_commit_ordered(CommitSequence::BEGIN)?;
    let mut second_listener = second.listen_commit_ordered(CommitSequence::BEGIN)?;

    for n in 0..3u64 {
        let outbox = if n % 2 == 0 { &first } else { &second };
        let mut op = outbox.begin_op().await?;
        outbox
            .publish_persisted_in_op(
                &mut op,
                TestEvent::Marker {
                    group: format!("e{n}"),
                },
            )
            .await?;
        outbox
            .publish_persisted_in_op(
                &mut op,
                TestEvent::Marker {
                    group: format!("e{n}b"),
                },
            )
            .await?;
        op.commit().await?;
    }

    let mut from_first = Vec::new();
    let mut from_second = Vec::new();
    for _ in 0..6 {
        let item = first_listener.next().await.expect("event")?;
        from_first.push((u64::from(item.position()), u64::from(item.sequence)));
        let item = second_listener.next().await.expect("event")?;
        from_second.push((u64::from(item.position()), u64::from(item.sequence)));
    }
    assert_eq!(
        from_first, from_second,
        "both processes must compute the same commit order",
    );
    Ok(())
}

/// The insert lane is untouched by the commit lane's existence: the group is
/// reported there too.
#[tokio::test]
#[serial_test::file_serial]
async fn insert_lane_carries_group() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
            .commit_lane(CommitLane::Enabled)
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let mut listener = outbox.listen_persisted(None);

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(
            &mut op,
            TestEvent::Marker {
                group: "insert".into(),
            },
        )
        .await?;
    outbox
        .publish_persisted_in_op(
            &mut op,
            TestEvent::Marker {
                group: "insert2".into(),
            },
        )
        .await?;
    op.commit().await?;

    let first = listener.next().await.expect("event")?;
    let second = listener.next().await.expect("event")?;
    assert_eq!(
        first.commit_group, second.commit_group,
        "group identity is available on the insert lane",
    );
    Ok(())
}
