mod helpers;

use futures::stream::StreamExt;
use obix::{CommitSequence, EventSequence, MailboxConfig, MailboxTables, SequenceTick};

use helpers::{TestTables, init_outbox, init_pool, wipeout_outbox_tables};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum TestEvent {
    Marker { group: String },
}

/// One seeded row: its insert sequence, its group (`None` writes a NULL
/// `commit_xid`, the legacy/placeholder shape) and whether it carries a
/// payload (`false` writes a gap-fill placeholder).
struct Row {
    sequence: i64,
    xid: Option<i64>,
    payload: bool,
}

const fn ev(sequence: i64, xid: i64) -> Row {
    Row {
        sequence,
        xid: Some(xid),
        payload: true,
    }
}

const fn placeholder(sequence: i64) -> Row {
    Row {
        sequence,
        xid: None,
        payload: false,
    }
}

const fn legacy(sequence: i64) -> Row {
    Row {
        sequence,
        xid: None,
        payload: true,
    }
}

/// Write rows at exact sequences and groups. Direct SQL rather than
/// `publish`, because the point of these tests is to pin the tick statement
/// against interleavings that are awkward to provoke through the write path.
async fn seed(pool: &sqlx::PgPool, rows: &[Row]) -> anyhow::Result<()> {
    for row in rows {
        let payload = row.payload.then(|| {
            serde_json::json!({ "type": "marker", "group": row.xid.map(|x| x.to_string()).unwrap_or_else(|| "legacy".into()) })
        });
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
    Ok(())
}

/// The log as `(commit_seq, sequence, group_last)`, in commit order.
async fn log_rows(pool: &sqlx::PgPool) -> anyhow::Result<Vec<(i64, i64, bool)>> {
    let rows = sqlx::query_as::<_, (i64, i64, bool)>(
        "SELECT commit_seq, sequence, group_last
         FROM persistent_outbox_commit_log ORDER BY commit_seq",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Drive the sequencer to quiescence, exactly as `Sequencer::tick_until_quiet`
/// does: re-tick while the scan watermark is below the frontier, stop when a
/// hole blocks the scan short of the window it asked for.
///
/// Returns the appended rows in the order the ticks produced them, so a test
/// can assert on delivery order and not just on the stored log.
async fn tick_until_quiet(
    pool: &sqlx::PgPool,
    frontier: i64,
    page: usize,
) -> anyhow::Result<Vec<(i64, i64)>> {
    let frontier = EventSequence::from(frontier as u64);
    let mut delivered = Vec::new();
    // Bounded so a regression of the livelock this guards against fails the
    // test instead of hanging the suite.
    for _ in 0..64 {
        let Some(tick): Option<SequenceTick<TestEvent>> =
            TestTables::sequence_tick(pool, frontier, page).await?
        else {
            break;
        };
        for item in &tick.appended {
            let event = item.as_ref().expect("seeded payloads decode");
            delivered.push((
                i64::from(event.commit_sequence.expect("commit lane sets it")),
                u64::from(event.sequence) as i64,
            ));
        }
        if tick.f_stop < tick.f_eff {
            break;
        }
        if tick.scan_water >= frontier {
            break;
        }
    }
    Ok(delivered)
}

async fn state(pool: &sqlx::PgPool) -> anyhow::Result<(i64, i64, i64)> {
    let row = sqlx::query_as::<_, (i64, i64, i64)>(
        "SELECT head, low_water, scan_water FROM persistent_outbox_commit_log_state WHERE id = 1",
    )
    .fetch_one(pool)
    .await?;
    Ok(row)
}

/// A transaction's events are contiguous in the commit lane even when
/// another transaction's inserts interleave with them, and groups are
/// ordered by their highest insert sequence.
#[tokio::test]
#[serial_test::file_serial]
async fn interleaved_groups_are_contiguous_and_commit_ordered() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipeout_outbox_tables(&pool).await?;

    // A inserts a1, B inserts b1+b2 and commits, A inserts a2 and commits.
    // Insert order is a1, b1, b2, a2.
    seed(&pool, &[ev(1, 7001), ev(2, 7002), ev(3, 7002), ev(4, 7001)]).await?;

    tick_until_quiet(&pool, 4, 1000).await?;

    // B closes at sequence 3, A at sequence 4, so B is ordered first and
    // each group's members are adjacent.
    assert_eq!(
        log_rows(&pool).await?,
        vec![(1, 2, false), (2, 3, true), (3, 1, false), (4, 4, true),],
    );
    Ok(())
}

/// Gap-fill placeholders occupy an insert sequence but are not events, so
/// they never reach the commit lane and never take a `commit_seq`.
#[tokio::test]
#[serial_test::file_serial]
async fn placeholders_are_never_logged() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipeout_outbox_tables(&pool).await?;

    seed(&pool, &[ev(1, 7010), placeholder(2), ev(3, 7011)]).await?;

    tick_until_quiet(&pool, 3, 1000).await?;

    // Dense despite the hole the placeholder fills: the lane numbers events,
    // not sequences.
    assert_eq!(log_rows(&pool).await?, vec![(1, 1, true), (2, 3, true)],);
    Ok(())
}

/// Rows written before `commit_xid` existed carry NULL and must each be
/// their own group rather than collapsing into one giant NULL group.
#[tokio::test]
#[serial_test::file_serial]
async fn legacy_null_xid_rows_are_singletons() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipeout_outbox_tables(&pool).await?;

    seed(&pool, &[legacy(1), legacy(2), legacy(3)]).await?;

    tick_until_quiet(&pool, 3, 1000).await?;

    // Every row closes its own group, so insert order is preserved and each
    // is a boundary.
    assert_eq!(
        log_rows(&pool).await?,
        vec![(1, 1, true), (2, 2, true), (3, 3, true)],
    );
    Ok(())
}

/// A group whose span exceeds the page size is still logged, and logged
/// whole. Regression test for a livelock: a scan window anchored on
/// `low_water` can never widen past an open group, because that group pins
/// `low_water`.
#[tokio::test]
#[serial_test::file_serial]
async fn group_spanning_beyond_page_is_held_then_delivered() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipeout_outbox_tables(&pool).await?;

    // Group A spans sequences 1 and 5; B, C, D are singletons between them.
    seed(
        &pool,
        &[
            ev(1, 7020),
            ev(2, 7021),
            ev(3, 7022),
            ev(4, 7023),
            ev(5, 7020),
        ],
    )
    .await?;

    // Page of 2 — smaller than group A's span of 5.
    tick_until_quiet(&pool, 5, 2).await?;

    // The singletons are logged first because they close first; group A is
    // held until its highest member is inside the window, then logged whole
    // and adjacent.
    assert_eq!(
        log_rows(&pool).await?,
        vec![
            (1, 2, true),
            (2, 3, true),
            (3, 4, true),
            (4, 1, false),
            (5, 5, true),
        ],
    );

    let (head, low_water, _) = state(&pool).await?;
    assert_eq!(head, 5);
    assert_eq!(
        low_water, 5,
        "every row logged, so low_water clears them all"
    );
    Ok(())
}

/// The tick never passes a hole: an absent sequence may still be an
/// in-flight transaction that will commit *below* rows already visible, so
/// logging past it could order a later-arriving event before one already
/// delivered. It resumes once the hole is filled.
#[tokio::test]
#[serial_test::file_serial]
async fn tick_stops_at_hole_and_resumes_once_filled() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipeout_outbox_tables(&pool).await?;

    // Sequence 2 is missing — an in-flight or rolled-back writer.
    seed(&pool, &[ev(1, 7030), ev(3, 7031), ev(4, 7032)]).await?;

    tick_until_quiet(&pool, 4, 1000).await?;

    assert_eq!(
        log_rows(&pool).await?,
        vec![(1, 1, true)],
        "nothing above the hole may be logged",
    );
    let (_, low_water, scan_water) = state(&pool).await?;
    assert_eq!(low_water, 1);
    assert_eq!(scan_water, 1, "the scan watermark must not pass the hole");

    // The gap filler places a placeholder; the sequencer may now proceed.
    seed(&pool, &[placeholder(2)]).await?;
    tick_until_quiet(&pool, 4, 1000).await?;

    assert_eq!(
        log_rows(&pool).await?,
        vec![(1, 1, true), (2, 3, true), (3, 4, true)],
    );
    Ok(())
}

/// Concurrent sequencers must not diverge: the state-row lock makes every
/// tick all-or-nothing, so the log stays dense and each insert sequence is
/// logged once. This is also the crash guarantee — a tick is one statement,
/// so a process dying mid-tick inserts nothing and leaves `head` unchanged.
#[tokio::test]
#[serial_test::file_serial]
async fn concurrent_ticks_stay_dense_and_log_each_sequence_once() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipeout_outbox_tables(&pool).await?;

    let rows: Vec<Row> = (1..=200).map(|n| ev(n, 7100 + (n % 37))).collect();
    seed(&pool, &rows).await?;

    // Four concurrent sequencers, all ticking the same state row.
    let mut tasks = Vec::new();
    for _ in 0..4 {
        let pool = pool.clone();
        tasks.push(tokio::spawn(async move {
            tick_until_quiet(&pool, 200, 16).await
        }));
    }
    for task in tasks {
        task.await??;
    }

    let logged = log_rows(&pool).await?;
    assert_eq!(logged.len(), 200, "every event logged exactly once");
    for (index, (commit_seq, _, _)) in logged.iter().enumerate() {
        assert_eq!(
            *commit_seq,
            index as i64 + 1,
            "commit_seq must be dense with no holes",
        );
    }
    let mut sequences: Vec<i64> = logged.iter().map(|(_, sequence, _)| *sequence).collect();
    sequences.sort_unstable();
    sequences.dedup();
    assert_eq!(sequences.len(), 200, "no insert sequence logged twice");

    let (head, low_water, _) = state(&pool).await?;
    assert_eq!(head, 200);
    assert_eq!(low_water, 200);
    Ok(())
}

/// Every group is contiguous in the log, and groups are ordered by their
/// highest insert sequence, under a randomised interleaving.
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

    tick_until_quiet(&pool, 300, 8).await?;

    let logged = log_rows(&pool).await?;
    assert_eq!(logged.len(), 300);

    // Dense.
    for (index, (commit_seq, _, _)) in logged.iter().enumerate() {
        assert_eq!(*commit_seq, index as i64 + 1);
    }

    let group_of: std::collections::HashMap<i64, i64> = assignments.into_iter().collect();

    // Each group occupies one unbroken run, and `group_last` marks its end.
    let mut seen_groups = std::collections::HashSet::new();
    let mut index = 0;
    while index < logged.len() {
        let group = group_of[&logged[index].1];
        assert!(
            seen_groups.insert(group),
            "group {group} appears in more than one run — it was split",
        );
        let mut members = Vec::new();
        while index < logged.len() && group_of[&logged[index].1] == group {
            members.push(logged[index].1);
            let is_last = logged[index].2;
            index += 1;
            let run_ended = index == logged.len() || group_of[&logged[index].1] != group;
            assert_eq!(
                is_last, run_ended,
                "group_last must mark exactly the run's end"
            );
        }
        // Within a group, members stay in insert order.
        let mut sorted = members.clone();
        sorted.sort_unstable();
        assert_eq!(members, sorted, "group members must stay in insert order");
    }

    Ok(())
}

/// Translating an insert cursor lands one below the lowest commit position
/// of anything above it, so nothing unseen is skipped.
#[tokio::test]
#[serial_test::file_serial]
async fn insert_cursor_translates_without_skipping() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipeout_outbox_tables(&pool).await?;

    seed(&pool, &[ev(1, 7300), ev(2, 7301), ev(3, 7301), ev(4, 7300)]).await?;
    tick_until_quiet(&pool, 4, 1000).await?;

    // Commit order is 2, 3, 1, 4. An insert cursor at 2 has seen sequences
    // 1 and 2, whose commit positions are 3 and 1.
    let cursor = TestTables::translate_insert_cursor(&pool, EventSequence::from(2u64)).await?;
    // Sequences above 2 are 3 (commit_seq 2) and 4 (commit_seq 4); the
    // lowest is 2, so the cursor is 1 and sequence 1 is redelivered.
    assert_eq!(cursor, CommitSequence::from(1u64));

    // Nothing above the insert cursor at all resolves to the log head.
    let at_head = TestTables::translate_insert_cursor(&pool, EventSequence::from(4u64)).await?;
    assert_eq!(at_head, CommitSequence::from(4u64));
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
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let mut listener = outbox.listen_commit_ordered(CommitSequence::BEGIN).await?;

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

    assert_eq!(first.commit_sequence, Some(CommitSequence::from(1u64)));
    assert_eq!(second.commit_sequence, Some(CommitSequence::from(2u64)));
    assert!(
        !first.commit_boundary,
        "the first of two events in a transaction does not close its group",
    );
    assert!(
        second.commit_boundary,
        "the last event of a transaction closes its group",
    );
    assert_eq!(
        first.commit_group, second.commit_group,
        "events of one transaction share a group",
    );
    assert!(first.commit_group.is_some());
    Ok(())
}

/// A commit-ordered listener created against an already-populated log reads
/// it back from the beginning, in commit order.
#[tokio::test]
#[serial_test::file_serial]
async fn commit_lane_backfills_existing_log() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
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

    // A listener starting at BEGIN must page the whole log back, not just
    // receive what is published after it subscribes.
    let mut listener = outbox.listen_commit_ordered(CommitSequence::BEGIN).await?;
    let mut received = Vec::new();
    for _ in 0..8 {
        let event = listener.next().await.expect("event")?;
        received.push(u64::from(event.commit_sequence.expect("commit lane")));
    }
    assert_eq!(received, (1..=8).collect::<Vec<u64>>());
    Ok(())
}

/// The insert lane is untouched by the commit lane's existence: no commit
/// position, no boundary flag, and the group is still reported.
#[tokio::test]
#[serial_test::file_serial]
async fn insert_lane_carries_group_but_no_commit_position() -> anyhow::Result<()> {
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
        .publish_persisted_in_op(
            &mut op,
            TestEvent::Marker {
                group: "insert".into(),
            },
        )
        .await?;
    op.commit().await?;

    let event = listener.next().await.expect("event")?;
    assert!(
        event.commit_sequence.is_none(),
        "insert lane must not carry a commit position",
    );
    assert!(
        !event.commit_boundary,
        "insert lane must never report a group boundary",
    );
    assert!(
        event.commit_group.is_some(),
        "group identity is available on both lanes",
    );
    Ok(())
}

/// The commit-ordered page read returns dense, ordered, decoded rows with
/// their lane metadata populated.
#[tokio::test]
#[serial_test::file_serial]
async fn commit_ordered_page_reads_in_lane_order() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipeout_outbox_tables(&pool).await?;

    seed(&pool, &[ev(1, 7400), ev(2, 7401), ev(3, 7401), ev(4, 7400)]).await?;
    tick_until_quiet(&pool, 4, 1000).await?;

    let page =
        TestTables::load_commit_ordered_page::<TestEvent>(&pool, CommitSequence::BEGIN, 10).await?;
    let decoded: Vec<(u64, u64, bool)> = page
        .into_iter()
        .map(|item| {
            let event = item.expect("seeded payloads decode");
            (
                u64::from(event.commit_sequence.expect("commit lane sets it")),
                u64::from(event.sequence),
                event.commit_boundary,
            )
        })
        .collect();

    assert_eq!(
        decoded,
        vec![(1, 2, false), (2, 3, true), (3, 1, false), (4, 4, true)],
    );

    // Paging is by commit position and stops at the head.
    let tail =
        TestTables::load_commit_ordered_page::<TestEvent>(&pool, CommitSequence::from(4u64), 10)
            .await?;
    assert!(tail.is_empty());
    Ok(())
}
