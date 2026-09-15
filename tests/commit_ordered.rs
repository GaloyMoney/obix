mod helpers;

use futures::stream::StreamExt;
use obix::{CommitGroupId, CommitSequence, EventSequence, MailboxConfig, MailboxTables};

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
/// `publish`, because the point of these tests is to pin the append
/// statement against interleavings that are awkward to provoke through the
/// write path.
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

async fn state(pool: &sqlx::PgPool) -> anyhow::Result<(i64, i64)> {
    let row = sqlx::query_as::<_, (i64, i64)>(
        "SELECT head, cursor FROM persistent_outbox_commit_log_state WHERE id = 1",
    )
    .fetch_one(pool)
    .await?;
    Ok(row)
}

/// Every seeded row in insert order, as the sequencer's listener would
/// deliver it.
async fn stream(pool: &sqlx::PgPool) -> anyhow::Result<Vec<(EventSequence, CommitGroupId, bool)>> {
    let rows = sqlx::query_as::<_, (i64, i64, bool)>(
        "SELECT sequence, commit_xid, payload IS NOT NULL
         FROM persistent_outbox_events ORDER BY sequence",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(sequence, xid, has_payload)| {
            (
                EventSequence::from(sequence as u64),
                CommitGroupId::from(xid),
                has_payload,
            )
        })
        .collect())
}

/// Replay the sequencer's fold (`src/out/persistent/sequencer.rs`) over the
/// seeded rows, without the task or the listener: the same skip rules and
/// the same one call to `append_commit_group` per first-sighted group.
///
/// Returns the appended rows in the order the fold produced them, so a test
/// can assert on delivery order and not just on the stored log.
async fn fold(pool: &sqlx::PgPool, seed_logged_ahead: bool) -> anyhow::Result<Vec<(i64, i64)>> {
    let (_, cursor) = {
        let (head, cursor) = state(pool).await?;
        (head, EventSequence::from(cursor as u64))
    };
    let mut logged_ahead: std::collections::BTreeSet<EventSequence> = if seed_logged_ahead {
        TestTables::commit_logged_above(pool, cursor)
            .await?
            .into_iter()
            .collect()
    } else {
        std::collections::BTreeSet::new()
    };
    let mut seen: std::collections::HashMap<CommitGroupId, EventSequence> =
        std::collections::HashMap::new();
    let mut delivered = Vec::new();

    for (sequence, group, has_payload) in stream(pool).await? {
        if sequence <= cursor && !logged_ahead.contains(&sequence) {
            // Already folded in an earlier pass.
            continue;
        }
        if logged_ahead.remove(&sequence) {
            continue;
        }
        if !has_payload {
            continue;
        }
        if let Some(group_max) = seen.get(&group).copied() {
            if sequence >= group_max {
                seen.remove(&group);
            }
            continue;
        }
        let append = TestTables::append_commit_group::<TestEvent>(pool, group, sequence).await?;
        if append.group_max > sequence {
            seen.insert(group, append.group_max);
        }
        for row in append.appended {
            let event = row.event.expect("seeded payloads decode");
            delivered.push((
                i64::from(row.commit_sequence),
                u64::from(event.sequence) as i64,
            ));
        }
    }
    Ok(delivered)
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

    fold(&pool, true).await?;

    // A is first-sighted at sequence 1 and B at 2, so A is ordered first;
    // each group's members are adjacent.
    assert_eq!(
        log_rows(&pool).await?,
        vec![(1, 1, false), (2, 4, true), (3, 2, false), (4, 3, true)],
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

    seed(&pool, &[ev(1, 7010), placeholder(2, 7010), ev(3, 7011)]).await?;

    fold(&pool, true).await?;

    // Dense despite the placeholder sharing group 7010: the lane numbers
    // events, not sequences.
    assert_eq!(log_rows(&pool).await?, vec![(1, 1, true), (2, 3, true)]);
    Ok(())
}

/// The append is conditional on the state row's cursor, re-checked against
/// the row version a lock wait resolved to. A second attempt at or below the
/// cursor changes nothing but still reports the group's extent.
#[tokio::test]
#[serial_test::file_serial]
async fn append_is_rejected_at_or_below_the_cursor() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipeout_outbox_tables(&pool).await?;

    seed(&pool, &[ev(1, 7020), ev(2, 7021), ev(3, 7020)]).await?;

    let group = CommitGroupId::from(7020);
    let first =
        TestTables::append_commit_group::<TestEvent>(&pool, group, EventSequence::from(1u64))
            .await?;
    assert_eq!(first.appended.len(), 2);
    assert_eq!(first.group_max, EventSequence::from(3u64));
    assert_eq!(state(&pool).await?, (2, 1));

    let second =
        TestTables::append_commit_group::<TestEvent>(&pool, group, EventSequence::from(1u64))
            .await?;
    assert!(
        second.appended.is_empty(),
        "an append at the cursor must be rejected",
    );
    assert_eq!(
        second.group_max,
        EventSequence::from(3u64),
        "the extent is reported even on rejection",
    );
    assert_eq!(
        state(&pool).await?,
        (2, 1),
        "a rejected append leaves the state row untouched",
    );
    Ok(())
}

/// Concurrent sequencers must not diverge: the cursor check makes every
/// append all-or-nothing, so the log stays dense and each insert sequence is
/// logged once. This is also the crash guarantee — an append is one
/// statement, so a process dying mid-append inserts nothing and leaves
/// `head` unchanged.
#[tokio::test]
#[serial_test::file_serial]
async fn concurrent_folds_stay_dense_and_log_each_sequence_once() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipeout_outbox_tables(&pool).await?;

    let rows: Vec<Row> = (1..=200).map(|n| ev(n, 7100 + (n % 37))).collect();
    seed(&pool, &rows).await?;

    let mut tasks = Vec::new();
    for _ in 0..4 {
        let pool = pool.clone();
        tasks.push(tokio::spawn(async move { fold(&pool, true).await }));
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
    assert_eq!(state(&pool).await?.0, 200);
    Ok(())
}

/// Every group is contiguous in the log, and groups are ordered by the
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

    fold(&pool, true).await?;

    let logged = log_rows(&pool).await?;
    assert_eq!(logged.len(), 300);
    for (index, (commit_seq, _, _)) in logged.iter().enumerate() {
        assert_eq!(*commit_seq, index as i64 + 1);
    }

    let group_of: std::collections::HashMap<i64, i64> = assignments.into_iter().collect();

    // Each group occupies one unbroken run, `group_last` marks its end, and
    // the runs are ordered by each group's lowest sequence.
    let mut seen_groups = std::collections::HashSet::new();
    let mut previous_run_min = 0;
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

        let run_min = members[0];
        assert!(
            run_min > previous_run_min,
            "runs must be ordered by their lowest member: {run_min} after {previous_run_min}",
        );
        previous_run_min = run_min;
    }

    Ok(())
}

/// A group appended before a crash can have members above the cursor the
/// crash left behind. The restart seed is what stops the resumed fold from
/// appending it a second time.
#[tokio::test]
#[serial_test::file_serial]
async fn restart_seed_prevents_double_append() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipeout_outbox_tables(&pool).await?;

    // Group X spans sequences 1 and 5, with singletons between.
    seed(
        &pool,
        &[
            ev(1, 7300),
            ev(2, 7301),
            ev(3, 7302),
            ev(4, 7303),
            ev(5, 7300),
        ],
    )
    .await?;

    // Simulate the crash: X was appended at sequence 1, nothing after.
    TestTables::append_commit_group::<TestEvent>(
        &pool,
        CommitGroupId::from(7300),
        EventSequence::from(1u64),
    )
    .await?;
    let (_, cursor) = state(&pool).await?;
    assert_eq!(cursor, 1);

    let seed_rows =
        TestTables::commit_logged_above(&pool, EventSequence::from(cursor as u64)).await?;
    assert_eq!(
        seed_rows,
        vec![EventSequence::from(5u64)],
        "sequence 5 is logged but sits above the cursor",
    );

    fold(&pool, true).await?;

    let logged = log_rows(&pool).await?;
    let mut sequences: Vec<i64> = logged.iter().map(|(_, sequence, _)| *sequence).collect();
    sequences.sort_unstable();
    let deduped = {
        let mut d = sequences.clone();
        d.dedup();
        d
    };
    assert_eq!(
        sequences, deduped,
        "no insert sequence may be logged twice: {logged:?}",
    );
    assert_eq!(logged.len(), 5, "every event logged exactly once");
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

    let mut listener = outbox.listen_commit_ordered(CommitSequence::BEGIN);

    // Two events in one transaction: one group, so the second closes it.
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Marker { group: "a1".into() })
        .await?;
    outbox
        .publish_persisted_in_op(&mut op, TestEvent::Marker { group: "a2".into() })
        .await?;
    op.commit().await?;

    let first = listener.next().await.expect("first event");
    let second = listener.next().await.expect("second event");

    assert_eq!(first.commit_sequence, CommitSequence::from(1u64));
    assert_eq!(second.commit_sequence, CommitSequence::from(2u64));
    assert!(
        !first.commit_boundary,
        "the first of two events in a transaction does not close its group",
    );
    assert!(
        second.commit_boundary,
        "the last event of a transaction closes its group",
    );
    let first = first.event?;
    let second = second.event?;
    assert_eq!(
        first.commit_group, second.commit_group,
        "events of one transaction share a group",
    );
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
    let mut listener = outbox.listen_commit_ordered(CommitSequence::BEGIN);
    let mut received = Vec::new();
    for _ in 0..8 {
        let item = listener.next().await.expect("event");
        item.event?;
        received.push(u64::from(item.commit_sequence));
    }
    assert_eq!(received, (1..=8).collect::<Vec<u64>>());
    Ok(())
}

/// The sequencer runs in every process, whether or not that process has a
/// commit-ordered listener: the log must not wait for a subscriber.
#[tokio::test]
#[serial_test::file_serial]
async fn sequencer_runs_without_a_commit_listener() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = init_outbox::<TestEvent>(
        &pool,
        MailboxConfig::builder()
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
        if state(&pool).await?.0 == 4 {
            break;
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "the log never reached the published head",
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    Ok(())
}

/// Two processes sharing a pool derive the same commit order: whichever one
/// wins an append, both deliver the identical lane.
#[tokio::test]
#[serial_test::file_serial]
async fn two_processes_deliver_identical_commit_order() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let config = MailboxConfig::builder()
        .build()
        .expect("Couldn't build MailboxConfig");
    let first = init_outbox::<TestEvent>(&pool, config.clone()).await?;
    let second = obix::out::Outbox::<TestEvent, TestTables>::init(&pool, config).await?;

    let mut first_listener = first.listen_commit_ordered(CommitSequence::BEGIN);
    let mut second_listener = second.listen_commit_ordered(CommitSequence::BEGIN);

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
        let item = first_listener.next().await.expect("event");
        from_first.push((
            u64::from(item.commit_sequence),
            u64::from(item.event?.sequence),
        ));
        let item = second_listener.next().await.expect("event");
        from_second.push((
            u64::from(item.commit_sequence),
            u64::from(item.event?.sequence),
        ));
    }
    assert_eq!(
        from_first, from_second,
        "both processes must deliver the same commit order",
    );
    Ok(())
}

/// The insert lane is untouched by the commit lane's existence: the group is
/// reported there too, and the item carries no lane position.
#[tokio::test]
#[serial_test::file_serial]
async fn insert_lane_carries_group() -> anyhow::Result<()> {
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
    let second = outbox
        .publish_persisted_in_op(
            &mut op,
            TestEvent::Marker {
                group: "insert2".into(),
            },
        )
        .await;
    second?;
    op.commit().await?;

    let first = listener.next().await.expect("event")?;
    let second = listener.next().await.expect("event")?;
    assert_eq!(
        first.commit_group, second.commit_group,
        "group identity is available on the insert lane",
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
    fold(&pool, true).await?;

    let page =
        TestTables::load_commit_ordered_page::<TestEvent>(&pool, CommitSequence::BEGIN, 10).await?;
    let decoded: Vec<(u64, u64, bool)> = page
        .into_iter()
        .map(|row| {
            let commit_sequence = u64::from(row.commit_sequence);
            let boundary = row.commit_boundary;
            let event = row.event.expect("seeded payloads decode");
            (commit_sequence, u64::from(event.sequence), boundary)
        })
        .collect();

    assert_eq!(
        decoded,
        vec![(1, 1, false), (2, 4, true), (3, 2, false), (4, 3, true)],
    );

    // Paging is by commit position and stops at the head.
    let tail =
        TestTables::load_commit_ordered_page::<TestEvent>(&pool, CommitSequence::from(4u64), 10)
            .await?;
    assert!(tail.is_empty());
    Ok(())
}
