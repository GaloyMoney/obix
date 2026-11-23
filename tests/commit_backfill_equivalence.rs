//! A commit-lane backfill must deliver the same numbering, row for row, under
//! every combination of `backfill_read_ahead`, `commit_backfill_scan` and
//! `commit_backfill_concurrency`.

mod helpers;

use std::collections::HashMap;
use std::time::Duration;

use futures::stream::StreamExt;
use obix::{CommitBackfillScan, CommitLane, CommitSequence, MailboxConfig, out::Outbox};
use serde::{Deserialize, Serialize};
use serial_test::file_serial;

use helpers::{TestTables, init_pool, wipeout_outbox_tables};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
enum TestEvent {
    Ping { n: u64 },
}

/// `(commit position, insert sequence, is-last-in-group)`.
type Row = (i64, i64, bool);

fn config(read_ahead: usize, scan: CommitBackfillScan, concurrency: usize) -> MailboxConfig {
    MailboxConfig::builder()
        .commit_lane(CommitLane::Enabled)
        .commit_checkpoint_every(3)
        .event_buffer_size(16)
        .backfill_page_size(20)
        .backfill_read_ahead(read_ahead)
        .commit_backfill_scan(scan)
        .commit_backfill_concurrency(concurrency)
        .gap_fill_grace(Duration::from_millis(100))
        .build()
}

async fn open(
    pool: &sqlx::PgPool,
    config: MailboxConfig,
) -> anyhow::Result<Outbox<TestEvent, TestTables>> {
    Ok(Outbox::<TestEvent, TestTables>::init(pool, config).await?)
}

async fn publish_group(
    outbox: &Outbox<TestEvent, TestTables>,
    next: &mut u64,
    count: u64,
) -> anyhow::Result<()> {
    let mut op = outbox.begin_op().await?;
    for _ in 0..count {
        outbox
            .publish_persisted_in_op(&mut op, TestEvent::Ping { n: *next })
            .await?;
        *next += 1;
    }
    op.commit().await?;
    Ok(())
}

/// Two groups held open at once, publishing alternately, so their rows
/// interleave in insert order.
async fn publish_interleaved(
    outbox: &Outbox<TestEvent, TestTables>,
    next: &mut u64,
) -> anyhow::Result<()> {
    let mut a = outbox.begin_op().await?;
    let mut b = outbox.begin_op().await?;
    for _ in 0..3 {
        outbox
            .publish_persisted_in_op(&mut a, TestEvent::Ping { n: *next })
            .await?;
        *next += 1;
        outbox
            .publish_persisted_in_op(&mut b, TestEvent::Ping { n: *next })
            .await?;
        *next += 1;
    }
    b.commit().await?;
    a.commit().await?;
    Ok(())
}

/// A writer that took sequences and rolled back, leaving holes: the events
/// are only inserted at commit, so the rollback is reproduced by consuming
/// the sequences directly. The gap filler later plugs them with placeholders.
async fn publish_rolled_back(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    for _ in 0..2 {
        sqlx::query("SELECT nextval('persistent_outbox_events_sequence_seq')")
            .execute(pool)
            .await?;
    }
    Ok(())
}

/// Returns how many events were committed.
async fn publish_data_set(
    pool: &sqlx::PgPool,
    outbox: &Outbox<TestEvent, TestTables>,
) -> anyhow::Result<usize> {
    let mut next = 0;
    let mut committed = 0;
    for round in 0..14u64 {
        for size in [1, 1, 5, 2] {
            publish_group(outbox, &mut next, size).await?;
            committed += size as usize;
        }
        if round % 2 == 0 {
            publish_interleaved(outbox, &mut next).await?;
            committed += 6;
        }
        if round % 5 == 3 {
            publish_rolled_back(pool).await?;
        }
        publish_group(outbox, &mut next, 20).await?;
        committed += 20;
    }
    Ok(committed)
}

async fn replay(
    outbox: &Outbox<TestEvent, TestTables>,
    after: i64,
    count: usize,
) -> anyhow::Result<Vec<Row>> {
    let mut listener = outbox.listen_commit_ordered(CommitSequence::from(after as u64))?;
    let mut rows = Vec::with_capacity(count);
    while rows.len() < count {
        let item = tokio::time::timeout(Duration::from_secs(30), listener.next())
            .await
            .map_err(|_| anyhow::anyhow!("stalled after {} of {count} rows", rows.len()))?
            .expect("the stream stays open")?;
        rows.push((
            i64::from(item.position()),
            u64::from(item.sequence) as i64,
            item.is_commit_boundary(),
        ));
    }
    Ok(rows)
}

async fn checkpoint_commit_seqs(pool: &sqlx::PgPool) -> anyhow::Result<Vec<i64>> {
    Ok(sqlx::query_scalar::<_, i64>(
        "SELECT commit_seq FROM persistent_outbox_commit_checkpoints ORDER BY commit_seq",
    )
    .fetch_all(pool)
    .await?)
}

/// Publish the data set and take the reference numbering from a default
/// outbox, once its checkpoints are in.
async fn seed(pool: &sqlx::PgPool) -> anyhow::Result<(usize, Vec<Row>)> {
    wipeout_outbox_tables(pool).await?;
    let outbox = open(pool, config(1, CommitBackfillScan::Wide, 1)).await?;
    let total = publish_data_set(pool, &outbox).await?;
    let reference = replay(&outbox, 0, total).await?;

    // The data set must exercise what the test claims to.
    assert!(
        checkpoint_commit_seqs(pool).await?.len() >= 10,
        "expected many checkpoints"
    );
    let open_groups: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM persistent_outbox_commit_checkpoints WHERE open_groups <> '[]'::jsonb",
    )
    .fetch_one(pool)
    .await?;
    assert!(open_groups > 0, "no checkpoint with an open group");
    // The gap filler fills the rolled-back sequences after its grace.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let placeholders: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM persistent_outbox_events WHERE payload IS NULL",
        )
        .fetch_one(pool)
        .await?;
        if placeholders > 0 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "no gap-filler placeholder"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok((total, reference))
}

#[tokio::test]
#[file_serial]
async fn every_knob_combination_replays_identically() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let (total, reference) = seed(&pool).await?;

    // Positions are dense from 1 and each group ends on a boundary.
    for (i, (position, _, _)) in reference.iter().enumerate() {
        assert_eq!(*position, i as i64 + 1);
    }
    let groups: HashMap<i64, i64> = sqlx::query_as::<_, (i64, i64)>(
        "SELECT sequence, commit_xid FROM persistent_outbox_events",
    )
    .fetch_all(&pool)
    .await?
    .into_iter()
    .collect();
    for pair in reference.windows(2) {
        let same_group = groups[&pair[0].1] == groups[&pair[1].1];
        assert_eq!(pair[0].2, !same_group, "a group was split or merged");
    }

    for read_ahead in [1, 3] {
        for scan in [CommitBackfillScan::Wide, CommitBackfillScan::Narrow] {
            for concurrency in [1, 4] {
                let outbox = open(&pool, config(read_ahead, scan, concurrency)).await?;
                let rows = replay(&outbox, 0, total).await?;
                assert_eq!(
                    rows, reference,
                    "read_ahead={read_ahead} scan={scan:?} concurrency={concurrency}"
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn resuming_mid_range_delivers_the_exact_tail() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let (total, reference) = seed(&pool).await?;
    let boundary = checkpoint_commit_seqs(&pool).await?[4];
    let off_boundary = boundary + 2;

    for concurrency in [1, 4] {
        for scan in [CommitBackfillScan::Wide, CommitBackfillScan::Narrow] {
            let outbox = open(&pool, config(1, scan, concurrency)).await?;
            for after in [boundary, off_boundary] {
                let tail = &reference[after as usize..];
                let rows = replay(&outbox, after, total - after as usize).await?;
                assert_eq!(rows[0].0, after + 1);
                assert_eq!(
                    &rows, tail,
                    "after={after} scan={scan:?} concurrency={concurrency}"
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn dropping_the_subscriber_ends_the_backfill_tasks() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let (_, _) = seed(&pool).await?;

    for scan in [CommitBackfillScan::Wide, CommitBackfillScan::Narrow] {
        let outbox = open(&pool, config(1, scan, 4)).await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
        let held = |pool: &sqlx::PgPool| pool.size() as usize - pool.num_idle();
        let baseline = held(&pool);

        let mut listener = outbox.listen_commit_ordered(CommitSequence::BEGIN)?;
        // Take one row, so the backfill is under way and its segments are
        // filling their channels; then hang up.
        let _ = tokio::time::timeout(Duration::from_secs(10), listener.next())
            .await?
            .expect("the stream stays open")?;
        drop(listener);

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while held(&pool) > baseline && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            held(&pool) <= baseline,
            "backfill tasks still hold pool connections ({scan:?})"
        );
    }
    Ok(())
}
