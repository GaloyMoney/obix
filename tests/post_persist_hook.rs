mod helpers;

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use es_entity::AtomicOperation;
use es_entity::hooks::{BoxFuture, HookOperation};
use futures::stream::StreamExt;
use obix::{
    MailboxConfig,
    out::{Outbox, PersistentOutboxEvent, PostPersistHook},
};
use serde::{Deserialize, Serialize};
use serial_test::file_serial;

use helpers::{TestTables, init_outbox, init_pool};

// All payload types intentionally share one serde shape: every outbox in
// these tests writes to the same physical test table, so every outbox's
// cache must be able to deserialize every row (gap-fill loads rows
// regardless of which outbox produced them).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum SourceEvent {
    Source(u64),
    Mapped(u64),
    Native(u64),
    Hop(u64),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum DestEvent {
    Source(u64),
    Mapped(u64),
    Native(u64),
    Hop(u64),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum HopEvent {
    Source(u64),
    Mapped(u64),
    Native(u64),
    Hop(u64),
}

async fn outbox_row_count(pool: &sqlx::PgPool) -> anyhow::Result<i64> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM persistent_outbox_events")
        .fetch_one(pool)
        .await?;
    Ok(count)
}

async fn payloads_by_sequence(pool: &sqlx::PgPool) -> anyhow::Result<Vec<serde_json::Value>> {
    let payloads: Vec<serde_json::Value> =
        sqlx::query_scalar("SELECT payload FROM persistent_outbox_events ORDER BY sequence")
            .fetch_all(pool)
            .await?;
    Ok(payloads)
}

fn default_config() -> MailboxConfig {
    MailboxConfig::builder()
        .build()
        .expect("Couldn't build MailboxConfig")
}

/// Records every chunk the hook sees (sequence + payload), plus the row
/// counts visible through the op's connection (inside the tx) and through
/// the pool (outside the tx) at invocation time.
struct RecordingHook {
    chunks: Arc<Mutex<Vec<Vec<(u64, SourceEvent)>>>>,
    in_tx_counts: Arc<Mutex<Vec<i64>>>,
    outside_tx_counts: Arc<Mutex<Vec<i64>>>,
    pool: sqlx::PgPool,
}

impl PostPersistHook<SourceEvent> for RecordingHook {
    fn on_persisted<'a>(
        &'a self,
        op: &'a mut HookOperation<'_>,
        events: &'a [PersistentOutboxEvent<SourceEvent>],
    ) -> BoxFuture<'a, Result<(), sqlx::Error>> {
        Box::pin(async move {
            let in_tx: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM persistent_outbox_events")
                .fetch_one(&mut *op.connection())
                .await?;
            let outside: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM persistent_outbox_events")
                .fetch_one(&self.pool)
                .await?;
            self.in_tx_counts.lock().unwrap().push(in_tx);
            self.outside_tx_counts.lock().unwrap().push(outside);
            self.chunks.lock().unwrap().push(
                events
                    .iter()
                    .map(|e| {
                        (
                            u64::from(e.sequence),
                            e.payload.clone().expect("payload present"),
                        )
                    })
                    .collect(),
            );
            Ok(())
        })
    }
}

/// Counts invocations — for asserting a hook did or did not fire.
#[derive(Clone)]
struct CountingHook {
    invocations: Arc<AtomicUsize>,
}

impl PostPersistHook<SourceEvent> for CountingHook {
    fn on_persisted<'a>(
        &'a self,
        _op: &'a mut HookOperation<'_>,
        _events: &'a [PersistentOutboxEvent<SourceEvent>],
    ) -> BoxFuture<'a, Result<(), sqlx::Error>> {
        self.invocations.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

/// Always errors — a hook failure must abort the whole transaction.
struct FailingHook;

impl PostPersistHook<SourceEvent> for FailingHook {
    fn on_persisted<'a>(
        &'a self,
        _op: &'a mut HookOperation<'_>,
        _events: &'a [PersistentOutboxEvent<SourceEvent>],
    ) -> BoxFuture<'a, Result<(), sqlx::Error>> {
        Box::pin(async { Err(sqlx::Error::Protocol("post-persist hook veto".into())) })
    }
}

/// The repost consumer from the design doc: map source events into the
/// destination outbox's payload type and publish them from inside the hook —
/// same transaction, immediate-insert path.
struct RepostHook {
    dest: Outbox<DestEvent, TestTables>,
}

impl PostPersistHook<SourceEvent> for RepostHook {
    fn on_persisted<'a>(
        &'a self,
        op: &'a mut HookOperation<'_>,
        events: &'a [PersistentOutboxEvent<SourceEvent>],
    ) -> BoxFuture<'a, Result<(), sqlx::Error>> {
        Box::pin(async move {
            let mapped: Vec<DestEvent> = events
                .iter()
                .filter_map(|e| match e.payload.as_ref()? {
                    SourceEvent::Source(n) => Some(DestEvent::Mapped(*n)),
                    _ => None,
                })
                .collect();
            if mapped.is_empty() {
                return Ok(());
            }
            self.dest.publish_all_persisted(op, mapped).await
        })
    }
}

/// Second hop for the chain test: dest events repost onwards into a third
/// outbox.
struct HopHook {
    next: Outbox<HopEvent, TestTables>,
}

impl PostPersistHook<DestEvent> for HopHook {
    fn on_persisted<'a>(
        &'a self,
        op: &'a mut HookOperation<'_>,
        events: &'a [PersistentOutboxEvent<DestEvent>],
    ) -> BoxFuture<'a, Result<(), sqlx::Error>> {
        Box::pin(async move {
            let mapped: Vec<HopEvent> = events
                .iter()
                .filter_map(|e| match e.payload.as_ref()? {
                    DestEvent::Mapped(n) => Some(HopEvent::Hop(*n)),
                    _ => None,
                })
                .collect();
            if mapped.is_empty() {
                return Ok(());
            }
            self.next.publish_all_persisted(op, mapped).await
        })
    }
}

#[tokio::test]
#[file_serial]
async fn hook_sees_persisted_events_pre_commit() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = init_outbox::<SourceEvent>(&pool, default_config()).await?;

    let chunks = Arc::new(Mutex::new(Vec::new()));
    let in_tx_counts = Arc::new(Mutex::new(Vec::new()));
    let outside_tx_counts = Arc::new(Mutex::new(Vec::new()));
    outbox.add_post_persist_hook(RecordingHook {
        chunks: chunks.clone(),
        in_tx_counts: in_tx_counts.clone(),
        outside_tx_counts: outside_tx_counts.clone(),
        pool: pool.clone(),
    });

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_all_persisted(&mut op, [SourceEvent::Source(1), SourceEvent::Source(2)])
        .await?;

    // On a hook-supporting op nothing persists (and no hook fires) until
    // commit.
    assert!(chunks.lock().unwrap().is_empty());

    op.commit().await?;

    let chunks = chunks.lock().unwrap();
    assert_eq!(chunks.len(), 1, "one chunk for a small publish");
    assert_eq!(
        chunks[0],
        [(1, SourceEvent::Source(1)), (2, SourceEvent::Source(2))],
        "hook sees persisted events with assigned sequences"
    );
    assert_eq!(
        in_tx_counts.lock().unwrap().as_slice(),
        [2],
        "rows visible through the op's connection inside the tx"
    );
    assert_eq!(
        outside_tx_counts.lock().unwrap().as_slice(),
        [0],
        "rows not visible outside the tx before commit"
    );
    assert_eq!(outbox_row_count(&pool).await?, 2);
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn hook_error_aborts_commit() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = init_outbox::<SourceEvent>(&pool, default_config()).await?;
    outbox.add_post_persist_hook(FailingHook);

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, SourceEvent::Source(1))
        .await?;
    assert!(
        op.commit().await.is_err(),
        "a hook error must fail the commit"
    );
    assert_eq!(
        outbox_row_count(&pool).await?,
        0,
        "the tx rolled back — including the events that triggered the hook"
    );
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn rollback_never_invokes_hook() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = init_outbox::<SourceEvent>(&pool, default_config()).await?;

    let invocations = Arc::new(AtomicUsize::new(0));
    outbox.add_post_persist_hook(CountingHook {
        invocations: invocations.clone(),
    });

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, SourceEvent::Source(1))
        .await?;
    drop(op); // rollback

    assert_eq!(invocations.load(Ordering::SeqCst), 0);
    assert_eq!(outbox_row_count(&pool).await?, 0);
    Ok(())
}

/// Publishes on an op without commit-hook support (a bare `sqlx::Transaction`)
/// insert immediately — post-persist hooks must fire on that path too, which
/// is what makes "every persist path" literally true.
#[tokio::test]
#[file_serial]
async fn force_path_fires_hooks() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = init_outbox::<SourceEvent>(&pool, default_config()).await?;

    let invocations = Arc::new(AtomicUsize::new(0));
    outbox.add_post_persist_hook(CountingHook {
        invocations: invocations.clone(),
    });

    let mut tx = pool.begin().await?;
    outbox
        .publish_persisted_in_op(&mut tx, SourceEvent::Source(1))
        .await?;
    assert_eq!(
        invocations.load(Ordering::SeqCst),
        1,
        "on the force path the hook fires during the publish call itself"
    );
    tx.commit().await?;
    assert_eq!(outbox_row_count(&pool).await?, 1);
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn repost_commits_atomically() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let source = init_outbox::<SourceEvent>(&pool, default_config()).await?;
    let dest = Outbox::<DestEvent, TestTables>::init(&pool, default_config()).await?;

    source.add_post_persist_hook(RepostHook { dest: dest.clone() });

    let mut op = source.begin_op().await?;
    source
        .publish_persisted_in_op(&mut op, SourceEvent::Source(7))
        .await?;
    op.commit().await?;

    assert_eq!(
        payloads_by_sequence(&pool).await?,
        [
            serde_json::json!({"Source": 7}),
            serde_json::json!({"Mapped": 7}),
        ],
        "source event and its reposted projection committed in one tx"
    );
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn repost_rolls_back_atomically() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let source = init_outbox::<SourceEvent>(&pool, default_config()).await?;
    let dest = Outbox::<DestEvent, TestTables>::init(&pool, default_config()).await?;

    source.add_post_persist_hook(RepostHook { dest: dest.clone() });

    let mut op = source.begin_op().await?;
    source
        .publish_persisted_in_op(&mut op, SourceEvent::Source(7))
        .await?;
    drop(op); // rollback

    assert_eq!(
        outbox_row_count(&pool).await?,
        0,
        "neither source nor reposted rows survive a rollback"
    );
    Ok(())
}

/// A hook publishing into outbox B runs B's own post-persist hooks (force
/// path) — chains compose.
#[tokio::test]
#[file_serial]
async fn chained_reposts_compose() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let source = init_outbox::<SourceEvent>(&pool, default_config()).await?;
    let dest = Outbox::<DestEvent, TestTables>::init(&pool, default_config()).await?;
    let hop = Outbox::<HopEvent, TestTables>::init(&pool, default_config()).await?;

    source.add_post_persist_hook(RepostHook { dest: dest.clone() });
    dest.add_post_persist_hook(HopHook { next: hop.clone() });

    let mut op = source.begin_op().await?;
    source
        .publish_persisted_in_op(&mut op, SourceEvent::Source(3))
        .await?;
    op.commit().await?;

    assert_eq!(
        payloads_by_sequence(&pool).await?,
        [
            serde_json::json!({"Source": 3}),
            serde_json::json!({"Mapped": 3}),
            serde_json::json!({"Hop": 3}),
        ],
        "A→B→C: each hop persisted in the same tx, in causal order"
    );
    Ok(())
}

/// Multiple publishes on one op merge into a single commit hook; the hook
/// fires once per persisted chunk and sees every event exactly once.
#[tokio::test]
#[file_serial]
async fn merged_publishes_chunked_exactly_once() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = init_outbox::<SourceEvent>(
        &pool,
        MailboxConfig::builder()
            .persist_events_batch_size(2usize)
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let chunks = Arc::new(Mutex::new(Vec::new()));
    outbox.add_post_persist_hook(RecordingHook {
        chunks: chunks.clone(),
        in_tx_counts: Arc::new(Mutex::new(Vec::new())),
        outside_tx_counts: Arc::new(Mutex::new(Vec::new())),
        pool: pool.clone(),
    });

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_all_persisted(
            &mut op,
            [
                SourceEvent::Source(1),
                SourceEvent::Source(2),
                SourceEvent::Source(3),
            ],
        )
        .await?;
    outbox
        .publish_all_persisted(&mut op, [SourceEvent::Source(4), SourceEvent::Source(5)])
        .await?;
    op.commit().await?;

    let chunks = chunks.lock().unwrap();
    assert_eq!(
        chunks.iter().map(Vec::len).collect::<Vec<_>>(),
        [2, 2, 1],
        "batch_size 2 over 5 merged events → chunks of 2, 2, 1"
    );
    let flattened: Vec<_> = chunks.iter().flatten().cloned().collect();
    assert_eq!(
        flattened,
        (1..=5)
            .map(|n| (n, SourceEvent::Source(n)))
            .collect::<Vec<_>>(),
        "chunks cover all merged events exactly once, in publish order"
    );
    Ok(())
}

/// Hooks are snapshotted when a publish constructs the op's commit hook.
/// Registration after that point does not affect the in-flight op (later
/// publishes merge into the existing hook) — only subsequent ops see it.
#[tokio::test]
#[file_serial]
async fn late_registration_affects_only_subsequent_ops() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = init_outbox::<SourceEvent>(&pool, default_config()).await?;

    let invocations = Arc::new(AtomicUsize::new(0));

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, SourceEvent::Source(1))
        .await?;
    outbox.add_post_persist_hook(CountingHook {
        invocations: invocations.clone(),
    });
    outbox
        .publish_persisted_in_op(&mut op, SourceEvent::Source(2))
        .await?;
    op.commit().await?;
    assert_eq!(
        invocations.load(Ordering::SeqCst),
        0,
        "registration after the op's first publish is not seen by that op"
    );

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, SourceEvent::Source(3))
        .await?;
    op.commit().await?;
    assert_eq!(
        invocations.load(Ordering::SeqCst),
        1,
        "the next op snapshots the registered hook"
    );
    Ok(())
}

/// With es-entity ≥ 0.11.7 commit hooks run in registration order, so the
/// relative order of destination-native vs reposted events follows the order
/// of each outbox's first publish in the op — deterministically, in both
/// directions.
#[tokio::test]
#[file_serial]
async fn ordering_follows_first_publish_order() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let source = init_outbox::<SourceEvent>(&pool, default_config()).await?;
    let dest = Outbox::<DestEvent, TestTables>::init(&pool, default_config()).await?;

    source.add_post_persist_hook(RepostHook { dest: dest.clone() });

    // Dest-native publish first → its commit hook registers (and runs) first.
    let mut op = source.begin_op().await?;
    dest.publish_persisted_in_op(&mut op, DestEvent::Native(1))
        .await?;
    source
        .publish_persisted_in_op(&mut op, SourceEvent::Source(2))
        .await?;
    op.commit().await?;

    assert_eq!(
        payloads_by_sequence(&pool).await?,
        [
            serde_json::json!({"Native": 1}),
            serde_json::json!({"Source": 2}),
            serde_json::json!({"Mapped": 2}),
        ],
        "dest-native published first ⇒ dest-native sequences < reposted"
    );

    // Inverted: source publish first → reposted events precede dest-native.
    let mut op = source.begin_op().await?;
    source
        .publish_persisted_in_op(&mut op, SourceEvent::Source(4))
        .await?;
    dest.publish_persisted_in_op(&mut op, DestEvent::Native(5))
        .await?;
    op.commit().await?;

    assert_eq!(
        payloads_by_sequence(&pool).await?[3..],
        [
            serde_json::json!({"Source": 4}),
            serde_json::json!({"Mapped": 4}),
            serde_json::json!({"Native": 5}),
        ],
        "source published first ⇒ reposted sequences < dest-native"
    );
    Ok(())
}

/// The immediate-insert path drops the destination's in-process broadcast
/// hook, so delivery to destination listeners is healed by pg NOTIFY (fires
/// at commit) and the poller/gap-fill — reposted events must still arrive.
#[tokio::test]
#[file_serial]
async fn reposted_events_reach_destination_listener() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let source = init_outbox::<SourceEvent>(&pool, default_config()).await?;
    let dest = Outbox::<DestEvent, TestTables>::init(&pool, default_config()).await?;

    source.add_post_persist_hook(RepostHook { dest: dest.clone() });

    let mut listener = dest.listen_persisted(None);

    let mut op = source.begin_op().await?;
    source
        .publish_persisted_in_op(&mut op, SourceEvent::Source(9))
        .await?;
    op.commit().await?;

    let deadline = std::time::Duration::from_secs(5);
    let received = tokio::time::timeout(deadline, async {
        while let Some(event) = listener.next().await {
            if event.payload == Some(DestEvent::Mapped(9)) {
                return true;
            }
        }
        false
    })
    .await
    .unwrap_or(false);
    assert!(
        received,
        "reposted event must reach a destination listener via NOTIFY/gap-fill \
         despite the dropped in-process broadcast"
    );
    Ok(())
}
