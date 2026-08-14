// Pre-existing test-only lints surfaced by a Rust stable toolchain bump
// (clippy 1.93 tightened `await_holding_lock` / `type_complexity`). These
// recording hooks intentionally hold a `std::sync::Mutex` guard across an
// `await` in assertion sections; the suppression is unrelated to the
// partitioning work and keeps `nix flake check` green.
#![allow(clippy::await_holding_lock)]
#![allow(clippy::type_complexity)]

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
/// same transaction. `dest`'s own commit hook joins the tail of the *same*
/// commit pass (es-entity's re-entrant hook registration) rather than
/// inserting inline here: its persist and its own post-persist hooks run
/// later, when the enclosing `execute_pre` loop reaches it — still before
/// the single `COMMIT`, and still ordered ahead of anything registered
/// after `source`'s hook.
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
    let payload_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM persistent_outbox_events WHERE payload IS NOT NULL",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        payload_rows, 0,
        "the tx rolled back — no event payload survives the hook error"
    );

    // The burned sequence is compensated reactively: a NULL-payload
    // placeholder appears without waiting for the gap-fill backstop.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let placeholders: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM persistent_outbox_events WHERE payload IS NULL",
        )
        .fetch_one(&pool)
        .await?;
        if placeholders == 1 {
            break;
        }
        if std::time::Instant::now() > deadline {
            anyhow::bail!("compensation placeholder for the aborted sequence never appeared");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

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

/// A hook publishing into outbox B runs B's own post-persist hooks — B's
/// repost into C joins the tail of the same pass in turn, so a linear chain
/// (A→B→C) still lands its hops in causal insertion order even though the
/// enclosing `execute_pre` loop processes them breadth-first (by
/// generation), not by recursing inline through each `on_persisted` call.
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

/// The `on_rollback` tier reaches a repost that already joined the pass.
/// `hop` — three hops deep — vetoes from its own post-persist hook, *after*
/// `hop`'s own persist has already allocated its sequence: the whole
/// transaction rolls back, including `source`'s and `dest`'s already-
/// succeeded hooks earlier in the pass.
///
/// Before es-entity's re-entrant commit-hook registration
/// (GaloyMoney/es-entity#200), `dest`'s repost always force-executed — a
/// repost's `HookOperation` could never register further hooks, so
/// `dest`'s hook never entered the `CommitHooks` bookkeeping at all.
/// Neither `post_commit` nor `on_rollback` ever ran for it, and its burned
/// sequence depended entirely on the slow, grace-gated backstop (2s
/// default). Now that the repost joins the same commit pass as `source`,
/// `dest`'s `PersistEvents` hook is tracked exactly like a directly-
/// registered hook: its `on_rollback` fires once the deeper failure
/// surfaces, and `dest`'s abandoned sequence is placeholder-filled
/// reactively — in milliseconds, not seconds.
#[tokio::test]
#[file_serial]
async fn repost_on_rollback_reaches_destination_reactively() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let source = init_outbox::<SourceEvent>(&pool, default_config()).await?;
    let dest = Outbox::<DestEvent, TestTables>::init(&pool, default_config()).await?;
    let hop = Outbox::<HopEvent, TestTables>::init(&pool, default_config()).await?;

    source.add_post_persist_hook(RepostHook { dest: dest.clone() });
    dest.add_post_persist_hook(HopHook { next: hop.clone() });

    /// Vetoes after `hop`'s own persist already ran — the failure that must
    /// unwind the whole chain, including the hops that already succeeded
    /// earlier in the same pass.
    struct VetoHopHook;

    impl PostPersistHook<HopEvent> for VetoHopHook {
        fn on_persisted<'a>(
            &'a self,
            _op: &'a mut HookOperation<'_>,
            _events: &'a [PersistentOutboxEvent<HopEvent>],
        ) -> BoxFuture<'a, Result<(), sqlx::Error>> {
            Box::pin(async { Err(sqlx::Error::Protocol("hop veto".into())) })
        }
    }
    hop.add_post_persist_hook(VetoHopHook);

    let mut dest_listener = dest.listen_persisted(None);

    let mut op = source.begin_op().await?;
    source
        .publish_persisted_in_op(&mut op, SourceEvent::Source(11))
        .await?;
    assert!(
        op.commit().await.is_err(),
        "hop's veto, three hops deep, must fail the whole commit"
    );

    // Reactive compensation must reach `dest` specifically: its
    // `PersistEvents` hook's `pre_commit` had already succeeded (staged
    // earlier in the same pass, its own persist done) by the time hop's
    // veto surfaced deeper in the queue — so es-entity fires its
    // `on_rollback`, not its own-failure branch. Bound well under the 2s
    // gap-fill grace so a regression back to the pre-#200 force-execute
    // path (no `on_rollback` at all) fails this test rather than just
    // running slow.
    let gap_event =
        tokio::time::timeout(std::time::Duration::from_millis(1500), dest_listener.next())
            .await
            .map_err(|_| {
                anyhow::anyhow!("dest's compensation placeholder did not arrive reactively")
            })?
            .expect("stream open")?;
    assert!(
        gap_event.payload.is_none(),
        "dest's abandoned sequence must resolve as a placeholder, not a real Mapped event"
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

/// Commit hooks run in registration order (es-entity ≥ 0.11.7) — but a
/// repost's destination hook shares its `TypeId` with any hook the
/// destination's own direct publishes already registered on the same op,
/// so es-entity's ordinary same-op merge rule applies: the merge keeps the
/// **earliest still-pending** registration's queue position and appends
/// later contributions to its event list in the order they fold in. When
/// `dest` published nothing of its own before the repost lands, the
/// repost's hook is fresh — it runs at its own (later) position, after
/// `source`. But when `dest` already published directly on the same op
/// *before* `source`'s hook ran, the repost merges INTO that still-pending
/// hook (es-entity's re-entrant registration folds into the tail of the
/// pass — see [`PostPersistHook`]'s contract docs) — so the merged hook
/// still runs at `dest`'s earlier position, and physically inserts `dest`'s
/// own events before the freshly-appended reposted ones, regardless of
/// which outbox's `publish_*_in_op` call came first in program order.
#[tokio::test]
#[file_serial]
async fn ordering_follows_first_publish_order() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let source = init_outbox::<SourceEvent>(&pool, default_config()).await?;
    let dest = Outbox::<DestEvent, TestTables>::init(&pool, default_config()).await?;

    source.add_post_persist_hook(RepostHook { dest: dest.clone() });

    // Dest-native publish first → dest's hook is already-executed by the
    // time the repost is staged, so the repost gets a fresh (later)
    // instance instead of merging — its position follows program order.
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

    // Inverted: source publish first → `dest`'s direct publish is still
    // PENDING (not yet executed) when `source`'s hook stages the repost, so
    // the repost merges into it and inherits dest's earlier queue position.
    // The merged batch still inserts dest's own event before the appended
    // repost — Native precedes Mapped despite `source` publishing first.
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
            serde_json::json!({"Native": 5}),
            serde_json::json!({"Mapped": 4}),
        ],
        "source published first, but dest's still-pending direct publish \
         absorbs the repost at dest's own queue position"
    );
    Ok(())
}

/// A repost's destination hook now joins the same commit pass as the
/// source, so its own `post_commit` runs — feeding the in-process broadcast
/// directly, not just healing via pg NOTIFY and the poller/gap-fill.
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

    // `dest`'s repost joins the same commit pass as `source` (the op
    // supports commit hooks), so `dest`'s own `PersistEvents::post_commit`
    // now runs — feeding its in-process broadcast directly, not just the
    // debounced NOTIFY fallback. A tight bound (well under the crate's 2s
    // gap-fill grace) is a meaningful regression signal: before es-entity's
    // re-entrant hook registration, `dest`'s hook always force-executed and
    // never fed the broadcast at all.
    let deadline = std::time::Duration::from_secs(2);
    let received = tokio::time::timeout(deadline, async {
        while let Some(Ok(event)) = listener.next().await {
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
        "reposted event must reach a destination listener via in-process forwarding"
    );
    Ok(())
}
