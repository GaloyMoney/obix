mod helpers;

use obix::{MailboxConfig, out::Outbox};
use serde::{Deserialize, Serialize};
use serial_test::file_serial;

use helpers::{TestTables, init_outbox, init_pool};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum SourceEvent {
    Posted(u64),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum DestEvent {
    Mapped(u64),
}

async fn outbox_row_count(pool: &sqlx::PgPool) -> anyhow::Result<i64> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM persistent_outbox_events")
        .fetch_one(pool)
        .await?;
    Ok(count)
}

#[tokio::test]
#[file_serial]
async fn cursor_sees_only_events_published_after_creation() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = init_outbox::<SourceEvent>(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let mut op = outbox.begin_op().await?;

    // Published before the cursor — must not be visible through it.
    outbox
        .publish_persisted_in_op(&mut op, SourceEvent::Posted(0))
        .await?;

    let mut cursor = outbox.cursor(&op)?;

    outbox
        .publish_all_persisted(&mut op, [SourceEvent::Posted(1), SourceEvent::Posted(2)])
        .await?;

    let new = outbox.take_published_since(&op, &mut cursor);
    assert_eq!(new, [SourceEvent::Posted(1), SourceEvent::Posted(2)]);

    // The read advanced the cursor — nothing new now.
    assert!(outbox.take_published_since(&op, &mut cursor).is_empty());

    // Stream-style continuation: a later publish is picked up by the same cursor.
    outbox
        .publish_persisted_in_op(&mut op, SourceEvent::Posted(3))
        .await?;
    assert_eq!(
        outbox.take_published_since(&op, &mut cursor),
        [SourceEvent::Posted(3)]
    );

    op.commit().await?;
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn peek_does_not_advance() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = init_outbox::<SourceEvent>(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let mut op = outbox.begin_op().await?;
    let mut cursor = outbox.cursor(&op)?;

    outbox
        .publish_all_persisted(&mut op, [SourceEvent::Posted(1), SourceEvent::Posted(2)])
        .await?;

    assert_eq!(outbox.peek_published_since(&op, &cursor).len(), 2);
    // Peeking again yields the same events — no advance.
    assert_eq!(outbox.peek_published_since(&op, &cursor).len(), 2);

    // A consuming read advances past them.
    assert_eq!(outbox.take_published_since(&op, &mut cursor).len(), 2);
    assert!(outbox.peek_published_since(&op, &cursor).is_empty());

    op.commit().await?;
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn cursor_is_scoped_to_its_outbox() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let source = init_outbox::<SourceEvent>(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;
    // Second outbox with a different payload type: its commit hook is a
    // distinct type, so cursors must not observe the source outbox's buffer.
    let dest = Outbox::<DestEvent, TestTables>::init(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let mut op = source.begin_op().await?;
    let mut dest_cursor = dest.cursor(&op)?;

    source
        .publish_persisted_in_op(&mut op, SourceEvent::Posted(1))
        .await?;

    assert!(
        dest.take_published_since(&op, &mut dest_cursor).is_empty(),
        "a cursor must not see another outbox's events"
    );

    dest.publish_persisted_in_op(&mut op, DestEvent::Mapped(1))
        .await?;
    assert_eq!(
        dest.take_published_since(&op, &mut dest_cursor),
        [DestEvent::Mapped(1)]
    );

    op.commit().await?;
    Ok(())
}

/// The motivating use case: republish a mapped projection of one outbox's
/// events onto another outbox — atomically, in the same transaction.
#[tokio::test]
#[file_serial]
async fn mapped_republish_commits_atomically() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let source = init_outbox::<SourceEvent>(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;
    let dest = Outbox::<DestEvent, TestTables>::init(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let mut op = source.begin_op().await?;
    let mut cursor = source.cursor(&op)?;

    // Stand-in for a downstream call (e.g. a ledger posting) that publishes
    // onto the shared op.
    source
        .publish_all_persisted(
            &mut op,
            [
                SourceEvent::Posted(1),
                SourceEvent::Posted(2),
                SourceEvent::Posted(3),
            ],
        )
        .await?;

    // Inline, per-use-case mapping at the call site.
    let mapped = source.map_published_since(&op, &mut cursor, |e| match e {
        SourceEvent::Posted(n) if *n % 2 == 1 => Some(DestEvent::Mapped(*n)),
        _ => None,
    });
    assert_eq!(mapped, [DestEvent::Mapped(1), DestEvent::Mapped(3)]);

    dest.publish_all_persisted(&mut op, mapped).await?;
    op.commit().await?;

    // 3 source events + 2 mapped events, all in one transaction.
    assert_eq!(outbox_row_count(&pool).await?, 5);
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn mapped_republish_rolls_back_atomically() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let source = init_outbox::<SourceEvent>(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;
    let dest = Outbox::<DestEvent, TestTables>::init(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    {
        let mut op = source.begin_op().await?;
        let mut cursor = source.cursor(&op)?;

        source
            .publish_all_persisted(&mut op, [SourceEvent::Posted(1), SourceEvent::Posted(2)])
            .await?;

        let mapped = source.map_published_since(&op, &mut cursor, |e| {
            let SourceEvent::Posted(n) = e;
            Some(DestEvent::Mapped(*n))
        });
        dest.publish_all_persisted(&mut op, mapped).await?;

        // Dropped without commit — everything rolls back together.
    }

    assert_eq!(
        outbox_row_count(&pool).await?,
        0,
        "neither source nor mapped events may survive a rollback"
    );
    Ok(())
}

/// Documented caveat: an op without commit-hook support (a bare
/// `sqlx::Transaction`) has no op-local buffer, so `cursor()` fails loudly
/// instead of handing back a cursor that would silently yield nothing.
#[tokio::test]
#[file_serial]
async fn cursor_errors_on_op_without_hook_support() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let outbox = init_outbox::<SourceEvent>(
        &pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?;

    let mut tx = pool.begin().await?;
    assert!(
        matches!(
            outbox.cursor(&tx),
            Err(obix::out::CursorError::HooksUnsupported)
        ),
        "a bare transaction supports no commit hooks — cursor() must fail loudly"
    );

    // Publishing still works on a bare tx (written immediately), independent
    // of cursors.
    outbox
        .publish_persisted_in_op(&mut tx, SourceEvent::Posted(1))
        .await?;
    tx.commit().await?;
    assert_eq!(outbox_row_count(&pool).await?, 1);
    Ok(())
}
