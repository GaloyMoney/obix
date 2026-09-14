mod helpers;

use es_entity::{
    AtomicOperation,
    hooks::{CommitHook, HookOperation, PreCommitRet},
};
use obix::{EventSequence, MailboxConfig, out::Outbox};
use serde_json::{Value, json};
use serial_test::file_serial;
use std::{sync::Arc, time::Duration};
use tokio::sync::{Notify, Semaphore};

use helpers::TestTables;
type TestOutbox = Outbox<Value, TestTables>;

async fn init() -> anyhow::Result<(sqlx::PgPool, TestOutbox)> {
    let pool = helpers::init_pool().await?;
    helpers::wipeout_outbox_tables(&pool).await?;
    let outbox = TestOutbox::init(
        &pool,
        MailboxConfig::builder()
            .persist_events_batch_size(1)
            .build()?,
    )
    .await?;
    Ok((pool, outbox))
}

struct PublishLater {
    outbox: TestOutbox,
    again: bool,
}
impl CommitHook for PublishLater {
    async fn pre_commit(
        self,
        mut op: HookOperation<'_>,
    ) -> Result<PreCommitRet<'_, Self>, sqlx::Error> {
        if self.again {
            assert!(
                op.add_commit_hook(PublishLater {
                    outbox: self.outbox.clone(),
                    again: false
                })
                .is_ok()
            );
        } else {
            self.outbox
                .publish_persisted_in_op(&mut op, json!("late"))
                .await?;
        }
        PreCommitRet::ok(self, op)
    }
}

#[tokio::test]
#[file_serial]
async fn seals_after_reentrant_generations_and_released_savepoints() -> anyhow::Result<()> {
    let (_, outbox) = init().await?;
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, json!("first"))
        .await?;
    {
        let mut sp = op.begin_savepoint().await?;
        outbox
            .publish_persisted_in_op(&mut sp, json!("discarded"))
            .await?;
        sp.rollback().await?;
    }
    {
        let mut sp = op.begin_savepoint().await?;
        outbox
            .publish_persisted_in_op(&mut sp, json!("released"))
            .await?;
        sp.release().await?;
    }
    assert!(
        op.add_commit_hook(PublishLater {
            outbox: outbox.clone(),
            again: true
        })
        .is_ok()
    );
    op.commit().await?;
    let batches = outbox
        .load_publication_batches(EventSequence::BEGIN, 1)
        .await?;
    assert_eq!(
        batches.len(),
        1,
        "one publication despite chunking and multiple normal-hook generations"
    );
    assert_eq!(
        batches[0]
            .events()
            .map(|e| e.payload.clone().unwrap_or(Value::Null))
            .collect::<Vec<_>>(),
        vec![json!("first"), json!("released"), json!("late")]
    );
    assert_eq!(batches[0].first_sequence(), EventSequence::from(1));
    assert_eq!(batches[0].last_sequence(), EventSequence::from(3));
    assert!(
        outbox
            .load_publication_batches(EventSequence::from(1), 1)
            .await
            .is_err(),
        "a cursor inside a publication must be rejected"
    );
    assert!(
        outbox
            .load_publication_batches(EventSequence::from(3), 1)
            .await?
            .is_empty()
    );
    Ok(())
}

// Registered by an ordinary hook AFTER PersistEvents stages its seal. This
// finalizer therefore runs after the boundary insert, while its SQL is uncommitted.
struct GateFinalizer {
    reached: Arc<Notify>,
    release: Arc<Semaphore>,
    abort: bool,
}
impl CommitHook for GateFinalizer {
    fn is_finalizer(&self) -> bool {
        true
    }
    async fn pre_commit(
        self,
        op: HookOperation<'_>,
    ) -> Result<PreCommitRet<'_, Self>, sqlx::Error> {
        self.reached.notify_one();
        self.release
            .acquire()
            .await
            .expect("test gate stays open")
            .forget();
        if self.abort {
            return Err(sqlx::Error::Protocol("abort after sealing".into()));
        }
        PreCommitRet::ok(self, op)
    }
}
struct AddFinalizer {
    finalizer: Option<GateFinalizer>,
}
impl CommitHook for AddFinalizer {
    async fn pre_commit(
        mut self,
        mut op: HookOperation<'_>,
    ) -> Result<PreCommitRet<'_, Self>, sqlx::Error> {
        assert!(op.add_commit_hook(self.finalizer.take().unwrap()).is_ok());
        PreCommitRet::ok(self, op)
    }
}

#[tokio::test]
#[file_serial]
async fn concurrent_appends_are_commit_ordered_and_rollback_does_not_burn_positions()
-> anyhow::Result<()> {
    for abort in [false, true] {
        let (_, outbox) = init().await?;
        let reached = Arc::new(Notify::new());
        let release = Arc::new(Semaphore::new(0));
        let mut a = outbox.begin_op().await?;
        outbox.publish_persisted_in_op(&mut a, json!("a")).await?;
        assert!(
            a.add_commit_hook(AddFinalizer {
                finalizer: Some(GateFinalizer {
                    reached: reached.clone(),
                    release: release.clone(),
                    abort
                })
            })
            .is_ok()
        );
        let a = tokio::spawn(async move { a.commit().await });
        tokio::time::timeout(Duration::from_secs(5), reached.notified()).await?;
        assert_eq!(
            outbox.highest_known_persistent_sequence().await?,
            EventSequence::BEGIN
        );
        assert!(
            outbox
                .load_publication_batches(EventSequence::BEGIN, 10)
                .await?
                .is_empty()
        );

        let mut b = outbox.begin_op().await?;
        outbox.publish_persisted_in_op(&mut b, json!("b")).await?;
        let mut b = tokio::spawn(async move { b.commit().await });
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut b)
                .await
                .is_err(),
            "the second append must wait for the uncommitted head reservation"
        );
        release.add_permits(1);
        assert_eq!(a.await?.is_err(), abort);
        tokio::time::timeout(Duration::from_secs(5), b).await???;
        let batches = outbox
            .load_publication_batches(EventSequence::BEGIN, 10)
            .await?;
        let expected = if abort {
            vec![json!("b")]
        } else {
            vec![json!("a"), json!("b")]
        };
        assert_eq!(batches.len(), expected.len());
        for (index, (batch, payload)) in batches.iter().zip(expected).enumerate() {
            assert_eq!(
                batch.first_sequence(),
                EventSequence::from(index as u64 + 1)
            );
            assert_eq!(
                batch.events().next().unwrap().payload.as_ref(),
                Some(&payload)
            );
        }
    }
    Ok(())
}

struct IllegalFinalizer {
    outbox: TestOutbox,
}
impl CommitHook for IllegalFinalizer {
    fn is_finalizer(&self) -> bool {
        true
    }
    async fn pre_commit(
        self,
        mut op: HookOperation<'_>,
    ) -> Result<PreCommitRet<'_, Self>, sqlx::Error> {
        self.outbox
            .publish_persisted_in_op(&mut op, json!("too late"))
            .await?;
        PreCommitRet::ok(self, op)
    }
}

#[tokio::test]
#[file_serial]
async fn late_publication_from_a_finalizer_rolls_back_instead_of_splitting() -> anyhow::Result<()> {
    let (pool, outbox) = init().await?;
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, json!("first"))
        .await?;
    assert!(
        op.add_commit_hook(IllegalFinalizer {
            outbox: outbox.clone()
        })
        .is_ok()
    );
    assert!(op.commit().await.is_err());
    assert_eq!(
        outbox.highest_known_persistent_sequence().await?,
        EventSequence::BEGIN
    );
    assert!(
        outbox
            .load_publication_batches(EventSequence::BEGIN, 10)
            .await?
            .is_empty()
    );
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM persistent_outbox_events")
        .fetch_one(&pool)
        .await?;
    assert_eq!(rows, 0);
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn bare_transactions_are_rejected_and_empty_operations_do_not_publish() -> anyhow::Result<()>
{
    let (pool, outbox) = init().await?;
    let mut bare = pool.begin().await?;
    assert!(
        outbox
            .publish_persisted_in_op(&mut bare, json!("unsupported"))
            .await
            .is_err()
    );
    bare.commit().await?;
    let mut empty = outbox.begin_op().await?;
    outbox
        .publish_all_persisted(&mut empty, Vec::<Value>::new())
        .await?;
    empty.commit().await?;
    assert_eq!(
        outbox.highest_known_persistent_sequence().await?,
        EventSequence::BEGIN
    );
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM persistent_outbox_events")
        .fetch_one(&pool)
        .await?;
    assert_eq!(rows, 0);
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn json_null_is_a_message_and_a_missing_member_fails_the_publication() -> anyhow::Result<()> {
    let (pool, outbox) = init().await?;
    let mut op = outbox.begin_op().await?;
    outbox
        .publish_all_persisted(&mut op, [Value::Null, json!("second")])
        .await?;
    op.commit().await?;
    let batches = outbox
        .load_publication_batches(EventSequence::BEGIN, 1)
        .await?;
    let events: Vec<_> = batches[0].events().collect();
    assert_eq!(events.len(), 2);
    // A JSON-null message is a message: it round-trips as JSON null, never
    // as an SQL-NULL placeholder.
    assert_eq!(events[0].payload, Some(Value::Null));
    assert_eq!(events[1].payload, Some(json!("second")));

    // Deleting a member inside a sealed range must fail the whole load, not
    // silently deliver a partial publication.
    sqlx::query("DELETE FROM persistent_outbox_events WHERE sequence = 2")
        .execute(&pool)
        .await?;
    assert!(matches!(
        outbox
            .load_publication_batches(EventSequence::BEGIN, 1)
            .await,
        Err(sqlx::Error::Protocol(_))
    ));
    sqlx::query(
        "INSERT INTO persistent_outbox_events (sequence, payload) VALUES (2, '\"second\"'::jsonb)",
    )
    .execute(&pool)
    .await?;
    assert_eq!(
        outbox
            .load_publication_batches(EventSequence::BEGIN, 1)
            .await?[0]
            .events()
            .count(),
        2
    );
    Ok(())
}
