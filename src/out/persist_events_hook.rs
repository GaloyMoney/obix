use std::marker::PhantomData;

use es_entity::hooks::{CommitHook, HookOperation, PreCommitRet};
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{broadcast, mpsc};

use crate::out::event::{PersistentDelivery, PersistentOutboxEvent};
use crate::out::post_persist_hook::PostPersistHooks;
use crate::sequence::EventSequence;
use crate::tables::MailboxTables;

pub struct PersistEvents<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    sender: broadcast::Sender<PersistentDelivery<P>>,
    /// Reports the committed batch's `(min, max)` to the debounced notifier.
    notifier_tx: mpsc::UnboundedSender<(EventSequence, EventSequence)>,
    /// Reports sequences this operation allocated but failed to commit to
    /// the [`AbandonedCompensator`](crate::out::compensator): see
    /// [`Drop`](Self::drop).
    abandoned_tx: mpsc::UnboundedSender<Vec<EventSequence>>,
    pre_commit_events: Vec<P>,
    /// Persisted events, stashed chunk-by-chunk *as* `pre_commit` runs (not
    /// at its end): if a later chunk, a later hook, or the COMMIT itself
    /// fails, this holds exactly the sequences the rolled-back transaction
    /// allocated — which is what `Drop` compensates. Consumed (emptied) by
    /// `post_commit` on the success path.
    post_commit_events: Vec<PersistentOutboxEvent<P>>,
    batch_size: usize,
    /// Snapshot of the outbox's registered post-persist hooks, taken when
    /// this commit hook is constructed (i.e. at the operation's first
    /// publish). Merged publishes keep the first snapshot.
    post_persist_hooks: PostPersistHooks<P>,
    /// Set on the force-execute path (no commit hooks): `post_commit` never
    /// runs, so the persist statement must carry the in-tx NOTIFY. Also
    /// defuses `Drop`'s rollback compensation — on that path the hook is
    /// dropped while the caller's transaction is still open and its fate
    /// unknowable, so those sequences are the backstop's responsibility.
    notify_in_tx: bool,
    _phantom: PhantomData<Tables>,
}

impl<P, Tables> PersistEvents<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    pub fn new(
        sender: broadcast::Sender<PersistentDelivery<P>>,
        notifier_tx: mpsc::UnboundedSender<(EventSequence, EventSequence)>,
        abandoned_tx: mpsc::UnboundedSender<Vec<EventSequence>>,
        events: impl IntoIterator<Item = impl Into<P>>,
        batch_size: usize,
        post_persist_hooks: PostPersistHooks<P>,
    ) -> Self {
        Self {
            sender,
            notifier_tx,
            abandoned_tx,
            pre_commit_events: events.into_iter().map(Into::into).collect(),
            post_commit_events: Vec::new(),
            batch_size,
            post_persist_hooks,
            notify_in_tx: false,
            _phantom: PhantomData,
        }
    }

    /// Use the in-transaction NOTIFY persist variant (force-execute path).
    pub(crate) fn with_in_tx_notify(mut self) -> Self {
        self.notify_in_tx = true;
        self
    }

    /// Events buffered on this hook, awaiting persistence at commit.
    /// Backs the [`Outbox::cursor`](crate::out::Outbox::cursor) read API.
    pub(crate) fn pending(&self) -> &[P] {
        &self.pre_commit_events
    }
}

impl<P, Tables> CommitHook for PersistEvents<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    async fn pre_commit(
        mut self,
        mut op: HookOperation<'_>,
    ) -> Result<PreCommitRet<'_, Self>, sqlx::Error> {
        let batch_size = self.batch_size.max(1);
        let events = std::mem::take(&mut self.pre_commit_events);
        self.post_commit_events.reserve(events.len());
        let mut events = events.into_iter();
        loop {
            let chunk: Vec<P> = events.by_ref().take(batch_size).collect();
            if chunk.is_empty() {
                break;
            }
            let persisted_chunk = if self.notify_in_tx {
                Tables::persist_events_notifying(&mut op, chunk.into_iter()).await?
            } else {
                Tables::persist_events(&mut op, chunk.into_iter()).await?
            };
            // Stash before running the post-persist hooks: a hook error
            // rolls the transaction back with these sequences already
            // allocated, and `Drop` must know about them to compensate.
            let chunk_start = self.post_commit_events.len();
            self.post_commit_events.extend(persisted_chunk);
            for hook in self.post_persist_hooks.iter() {
                hook.on_persisted(&mut op, &self.post_commit_events[chunk_start..])
                    .await?;
            }
        }
        PreCommitRet::ok(self, op)
    }

    fn post_commit(mut self) {
        // Taking the events doubles as the `Drop` defuse: an empty
        // `post_commit_events` at drop time means either nothing was
        // persisted or the commit succeeded and delivery happened here.
        let post_commit_events = std::mem::take(&mut self.post_commit_events);
        let batch_range = match (post_commit_events.first(), post_commit_events.last()) {
            (Some(first), Some(last)) => Some((first.sequence, last.sequence)),
            _ => None,
        };
        for event in post_commit_events {
            let _ = self.sender.send(PersistentDelivery::from(Ok(event)));
        }
        if let Some(range) = batch_range {
            let _ = self.notifier_tx.send(range);
        }
    }

    fn merge(&mut self, other: &mut Self) -> bool {
        self.pre_commit_events.append(&mut other.pre_commit_events);
        true
    }
}

/// Rollback compensation (the reactive tier of gap filling).
///
/// es-entity's commit path consumes the hook on success — `pre_commit`
/// returns it into the machinery and `post_commit` empties
/// `post_commit_events`. So reaching `Drop` while `post_commit_events` is
/// populated means exactly one thing: the persist INSERT ran (sequences
/// were allocated) and the transaction did not commit — a later chunk or
/// hook errored, the COMMIT failed, or the operation was dropped. Those
/// sequences are this process's own abandoned allocations; report them to
/// the [`AbandonedCompensator`](crate::out::compensator) for an immediate
/// placeholder fill instead of leaving downstream listeners stalled until
/// the grace-gated backstop proves them lost.
///
/// Deliberately inert on the force-execute path (`notify_in_tx`): there the
/// hook is dropped while the caller's bare transaction is still open, so
/// "dropped without post_commit" implies nothing about the outcome.
impl<P, Tables> Drop for PersistEvents<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    fn drop(&mut self) {
        if self.notify_in_tx || self.post_commit_events.is_empty() {
            return;
        }
        let abandoned = self
            .post_commit_events
            .drain(..)
            .map(|event| event.sequence)
            .collect();
        let _ = self.abandoned_tx.send(abandoned);
    }
}
