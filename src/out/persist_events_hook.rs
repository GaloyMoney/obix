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
    /// Reports the committed batch's `(min, max)` sequences to the
    /// per-process debounced notifier from `post_commit`.
    notifier_tx: mpsc::UnboundedSender<(EventSequence, EventSequence)>,
    pre_commit_events: Vec<P>,
    post_commit_events: Vec<PersistentOutboxEvent<P>>,
    batch_size: usize,
    /// Snapshot of the outbox's registered post-persist hooks, taken when
    /// this commit hook is constructed (i.e. at the operation's first
    /// publish). Merged publishes keep the first snapshot.
    post_persist_hooks: PostPersistHooks<P>,
    /// Set for publishes onto operations without commit-hook support (a bare
    /// `sqlx::Transaction`, via `force_execute_pre_commit`): `post_commit`
    /// never runs there, so the debounced notifier cannot observe the commit
    /// — the persist statement itself must carry the in-tx NOTIFY.
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
        events: impl IntoIterator<Item = impl Into<P>>,
        batch_size: usize,
        post_persist_hooks: PostPersistHooks<P>,
    ) -> Self {
        Self {
            sender,
            notifier_tx,
            pre_commit_events: events.into_iter().map(Into::into).collect(),
            post_commit_events: Vec::new(),
            batch_size,
            post_persist_hooks,
            notify_in_tx: false,
            _phantom: PhantomData,
        }
    }

    /// Switch the persist statement to the in-transaction NOTIFY variant.
    /// Only for the `force_execute_pre_commit` path — see
    /// [`Self::notify_in_tx`].
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
        let mut persisted = Vec::with_capacity(events.len());
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
            for hook in self.post_persist_hooks.iter() {
                hook.on_persisted(&mut op, &persisted_chunk).await?;
            }
            persisted.extend(persisted_chunk);
        }
        self.post_commit_events = persisted;
        PreCommitRet::ok(self, op)
    }

    fn post_commit(self) {
        let Self {
            sender,
            notifier_tx,
            post_commit_events,
            ..
        } = self;
        // The persist query orders by sequence and chunks append in order,
        // so first/last bound the whole committed batch.
        let batch_range = match (post_commit_events.first(), post_commit_events.last()) {
            (Some(first), Some(last)) => Some((first.sequence, last.sequence)),
            _ => None,
        };
        for event in post_commit_events {
            let _ = sender.send(PersistentDelivery::from(Ok(event)));
        }
        if let Some(range) = batch_range {
            let _ = notifier_tx.send(range);
        }
    }

    fn merge(&mut self, other: &mut Self) -> bool {
        self.pre_commit_events.append(&mut other.pre_commit_events);
        true
    }
}
