use std::{marker::PhantomData, sync::Arc};

use es_entity::hooks::{CommitHook, HookOperation, PreCommitRet};
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::broadcast;

use crate::out::event::PersistentOutboxEvent;
use crate::out::post_persist_hook::PostPersistHooks;
use crate::tables::MailboxTables;

pub struct PersistEvents<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    sender: broadcast::Sender<Arc<PersistentOutboxEvent<P>>>,
    pre_commit_events: Vec<P>,
    post_commit_events: Vec<PersistentOutboxEvent<P>>,
    batch_size: usize,
    /// Snapshot of the outbox's registered post-persist hooks, taken when
    /// this commit hook is constructed (i.e. at the operation's first
    /// publish). Merged publishes keep the first snapshot.
    post_persist_hooks: PostPersistHooks<P>,
    _phantom: PhantomData<Tables>,
}

impl<P, Tables> PersistEvents<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    pub fn new(
        sender: broadcast::Sender<Arc<PersistentOutboxEvent<P>>>,
        events: impl IntoIterator<Item = impl Into<P>>,
        batch_size: usize,
        post_persist_hooks: PostPersistHooks<P>,
    ) -> Self {
        Self {
            sender,
            pre_commit_events: events.into_iter().map(Into::into).collect(),
            post_commit_events: Vec::new(),
            batch_size,
            post_persist_hooks,
            _phantom: PhantomData,
        }
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
            let persisted_chunk = Tables::persist_events(&mut op, chunk.into_iter()).await?;
            for hook in self.post_persist_hooks.iter() {
                hook.on_persisted(&mut op, &persisted_chunk).await?;
            }
            persisted.extend(persisted_chunk);
        }
        self.post_commit_events = persisted;
        PreCommitRet::ok(self, op)
    }

    fn post_commit(self) {
        for event in self.post_commit_events {
            let _ = self.sender.send(event.into());
        }
    }

    fn merge(&mut self, other: &mut Self) -> bool {
        self.pre_commit_events.append(&mut other.pre_commit_events);
        true
    }
}
