use std::{marker::PhantomData, sync::Arc};

use es_entity::hooks::{CommitHook, HookOperation, PreCommitRet};
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::broadcast;

use crate::out::event::PersistentOutboxEvent;
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
    ) -> Self {
        Self {
            sender,
            pre_commit_events: events.into_iter().map(Into::into).collect(),
            post_commit_events: Vec::new(),
            batch_size,
            _phantom: PhantomData,
        }
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
            persisted.extend(Tables::persist_events(&mut op, chunk.into_iter()).await?);
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
