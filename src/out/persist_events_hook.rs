use std::marker::PhantomData;

use es_entity::hooks::{CommitHook, HookOperation, PreCommitRet};
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::broadcast;

use crate::out::event::{OutboxEvent, PersistentOutboxEvent};
use crate::tables::MailboxTables;

pub struct PersistEvents<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    sender: broadcast::Sender<OutboxEvent<P>>,
    pre_commit_events: Vec<P>,
    post_commit_events: Vec<PersistentOutboxEvent<P>>,
    _phantom: PhantomData<Tables>,
}

impl<P, Tables> PersistEvents<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    pub fn new(
        sender: broadcast::Sender<OutboxEvent<P>>,
        events: impl IntoIterator<Item = impl Into<P>>,
    ) -> Self {
        Self {
            sender,
            pre_commit_events: events.into_iter().map(Into::into).collect(),
            post_commit_events: Vec::new(),
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
        let persisted = Tables::persist_events(&mut op, self.pre_commit_events.drain(..)).await?;
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
