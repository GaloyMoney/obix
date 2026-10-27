use std::{marker::PhantomData, sync::Arc};

use es_entity::hooks::{CommitHook, HookOperation, PreCommitRet};
use tokio::sync::broadcast;

use super::event::{EphemeralEventType, EphemeralOutboxEvent, RawPayload};
use crate::tables::MailboxTables;

pub struct PersistEphemeralEvents<Tables>
where
    Tables: MailboxTables,
{
    sender: broadcast::Sender<Arc<EphemeralOutboxEvent<RawPayload>>>,
    pre_commit_events: Vec<(EphemeralEventType, RawPayload)>,
    post_commit_events: Vec<EphemeralOutboxEvent<RawPayload>>,
    _phantom: PhantomData<Tables>,
}

impl<Tables> PersistEphemeralEvents<Tables>
where
    Tables: MailboxTables,
{
    pub fn new(
        sender: broadcast::Sender<Arc<EphemeralOutboxEvent<RawPayload>>>,
        event_type: EphemeralEventType,
        payload: RawPayload,
    ) -> Self {
        Self {
            sender,
            pre_commit_events: vec![(event_type, payload)],
            post_commit_events: Vec::new(),
            _phantom: PhantomData,
        }
    }
}

impl<Tables> CommitHook for PersistEphemeralEvents<Tables>
where
    Tables: MailboxTables,
{
    async fn pre_commit(
        mut self,
        mut op: HookOperation<'_>,
    ) -> Result<PreCommitRet<'_, Self>, sqlx::Error> {
        for (event_type, payload) in self.pre_commit_events.drain(..) {
            let persisted =
                Tables::persist_ephemeral_event_in_op(&mut op, event_type, payload).await?;
            self.post_commit_events.push(persisted);
        }
        PreCommitRet::ok(self, op)
    }

    fn post_commit(self) {
        for event in self.post_commit_events {
            let _ = self.sender.send(Arc::new(event));
        }
    }

    fn merge(&mut self, other: &mut Self) -> bool {
        self.pre_commit_events.append(&mut other.pre_commit_events);
        true
    }
}
