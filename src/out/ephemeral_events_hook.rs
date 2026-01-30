use std::{marker::PhantomData, sync::Arc};

use es_entity::hooks::{CommitHook, HookOperation, PreCommitRet};
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::broadcast;

use crate::out::event::{EphemeralEventType, EphemeralOutboxEvent};
use crate::tables::MailboxTables;

pub struct EphemeralEvent<P> {
    pub event_type: EphemeralEventType,
    pub payload: P,
}

pub struct PersistEphemeralEvents<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    sender: broadcast::Sender<Arc<EphemeralOutboxEvent<P>>>,
    pre_commit_events: Vec<EphemeralEvent<P>>,
    post_commit_events: Vec<EphemeralOutboxEvent<P>>,
    _phantom: PhantomData<Tables>,
}

impl<P, Tables> PersistEphemeralEvents<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    pub fn new(
        sender: broadcast::Sender<Arc<EphemeralOutboxEvent<P>>>,
        event_type: EphemeralEventType,
        event: impl Into<P>,
    ) -> Self {
        Self {
            sender,
            pre_commit_events: vec![EphemeralEvent {
                event_type,
                payload: event.into(),
            }],
            post_commit_events: Vec::new(),
            _phantom: PhantomData,
        }
    }
}

impl<P, Tables> CommitHook for PersistEphemeralEvents<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    async fn pre_commit(
        mut self,
        mut op: HookOperation<'_>,
    ) -> Result<PreCommitRet<'_, Self>, sqlx::Error> {
        for event in self.pre_commit_events.drain(..) {
            let persisted =
                Tables::persist_ephemeral_event_in_op(&mut op, event.event_type, event.payload)
                    .await?;
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
