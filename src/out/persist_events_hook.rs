use std::any::TypeId;
use std::marker::PhantomData;
use std::sync::Arc;

use es_entity::{
    AtomicOperation,
    hooks::{CommitHook, HookOperation, PreCommitRet},
};
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
    notifier_tx: mpsc::UnboundedSender<(EventSequence, EventSequence)>,
    pre_commit_events: Vec<P>,
    post_commit_events: Vec<PersistentOutboxEvent<P>>,
    batch_size: usize,
    post_persist_hooks: PostPersistHooks<P>,
    runs_after: Arc<[TypeId]>,
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
        runs_after: Arc<[TypeId]>,
    ) -> Self {
        Self {
            sender,
            notifier_tx,
            pre_commit_events: events.into_iter().map(Into::into).collect(),
            post_commit_events: Vec::new(),
            batch_size,
            post_persist_hooks,
            runs_after,
            _phantom: PhantomData,
        }
    }

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
        let mut events = std::mem::take(&mut self.pre_commit_events).into_iter();
        loop {
            let chunk: Vec<P> = events.by_ref().take(batch_size).collect();
            if chunk.is_empty() {
                break;
            }
            let persisted_chunk = Tables::persist_events(&mut op, chunk.into_iter()).await?;
            let chunk_start = self.post_commit_events.len();
            self.post_commit_events.extend(persisted_chunk);
            for hook in self.post_persist_hooks.iter() {
                hook.on_persisted(&mut op, &self.post_commit_events[chunk_start..])
                    .await?;
            }
        }
        if !self.post_commit_events.is_empty() {
            let seal = super::publication_batch::SealPublicationBatch::<Tables>::new(
                &self.post_commit_events,
            );
            if op.add_commit_hook(seal).is_err() {
                return Err(sqlx::Error::Protocol(
                    "publication seal requires commit finalization".into(),
                ));
            }
        }
        PreCommitRet::ok(self, op)
    }

    fn post_commit(self) {
        let range = self
            .post_commit_events
            .first()
            .zip(self.post_commit_events.last())
            .map(|(first, last)| (first.sequence, last.sequence));
        for event in self.post_commit_events {
            let _ = self.sender.send(PersistentDelivery::from(Ok(event)));
        }
        if let Some(range) = range {
            let _ = self.notifier_tx.send(range);
        }
    }

    fn merge(&mut self, other: &mut Self) -> bool {
        self.pre_commit_events.append(&mut other.pre_commit_events);
        true
    }

    fn runs_after(&self) -> &[TypeId] {
        &self.runs_after
    }
}
