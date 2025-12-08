mod cache;
mod event;
mod listener;
mod persist_events_hook;

use futures::{StreamExt, stream::BoxStream};
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::broadcast;

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use crate::{config::*, handle::OwnedTaskHandle, sequence::EventSequence, tables::*};
use cache::OutboxEventCache;
pub use event::*;
use listener::OutboxListener;

#[derive(Debug)]
#[allow(dead_code)]
pub struct Outbox<P, Tables = DefaultMailboxTables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pool: sqlx::PgPool,
    persistent_event_sender: broadcast::Sender<Arc<PersistentOutboxEvent<P>>>,
    highest_known_sequence: Arc<AtomicU64>,
    event_buffer_size: usize,
    cache: Arc<OutboxEventCache<P, Tables>>,
}

impl<P, Tables> Clone for Outbox<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
    Tables: MailboxTables,
{
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            persistent_event_sender: self.persistent_event_sender.clone(),
            highest_known_sequence: self.highest_known_sequence.clone(),
            event_buffer_size: self.event_buffer_size,
            cache: self.cache.clone(),
        }
    }
}

impl<P, Tables> Outbox<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    pub async fn init(pool: &sqlx::PgPool, config: MailboxConfig) -> Result<Self, sqlx::Error> {
        let pool = pool.clone();
        let (sender, _) = broadcast::channel(config.event_buffer_size);
        let highest_known_sequence = Arc::new(AtomicU64::from(
            Tables::highest_known_persistent_sequence(&pool).await?,
        ));

        let cache = OutboxEventCache::init(
            &pool,
            config,
            sender.clone(),
            highest_known_sequence.clone(),
        )
        .await?;

        Ok(Self {
            pool,
            persistent_event_sender: sender,
            highest_known_sequence,
            event_buffer_size: config.event_buffer_size,
            cache: Arc::new(cache),
        })
    }

    pub async fn begin_op(&self) -> Result<es_entity::DbOp<'static>, sqlx::Error> {
        es_entity::DbOp::init(&self.pool).await
    }

    pub async fn publish_persisted_in_op(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        event: impl Into<P>,
    ) -> Result<(), sqlx::Error> {
        self.publish_all_persisted(op, std::iter::once(event)).await
    }

    pub async fn publish_all_persisted(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        events: impl IntoIterator<Item = impl Into<P>>,
    ) -> Result<(), sqlx::Error> {
        let hook = persist_events_hook::PersistEvents::<P, Tables>::new(
            self.persistent_event_sender.clone(),
            events,
        );
        if let Err(hook) = op.add_commit_hook(hook) {
            use es_entity::hooks::CommitHook;
            hook.force_execute_pre_commit(op).await?;
        }
        Ok(())
    }

    pub fn listen_persisted(
        &self,
        start_after: impl Into<Option<EventSequence>>,
    ) -> OutboxListener<P> {
        let sub = self.persistent_event_sender.subscribe();
        let latest_known = EventSequence::from(self.highest_known_sequence.load(Ordering::Relaxed));
        let start = start_after.into().unwrap_or(latest_known);
        OutboxListener::new(sub, start, latest_known, self.event_buffer_size)
    }
}
