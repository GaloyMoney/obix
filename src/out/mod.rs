mod ephemeral;
mod event;
mod persistent;
mod persist_events_hook;

use serde::{Serialize, de::DeserializeOwned};

use std::sync::Arc;

use crate::{config::*, sequence::EventSequence, tables::*};
use ephemeral::{EphemeralOutboxEventCache, EphemeralOutboxListener};
use persistent::{PersistentOutboxEventCache, PersistentOutboxListener};
pub use event::*;

#[derive(Debug)]
#[allow(dead_code)]
pub struct Outbox<P, Tables = DefaultMailboxTables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pool: sqlx::PgPool,
    event_buffer_size: usize,
    persistent_cache: Arc<PersistentOutboxEventCache<P, Tables>>,
    ephemeral_cache: Arc<EphemeralOutboxEventCache<P, Tables>>,
}

impl<P, Tables> Clone for Outbox<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
    Tables: MailboxTables,
{
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            event_buffer_size: self.event_buffer_size,
            persistent_cache: self.persistent_cache.clone(),
            ephemeral_cache: self.ephemeral_cache.clone(),
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

        let persistent_cache = PersistentOutboxEventCache::init(&pool, config).await?;
        let ephemeral_cache = EphemeralOutboxEventCache::init(&pool, config).await?;

        Ok(Self {
            pool,
            event_buffer_size: config.event_buffer_size,
            persistent_cache: Arc::new(persistent_cache),
            ephemeral_cache: Arc::new(ephemeral_cache),
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
            self.persistent_cache.cache_fill_sender(),
            events,
        );
        if let Err(hook) = op.add_commit_hook(hook) {
            use es_entity::hooks::CommitHook;
            hook.force_execute_pre_commit(op).await?;
        }
        Ok(())
    }

    pub async fn publish_ephemeral(
        &self,
        event_type: EphemeralEventType,
        event: impl Into<P>,
    ) -> Result<(), sqlx::Error> {
        let event = Tables::persist_ephemeral_event(&self.pool, event_type, event.into()).await?;
        let _ = self.ephemeral_cache.cache_fill_sender().send(Arc::new(event));
        Ok(())
    }

    pub fn listen_persisted(
        &self,
        start_after: impl Into<Option<EventSequence>>,
    ) -> PersistentOutboxListener<P> {
        PersistentOutboxListener::new(self.persistent_cache.handle(), start_after, self.event_buffer_size)
    }

    pub fn listen_ephemeral(&self) -> EphemeralOutboxListener<P> {
        EphemeralOutboxListener::new(self.ephemeral_cache.handle())
    }
}
