mod all_listener;
mod ephemeral;
mod ephemeral_events_hook;
mod event;
mod job;
mod persist_events_hook;
mod persistent;
mod pg_notify;

use es_entity::clock::ClockHandle;
use serde::{Serialize, de::DeserializeOwned};

use std::sync::Arc;

pub use self::job::{EventHandlerContext, OutboxEventHandler, OutboxEventJobConfig};
use crate::{config::*, handle::OwnedTaskHandle, sequence::EventSequence, tables::*};
pub use all_listener::AllOutboxListener;
use ephemeral::EphemeralOutboxEventCache;
pub use ephemeral::EphemeralOutboxListener;
pub use event::*;
use persistent::PersistentOutboxEventCache;
pub use persistent::PersistentOutboxListener;

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
    _pg_listener_handle: Arc<OwnedTaskHandle>,
    clock: ClockHandle,
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
            _pg_listener_handle: self._pg_listener_handle.clone(),
            clock: self.clock.clone(),
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

        let (persistent_notification_tx, persistent_notification_rx) =
            tokio::sync::mpsc::channel(config.event_buffer_size);
        let (ephemeral_notification_tx, ephemeral_notification_rx) =
            tokio::sync::mpsc::channel(config.event_buffer_size);
        let pg_listener_handle = pg_notify::spawn_pg_listener::<Tables>(
            &pool,
            persistent_notification_tx,
            ephemeral_notification_tx,
        )
        .await?;

        let persistent_cache =
            PersistentOutboxEventCache::init(&pool, &config, persistent_notification_rx).await?;
        let ephemeral_cache =
            EphemeralOutboxEventCache::init(&pool, &config, ephemeral_notification_rx).await?;

        Ok(Self {
            pool,
            event_buffer_size: config.event_buffer_size,
            persistent_cache: Arc::new(persistent_cache),
            ephemeral_cache: Arc::new(ephemeral_cache),
            _pg_listener_handle: Arc::new(pg_listener_handle),
            clock: config.clock.clone(),
        })
    }

    pub async fn begin_op(&self) -> Result<es_entity::DbOp<'static>, sqlx::Error> {
        es_entity::DbOp::init_with_clock(&self.pool, &self.clock).await
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
        let now = self.clock.artificial_now();
        let event =
            Tables::persist_ephemeral_event(&self.pool, now, event_type, event.into()).await?;
        let _ = self
            .ephemeral_cache
            .cache_fill_sender()
            .send(Arc::new(event));
        Ok(())
    }

    pub async fn publish_ephemeral_in_op(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        event_type: EphemeralEventType,
        event: impl Into<P>,
    ) -> Result<(), sqlx::Error> {
        let hook = ephemeral_events_hook::PersistEphemeralEvents::<P, Tables>::new(
            self.ephemeral_cache.cache_fill_sender().clone(),
            event_type,
            event,
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
    ) -> PersistentOutboxListener<P> {
        PersistentOutboxListener::new(
            self.persistent_cache.handle(),
            start_after,
            self.event_buffer_size,
        )
    }

    pub fn listen_ephemeral(&self) -> EphemeralOutboxListener<P> {
        EphemeralOutboxListener::new(self.ephemeral_cache.handle())
    }

    pub fn listen_all(
        &self,
        start_after: impl Into<Option<EventSequence>>,
    ) -> AllOutboxListener<P> {
        all_listener::AllOutboxListener::new(
            self.persistent_cache.handle(),
            self.ephemeral_cache.handle(),
            start_after,
            self.event_buffer_size,
        )
    }

    pub async fn register_event_handler<H>(
        &self,
        jobs: &mut ::job::Jobs,
        config: OutboxEventJobConfig,
        handler: H,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        H: OutboxEventHandler<P>,
    {
        let initializer =
            job::OutboxEventJobInitializer::<H, P, Tables>::new(self.clone(), handler, &config);
        let spawner = jobs.add_initializer(initializer);
        spawner
            .spawn_unique(::job::JobId::new(), job::OutboxEventJobData::default())
            .await?;
        Ok(())
    }

    pub async fn register_event_handler_with<H>(
        &self,
        jobs: &mut ::job::Jobs,
        config: OutboxEventJobConfig,
        build: impl FnOnce(&mut EventHandlerContext<'_>) -> H,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        H: OutboxEventHandler<P>,
    {
        let mut ctx = EventHandlerContext::new(jobs);
        let handler = build(&mut ctx);
        let jobs = ctx.into_jobs();
        let initializer =
            job::OutboxEventJobInitializer::<H, P, Tables>::new(self.clone(), handler, &config);
        let spawner = jobs.add_initializer(initializer);
        spawner
            .spawn_unique(::job::JobId::new(), job::OutboxEventJobData::default())
            .await?;
        Ok(())
    }
}
