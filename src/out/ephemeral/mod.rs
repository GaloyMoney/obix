mod cache;
mod hook;
mod listener;
mod subscriber;

use es_entity::clock::{Clock, ClockHandle};
use serde::{Serialize, de::DeserializeOwned};

use std::sync::Arc;

use cache::EphemeralOutboxEventCache;
pub use listener::EphemeralOutboxListener;
pub use subscriber::{EphemeralSubscriber, EphemeralSubscriberConfig, EphemeralSubscription};

use super::{event::*, pg_notify};
use crate::{error::ObixFault, handle::OwnedTaskHandle, tables::*};

/// Configuration of an [`EphemeralOutbox`].
#[derive(Clone, bon::Builder)]
pub struct EphemeralConfig {
    #[builder(default = 100)]
    pub event_buffer_size: usize,
    #[builder(default = Clock::handle().clone())]
    pub clock: ClockHandle,
}

/// The ephemeral stream: last-value-wins per [`EphemeralEventType`],
/// at-most-once, never replayed. It is a sibling of
/// [`Outbox`](crate::Outbox), with its own payload type `Q`, its own cache
/// and its own LISTEN connection.
pub struct EphemeralOutbox<Q, Tables = DefaultMailboxTables>
where
    Q: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pool: sqlx::PgPool,
    event_buffer_size: usize,
    cache: Arc<EphemeralOutboxEventCache<Q, Tables>>,
    _pg_listener_handle: Arc<OwnedTaskHandle>,
    clock: ClockHandle,
}

impl<Q, Tables> std::fmt::Debug for EphemeralOutbox<Q, Tables>
where
    Q: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EphemeralOutbox")
            .field("event_buffer_size", &self.event_buffer_size)
            .finish_non_exhaustive()
    }
}

impl<Q, Tables> Clone for EphemeralOutbox<Q, Tables>
where
    Q: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            event_buffer_size: self.event_buffer_size,
            cache: self.cache.clone(),
            _pg_listener_handle: self._pg_listener_handle.clone(),
            clock: self.clock.clone(),
        }
    }
}

impl<Q, Tables> EphemeralOutbox<Q, Tables>
where
    Q: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    pub async fn init(pool: &sqlx::PgPool, config: EphemeralConfig) -> Result<Self, ObixFault> {
        let pool = pool.clone();

        let (notification_tx, notification_rx) =
            tokio::sync::mpsc::channel(config.event_buffer_size);
        let pg_listener_handle = pg_notify::spawn_pg_listener(
            &pool,
            Tables::ephemeral_outbox_events_channel(),
            notification_tx,
        )
        .await?;

        let cache =
            EphemeralOutboxEventCache::init(&pool, config.event_buffer_size, notification_rx)
                .await?;

        Ok(Self {
            pool,
            event_buffer_size: config.event_buffer_size,
            cache: Arc::new(cache),
            _pg_listener_handle: Arc::new(pg_listener_handle),
            clock: config.clock,
        })
    }

    /// Raw `sqlx::Error`, like [`Outbox::begin_op`](crate::Outbox::begin_op).
    pub async fn begin_op(&self) -> Result<es_entity::DbOp<'static>, sqlx::Error> {
        es_entity::DbOp::init_with_clock(&self.pool, &self.clock).await
    }

    pub async fn publish(
        &self,
        event_type: EphemeralEventType,
        event: impl Into<Q>,
    ) -> Result<(), ObixFault> {
        let now = self.clock.manual_now();
        let event =
            Tables::persist_ephemeral_event(&self.pool, now, event_type, event.into()).await?;
        let _ = self.cache.cache_fill_sender().send(Arc::new(event));
        Ok(())
    }

    pub async fn publish_in_op(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        event_type: EphemeralEventType,
        event: impl Into<Q>,
    ) -> Result<(), ObixFault> {
        let hook = hook::PersistEphemeralEvents::<Q, Tables>::new(
            self.cache.cache_fill_sender().clone(),
            event_type,
            event,
        );
        if let Err(hook) = op.add_commit_hook(hook) {
            use es_entity::hooks::CommitHook;
            hook.force_execute_pre_commit(op).await?;
        }
        Ok(())
    }

    pub fn listen(&self) -> EphemeralOutboxListener<Q> {
        EphemeralOutboxListener::new(self.cache.handle())
    }

    /// Register `handler` as a resident job consuming this stream: one
    /// instance cluster-wide, always on.
    ///
    /// That presence is a contract, not an implementation detail: ephemeral
    /// events cannot be replayed, so only an always-present consumer may hear
    /// them. It is also why there is nothing to pause — a flow that needs to
    /// pause or stage is persistent-only by definition; see
    /// [`KeyedSubscriber`](crate::KeyedSubscriber).
    ///
    /// Registration is idempotent per job type: registering the same job type
    /// twice resolves to the already-persisted job, so both calls hand back
    /// the same [`job_id`](EphemeralSubscription::job_id).
    ///
    /// Must be called **before** [`::job::Jobs::start_poll`].
    pub async fn register_subscriber<H>(
        &self,
        jobs: &mut ::job::Jobs,
        config: EphemeralSubscriberConfig,
        handler: H,
    ) -> Result<EphemeralSubscription, ObixFault>
    where
        H: EphemeralSubscriber<Q>,
    {
        let initializer = subscriber::EphemeralSubscriberJobInitializer::<H, Q, Tables>::new(
            self.clone(),
            handler,
            &config,
        );
        let spawner = jobs.add_resident_initializer(initializer);
        let handle = spawner
            .spawn(subscriber::EphemeralSubscriberJobData::default())
            .await?;
        Ok(EphemeralSubscription::new(handle.id()))
    }
}
