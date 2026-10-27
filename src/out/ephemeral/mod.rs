mod cache;
mod event;
mod hook;
mod listener;
mod subscriber;

use es_entity::clock::{Clock, ClockHandle};

use std::sync::Arc;

use cache::EphemeralOutboxEventCache;
pub use event::{
    EphemeralEvent, EphemeralEventType, EphemeralOutboxEvent, EphemeralSelection, RawPayload,
};
pub use listener::EphemeralOutboxListener;
pub use subscriber::{EphemeralSubscriber, EphemeralSubscriberConfig, EphemeralSubscription};

use super::pg_notify;
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
/// [`Outbox`](crate::Outbox), with its own cache and its own LISTEN
/// connection. It has no payload type: what is published is any
/// [`EphemeralEvent`], stored under the type's key, and what is heard is any
/// [`EphemeralSelection`] of those keys.
pub struct EphemeralOutbox<Tables = DefaultMailboxTables> {
    pool: sqlx::PgPool,
    event_buffer_size: usize,
    cache: Arc<EphemeralOutboxEventCache<Tables>>,
    _pg_listener_handle: Arc<OwnedTaskHandle>,
    clock: ClockHandle,
}

impl<Tables> std::fmt::Debug for EphemeralOutbox<Tables> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EphemeralOutbox")
            .field("event_buffer_size", &self.event_buffer_size)
            .finish_non_exhaustive()
    }
}

impl<Tables> Clone for EphemeralOutbox<Tables> {
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

impl<Tables> EphemeralOutbox<Tables>
where
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

    /// Publish `event` under its type's key, replacing the previous value of
    /// that key. A selection is not publishable — only a leaf
    /// [`EphemeralEvent`]:
    ///
    /// ```compile_fail,E0277
    /// use obix::{EphemeralEvent, EphemeralEventType, EphemeralOutbox, EphemeralSelection};
    ///
    /// #[derive(serde::Serialize, serde::Deserialize)]
    /// struct Status(u64);
    /// impl EphemeralEvent for Status {
    ///     const EVENT_TYPE: EphemeralEventType = EphemeralEventType::new("doc.status");
    /// }
    ///
    /// #[derive(EphemeralSelection)]
    /// enum Statuses {
    ///     Status(Status),
    /// }
    ///
    /// async fn publish_a_selection(outbox: &EphemeralOutbox) {
    ///     let _ = outbox.publish(Statuses::Status(Status(1))).await;
    /// }
    /// ```
    pub async fn publish<T: EphemeralEvent>(&self, event: T) -> Result<(), ObixFault> {
        let payload = serde_json::value::to_raw_value(&event)?;
        let now = self.clock.manual_now();
        let event =
            Tables::persist_ephemeral_event(&self.pool, now, T::EVENT_TYPE, payload).await?;
        let _ = self.cache.cache_fill_sender().send(Arc::new(event));
        Ok(())
    }

    pub async fn publish_in_op<T: EphemeralEvent>(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        event: T,
    ) -> Result<(), ObixFault> {
        let payload = serde_json::value::to_raw_value(&event)?;
        let hook = hook::PersistEphemeralEvents::<Tables>::new(
            self.cache.cache_fill_sender().clone(),
            T::EVENT_TYPE,
            payload,
        );
        if let Err(hook) = op.add_commit_hook(hook) {
            use es_entity::hooks::CommitHook;
            hook.force_execute_pre_commit(op).await?;
        }
        Ok(())
    }

    /// One broadcast receiver filtered on `S::EVENT_TYPES`, each row decoded
    /// with `S::decode`. Arrival order is broadcast order across all of the
    /// selection's keys. Backfills the current row of each key in
    /// `S::EVENT_TYPES` first.
    ///
    /// A row that does not decode as `S` is skipped and recorded.
    pub fn listen<S: EphemeralSelection>(&self) -> EphemeralOutboxListener<S> {
        EphemeralOutboxListener::new(
            self.cache.handle(),
            S::EVENT_TYPES,
            listener::decode_selection::<S>,
        )
    }

    /// The dynamic primitive under [`listen`](Self::listen): filter on
    /// `keys`, no decode. For consumers whose key set is assembled at
    /// runtime; a consumer that can name its types uses `listen`.
    pub fn listen_raw(
        &self,
        keys: &'static [EphemeralEventType],
    ) -> EphemeralOutboxListener<RawPayload> {
        EphemeralOutboxListener::new(self.cache.handle(), keys, listener::decode_raw)
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
    pub async fn register_subscriber<S, H>(
        &self,
        jobs: &mut ::job::Jobs,
        config: EphemeralSubscriberConfig,
        handler: H,
    ) -> Result<EphemeralSubscription, ObixFault>
    where
        S: EphemeralSelection,
        H: EphemeralSubscriber<S>,
    {
        self.register::<S, H>(jobs, config, |outbox| outbox.listen::<S>(), handler)
            .await
    }

    /// [`register_subscriber`](Self::register_subscriber) for a key set
    /// assembled at runtime: the handler is handed each row of `keys`
    /// undecoded.
    pub async fn register_subscriber_raw<H>(
        &self,
        jobs: &mut ::job::Jobs,
        config: EphemeralSubscriberConfig,
        keys: &'static [EphemeralEventType],
        handler: H,
    ) -> Result<EphemeralSubscription, ObixFault>
    where
        H: EphemeralSubscriber<RawPayload>,
    {
        self.register::<RawPayload, H>(jobs, config, move |outbox| outbox.listen_raw(keys), handler)
            .await
    }

    async fn register<P, H>(
        &self,
        jobs: &mut ::job::Jobs,
        config: EphemeralSubscriberConfig,
        listen: impl Fn(&Self) -> EphemeralOutboxListener<P> + Send + Sync + 'static,
        handler: H,
    ) -> Result<EphemeralSubscription, ObixFault>
    where
        P: Send + Sync + 'static,
        H: EphemeralSubscriber<P>,
    {
        let initializer = subscriber::EphemeralSubscriberJobInitializer::<H, P, Tables>::new(
            self.clone(),
            handler,
            Arc::new(listen),
            &config,
        );
        let spawner = jobs.add_resident_initializer(initializer);
        let handle = spawner
            .spawn(subscriber::EphemeralSubscriberJobData::default())
            .await?;
        Ok(EphemeralSubscription::new(handle.id()))
    }
}
