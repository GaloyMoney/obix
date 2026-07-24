use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{broadcast, mpsc, oneshot};

use std::sync::Arc;

pub(crate) type EphemeralCacheMapEntry<P> = (EphemeralMailboxKey, Arc<EphemeralOutboxEvent<P>>);
pub(crate) type EphemeralCacheMap<P> =
    im::HashMap<EphemeralMailboxKey, Arc<EphemeralOutboxEvent<P>>>;

use crate::{
    config::*,
    handle::{OwnedTaskHandle, spawn_supervised},
    out::{event::*, pg_notify::NotifyMessage},
};

pub struct CacheHandle<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    ephemeral_event_receiver: Option<broadcast::Receiver<Arc<EphemeralOutboxEvent<P>>>>,
    backfill_request: mpsc::UnboundedSender<oneshot::Sender<EphemeralCacheMap<P>>>,
}

impl<P> CacheHandle<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn ephemeral_event_stream(&mut self) -> broadcast::Receiver<Arc<EphemeralOutboxEvent<P>>> {
        self.ephemeral_event_receiver
            .take()
            .expect("receiver already taken")
    }

    pub fn request_current_ephemeral_events(&self) -> oneshot::Receiver<EphemeralCacheMap<P>> {
        let (tx, rx) = oneshot::channel();
        let _ = self.backfill_request.send(tx);
        rx
    }
}

#[derive(Debug)]
pub struct EphemeralOutboxEventCache<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    ephemeral_event_sender: broadcast::Sender<Arc<EphemeralOutboxEvent<P>>>,
    backfill_request_send: mpsc::UnboundedSender<oneshot::Sender<EphemeralCacheMap<P>>>,
    cache_fill_sender: broadcast::Sender<Arc<EphemeralOutboxEvent<P>>>,
    _cache_loop_handle: OwnedTaskHandle,
    _phantom: std::marker::PhantomData<Tables>,
}

impl<P, Tables> EphemeralOutboxEventCache<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
    Tables: crate::tables::MailboxTables,
{
    pub fn handle(&self) -> CacheHandle<P> {
        CacheHandle {
            ephemeral_event_receiver: Some(self.ephemeral_event_sender.subscribe()),
            backfill_request: self.backfill_request_send.clone(),
        }
    }

    pub fn cache_fill_sender(&self) -> &broadcast::Sender<Arc<EphemeralOutboxEvent<P>>> {
        &self.cache_fill_sender
    }

    pub async fn init(
        pool: &sqlx::PgPool,
        config: &MailboxConfig,
        ephemeral_notification_rx: mpsc::Receiver<NotifyMessage>,
    ) -> Result<Self, sqlx::Error> {
        let (backfill_send, backfill_recv) = mpsc::unbounded_channel();
        let (cache_fill_send, cache_fill_recv) = broadcast::channel(config.event_buffer_size);
        let (ephemeral_event_sender, _) = broadcast::channel(config.event_buffer_size);

        let cache_loop_handle = Self::spawn_cache_loop(
            pool,
            config,
            ephemeral_event_sender.clone(),
            backfill_recv,
            cache_fill_recv,
            cache_fill_send.clone(),
            ephemeral_notification_rx,
        )
        .await?;

        let ret = Self {
            backfill_request_send: backfill_send,
            ephemeral_event_sender,
            cache_fill_sender: cache_fill_send,
            _cache_loop_handle: cache_loop_handle,
            _phantom: std::marker::PhantomData,
        };
        Ok(ret)
    }

    fn insert_into_cache_and_broadcast(
        cache: EphemeralCacheMap<P>,
        event: Arc<EphemeralOutboxEvent<P>>,
        ephemeral_event_sender: &broadcast::Sender<Arc<EphemeralOutboxEvent<P>>>,
    ) -> EphemeralCacheMap<P> {
        let key = (event.event_type.clone(), event.conflation_key.clone());
        // Last-write-wins by recorded_at: notification-triggered fetches for
        // the same (event_type, conflation_key) can complete out of order —
        // a strictly older event must never overwrite (or be broadcast
        // after) a newer one.
        if let Some(cached) = cache.get(&key)
            && cached.recorded_at > event.recorded_at
        {
            return cache;
        }
        let cache = cache.update(key, event.clone());
        let _ = ephemeral_event_sender.send(event);
        cache
    }

    async fn fetch_event_by_type(
        pool: sqlx::PgPool,
        event_type: EphemeralEventType,
        cache_fill_sender: broadcast::Sender<Arc<EphemeralOutboxEvent<P>>>,
    ) {
        if let Ok(events) = Tables::load_ephemeral_events::<P>(&pool, Some(event_type)).await {
            for event in events {
                let _ = cache_fill_sender.send(Arc::new(event));
            }
        }
    }

    /// Handle a `{event_type, recorded_at}` notification hint.
    ///
    /// The payload is NEVER taken from the notification body. PostgreSQL
    /// performs no authorization on LISTEN/NOTIFY channels — any role able
    /// to connect to this database can signal any channel — so a
    /// notification is only ever a hint that something changed; the event
    /// itself is always (re-)fetched from the table with this process's own
    /// credentials. `recorded_at` bounds that fetch: when the in-process
    /// cache already holds an event at least as recent (the publisher's
    /// post-commit broadcast beat the NOTIFY), the hint is a no-op.
    fn handle_ephemeral_notification(payload: &str) -> Option<EphemeralEventType> {
        #[derive(serde::Deserialize)]
        struct NotificationHeader {
            event_type: EphemeralEventType,
        }

        let header: NotificationHeader = match serde_json::from_str(payload) {
            Ok(header) => header,
            Err(error) => {
                record_notification_undecodable(&error);
                return None;
            }
        };

        // The hint carries only event_type (no conflation_key), so we
        // cannot check per-key freshness against the cache. Always fetch —
        // the LWW guard in insert_into_cache_and_broadcast prevents a stale
        // fetch result from overwriting or re-broadcasting a newer event.
        Some(header.event_type)
    }

    fn process_notification(
        notification: sqlx::postgres::PgNotification,
    ) -> Option<EphemeralEventType> {
        if notification.channel() == Tables::ephemeral_outbox_events_channel() {
            Self::handle_ephemeral_notification(notification.payload())
        } else {
            None
        }
    }

    async fn spawn_cache_loop(
        pool: &sqlx::PgPool,
        _config: &MailboxConfig,
        ephemeral_event_sender: broadcast::Sender<Arc<EphemeralOutboxEvent<P>>>,
        mut backfill_request: mpsc::UnboundedReceiver<oneshot::Sender<EphemeralCacheMap<P>>>,
        mut cache_fill_receiver: broadcast::Receiver<Arc<EphemeralOutboxEvent<P>>>,
        cache_fill_sender: broadcast::Sender<Arc<EphemeralOutboxEvent<P>>>,
        mut ephemeral_notification_rx: mpsc::Receiver<NotifyMessage>,
    ) -> Result<OwnedTaskHandle, sqlx::Error> {
        let pool = pool.clone();

        let handle = spawn_supervised("obix::ephemeral_cache_loop", async move {
            let mut ephemeral_cache: im::HashMap<
                (EphemeralEventType, EphemeralEventKey),
                Arc<EphemeralOutboxEvent<P>>,
            > = im::HashMap::new();

            loop {
                tokio::select! {
                    biased;

                    result = backfill_request.recv() => {
                        match result {
                            Some(sender) => {
                                let _ = sender.send(ephemeral_cache.clone());
                            }
                            None => {
                                record_backfill_channel_closed();
                                break;
                            }
                        }
                        continue;
                    }

                    result = cache_fill_receiver.recv() => {
                        match result {
                            Ok(event) => {
                                ephemeral_cache = Self::insert_into_cache_and_broadcast(
                                    ephemeral_cache,
                                    event,
                                    &ephemeral_event_sender,
                                );

                                while let Ok(event) = cache_fill_receiver.try_recv() {
                                    ephemeral_cache = Self::insert_into_cache_and_broadcast(
                                        ephemeral_cache,
                                        event,
                                        &ephemeral_event_sender,
                                    );
                                }
                            }
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                record_cache_fill_lagged(n);
                                continue;
                            }
                            Err(broadcast::error::RecvError::Closed) => {
                                record_cache_fill_closed();
                                break;
                            }
                        }
                    }

                    result = ephemeral_notification_rx.recv() => {
                        match result {
                            Some(message) => {
                                let mut resync_needed = false;
                                let mut messages = vec![message];
                                // Process any additional buffered notifications
                                while let Ok(message) = ephemeral_notification_rx.try_recv() {
                                    messages.push(message);
                                }
                                // Collect the unique event types that need a
                                // fetch. A burst of N notifications for the
                                // same type is drained against one cache
                                // snapshot (fetches are async, the cache is
                                // unchanged during the drain), so deduping
                                // here collapses them into a single fetch
                                // rather than N redundant broadcasts of the
                                // same latest event.
                                let mut types_to_fetch =
                                    std::collections::HashSet::<EphemeralEventType>::new();
                                for message in messages {
                                    match message {
                                        NotifyMessage::Notification(notification) => {
                                            if let Some(event_type) =
                                                Self::process_notification(notification)
                                            {
                                                types_to_fetch.insert(event_type);
                                            }
                                        }
                                        NotifyMessage::Resync => {
                                            resync_needed = true;
                                        }
                                    }
                                }
                                for event_type in types_to_fetch {
                                    tokio::spawn(Self::fetch_event_by_type(
                                        pool.clone(),
                                        event_type,
                                        cache_fill_sender.clone(),
                                    ));
                                }

                                if resync_needed {
                                    match Tables::load_ephemeral_events::<P>(&pool, None).await {
                                        Ok(events) => {
                                            for event in events {
                                                let stale = ephemeral_cache
                                                    .get(&(
                                                        event.event_type.clone(),
                                                        event.conflation_key.clone(),
                                                    ))
                                                    .is_some_and(|cached| {
                                                        cached.recorded_at >= event.recorded_at
                                                    });
                                                if !stale {
                                                    ephemeral_cache =
                                                        Self::insert_into_cache_and_broadcast(
                                                            ephemeral_cache,
                                                            Arc::new(event),
                                                            &ephemeral_event_sender,
                                                        );
                                                }
                                            }
                                        }
                                        Err(e) => record_resync_failed(&e),
                                    }
                                }
                            }
                            None => {
                                record_notification_channel_closed();
                                break;
                            }
                        }
                    }
                }
            }
        });
        Ok(OwnedTaskHandle::new(handle))
    }
}

#[tracing::instrument(
    name = "obix.ephemeral_cache.backfill_channel_closed",
    level = "error",
    fields(otel.status_code = "ERROR"),
)]
fn record_backfill_channel_closed() {}

#[tracing::instrument(
    name = "obix.ephemeral_cache.cache_fill_lagged",
    level = "error",
    fields(otel.status_code = "ERROR"),
)]
fn record_cache_fill_lagged(dropped: u64) {}

#[tracing::instrument(
    name = "obix.ephemeral_cache.cache_fill_closed",
    level = "error",
    fields(otel.status_code = "ERROR"),
)]
fn record_cache_fill_closed() {}

#[tracing::instrument(
    name = "obix.ephemeral_cache.notification_channel_closed",
    level = "error",
    fields(otel.status_code = "ERROR"),
)]
fn record_notification_channel_closed() {}

#[tracing::instrument(
    name = "obix.ephemeral_cache.resync_failed",
    level = "error",
    skip_all,
    fields(otel.status_code = "ERROR", error = %error),
)]
fn record_resync_failed(error: &sqlx::Error) {}

#[tracing::instrument(
    name = "obix.ephemeral_cache.notification_undecodable",
    level = "warn",
    skip_all,
    fields(error = %error),
)]
fn record_notification_undecodable(error: &serde_json::Error) {}
