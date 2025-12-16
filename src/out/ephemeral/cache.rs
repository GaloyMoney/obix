use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{broadcast, mpsc, oneshot};

use std::sync::Arc;

use crate::out::event::*;
use crate::{config::*, handle::OwnedTaskHandle};

pub struct CacheHandle<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    ephemeral_event_receiver: Option<broadcast::Receiver<Arc<EphemeralOutboxEvent<P>>>>,
    backfill_request: mpsc::UnboundedSender<
        oneshot::Sender<im::HashMap<EphemeralEventType, Arc<EphemeralOutboxEvent<P>>>>,
    >,
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

    pub fn request_current_ephemeral_events(
        &self,
    ) -> oneshot::Receiver<im::HashMap<EphemeralEventType, Arc<EphemeralOutboxEvent<P>>>> {
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
    backfill_request_send: mpsc::UnboundedSender<
        oneshot::Sender<im::HashMap<EphemeralEventType, Arc<EphemeralOutboxEvent<P>>>>,
    >,
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
        config: MailboxConfig,
        ephemeral_notification_rx: mpsc::Receiver<sqlx::postgres::PgNotification>,
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
        cache: im::HashMap<EphemeralEventType, Arc<EphemeralOutboxEvent<P>>>,
        event: Arc<EphemeralOutboxEvent<P>>,
        ephemeral_event_sender: &broadcast::Sender<Arc<EphemeralOutboxEvent<P>>>,
    ) -> im::HashMap<EphemeralEventType, Arc<EphemeralOutboxEvent<P>>> {
        let event_type = event.event_type.clone();
        let cache = cache.update(event_type, event.clone());
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

    fn handle_ephemeral_notification(
        pool: &sqlx::PgPool,
        payload: &str,
        cache: &im::HashMap<EphemeralEventType, Arc<EphemeralOutboxEvent<P>>>,
        cache_fill_sender: &broadcast::Sender<Arc<EphemeralOutboxEvent<P>>>,
    ) -> Option<EphemeralOutboxEvent<P>> {
        #[derive(serde::Deserialize)]
        struct NotificationHeader {
            event_type: String,
            #[serde(default)]
            payload_omitted: bool,
        }

        let header: NotificationHeader = serde_json::from_str(payload).ok()?;
        let event_type = serde_json::from_value(serde_json::Value::String(header.event_type))
            .expect("Couldn't deserialize event_type");

        if header.payload_omitted {
            if cache.contains_key(&event_type) {
                return None;
            }
            tokio::spawn(Self::fetch_event_by_type(
                pool.clone(),
                event_type,
                cache_fill_sender.clone(),
            ));
            None
        } else {
            serde_json::from_str(payload).ok()
        }
    }

    fn process_notification(
        notification: sqlx::postgres::PgNotification,
        pool: &sqlx::PgPool,
        cache: &im::HashMap<EphemeralEventType, Arc<EphemeralOutboxEvent<P>>>,
        cache_fill_sender: &broadcast::Sender<Arc<EphemeralOutboxEvent<P>>>,
    ) -> Option<EphemeralOutboxEvent<P>> {
        if notification.channel() == Tables::ephemeral_outbox_events_channel() {
            Self::handle_ephemeral_notification(
                pool,
                notification.payload(),
                cache,
                cache_fill_sender,
            )
        } else {
            None
        }
    }

    async fn spawn_cache_loop(
        pool: &sqlx::PgPool,
        _config: MailboxConfig,
        ephemeral_event_sender: broadcast::Sender<Arc<EphemeralOutboxEvent<P>>>,
        mut backfill_request: mpsc::UnboundedReceiver<
            oneshot::Sender<im::HashMap<EphemeralEventType, Arc<EphemeralOutboxEvent<P>>>>,
        >,
        mut cache_fill_receiver: broadcast::Receiver<Arc<EphemeralOutboxEvent<P>>>,
        cache_fill_sender: broadcast::Sender<Arc<EphemeralOutboxEvent<P>>>,
        mut ephemeral_notification_rx: mpsc::Receiver<sqlx::postgres::PgNotification>,
    ) -> Result<OwnedTaskHandle, sqlx::Error> {
        let pool = pool.clone();

        let handle = tokio::spawn(async move {
            let mut ephemeral_cache: im::HashMap<EphemeralEventType, Arc<EphemeralOutboxEvent<P>>> =
                im::HashMap::new();

            loop {
                tokio::select! {
                    biased;

                    result = backfill_request.recv() => {
                        match result {
                            Some(sender) => {
                                let _ = sender.send(ephemeral_cache.clone());
                            }
                            None => {
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
                            Err(broadcast::error::RecvError::Lagged(_)) => {
                                continue;
                            }
                            Err(broadcast::error::RecvError::Closed) => {
                                break;
                            }
                        }
                    }

                    result = ephemeral_notification_rx.recv() => {
                        match result {
                            Some(notification) => {
                                if let Some(event) = Self::process_notification(
                                    notification,
                                    &pool,
                                    &ephemeral_cache,
                                    &cache_fill_sender,
                                ) {
                                    ephemeral_cache = Self::insert_into_cache_and_broadcast(
                                        ephemeral_cache,
                                        Arc::new(event),
                                        &ephemeral_event_sender,
                                    );
                                }

                                // Process any additional buffered notifications
                                while let Ok(notification) = ephemeral_notification_rx.try_recv() {
                                    if let Some(event) = Self::process_notification(
                                        notification,
                                        &pool,
                                        &ephemeral_cache,
                                        &cache_fill_sender,
                                    ) {
                                        ephemeral_cache = Self::insert_into_cache_and_broadcast(
                                            ephemeral_cache,
                                            Arc::new(event),
                                            &ephemeral_event_sender,
                                        );
                                    }
                                }
                            }
                            None => {
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
