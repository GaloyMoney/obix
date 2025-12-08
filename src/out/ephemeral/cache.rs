use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;

use std::sync::Arc;

use crate::out::event::*;
use crate::{config::*, handle::OwnedTaskHandle};

pub struct CacheHandle<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    ephemeral_event_receiver: Option<broadcast::Receiver<Arc<EphemeralOutboxEvent<P>>>>,
    backfill_request: mpsc::UnboundedSender<mpsc::Sender<Arc<EphemeralOutboxEvent<P>>>>,
    backfill_buffer_size: usize,
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

    pub fn request_current_ephemeral_events(&self) -> ReceiverStream<Arc<EphemeralOutboxEvent<P>>> {
        let (tx, rx) = mpsc::channel(self.backfill_buffer_size);
        let _ = self.backfill_request.send(tx);
        ReceiverStream::new(rx)
    }
}

#[derive(Debug)]
pub struct EphemeralOutboxEventCache<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    ephemeral_event_sender: broadcast::Sender<Arc<EphemeralOutboxEvent<P>>>,
    backfill_request_send: mpsc::UnboundedSender<mpsc::Sender<Arc<EphemeralOutboxEvent<P>>>>,
    backfill_buffer_size: usize,
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
            backfill_buffer_size: self.backfill_buffer_size,
        }
    }

    pub fn cache_fill_sender(&self) -> broadcast::Sender<Arc<EphemeralOutboxEvent<P>>> {
        self.cache_fill_sender.clone()
    }

    pub async fn init(pool: &sqlx::PgPool, config: MailboxConfig) -> Result<Self, sqlx::Error> {
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
        )
        .await?;

        let ret = Self {
            backfill_request_send: backfill_send,
            ephemeral_event_sender,
            backfill_buffer_size: config.event_buffer_size,
            cache_fill_sender: cache_fill_send,
            _cache_loop_handle: cache_loop_handle,
            _phantom: std::marker::PhantomData,
        };
        Ok(ret)
    }

    // @ claude this is not maybe
    fn insert_into_cache_and_maybe_broadcast(
        cache: im::HashMap<EphemeralEventType, Arc<EphemeralOutboxEvent<P>>>,
        event: Arc<EphemeralOutboxEvent<P>>,
        ephemeral_event_sender: &broadcast::Sender<Arc<EphemeralOutboxEvent<P>>>,
    ) -> im::HashMap<EphemeralEventType, Arc<EphemeralOutboxEvent<P>>> {
        let event_type = event.event_type.clone();
        let cache = cache.update(event_type, event.clone());
        let _ = ephemeral_event_sender.send(event);
        cache
    }

    async fn handle_backfill_request(
        sender: mpsc::Sender<Arc<EphemeralOutboxEvent<P>>>,
        cache_snapshot: im::HashMap<EphemeralEventType, Arc<EphemeralOutboxEvent<P>>>,
    ) {
        for (_, event) in cache_snapshot.iter() {
            if sender.send(event.clone()).await.is_err() {
                return;
            }
        }
    }

    fn handle_ephemeral_notification(payload: &str) -> Option<EphemeralOutboxEvent<P>> {
        serde_json::from_str(payload).ok()
    }

    fn process_notification(
        notification: &sqlx::postgres::PgNotification,
    ) -> Option<EphemeralOutboxEvent<P>> {
        if notification.channel() == Tables::ephemeral_outbox_events_channel() {
            Self::handle_ephemeral_notification(notification.payload())
        } else {
            None
        }
    }

    async fn spawn_cache_loop(
        pool: &sqlx::PgPool,
        _config: MailboxConfig,
        ephemeral_event_sender: broadcast::Sender<Arc<EphemeralOutboxEvent<P>>>,
        mut backfill_request: mpsc::UnboundedReceiver<mpsc::Sender<Arc<EphemeralOutboxEvent<P>>>>,
        mut cache_fill_receiver: broadcast::Receiver<Arc<EphemeralOutboxEvent<P>>>,
        _cache_fill_sender: broadcast::Sender<Arc<EphemeralOutboxEvent<P>>>,
    ) -> Result<OwnedTaskHandle, sqlx::Error> {
        let pool = pool.clone();
        let mut listener = sqlx::postgres::PgListener::connect_with(&pool).await?;
        listener
            .listen(Tables::ephemeral_outbox_events_channel())
            .await?;

        let handle = tokio::spawn(async move {
            let mut ephemeral_cache: im::HashMap<EphemeralEventType, Arc<EphemeralOutboxEvent<P>>> =
                im::HashMap::new();

            loop {
                tokio::select! {
                    biased;

                    result = backfill_request.recv() => {
                        match result {
                            Some(sender) => {
                                let cache_snapshot = ephemeral_cache.clone();
                                tokio::spawn(Self::handle_backfill_request(
                                    sender,
                                    cache_snapshot,
                                ));
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
                                ephemeral_cache = Self::insert_into_cache_and_maybe_broadcast(
                                    ephemeral_cache,
                                    event,
                                    &ephemeral_event_sender,
                                );

                                while let Ok(event) = cache_fill_receiver.try_recv() {
                                    ephemeral_cache = Self::insert_into_cache_and_maybe_broadcast(
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

                    result = listener.recv() => {
                        match result {
                            Ok(notification) => {
                                if let Some(event) = Self::process_notification(&notification) {
                                    ephemeral_cache = Self::insert_into_cache_and_maybe_broadcast(
                                        ephemeral_cache,
                                        Arc::new(event),
                                        &ephemeral_event_sender,
                                    );
                                }

                                while let Some(notification) = listener.next_buffered() {
                                    if let Some(event) = Self::process_notification(&notification) {
                                        ephemeral_cache = Self::insert_into_cache_and_maybe_broadcast(
                                            ephemeral_cache,
                                            Arc::new(event),
                                            &ephemeral_event_sender,
                                        );
                                    }
                                }
                            }
                            Err(_) => {
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
