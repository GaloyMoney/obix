use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream};

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use super::event::*;
use crate::{config::*, handle::OwnedTaskHandle, sequence::EventSequence};

pub struct CacheHandle<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    highest_known_sequence: Arc<AtomicU64>,
    persistent_event_receiver: Option<broadcast::Receiver<Arc<PersistentOutboxEvent<P>>>>,
    backfill_request:
        mpsc::UnboundedSender<(EventSequence, mpsc::Sender<Arc<PersistentOutboxEvent<P>>>)>,
    backfill_buffer_size: usize,
}

impl<P> CacheHandle<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn latest_known_persisted(&self) -> EventSequence {
        EventSequence::from(self.highest_known_sequence.load(Ordering::Relaxed))
    }

    pub fn persistent_event_stream(&mut self) -> BroadcastStream<Arc<PersistentOutboxEvent<P>>> {
        BroadcastStream::new(
            self.persistent_event_receiver
                .take()
                .expect("receiver already taken"),
        )
    }

    pub fn request_old_persistent_events(
        &self,
        start_after: EventSequence,
    ) -> ReceiverStream<Arc<PersistentOutboxEvent<P>>> {
        let (tx, rx) = mpsc::channel(self.backfill_buffer_size);
        let _ = self.backfill_request.send((start_after, tx));
        ReceiverStream::new(rx)
    }
}

#[derive(Debug)]
pub struct OutboxEventCache<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    highest_known_sequence: Arc<AtomicU64>,
    persistent_event_sender: broadcast::Sender<Arc<PersistentOutboxEvent<P>>>,
    backfill_request_send:
        mpsc::UnboundedSender<(EventSequence, mpsc::Sender<Arc<PersistentOutboxEvent<P>>>)>,
    backfill_buffer_size: usize,
    cache_fill_sender: broadcast::Sender<Arc<PersistentOutboxEvent<P>>>,
    _cache_loop_handle: OwnedTaskHandle,
    _phantom: std::marker::PhantomData<Tables>,
}

impl<P, Tables> OutboxEventCache<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
    Tables: crate::tables::MailboxTables,
{
    pub fn handle(&self) -> CacheHandle<P> {
        CacheHandle {
            highest_known_sequence: self.highest_known_sequence.clone(),
            persistent_event_receiver: Some(self.persistent_event_sender.subscribe()),
            backfill_request: self.backfill_request_send.clone(),
            backfill_buffer_size: self.backfill_buffer_size,
        }
    }

    pub fn cache_fill_sender(&self) -> broadcast::Sender<Arc<PersistentOutboxEvent<P>>> {
        self.cache_fill_sender.clone()
    }

    pub async fn init(pool: &sqlx::PgPool, config: MailboxConfig) -> Result<Self, sqlx::Error> {
        let (backfill_send, backfill_recv) = mpsc::unbounded_channel();
        let (cache_fill_send, cache_fill_recv) = broadcast::channel(config.event_buffer_size);
        let (persistent_event_sender, _) = broadcast::channel(config.event_buffer_size);
        let highest_known_sequence = Arc::new(AtomicU64::from(
            Tables::highest_known_persistent_sequence(pool).await?,
        ));
        let handle = Self::spawn_cache_loop(
            pool,
            config,
            persistent_event_sender.clone(),
            highest_known_sequence.clone(),
            backfill_recv,
            cache_fill_recv,
        )
        .await?;
        let ret = Self {
            highest_known_sequence,
            backfill_request_send: backfill_send,
            persistent_event_sender,
            backfill_buffer_size: config.event_buffer_size,
            cache_fill_sender: cache_fill_send,
            _cache_loop_handle: handle,
            _phantom: std::marker::PhantomData,
        };
        Ok(ret)
    }

    async fn spawn_cache_loop(
        pool: &sqlx::PgPool,
        config: MailboxConfig,
        persistent_event_sender: broadcast::Sender<Arc<PersistentOutboxEvent<P>>>,
        highest_known_sequence: Arc<AtomicU64>,
        mut backfill_request: mpsc::UnboundedReceiver<(
            EventSequence,
            mpsc::Sender<Arc<PersistentOutboxEvent<P>>>,
        )>,
        mut cache_fill_receiver: broadcast::Receiver<Arc<PersistentOutboxEvent<P>>>,
    ) -> Result<OwnedTaskHandle, sqlx::Error> {
        let pool = pool.clone();
        let mut listener = sqlx::postgres::PgListener::connect_with(&pool).await?;
        listener
            .listen_all([
                Tables::persistent_outbox_events_channel(),
                Tables::ephemeral_outbox_events_channel(),
            ])
            .await?;
        let mut persistent_cache: im::OrdMap<EventSequence, Arc<PersistentOutboxEvent<P>>> =
            im::OrdMap::new();

        let high_water =
            config.event_cache_size * (100 + config.event_cache_trim_percent as usize) / 100;
        let low_water =
            config.event_cache_size * (100 - config.event_cache_trim_percent as usize) / 100;

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;

                    // Highest priority - local publishes (short-circuit pg_notify)
                    result = cache_fill_receiver.recv() => {
                        match result {
                            Ok(event) => {
                                let sequence = event.sequence;
                                if persistent_cache.insert(sequence, event.clone()).is_none() {
                                    let new_highest_sequence = u64::from(sequence);
                                    highest_known_sequence.fetch_max(new_highest_sequence, Ordering::AcqRel);
                                    let _ = persistent_event_sender.send(event);
                                }
                            }
                            Err(broadcast::error::RecvError::Lagged(_)) => {
                                // Missed messages will arrive via pg_notify
                                continue;
                            }
                            Err(broadcast::error::RecvError::Closed) => {
                                break;
                            }
                        }
                    }

                    // High priority - database notifications (remote publishes)
                    result = listener.recv() => {
                        match result {
                            Ok(notification) => {
                                if notification.channel() == Tables::persistent_outbox_events_channel() {
                                    if let Ok(event) =
                                        serde_json::from_str::<PersistentOutboxEvent<P>>(notification.payload())
                                    {
                                        let sequence = event.sequence;
                                        let event = Arc::new(event);
                                        if persistent_cache.insert(sequence, event.clone()).is_none() {
                                            let new_highest_sequence = u64::from(sequence);
                                            highest_known_sequence.fetch_max(new_highest_sequence, Ordering::AcqRel);
                                            let _ = persistent_event_sender.send(event);
                                        }
                                    }
                                } else if let Ok(_event) =
                                    serde_json::from_str::<EphemeralOutboxEvent<P>>(notification.payload())
                                {
                                    unimplemented!();
                                }
                            }
                            Err(_) => {
                                break;
                            }
                        }
                    }

                    // Lower priority - backfill requests
                    result = backfill_request.recv() => {
                        match result {
                            Some((start_after, sender)) => {
                                let cache_snapshot = persistent_cache.clone();
                                tokio::spawn(async move {
                                    use std::ops::Bound;
                                    for (_, event) in cache_snapshot.range((Bound::Excluded(start_after), Bound::Unbounded)) {
                                        if sender.send(event.clone()).await.is_err() {
                                            break;
                                        }
                                    }
                                }

                                    );
                            }
                            None => {
                                break;
                            }
                        }
                    }
                }

                // Purge old entries if cache exceeds high water mark
                if persistent_cache.len() > high_water {
                    let to_remove = persistent_cache.len() - low_water;
                    if let Some((&split_key, _)) = persistent_cache.iter().nth(to_remove) {
                        let (_, right) = persistent_cache.split(&split_key);
                        persistent_cache = right;
                    }
                }
            }
        });
        Ok(OwnedTaskHandle::new(handle))
    }
}
