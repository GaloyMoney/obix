use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{broadcast, mpsc};

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use super::event::*;
use crate::{config::*, handle::OwnedTaskHandle, sequence::EventSequence};

pub struct CacheHandle<P> {
    _phantom: std::marker::PhantomData<P>,
}

impl<P> CacheHandle<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn request_old_persistent_events(
        &self,
        start_after: EventSequence,
    ) -> mpsc::Receiver<Arc<PersistentOutboxEvent<P>>> {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct OutboxEventCache<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    persistent_event_sender: broadcast::Sender<Arc<PersistentOutboxEvent<P>>>,
    cache_loop_handle: OwnedTaskHandle,
    _phantom: std::marker::PhantomData<Tables>,
}

impl<P, Tables> OutboxEventCache<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
    Tables: crate::tables::MailboxTables,
{
    pub async fn init(
        pool: &sqlx::PgPool,
        config: MailboxConfig,
        persistent_event_sender: broadcast::Sender<Arc<PersistentOutboxEvent<P>>>,
        highest_known_sequence: Arc<AtomicU64>,
    ) -> Result<Self, sqlx::Error> {
        let handle = Self::spawn_cache_loop(
            pool,
            config,
            persistent_event_sender.clone(),
            highest_known_sequence,
        )
        .await?;
        let ret = Self {
            persistent_event_sender,
            cache_loop_handle: handle,
            _phantom: std::marker::PhantomData,
        };
        Ok(ret)
    }

    async fn spawn_cache_loop(
        pool: &sqlx::PgPool,
        config: MailboxConfig,
        persistent_event_sender: broadcast::Sender<Arc<PersistentOutboxEvent<P>>>,
        highest_known_sequence: Arc<AtomicU64>,
    ) -> Result<OwnedTaskHandle, sqlx::Error> {
        let mut listener = sqlx::postgres::PgListener::connect_with(pool).await?;
        listener
            .listen_all([
                Tables::persistent_outbox_events_channel(),
                Tables::ephemeral_outbox_events_channel(),
            ])
            .await?;
        let mut persistent_cache: im::OrdMap<EventSequence, Arc<PersistentOutboxEvent<P>>> =
            im::OrdMap::new();
        let mut persistent_event_receiver = persistent_event_sender.subscribe();

        let high_water =
            config.event_cache_size * (100 + config.event_cache_trim_percent as usize) / 100;
        let low_water =
            config.event_cache_size * (100 - config.event_cache_trim_percent as usize) / 100;

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
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
                    result = persistent_event_receiver.recv() => {
                        match result {
                            Ok(event) => {
                                let sequence = event.sequence;
                                persistent_cache.insert(sequence, event);
                            }
                            Err(broadcast::error::RecvError::Closed) => {
                                break;
                            }
                            Err(broadcast::error::RecvError::Lagged(_)) => {
                                continue;
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
