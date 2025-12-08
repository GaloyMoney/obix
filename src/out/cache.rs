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
            cache_fill_send.clone(),
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

    fn insert_into_cache_and_maybe_broadcast(
        cache: im::OrdMap<EventSequence, Arc<PersistentOutboxEvent<P>>>,
        event: Arc<PersistentOutboxEvent<P>>,
        highest_known_sequence: &AtomicU64,
        persistent_event_sender: &broadcast::Sender<Arc<PersistentOutboxEvent<P>>>,
        mut last_broadcast_sequence: EventSequence,
        cache_size: usize,
    ) -> (
        im::OrdMap<EventSequence, Arc<PersistentOutboxEvent<P>>>,
        EventSequence,
    ) {
        use std::ops::Bound;

        let sequence = event.sequence;
        let highest_known = highest_known_sequence.load(Ordering::Relaxed);

        // Skip events that are too old to be useful
        let threshold = highest_known.saturating_sub(cache_size as u64);
        if u64::from(sequence) <= threshold {
            return (cache, last_broadcast_sequence);
        }

        highest_known_sequence.fetch_max(u64::from(sequence), Ordering::AcqRel);
        let cache = cache.alter(|existing| existing.or(Some(event)), sequence);

        for (seq, evt) in cache.range((Bound::Excluded(last_broadcast_sequence), Bound::Unbounded))
        {
            if *seq != last_broadcast_sequence.next() {
                break;
            }
            if persistent_event_sender.send(evt.clone()).is_err() {
                break;
            }
            last_broadcast_sequence = *seq;
        }

        (cache, last_broadcast_sequence)
    }

    async fn handle_backfill_request(
        pool: sqlx::PgPool,
        start_after: EventSequence,
        sender: mpsc::Sender<Arc<PersistentOutboxEvent<P>>>,
        cache_snapshot: im::OrdMap<EventSequence, Arc<PersistentOutboxEvent<P>>>,
        cache_fill_sender: broadcast::Sender<Arc<PersistentOutboxEvent<P>>>,
        highest: EventSequence,
        buffer_size: usize,
    ) {
        use std::ops::Bound;

        let mut current_sequence = start_after;

        while current_sequence < highest {
            let next_needed = current_sequence.next();
            if cache_snapshot.contains_key(&next_needed) {
                break;
            }

            match Tables::load_next_page::<P>(&pool, current_sequence, buffer_size).await {
                Ok(events) if events.is_empty() => break,
                Ok(events) => {
                    for event in events {
                        let seq = event.sequence;
                        let event = Arc::new(event);
                        let _ = cache_fill_sender.send(event.clone());
                        if sender.send(event).await.is_err() {
                            return;
                        }
                        current_sequence = seq;
                    }
                }
                Err(_) => break,
            }
        }

        for (_, event) in
            cache_snapshot.range((Bound::Excluded(current_sequence), Bound::Unbounded))
        {
            if sender.send(event.clone()).await.is_err() {
                return;
            }
        }
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
        cache_fill_sender: broadcast::Sender<Arc<PersistentOutboxEvent<P>>>,
    ) -> Result<OwnedTaskHandle, sqlx::Error> {
        let pool = pool.clone();
        let mut listener = sqlx::postgres::PgListener::connect_with(&pool).await?;
        listener
            .listen_all([
                Tables::persistent_outbox_events_channel(),
                Tables::ephemeral_outbox_events_channel(),
            ])
            .await?;

        let cache_size = config.event_cache_size;
        let high_water = cache_size * (100 + config.event_cache_trim_percent as usize) / 100;
        let low_water = cache_size * (100 - config.event_cache_trim_percent as usize) / 100;

        let initial_sequence = EventSequence::from(highest_known_sequence.load(Ordering::Relaxed));

        let handle = tokio::spawn(async move {
            let mut persistent_cache: im::OrdMap<EventSequence, Arc<PersistentOutboxEvent<P>>> =
                im::OrdMap::new();
            let mut last_broadcast_sequence = initial_sequence;

            loop {
                tokio::select! {
                    biased;

                    result = cache_fill_receiver.recv() => {
                        match result {
                            Ok(event) => {
                                (persistent_cache, last_broadcast_sequence) =
                                    Self::insert_into_cache_and_maybe_broadcast(
                                        persistent_cache,
                                        event,
                                        &highest_known_sequence,
                                        &persistent_event_sender,
                                        last_broadcast_sequence,
                                        cache_size,
                                    );
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
                                if notification.channel() == Tables::persistent_outbox_events_channel() {
                                    if let Ok(event) =
                                        serde_json::from_str::<PersistentOutboxEvent<P>>(notification.payload())
                                    {
                                        (persistent_cache, last_broadcast_sequence) =
                                            Self::insert_into_cache_and_maybe_broadcast(
                                                persistent_cache,
                                                Arc::new(event),
                                                &highest_known_sequence,
                                                &persistent_event_sender,
                                                last_broadcast_sequence,
                                                cache_size,
                                            );
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

                    result = backfill_request.recv() => {
                        match result {
                            Some((start_after, sender)) => {
                                let cache_snapshot = persistent_cache.clone();
                                let highest = EventSequence::from(
                                    highest_known_sequence.load(Ordering::Relaxed)
                                );

                                tokio::spawn(Self::handle_backfill_request(
                                    pool.clone(),
                                    start_after,
                                    sender,
                                    cache_snapshot,
                                    cache_fill_sender.clone(),
                                    highest,
                                    cache_size,
                                ));
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
