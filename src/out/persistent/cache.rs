use serde::{Serialize, de::DeserializeOwned};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream};

use crate::{
    config::*,
    handle::{OwnedTaskHandle, spawn_supervised},
    out::{event::*, pg_notify::NotifyMessage},
    sequence::EventSequence,
};

pub struct CacheHandle<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    highest_known_sequence: Arc<AtomicU64>,
    persistent_event_receiver: Option<broadcast::Receiver<PersistentDelivery<P>>>,
    backfill_request: mpsc::UnboundedSender<(EventSequence, mpsc::Sender<PersistentDelivery<P>>)>,
    backfill_buffer_size: usize,
}

impl<P> CacheHandle<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn latest_known_persisted(&self) -> EventSequence {
        EventSequence::from(self.highest_known_sequence.load(Ordering::Relaxed))
    }

    pub fn persistent_event_stream(&mut self) -> BroadcastStream<PersistentDelivery<P>> {
        BroadcastStream::new(
            self.persistent_event_receiver
                .take()
                .expect("receiver already taken"),
        )
    }

    pub fn request_old_persistent_events(
        &self,
        start_after: EventSequence,
    ) -> ReceiverStream<PersistentDelivery<P>> {
        let (tx, rx) = mpsc::channel(self.backfill_buffer_size);
        let _ = self.backfill_request.send((start_after, tx));
        ReceiverStream::new(rx)
    }
}

#[derive(Debug)]
pub struct PersistentOutboxEventCache<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    highest_known_sequence: Arc<AtomicU64>,
    persistent_event_sender: broadcast::Sender<PersistentDelivery<P>>,
    backfill_request_send:
        mpsc::UnboundedSender<(EventSequence, mpsc::Sender<PersistentDelivery<P>>)>,
    backfill_buffer_size: usize,
    cache_fill_sender: broadcast::Sender<PersistentDelivery<P>>,
    _cache_loop_handle: OwnedTaskHandle,
    _phantom: std::marker::PhantomData<Tables>,
}

impl<P, Tables> PersistentOutboxEventCache<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
    Tables: crate::tables::MailboxTables,
{
    const RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

    pub fn handle(&self) -> CacheHandle<P> {
        CacheHandle {
            highest_known_sequence: self.highest_known_sequence.clone(),
            persistent_event_receiver: Some(self.persistent_event_sender.subscribe()),
            backfill_request: self.backfill_request_send.clone(),
            backfill_buffer_size: self.backfill_buffer_size,
        }
    }

    pub fn cache_fill_sender(&self) -> broadcast::Sender<PersistentDelivery<P>> {
        self.cache_fill_sender.clone()
    }

    pub async fn init(
        pool: &sqlx::PgPool,
        config: &MailboxConfig,
        persistent_notification_rx: mpsc::Receiver<NotifyMessage>,
    ) -> Result<Self, sqlx::Error> {
        let (backfill_send, backfill_recv) = mpsc::unbounded_channel();
        let (cache_fill_send, cache_fill_recv) = broadcast::channel(config.event_buffer_size);
        let (persistent_event_sender, _) = broadcast::channel(config.event_buffer_size);
        let highest_known_sequence = Arc::new(AtomicU64::from(
            Tables::highest_known_persistent_sequence(pool).await?,
        ));
        let cache_loop_handle = Self::spawn_cache_loop(
            pool,
            config,
            persistent_event_sender.clone(),
            highest_known_sequence.clone(),
            backfill_recv,
            cache_fill_recv,
            cache_fill_send.clone(),
            persistent_notification_rx,
        );
        Ok(Self {
            highest_known_sequence,
            persistent_event_sender,
            backfill_request_send: backfill_send,
            backfill_buffer_size: config.backfill_page_size.max(1),
            cache_fill_sender: cache_fill_send,
            _cache_loop_handle: cache_loop_handle,
            _phantom: std::marker::PhantomData,
        })
    }

    /// A lagging reader consumes the same committed stream as live listeners.
    /// Missing cache entries trigger bounded, read-only page loads. Rollback
    /// cannot leave database holes, and readers never manufacture placeholders.
    async fn handle_backfill_request(
        pool: sqlx::PgPool,
        mut after: EventSequence,
        sender: mpsc::Sender<PersistentDelivery<P>>,
        cache: im::OrdMap<EventSequence, PersistentDelivery<P>>,
        highest: EventSequence,
        page_size: usize,
    ) {
        while after < highest {
            if sender.is_closed() {
                return;
            }
            if let Some(event) = cache.get(&after.next()) {
                if sender.send(event.clone()).await.is_err() {
                    return;
                }
                after = after.next();
                continue;
            }
            match sender.reserve().await {
                Ok(permit) => drop(permit),
                Err(_) => return,
            }
            let events = match Tables::load_next_page::<P>(&pool, after, page_size).await {
                Ok(events) => events,
                Err(error) => {
                    record_read_failed(&error, u64::from(after));
                    tokio::time::sleep(Self::RETRY_INTERVAL).await;
                    continue;
                }
            };
            let before = after;
            for event in events {
                let delivery = PersistentDelivery::from(event);
                if delivery.sequence() != after.next() || delivery.sequence() > highest {
                    break;
                }
                if sender.send(delivery).await.is_err() {
                    return;
                }
                after = after.next();
            }
            if before == after {
                // Fail closed on corruption or an unavailable retained prefix.
                // Do not turn missing business events into successful progress.
                tokio::select! {
                    _ = sender.closed() => return,
                    _ = tokio::time::sleep(Self::RETRY_INTERVAL) => {}
                }
            }
        }
    }

    fn insert_and_broadcast(
        cache: &mut im::OrdMap<EventSequence, PersistentDelivery<P>>,
        event: PersistentDelivery<P>,
        highest: &AtomicU64,
        sender: &broadcast::Sender<PersistentDelivery<P>>,
        last: &mut EventSequence,
        cache_size: usize,
    ) {
        let sequence = event.sequence();
        let threshold = highest
            .load(Ordering::Relaxed)
            .saturating_sub(cache_size as u64)
            .min(u64::from(*last));
        if u64::from(sequence) <= threshold {
            return;
        }
        highest.fetch_max(u64::from(sequence), Ordering::AcqRel);
        if !cache.contains_key(&sequence) {
            cache.insert(sequence, event);
        }
        while let Some(event) = cache.get(&last.next()) {
            let _ = sender.send(event.clone());
            *last = last.next();
        }
    }

    async fn confirmed_head(pool: &sqlx::PgPool) -> Option<EventSequence> {
        match Tables::highest_known_persistent_sequence(pool).await {
            Ok(head) => Some(head),
            Err(error) => {
                record_read_failed(&error, 0);
                None
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_cache_loop(
        pool: &sqlx::PgPool,
        config: &MailboxConfig,
        persistent_event_sender: broadcast::Sender<PersistentDelivery<P>>,
        highest_known_sequence: Arc<AtomicU64>,
        mut backfill_request: mpsc::UnboundedReceiver<(
            EventSequence,
            mpsc::Sender<PersistentDelivery<P>>,
        )>,
        mut cache_fill_receiver: broadcast::Receiver<PersistentDelivery<P>>,
        cache_fill_sender: broadcast::Sender<PersistentDelivery<P>>,
        mut notification_receiver: mpsc::Receiver<NotifyMessage>,
    ) -> OwnedTaskHandle {
        let pool = pool.clone();
        let cache_size = config.event_cache_size;
        let page_size = config.backfill_page_size.max(1);
        let high_water = cache_size * (100 + config.event_cache_trim_percent as usize) / 100;
        let low_water = cache_size * (100 - config.event_cache_trim_percent as usize) / 100;
        let idle_resync = config.idle_resync_interval;
        let initial = EventSequence::from(highest_known_sequence.load(Ordering::Relaxed));
        let handle = spawn_supervised("obix::persistent_cache_loop", async move {
            let mut cache = im::OrdMap::new();
            let mut last = initial;
            let mut last_progress_at = tokio::time::Instant::now();
            let mut retry_at = tokio::time::Instant::now();
            loop {
                let recovery_needed =
                    u64::from(last) < highest_known_sequence.load(Ordering::Relaxed);
                tokio::select! {
                    request = backfill_request.recv() => {
                        let Some((after, sender)) = request else { break; };
                        tokio::spawn(Self::handle_backfill_request(
                            pool.clone(), after, sender, cache.clone(),
                            EventSequence::from(highest_known_sequence.load(Ordering::Relaxed)), page_size,
                        ));
                    }
                    event = cache_fill_receiver.recv() => {
                        match event {
                            Ok(event) => {
                                Self::insert_and_broadcast(&mut cache, event, &highest_known_sequence, &persistent_event_sender, &mut last, cache_size);
                                while let Ok(event) = cache_fill_receiver.try_recv() {
                                    Self::insert_and_broadcast(&mut cache, event, &highest_known_sequence, &persistent_event_sender, &mut last, cache_size);
                                }
                                last_progress_at = tokio::time::Instant::now();
                            }
                            Err(broadcast::error::RecvError::Lagged(_)) => { retry_at = tokio::time::Instant::now(); }
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }
                    message = notification_receiver.recv() => {
                        let Some(message) = message else { break; };
                        #[derive(serde::Deserialize)]
                        struct Header { max_sequence: EventSequence }
                        let needs_resync = match message {
                            NotifyMessage::Resync => true,
                            NotifyMessage::Notification(notification) => serde_json::from_str::<Header>(notification.payload())
                                .is_ok_and(|header| u64::from(header.max_sequence) > highest_known_sequence.load(Ordering::Relaxed)),
                        };
                        // NOTIFY is unauthenticated. Only a database head read
                        // may advance the cursor, never the claimed range.
                        if needs_resync && let Some(head) = Self::confirmed_head(&pool).await {
                            highest_known_sequence.fetch_max(u64::from(head), Ordering::AcqRel);
                            last_progress_at = tokio::time::Instant::now();
                            retry_at = tokio::time::Instant::now();
                        }
                    }
                    _ = tokio::time::sleep_until(last_progress_at + idle_resync) => {
                        if let Some(head) = Self::confirmed_head(&pool).await {
                            highest_known_sequence.fetch_max(u64::from(head), Ordering::AcqRel);
                        }
                        last_progress_at = tokio::time::Instant::now();
                        retry_at = tokio::time::Instant::now();
                    }
                    _ = tokio::time::sleep_until(retry_at), if recovery_needed => {
                        let before = last;
                        match Tables::load_next_page::<P>(&pool, last, page_size).await {
                            Ok(events) => {
                                for event in events {
                                    Self::insert_and_broadcast(&mut cache, PersistentDelivery::from(event), &highest_known_sequence, &persistent_event_sender, &mut last, cache_size);
                                }
                            }
                            Err(error) => record_read_failed(&error, u64::from(last)),
                        }
                        retry_at = tokio::time::Instant::now() + if last == before { Self::RETRY_INTERVAL } else { std::time::Duration::ZERO };
                    }
                }
                if cache.len() > high_water {
                    let to_remove = cache.len() - low_water;
                    if let Some((&split_key, _)) = cache.iter().nth(to_remove) {
                        let (_, right) = cache.split(&split_key);
                        cache = right;
                    }
                }
            }
            drop(cache_fill_sender);
        });
        OwnedTaskHandle::new(handle)
    }
}

#[tracing::instrument(name = "obix.persistent_cache.read_failed", level = "warn", skip_all,
    fields(error = %error, sequence = sequence))]
fn record_read_failed(error: &sqlx::Error, sequence: u64) {}
