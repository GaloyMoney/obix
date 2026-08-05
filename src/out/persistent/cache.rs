use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream};

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use crate::{
    archive::ArchiveReader,
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

/// Outcome of parsing a `{min_sequence, max_sequence}` notification.
struct NotifiedRange {
    /// Highest sequence the notification proves committed.
    max_sequence: EventSequence,
    /// Sub-range of the notified sequences missing from the cache as
    /// `(after, up_to)` — exclusive lower bound, inclusive upper — or
    /// `None` when every notified sequence is already cached (the warm
    /// in-process path: the post-commit broadcast beat the NOTIFY).
    missing: Option<(EventSequence, EventSequence)>,
}

/// Result of the per-page archive-watermark (re-)check during backfill.
enum ArchiveStep {
    /// End this backfill; the listener re-requests one.
    Done,
    /// Continue the pg walk from `sequence`. `degraded_below` bounds a
    /// sub-range that must be served SELECT-only rather than via
    /// [`Tables::load_next_page`]: sequences at or below an archive
    /// watermark may have been pruned (their partition dropped), and
    /// `load_next_page` gap-fills by inserting placeholder rows — which
    /// below the watermark would write (and deliver) placeholders for
    /// archived events into the live table. `Some(w)` means: while
    /// `sequence < w`, walk SELECT-only.
    WalkFrom {
        sequence: EventSequence,
        degraded_below: Option<EventSequence>,
    },
}

/// Shared dependencies handed to each backfill task.
#[derive(Clone)]
struct BackfillDeps<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pool: sqlx::PgPool,
    cache_fill_sender: broadcast::Sender<PersistentDelivery<P>>,
    buffer_size: usize,
    archive_reader: Option<ArchiveReader>,
}

/// Bookkeeping for the sequence the broadcast cursor is stalled on.
struct GapFillState {
    /// Grace period before the first fill (`MailboxConfig::gap_fill_grace`).
    grace: std::time::Duration,
    /// The sequence the broadcast cursor is stalled on. The state is
    /// discarded when the cursor advances past it.
    stalled_on: EventSequence,
    /// When the stall was first observed. The first fill is withheld until
    /// `grace` has elapsed since this instant.
    first_seen: tokio::time::Instant,
    last_attempt: Option<tokio::time::Instant>,
}

impl GapFillState {
    /// How long after a gap-fill attempt the same stalled sequence is
    /// retried (the attempt itself may have raced a still-uncommitted row).
    const REFILL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

    fn new(grace: std::time::Duration, stalled_on: EventSequence) -> Self {
        Self {
            grace,
            stalled_on,
            first_seen: tokio::time::Instant::now(),
            last_attempt: None,
        }
    }

    /// Whether this state tracks the sequence the broadcast cursor is
    /// currently stalled on.
    fn is_up_to_date(&self, stalled_on: EventSequence) -> bool {
        self.stalled_on == stalled_on
    }

    /// When the next gap-fill action falls due: `grace` after the stall was
    /// first observed, or [`Self::REFILL_INTERVAL`] after the last attempt.
    fn next_fill_due(&self) -> tokio::time::Instant {
        match self.last_attempt {
            None => self.first_seen + self.grace,
            Some(attempted) => attempted + Self::REFILL_INTERVAL,
        }
    }

    fn fill_is_due(&self) -> bool {
        self.next_fill_due() <= tokio::time::Instant::now()
    }

    fn record_attempt(&mut self) {
        self.last_attempt = Some(tokio::time::Instant::now());
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
        archive_reader: Option<ArchiveReader>,
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
            archive_reader.clone(),
        )
        .await?;

        let ret = Self {
            highest_known_sequence,
            backfill_request_send: backfill_send,
            persistent_event_sender,
            backfill_buffer_size: config.event_buffer_size,
            cache_fill_sender: cache_fill_send,
            _cache_loop_handle: cache_loop_handle,
            _phantom: std::marker::PhantomData,
        };
        Ok(ret)
    }

    fn insert_into_cache_and_maybe_broadcast(
        cache: im::OrdMap<EventSequence, PersistentDelivery<P>>,
        event: PersistentDelivery<P>,
        highest_known_sequence: &AtomicU64,
        persistent_event_sender: &broadcast::Sender<PersistentDelivery<P>>,
        mut last_broadcast_sequence: EventSequence,
        cache_size: usize,
    ) -> (
        im::OrdMap<EventSequence, PersistentDelivery<P>>,
        EventSequence,
    ) {
        use std::ops::Bound;

        let sequence = event.sequence();
        let highest_known = highest_known_sequence.load(Ordering::Relaxed);

        // Skip events that are too old to be useful, but never let the
        // threshold move past the broadcast cursor — events still required
        // for the contiguity loop to advance (sequence > last_broadcast_sequence)
        // must always reach the cache. Without this clamp, a burst that
        // advances `highest_known` ahead of `last_broadcast_sequence`
        // silently drops the events between them and permanently breaks
        // broadcast (see lana-bank#5035).
        let threshold = highest_known
            .saturating_sub(cache_size as u64)
            .min(u64::from(last_broadcast_sequence));
        if u64::from(sequence) <= threshold {
            return (cache, last_broadcast_sequence);
        }

        highest_known_sequence.fetch_max(u64::from(sequence), Ordering::AcqRel);
        let cache = cache.alter(|existing| existing.or(Some(event)), sequence);

        for (seq, evt) in cache.range((Bound::Excluded(last_broadcast_sequence), Bound::Unbounded))
        {
            if *seq != last_broadcast_sequence.next() {
                record_sequence_gap(
                    u64::from(last_broadcast_sequence),
                    u64::from(*seq),
                    highest_known_sequence.load(Ordering::Relaxed),
                );
                break;
            }
            if persistent_event_sender.send(evt.clone()).is_err() {
                record_no_receivers(u64::from(*seq));
                break;
            }
            last_broadcast_sequence = *seq;
        }

        (cache, last_broadcast_sequence)
    }

    /// Serve archived history `(current_sequence, watermark]` from object
    /// storage, refreshing the watermark on every call. A no-op when the
    /// watermark has not passed `current_sequence`. Returns where the pg
    /// walk should resume, and whether it must degrade to SELECT-only
    /// (`degraded_below`: the walk may not use `load_next_page`, which
    /// gap-fills by inserting placeholder rows — below the watermark
    /// those would be written for archived sequences).
    ///
    /// `walk_upper_bound` bounds the SELECT-only walk when the watermark
    /// itself is unknown (lookup failure): we cannot tell where the
    /// archived range ends, so the whole walk must stay SELECT-only.
    async fn serve_archived_up_to_watermark(
        pool: &sqlx::PgPool,
        archive_reader: &Option<ArchiveReader>,
        current_sequence: EventSequence,
        walk_upper_bound: EventSequence,
        sender: &mpsc::Sender<PersistentDelivery<P>>,
    ) -> ArchiveStep {
        let watermark = match Tables::archive_watermark(pool).await {
            Ok(watermark) => watermark,
            // Fail-open but never gap-fill: without the watermark we
            // cannot tell where the archived range ends, so a
            // `load_next_page` below it would INSERT placeholder rows
            // for possibly-pruned sequences. Bound the SELECT-only walk
            // at the sequence head — the loop ends there anyway.
            Err(error) => {
                record_archive_watermark_failed(&error);
                return ArchiveStep::WalkFrom {
                    sequence: current_sequence,
                    degraded_below: Some(walk_upper_bound),
                };
            }
        };
        let Some(watermark) = watermark.filter(|w| current_sequence < *w) else {
            return ArchiveStep::WalkFrom {
                sequence: current_sequence,
                degraded_below: None,
            };
        };
        match archive_reader {
            Some(reader) => {
                match reader
                    .stream_archived::<P, Tables>(pool, current_sequence, watermark, sender)
                    .await
                {
                    Ok(last_sent) if last_sent < watermark => {
                        // The archive stream ended early — only possible
                        // when the listener dropped mid-stream (a send
                        // failed) or the chunks could not be fully
                        // served. Resuming the pg walk from here would
                        // be below the watermark: `load_next_page` would
                        // gap-fill INSERT placeholders for archived
                        // (possibly partition-dropped) sequences. Keep
                        // the continuation SELECT-only instead.
                        ArchiveStep::WalkFrom {
                            sequence: last_sent,
                            degraded_below: Some(watermark),
                        }
                    }
                    Ok(last_sent) => ArchiveStep::WalkFrom {
                        sequence: last_sent,
                        degraded_below: None,
                    },
                    Err(error) => {
                        record_archive_backfill_failed(&error, u64::from(current_sequence));
                        // The listener re-requests backfill as soon as
                        // this one ends; delay so a storage outage
                        // isn't a hot retry loop.
                        tokio::time::sleep(ARCHIVE_BACKFILL_RETRY_DELAY).await;
                        ArchiveStep::Done
                    }
                }
            }
            None => {
                // Archiving ran on this deployment but this outbox has
                // no archive configured: the archived range will come
                // back as placeholders from a SELECT-only pg walk.
                // Loud, not silent.
                record_archive_reader_missing(u64::from(current_sequence), u64::from(watermark));
                ArchiveStep::WalkFrom {
                    sequence: current_sequence,
                    degraded_below: Some(watermark),
                }
            }
        }
    }

    fn placeholder_delivery(sequence: EventSequence) -> PersistentDelivery<P> {
        PersistentDelivery::from(Ok::<_, UndecodableEventError>(PersistentOutboxEvent {
            id: OutboxEventId::new(),
            sequence,
            payload: None,
            tracing_context: None,
            recorded_at: chrono::Utc::now(),
        }))
    }

    async fn fill_gap(
        pool: sqlx::PgPool,
        from_sequence: EventSequence,
        cache_fill_sender: broadcast::Sender<PersistentDelivery<P>>,
        buffer_size: usize,
    ) {
        if let Ok(events) = Tables::load_next_page::<P>(&pool, from_sequence, buffer_size).await {
            for item in events {
                let _ = cache_fill_sender.send(PersistentDelivery::from(item));
            }
        }
    }

    async fn handle_backfill_request(
        deps: BackfillDeps<P>,
        start_after: EventSequence,
        sender: mpsc::Sender<PersistentDelivery<P>>,
        cache_snapshot: im::OrdMap<EventSequence, PersistentDelivery<P>>,
        highest: EventSequence,
    ) {
        use std::ops::Bound;

        let BackfillDeps {
            pool,
            cache_fill_sender,
            buffer_size,
            archive_reader,
        } = deps;
        let mut current_sequence = start_after;

        // Archived history below the watermark no longer exists in
        // postgres — a pg load for that range would synthesize
        // placeholder rows and mask the archived events as rolled-back
        // transactions. The watermark is therefore checked before every
        // page — including the first — and sub-watermark history is
        // served from the archive. The per-page re-check matters: a
        // concurrent archiver run may prune rows this walk was about to
        // read, and a one-time check cannot see that.
        loop {
            // `degraded_below`: sequences below this bound must be
            // served SELECT-only — `load_next_page` would INSERT
            // placeholder rows for pruned sequences into the live table.
            // Set when the archive reader is missing (misconfigured
            // deployment), the archive stream ended early below the
            // watermark (listener drop), or the watermark lookup failed
            // (the whole walk degrades).
            let select_only_below = match Self::serve_archived_up_to_watermark(
                &pool,
                &archive_reader,
                current_sequence,
                highest,
                &sender,
            )
            .await
            {
                ArchiveStep::Done => return,
                ArchiveStep::WalkFrom {
                    sequence,
                    degraded_below,
                } => {
                    current_sequence = sequence;
                    degraded_below
                }
            };
            if current_sequence >= highest {
                break;
            }
            let next_needed = current_sequence.next();
            if cache_snapshot.contains_key(&next_needed) {
                break;
            }

            if let Some(watermark) = select_only_below.filter(|w| current_sequence < *w) {
                // Degraded sub-watermark walk (no archive reader
                // configured): SELECT-only, placeholder deliveries are
                // synthesized in memory — nothing is written back.
                let window_end = watermark.min(EventSequence::from(
                    u64::from(current_sequence) + buffer_size as u64,
                ));
                match Tables::load_events_in_range::<P>(&pool, current_sequence, window_end).await {
                    Ok(events) => {
                        let mut expected = current_sequence.next();
                        for item in events {
                            let delivery = PersistentDelivery::from(item);
                            let seq = delivery.sequence();
                            while expected < seq {
                                let placeholder = Self::placeholder_delivery(expected);
                                let _ = cache_fill_sender.send(placeholder.clone());
                                if sender.send(placeholder).await.is_err() {
                                    return;
                                }
                                expected = expected.next();
                            }
                            let _ = cache_fill_sender.send(delivery.clone());
                            if sender.send(delivery).await.is_err() {
                                return;
                            }
                            expected = seq.next();
                        }
                        // Sequences absent below the watermark are
                        // pruned (archived) rows — placeholders.
                        while expected <= window_end {
                            let placeholder = Self::placeholder_delivery(expected);
                            let _ = cache_fill_sender.send(placeholder.clone());
                            if sender.send(placeholder).await.is_err() {
                                return;
                            }
                            expected = expected.next();
                        }
                        current_sequence = window_end;
                    }
                    Err(e) => {
                        record_backfill_failed(&e, u64::from(current_sequence));
                        break;
                    }
                }
                continue;
            }

            match Tables::load_next_page::<P>(&pool, current_sequence, buffer_size).await {
                Ok(events) if events.is_empty() => break,
                Ok(events) => {
                    for item in events {
                        let delivery = PersistentDelivery::from(item);
                        let seq = delivery.sequence();
                        let _ = cache_fill_sender.send(delivery.clone());
                        if sender.send(delivery).await.is_err() {
                            return;
                        }
                        current_sequence = seq;
                    }
                }
                Err(e) => {
                    record_backfill_failed(&e, u64::from(current_sequence));
                    break;
                }
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

    /// Fetch the notified-but-uncached range with a SELECT-only scan. Never
    /// writes placeholders: sequences absent from the result belong to
    /// transactions that were still in flight when the notification was
    /// sent and remain the grace-period gap fill's responsibility.
    async fn fetch_notified_range(
        pool: sqlx::PgPool,
        after: EventSequence,
        up_to: EventSequence,
        cache_fill_sender: broadcast::Sender<PersistentDelivery<P>>,
    ) {
        if let Ok(events) = Tables::load_events_in_range::<P>(&pool, after, up_to).await {
            for item in events {
                let _ = cache_fill_sender.send(PersistentDelivery::from(item));
            }
        }
    }

    /// Parse a `{min_sequence, max_sequence}` notification (emitted once per
    /// insert statement by the publisher) and decide what must be fetched.
    /// Returns `None` for unparsable payloads.
    fn handle_notification(
        payload: &str,
        cache: &im::OrdMap<EventSequence, PersistentDelivery<P>>,
    ) -> Option<NotifiedRange> {
        #[derive(serde::Deserialize)]
        struct NotificationHeader {
            min_sequence: EventSequence,
            max_sequence: EventSequence,
        }

        let header: NotificationHeader = serde_json::from_str(payload).ok()?;

        let mut missing_sequences = (u64::from(header.min_sequence)
            ..=u64::from(header.max_sequence))
            .map(EventSequence::from)
            .filter(|sequence| !cache.contains_key(sequence));

        let missing = missing_sequences.next().map(|first| {
            let last = missing_sequences.next_back().unwrap_or(first);
            (
                EventSequence::from(u64::from(first).saturating_sub(1)),
                last,
            )
        });

        Some(NotifiedRange {
            max_sequence: header.max_sequence,
            missing,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn spawn_cache_loop(
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
        archive_reader: Option<ArchiveReader>,
    ) -> Result<OwnedTaskHandle, sqlx::Error> {
        let pool = pool.clone();

        let cache_size = config.event_cache_size;
        let high_water = cache_size * (100 + config.event_cache_trim_percent as usize) / 100;
        let low_water = cache_size * (100 - config.event_cache_trim_percent as usize) / 100;
        let gap_fill_grace = config.gap_fill_grace;

        let initial_sequence = EventSequence::from(highest_known_sequence.load(Ordering::Relaxed));

        let handle = spawn_supervised("obix::persistent_cache_loop", async move {
            let mut persistent_cache: im::OrdMap<EventSequence, PersistentDelivery<P>> =
                im::OrdMap::new();
            let mut last_broadcast_sequence = initial_sequence;
            // Bookkeeping for the sequence the broadcast is stalled on.
            // Reset whenever the broadcast cursor advances.
            let mut gap_state: Option<GapFillState> = None;

            loop {
                // Wake up exactly when the next gap-fill action falls due,
                // so a stalled broadcast is filled even when no further
                // events or notifications arrive.
                let gap_fill_at = gap_state.as_ref().map(GapFillState::next_fill_due);

                tokio::select! {
                    biased;

                    result = backfill_request.recv() => {
                        match result {
                            Some((start_after, sender)) => {
                                let cache_snapshot = persistent_cache.clone();
                                let highest = EventSequence::from(
                                    highest_known_sequence.load(Ordering::Relaxed)
                                );

                                tokio::spawn(Self::handle_backfill_request(
                                    BackfillDeps {
                                        pool: pool.clone(),
                                        cache_fill_sender: cache_fill_sender.clone(),
                                        buffer_size: cache_size,
                                        archive_reader: archive_reader.clone(),
                                    },
                                    start_after,
                                    sender,
                                    cache_snapshot,
                                    highest,
                                ));
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
                                (persistent_cache, last_broadcast_sequence) =
                                    Self::insert_into_cache_and_maybe_broadcast(
                                        persistent_cache,
                                        event,
                                        &highest_known_sequence,
                                        &persistent_event_sender,
                                        last_broadcast_sequence,
                                        cache_size,
                                    );

                                while let Ok(event) = cache_fill_receiver.try_recv() {
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
                            }
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                record_cache_fill_lagged(
                                    n,
                                    u64::from(last_broadcast_sequence),
                                    highest_known_sequence.load(Ordering::Relaxed),
                                );
                                continue;
                            }
                            Err(broadcast::error::RecvError::Closed) => {
                                record_cache_fill_closed();
                                break;
                            }
                        }
                    }

                    result = notification_receiver.recv() => {
                        match result {
                            Some(message) => {
                                let mut resync_needed = false;
                                let mut fetch_range: Option<(EventSequence, EventSequence)> = None;
                                let mut claimed_head: Option<EventSequence> = None;
                                let mut messages = vec![message];
                                while let Ok(message) = notification_receiver.try_recv() {
                                    messages.push(message);
                                }
                                for message in messages {
                                    match message {
                                        NotifyMessage::Notification(notification) => {
                                            if let Some(notified) = Self::handle_notification(
                                                notification.payload(),
                                                &persistent_cache,
                                            ) {
                                                // NOT applied to
                                                // highest_known_sequence yet —
                                                // see the clamp below.
                                                claimed_head = Some(match claimed_head {
                                                    Some(max) => max.max(notified.max_sequence),
                                                    None => notified.max_sequence,
                                                });
                                                if let Some((after, up_to)) = notified.missing {
                                                    fetch_range = Some(match fetch_range {
                                                        Some((lo, hi)) => {
                                                            (lo.min(after), hi.max(up_to))
                                                        }
                                                        None => (after, up_to),
                                                    });
                                                }
                                            }
                                        }
                                        NotifyMessage::Resync => {
                                            resync_needed = true;
                                        }
                                    }
                                }

                                // A NOTIFY payload is unauthenticated: any role able to
                                // connect to this database can signal any channel. A
                                // forged {min, max} claiming a huge max_sequence must
                                // neither advance highest_known_sequence to a phantom
                                // value (which would pin the gap-fill loop below,
                                // grinding a fill query every second forever) NOR drive
                                // an unbounded range scan — the same forged
                                // {min:1, max:i64::MAX} would otherwise spawn a
                                // fetch_notified_range(0, i64::MAX) streaming the
                                // entire table tail through cache_fill on every forgery.
                                //
                                // Both the head advance and the fetch up_to are clamped
                                // to the sequence's authoritative last_value, which is
                                // >= any legitimately notified sequence (so free for
                                // real notifications, protective for forged ones).
                                //
                                // last_value advances at nextval (pre-commit), so a
                                // forged claim inside (committed_head, last_value]
                                // still passes the clamp and can trigger a grace-period
                                // gap-fill against in-flight sequences — but that work
                                // is bounded by last_value and self-heals via the
                                // ON CONFLICT speculative-insert block, so it never
                                // stalls. On a transient head-read failure the fetch is
                                // skipped (rather than fired unclamped); a subsequent
                                // notification or resync retries.
                                let current_head =
                                    highest_known_sequence.load(Ordering::Relaxed);
                                let claim_advances = claimed_head
                                    .is_some_and(|claimed| u64::from(claimed) > current_head);
                                if claim_advances || resync_needed || fetch_range.is_some() {
                                    match Tables::highest_known_persistent_sequence(&pool).await {
                                        Ok(head) => {
                                            let confirmed_head = claimed_head
                                                .map(|claimed| {
                                                    EventSequence::from(
                                                        u64::from(claimed)
                                                            .min(u64::from(head)),
                                                    )
                                                })
                                                .unwrap_or(head);
                                            highest_known_sequence.fetch_max(
                                                u64::from(confirmed_head),
                                                Ordering::AcqRel,
                                            );
                                            if let Some((after, up_to)) = fetch_range {
                                                let clamped_up_to = EventSequence::from(
                                                    u64::from(up_to)
                                                        .min(u64::from(confirmed_head)),
                                                );
                                                if u64::from(after)
                                                    < u64::from(clamped_up_to)
                                                {
                                                    tokio::spawn(Self::fetch_notified_range(
                                                        pool.clone(),
                                                        after,
                                                        clamped_up_to,
                                                        cache_fill_sender.clone(),
                                                    ));
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

                    _ = async {
                        match gap_fill_at {
                            Some(deadline) => tokio::time::sleep_until(deadline).await,
                            None => std::future::pending::<()>().await,
                        }
                    } => {}
                }

                // Proactive gap fill: if broadcasting is stuck waiting for a missing
                // sequence, trigger load_next_page which will fill the gap via
                // fill_gaps_query. The first fill is delayed by `gap_fill_grace`:
                // most gaps are transactions that hold a sequence but have not
                // committed yet, and the gap-fill upsert blocks on exactly those
                // in-flight rows (ON CONFLICT speculative insertion) until they
                // commit — filling immediately pays a blocked connection per gap
                // for what a brief wait resolves without any DB work.
                let next_needed = last_broadcast_sequence.next();
                let highest = highest_known_sequence.load(Ordering::Relaxed);
                if u64::from(next_needed) <= highest && !persistent_cache.contains_key(&next_needed)
                {
                    if gap_state
                        .as_ref()
                        .is_some_and(|state| !state.is_up_to_date(last_broadcast_sequence))
                    {
                        gap_state = None;
                    }
                    let state = gap_state.get_or_insert_with(|| {
                        GapFillState::new(gap_fill_grace, last_broadcast_sequence)
                    });
                    if state.fill_is_due() {
                        state.record_attempt();
                        tokio::spawn(Self::fill_gap(
                            pool.clone(),
                            last_broadcast_sequence,
                            cache_fill_sender.clone(),
                            cache_size,
                        ));
                    }
                } else {
                    gap_state = None;
                }

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

/// Delay before a failed archive backfill ends, so the listener's
/// immediate re-request doesn't spin against an unavailable storage
/// backend.
const ARCHIVE_BACKFILL_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(5);

#[tracing::instrument(name = "obix.persistent_cache.sequence_gap", level = "warn")]
fn record_sequence_gap(last_broadcast_sequence: u64, next_in_cache: u64, highest_known: u64) {}

#[tracing::instrument(name = "obix.persistent_cache.no_receivers", level = "warn")]
fn record_no_receivers(sequence: u64) {}

#[tracing::instrument(
    name = "obix.persistent_cache.backfill_failed",
    level = "warn",
    skip_all,
    fields(error = %error, current_sequence = current_sequence),
)]
fn record_backfill_failed(error: &sqlx::Error, current_sequence: u64) {}

#[tracing::instrument(
    name = "obix.persistent_cache.backfill_channel_closed",
    level = "error",
    fields(otel.status_code = "ERROR"),
)]
fn record_backfill_channel_closed() {}

#[tracing::instrument(
    name = "obix.persistent_cache.cache_fill_lagged",
    level = "error",
    fields(otel.status_code = "ERROR"),
)]
fn record_cache_fill_lagged(dropped: u64, last_broadcast_sequence: u64, highest_known: u64) {}

#[tracing::instrument(
    name = "obix.persistent_cache.cache_fill_closed",
    level = "error",
    fields(otel.status_code = "ERROR"),
)]
fn record_cache_fill_closed() {}

#[tracing::instrument(
    name = "obix.persistent_cache.notification_channel_closed",
    level = "error",
    fields(otel.status_code = "ERROR"),
)]
fn record_notification_channel_closed() {}

#[tracing::instrument(
    name = "obix.persistent_cache.resync_failed",
    level = "error",
    skip_all,
    fields(otel.status_code = "ERROR", error = %error),
)]
fn record_resync_failed(error: &sqlx::Error) {}

#[tracing::instrument(
    name = "obix.persistent_cache.archive_backfill_failed",
    level = "error",
    skip_all,
    fields(otel.status_code = "ERROR", error = %error, current_sequence = current_sequence),
)]
fn record_archive_backfill_failed(error: &crate::archive::ArchiveError, current_sequence: u64) {}

#[tracing::instrument(
    name = "obix.persistent_cache.archive_reader_missing",
    level = "error",
    skip_all,
    fields(otel.status_code = "ERROR", start_after = start_after, watermark = watermark),
)]
fn record_archive_reader_missing(start_after: u64, watermark: u64) {}

#[tracing::instrument(
    name = "obix.persistent_cache.archive_watermark_failed",
    level = "warn",
    skip_all,
    fields(error = %error),
)]
fn record_archive_watermark_failed(error: &sqlx::Error) {}
