use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream};

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

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
    /// retried, and the cadence of a running fill episode's re-read/proof
    /// loop.
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

/// Bounds for historical gap fills during backfill: every sequence
/// `<= head` was already allocated when the cache loop started, so a fresh
/// abandonment marker trivially post-dates its allocation and the xmin
/// horizon passing that marker proves it lost. Backfill never fills
/// younger (frontier) gaps — those belong exclusively to the grace-gated
/// proactive fill.
#[derive(Clone, Copy)]
struct HistoricalFillBounds {
    head: u64,
    batch_limit: usize,
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
        notifier_report_tx: mpsc::UnboundedSender<(EventSequence, EventSequence)>,
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
            notifier_report_tx,
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

    /// One gap-fill episode for a stalled broadcast cursor. Self-contained:
    /// the episode resolves the stall or exits, and the loop's retry timer
    /// spawns at most one episode at a time.
    ///
    /// Structure:
    /// 1. Assign an abandonment marker (fresh xid + allocation head — every
    ///    sequence `<=` head was allocated before the marker).
    /// 2. Loop at [`GapFillState::REFILL_INTERVAL`]: re-read the page after
    ///    `from_sequence`, feeding whatever committed since into the cache
    ///    (this alone resolves stalls whose commit signal was missed). If
    ///    nothing in the eligible window is missing, done — read-only.
    /// 3. Once the xmin horizon passes the marker, the still-missing
    ///    sequences are provably abandoned — their writers ended without
    ///    committing them — so the placeholder insert cannot collide with
    ///    a live writer. Insert under the cluster-wide fill lock, capped at
    ///    `batch_limit`; deliver the placeholders locally and report the
    ///    range to the debounced notifier so other processes resume
    ///    reactively. If another node holds the lock, keep looping — its
    ///    rows surface in the next page re-read.
    async fn fill_gap(
        pool: sqlx::PgPool,
        from_sequence: EventSequence,
        batch_limit: usize,
        cache_fill_sender: broadcast::Sender<PersistentDelivery<P>>,
        notifier_tx: mpsc::UnboundedSender<(EventSequence, EventSequence)>,
        buffer_size: usize,
    ) {
        let Ok((marker, head)) = Tables::abandonment_marker(&pool).await else {
            return;
        };
        let from = u64::from(from_sequence);
        let fill_up_to = u64::from(head).min(from + buffer_size as u64);

        loop {
            let Ok(events) = Tables::load_next_page::<P>(&pool, from_sequence, buffer_size).await
            else {
                return;
            };
            let mut present = std::collections::HashSet::new();
            for item in events {
                let delivery = PersistentDelivery::from(item);
                present.insert(u64::from(delivery.sequence()));
                let _ = cache_fill_sender.send(delivery);
            }

            let missing = ((from + 1)..=fill_up_to)
                .filter(|sequence| !present.contains(sequence))
                .take(batch_limit)
                .map(EventSequence::from)
                .collect::<Vec<_>>();
            if missing.is_empty() {
                return;
            }

            match Tables::abandonment_proof_passed(&pool, &marker).await {
                Ok(true) => {
                    match Tables::fill_gaps_deduped::<P>(&pool, missing).await {
                        Ok(Some(placeholders)) => {
                            let mut range: Option<(EventSequence, EventSequence)> = None;
                            for item in placeholders {
                                let delivery = PersistentDelivery::from(item);
                                let sequence = delivery.sequence();
                                range = Some(match range {
                                    Some((lo, hi)) => (lo.min(sequence), hi.max(sequence)),
                                    None => (sequence, sequence),
                                });
                                let _ = cache_fill_sender.send(delivery);
                            }
                            if let Some(range) = range {
                                let _ = notifier_tx.send(range);
                            }
                            return;
                        }
                        // Another node holds the fill lock: loop — its rows
                        // are committed by the time the lock releases, so
                        // the next page re-read delivers them.
                        Ok(None) => {}
                        Err(_) => return,
                    }
                }
                // A transaction from before the marker still runs and may
                // own a missing sequence: abandonment is unprovable, keep
                // waiting (the periodic re-read above still delivers
                // late-arriving commits meanwhile).
                Ok(false) => {}
                Err(_) => return,
            }

            tokio::time::sleep(GapFillState::REFILL_INTERVAL).await;
        }
    }

    /// Wait until the given (historically-allocated) sequences are provably
    /// abandoned, then insert their placeholders under the cluster-wide
    /// fill lock. A fresh abandonment marker post-dates the sequences'
    /// allocation by construction (callers only pass sequences `<=` the
    /// cache loop's init head), so the xmin horizon passing the marker
    /// means every transaction that could have produced them has ended.
    /// The wait is typically zero — pre-init writers are long gone —
    /// and polls briefly otherwise. `None` = fill lock held elsewhere or
    /// transient error; the caller re-reads instead of inserting.
    async fn fill_provably_abandoned(
        pool: &sqlx::PgPool,
        missing: Vec<EventSequence>,
    ) -> Option<crate::tables::PersistentEventRows<P>> {
        const PROOF_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(250);
        let (marker, _) = Tables::abandonment_marker(pool).await.ok()?;
        loop {
            match Tables::abandonment_proof_passed(pool, &marker).await {
                Ok(true) => break,
                Ok(false) => tokio::time::sleep(PROOF_POLL_INTERVAL).await,
                Err(_) => return None,
            }
        }
        Tables::fill_gaps_deduped::<P>(pool, missing)
            .await
            .ok()
            .flatten()
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_backfill_request(
        pool: sqlx::PgPool,
        start_after: EventSequence,
        sender: mpsc::Sender<PersistentDelivery<P>>,
        cache_snapshot: im::OrdMap<EventSequence, PersistentDelivery<P>>,
        cache_fill_sender: broadcast::Sender<PersistentDelivery<P>>,
        highest: EventSequence,
        buffer_size: usize,
        fill_bounds: HistoricalFillBounds,
    ) {
        use std::ops::Bound;

        let mut current_sequence = start_after;

        while current_sequence < highest {
            let next_needed = current_sequence.next();
            if cache_snapshot.contains_key(&next_needed) {
                break;
            }

            match Tables::load_next_page::<P>(&pool, current_sequence, buffer_size).await {
                Ok(events) if events.is_empty() => {
                    // No committed row follows `current_sequence`. Any
                    // sequences left up to the init head are burned
                    // pre-start allocations (e.g. rolled back with no
                    // process observing the frontier at the time) — fill
                    // them, or a replaying listener stalls on them forever.
                    let from = u64::from(current_sequence);
                    let fill_to = fill_bounds.head.min(from + buffer_size as u64);
                    if from >= fill_to {
                        break;
                    }
                    let missing = ((from + 1)..=fill_to)
                        .take(fill_bounds.batch_limit)
                        .map(EventSequence::from)
                        .collect::<Vec<_>>();
                    let requested = missing.len();
                    let Some(placeholders) = Self::fill_provably_abandoned(&pool, missing).await
                    else {
                        // Fill lock raced or transient error: if another
                        // node was filling, its rows are committed by the
                        // time its lock releases — re-read the page rather
                        // than advancing past them.
                        continue;
                    };
                    let all_inserted = placeholders.len() == requested;
                    for item in placeholders {
                        let delivery = PersistentDelivery::from(item);
                        let _ = cache_fill_sender.send(delivery.clone());
                        if sender.send(delivery).await.is_err() {
                            return;
                        }
                    }
                    if all_inserted {
                        current_sequence = EventSequence::from(from + requested as u64);
                    }
                    // else: a concurrent filler owns part of the range; its
                    // rows are committed by now (DO NOTHING waits out the
                    // conflict), so the next page read delivers them — do
                    // not advance past them.
                }
                Ok(events) => {
                    let mut page = Vec::with_capacity(events.len());
                    let mut present = std::collections::HashSet::new();
                    for item in events {
                        let delivery = PersistentDelivery::from(item);
                        present.insert(u64::from(delivery.sequence()));
                        page.push(delivery);
                    }
                    let page_last = page
                        .last()
                        .map(|delivery| u64::from(delivery.sequence()))
                        .expect("page is non-empty");

                    // Interior historical gaps: fill only sequences
                    // allocated before the cache loop started (<= init
                    // head). Younger gaps are skipped — they are either
                    // in-flight writers about to commit or the grace-gated
                    // fill's responsibility, and they reach the listener
                    // through the live broadcast once resolved.
                    let from = u64::from(current_sequence);
                    let fill_to = fill_bounds.head.min(page_last);
                    let missing = ((from + 1)..=fill_to)
                        .filter(|sequence| !present.contains(sequence))
                        .take(fill_bounds.batch_limit)
                        .map(EventSequence::from)
                        .collect::<Vec<_>>();
                    if !missing.is_empty()
                        && let Some(placeholders) =
                            Self::fill_provably_abandoned(&pool, missing).await
                    {
                        // A raced fill lock (None) is fine here: the winner's
                        // rows are committed rows the listener's next
                        // backfill round reads back; the contiguity gate
                        // stalls it until then.
                        page.extend(placeholders.into_iter().map(PersistentDelivery::from));
                        page.sort_by_key(|delivery| delivery.sequence());
                    }

                    for delivery in page {
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

    /// Authoritative head read (the O(1) sequence `last_value` query).
    /// Logs and returns `None` on failure so callers skip their advance.
    async fn read_confirmed_head(pool: &sqlx::PgPool) -> Option<EventSequence> {
        match Tables::highest_known_persistent_sequence(pool).await {
            Ok(head) => Some(head),
            Err(e) => {
                record_resync_failed(&e);
                None
            }
        }
    }

    /// Parse a `{min_sequence, max_sequence}` notification (emitted per
    /// debounce tick by each process's notifier, or in-transaction by
    /// bare-transaction publishes) and decide what must be fetched.
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
        notifier_report_tx: mpsc::UnboundedSender<(EventSequence, EventSequence)>,
    ) -> Result<OwnedTaskHandle, sqlx::Error> {
        let pool = pool.clone();

        let cache_size = config.event_cache_size;
        let high_water = cache_size * (100 + config.event_cache_trim_percent as usize) / 100;
        let low_water = cache_size * (100 - config.event_cache_trim_percent as usize) / 100;
        let gap_fill_grace = config.gap_fill_grace;
        let gap_fill_batch_limit = config.gap_fill_batch_limit;
        let idle_resync_interval = config.idle_resync_interval;

        let initial_sequence = EventSequence::from(highest_known_sequence.load(Ordering::Relaxed));

        let handle = spawn_supervised("obix::persistent_cache_loop", async move {
            let mut persistent_cache: im::OrdMap<EventSequence, PersistentDelivery<P>> =
                im::OrdMap::new();
            let mut last_broadcast_sequence = initial_sequence;
            let mut gap_state: Option<GapFillState> = None;
            // At most one fill episode runs at a time: an episode that is
            // waiting out a live pre-marker transaction must not be joined
            // by the next 1s retry — previously up to ~8 concurrent
            // multi-second fills stacked up, each holding a connection.
            let mut fill_task: Option<tokio::task::JoinHandle<()>> = None;
            // Bound for backfill's historical fills: every sequence <= the
            // head read at init was allocated before this loop started.
            let fill_bounds = HistoricalFillBounds {
                head: u64::from(initial_sequence),
                batch_limit: gap_fill_batch_limit,
            };
            let mut last_progress_at = tokio::time::Instant::now();

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
                                    pool.clone(),
                                    start_after,
                                    sender,
                                    cache_snapshot,
                                    cache_fill_sender.clone(),
                                    highest,
                                    cache_size,
                                    fill_bounds,
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
                                let watermark_before =
                                    highest_known_sequence.load(Ordering::Relaxed);

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

                                if highest_known_sequence.load(Ordering::Relaxed)
                                    > watermark_before
                                {
                                    last_progress_at = tokio::time::Instant::now();
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
                                // gap-fill episode against in-flight sequences — but
                                // that work is bounded by last_value, read-only until
                                // the sequences are provably abandoned (the xmin-
                                // horizon proof), and batch-capped once they are, so
                                // it never rewrites committed rows and never blocks on
                                // a live writer: in-flight writers resolve the gap by
                                // committing, aborted ones become provably lost the
                                // moment they end. On a transient head-read failure
                                // the fetch is skipped (rather than fired unclamped);
                                // a subsequent notification or resync retries.
                                let current_head =
                                    highest_known_sequence.load(Ordering::Relaxed);
                                let claim_advances = claimed_head
                                    .is_some_and(|claimed| u64::from(claimed) > current_head);
                                if (claim_advances || resync_needed || fetch_range.is_some())
                                    && let Some(head) = Self::read_confirmed_head(&pool).await
                                {
                                    last_progress_at = tokio::time::Instant::now();
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

                    _ = tokio::time::sleep_until(last_progress_at + idle_resync_interval) => {
                        if let Some(head) = Self::read_confirmed_head(&pool).await {
                            highest_known_sequence.fetch_max(
                                u64::from(head),
                                Ordering::AcqRel,
                            );
                        }
                        last_progress_at = tokio::time::Instant::now();
                    }
                }

                // Proactive gap fill: if broadcasting is stuck waiting for a
                // missing sequence, spawn a fill episode. The first episode
                // is delayed by `gap_fill_grace` — most gaps are in-flight
                // transactions that resolve on their own within ms, and this
                // process's own commit-failed allocations are compensated
                // reactively without ever reaching here. An episode is
                // read-only (page re-reads) until the missing sequences are
                // provably abandoned via the abandonment marker, then
                // inserts at most `gap_fill_batch_limit` placeholders under
                // the cluster-wide fill lock — it can never collide with a
                // live writer's speculative-insertion lock.
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
                        // A still-running episode (e.g. waiting out a live
                        // pre-marker transaction) is never joined by
                        // another: the attempt is recorded (pushing the
                        // next retry out) but not spawned.
                        if fill_task.as_ref().is_none_or(|task| task.is_finished()) {
                            fill_task = Some(tokio::spawn(Self::fill_gap(
                                pool.clone(),
                                last_broadcast_sequence,
                                gap_fill_batch_limit,
                                cache_fill_sender.clone(),
                                notifier_report_tx.clone(),
                                cache_size,
                            )));
                        }
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
