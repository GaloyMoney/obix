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
    out::{event::*, gap_fill::GapFillRequest, pg_notify::NotifyMessage},
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
        gap_fill_tx: mpsc::UnboundedSender<GapFillRequest>,
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
            gap_fill_tx,
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

    /// How long a parked backfill waits before re-reading regardless of
    /// wake-up signals, and how long it backs off after a transient page
    /// read error. The cache-fill wake-up makes typical resumption
    /// immediate; this interval only bounds the lost-signal worst case.
    const BACKFILL_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

    /// Park a stalled backfill until its needed sequence plausibly
    /// resolved: woken by that exact sequence arriving on the cache-fill
    /// stream (every resolution path lands there — in-process post-commit
    /// broadcast, notification fetch, the GapFiller's placeholders and
    /// compensations), or by the retry interval elapsing as the
    /// lost-signal backstop. The caller re-reads the page either way; the
    /// wake-up is a hint, never trusted as data.
    ///
    /// Takes a receiver the caller subscribed **before** the page read
    /// that discovered the gap (and thus before any fill request it sent):
    /// a broadcast receiver only sees messages sent after `subscribe()`,
    /// so a late subscription would let the resolving delivery slip into
    /// the gap between request and park — anything resolved before the
    /// subscription is instead visible to the page read itself.
    async fn park_until_resolved(
        mut wakeup: broadcast::Receiver<PersistentDelivery<P>>,
        needed: EventSequence,
    ) {
        let deadline = tokio::time::Instant::now() + Self::BACKFILL_RETRY_INTERVAL;
        loop {
            match tokio::time::timeout_at(deadline, wakeup.recv()).await {
                Ok(Ok(delivery)) if delivery.sequence() == needed => return,
                Ok(Ok(_)) => {}
                // Lagged or closed: no reliable signal left — re-read.
                Ok(Err(_)) => return,
                // Interval elapsed: re-read regardless.
                Err(_) => return,
            }
        }
    }

    /// Serve one backfill request: deliver `(start_after, highest]` to the
    /// listener **in order, gap-free, in a single request**. The
    /// listener-facing contract is deliberately simple — one request per
    /// range, ever — so every gap condition is handled (or waited out)
    /// here rather than leaking to the listener:
    ///
    /// - Historical gaps (allocated before the cache loop started) are
    ///   reported to the [`GapFiller`](crate::out::gap_fill::GapFiller),
    ///   which merges overlapping requests from concurrent backfills into
    ///   one proof-gated, batch-capped, cluster-deduped fill; its
    ///   placeholders land on the cache-fill stream, wake the park below,
    ///   and the next page read delivers them in order.
    /// - Young frontier gaps (an in-flight or just-failed writer) are
    ///   **parked on**, never reported: the writer commits, the GapFiller
    ///   compensates the rollback, or its grace-gated stall episode fills
    ///   the gap once provably abandoned — all of which land on the
    ///   cache-fill stream and wake the park. Liveness holds because the
    ///   central broadcast cursor sweeps every sequence: any gap this task
    ///   can park on is at or below a stall the cache loop reports.
    /// - Transient read errors back off and retry. Nothing terminates the
    ///   request short of range-complete or the listener going away.
    #[allow(clippy::too_many_arguments)]
    async fn handle_backfill_request(
        pool: sqlx::PgPool,
        start_after: EventSequence,
        sender: mpsc::Sender<PersistentDelivery<P>>,
        cache_snapshot: im::OrdMap<EventSequence, PersistentDelivery<P>>,
        cache_fill_sender: broadcast::Sender<PersistentDelivery<P>>,
        highest: EventSequence,
        buffer_size: usize,
        init_head: u64,
        gap_fill_tx: mpsc::UnboundedSender<GapFillRequest>,
    ) {
        use std::ops::Bound;

        let mut current_sequence = start_after;

        while current_sequence < highest {
            // Serve straight from the request-time cache snapshot while it
            // holds the next contiguous run — no DB round trip.
            if cache_snapshot.contains_key(&current_sequence.next()) {
                for (_, event) in
                    cache_snapshot.range((Bound::Excluded(current_sequence), Bound::Unbounded))
                {
                    if event.sequence() != current_sequence.next() {
                        break;
                    }
                    if sender.send(event.clone()).await.is_err() {
                        return;
                    }
                    current_sequence = event.sequence();
                }
                continue;
            }

            // Authoritative page read — with the park's wake-up receiver
            // subscribed FIRST: any resolution landing after this point is
            // buffered for the park below, and anything resolved before it
            // is visible to the read itself. Subscribing later (inside the
            // park) would let a resolving delivery — in particular the
            // GapFiller's response to the Historical request sent below —
            // slip into the unobserved window and cost the full retry
            // interval.
            let wakeup = cache_fill_sender.subscribe();
            let select_from = current_sequence;
            let events = match Tables::load_next_page::<P>(&pool, select_from, buffer_size).await {
                Ok(events) => events,
                Err(e) => {
                    record_backfill_failed(&e, u64::from(current_sequence));
                    tokio::time::sleep(Self::BACKFILL_RETRY_INTERVAL).await;
                    continue;
                }
            };
            let returned = events.len();
            let mut present = std::collections::HashSet::with_capacity(returned);
            let mut page = Vec::with_capacity(returned);
            for item in events {
                let delivery = PersistentDelivery::from(item);
                present.insert(u64::from(delivery.sequence()));
                page.push(delivery);
            }

            // Deliver the contiguous prefix; anything above a gap is left
            // for a later read so delivery stays ordered and gap-free.
            let mut delivered = 0;
            for delivery in &page {
                if delivery.sequence() != current_sequence.next() {
                    break;
                }
                let _ = cache_fill_sender.send(delivery.clone());
                if sender.send(delivery.clone()).await.is_err() {
                    return;
                }
                current_sequence = delivery.sequence();
                delivered += 1;
            }
            if delivered == returned && returned == buffer_size {
                // Full contiguous page — more may follow immediately.
                continue;
            }
            if current_sequence >= highest {
                break;
            }

            // Stalled on a gap at `current_sequence.next()`. Historical
            // gaps (allocated before the cache loop started — e.g. rolled
            // back with no process observing the frontier at the time) are
            // reported to the GapFiller; young frontier gaps are not (the
            // cache loop reports the cursor's stall, and this task never
            // decides fills). Either way, park until the resolution lands
            // on the cache-fill stream, then re-read.
            let next_needed = u64::from(current_sequence.next());
            if next_needed <= init_head {
                let fill_to = init_head.min(u64::from(select_from) + buffer_size as u64);
                let missing = (next_needed..=fill_to)
                    .filter(|sequence| !present.contains(sequence))
                    .map(EventSequence::from)
                    .collect::<Vec<_>>();
                let _ = gap_fill_tx.send(GapFillRequest::Historical(missing));
            }
            Self::park_until_resolved(wakeup, current_sequence.next()).await;
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
        gap_fill_tx: mpsc::UnboundedSender<GapFillRequest>,
    ) -> Result<OwnedTaskHandle, sqlx::Error> {
        let pool = pool.clone();

        let cache_size = config.event_cache_size;
        let high_water = cache_size * (100 + config.event_cache_trim_percent as usize) / 100;
        let low_water = cache_size * (100 - config.event_cache_trim_percent as usize) / 100;
        let idle_resync_interval = config.idle_resync_interval;

        let initial_sequence = EventSequence::from(highest_known_sequence.load(Ordering::Relaxed));

        let handle = spawn_supervised("obix::persistent_cache_loop", async move {
            let mut persistent_cache: im::OrdMap<EventSequence, PersistentDelivery<P>> =
                im::OrdMap::new();
            let mut last_broadcast_sequence = initial_sequence;
            // The stall position last reported to the GapFiller — the
            // report is sent once per stall, re-armed on cache-fill lag
            // and on idle resync so a lost delivery can never leave a
            // stall unreported forever.
            let mut reported_stall: Option<EventSequence> = None;
            // Bound for backfill's historical classification: every
            // sequence <= the head read at init was allocated before this
            // loop started.
            let init_head = u64::from(initial_sequence);
            let mut last_progress_at = tokio::time::Instant::now();

            loop {
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
                                    init_head,
                                    gap_fill_tx.clone(),
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
                                // Dropped deliveries may include a fill that
                                // would have resolved the reported stall —
                                // re-arm so it is re-reported if it persists.
                                reported_stall = None;
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

                    _ = tokio::time::sleep_until(last_progress_at + idle_resync_interval) => {
                        if let Some(head) = Self::read_confirmed_head(&pool).await {
                            highest_known_sequence.fetch_max(
                                u64::from(head),
                                Ordering::AcqRel,
                            );
                        }
                        // Re-arm the stall report as a lost-signal backstop:
                        // if the GapFiller's episode ended believing the
                        // stall resolved but the resolving delivery was
                        // lost, the re-report below restarts it.
                        reported_stall = None;
                        last_progress_at = tokio::time::Instant::now();
                    }
                }

                // Stall reporting: when broadcasting is stuck waiting for a
                // missing sequence, tell the GapFiller — once per stall
                // position, cleared when the cursor moves. All fill policy
                // (grace, abandonment proof, batching, cluster dedup)
                // lives in the GapFiller; this loop only observes its own
                // cursor. This process's commit-failed allocations are
                // compensated reactively without ever stalling here, and a
                // stall that resolves within its grace period costs the
                // GapFiller zero DB work.
                let next_needed = last_broadcast_sequence.next();
                let highest = highest_known_sequence.load(Ordering::Relaxed);
                if u64::from(next_needed) <= highest && !persistent_cache.contains_key(&next_needed)
                {
                    if reported_stall != Some(last_broadcast_sequence) {
                        let _ = gap_fill_tx.send(GapFillRequest::Stalled(last_broadcast_sequence));
                        reported_stall = Some(last_broadcast_sequence);
                    }
                } else if reported_stall.take().is_some() {
                    let _ = gap_fill_tx.send(GapFillRequest::StallCleared);
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
