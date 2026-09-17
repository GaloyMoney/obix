//! **Behind vs. hole.** The cursor (`last_broadcast_sequence`) can be behind
//! the head for two different reasons, and the loop's stall-reporting block
//! (bottom of [`PersistentOutboxEventCache::spawn_cache_loop`]) tells them
//! apart: *behind* just means `cursor + 1` is uncached — it could be a
//! committed backlog (a bulk commit larger than the broadcast buffer) or an
//! in-flight/abandoned writer (a *hole*). The long-lived feeder task
//! (`feeder.rs`) drains a committed backlog: a local commit's batch, handed
//! to it directly via [`CacheFeeder::accept`], is fed from memory without
//! being asked; a backlog with no in-memory copy (another process's commit)
//! is read from the database, only on this loop's request and only up to
//! the first pending in-memory sequence. Only when neither source has
//! anything does the position get treated as a hole and handed to the
//! GapFiller's grace-gated episode, exactly as before this distinction
//! existed. See [`decide_stall_action`] for the rule.

use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{broadcast, mpsc, watch};
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream};
use tracing::Instrument;

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use super::feeder::{CacheFeeder, CatchUpOutcome, FeedRequest, run_feeder};
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
    /// Lowest sequence the notification proves committed — its own writer's
    /// claim that this sequence landed, which re-arms a stale
    /// `catch_up_exhausted_at` at or above it (see `decide_stall_action`).
    min_sequence: EventSequence,
    /// Highest sequence the notification proves committed.
    max_sequence: EventSequence,
    /// Sub-range of the notified sequences missing from the cache as
    /// `(after, up_to)` — exclusive lower bound, inclusive upper — or
    /// `None` when every notified sequence is already cached (the warm
    /// in-process path: the post-commit broadcast beat the NOTIFY).
    missing: Option<(EventSequence, EventSequence)>,
}

/// What the stall-reporting block at the bottom of the cache loop does this
/// iteration.
#[derive(Debug, PartialEq, Eq)]
enum StallAction {
    Nothing,
    ClearStall,
    RequestCatchUp,
    ReportStall,
}

/// The behind-vs-hole decision rule (module doc), extracted pure so its
/// branches are unit-testable without a database.
///
/// `distance` is the gap-aware distance from [`catch_up_distance`] — the
/// cursor's distance to whatever the feeder cannot already supply from
/// memory, not raw `highest_known - cursor`. `threshold` is
/// `backfill_page_size`. `catch_up_active` and `exhausted_at` are the loop's
/// own state (whether a catch-up request is currently outstanding, and the
/// position the last one ended a hole at). `memory_next` is the feeder's
/// front — the smallest pending in-memory sequence above the cursor, or
/// `None`.
#[allow(clippy::too_many_arguments)]
fn decide_stall_action(
    behind: bool,
    catch_up_active: bool,
    exhausted_at: Option<EventSequence>,
    cursor: EventSequence,
    distance: u64,
    threshold: u64,
    reported_stall: Option<EventSequence>,
    memory_next: Option<EventSequence>,
) -> StallAction {
    if !behind {
        return if reported_stall.is_some() {
            StallAction::ClearStall
        } else {
            StallAction::Nothing
        };
    }
    // The feeder is already feeding this exact position from memory,
    // unrequested — neither a stall report nor a catch-up request is
    // warranted while that is true.
    if memory_next == Some(cursor.next()) {
        return StallAction::Nothing;
    }
    if catch_up_active {
        return StallAction::Nothing;
    }
    if exhausted_at != Some(cursor) && distance >= threshold {
        return StallAction::RequestCatchUp;
    }
    if reported_stall != Some(cursor) {
        StallAction::ReportStall
    } else {
        StallAction::Nothing
    }
}

/// How far the cursor is behind what the feeder cannot already supply from
/// memory: up to the first pending in-memory sequence when there is one,
/// else up to the head. Using raw `highest - cursor` here would let a large
/// pending local batch (which advances `highest_known` on arrival, per
/// `CacheFeeder::accept`) make an unrelated small frontier gap below it look
/// like a full page behind — triggering a database probe for a handful of
/// sequences, exactly the per-frontier-gap probing the threshold exists to
/// prevent.
fn catch_up_distance(cursor: u64, highest: u64, memory_next: Option<EventSequence>) -> u64 {
    let bound = memory_next.map_or(highest, |m| u64::from(m).saturating_sub(1).min(highest));
    bound.saturating_sub(cursor)
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
    feeder: CacheFeeder<P>,
    _cache_loop_handle: OwnedTaskHandle,
    _feeder_handle: OwnedTaskHandle,
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

    /// The hook's entire cache-facing surface: `PersistEvents::post_commit`
    /// hands its committed batch straight to [`CacheFeeder::accept`], which
    /// advances the head watermark and queues it for the feeder task to
    /// drain from memory — no DB round trip to read it back, and no
    /// truncation policy living in the hook.
    pub(crate) fn feeder(&self) -> CacheFeeder<P> {
        self.feeder.clone()
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

        // The cursor, observable so the feeder can pace itself off it.
        // `send_replace` never errors regardless of receiver count, so no
        // receiver needs to exist yet.
        let (cursor_tx, feeder_cursor_rx) =
            watch::channel(highest_known_sequence.load(Ordering::Relaxed));
        let (request_tx, request_rx) = mpsc::unbounded_channel::<FeedRequest>();
        let (catch_up_done_tx, catch_up_done_rx) = mpsc::unbounded_channel::<CatchUpOutcome>();
        // Pending local commit batches, handed straight to the feeder — see
        // `CacheFeeder::accept`. `front_rx` is the feeder's front (the
        // smallest pending in-memory sequence above the cursor), published
        // for the cache loop's decision rule as `memory_next`.
        let (batches_tx, batches_rx) = mpsc::unbounded_channel();
        let (front_tx, front_rx) = watch::channel(None);
        let feeder = CacheFeeder::new(batches_tx, highest_known_sequence.clone(), front_tx.clone());
        // Bounds a fed page: it is sent into the cache-fill broadcast, so a
        // page wider than that channel would lag the very channel it is
        // trying to refill.
        let catch_up_page_size = config
            .backfill_page_size
            .max(1)
            .min((config.event_buffer_size / 2).max(1))
            .max(1);
        let feeder_handle = spawn_supervised(
            "obix::persistent_cache_feeder",
            run_feeder::<P, Tables>(
                pool.clone(),
                cache_fill_send.clone(),
                feeder_cursor_rx,
                highest_known_sequence.clone(),
                catch_up_page_size,
                batches_rx,
                request_rx,
                catch_up_done_tx,
                front_tx,
            ),
        );

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
            cursor_tx,
            request_tx,
            catch_up_done_rx,
            front_rx,
        )
        .await?;

        let ret = Self {
            highest_known_sequence,
            backfill_request_send: backfill_send,
            persistent_event_sender,
            backfill_buffer_size: config.backfill_page_size.max(1),
            cache_fill_sender: cache_fill_send,
            feeder,
            _cache_loop_handle: cache_loop_handle,
            _feeder_handle: OwnedTaskHandle::new(feeder_handle),
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
            last_broadcast_sequence = *seq;
            if persistent_event_sender.send(evt.clone()).is_err() {
                record_no_receivers(u64::from(*seq));
            }
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
    /// compensations), or, once the retry interval elapses, by an
    /// authoritative index probe as the lost-signal backstop. The wake-up
    /// is a hint, never trusted as data; the probe is the one that decides,
    /// which keeps a park behind a slow or abandoned sequence costing one
    /// index lookup per interval. A failed probe returns rather than
    /// retries — the caller's re-read has its own backoff.
    ///
    /// Takes a receiver the caller subscribed **before** the page read
    /// that discovered the gap (and thus before any fill request it sent):
    /// a broadcast receiver only sees messages sent after `subscribe()`,
    /// so a late subscription would let the resolving delivery slip into
    /// the gap between request and park — anything resolved before the
    /// subscription is instead visible to the page read itself.
    async fn park_until_resolved(
        pool: &sqlx::PgPool,
        mut wakeup: broadcast::Receiver<PersistentDelivery<P>>,
        needed: EventSequence,
    ) {
        loop {
            let deadline = tokio::time::Instant::now() + Self::BACKFILL_RETRY_INTERVAL;
            loop {
                match tokio::time::timeout_at(deadline, wakeup.recv()).await {
                    Ok(Ok(delivery)) if delivery.sequence() == needed => return,
                    Ok(Ok(_)) => {}
                    // Dropped wake-ups are still only hints: re-subscribe at
                    // the tail, then let the probe decide. A resolution
                    // inside the lost window is visible to the probe, one
                    // after it to the fresh receiver.
                    Ok(Err(broadcast::error::RecvError::Lagged(_))) => {
                        wakeup = wakeup.resubscribe();
                        break;
                    }
                    Ok(Err(broadcast::error::RecvError::Closed)) => return,
                    Err(_) => break,
                }
            }
            match Tables::sequence_present(pool, needed).await {
                Ok(false) => continue,
                // Landed, or the probe failed — the caller re-reads and decides.
                Ok(true) | Err(_) => return,
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
        page_size: usize,
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

            // Don't spend a query without demand: a listener that has stopped
            // polling parks the reader here rather than materialising a page
            // nobody will take. A gate only, never a size (see
            // `backfill_page_size`); dropping the permit returns the slot.
            match sender.reserve().await {
                Ok(permit) => drop(permit),
                Err(_) => return,
            }

            // Subscribe before the page read (and the Historical request
            // below) so no resolution falls between them, but after the
            // demand gate — `reserve` blocks as long as the consumer takes,
            // and a receiver held across that wait enters the park lagged.
            let wakeup = cache_fill_sender.subscribe();

            let select_from = current_sequence;
            // One span per page read — the only place this path's cost is
            // observable. `rows` against `delivered` separates genuine
            // catch-up from a read that advanced almost nothing.
            let page_span = tracing::info_span!(
                "obix.persistent_cache.backfill_page",
                from_sequence = u64::from(select_from),
                limit = page_size,
                rows = tracing::field::Empty,
                delivered = tracing::field::Empty,
            );
            let events = match Tables::load_next_contiguous_page::<P>(&pool, select_from, page_size)
                .instrument(page_span.clone())
                .await
            {
                Ok(events) => events,
                Err(e) => {
                    record_backfill_failed(&e, u64::from(current_sequence));
                    tokio::time::sleep(Self::BACKFILL_RETRY_INTERVAL).await;
                    continue;
                }
            };
            let returned = events.len();
            page_span.record("rows", returned);

            // The read is cut at the first gap, so every returned row is
            // deliverable. The ordering check enforces that contract against
            // whatever a `MailboxTables` implementation hands back.
            let mut delivered = 0;
            for item in events {
                let delivery = PersistentDelivery::from(item);
                if delivery.sequence() != current_sequence.next() {
                    break;
                }
                let _ = cache_fill_sender.send(delivery.clone());
                if sender.send(delivery).await.is_err() {
                    return;
                }
                current_sequence = current_sequence.next();
                delivered += 1;
            }
            page_span.record("delivered", delivered);
            if delivered == returned && returned == page_size {
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
                let fill_to = init_head.min(u64::from(select_from) + page_size as u64);
                if next_needed <= fill_to {
                    match Tables::missing_sequences(
                        &pool,
                        current_sequence,
                        EventSequence::from(fill_to),
                    )
                    .await
                    {
                        Ok(missing) if !missing.is_empty() => {
                            let _ = gap_fill_tx.send(GapFillRequest::Historical(missing));
                        }
                        Ok(_) => {}
                        Err(e) => record_backfill_failed(&e, next_needed),
                    }
                }
            }
            Self::park_until_resolved(&pool, wakeup, current_sequence.next()).await;
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
            min_sequence: header.min_sequence,
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
        cursor_tx: watch::Sender<u64>,
        request_tx: mpsc::UnboundedSender<FeedRequest>,
        mut catch_up_done_rx: mpsc::UnboundedReceiver<CatchUpOutcome>,
        front_rx: watch::Receiver<Option<u64>>,
    ) -> Result<OwnedTaskHandle, sqlx::Error> {
        let pool = pool.clone();

        let cache_size = config.event_cache_size;
        let backfill_page_size = config.backfill_page_size.max(1);
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

            // Whether a feeder-run catch-up is currently in flight — the
            // feeder task itself is long-lived (spawned once in `init`);
            // this just tracks whether it currently owns progress. See
            // `decide_stall_action`'s `catch_up_active` gate.
            let mut catch_up_active = false;
            // The position the last catch-up run ended a hole at — prevents
            // re-probing the same hole every loop iteration. See
            // `decide_stall_action` for the re-arm points.
            let mut catch_up_exhausted_at: Option<EventSequence> = None;

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
                                    backfill_page_size,
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

                    result = catch_up_done_rx.recv() => {
                        // No `continue`: falls through to the stall-reporting
                        // block below, which reports the hole (or moves on)
                        // exactly as it would for an ordinary stall.
                        //
                        // A GapFiller episode is only cleared here on
                        // genuine progress (`AtHead`/`ReachedMemory`, or a
                        // hole at a *different* position than whatever is
                        // currently reported) — never merely because a
                        // catch-up ran. A catch-up that re-probes and lands
                        // on the exact position already reported must leave
                        // that episode alone: clearing it unconditionally on
                        // every run would restart its grace period every
                        // time a spurious re-arm (e.g. an unrelated
                        // notification) triggers a redundant, no-progress
                        // catch-up.
                        if let Some(outcome) = result {
                            catch_up_active = false;
                            match outcome {
                                CatchUpOutcome::AtHead | CatchUpOutcome::ReachedMemory => {
                                    catch_up_exhausted_at = None;
                                    if reported_stall.take().is_some() {
                                        let _ = gap_fill_tx.send(GapFillRequest::StallCleared);
                                    }
                                }
                                CatchUpOutcome::HoleAfter(pos) => {
                                    catch_up_exhausted_at = Some(pos);
                                    if reported_stall.is_some_and(|stalled_at| stalled_at != pos) {
                                        reported_stall = None;
                                        let _ = gap_fill_tx.send(GapFillRequest::StallCleared);
                                    }
                                }
                            }
                        }
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

                                // Once per drain, not per event: lets a
                                // parked catch-up task observe how far the
                                // cursor actually moved.
                                cursor_tx.send_replace(u64::from(last_broadcast_sequence));

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
                                // The drop may also have carried committed
                                // rows past a previously exhausted catch-up
                                // position — re-arm that too.
                                reported_stall = None;
                                catch_up_exhausted_at = None;
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
                                                // The notification's own writer
                                                // claims a sequence at or past
                                                // the cursor's hole landed —
                                                // re-arm so a stale exhausted
                                                // catch-up is retried.
                                                if u64::from(notified.min_sequence)
                                                    <= u64::from(last_broadcast_sequence.next())
                                                {
                                                    catch_up_exhausted_at = None;
                                                }
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
                                            catch_up_exhausted_at = None;
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
                                            let width = u64::from(clamped_up_to)
                                                - u64::from(after);
                                            if width <= backfill_page_size as u64 {
                                                tokio::spawn(Self::fetch_notified_range(
                                                    pool.clone(),
                                                    after,
                                                    clamped_up_to,
                                                    cache_fill_sender.clone(),
                                                ));
                                            } else if u64::from(after)
                                                <= u64::from(last_broadcast_sequence)
                                            {
                                                // Wider than one page: not
                                                // this task's job — the
                                                // decision rule's catch-up
                                                // reads it contiguously once
                                                // the cursor reaches it.
                                                // Re-arm only when the range
                                                // reaches back to (or before)
                                                // the cursor: an unrelated
                                                // wide range elsewhere must
                                                // never clear a stale
                                                // exhaustion, or `StartCatchUp`
                                                // sends `StallCleared` and
                                                // resets an unresolved hole's
                                                // grace-gated episode for no
                                                // reason.
                                                catch_up_exhausted_at = None;
                                            }
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
                        // lost, the re-report below restarts it. Same
                        // backstop for a stale exhausted catch-up position.
                        reported_stall = None;
                        catch_up_exhausted_at = None;
                        last_progress_at = tokio::time::Instant::now();
                    }
                }

                // Behind vs. hole (module doc): tell the GapFiller about a
                // stall, request a catch-up, or do nothing — see
                // `decide_stall_action`. All fill policy (grace, abandonment
                // proof, batching, cluster dedup) lives in the GapFiller;
                // this loop only observes its own cursor and, now, whether
                // the feeder owns progress toward it (from memory or DB).
                let next_needed = last_broadcast_sequence.next();
                let highest = highest_known_sequence.load(Ordering::Relaxed);
                let behind = u64::from(next_needed) <= highest
                    && !persistent_cache.contains_key(&next_needed);
                let cursor_u = u64::from(last_broadcast_sequence);
                // The feeder publishes its front once per iteration, before
                // sending that iteration's page: for the whole span of that
                // page's send (and the cursor-consume wait after it), the
                // published value trails the cursor this loop just
                // advanced to — it can sit at or below it, never because
                // memory fell behind but because it hasn't re-published
                // yet. Filtering a stale-or-equal value down to `None`
                // ("nothing pending") would be wrong: it was pending a
                // moment ago and the feeder that fed it is still working.
                // Clamping it up to `cursor + 1` instead reads it as
                // "memory is actively feeding here", the same as a
                // perfectly fresh value would — a genuinely empty front
                // (the feeder has never held anything, or has fully
                // drained and retained it) is unaffected, since `map`
                // leaves `None` as `None`.
                let memory_next =
                    (*front_rx.borrow()).map(|m| EventSequence::from(m.max(cursor_u + 1)));
                let distance = catch_up_distance(cursor_u, highest, memory_next);

                match decide_stall_action(
                    behind,
                    catch_up_active,
                    catch_up_exhausted_at,
                    last_broadcast_sequence,
                    distance,
                    backfill_page_size as u64,
                    reported_stall,
                    memory_next,
                ) {
                    StallAction::Nothing => {}
                    StallAction::ClearStall => {
                        reported_stall = None;
                        let _ = gap_fill_tx.send(GapFillRequest::StallCleared);
                    }
                    StallAction::RequestCatchUp => {
                        // Do NOT clear `reported_stall` / notify the
                        // GapFiller here: starting a catch-up is not itself
                        // progress. If this run just re-discovers the same
                        // position an existing GapFiller episode is already
                        // working, that episode must be left running,
                        // untouched — see the `catch_up_done_rx` arm, which
                        // clears it only once the outcome proves progress.
                        let _ = request_tx.send(FeedRequest::CatchUp);
                        catch_up_active = true;
                    }
                    StallAction::ReportStall => {
                        let _ = gap_fill_tx.send(GapFillRequest::Stalled(last_broadcast_sequence));
                        reported_stall = Some(last_broadcast_sequence);
                    }
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

#[cfg(test)]
mod tests {
    use super::*;

    const THRESHOLD: u64 = 1000;

    fn seq(n: u64) -> EventSequence {
        EventSequence::from(n)
    }

    /// Not behind, no stall previously reported: nothing to do.
    #[test]
    fn not_behind_and_clean_does_nothing() {
        assert_eq!(
            decide_stall_action(false, false, None, seq(10), 0, THRESHOLD, None, None),
            StallAction::Nothing
        );
    }

    /// Not behind, but a stall was reported (the cursor just caught up to
    /// it): clear it.
    #[test]
    fn not_behind_with_reported_stall_clears_it() {
        assert_eq!(
            decide_stall_action(
                false,
                false,
                None,
                seq(10),
                0,
                THRESHOLD,
                Some(seq(10)),
                None
            ),
            StallAction::ClearStall
        );
    }

    /// A catch-up is already running: it owns progress, never report a
    /// stall out from under it.
    #[test]
    fn behind_with_catch_up_active_does_nothing() {
        assert_eq!(
            decide_stall_action(
                true,
                true,
                None,
                seq(10),
                THRESHOLD * 5,
                THRESHOLD,
                None,
                None
            ),
            StallAction::Nothing
        );
    }

    /// Distance at the threshold requests a catch-up rather than reporting a
    /// stall — the boundary is inclusive.
    #[test]
    fn distance_at_threshold_requests_catch_up() {
        assert_eq!(
            decide_stall_action(true, false, None, seq(10), THRESHOLD, THRESHOLD, None, None),
            StallAction::RequestCatchUp
        );
    }

    /// One below the threshold reports a stall instead — the boundary the
    /// test above pins is exact, not "roughly there".
    #[test]
    fn distance_below_threshold_reports_stall() {
        assert_eq!(
            decide_stall_action(
                true,
                false,
                None,
                seq(10),
                THRESHOLD - 1,
                THRESHOLD,
                None,
                None
            ),
            StallAction::ReportStall
        );
    }

    /// Already reported at this exact cursor: don't resend every iteration.
    #[test]
    fn distance_below_threshold_already_reported_does_nothing() {
        assert_eq!(
            decide_stall_action(
                true,
                false,
                None,
                seq(10),
                THRESHOLD - 1,
                THRESHOLD,
                Some(seq(10)),
                None,
            ),
            StallAction::Nothing
        );
    }

    /// A catch-up already proved this exact position a hole: past-threshold
    /// distance does not re-probe it every iteration, it falls back to the
    /// (unchanged) stall path.
    #[test]
    fn exhausted_at_cursor_falls_back_to_stall_report() {
        assert_eq!(
            decide_stall_action(
                true,
                false,
                Some(seq(10)),
                seq(10),
                THRESHOLD * 5,
                THRESHOLD,
                None,
                None,
            ),
            StallAction::ReportStall
        );
    }

    /// The cursor moved past the position a prior catch-up exhausted: the
    /// stale exhaustion no longer applies to the new cursor, so a
    /// far-enough distance requests a fresh catch-up.
    #[test]
    fn exhausted_at_stale_position_requests_catch_up() {
        assert_eq!(
            decide_stall_action(
                true,
                false,
                Some(seq(3)),
                seq(10),
                THRESHOLD * 5,
                THRESHOLD,
                None,
                None,
            ),
            StallAction::RequestCatchUp
        );
    }

    /// The feeder is already feeding this exact position from memory: do
    /// nothing, even though the raw distance (computed from `highest`, not
    /// `catch_up_distance`) is far past the threshold — a caller that forgot
    /// to route `distance` through `catch_up_distance` would still pass this
    /// case only by accident, so it is exercised directly at the
    /// `decide_stall_action` level too.
    #[test]
    fn memory_next_at_cursor_plus_one_does_nothing_even_when_far_behind() {
        assert_eq!(
            decide_stall_action(
                true,
                false,
                None,
                seq(10),
                THRESHOLD * 5,
                THRESHOLD,
                None,
                Some(seq(11)),
            ),
            StallAction::Nothing
        );
    }

    /// A pending in-memory batch further ahead than `cursor + 1` does not
    /// short-circuit anything: the ordinary threshold comparison (against
    /// whatever `distance` the caller computed) still governs.
    #[test]
    fn memory_next_further_ahead_falls_through_to_the_rule() {
        assert_eq!(
            decide_stall_action(
                true,
                false,
                None,
                seq(10),
                THRESHOLD,
                THRESHOLD,
                None,
                Some(seq(15)),
            ),
            StallAction::RequestCatchUp
        );
        assert_eq!(
            decide_stall_action(
                true,
                false,
                None,
                seq(10),
                THRESHOLD - 1,
                THRESHOLD,
                None,
                Some(seq(15)),
            ),
            StallAction::ReportStall
        );
    }

    /// No pending in-memory batch: distance is the raw cursor-to-head gap.
    #[test]
    fn catch_up_distance_with_no_memory_uses_the_head() {
        assert_eq!(catch_up_distance(10, 20, None), 10);
    }

    /// A pending in-memory batch bounds the distance to just below it,
    /// regardless of how far ahead the (memory-inflated) head is — this is
    /// what stops a large local batch from making an unrelated small
    /// frontier gap look like a full page behind.
    #[test]
    fn catch_up_distance_with_memory_bounds_to_just_below_it() {
        assert_eq!(catch_up_distance(10, 200_010, Some(seq(20))), 9);
    }

    /// A pending in-memory sequence beyond the head (should not happen in
    /// practice, since `accept` advances the head before enqueueing, but the
    /// clamp is cheap insurance) never produces a distance larger than the
    /// raw cursor-to-head gap.
    #[test]
    fn catch_up_distance_clamps_memory_beyond_head() {
        assert_eq!(catch_up_distance(10, 20, Some(seq(1000))), 10);
    }
}
