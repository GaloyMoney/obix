use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{broadcast, mpsc};
use tracing::Instrument;

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use super::feeder::{self, CacheFeeder, CatchUpOutcome, FeederHandle, FeederReport};
use crate::{
    config::*,
    handle::{OwnedTaskHandle, spawn_supervised},
    out::{
        event::*,
        gap_fill::GapFillRequest,
        lane::{InsertOrder, LaneHandle},
        pg_notify::NotifyMessage,
    },
    sequence::EventSequence,
};

/// What this cache hands a listener — the insert lane's
/// [`LaneHandle`](crate::out::lane::LaneHandle).
pub type CacheHandle<P> = LaneHandle<InsertOrder, P>;

/// The insert lane's internal transport: a [`PersistentDelivery`] positioned
/// at its own sequence.
type InsertTransport<P> = Transport<InsertOrder, P>;

/// Outcome of parsing a `{min_sequence, max_sequence}` notification.
struct NotifiedRange {
    /// Lowest sequence the notification proves committed.
    min_sequence: EventSequence,
    /// Highest sequence the notification proves committed.
    max_sequence: EventSequence,
    /// Sub-range of the notified sequences missing from the cache as
    /// `(after, up_to)` — exclusive lower bound, inclusive upper — or
    /// `None` when every notified sequence is already cached (the warm
    /// in-process path: the post-commit broadcast beat the NOTIFY).
    missing: Option<(EventSequence, EventSequence)>,
}

/// The cursor's position relative to everything that could advance it,
/// recomputed every loop iteration.
struct CursorLag {
    cursor: EventSequence,
    /// `cursor + 1` is at or below the head but uncached: either a committed
    /// backlog or a hole — which one is not known yet.
    behind: bool,
    /// Distance to whatever memory cannot supply, per [`distance_to_unfed`].
    distance: u64,
    /// The feeder's front, per [`FeederHandle::memory_next`].
    memory_next: Option<EventSequence>,
}

/// Distance from `cursor` to whatever memory cannot supply: just below the
/// front when there is one, else the head. Raw `head - cursor` would let a
/// large pending batch make a small gap below it look like a page behind.
fn distance_to_unfed(
    cursor: EventSequence,
    head: EventSequence,
    memory_next: Option<EventSequence>,
) -> u64 {
    let bound = memory_next.map_or(head, |front| front.prev().min(head));
    cursor.distance_to(bound)
}

/// The cache loop's stall and catch-up bookkeeping.
#[derive(Default)]
struct StallTracker {
    /// Position last reported to the GapFiller: reported once per stall.
    reported: Option<EventSequence>,
    /// Position a catch-up proved a hole at, so it is not re-probed every
    /// iteration.
    exhausted_at: Option<EventSequence>,
    /// A catch-up request is outstanding — it owns progress until it answers.
    catch_up_active: bool,
}

impl StallTracker {
    /// Lost-signal backstop: re-report a stall that may never have resolved,
    /// and re-probe a position that may since have filled.
    fn rearm(&mut self) {
        self.reported = None;
        self.exhausted_at = None;
    }

    /// Committed rows may have landed past the exhausted position; any
    /// reported stall is left alone.
    fn rearm_exhaustion(&mut self) {
        self.exhausted_at = None;
    }

    fn catch_up_requested(&mut self) {
        self.catch_up_active = true;
    }

    /// Record a finished catch-up; `true` when the GapFiller's episode should
    /// now be cleared. A run that merely re-discovers the position already
    /// reported must leave that episode's grace clock alone.
    fn catch_up_finished(&mut self, outcome: CatchUpOutcome) -> bool {
        self.catch_up_active = false;
        match outcome {
            CatchUpOutcome::AtHead | CatchUpOutcome::ReachedMemory => {
                self.exhausted_at = None;
                self.reported.take().is_some()
            }
            CatchUpOutcome::HoleAfter(pos) => {
                self.exhausted_at = Some(pos);
                let moved_on = self.reported.is_some_and(|at| at != pos);
                if moved_on {
                    self.reported = None;
                }
                moved_on
            }
        }
    }

    fn reported_at(&mut self, cursor: EventSequence) {
        self.reported = Some(cursor);
    }

    fn cleared(&mut self) {
        self.reported = None;
    }
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

impl StallAction {
    /// The behind-vs-hole rule: *behind* only means `cursor + 1` is uncached,
    /// which is a committed backlog the feeder can drain or a hole the
    /// GapFiller must resolve. Pure, so every branch is unit-testable.
    fn determine(lag: &CursorLag, stall: &StallTracker, threshold: u64) -> Self {
        if !lag.behind {
            return if stall.reported.is_some() {
                Self::ClearStall
            } else {
                Self::Nothing
            };
        }
        // Something else already owns progress toward this position.
        if lag.memory_next == Some(lag.cursor.next()) || stall.catch_up_active {
            return Self::Nothing;
        }
        if stall.exhausted_at != Some(lag.cursor) && lag.distance >= threshold {
            return Self::RequestCatchUp;
        }
        if stall.reported != Some(lag.cursor) {
            Self::ReportStall
        } else {
            Self::Nothing
        }
    }
}

#[derive(Debug)]
pub struct PersistentOutboxEventCache<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    highest_known_sequence: Arc<AtomicU64>,
    persistent_event_sender: broadcast::Sender<InsertTransport<P>>,
    backfill_request_send: mpsc::UnboundedSender<(EventSequence, mpsc::Sender<InsertTransport<P>>)>,
    backfill_buffer_size: usize,
    cache_fill_sender: broadcast::Sender<InsertTransport<P>>,
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
        LaneHandle::new(
            self.highest_known_sequence.clone(),
            self.persistent_event_sender.subscribe(),
            self.backfill_request_send.clone(),
            self.backfill_buffer_size,
        )
    }

    pub fn cache_fill_sender(&self) -> broadcast::Sender<InsertTransport<P>> {
        self.cache_fill_sender.clone()
    }

    /// The hook's entire cache-facing surface — see [`CacheFeeder::accept`].
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

        let (feeder, feeder_handle, feeder_task) = feeder::spawn::<P, Tables>(
            pool.clone(),
            cache_fill_send.clone(),
            highest_known_sequence.clone(),
            config,
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
            feeder_handle,
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
            _feeder_handle: feeder_task,
            _phantom: std::marker::PhantomData,
        };
        Ok(ret)
    }

    fn insert_into_cache_and_maybe_broadcast(
        cache: im::OrdMap<EventSequence, InsertTransport<P>>,
        event: InsertTransport<P>,
        highest_known_sequence: &AtomicU64,
        persistent_event_sender: &broadcast::Sender<InsertTransport<P>>,
        mut last_broadcast_sequence: EventSequence,
        cache_size: usize,
    ) -> (im::OrdMap<EventSequence, InsertTransport<P>>, EventSequence) {
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
        mut wakeup: broadcast::Receiver<InsertTransport<P>>,
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
        sender: mpsc::Sender<InsertTransport<P>>,
        cache_snapshot: im::OrdMap<EventSequence, InsertTransport<P>>,
        cache_fill_sender: broadcast::Sender<InsertTransport<P>>,
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
                let delivery = InsertTransport::insert(PersistentDelivery::from(item));
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
        cache_fill_sender: broadcast::Sender<InsertTransport<P>>,
    ) {
        if let Ok(events) = Tables::load_events_in_range::<P>(&pool, after, up_to).await {
            for item in events {
                let _ =
                    cache_fill_sender.send(InsertTransport::insert(PersistentDelivery::from(item)));
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
        cache: &im::OrdMap<EventSequence, InsertTransport<P>>,
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
        persistent_event_sender: broadcast::Sender<InsertTransport<P>>,
        highest_known_sequence: Arc<AtomicU64>,
        mut backfill_request: mpsc::UnboundedReceiver<(
            EventSequence,
            mpsc::Sender<InsertTransport<P>>,
        )>,
        mut cache_fill_receiver: broadcast::Receiver<InsertTransport<P>>,
        cache_fill_sender: broadcast::Sender<InsertTransport<P>>,
        mut notification_receiver: mpsc::Receiver<NotifyMessage>,
        gap_fill_tx: mpsc::UnboundedSender<GapFillRequest>,
        mut feeder: FeederHandle,
    ) -> Result<OwnedTaskHandle, sqlx::Error> {
        let pool = pool.clone();

        let cache_size = config.event_cache_size;
        let backfill_page_size = config.backfill_page_size.max(1);
        let high_water = cache_size * (100 + config.event_cache_trim_percent as usize) / 100;
        let low_water = cache_size * (100 - config.event_cache_trim_percent as usize) / 100;
        let idle_resync_interval = config.idle_resync_interval;

        let initial_sequence = EventSequence::from(highest_known_sequence.load(Ordering::Relaxed));

        let handle = spawn_supervised("obix::persistent_cache_loop", async move {
            let mut persistent_cache: im::OrdMap<EventSequence, InsertTransport<P>> =
                im::OrdMap::new();
            let mut last_broadcast_sequence = initial_sequence;
            let mut stall = StallTracker::default();
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

                    // No `continue` on either report: both fall through to
                    // the decision block, which re-runs the rule against a
                    // fresh front instead of waiting for an unrelated wake.
                    report = feeder.next_report() => {
                        match report {
                            FeederReport::CaughtUp(outcome) => {
                                if stall.catch_up_finished(outcome) {
                                    let _ = gap_fill_tx.send(GapFillRequest::StallCleared);
                                }
                            }
                            FeederReport::FrontMoved => {}
                            FeederReport::Gone => {
                                record_feeder_gone();
                                break;
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
                                // A dropped delivery may have been the fill
                                // that resolved the stall, or have carried
                                // rows past the exhausted position.
                                stall.rearm();
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
                                                // Claims a sequence at or past
                                                // the hole landed: retry a
                                                // stale exhaustion.
                                                if notified.min_sequence
                                                    <= last_broadcast_sequence.next()
                                                {
                                                    stall.rearm_exhaustion();
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
                                            stall.rearm_exhaustion();
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
                                                // Wider than a page: catch-up
                                                // reads it on arrival. Re-armed
                                                // only as this reaches back.
                                                stall.rearm_exhaustion();
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
                        stall.rearm();
                        last_progress_at = tokio::time::Instant::now();
                    }
                }

                // All fill policy (grace, abandonment proof, batching,
                // cluster dedup) stays in the GapFiller; this loop only
                // observes its own cursor and who owns progress toward it.
                let cursor = last_broadcast_sequence;
                feeder.cursor_reached(cursor);
                let head = EventSequence::from(highest_known_sequence.load(Ordering::Relaxed));
                let memory_next = feeder.memory_next(cursor);
                let lag = CursorLag {
                    cursor,
                    behind: cursor.next() <= head && !persistent_cache.contains_key(&cursor.next()),
                    distance: distance_to_unfed(cursor, head, memory_next),
                    memory_next,
                };

                match StallAction::determine(&lag, &stall, backfill_page_size as u64) {
                    StallAction::Nothing => {}
                    StallAction::ClearStall => {
                        stall.cleared();
                        let _ = gap_fill_tx.send(GapFillRequest::StallCleared);
                    }
                    StallAction::RequestCatchUp => {
                        // Deliberately not `cleared()`: starting a catch-up
                        // is not progress, and an episode already working
                        // this position must keep its grace clock.
                        feeder.request_catch_up();
                        stall.catch_up_requested();
                    }
                    StallAction::ReportStall => {
                        let _ = gap_fill_tx.send(GapFillRequest::Stalled(cursor));
                        stall.reported_at(cursor);
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
    name = "obix.persistent_cache.feeder_gone",
    level = "error",
    fields(otel.status_code = "ERROR"),
)]
fn record_feeder_gone() {}

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

    /// Behind at `cursor`, `distance` from whatever memory cannot supply.
    fn behind(cursor: u64, distance: u64) -> CursorLag {
        CursorLag {
            cursor: seq(cursor),
            behind: true,
            distance,
            memory_next: None,
        }
    }

    fn determine(lag: &CursorLag, stall: &StallTracker) -> StallAction {
        StallAction::determine(lag, stall, THRESHOLD)
    }

    /// Not behind, no stall previously reported: nothing to do.
    #[test]
    fn not_behind_and_clean_does_nothing() {
        let lag = CursorLag {
            behind: false,
            ..behind(10, 0)
        };
        assert_eq!(
            determine(&lag, &StallTracker::default()),
            StallAction::Nothing
        );
    }

    /// Not behind, but a stall was reported (the cursor just caught up to
    /// it): clear it.
    #[test]
    fn not_behind_with_reported_stall_clears_it() {
        let lag = CursorLag {
            behind: false,
            ..behind(10, 0)
        };
        let stall = StallTracker {
            reported: Some(seq(10)),
            ..Default::default()
        };
        assert_eq!(determine(&lag, &stall), StallAction::ClearStall);
    }

    /// A catch-up is already running: it owns progress, never report a
    /// stall out from under it.
    #[test]
    fn behind_with_catch_up_active_does_nothing() {
        let stall = StallTracker {
            catch_up_active: true,
            ..Default::default()
        };
        assert_eq!(
            determine(&behind(10, THRESHOLD * 5), &stall),
            StallAction::Nothing
        );
    }

    /// Distance at the threshold requests a catch-up rather than reporting a
    /// stall — the boundary is inclusive.
    #[test]
    fn distance_at_threshold_requests_catch_up() {
        assert_eq!(
            determine(&behind(10, THRESHOLD), &StallTracker::default()),
            StallAction::RequestCatchUp
        );
    }

    /// One below the threshold reports a stall instead — the boundary the
    /// test above pins is exact, not "roughly there".
    #[test]
    fn distance_below_threshold_reports_stall() {
        assert_eq!(
            determine(&behind(10, THRESHOLD - 1), &StallTracker::default()),
            StallAction::ReportStall
        );
    }

    /// Already reported at this exact cursor: don't resend every iteration.
    #[test]
    fn distance_below_threshold_already_reported_does_nothing() {
        let stall = StallTracker {
            reported: Some(seq(10)),
            ..Default::default()
        };
        assert_eq!(
            determine(&behind(10, THRESHOLD - 1), &stall),
            StallAction::Nothing
        );
    }

    /// A catch-up already proved this exact position a hole: past-threshold
    /// distance does not re-probe it every iteration, it falls back to the
    /// (unchanged) stall path.
    #[test]
    fn exhausted_at_cursor_falls_back_to_stall_report() {
        let stall = StallTracker {
            exhausted_at: Some(seq(10)),
            ..Default::default()
        };
        assert_eq!(
            determine(&behind(10, THRESHOLD * 5), &stall),
            StallAction::ReportStall
        );
    }

    /// The cursor moved past the position a prior catch-up exhausted: the
    /// stale exhaustion no longer applies, so a far-enough distance requests
    /// a fresh catch-up.
    #[test]
    fn exhausted_at_stale_position_requests_catch_up() {
        let stall = StallTracker {
            exhausted_at: Some(seq(3)),
            ..Default::default()
        };
        assert_eq!(
            determine(&behind(10, THRESHOLD * 5), &stall),
            StallAction::RequestCatchUp
        );
    }

    /// The feeder is already feeding this position: do nothing, even with a
    /// distance far past the threshold. Exercised directly, so a caller that
    /// forgot `distance_to_unfed` cannot pass by accident.
    #[test]
    fn memory_next_at_cursor_plus_one_does_nothing_even_when_far_behind() {
        let lag = CursorLag {
            memory_next: Some(seq(11)),
            ..behind(10, THRESHOLD * 5)
        };
        assert_eq!(
            determine(&lag, &StallTracker::default()),
            StallAction::Nothing
        );
    }

    /// A pending batch further ahead than `cursor + 1` short-circuits
    /// nothing: the ordinary threshold comparison still governs.
    #[test]
    fn memory_next_further_ahead_falls_through_to_the_rule() {
        let at_threshold = CursorLag {
            memory_next: Some(seq(15)),
            ..behind(10, THRESHOLD)
        };
        assert_eq!(
            determine(&at_threshold, &StallTracker::default()),
            StallAction::RequestCatchUp
        );
        let below = CursorLag {
            memory_next: Some(seq(15)),
            ..behind(10, THRESHOLD - 1)
        };
        assert_eq!(
            determine(&below, &StallTracker::default()),
            StallAction::ReportStall
        );
    }

    /// An `AtHead` outcome clears a reported stall; a hole at the same
    /// position leaves that episode's grace clock alone.
    #[test]
    fn catch_up_outcomes_only_clear_an_episode_on_progress() {
        let mut stall = StallTracker {
            reported: Some(seq(10)),
            catch_up_active: true,
            ..Default::default()
        };
        assert!(stall.catch_up_finished(CatchUpOutcome::AtHead));
        assert!(!stall.catch_up_active);

        let mut stall = StallTracker {
            reported: Some(seq(10)),
            ..Default::default()
        };
        assert!(!stall.catch_up_finished(CatchUpOutcome::HoleAfter(seq(10))));
        assert_eq!(stall.reported, Some(seq(10)));
        assert_eq!(stall.exhausted_at, Some(seq(10)));
    }

    /// A hole at a *different* position than the one reported is progress:
    /// clear the old episode.
    #[test]
    fn a_hole_at_a_new_position_clears_the_old_episode() {
        let mut stall = StallTracker {
            reported: Some(seq(3)),
            ..Default::default()
        };
        assert!(stall.catch_up_finished(CatchUpOutcome::HoleAfter(seq(10))));
        assert_eq!(stall.reported, None);
    }

    /// No pending in-memory batch: distance is the raw cursor-to-head gap.
    #[test]
    fn distance_with_no_memory_uses_the_head() {
        assert_eq!(distance_to_unfed(seq(10), seq(20), None), 10);
    }

    /// A pending batch bounds the distance to just below it, however far
    /// ahead the (memory-inflated) head is — what stops a large local batch
    /// from making an unrelated small gap look like a whole page behind.
    #[test]
    fn distance_with_memory_bounds_to_just_below_it() {
        assert_eq!(distance_to_unfed(seq(10), seq(200_010), Some(seq(20))), 9);
    }

    /// A pending sequence beyond the head (`accept` advances the head first,
    /// so this should not arise) never exceeds the raw cursor-to-head gap.
    #[test]
    fn distance_clamps_memory_beyond_head() {
        assert_eq!(distance_to_unfed(seq(10), seq(20), Some(seq(1000))), 10);
    }
}
