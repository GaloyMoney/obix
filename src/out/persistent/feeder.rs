//! One long-lived task per cache, feeding committed rows into the cache-fill
//! broadcast from memory (a local commit's own batch) or, only when asked,
//! from the database. `cache.rs` decides *whether*; this decides *how far*.

use std::marker::PhantomData;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{broadcast, mpsc, watch};
use tracing::Instrument;

use crate::{
    config::MailboxConfig,
    handle::{OwnedTaskHandle, spawn_supervised},
    out::{
        event::{PersistentDelivery, PersistentOutboxEvent, Transport},
        lane::InsertOrder,
    },
    sequence::EventSequence,
    tables::MailboxTables,
};

/// The insert lane's internal transport, positioned at its own sequence —
/// what the cache-fill broadcast actually carries. Matches
/// `cache::InsertTransport`, kept as its own alias since that one is private
/// to `cache.rs`.
type InsertTransport<P> = Transport<InsertOrder, P>;

/// The cache loop asking for a database read. Carries no policy — the loop
/// already decided.
pub(crate) enum FeedRequest {
    CatchUp,
}

/// How one answered [`FeedRequest`] ended.
pub(crate) enum CatchUpOutcome {
    /// Reached the head with nothing pending ahead: nothing left to do.
    AtHead,
    /// Stopped at `pos` because `pos + 1` is missing from the table — a hole,
    /// not a backlog. Handed to the grace-gated stall path.
    HoleAfter(EventSequence),
    /// Reached the first pending in-memory sequence with no hole: memory
    /// takes over. Genuine progress, same as `AtHead`.
    ReachedMemory,
}

/// What the feeder reports back to the cache loop.
pub(crate) enum FeederReport {
    CaughtUp(CatchUpOutcome),
    /// The front moved: re-run the decision rule now rather than waiting for
    /// an unrelated wake-up.
    FrontMoved,
    /// The feeder task is gone.
    Gone,
}

/// How far a database read may go, and what reaching it means.
enum ReadBound {
    /// Just below the front — memory feeds from there on.
    Memory(EventSequence),
    /// Nothing pending in memory, so the head bounds the read.
    Head(EventSequence),
}

impl ReadBound {
    fn until(&self) -> EventSequence {
        match self {
            Self::Memory(until) | Self::Head(until) => *until,
        }
    }

    fn reached(&self) -> CatchUpOutcome {
        match self {
            Self::Memory(_) => CatchUpOutcome::ReachedMemory,
            Self::Head(_) => CatchUpOutcome::AtHead,
        }
    }
}

/// How long the feeder waits for the cache loop to consume a delivered page
/// before re-deriving from wherever the cursor actually reached.
const CONSUME_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

/// How long a failed page read backs off. Mirrors
/// `PersistentOutboxEventCache::BACKFILL_RETRY_INTERVAL`.
const RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

/// What `PersistEvents::post_commit` needs from the cache: this one call.
pub(crate) struct CacheFeeder<P>
where
    P: Serialize + DeserializeOwned + Send,
{
    batches: mpsc::UnboundedSender<Vec<InsertTransport<P>>>,
    highest_known: Arc<AtomicU64>,
    front: watch::Sender<Option<EventSequence>>,
}

impl<P> std::fmt::Debug for CacheFeeder<P>
where
    P: Serialize + DeserializeOwned + Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheFeeder").finish_non_exhaustive()
    }
}

impl<P> Clone for CacheFeeder<P>
where
    P: Serialize + DeserializeOwned + Send,
{
    fn clone(&self) -> Self {
        Self {
            batches: self.batches.clone(),
            highest_known: self.highest_known.clone(),
            front: self.front.clone(),
        }
    }
}

impl<P> CacheFeeder<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Hand a committed batch to the cache, still owned. INVARIANT: head and
    /// front both advance here before it is queued — else the loop sees the
    /// head jump with a stale front and re-reads rows already queued.
    pub(crate) fn accept(&self, mut batch: Vec<PersistentOutboxEvent<P>>) {
        if batch.is_empty() {
            return;
        }
        // Ascending by construction (`persist_events` returns each chunk
        // ordered, and sequences allocate in order), so this is a defensive
        // fallback — free on the happy path, required before trusting `last`.
        if !batch.is_sorted_by_key(|e| e.sequence) {
            batch.sort_by_key(|e| e.sequence);
        }
        let first = batch.first().expect("checked non-empty above").sequence;
        let last = batch.last().expect("checked non-empty above").sequence;
        self.highest_known
            .fetch_max(u64::from(last), Ordering::AcqRel);
        // Only ever narrowed, never widened: the feeder task overwrites this
        // with the authoritative value (retained batches included) on its next
        // iteration, so a running minimum can only name a real pending row.
        self.front.send_modify(|current| {
            *current = Some(match *current {
                Some(existing) => existing.min(first),
                None => first,
            });
        });
        // `PersistentDelivery` clones cheaply (Arc) for every re-send a paced
        // feed needs, with no `P: Clone` bound.
        let deliveries: Vec<InsertTransport<P>> = batch
            .into_iter()
            .map(|event| InsertTransport::insert(PersistentDelivery::from(Ok(event))))
            .collect();
        let _ = self.batches.send(deliveries);
    }
}

/// What the cache loop needs from the feeder: one value in place of four
/// channel ends.
pub(crate) struct FeederHandle {
    cursor: watch::Sender<EventSequence>,
    requests: mpsc::UnboundedSender<FeedRequest>,
    outcomes: mpsc::UnboundedReceiver<CatchUpOutcome>,
    front: watch::Receiver<Option<EventSequence>>,
}

impl FeederHandle {
    /// Publish the broadcast cursor so the feeder can pace itself off it.
    pub(crate) fn cursor_reached(&self, cursor: EventSequence) {
        self.cursor.send_replace(cursor);
    }

    pub(crate) fn request_catch_up(&self) {
        let _ = self.requests.send(FeedRequest::CatchUp);
    }

    /// The smallest pending in-memory sequence above `cursor`, clamped up to
    /// `cursor + 1`: the feeder publishes its front *before* sending that
    /// page, so a trailing value means "feeding here", not "drained".
    pub(crate) fn memory_next(&self, cursor: EventSequence) -> Option<EventSequence> {
        (*self.front.borrow()).map(|front| front.max(cursor.next()))
    }

    /// Cancel-safe — both halves are, so this composes inside the cache
    /// loop's `select!`. Either channel closing means the task is gone.
    pub(crate) async fn next_report(&mut self) -> FeederReport {
        tokio::select! {
            biased;

            outcome = self.outcomes.recv() => match outcome {
                Some(outcome) => FeederReport::CaughtUp(outcome),
                None => FeederReport::Gone,
            },
            changed = self.front.changed() => match changed {
                Ok(()) => FeederReport::FrontMoved,
                Err(_) => FeederReport::Gone,
            },
        }
    }
}

/// A committed batch the feeder holds, unfed or partially fed. Kept out of
/// the cache's `OrdMap` deliberately: its trim evicts the lowest entries even
/// above the cursor, which would drop a large batch's unfed tail.
struct PendingBatch<P>
where
    P: Serialize + DeserializeOwned + Send,
{
    batch: Vec<InsertTransport<P>>,
    /// Opened on the first feed and reused for every later page — one
    /// `memory_feed` span per batch, never per page.
    span: Option<tracing::Span>,
    pages_sent: u64,
}

impl<P> PendingBatch<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn new(batch: Vec<InsertTransport<P>>) -> Self {
        Self {
            batch,
            span: None,
            pages_sent: 0,
        }
    }

    /// `BEGIN` for an empty batch, which `accept` never enqueues.
    fn last_sequence(&self) -> EventSequence {
        self.batch
            .last()
            .map(|e| e.sequence())
            .unwrap_or(EventSequence::BEGIN)
    }

    /// Send one page from `pos` (guaranteed in range by `next_after`) and
    /// return the sequence it ended on. Unasked-for: the loop reads
    /// `memory_next == cursor + 1` as "feeding in progress" and stays quiet.
    fn feed_page(
        &mut self,
        cache_fill: &broadcast::Sender<InsertTransport<P>>,
        pos: usize,
        page_size: usize,
    ) -> EventSequence {
        let span = self.span.get_or_insert_with(|| {
            tracing::info_span!(
                "obix.persistent_cache.memory_feed",
                first = self.batch.first().map(|e| u64::from(e.sequence())),
                last = self.batch.last().map(|e| u64::from(e.sequence())),
                rows = self.batch.len() as u64,
                pages = tracing::field::Empty,
            )
        });
        let end = (pos + page_size).min(self.batch.len());
        let _enter = span.enter();
        for delivery in &self.batch[pos..end] {
            let _ = cache_fill.send(delivery.clone());
        }
        self.pages_sent += 1;
        span.record("pages", self.pages_sent);
        self.batch[end - 1].sequence()
    }
}

/// The smallest pending sequence strictly above `cursor`, as
/// `(batch index, position)`. A linear scan is fine: `pending`'s length is
/// bounded by concurrent commits, never by event volume.
fn next_after<P>(pending: &[PendingBatch<P>], cursor: EventSequence) -> Option<(usize, usize)>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    let mut best: Option<(usize, usize, EventSequence)> = None;
    for (batch_idx, pending_batch) in pending.iter().enumerate() {
        let pos = pending_batch
            .batch
            .partition_point(|e| e.sequence() <= cursor);
        let Some(delivery) = pending_batch.batch.get(pos) else {
            continue;
        };
        let seq = delivery.sequence();
        if best.is_none_or(|(_, _, best_seq)| seq < best_seq) {
            best = Some((batch_idx, pos, seq));
        }
    }
    best.map(|(i, p, _)| (i, p))
}

/// Why one iteration of the feeder loop woke up.
enum Wake<P>
where
    P: Serialize + DeserializeOwned + Send,
{
    Batch(Vec<InsertTransport<P>>),
    CatchUp,
    CursorMoved,
    Closed,
}

/// Create the feeder, spawn its task, and return both sides: the hook's
/// [`CacheFeeder`] and the cache loop's [`FeederHandle`].
pub(crate) fn spawn<P, Tables>(
    pool: sqlx::PgPool,
    cache_fill: broadcast::Sender<InsertTransport<P>>,
    highest_known: Arc<AtomicU64>,
    config: &MailboxConfig,
) -> (CacheFeeder<P>, FeederHandle, OwnedTaskHandle)
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
    Tables: MailboxTables,
{
    let head = EventSequence::from(highest_known.load(Ordering::Relaxed));
    let (cursor_tx, cursor_rx) = watch::channel(head);
    let (request_tx, request_rx) = mpsc::unbounded_channel();
    let (outcome_tx, outcome_rx) = mpsc::unbounded_channel();
    let (batch_tx, batch_rx) = mpsc::unbounded_channel();
    let (front_tx, front_rx) = watch::channel(None);

    // A fed page goes into the cache-fill broadcast, so a page wider than that
    // channel would lag the very channel it is refilling.
    let page_size = config
        .backfill_page_size
        .clamp(1, (config.event_buffer_size / 2).max(1));

    let feeder = Feeder::<P, Tables> {
        pool,
        cache_fill,
        cursor: cursor_rx,
        highest_known: highest_known.clone(),
        page_size,
        batches: batch_rx,
        requests: request_rx,
        outcomes: outcome_tx,
        front: front_tx.clone(),
        pending: Vec::new(),
        _tables: PhantomData,
    };
    let task = spawn_supervised("obix::persistent_cache_feeder", feeder.run());

    (
        CacheFeeder {
            batches: batch_tx,
            highest_known,
            front: front_tx,
        },
        FeederHandle {
            cursor: cursor_tx,
            requests: request_tx,
            outcomes: outcome_rx,
            front: front_rx,
        },
        OwnedTaskHandle::new(task),
    )
}

/// The feeder task: the other end of every channel in [`FeederHandle`] and
/// [`CacheFeeder`], plus the batches it is still holding.
struct Feeder<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pool: sqlx::PgPool,
    cache_fill: broadcast::Sender<InsertTransport<P>>,
    cursor: watch::Receiver<EventSequence>,
    highest_known: Arc<AtomicU64>,
    page_size: usize,
    batches: mpsc::UnboundedReceiver<Vec<InsertTransport<P>>>,
    requests: mpsc::UnboundedReceiver<FeedRequest>,
    outcomes: mpsc::UnboundedSender<CatchUpOutcome>,
    front: watch::Sender<Option<EventSequence>>,
    pending: Vec<PendingBatch<P>>,
    _tables: PhantomData<Tables>,
}

impl<P, Tables> Feeder<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
    Tables: MailboxTables,
{
    /// Take everything that arrived, drop what the cursor has passed, and
    /// return the cursor it was reconciled against.
    fn reconcile(&mut self) -> EventSequence {
        while let Ok(batch) = self.batches.try_recv() {
            self.pending.push(PendingBatch::new(batch));
        }
        let cursor = *self.cursor.borrow();
        self.pending.retain(|b| b.last_sequence() > cursor);
        cursor
    }

    fn read_bound(&self, cursor: EventSequence) -> ReadBound {
        match next_after(&self.pending, cursor) {
            Some((i, p)) => ReadBound::Memory(self.pending[i].batch[p].sequence().prev()),
            None => ReadBound::Head(EventSequence::from(
                self.highest_known.load(Ordering::Relaxed),
            )),
        }
    }

    /// Publish the front for the cache loop's decision rule. Silent when
    /// unchanged: the loop republishes the cursor every iteration, and waking
    /// it back with an identical front costs ~40% more CPU across `catch_up`.
    fn publish_front(&self, front: Option<EventSequence>) {
        self.front.send_if_modified(|current| {
            let moved = *current != front;
            *current = front;
            moved
        });
    }

    /// One page in flight: wait for the cache loop to consume it before the
    /// next. A timeout only means the loop is behind — re-derive and retry.
    async fn await_consumed(&mut self, through: EventSequence) {
        let _ =
            tokio::time::timeout(CONSUME_TIMEOUT, self.cursor.wait_for(|c| *c >= through)).await;
    }

    /// Reconcile, publish the front, feed a memory page if one is next in
    /// line, else park. The loop's `catch_up_active` gate keeps at most one
    /// request outstanding, so only one read is ever in flight.
    async fn run(mut self) {
        loop {
            let cursor = self.reconcile();
            let front = next_after(&self.pending, cursor);
            let memory_next = front.map(|(i, p)| self.pending[i].batch[p].sequence());
            self.publish_front(memory_next);

            if memory_next == Some(cursor.next()) {
                let (batch_idx, pos) = front.expect("memory_next came from front");
                let page_size = self.page_size;
                let last = self.pending[batch_idx].feed_page(&self.cache_fill, pos, page_size);
                self.await_consumed(last).await;
                continue;
            }

            let wake = tokio::select! {
                biased;

                batch = self.batches.recv() => match batch {
                    Some(batch) => Wake::Batch(batch),
                    None => Wake::Closed,
                },
                request = self.requests.recv() => match request {
                    Some(FeedRequest::CatchUp) => Wake::CatchUp,
                    None => Wake::Closed,
                },
                _ = self.cursor.changed() => Wake::CursorMoved,
            };

            match wake {
                Wake::Batch(batch) => self.pending.push(PendingBatch::new(batch)),
                Wake::CatchUp => {
                    let outcome = self.db_catch_up().await;
                    let _ = self.outcomes.send(outcome);
                }
                Wake::CursorMoved => {}
                Wake::Closed => return,
            }
        }
    }

    /// Drain a backlog of *committed* rows at DB speed, bounded at the front
    /// rather than the head. Bound and cursor are re-derived every iteration,
    /// so a batch arriving mid-read lowers the bound at once. Never writes.
    async fn db_catch_up(&mut self) -> CatchUpOutcome {
        let cursor = self.reconcile();
        let bound = self.read_bound(cursor);
        if cursor >= bound.until() {
            return bound.reached();
        }

        // Opened only now, after the no-op early return above, so "zero
        // catch-up spans" is assertable for a purely memory-fed backlog.
        let span = tracing::info_span!(
            "obix.persistent_cache.catch_up",
            from = u64::from(cursor),
            to = tracing::field::Empty,
            pages = tracing::field::Empty,
            rows = tracing::field::Empty,
            outcome = tracing::field::Empty,
        );
        let mut pages: u64 = 0;
        let mut rows: u64 = 0;
        let outcome = async {
            loop {
                let cursor = self.reconcile();
                let bound = self.read_bound(cursor);
                if cursor >= bound.until() {
                    break bound.reached();
                }

                let read_size = self
                    .page_size
                    .min(cursor.distance_to(bound.until()) as usize)
                    .max(1);
                let events =
                    match Tables::load_next_contiguous_page::<P>(&self.pool, cursor, read_size)
                        .await
                    {
                        Ok(events) => events,
                        Err(e) => {
                            record_catch_up_failed(&e, u64::from(cursor));
                            tokio::time::sleep(RETRY_INTERVAL).await;
                            continue;
                        }
                    };
                pages += 1;

                let deliveries: Vec<InsertTransport<P>> = events
                    .into_iter()
                    .map(|event| InsertTransport::insert(PersistentDelivery::from(event)))
                    .collect();
                let Some(first) = deliveries.first() else {
                    break CatchUpOutcome::HoleAfter(cursor);
                };
                if first.sequence() != cursor.next() {
                    break CatchUpOutcome::HoleAfter(cursor);
                }
                rows += deliveries.len() as u64;
                let last = deliveries
                    .last()
                    .expect("checked non-empty above")
                    .sequence();
                for delivery in &deliveries {
                    let _ = self.cache_fill.send(delivery.clone());
                }
                self.await_consumed(last).await;
            }
        }
        .instrument(span.clone())
        .await;

        span.record("to", u64::from(*self.cursor.borrow()));
        span.record("pages", pages);
        span.record("rows", rows);
        span.record(
            "outcome",
            match &outcome {
                CatchUpOutcome::AtHead => "at_head",
                CatchUpOutcome::HoleAfter(_) => "hole",
                CatchUpOutcome::ReachedMemory => "reached_memory",
            },
        );
        outcome
    }
}

#[tracing::instrument(
    name = "obix.persistent_cache.catch_up_failed",
    level = "warn",
    skip_all,
    fields(error = %error, from = from),
)]
fn record_catch_up_failed(error: &sqlx::Error, from: u64) {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{out::event::OutboxEventId, sequence::CommitGroupId};

    /// `next_after` only inspects sequences, so a unit payload is all a batch
    /// needs here.
    fn delivery(seq: u64) -> InsertTransport<()> {
        InsertTransport::insert(PersistentDelivery::from(Ok(PersistentOutboxEvent {
            id: OutboxEventId::new(),
            sequence: EventSequence::from(seq),
            payload: None,
            tracing_context: None,
            recorded_at: chrono::Utc::now(),
            commit_group: CommitGroupId::from(0i64),
        })))
    }

    fn batch(seqs: &[u64]) -> PendingBatch<()> {
        PendingBatch::new(seqs.iter().copied().map(delivery).collect())
    }

    fn seq(n: u64) -> EventSequence {
        EventSequence::from(n)
    }

    /// Picks the smallest sequence above the cursor across batches whose
    /// ranges interleave rather than sit in arrival order — the point of the
    /// scan, not sorted input.
    #[test]
    fn picks_the_smallest_sequence_above_the_cursor_across_batches() {
        let pending = vec![batch(&[10, 11, 12]), batch(&[1, 2, 3]), batch(&[6, 7, 8])];
        assert_eq!(next_after(&pending, seq(2)), Some((1, 2)));
    }

    /// A batch entirely at or below the cursor contributes nothing.
    #[test]
    fn ignores_batches_entirely_at_or_below_the_cursor() {
        let pending = vec![batch(&[1, 2, 3]), batch(&[10, 11, 12])];
        assert_eq!(next_after(&pending, seq(3)), Some((1, 0)));
        assert_eq!(next_after(&pending, seq(12)), None);
    }

    /// Nothing pending at all.
    #[test]
    fn empty_pending_has_nothing_ahead() {
        let pending: Vec<PendingBatch<()>> = Vec::new();
        assert_eq!(next_after(&pending, seq(0)), None);
    }

    /// A handle plus the feeder-side senders, so a test can kill either half.
    fn handle() -> (
        FeederHandle,
        mpsc::UnboundedSender<CatchUpOutcome>,
        watch::Sender<Option<EventSequence>>,
    ) {
        let (cursor, _cursor_rx) = watch::channel(EventSequence::BEGIN);
        let (requests, _requests_rx) = mpsc::unbounded_channel();
        let (outcome_tx, outcomes) = mpsc::unbounded_channel();
        let (front_tx, front) = watch::channel(None);
        (
            FeederHandle {
                cursor,
                requests,
                outcomes,
                front,
            },
            outcome_tx,
            front_tx,
        )
    }

    /// A dead feeder must be *reported*, repeatedly, even while the other half
    /// still lives. An arm that instead resolved to "nothing happened" would
    /// spin the cache loop's `biased` select forever without ever breaking.
    #[tokio::test]
    async fn a_dead_outcome_channel_keeps_reporting_gone_while_the_front_lives() {
        let (mut handle, outcome_tx, front_tx) = handle();
        drop(outcome_tx);

        for _ in 0..3 {
            let report =
                tokio::time::timeout(std::time::Duration::from_secs(5), handle.next_report())
                    .await
                    .expect("must not block once the feeder is gone");
            assert!(matches!(report, FeederReport::Gone));
        }
        drop(front_tx);
    }

    /// The same, with the halves swapped: a closed front is equally fatal.
    #[tokio::test]
    async fn a_dead_front_channel_reports_gone_while_outcomes_live() {
        let (mut handle, outcome_tx, front_tx) = handle();
        drop(front_tx);

        let report = tokio::time::timeout(std::time::Duration::from_secs(5), handle.next_report())
            .await
            .expect("must not block once the feeder is gone");
        assert!(matches!(report, FeederReport::Gone));
        drop(outcome_tx);
    }

    /// A queued outcome is not lost to a closed front: `biased` polls outcomes
    /// first, so real progress is drained before `Gone` is reported.
    #[tokio::test]
    async fn a_queued_outcome_is_delivered_before_a_closed_front_reports_gone() {
        let (mut handle, outcome_tx, front_tx) = handle();
        outcome_tx
            .send(CatchUpOutcome::AtHead)
            .expect("handle holds the receiver");
        drop(front_tx);

        assert!(matches!(
            handle.next_report().await,
            FeederReport::CaughtUp(CatchUpOutcome::AtHead)
        ));
        drop(outcome_tx);
        assert!(matches!(handle.next_report().await, FeederReport::Gone));
    }
}
