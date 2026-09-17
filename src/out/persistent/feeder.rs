//! The feeder: a single long-lived task per persistent cache that feeds
//! committed rows into the cache-fill broadcast from two sources — already
//! in-memory commit batches (fed as soon as they are next-in-line, without
//! being asked) and the database (read only on request from the cache
//! loop's decision rule, `decide_stall_action` in `cache.rs`, and only for
//! whatever a pending in-memory batch cannot supply). This used to be two
//! things: `PersistEvents::post_commit` truncating a too-large batch and
//! re-reading the remainder from Postgres, and a `run_catch_up` task spawned
//! fresh per stall episode. Both are gone; the committed batch a
//! transaction already holds in memory is handed straight to this task via
//! [`CacheFeeder::accept`] and fed out as fast as the cursor consumes it.
//!
//! Policy split: this file decides nothing about *whether* to read from the
//! database — that is `decide_stall_action`'s job, entirely in `cache.rs`.
//! This task only executes *how* and *how far*: memory before DB, and DB
//! bounded to stop at the first pending in-memory sequence, because only
//! this task knows where that is.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{broadcast, mpsc, watch};
use tracing::Instrument;

use crate::{
    out::event::{PersistentDelivery, PersistentOutboxEvent},
    sequence::EventSequence,
    tables::MailboxTables,
};

/// A request from the cache loop for the feeder to read committed rows from
/// the database, starting at the cursor. The feeder decides nothing about
/// *whether* to read — `decide_stall_action` decides that; this is the loop
/// telling the feeder it decided yes.
pub(crate) enum FeedRequest {
    CatchUp,
}

/// Outcome of one feeder run answering a [`FeedRequest`], reported to the
/// cache loop over its done channel.
pub(crate) enum CatchUpOutcome {
    /// The cursor reached the head with no gap and no pending in-memory
    /// batch ahead of it — nothing left to catch up on.
    AtHead,
    /// The catch-up stopped at `pos` because `pos + 1` is missing from the
    /// table: a hole, not a backlog of committed rows. Handed to the same
    /// grace-gated stall path a stall has always used.
    HoleAfter(EventSequence),
    /// The database read reached the first pending in-memory sequence
    /// without finding a hole — memory takes over from here. Genuine
    /// progress, same as `AtHead`, for the cache loop's purposes.
    ReachedMemory,
}

/// How long the feeder waits for the cache loop to consume a delivered page
/// (observed via the cursor watch) before re-reading from wherever the real
/// cursor has reached. A hint only — a timeout just means the loop is
/// slower than expected, not that anything is wrong.
const CATCH_UP_CONSUME_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

/// How long a failed page read backs off before retrying. Mirrors
/// `PersistentOutboxEventCache::BACKFILL_RETRY_INTERVAL`.
const CATCH_UP_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

/// The handle `PersistEvents::post_commit` holds: the hook's entire
/// cache-facing surface is this one call.
pub(crate) struct CacheFeeder<P>
where
    P: Serialize + DeserializeOwned + Send,
{
    batches: mpsc::UnboundedSender<Vec<PersistentDelivery<P>>>,
    highest_known: Arc<AtomicU64>,
    /// Same channel the feeder task publishes `memory_next` on. `accept`
    /// writes a same-or-lower hint here synchronously, before returning —
    /// see the comment inside `accept` for why this is required, not an
    /// optimisation.
    front_tx: watch::Sender<Option<u64>>,
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
            front_tx: self.front_tx.clone(),
        }
    }
}

impl<P> CacheFeeder<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub(crate) fn new(
        batches: mpsc::UnboundedSender<Vec<PersistentDelivery<P>>>,
        highest_known: Arc<AtomicU64>,
        front_tx: watch::Sender<Option<u64>>,
    ) -> Self {
        Self {
            batches,
            highest_known,
            front_tx,
        }
    }

    /// Hand a committed batch to the cache, still owned — no DB round trip
    /// to read it back. Advances the head watermark to the batch's last
    /// sequence BEFORE enqueueing.
    ///
    /// INVARIANT: advance-before-send — the loop's decision rule must see
    /// the batch's tail as the head on its very next wake, never a stale
    /// one (an advance-after-send design pins the cursor at zero in any
    /// process with no receiver on the cache-fill stream, since tokio's
    /// broadcast `send` only errors on zero receivers).
    pub(crate) fn accept(&self, mut batch: Vec<PersistentOutboxEvent<P>>) {
        if batch.is_empty() {
            return;
        }
        // `persist_events` returns each chunk `ORDER BY sequence`, and
        // sequences within one transaction allocate in order, so a batch is
        // ascending by construction. The sort is a defensive fallback
        // against any `MailboxTables` implementation that doesn't hold that
        // — free on the happy path, and required before trusting `last()`
        // as the true maximum.
        if !batch.is_sorted_by_key(|e| e.sequence) {
            batch.sort_by_key(|e| e.sequence);
        }
        let first = batch.first().expect("checked non-empty above").sequence;
        let last = batch.last().expect("checked non-empty above").sequence;
        self.highest_known
            .fetch_max(u64::from(last), Ordering::AcqRel);
        // Narrow `front_tx` synchronously, in the same call that advances
        // `highest_known` — never widen it, only take the running minimum.
        // Without this, there is a real window between this advance and the
        // feeder task's next scheduled iteration where the cache loop can
        // observe `highest_known` already jumped ahead while `front_tx`
        // still reflects the *previous* state (`None` on the very first
        // batch): `catch_up_distance` then sees a full page of "distance"
        // that is actually this batch sitting in the channel about to be
        // fed, and requests a database catch-up that is stale by the time
        // the feeder — which always drains `batches_rx` before consuming a
        // request — gets around to answering it. The feeder's own loop
        // still overwrites this with the authoritative value (including
        // retained-out batches) every iteration; this is only a bridge
        // across that scheduling gap, always safe because it can only ever
        // report a real pending sequence, never a fabricated one.
        self.front_tx.send_modify(|current| {
            *current = Some(match *current {
                Some(existing) => existing.min(u64::from(first)),
                None => u64::from(first),
            });
        });
        // Converted once, up front: `PersistentDelivery` clones cheaply
        // (Arc) for every re-send a paced page feed needs, with no bound on
        // `P: Clone` — the same currency the DB catch-up path has always
        // used.
        let deliveries: Vec<PersistentDelivery<P>> = batch
            .into_iter()
            .map(|event| PersistentDelivery::from(Ok(event)))
            .collect();
        let _ = self.batches.send(deliveries);
    }
}

/// A committed batch the feeder holds, still unfed or partially fed. Kept
/// out of the cache's `OrdMap` deliberately: the cache trim evicts the
/// lowest entries even above the cursor, which would silently drop the
/// unfed tail of a large batch.
struct PendingBatch<P>
where
    P: Serialize + DeserializeOwned + Send,
{
    batch: Vec<PersistentDelivery<P>>,
    /// Opened on this batch's first feed, reused for every subsequent page
    /// of it — one `obix.persistent_cache.memory_feed` span per batch,
    /// never per page or per loop iteration.
    span: Option<tracing::Span>,
    pages_sent: u64,
}

impl<P> PendingBatch<P>
where
    P: Serialize + DeserializeOwned + Send,
{
    fn new(batch: Vec<PersistentDelivery<P>>) -> Self {
        Self {
            batch,
            span: None,
            pages_sent: 0,
        }
    }

    /// `0` for an empty batch, which `accept` never enqueues — a pending
    /// batch is never actually empty, but this avoids a panic if one ever
    /// were, at the cost of never being released by `retain`.
    fn last_sequence(&self) -> u64 {
        self.batch
            .last()
            .map(|e| u64::from(e.sequence()))
            .unwrap_or(0)
    }
}

/// Find the smallest pending sequence strictly greater than `cursor`, and
/// where it lives. A linear scan over `pending` is fine: its length is
/// bounded by the number of commits concurrently in flight, never by event
/// volume — a single batch can hold hundreds of thousands of rows, but
/// `pending` itself stays small.
fn next_after<P>(pending: &[PendingBatch<P>], cursor: u64) -> Option<(usize, usize)>
where
    P: Serialize + DeserializeOwned + Send,
{
    let mut best: Option<(usize, usize, u64)> = None;
    for (batch_idx, pending_batch) in pending.iter().enumerate() {
        let pos = pending_batch
            .batch
            .partition_point(|e| u64::from(e.sequence()) <= cursor);
        let Some(delivery) = pending_batch.batch.get(pos) else {
            continue;
        };
        let seq = u64::from(delivery.sequence());
        if best.is_none_or(|(_, _, best_seq)| seq < best_seq) {
            best = Some((batch_idx, pos, seq));
        }
    }
    best.map(|(i, p, _)| (i, p))
}

fn open_memory_feed_span<P>(
    batch: &[PersistentDelivery<P>],
    pending_batches: usize,
) -> tracing::Span
where
    P: Serialize + DeserializeOwned + Send,
{
    let first = batch.first().map(|e| u64::from(e.sequence())).unwrap_or(0);
    let last = batch.last().map(|e| u64::from(e.sequence())).unwrap_or(0);
    tracing::info_span!(
        "obix.persistent_cache.memory_feed",
        first = first,
        last = last,
        rows = batch.len() as u64,
        pages = tracing::field::Empty,
        pending_batches = pending_batches as u64,
    )
}

/// Feed one page from an already-known-contiguous pending batch, starting
/// at `pos` (guaranteed by `next_after` to be `< batch.len()`). Sends
/// without being asked — the cache loop's decision rule treats
/// `memory_next == cursor + 1` as "feeding in progress" and does nothing.
///
/// Paced the same way the DB branch always was: one page, then wait for the
/// cache loop to have consumed it (observed via the cursor watch) before
/// the caller re-derives its next move from the real cursor. A page whose
/// interior holds a hole belonging to a *different*, not-yet-arrived batch
/// simply stops the cursor mid-page; the next iteration re-evaluates
/// `next_after` against the real cursor rather than blindly continuing.
async fn feed_memory_page<P>(
    cache_fill_sender: &broadcast::Sender<PersistentDelivery<P>>,
    cursor_rx: &mut watch::Receiver<u64>,
    pending_batches_at_open: usize,
    pending_batch: &mut PendingBatch<P>,
    pos: usize,
    page_size: usize,
) where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    if pending_batch.span.is_none() {
        pending_batch.span = Some(open_memory_feed_span(
            &pending_batch.batch,
            pending_batches_at_open,
        ));
    }
    let span = pending_batch
        .span
        .clone()
        .expect("set immediately above if absent");

    let end = (pos + page_size).min(pending_batch.batch.len());
    let last = pending_batch.batch[end - 1].sequence();
    {
        let _enter = span.enter();
        for delivery in &pending_batch.batch[pos..end] {
            let _ = cache_fill_sender.send(delivery.clone());
        }
        pending_batch.pages_sent += 1;
        span.record("pages", pending_batch.pages_sent);
    }

    let _ = tokio::time::timeout(
        CATCH_UP_CONSUME_TIMEOUT,
        cursor_rx.wait_for(|c| *c >= u64::from(last)),
    )
    .await;
}

/// Run the feeder for the cache's whole lifetime. Every iteration:
///
/// 1. Drains any batch that arrived on `batches_rx` (never skipped — a
///    request answered by [`db_catch_up`] drains it too, mid-read, so a
///    batch that lowers the bound is seen immediately).
/// 2. Re-derives `memory_next` from the real cursor and publishes it on
///    `front_tx` for the cache loop's decision rule.
/// 3. Feeds one memory page if `memory_next == cursor + 1` — unconditional,
///    nobody has to ask.
/// 4. Otherwise answers at most one pending [`FeedRequest`] with a bounded
///    database read.
/// 5. Otherwise parks until a batch arrives, a request arrives, or the
///    cursor changes.
///
/// `decide_stall_action`'s `catch_up_active` gate ensures at most one
/// request is ever outstanding from the loop's side, so there is never more
/// than one database read in flight even though this task is long-lived.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_feeder<P, Tables>(
    pool: sqlx::PgPool,
    cache_fill_sender: broadcast::Sender<PersistentDelivery<P>>,
    mut cursor_rx: watch::Receiver<u64>,
    highest_known_sequence: Arc<AtomicU64>,
    page_size: usize,
    mut batches_rx: mpsc::UnboundedReceiver<Vec<PersistentDelivery<P>>>,
    mut request_rx: mpsc::UnboundedReceiver<FeedRequest>,
    done_tx: mpsc::UnboundedSender<CatchUpOutcome>,
    front_tx: watch::Sender<Option<u64>>,
) where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
    Tables: MailboxTables,
{
    let mut pending: Vec<PendingBatch<P>> = Vec::new();

    loop {
        while let Ok(batch) = batches_rx.try_recv() {
            pending.push(PendingBatch::new(batch));
        }
        let cursor = *cursor_rx.borrow();
        pending.retain(|b| b.last_sequence() > cursor);
        let front = next_after(&pending, cursor);
        let memory_next = front.map(|(i, p)| u64::from(pending[i].batch[p].sequence()));
        let _ = front_tx.send_replace(memory_next);

        if memory_next == Some(cursor + 1) {
            let (batch_idx, pos) = front.expect("memory_next came from front");
            let pending_len = pending.len();
            feed_memory_page(
                &cache_fill_sender,
                &mut cursor_rx,
                pending_len,
                &mut pending[batch_idx],
                pos,
                page_size,
            )
            .await;
            continue;
        }

        tokio::select! {
            biased;

            batch = batches_rx.recv() => {
                match batch {
                    Some(b) => pending.push(PendingBatch::new(b)),
                    None => return,
                }
            }
            request = request_rx.recv() => {
                match request {
                    Some(FeedRequest::CatchUp) => {
                        let outcome = db_catch_up::<P, Tables>(
                            &pool,
                            &cache_fill_sender,
                            &mut cursor_rx,
                            &highest_known_sequence,
                            &mut batches_rx,
                            &mut pending,
                            page_size,
                        )
                        .await;
                        let _ = done_tx.send(outcome);
                    }
                    None => return,
                }
            }
            _ = cursor_rx.changed() => {}
        }
    }
}

/// Drain the cursor's backlog against *committed* rows at DB speed: page
/// [`MailboxTables::load_next_contiguous_page`] from wherever the cursor
/// actually is, one page in flight, bounded to stop at the first pending
/// in-memory sequence rather than the head — reading past it would re-read
/// from Postgres what this task is about to feed from memory. Ends on a
/// short page (a hole — the cache loop's stall reporting takes over), on
/// reaching the bound with a pending batch ahead (`ReachedMemory`), or on
/// reaching the head with none (`AtHead`). Never writes anything; the
/// GapFiller remains the only writer of placeholder rows.
///
/// The bound and the cursor are both re-derived fresh every iteration —
/// re-derived, not tracked locally — because a batch arriving mid-read (via
/// `batches_rx`) must lower the bound immediately, exactly as the cursor
/// itself is re-read fresh so this discovers its own progress.
///
/// Opens the `obix.persistent_cache.catch_up` span only once a page read is
/// actually attempted: a request answered entirely by the immediate
/// `cursor >= until` check creates no span, which is what makes "zero
/// catch-up spans" a hard assertion for a purely memory-fed backlog rather
/// than a race.
async fn db_catch_up<P, Tables>(
    pool: &sqlx::PgPool,
    cache_fill_sender: &broadcast::Sender<PersistentDelivery<P>>,
    cursor_rx: &mut watch::Receiver<u64>,
    highest_known_sequence: &AtomicU64,
    batches_rx: &mut mpsc::UnboundedReceiver<Vec<PersistentDelivery<P>>>,
    pending: &mut Vec<PendingBatch<P>>,
    page_size: usize,
) -> CatchUpOutcome
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
    Tables: MailboxTables,
{
    fn bound<P>(pending: &[PendingBatch<P>], highest_known: &AtomicU64, cursor: u64) -> (u64, bool)
    where
        P: Serialize + DeserializeOwned + Send,
    {
        match next_after(pending, cursor) {
            Some((i, p)) => (
                u64::from(pending[i].batch[p].sequence()).saturating_sub(1),
                true,
            ),
            None => (highest_known.load(Ordering::Relaxed), false),
        }
    }

    let refresh = |batches_rx: &mut mpsc::UnboundedReceiver<Vec<PersistentDelivery<P>>>,
                   pending: &mut Vec<PendingBatch<P>>| {
        while let Ok(batch) = batches_rx.try_recv() {
            pending.push(PendingBatch::new(batch));
        }
    };

    refresh(batches_rx, pending);
    let cursor0 = *cursor_rx.borrow();
    pending.retain(|b| b.last_sequence() > cursor0);
    let (until0, bounded0) = bound(pending, highest_known_sequence, cursor0);
    if cursor0 >= until0 {
        return if bounded0 {
            CatchUpOutcome::ReachedMemory
        } else {
            CatchUpOutcome::AtHead
        };
    }

    let catch_up_span = tracing::info_span!(
        "obix.persistent_cache.catch_up",
        from = cursor0,
        to = tracing::field::Empty,
        pages = tracing::field::Empty,
        rows = tracing::field::Empty,
        outcome = tracing::field::Empty,
    );
    let mut pages: u64 = 0;
    let mut rows: u64 = 0;
    let outcome = async {
        loop {
            refresh(batches_rx, pending);
            let cursor = *cursor_rx.borrow();
            pending.retain(|b| b.last_sequence() > cursor);
            let (until, bounded) = bound(pending, highest_known_sequence, cursor);
            let from = EventSequence::from(cursor);
            if cursor >= until {
                break if bounded {
                    CatchUpOutcome::ReachedMemory
                } else {
                    CatchUpOutcome::AtHead
                };
            }

            let read_size = page_size.min((until - cursor) as usize).max(1);
            let events = match Tables::load_next_contiguous_page::<P>(pool, from, read_size).await {
                Ok(events) => events,
                Err(e) => {
                    record_catch_up_failed(&e, cursor);
                    tokio::time::sleep(CATCH_UP_RETRY_INTERVAL).await;
                    continue;
                }
            };
            pages += 1;

            let deliveries: Vec<PersistentDelivery<P>> =
                events.into_iter().map(PersistentDelivery::from).collect();
            let Some(first) = deliveries.first() else {
                break CatchUpOutcome::HoleAfter(from);
            };
            if first.sequence() != from.next() {
                break CatchUpOutcome::HoleAfter(from);
            }
            rows += deliveries.len() as u64;
            let last = deliveries
                .last()
                .expect("checked non-empty above")
                .sequence();
            for delivery in &deliveries {
                let _ = cache_fill_sender.send(delivery.clone());
            }

            // One page in flight: wait for the cache loop to have consumed
            // it before reading the next. A timeout just means the loop is
            // behind — re-read from wherever it actually is.
            let _ = tokio::time::timeout(
                CATCH_UP_CONSUME_TIMEOUT,
                cursor_rx.wait_for(|c| *c >= u64::from(last)),
            )
            .await;
        }
    }
    .instrument(catch_up_span.clone())
    .await;

    catch_up_span.record("to", *cursor_rx.borrow());
    catch_up_span.record("pages", pages);
    catch_up_span.record("rows", rows);
    let outcome_label = match &outcome {
        CatchUpOutcome::AtHead => "at_head",
        CatchUpOutcome::HoleAfter(_) => "hole",
        CatchUpOutcome::ReachedMemory => "reached_memory",
    };
    catch_up_span.record("outcome", outcome_label);
    outcome
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

    /// A payload-free delivery at `seq` — `next_after` only ever inspects
    /// sequence numbers, so a unit payload (`()`, which serde handles like
    /// any other type) is all a batch needs here.
    fn delivery(seq: u64) -> PersistentDelivery<()> {
        PersistentDelivery::from(Ok(PersistentOutboxEvent {
            id: OutboxEventId::new(),
            sequence: EventSequence::from(seq),
            payload: None,
            tracing_context: None,
            recorded_at: chrono::Utc::now(),
            commit_group: CommitGroupId::from(0i64),
        }))
    }

    fn batch(seqs: &[u64]) -> PendingBatch<()> {
        PendingBatch::new(seqs.iter().copied().map(delivery).collect())
    }

    /// Picks the smallest pending sequence strictly greater than the
    /// cursor, across three batches whose ranges interleave rather than
    /// sit in arrival order — the point of the scan, not sorted input.
    #[test]
    fn picks_the_smallest_sequence_above_the_cursor_across_batches() {
        let pending = vec![batch(&[10, 11, 12]), batch(&[1, 2, 3]), batch(&[6, 7, 8])];
        // cursor at 2: batch 1's next is 3, batch 2's next is 6, batch 0's
        // next is 10 — the smallest is batch 1 at index 2 (sequence 3).
        assert_eq!(next_after(&pending, 2), Some((1, 2)));
    }

    /// A batch entirely at or below the cursor contributes nothing.
    #[test]
    fn ignores_batches_entirely_at_or_below_the_cursor() {
        let pending = vec![batch(&[1, 2, 3]), batch(&[10, 11, 12])];
        assert_eq!(next_after(&pending, 3), Some((1, 0)));
        assert_eq!(next_after(&pending, 12), None);
    }

    /// Nothing pending at all: `None`.
    #[test]
    fn empty_pending_has_nothing_ahead() {
        let pending: Vec<PendingBatch<()>> = Vec::new();
        assert_eq!(next_after(&pending, 0), None);
    }
}
