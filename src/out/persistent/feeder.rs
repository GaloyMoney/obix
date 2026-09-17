//! The feeder: a single long-lived task per persistent cache that reads
//! committed rows from the database and pages them into the cache-fill
//! broadcast, on request from the cache loop's decision rule
//! (`decide_stall_action` in `cache.rs`). This task used to be
//! `run_catch_up`, spawned fresh per stall episode from
//! `PersistentOutboxEventCache::spawn_cache_loop`; it is now spawned once at
//! `PersistentOutboxEventCache::init` and simply waits for the next request.
//! Behaviour is unchanged — the cache loop still decides *whether* to catch
//! up (see `cache.rs`'s module doc); this task only reads.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{broadcast, mpsc, watch};
use tracing::Instrument;

use crate::{out::event::PersistentDelivery, sequence::EventSequence, tables::MailboxTables};

/// A request from the cache loop for the feeder to read committed rows from
/// the database, starting at the cursor. The feeder decides nothing about
/// *whether* to read — `decide_stall_action` decides that; this is the loop
/// telling the feeder it decided yes.
pub(crate) enum FeedRequest {
    CatchUp,
}

/// Outcome of one DB catch-up read, reported to the cache loop over its done
/// channel.
pub(crate) enum CatchUpOutcome {
    /// The cursor reached the head with no gap — nothing left to catch up.
    AtHead,
    /// The catch-up stopped at `pos` because `pos + 1` is missing from the
    /// table: a hole, not a backlog of committed rows. Handed to the same
    /// grace-gated stall path a stall has always used.
    HoleAfter(EventSequence),
}

/// How long the feeder waits for the cache loop to consume a delivered page
/// (observed via the cursor watch) before re-reading from wherever the real
/// cursor has reached. A hint only — a timeout just means the loop is
/// slower than expected, not that anything is wrong.
const CATCH_UP_CONSUME_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

/// How long a failed page read backs off before retrying. Mirrors
/// `PersistentOutboxEventCache::BACKFILL_RETRY_INTERVAL`.
const CATCH_UP_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

/// Run the feeder for the cache's whole lifetime: wait for a
/// [`FeedRequest`], read from the database until the cursor reaches the head
/// or a hole, report the [`CatchUpOutcome`], wait for the next request.
/// `decide_stall_action`'s `catch_up_active` gate ensures at most one
/// request is ever outstanding, so there is never more than one DB read in
/// flight even though the task itself is long-lived.
pub(crate) async fn run_feeder<P, Tables>(
    pool: sqlx::PgPool,
    cache_fill_sender: broadcast::Sender<PersistentDelivery<P>>,
    mut cursor_rx: watch::Receiver<u64>,
    highest_known_sequence: Arc<AtomicU64>,
    page_size: usize,
    mut request_rx: mpsc::UnboundedReceiver<FeedRequest>,
    done_tx: mpsc::UnboundedSender<CatchUpOutcome>,
) where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
    Tables: MailboxTables,
{
    while let Some(FeedRequest::CatchUp) = request_rx.recv().await {
        let outcome = db_catch_up::<P, Tables>(
            &pool,
            &cache_fill_sender,
            &mut cursor_rx,
            &highest_known_sequence,
            page_size,
        )
        .await;
        let _ = done_tx.send(outcome);
    }
}

/// Drain the cursor's backlog against *committed* rows at DB speed: page
/// [`MailboxTables::load_next_contiguous_page`] from wherever the cursor
/// actually is, one page in flight, until a page is empty (a hole — the
/// cache loop's stall reporting takes over) or the cursor reaches the head
/// (nothing left to catch up on). Never writes anything; the GapFiller
/// remains the only writer of placeholder rows.
///
/// Paced by the cursor itself — read from `cursor_rx.borrow()` fresh on
/// every iteration, not from a locally tracked position — because the
/// contiguous run this reads delivers is exactly what lets the cursor move
/// at all, so re-reading it is how this discovers its own progress.
async fn db_catch_up<P, Tables>(
    pool: &sqlx::PgPool,
    cache_fill_sender: &broadcast::Sender<PersistentDelivery<P>>,
    cursor_rx: &mut watch::Receiver<u64>,
    highest_known_sequence: &AtomicU64,
    page_size: usize,
) -> CatchUpOutcome
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
    Tables: MailboxTables,
{
    let catch_up_span = tracing::info_span!(
        "obix.persistent_cache.catch_up",
        from = *cursor_rx.borrow(),
        to = tracing::field::Empty,
        pages = tracing::field::Empty,
        rows = tracing::field::Empty,
        outcome = tracing::field::Empty,
    );
    let mut pages: u64 = 0;
    let mut rows: u64 = 0;
    let outcome = async {
        loop {
            let from = EventSequence::from(*cursor_rx.borrow());
            let head = EventSequence::from(highest_known_sequence.load(Ordering::Relaxed));
            if from >= head {
                break CatchUpOutcome::AtHead;
            }

            let events = match Tables::load_next_contiguous_page::<P>(pool, from, page_size).await {
                Ok(events) => events,
                Err(e) => {
                    record_catch_up_failed(&e, u64::from(from));
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
