use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{broadcast, mpsc};
use tracing::Instrument;

use std::collections::BTreeSet;
use std::sync::Arc;

use crate::{
    config::MailboxConfig,
    handle::{OwnedTaskHandle, spawn_supervised},
    out::event::PersistentDelivery,
    sequence::EventSequence,
    tables::{MailboxTables, PersistentEventRows},
};

/// Requests handled by the [`GapFiller`] — the only component in the
/// process that writes placeholder rows.
pub(crate) enum GapFillRequest {
    /// This process's own allocated-but-uncommitted sequences, reported by
    /// the publish hook (es-entity `on_rollback`, or `pre_commit`'s own
    /// error branches). Exact by construction and owner-only — no other
    /// node can be compensating the same sequences — so they are filled
    /// immediately and *without* the cluster fill lock.
    Abandoned(Vec<EventSequence>),
    /// The broadcast cursor is stalled: its next needed sequence is
    /// allocated but missing. Starts the grace-gated fill episode —
    /// read-only page re-reads until the missing sequences are provably
    /// abandoned, then batch-capped fills under the cluster lock.
    Stalled(EventSequence),
    /// The broadcast cursor advanced past the previously reported stall;
    /// the episode (and any pending grace deadline) is dropped. A stall
    /// that resolves within its grace period costs zero DB work.
    StallCleared,
    /// Sequences a backfill found missing below the cache loop's init
    /// head. Provably old by construction — any fresh marker post-dates
    /// their allocation — so no grace: proof-gated, batch-capped fills
    /// under the cluster lock. Overlapping requests from concurrent
    /// backfills merge into one fill pass.
    Historical(Vec<EventSequence>),
}

/// Per-process gap filler: the single component that writes placeholder
/// rows, mirroring the debounced-notifier shape (resident task, unbounded
/// request channel, fire-and-forget senders).
///
/// Centralizing the writes buys three properties:
/// - **One fill policy.** Grace periods, abandonment proofs, batch caps
///   and retry cadences live here; requesters (the cache loop's stall
///   observation, backfill's historical gaps, the publish hook's rollback
///   reports) only describe *what* they saw, never decide *when or how*
///   to fill.
/// - **At most one cluster-lock attempt in flight per process** — and per
///   pass, exactly one locked statement: the ready batches of both locked
///   categories are combined into a single insert (one transaction, one
///   advisory-lock acquisition) rather than locking once per category.
/// - **Interim requests merge.** The run loop drains the channel between
///   every pass, so requests arriving while a pass waits on the
///   abandonment proof, retries a raced lock, or works through batches
///   join the pending set instead of paying their own proof/lock round
///   trips.
///
/// Two write categories: **unlocked** for owner-reported rollback
/// compensation (exact, contention-free by construction — spawned onto
/// its own task, since it may lawfully park on the aborting transaction's
/// locks and must not block the pipeline), **locked**
/// (`pg_try_advisory_xact_lock` inside [`MailboxTables::fill_gaps_deduped`])
/// for everything inferred — stall episodes and historical fills — where
/// other nodes may infer the same gap.
pub(crate) struct GapFiller {
    tx: mpsc::UnboundedSender<GapFillRequest>,
    _handle: Arc<OwnedTaskHandle>,
}

impl Clone for GapFiller {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            _handle: self._handle.clone(),
        }
    }
}

impl GapFiller {
    /// The request channel is created by the caller (`Outbox::init`)
    /// because the cache loop needs the sender before this task can be
    /// spawned (the task in turn needs the cache's fill sender).
    pub fn spawn<P, Tables>(
        pool: &sqlx::PgPool,
        requests: mpsc::UnboundedReceiver<GapFillRequest>,
        tx: mpsc::UnboundedSender<GapFillRequest>,
        cache_fill_sender: broadcast::Sender<PersistentDelivery<P>>,
        notifier_tx: mpsc::UnboundedSender<(EventSequence, EventSequence)>,
        config: &MailboxConfig,
    ) -> Self
    where
        P: Serialize + DeserializeOwned + Send + Sync + 'static,
        Tables: MailboxTables,
    {
        let task = GapFillerTask::<P, Tables> {
            pool: pool.clone(),
            cache_fill_sender,
            notifier_tx,
            grace: config.gap_fill_grace,
            batch_limit: config.gap_fill_batch_limit,
            // The episode window is the backfill page size: the episode's
            // re-read is a page read, so one episode covers exactly what
            // one page read can see. Deliberately NOT the batch limit —
            // the window bounds an episode's *scope* (one grace period,
            // one marker), the batch limit bounds one *insert*: a window
            // wider than the cap is what lets a multi-batch gap fill at
            // the loop cadence instead of paying grace per batch.
            page_size: config.backfill_page_size.max(1),
            stall: None,
            historical: BTreeSet::new(),
            historical_marker: None,
            historical_proven: false,
            historical_due: None,
            _tables: std::marker::PhantomData,
        };
        let handle = spawn_supervised("obix::gap_filler", task.run(requests));
        Self {
            tx,
            _handle: Arc::new(OwnedTaskHandle::new(handle)),
        }
    }

    /// Sender for gap-fill requests; unbounded so synchronous callers
    /// (`on_rollback`) never block.
    pub fn report_sender(&self) -> mpsc::UnboundedSender<GapFillRequest> {
        self.tx.clone()
    }
}

/// The grace-gated fill episode for the current broadcast-cursor stall —
/// at most one exists, structurally: it is a single `Option` slot in a
/// sequential task.
struct StallEpisode {
    /// The stalled cursor position; the episode's window is
    /// `(from, min(marker head, from + page_size)]`.
    from: u64,
    next_due: tokio::time::Instant,
    /// Abandonment marker + observed allocation head, assigned at the
    /// episode's first tick and kept for its lifetime — one burned xid
    /// per episode, not per retry.
    marker: Option<(String, EventSequence)>,
}

struct GapFillerTask<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pool: sqlx::PgPool,
    cache_fill_sender: broadcast::Sender<PersistentDelivery<P>>,
    notifier_tx: mpsc::UnboundedSender<(EventSequence, EventSequence)>,
    grace: std::time::Duration,
    batch_limit: usize,
    page_size: usize,
    stall: Option<StallEpisode>,
    /// Pending historical sequences, merged across requests. `u64` keys so
    /// overlapping ranges dedupe structurally.
    historical: BTreeSet<u64>,
    /// One marker proves every historical fill this process ever does:
    /// historical sequences were allocated before the cache loop started,
    /// so any marker assigned afterwards post-dates them — and the xmin
    /// horizon passing it once stays passed (the horizon is monotone).
    historical_marker: Option<String>,
    historical_proven: bool,
    historical_due: Option<tokio::time::Instant>,
    _tables: std::marker::PhantomData<Tables>,
}

impl<P, Tables> GapFillerTask<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
    Tables: MailboxTables,
{
    /// Stall-episode tick cadence, and the backoff after transient DB
    /// errors in any category.
    const REFILL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
    /// Cadence for abandonment-proof polls and raced-fill-lock retries —
    /// both resolve quickly (a transaction ending, a lock releasing), so
    /// they poll faster than the error backoff.
    const PROOF_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(250);
    /// How long a failed compensation insert waits before retrying, and
    /// how many attempts are made before the sequences are left to the
    /// stall episode.
    const COMPENSATION_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);
    const COMPENSATION_MAX_ATTEMPTS: u32 = 5;

    async fn run(mut self, mut requests: mpsc::UnboundedReceiver<GapFillRequest>) {
        loop {
            let deadline = self.next_deadline();
            tokio::select! {
                request = requests.recv() => match request {
                    Some(request) => self.accept(request),
                    None => return,
                },
                _ = async {
                    match deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending::<()>().await,
                    }
                } => {}
            }
            // Merge everything queued before doing any work: requests that
            // arrived while a previous step waited on the proof, retried a
            // raced lock, or held the lock for a batch join this pass
            // instead of paying their own round trips.
            while let Ok(request) = requests.try_recv() {
                self.accept(request);
            }
            self.step().await;
        }
    }

    fn next_deadline(&self) -> Option<tokio::time::Instant> {
        [
            self.historical_due,
            self.stall.as_ref().map(|stall| stall.next_due),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    fn accept(&mut self, request: GapFillRequest) {
        let now = tokio::time::Instant::now();
        match request {
            GapFillRequest::Abandoned(sequences) => {
                // Owner-only and unlocked — and possibly parked on the
                // aborting transaction's speculative-insertion locks (the
                // own-failure path reports before the rollback lands) —
                // so compensation runs on its own task instead of
                // head-of-line blocking the locked fill pipeline behind
                // an in-flight rollback.
                self.spawn_compensation(sequences);
            }
            GapFillRequest::Stalled(stalled_on) => {
                let from = u64::from(stalled_on);
                // The active episode can resolve a stall at `from` only if
                // the stall's needed sequence (`from + 1`) lies inside the
                // episode's fill window `(from, min(marker head,
                // from + page_size)]` — i.e. `from` < window end. Such
                // stalls are the episode's own progress (the cursor
                // advancing over just-filled batches); the episode keeps
                // its window, marker and original grace (no
                // grace-per-batch). Anything at or beyond the window end —
                // in particular a gap allocated *after* the episode's
                // marker was taken, which its stale head can never cover —
                // must start a new episode with a fresh marker: swallowing
                // it here would strand the cursor, because the cache loop
                // dedups stall reports by position and would not re-send
                // until its idle-resync re-arm.
                let covered = self.stall.as_ref().is_some_and(|stall| {
                    let window_end = match &stall.marker {
                        Some((_, head)) => u64::from(*head).min(stall.from + self.page_size as u64),
                        // Still in grace, no marker yet: the marker, once
                        // assigned, will post-date this stall's allocation,
                        // so the page bound alone is sound here.
                        None => stall.from + self.page_size as u64,
                    };
                    from < window_end
                });
                if !covered {
                    self.stall = Some(StallEpisode {
                        from,
                        next_due: now + self.grace,
                        marker: None,
                    });
                }
            }
            GapFillRequest::StallCleared => {
                self.stall = None;
            }
            GapFillRequest::Historical(sequences) => {
                self.historical.extend(sequences.into_iter().map(u64::from));
                self.historical_due.get_or_insert(now);
            }
        }
    }

    /// One bounded pass: gather the *ready* (proof-passed) batches from
    /// both locked categories, then insert them in a **single** locked
    /// statement — one transaction, one advisory-lock acquisition —
    /// before returning to the run loop (which drains the request channel
    /// between passes).
    async fn step(&mut self) {
        let now = tokio::time::Instant::now();
        let stall_batch = if self
            .stall
            .as_ref()
            .is_some_and(|stall| stall.next_due <= now)
        {
            self.prepare_stall_batch().await
        } else {
            Vec::new()
        };
        let historical_batch = if self.historical_due.is_some_and(|due| due <= now) {
            self.prepare_historical_batch(self.batch_limit.saturating_sub(stall_batch.len()))
                .await
        } else {
            Vec::new()
        };
        self.locked_fill(stall_batch, historical_batch).await;
    }

    /// Owner-reported sequences: insert immediately, no proof, no lock —
    /// on a task of their own. The insert may park on the aborting
    /// transaction's speculative-insertion locks (the own-failure path
    /// reports before the rollback lands), and that wait must not
    /// head-of-line block the locked fill pipeline. Concurrent
    /// compensations are safe: each failed operation reports its own
    /// disjoint sequences, and `DO NOTHING` makes the insert idempotent
    /// against a commit that actually landed.
    fn spawn_compensation(&self, sequences: Vec<EventSequence>) {
        let pool = self.pool.clone();
        let cache_fill_sender = self.cache_fill_sender.clone();
        let notifier_tx = self.notifier_tx.clone();
        tokio::spawn(async move {
            let mut attempts = 0;
            loop {
                match Tables::fill_gaps::<P>(&pool, sequences.clone()).await {
                    Ok(placeholders) => {
                        deliver_and_notify(&cache_fill_sender, &notifier_tx, placeholders);
                        return;
                    }
                    Err(error) => {
                        attempts += 1;
                        record_compensation_failed(&error, attempts);
                        if attempts >= Self::COMPENSATION_MAX_ATTEMPTS {
                            // The stall episode proves and fills them later.
                            return;
                        }
                        tokio::time::sleep(Self::COMPENSATION_RETRY_INTERVAL).await;
                    }
                }
            }
        });
    }

    /// One tick of the stall episode, up to (not including) the insert:
    /// re-read the window and deliver whatever committed since — this
    /// alone resolves stalls whose commit signal was lost — then return
    /// the still-missing batch if the episode's abandonment proof has
    /// passed, or an empty batch while it hasn't (read-only tick).
    ///
    /// The episode terminates only when a re-read finds the window
    /// complete, or the cursor reports the stall cleared / moved beyond
    /// the window. Every other outcome — proof pending, raced lock,
    /// transient error, and *successful fills included* — reschedules
    /// another tick, so a sequence that committed between re-read and
    /// insert (its conflict is silent, its signal possibly lost) is
    /// re-delivered by the next re-read rather than dropped.
    async fn prepare_stall_batch(&mut self) -> Vec<EventSequence> {
        let now = tokio::time::Instant::now();
        let pool = self.pool.clone();
        let Some(stall) = self.stall.as_mut() else {
            return Vec::new();
        };
        stall.next_due = now + Self::REFILL_INTERVAL;

        let (marker, head) = match &stall.marker {
            Some((marker, head)) => (marker.clone(), *head),
            None => match Tables::abandonment_marker(&pool).await {
                Ok((marker, head)) => {
                    stall.marker = Some((marker.clone(), head));
                    (marker, head)
                }
                Err(error) => {
                    record_gap_fill_failed(&error);
                    return Vec::new();
                }
            },
        };
        let from = stall.from;
        let fill_up_to = u64::from(head).min(from + self.page_size as u64);

        // One span per episode tick: the re-read is the episode's whole
        // recurring cost, and `rows` against `missing` says whether the
        // tick was productive (the window closed) or another read of a
        // window still waiting on an unresolved sequence.
        let page_span = tracing::info_span!(
            "obix.gap_filler.stall_page",
            from = from,
            limit = self.page_size,
            fill_up_to = fill_up_to,
            rows = tracing::field::Empty,
            missing = tracing::field::Empty,
        );
        let events =
            match Tables::load_next_page::<P>(&pool, EventSequence::from(from), self.page_size)
                .instrument(page_span.clone())
                .await
            {
                Ok(events) => events,
                Err(error) => {
                    record_gap_fill_failed(&error);
                    return Vec::new();
                }
            };
        page_span.record("rows", events.len());
        let mut present = std::collections::HashSet::new();
        for item in events {
            let delivery = PersistentDelivery::from(item);
            present.insert(u64::from(delivery.sequence()));
            let _ = self.cache_fill_sender.send(delivery);
        }

        let missing: Vec<EventSequence> = ((from + 1)..=fill_up_to)
            .filter(|sequence| !present.contains(sequence))
            .take(self.batch_limit)
            .map(EventSequence::from)
            .collect();
        page_span.record("missing", missing.len());
        if missing.is_empty() {
            // The window is complete: the re-read just delivered whatever
            // the cursor was missing, so resolution is in hand. A stall
            // beyond this window reports as a new episode.
            self.stall = None;
            return Vec::new();
        }

        match Tables::abandonment_proof_passed(&pool, &marker).await {
            Ok(true) => missing,
            // A transaction from before the marker still runs and may own
            // a missing sequence: abandonment is unprovable — the next
            // tick re-checks, its re-read delivering late commits
            // meanwhile.
            Ok(false) => Vec::new(),
            Err(error) => {
                record_gap_fill_failed(&error);
                Vec::new()
            }
        }
    }

    /// The ready historical batch, up to `capacity` sequences: establish
    /// the (process-lifetime) abandonment proof if not yet proven, else
    /// hand out the next slice of the pending set. With `capacity` 0 (the
    /// stall batch filled the statement), the due time is left as-is so
    /// the immediately following pass — after another channel drain —
    /// picks the work up with full capacity.
    async fn prepare_historical_batch(&mut self, capacity: usize) -> Vec<EventSequence> {
        let now = tokio::time::Instant::now();
        if self.historical.is_empty() {
            self.historical_due = None;
            return Vec::new();
        }
        if capacity == 0 {
            return Vec::new();
        }

        if !self.historical_proven {
            let marker = match self.historical_marker.clone() {
                Some(marker) => marker,
                None => match Tables::abandonment_marker(&self.pool).await {
                    Ok((marker, _)) => {
                        self.historical_marker = Some(marker.clone());
                        marker
                    }
                    Err(error) => {
                        record_gap_fill_failed(&error);
                        self.historical_due = Some(now + Self::REFILL_INTERVAL);
                        return Vec::new();
                    }
                },
            };
            match Tables::abandonment_proof_passed(&self.pool, &marker).await {
                Ok(true) => self.historical_proven = true,
                Ok(false) => {
                    self.historical_due = Some(now + Self::PROOF_POLL_INTERVAL);
                    return Vec::new();
                }
                Err(error) => {
                    record_gap_fill_failed(&error);
                    self.historical_due = Some(now + Self::REFILL_INTERVAL);
                    return Vec::new();
                }
            }
        }

        self.historical
            .iter()
            .take(capacity)
            .copied()
            .map(EventSequence::from)
            .collect()
    }

    /// Insert both ready batches in one locked statement. The batches are
    /// disjoint by construction (historical sequences lie at or below the
    /// init head, the stall window strictly above it) and together bounded
    /// by `batch_limit`. Only the stall portion is notified cross-process:
    /// historical ranges lie far below other nodes' heads and would only
    /// trigger pointless fetches — their consumers (parked backfills) wake
    /// via the cache-fill stream.
    async fn locked_fill(
        &mut self,
        stall_batch: Vec<EventSequence>,
        historical_batch: Vec<EventSequence>,
    ) {
        if stall_batch.is_empty() && historical_batch.is_empty() {
            return;
        }
        let now = tokio::time::Instant::now();

        let mut combined = stall_batch.clone();
        combined.extend(historical_batch.iter().copied());
        match Tables::fill_gaps_deduped::<P>(&self.pool, combined).await {
            Ok(Some(placeholders)) => {
                // Every attempted historical sequence is settled: inserted
                // by us (delivered below, waking any parked backfill) or
                // already committed (the backfill's own re-read picks it
                // up). Stall-batch sequences settle via the episode's next
                // re-read.
                for sequence in &historical_batch {
                    self.historical.remove(&u64::from(*sequence));
                }
                if !historical_batch.is_empty() {
                    self.historical_due = if self.historical.is_empty() {
                        None
                    } else {
                        Some(now)
                    };
                }

                let stall_set: std::collections::HashSet<u64> =
                    stall_batch.iter().map(|s| u64::from(*s)).collect();
                let mut stall_range: Option<(EventSequence, EventSequence)> = None;
                for item in placeholders {
                    let delivery = PersistentDelivery::from(item);
                    let sequence = delivery.sequence();
                    if stall_set.contains(&u64::from(sequence)) {
                        stall_range = Some(match stall_range {
                            Some((lo, hi)) => (lo.min(sequence), hi.max(sequence)),
                            None => (sequence, sequence),
                        });
                    }
                    let _ = self.cache_fill_sender.send(delivery);
                }
                if let Some(range) = stall_range {
                    let _ = self.notifier_tx.send(range);
                }
            }
            Ok(None) => {
                // Another node holds the fill lock — its rows are
                // committed by the time it releases; retry shortly rather
                // than spinning. The stall episode is already rescheduled.
                if !historical_batch.is_empty() {
                    self.historical_due = Some(now + Self::PROOF_POLL_INTERVAL);
                }
            }
            Err(error) => {
                record_gap_fill_failed(&error);
                if !historical_batch.is_empty() {
                    self.historical_due = Some(now + Self::REFILL_INTERVAL);
                }
            }
        }
    }
}

/// Deliver filled rows into the local cache-fill stream (which also wakes
/// parked backfills) and report their `(min, max)` range to the debounced
/// notifier so other processes resume reactively.
fn deliver_and_notify<P>(
    cache_fill_sender: &broadcast::Sender<PersistentDelivery<P>>,
    notifier_tx: &mpsc::UnboundedSender<(EventSequence, EventSequence)>,
    rows: PersistentEventRows<P>,
) where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    let mut range: Option<(EventSequence, EventSequence)> = None;
    for item in rows {
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
}

#[tracing::instrument(
    name = "obix.gap_filler.compensation_failed",
    level = "warn",
    skip_all,
    fields(error = %error, attempts = attempts),
)]
fn record_compensation_failed(error: &sqlx::Error, attempts: u32) {}

#[tracing::instrument(
    name = "obix.gap_filler.fill_failed",
    level = "warn",
    skip_all,
    fields(error = %error),
)]
fn record_gap_fill_failed(error: &sqlx::Error) {}
