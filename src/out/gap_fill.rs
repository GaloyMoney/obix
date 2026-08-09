use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{broadcast, mpsc};

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
/// - **At most one cluster-lock attempt in flight per process.** The task
///   is sequential, so concurrent requesters can never race each other to
///   the advisory lock from the same process.
/// - **Interim requests merge.** The run loop drains the channel between
///   every step, so requests arriving while a fill pass waits on the
///   abandonment proof, retries a raced lock, or works through batches
///   join the pending set instead of paying their own proof/lock round
///   trips.
///
/// Two write categories: **unlocked** for owner-reported rollback
/// compensation (exact, contention-free by construction), **locked**
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
            page_size: config.event_cache_size,
            stall: None,
            historical: BTreeSet::new(),
            historical_marker: None,
            historical_proven: false,
            historical_due: None,
            abandoned: Vec::new(),
            abandoned_due: None,
            abandoned_attempts: 0,
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
    abandoned: Vec<EventSequence>,
    abandoned_due: Option<tokio::time::Instant>,
    abandoned_attempts: u32,
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
            self.abandoned_due,
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
                self.abandoned.extend(sequences);
                // A fresh report gets a fresh retry budget.
                self.abandoned_attempts = 0;
                self.abandoned_due.get_or_insert(now);
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

    /// One bounded unit of work per due category, then back to the run
    /// loop (which drains the request channel between steps).
    async fn step(&mut self) {
        let now = tokio::time::Instant::now();
        if self.abandoned_due.is_some_and(|due| due <= now) {
            self.compensate_abandoned().await;
        }
        if self.historical_due.is_some_and(|due| due <= now) {
            self.fill_historical_step().await;
        }
        if self
            .stall
            .as_ref()
            .is_some_and(|stall| stall.next_due <= now)
        {
            self.stall_episode_tick().await;
        }
    }

    /// Deliver filled rows into the local cache-fill stream (which also
    /// wakes parked backfills); when `notify`, report the `(min, max)`
    /// range to the debounced notifier so other processes resume
    /// reactively. Historical fills do not notify — their ranges lie far
    /// below other nodes' heads and would only trigger pointless fetches.
    fn deliver(&self, rows: PersistentEventRows<P>, notify: bool) {
        let mut range: Option<(EventSequence, EventSequence)> = None;
        for item in rows {
            let delivery = PersistentDelivery::from(item);
            let sequence = delivery.sequence();
            range = Some(match range {
                Some((lo, hi)) => (lo.min(sequence), hi.max(sequence)),
                None => (sequence, sequence),
            });
            let _ = self.cache_fill_sender.send(delivery);
        }
        if notify && let Some(range) = range {
            let _ = self.notifier_tx.send(range);
        }
    }

    /// Owner-reported sequences: insert immediately, no proof, no lock.
    /// The insert may briefly park on the aborting transaction's
    /// speculative-insertion lock (the own-failure path reports before
    /// the rollback lands) — bounded by the rollback itself. `DO NOTHING`
    /// makes it idempotent against a commit that actually landed.
    async fn compensate_abandoned(&mut self) {
        match Tables::fill_gaps::<P>(&self.pool, self.abandoned.clone()).await {
            Ok(placeholders) => {
                self.deliver(placeholders, true);
                self.abandoned.clear();
                self.abandoned_due = None;
                self.abandoned_attempts = 0;
            }
            Err(error) => {
                self.abandoned_attempts += 1;
                record_compensation_failed(&error, self.abandoned_attempts);
                if self.abandoned_attempts >= Self::COMPENSATION_MAX_ATTEMPTS {
                    // The stall episode proves and fills them later.
                    self.abandoned.clear();
                    self.abandoned_due = None;
                    self.abandoned_attempts = 0;
                } else {
                    self.abandoned_due =
                        Some(tokio::time::Instant::now() + Self::COMPENSATION_RETRY_INTERVAL);
                }
            }
        }
    }

    /// One historical step: establish the (process-lifetime) abandonment
    /// proof if not yet proven, else fill one batch. Remaining work
    /// reschedules immediately — the run loop drains the request channel
    /// in between, merging any newly arrived sequences into the next
    /// batch.
    async fn fill_historical_step(&mut self) {
        let now = tokio::time::Instant::now();
        if self.historical.is_empty() {
            self.historical_due = None;
            return;
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
                        return;
                    }
                },
            };
            match Tables::abandonment_proof_passed(&self.pool, &marker).await {
                Ok(true) => self.historical_proven = true,
                Ok(false) => {
                    self.historical_due = Some(now + Self::PROOF_POLL_INTERVAL);
                    return;
                }
                Err(error) => {
                    record_gap_fill_failed(&error);
                    self.historical_due = Some(now + Self::REFILL_INTERVAL);
                    return;
                }
            }
        }

        let batch: Vec<EventSequence> = self
            .historical
            .iter()
            .take(self.batch_limit)
            .copied()
            .map(EventSequence::from)
            .collect();
        match Tables::fill_gaps_deduped::<P>(&self.pool, batch.clone()).await {
            Ok(Some(placeholders)) => {
                // Everything attempted is settled: inserted by us
                // (delivered below, waking any parked backfill) or already
                // committed (the backfill's own re-read picks it up).
                for sequence in &batch {
                    self.historical.remove(&u64::from(*sequence));
                }
                self.deliver(placeholders, false);
                self.historical_due = if self.historical.is_empty() {
                    None
                } else {
                    Some(now)
                };
            }
            Ok(None) => {
                // Another node holds the fill lock — its rows are
                // committed by the time it releases; retry shortly rather
                // than spinning.
                self.historical_due = Some(now + Self::PROOF_POLL_INTERVAL);
            }
            Err(error) => {
                record_gap_fill_failed(&error);
                self.historical_due = Some(now + Self::REFILL_INTERVAL);
            }
        }
    }

    /// One tick of the stall episode: re-read the window (delivering
    /// whatever committed since — this alone resolves stalls whose commit
    /// signal was lost), then, once the abandonment proof passes, fill
    /// one batch of the still-missing sequences under the cluster lock.
    ///
    /// The episode terminates only when a re-read finds the window
    /// complete, or the cursor reports the stall cleared / moved beyond
    /// the window. Every other outcome — proof pending, raced lock,
    /// transient error, and *successful fills included* — reschedules
    /// another tick, so a sequence that committed between re-read and
    /// insert (its conflict is silent, its signal possibly lost) is
    /// re-delivered by the next re-read rather than dropped.
    async fn stall_episode_tick(&mut self) {
        let now = tokio::time::Instant::now();
        let pool = self.pool.clone();
        let Some(stall) = self.stall.as_mut() else {
            return;
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
                    return;
                }
            },
        };
        let from = stall.from;
        let fill_up_to = u64::from(head).min(from + self.page_size as u64);

        let events =
            match Tables::load_next_page::<P>(&pool, EventSequence::from(from), self.page_size)
                .await
            {
                Ok(events) => events,
                Err(error) => {
                    record_gap_fill_failed(&error);
                    return;
                }
            };
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
        if missing.is_empty() {
            // The window is complete: the re-read just delivered whatever
            // the cursor was missing, so resolution is in hand. A stall
            // beyond this window reports as a new episode.
            self.stall = None;
            return;
        }

        match Tables::abandonment_proof_passed(&pool, &marker).await {
            Ok(true) => match Tables::fill_gaps_deduped::<P>(&pool, missing).await {
                Ok(Some(placeholders)) => self.deliver(placeholders, true),
                // Another node holds the fill lock: its rows surface in
                // the next tick's re-read.
                Ok(None) => {}
                Err(error) => record_gap_fill_failed(&error),
            },
            // A transaction from before the marker still runs and may own
            // a missing sequence: abandonment is unprovable — the next
            // tick re-checks, its re-read delivering late commits
            // meanwhile.
            Ok(false) => {}
            Err(error) => record_gap_fill_failed(&error),
        }
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
