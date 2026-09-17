use futures::{FutureExt, StreamExt};
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{broadcast, mpsc};

use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use super::cache::PersistentOutboxEventCache;
use super::listener::PersistentOutboxListener;
use crate::{
    handle::{OwnedTaskHandle, spawn_supervised},
    out::event::*,
    out::lane::{CommitOrder, InsertOrder, LaneHandle},
    sequence::{CommitGroupId, CommitSequence, EventSequence},
    tables::{CommitCheckpoint, MailboxTables, PersistentEventRows},
};

/// How long a failed group fetch waits before retrying the same group.
const FETCH_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(250);

/// How far this process's sequencer has folded the insert-ordered stream, and
/// how many rows it has emitted on the commit lane.
///
/// Both are in-process values. The commit lane is not materialised — every
/// `Enabled` process computes the same numbering from the same table — so
/// "the head" is a per-process fact about how far *this* fold has got, not a
/// cluster-wide one.
#[derive(Clone)]
pub(crate) struct SequencerPositions {
    fold_position: Arc<AtomicU64>,
    commit_head: Arc<AtomicU64>,
}

impl SequencerPositions {
    /// The highest insert sequence this process's fold has passed. Every
    /// sequence at or below it has been emitted on the commit lane (with its
    /// whole group) or skipped.
    pub(crate) fn fold_position(&self) -> EventSequence {
        EventSequence::from(self.fold_position.load(Ordering::Acquire))
    }

    /// The highest position this process's fold has emitted.
    pub(crate) fn commit_head(&self) -> CommitSequence {
        CommitSequence::from(self.commit_head.load(Ordering::Acquire))
    }
}

/// What a commit-ordered listener needs from the sequencer: the lane's
/// fan-out and its head.
pub(crate) struct SequencerHandle<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    commit_head: Arc<AtomicU64>,
    commit_sender: broadcast::Sender<CommitTransport<P>>,
    backfill_request: mpsc::UnboundedSender<(CommitSequence, mpsc::Sender<CommitTransport<P>>)>,
    backfill_buffer_size: usize,
    positions: SequencerPositions,
    _task: OwnedTaskHandle,
}

/// The commit lane's internal transport: a [`PersistentDelivery`] positioned
/// at the slot the fold emitted it in, and flagged when it closes its source
/// transaction.
type CommitTransport<P> = Transport<CommitOrder, P>;

impl<P> std::fmt::Debug for SequencerHandle<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SequencerHandle")
            .field("commit_head", &self.commit_head.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl<P> SequencerHandle<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub(crate) fn positions(&self) -> SequencerPositions {
        self.positions.clone()
    }

    /// What a commit-ordered listener consumes: the same
    /// [`LaneHandle`](crate::out::lane::LaneHandle) shape the insert cache
    /// hands out, over this lane's positions.
    pub(crate) fn handle(&self) -> LaneHandle<CommitOrder, P> {
        LaneHandle::new(
            self.commit_head.clone(),
            self.commit_sender.subscribe(),
            self.backfill_request.clone(),
            self.backfill_buffer_size,
        )
    }
}

/// Where a fold's output goes.
enum Sink<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// The live lane: broadcast to every listener.
    Live(broadcast::Sender<CommitTransport<P>>),
    /// One backfill request: only positions above what the consumer already
    /// has, and only until the live fold's head is reached — from there the
    /// consumer's listener takes over from the broadcast, the same hand-off
    /// the insert lane's backfill makes.
    Backfill {
        sender: mpsc::Sender<CommitTransport<P>>,
        after: CommitSequence,
        until: CommitSequence,
    },
}

impl<P> Sink<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Emit one positioned delivery. `false` means the fold should stop.
    async fn emit(&mut self, delivery: CommitTransport<P>) -> bool {
        match self {
            Self::Live(sender) => {
                let _ = sender.send(delivery);
                true
            }
            Self::Backfill { sender, after, .. } => {
                if delivery.position() <= *after {
                    return true;
                }
                sender.send(delivery).await.is_ok()
            }
        }
    }

    /// Whether this fold has produced everything it was asked for.
    fn finished(&self, head: CommitSequence) -> bool {
        match self {
            Self::Live(_) => false,
            Self::Backfill { until, .. } => head >= *until,
        }
    }
}

/// Folds the insert-ordered stream into commit order.
///
/// The order is **computed, never materialised**: a group is emitted whole at
/// first sight of its lowest member over the contiguous, gap-filled insert
/// stream, its members in `sequence` order, and a position is a running count
/// of rows emitted. That makes the numbering a pure function of the events
/// table — every process derives the same `CommitSequence`s independently,
/// with no lock, no log and no coordination.
///
/// What is persisted is a *sparse checkpoint* of the fold
/// ([`CommitCheckpoint`]): enough to resume mid-stream without replaying
/// history, written every `checkpoint_every` groups or `checkpoint_interval`,
/// never per group.
struct Sequencer<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pool: sqlx::PgPool,
    /// Rows emitted so far — the next group's members are numbered from
    /// `head + 1`.
    head: CommitSequence,
    /// Published copy of `head`, for the fence and for listeners.
    commit_head: Option<Arc<AtomicU64>>,
    fold_position: Option<Arc<AtomicU64>>,
    sink: Sink<P>,
    /// Groups already emitted whose later members the fold has not reached
    /// yet, mapped to their highest member. Dropped as the stream passes
    /// them. Seeded from a checkpoint's `open_groups`, which is what stops a
    /// resumed fold re-emitting a group that straddles its resume point.
    seen: HashMap<CommitGroupId, EventSequence>,
    checkpoint_every: usize,
    checkpoint_interval: std::time::Duration,
    groups_since_checkpoint: usize,
    last_checkpoint: tokio::time::Instant,
    _phantom: std::marker::PhantomData<Tables>,
}

impl<P, Tables> Sequencer<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
    Tables: MailboxTables,
{
    /// Fold a drained batch of insert-lane deliveries.
    ///
    /// The members of every group first sighted in the batch are fetched in
    /// ONE statement, then the batch is placed in order from that map. The
    /// fetch is per page; emission, the head advance and the published fold
    /// position all stay per delivery and in order, so the fence invariant
    /// below is unaffected by the batching.
    ///
    /// `false` means the fold should stop (the sink is done or gone).
    async fn fold_batch(&mut self, batch: Vec<Transport<InsertOrder, P>>) -> bool {
        let mut members = self.fetch_members(&batch).await;
        for delivery in batch {
            let sequence = delivery.sequence();
            if !self.place(delivery, &mut members).await {
                return false;
            }
            // INVARIANT: published only once this delivery is fully ACCOUNTED
            // FOR — its group emitted and the head advanced, or provably
            // never going to be. The commit-lane fence reads this and then
            // reads the head, so publishing before the group is emitted
            // leaves a window in which the fence sees the fold past the
            // insert frontier while the head still excludes that group, and
            // returns with the frontier event undelivered.
            //
            // It must still advance on every early return in `place`, or the
            // fence stalls on an aborted tail instead.
            if let Some(position) = &self.fold_position {
                position.store(u64::from(sequence), Ordering::Release);
            }
            if self.sink.finished(self.head) {
                return false;
            }
        }
        true
    }

    /// One statement for every group first sighted in `batch`.
    ///
    /// The floor is the lowest first-sight sequence in the batch: a group's
    /// members never sit below its own MIN, and every MIN here is in the
    /// batch, so the bound prunes partitions without losing a row.
    async fn fetch_members(
        &self,
        batch: &[Transport<InsertOrder, P>],
    ) -> HashMap<CommitGroupId, PersistentEventRows<P>> {
        let mut wanted: Vec<CommitGroupId> = Vec::new();
        let mut floor: Option<EventSequence> = None;
        for delivery in batch {
            if !delivery.has_payload() {
                continue;
            }
            let group = delivery.commit_group();
            if self.seen.contains_key(&group) || wanted.contains(&group) {
                continue;
            }
            wanted.push(group);
            floor.get_or_insert(delivery.sequence());
        }
        let Some(floor) = floor else {
            return HashMap::new();
        };

        // Retried rather than skipped: nothing has been emitted for these
        // groups, so a retry re-reads the same rows and produces the same
        // numbering. Giving up would silently drop them from the lane.
        loop {
            match Tables::load_group_members::<P>(&self.pool, &wanted, floor).await {
                Ok(groups) => return groups.into_iter().collect(),
                Err(error) => {
                    record_fetch_failed(&error, u64::from(floor));
                    tokio::time::sleep(FETCH_RETRY_INTERVAL).await;
                }
            }
        }
    }

    /// Emit one delivery's group if this is its first sight, or establish
    /// that it never needs emitting.
    async fn place(
        &mut self,
        delivery: Transport<InsertOrder, P>,
        members: &mut HashMap<CommitGroupId, PersistentEventRows<P>>,
    ) -> bool {
        if !delivery.has_payload() {
            return true;
        }
        let sequence = delivery.sequence();
        let group = delivery.commit_group();
        if let Some(group_max) = self.seen.get(&group).copied() {
            if sequence >= group_max {
                self.seen.remove(&group);
            }
            return true;
        }

        // INVARIANT: `place` only reaches here on `group`'s first sight, and
        // every member of a commit group is inserted in the same transaction
        // as the one that produced `delivery` — so by the time `delivery`
        // itself is visible (committed, delivered on the insert lane), every
        // other member is already committed too. An empty result — from the
        // batch prefetch or the single-group fallback — can therefore never
        // be `group`'s true membership; it is a transient visibility miss,
        // and is retried exactly like a fetch error rather than accepted,
        // which would silently and permanently drop the group from this
        // process's commit lane.
        let rows = match members.remove(&group).filter(|rows| !rows.is_empty()) {
            Some(rows) => rows,
            None => self.fetch_group(group, sequence).await,
        };

        let group_max = rows
            .iter()
            .map(|row| match row {
                Ok(event) => event.sequence,
                Err(error) => error.sequence,
            })
            .max()
            .unwrap_or(sequence);
        if group_max > sequence {
            self.seen.insert(group, group_max);
        }

        let last = rows.len() - 1;
        for (index, row) in rows.into_iter().enumerate() {
            self.head = self.head.next();
            let delivery = Delivery::new(self.head, index == last, PersistentDelivery::from(row));
            if !self.sink.emit(delivery).await {
                return false;
            }
        }
        if let Some(head) = &self.commit_head {
            head.store(u64::from(self.head), Ordering::Release);
        }

        self.groups_since_checkpoint += 1;
        if self.groups_since_checkpoint >= self.checkpoint_every
            || self.last_checkpoint.elapsed() >= self.checkpoint_interval
        {
            self.checkpoint(sequence);
        }
        true
    }

    /// Fetch one group's membership, retrying until it is non-empty: an
    /// `Ok` result missing `group` entirely is treated the same as a fetch
    /// error (see the INVARIANT at the call site), not returned as "no
    /// members".
    async fn fetch_group(
        &self,
        group: CommitGroupId,
        floor: EventSequence,
    ) -> PersistentEventRows<P> {
        loop {
            match Tables::load_group_members::<P>(&self.pool, &[group], floor).await {
                Ok(groups) => {
                    if let Some(rows) = groups
                        .into_iter()
                        .find(|(id, _)| *id == group)
                        .map(|(_, rows)| rows)
                        .filter(|rows| !rows.is_empty())
                    {
                        return rows;
                    }
                    record_empty_group_fetch(u64::from(floor), i64::from(group));
                }
                Err(error) => record_fetch_failed(&error, u64::from(floor)),
            }
            tokio::time::sleep(FETCH_RETRY_INTERVAL).await;
        }
    }

    /// Record where the fold is, without blocking it.
    ///
    /// Captured at `sequence` *after* placing it, which is the same point the
    /// fold position is published — so the triple is consistent by
    /// construction. Fire-and-forget: the content is a pure function of the
    /// table, so a lost write costs only a longer resume next time.
    fn checkpoint(&mut self, sequence: EventSequence) {
        self.groups_since_checkpoint = 0;
        self.last_checkpoint = tokio::time::Instant::now();
        if !matches!(self.sink, Sink::Live(_)) {
            return;
        }
        let checkpoint = CommitCheckpoint {
            sequence,
            commit_seq: self.head,
            open_groups: self.seen.iter().map(|(g, max)| (*g, *max)).collect(),
        };
        let pool = self.pool.clone();
        tokio::spawn(async move {
            if let Err(error) = Tables::write_commit_checkpoint(&pool, &checkpoint).await {
                record_checkpoint_failed(&error, u64::from(checkpoint.sequence));
            }
        });
    }
}

/// What a backfill needs to run a private fold: the same inputs the live one
/// was spawned with.
struct BackfillSource<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pool: sqlx::PgPool,
    cache: Arc<PersistentOutboxEventCache<P, Tables>>,
    page: usize,
    checkpoint_every: usize,
    checkpoint_interval: std::time::Duration,
}

impl<P, Tables> Clone for BackfillSource<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            cache: self.cache.clone(),
            page: self.page,
            checkpoint_every: self.checkpoint_every,
            checkpoint_interval: self.checkpoint_interval,
        }
    }
}

/// Serve one commit-lane backfill request by **re-folding** from the nearest
/// checkpoint.
///
/// There is no log to page: the consumer's cursor names a position, the
/// nearest checkpoint at or below it names where the fold that produced that
/// position was, and replaying from there reproduces it exactly (the
/// numbering is a pure function of the table). Overshoot — rows re-folded but
/// below the consumer's cursor, so never sent — is bounded by the checkpoint
/// cadence.
async fn serve_commit_backfill<P, Tables>(
    source: BackfillSource<P, Tables>,
    after: CommitSequence,
    until: CommitSequence,
    sender: mpsc::Sender<CommitTransport<P>>,
) where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    let BackfillSource {
        pool,
        cache,
        page,
        checkpoint_every,
        checkpoint_interval,
    } = source;
    let seed = match Tables::load_commit_checkpoint_for(&pool, after).await {
        Ok(seed) => seed,
        Err(error) => {
            record_checkpoint_read_failed(&error, u64::from(after));
            return;
        }
    };
    let (from, head, seen) = match seed {
        Some(checkpoint) => (
            checkpoint.sequence,
            checkpoint.commit_seq,
            checkpoint.open_groups.into_iter().collect(),
        ),
        None => (EventSequence::BEGIN, CommitSequence::BEGIN, HashMap::new()),
    };
    record_backfill_started(u64::from(after), u64::from(from), u64::from(head));

    let mut listener = PersistentOutboxListener::transport_stream(cache.handle(), Some(from), page);
    let mut sequencer = Sequencer::<P, Tables> {
        pool,
        head,
        commit_head: None,
        fold_position: None,
        sink: Sink::Backfill {
            sender,
            after,
            until,
        },
        seen,
        checkpoint_every,
        checkpoint_interval,
        groups_since_checkpoint: 0,
        last_checkpoint: tokio::time::Instant::now(),
        _phantom: std::marker::PhantomData,
    };

    loop {
        let Some(batch) = next_batch(&mut listener, page).await else {
            return;
        };
        if !sequencer.fold_batch(batch).await {
            return;
        }
    }
}

/// Wait for one delivery, then take whatever else is already buffered, up to
/// `page`. `None` means the stream ended.
async fn next_batch<P, S>(listener: &mut S, page: usize) -> Option<Vec<Transport<InsertOrder, P>>>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
    S: StreamExt<Item = Transport<InsertOrder, P>> + Unpin,
{
    let first = listener.next().await?;
    let mut batch = Vec::with_capacity(page);
    batch.push(first);
    while batch.len() < page {
        match listener.next().now_or_never() {
            Some(Some(delivery)) => batch.push(delivery),
            Some(None) => break,
            None => break,
        }
    }
    Some(batch)
}

/// Start this process's sequencer. Called from `Outbox::init` when the commit
/// lane is [`Enabled`](crate::CommitLane::Enabled).
///
/// The fold resumes from the newest checkpoint, or from the beginning of the
/// stream when there is none — so enabling the lane late is the same code
/// path as a restart after downtime, only longer. The numbering is a pure
/// function of the persisted table, so what it produces is what a sequencer
/// running from day one would have produced.
pub(crate) async fn spawn<P, Tables>(
    pool: &sqlx::PgPool,
    cache: Arc<PersistentOutboxEventCache<P, Tables>>,
    buffer_size: usize,
    page: usize,
    checkpoint_every: usize,
    checkpoint_interval: std::time::Duration,
) -> Result<SequencerHandle<P>, sqlx::Error>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    let page = page.max(1);
    let frontier = Tables::highest_known_persistent_sequence(pool).await?;
    let seed = Tables::load_commit_checkpoint(pool, frontier).await?;
    let (from, head, seen): (_, _, HashMap<_, _>) = match seed {
        Some(checkpoint) => (
            checkpoint.sequence,
            checkpoint.commit_seq,
            checkpoint.open_groups.into_iter().collect(),
        ),
        None => (EventSequence::BEGIN, CommitSequence::BEGIN, HashMap::new()),
    };
    record_started(
        u64::from(from),
        u64::from(frontier),
        u64::from(frontier).saturating_sub(u64::from(from)),
    );

    let (commit_sender, _) = broadcast::channel(buffer_size);
    let commit_head = Arc::new(AtomicU64::new(u64::from(head)));
    let fold_position = Arc::new(AtomicU64::new(u64::from(from)));
    let (backfill_request, mut backfill_rx) = mpsc::unbounded_channel();
    let backfill_source = BackfillSource {
        pool: pool.clone(),
        cache: cache.clone(),
        page,
        checkpoint_every: checkpoint_every.max(1),
        checkpoint_interval,
    };
    let backfill_head = commit_head.clone();

    let mut listener =
        PersistentOutboxListener::transport_stream(cache.handle(), Some(from), buffer_size);
    let mut sequencer = Sequencer::<P, Tables> {
        pool: pool.clone(),
        head,
        commit_head: Some(commit_head.clone()),
        fold_position: Some(fold_position.clone()),
        sink: Sink::Live(commit_sender.clone()),
        seen,
        checkpoint_every: checkpoint_every.max(1),
        checkpoint_interval,
        groups_since_checkpoint: 0,
        last_checkpoint: tokio::time::Instant::now(),
        _phantom: std::marker::PhantomData,
    };

    let task = spawn_supervised("obix::commit_sequencer", async move {
        loop {
            tokio::select! {
                request = backfill_rx.recv() => {
                    match request {
                        Some((after, sender)) => {
                            // Sampled here, not inside the task: the hand-off
                            // point is the head as of the request, so a fold
                            // that runs on cannot keep the backfill running
                            // behind it forever.
                            let until = CommitSequence::from(
                                backfill_head.load(Ordering::Acquire),
                            );
                            tokio::spawn(serve_commit_backfill::<P, Tables>(
                                backfill_source.clone(),
                                after,
                                until,
                                sender,
                            ));
                        }
                        None => break,
                    }
                }

                batch = next_batch(&mut listener, page) => {
                    match batch {
                        Some(batch) => {
                            if !sequencer.fold_batch(batch).await {
                                break;
                            }
                        }
                        None => {
                            record_stream_closed();
                            break;
                        }
                    }
                }
            }
        }
    });

    Ok(SequencerHandle {
        commit_head: commit_head.clone(),
        commit_sender,
        backfill_request,
        backfill_buffer_size: page,
        positions: SequencerPositions {
            fold_position,
            commit_head,
        },
        _task: OwnedTaskHandle::new(task),
    })
}

/// Where the fold is resuming from and how far it has to go — so that
/// enabling the commit lane on a database with history is visible as a
/// backfill in the logs rather than as unexplained load.
#[tracing::instrument(
    name = "obix.sequencer.started",
    level = "info",
    skip_all,
    fields(from_sequence = from_sequence, insert_frontier = insert_frontier, behind = behind),
)]
fn record_started(from_sequence: u64, insert_frontier: u64, behind: u64) {}

/// Which checkpoint a backfill re-folds from — the assertion test 22 reads.
#[tracing::instrument(
    name = "obix.sequencer.backfill_started",
    level = "info",
    skip_all,
    fields(after = after, from_sequence = from_sequence, from_commit_seq = from_commit_seq),
)]
fn record_backfill_started(after: u64, from_sequence: u64, from_commit_seq: u64) {}

#[tracing::instrument(
    name = "obix.sequencer.fetch_failed",
    level = "warn",
    skip_all,
    fields(error = %error, floor = floor),
)]
fn record_fetch_failed(error: &sqlx::Error, floor: u64) {}

/// A single-group fetch came back `Ok` but without `group` at all — per the
/// INVARIANT in `Sequencer::place`, this can only be a transient visibility
/// miss (every member is already committed by the time the fold sees any
/// one of them), never `group`'s true membership. Logged at `warn` because
/// the caller retries silently otherwise, and this should never fire.
#[tracing::instrument(
    name = "obix.sequencer.empty_group_fetch",
    level = "warn",
    fields(floor = floor, group = group),
)]
fn record_empty_group_fetch(floor: u64, group: i64) {}

#[tracing::instrument(
    name = "obix.sequencer.checkpoint_failed",
    level = "warn",
    skip_all,
    fields(error = %error, sequence = sequence),
)]
fn record_checkpoint_failed(error: &sqlx::Error, sequence: u64) {}

#[tracing::instrument(
    name = "obix.sequencer.checkpoint_read_failed",
    level = "warn",
    skip_all,
    fields(error = %error, after = after),
)]
fn record_checkpoint_read_failed(error: &sqlx::Error, after: u64) {}

#[tracing::instrument(
    name = "obix.sequencer.stream_closed",
    level = "error",
    fields(otel.status_code = "ERROR"),
)]
fn record_stream_closed() {}
