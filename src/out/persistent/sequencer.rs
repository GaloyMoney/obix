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

/// How far this process's fold has got. Both values are in-process: the lane is
/// computed, so every `Enabled` process derives the same numbering on its own.
#[derive(Clone)]
pub(crate) struct SequencerPositions {
    fold_position: Arc<AtomicU64>,
    commit_head: Arc<AtomicU64>,
}

impl SequencerPositions {
    /// The highest insert sequence this fold has passed: emitted, or skipped.
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
    Live(broadcast::Sender<CommitTransport<P>>),
    /// One backfill request: positions in `(after, until]`, then the
    /// consumer's listener takes over from the broadcast.
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

    fn finished(&self, head: CommitSequence) -> bool {
        match self {
            Self::Live(_) => false,
            Self::Backfill { until, .. } => head >= *until,
        }
    }
}

/// Folds the insert-ordered stream into commit order: a group is emitted whole
/// at first sight of its lowest member, its members in `sequence` order, and a
/// position is a running count of rows emitted. Pure function of the events
/// table; only sparse [`CommitCheckpoint`]s of the fold are persisted.
struct Sequencer<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pool: sqlx::PgPool,
    /// Rows emitted so far — the next group is numbered from `head + 1`.
    head: CommitSequence,
    /// Published copy of `head`, for the fence and for listeners.
    commit_head: Option<Arc<AtomicU64>>,
    fold_position: Option<Arc<AtomicU64>>,
    sink: Sink<P>,
    /// Emitted groups with members the fold has not reached yet, by highest
    /// member. Seeded from a checkpoint so a resumed fold cannot re-emit them.
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
    /// Fold a drained batch of insert-lane deliveries, fetching every
    /// first-sighted group's members in one statement. `false` means stop.
    async fn fold_batch(&mut self, batch: Vec<Transport<InsertOrder, P>>) -> bool {
        let mut members = self.fetch_members(&batch).await;
        for delivery in batch {
            let sequence = delivery.sequence();
            if !self.place(delivery, &mut members).await {
                return false;
            }
            // INVARIANT: publish only after this delivery is accounted for —
            // its group emitted and the head advanced, or provably never. The
            // fence reads this then the head, so publishing earlier lets it
            // return with the frontier event still undelivered. It must also
            // advance on every early return in `place`, or the fence stalls.
            if let Some(position) = &self.fold_position {
                position.store(u64::from(sequence), Ordering::Release);
            }
            if self.sink.finished(self.head) {
                return false;
            }
        }
        true
    }

    /// One statement for every group first sighted in `batch`. The floor is the
    /// lowest first sight: no group's members sit below their own MIN.
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

        // Retried, never skipped: giving up would drop these groups from the
        // lane, and a retry re-reads the same rows for the same numbering.
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

    /// Emit one delivery's group on first sight, or establish it needs none.
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

        // INVARIANT: a group's members all commit in one transaction, so once
        // any one is delivered the rest are committed too. An empty result is
        // therefore a transient visibility miss, never the true membership —
        // retried like a fetch error, since accepting it would drop the group.
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

    /// Fetch one group's membership, retrying until it is non-empty.
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

    /// Record where the fold is. Fire-and-forget: a lost write only costs a
    /// longer resume next time.
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

/// Serve one commit-lane backfill by re-folding from the nearest checkpoint at
/// or below `after`, which reproduces the same numbering.
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

/// Start this process's sequencer, resuming from the newest checkpoint or from
/// the beginning of the stream when there is none.
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
                            // Sampled here, not in the task: the hand-off point
                            // is the head as of the request, so an advancing
                            // fold cannot outrun the backfill forever.
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

#[tracing::instrument(
    name = "obix.sequencer.started",
    level = "info",
    skip_all,
    fields(from_sequence = from_sequence, insert_frontier = insert_frontier, behind = behind),
)]
fn record_started(from_sequence: u64, insert_frontier: u64, behind: u64) {}

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

/// Should never fire: see the INVARIANT in `Sequencer::place`.
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
