use crate::error::ObixFault;
use es_entity::errlanes::Laned;
use futures::{FutureExt, StreamExt};
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{broadcast, mpsc};
use tracing::Instrument;

use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use super::cache::PersistentOutboxEventCache;
use super::listener::{PersistentOutboxListener, TransportStream};
use crate::{
    handle::{OwnedTaskHandle, spawn_supervised},
    out::event::*,
    out::lane::{CommitOrder, InsertOrder, LaneHandle, Upstream},
    sequence::{CommitGroupId, CommitSequence, EventSequence},
    tables::{CommitCheckpoint, GroupOrderRow, MailboxTables, PersistentEventRows},
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
    /// The insert head against this fold's position: what the commit head
    /// does not show yet while the fold catches up after a boot.
    upstream: Upstream,
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
        .with_upstream(self.upstream.clone())
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

    /// Which fold this sink belongs to, as a static label for spans.
    fn kind(&self) -> &'static str {
        match self {
            Self::Live(_) => "live",
            Self::Backfill { .. } => "backfill",
        }
    }

    fn finished(&self, head: CommitSequence) -> bool {
        match self {
            Self::Live(_) => false,
            Self::Backfill { until, .. } => head >= *until,
        }
    }
}

/// What a folded batch asks of its driver.
enum FoldStep {
    /// Keep folding.
    Continue,
    /// A backfill's sink has reached its `until`.
    Finished,
    /// The sink's receiver hung up.
    ConsumerGone,
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
    /// first-sighted group's members in one statement.
    async fn fold_batch(&mut self, batch: Vec<GroupOrderRow>) -> FoldStep {
        let mut members = self.fetch_members(&batch).await;
        for row in batch {
            let sequence = row.sequence;
            if !self.place(row, &mut members).await {
                return FoldStep::ConsumerGone;
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
                return FoldStep::Finished;
            }
        }
        FoldStep::Continue
    }

    /// One statement for every group first sighted in `batch`. The floor is the
    /// lowest first sight: no group's members sit below their own MIN.
    async fn fetch_members(
        &self,
        batch: &[GroupOrderRow],
    ) -> HashMap<CommitGroupId, PersistentEventRows<P>> {
        let mut wanted: Vec<CommitGroupId> = Vec::new();
        let mut floor: Option<EventSequence> = None;
        for row in batch {
            if !row.has_payload {
                continue;
            }
            let group = row.commit_group;
            if self.seen.contains_key(&group) || wanted.contains(&group) {
                continue;
            }
            wanted.push(group);
            floor.get_or_insert(row.sequence);
        }
        let Some(floor) = floor else {
            return HashMap::new();
        };

        // Retried, never skipped: giving up would drop these groups from the
        // lane, and a retry re-reads the same rows for the same numbering.
        loop {
            match self.load_members(&wanted, floor).await {
                Ok(groups) => return groups.into_iter().collect(),
                Err(error) => {
                    record_fetch_failed(error, u64::from(floor));
                    tokio::time::sleep(FETCH_RETRY_INTERVAL).await;
                }
            }
        }
    }

    /// Emit one delivery's group on first sight, or establish it needs none.
    async fn place(
        &mut self,
        row: GroupOrderRow,
        members: &mut HashMap<CommitGroupId, PersistentEventRows<P>>,
    ) -> bool {
        if !row.has_payload {
            return true;
        }
        let sequence = row.sequence;
        let group = row.commit_group;
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

    /// One `load_group_members` statement, with the rows it returned recorded
    /// on a span of its own: the payload reads of the fold's second pass.
    async fn load_members(
        &self,
        groups: &[CommitGroupId],
        floor: EventSequence,
    ) -> Result<Vec<(CommitGroupId, PersistentEventRows<P>)>, sqlx::Error> {
        let span = tracing::debug_span!(
            "obix.sequencer.group_fetch",
            fold = self.sink.kind(),
            floor = u64::from(floor),
            groups = groups.len(),
            rows = tracing::field::Empty,
        );
        let loaded = Tables::load_group_members::<P>(&self.pool, groups, floor)
            .instrument(span.clone())
            .await;
        if let Ok(loaded) = &loaded {
            span.record(
                "rows",
                loaded.iter().map(|(_, rows)| rows.len()).sum::<usize>(),
            );
        }
        loaded
    }

    /// Fetch one group's membership, retrying until it is non-empty.
    async fn fetch_group(
        &self,
        group: CommitGroupId,
        floor: EventSequence,
    ) -> PersistentEventRows<P> {
        loop {
            match self.load_members(&[group], floor).await {
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
                Err(error) => record_fetch_failed(error, u64::from(floor)),
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
                record_checkpoint_failed(error, u64::from(checkpoint.sequence));
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

/// Where a fold resumes: a checkpoint's state, or the start of the stream.
struct FoldSeed {
    from: EventSequence,
    head: CommitSequence,
    seen: HashMap<CommitGroupId, EventSequence>,
}

impl FoldSeed {
    fn start() -> Self {
        Self {
            from: EventSequence::BEGIN,
            head: CommitSequence::BEGIN,
            seen: HashMap::new(),
        }
    }
}

impl From<CommitCheckpoint> for FoldSeed {
    fn from(checkpoint: CommitCheckpoint) -> Self {
        Self {
            from: checkpoint.sequence,
            head: checkpoint.commit_seq,
            seen: checkpoint.open_groups.into_iter().collect(),
        }
    }
}

/// What a backfill's re-fold did: the commit positions it advanced and how it
/// ended (see [`record_backfill_finished`]).
struct FoldReport {
    rows: u64,
    outcome: &'static str,
}

/// How many leading rows of `page` continue the gapless run above `cursor`.
/// The insert lane is gapless up to what the live fold has passed, so a short
/// run means the page cannot be fed to the fold past that point.
fn contiguous_run(cursor: EventSequence, page: &[GroupOrderRow]) -> usize {
    let mut expected = cursor;
    let mut run = 0;
    for row in page {
        expected = expected.next();
        if row.sequence != expected {
            break;
        }
        run += 1;
    }
    run
}

fn order_row<P>(delivery: &Transport<InsertOrder, P>) -> GroupOrderRow
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    GroupOrderRow {
        sequence: delivery.sequence(),
        commit_group: delivery.commit_group(),
        has_payload: delivery.has_payload(),
    }
}

/// A backfill's walk of the insert lane, in order. It reads payload-free pages
/// of `(sequence, group, has-payload)` straight from the table, so each payload
/// is read once, by the group fetch. On a page it cannot trust (a hole, or
/// nothing where the fold still needs rows) it hands over to the insert-lane
/// transport stream from the last contiguous row, which waits out holes the
/// way the live fold does.
struct OrderScan<'a, P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    source: &'a BackfillSource<P, Tables>,
    /// The highest sequence handed to the fold so far.
    cursor: EventSequence,
    /// Set once the scan has fallen back to the transport stream.
    stream: Option<TransportStream<InsertOrder, P>>,
}

impl<'a, P, Tables> OrderScan<'a, P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    fn new(source: &'a BackfillSource<P, Tables>, from: EventSequence) -> Self {
        Self {
            source,
            cursor: from,
            stream: None,
        }
    }

    /// The next batch of ordering facts. `None` means the scan ended.
    async fn next_batch(&mut self) -> Option<Vec<GroupOrderRow>> {
        let page = self.source.page;
        loop {
            if let Some(stream) = &mut self.stream {
                let batch = next_batch(stream, page).await?;
                return Some(batch.iter().map(order_row).collect());
            }
            let mut rows = self.read_order_page().await;
            // Never skip: keep the trusted prefix, and carry on from the last
            // row of it on the transport stream.
            let run = contiguous_run(self.cursor, &rows);
            let intact = run > 0 && run == rows.len();
            rows.truncate(run);
            if let Some(last) = rows.last() {
                self.cursor = last.sequence;
            }
            if !intact {
                record_order_scan_fallback(u64::from(self.cursor));
                self.stream = Some(PersistentOutboxListener::transport_stream(
                    self.source.cache.handle(),
                    Some(self.cursor),
                    page,
                ));
            }
            if !rows.is_empty() {
                return Some(rows);
            }
        }
    }

    /// One order page, retried on error and never skipped.
    async fn read_order_page(&self) -> Vec<GroupOrderRow> {
        loop {
            let span = tracing::debug_span!(
                "obix.sequencer.group_order_page",
                after = u64::from(self.cursor),
                limit = self.source.page,
                rows = tracing::field::Empty,
            );
            let loaded =
                Tables::load_group_order_page(&self.source.pool, self.cursor, self.source.page)
                    .instrument(span.clone())
                    .await;
            match loaded {
                Ok(rows) => {
                    span.record("rows", rows.len());
                    return rows;
                }
                Err(error) => {
                    record_order_page_failed(error, u64::from(self.cursor));
                    tokio::time::sleep(FETCH_RETRY_INTERVAL).await;
                }
            }
        }
    }
}

/// Fold the insert lane from `seed` until the sink's `until`, delivering the
/// positions above `after` into `sender`.
async fn refold<P, Tables>(
    source: &BackfillSource<P, Tables>,
    seed: FoldSeed,
    after: CommitSequence,
    until: CommitSequence,
    sender: mpsc::Sender<CommitTransport<P>>,
) -> FoldReport
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    let mut scan = OrderScan::new(source, seed.from);
    let mut sequencer = Sequencer::<P, Tables> {
        pool: source.pool.clone(),
        head: seed.head,
        commit_head: None,
        fold_position: None,
        sink: Sink::Backfill {
            sender,
            after,
            until,
        },
        seen: seed.seen,
        checkpoint_every: source.checkpoint_every,
        checkpoint_interval: source.checkpoint_interval,
        groups_since_checkpoint: 0,
        last_checkpoint: tokio::time::Instant::now(),
        _phantom: std::marker::PhantomData,
    };

    let from_commit = sequencer.head;
    let outcome = loop {
        let Some(batch) = scan.next_batch().await else {
            break "stream_ended";
        };
        match sequencer.fold_batch(batch).await {
            FoldStep::Continue => {}
            FoldStep::Finished => break "reached_until",
            FoldStep::ConsumerGone => break "consumer_gone",
        }
    };
    FoldReport {
        rows: u64::from(sequencer.head).saturating_sub(u64::from(from_commit)),
        outcome,
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
    let seed = match Tables::load_commit_checkpoint_for(&source.pool, after).await {
        Ok(seed) => seed,
        Err(error) => {
            record_checkpoint_read_failed(error, u64::from(after));
            return;
        }
    };
    let seed = seed.map(FoldSeed::from).unwrap_or_else(FoldSeed::start);
    let from = seed.from;
    record_backfill_started(u64::from(after), u64::from(from), u64::from(seed.head));
    let serving = std::time::Instant::now();

    let report = refold(&source, seed, after, until, sender).await;
    record_backfill_finished(
        u64::from(after),
        u64::from(until),
        u64::from(from),
        report.rows,
        serving.elapsed().as_millis() as u64,
        report.outcome,
    );
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
                            let batch = batch.iter().map(order_row).collect();
                            if !matches!(
                                sequencer.fold_batch(batch).await,
                                FoldStep::Continue
                            ) {
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
        upstream: Upstream {
            source_head: cache.head_counter(),
            derived_through: fold_position.clone(),
        },
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

/// What one commit-lane backfill cost: the commit positions its re-fold
/// advanced (`rows`, `head - from_commit`) against the rows it actually owed
/// (`owed = until - after`). A `rows` far above `owed` means the re-fold
/// started well below the request, which is the price of reconstructing the
/// numbering from a checkpoint. Groups are emitted whole, so `rows` can exceed
/// `owed` slightly even when a checkpoint sat exactly at `after`.
///
/// `outcome` says how the re-fold ended: `reached_until`, `consumer_gone` (the
/// sink's receiver hung up) or `stream_ended` (the input ended). Only the
/// first is a complete backfill.
#[tracing::instrument(
    name = "obix.sequencer.backfill_finished",
    level = "info",
    skip_all,
    fields(
        after = after,
        until = until,
        from_sequence = from_sequence,
        rows = rows,
        owed = until.saturating_sub(after),
        elapsed_ms = elapsed_ms,
        outcome = outcome,
    ),
)]
fn record_backfill_finished(
    after: u64,
    until: u64,
    from_sequence: u64,
    rows: u64,
    elapsed_ms: u64,
    outcome: &'static str,
) {
}

/// A backfill's order scan met a page it cannot trust — a hole, or nothing
/// where the fold still needed rows — and handed the rest of the fold to the
/// insert-lane transport stream. Carries the sequence only.
#[tracing::instrument(
    name = "obix.sequencer.order_scan_fallback",
    level = "warn",
    skip_all,
    fields(sequence = sequence),
)]
fn record_order_scan_fallback(sequence: u64) {}

#[tracing::instrument(
    name = "obix.sequencer.order_page_failed",
    level = "warn",
    skip_all,
    fields(
        error = tracing::field::Empty,
        error.lane = tracing::field::Empty,
        error.code = tracing::field::Empty,
        error.level = tracing::field::Empty,
        exception.message = tracing::field::Empty,
        exception.type = tracing::field::Empty,
        after = after,
    ),
)]
fn record_order_page_failed(fault: impl Into<ObixFault>, after: u64) {
    fault.into().record(&tracing::Span::current());
}

#[tracing::instrument(
    name = "obix.sequencer.fetch_failed",
    level = "warn",
    skip_all,
    fields(
        error = tracing::field::Empty,
        error.lane = tracing::field::Empty,
        error.code = tracing::field::Empty,
        error.level = tracing::field::Empty,
        exception.message = tracing::field::Empty,
        exception.type = tracing::field::Empty,
        floor = floor,
    ),
)]
fn record_fetch_failed(fault: impl Into<ObixFault>, floor: u64) {
    fault.into().record(&tracing::Span::current());
}

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
    fields(
        error = tracing::field::Empty,
        error.lane = tracing::field::Empty,
        error.code = tracing::field::Empty,
        error.level = tracing::field::Empty,
        exception.message = tracing::field::Empty,
        exception.type = tracing::field::Empty,
        sequence = sequence,
    ),
)]
fn record_checkpoint_failed(fault: impl Into<ObixFault>, sequence: u64) {
    fault.into().record(&tracing::Span::current());
}

#[tracing::instrument(
    name = "obix.sequencer.checkpoint_read_failed",
    level = "warn",
    skip_all,
    fields(
        error = tracing::field::Empty,
        error.lane = tracing::field::Empty,
        error.code = tracing::field::Empty,
        error.level = tracing::field::Empty,
        exception.message = tracing::field::Empty,
        exception.type = tracing::field::Empty,
        after = after,
    ),
)]
fn record_checkpoint_read_failed(fault: impl Into<ObixFault>, after: u64) {
    fault.into().record(&tracing::Span::current());
}

#[tracing::instrument(
    name = "obix.sequencer.stream_closed",
    level = "error",
    fields(otel.status_code = "ERROR"),
)]
fn record_stream_closed() {}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(sequence: u64) -> GroupOrderRow {
        GroupOrderRow {
            sequence: EventSequence::from(sequence),
            commit_group: CommitGroupId::from(1),
            has_payload: true,
        }
    }

    #[test]
    fn contiguous_run_covers_a_gapless_page() {
        let page = [row(11), row(12), row(13)];
        assert_eq!(contiguous_run(EventSequence::from(10), &page), 3);
    }

    #[test]
    fn contiguous_run_stops_at_a_hole() {
        let page = [row(11), row(12), row(14), row(15)];
        assert_eq!(contiguous_run(EventSequence::from(10), &page), 2);
    }

    #[test]
    fn contiguous_run_is_zero_when_the_page_starts_past_the_cursor() {
        let page = [row(12), row(13)];
        assert_eq!(contiguous_run(EventSequence::from(10), &page), 0);
        assert_eq!(contiguous_run(EventSequence::from(10), &[]), 0);
    }

    #[test]
    fn contiguous_run_rejects_repeats_and_reordering() {
        let page = [row(11), row(11), row(12)];
        assert_eq!(contiguous_run(EventSequence::from(10), &page), 1);
        let page = [row(11), row(13), row(12)];
        assert_eq!(contiguous_run(EventSequence::from(10), &page), 1);
    }
}
