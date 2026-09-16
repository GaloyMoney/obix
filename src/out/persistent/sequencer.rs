use futures::StreamExt;
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream};

use std::collections::{BTreeSet, HashMap};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use super::cache::CacheHandle;
use super::listener::PersistentOutboxListener;
use crate::{
    handle::{OwnedTaskHandle, spawn_supervised},
    out::event::*,
    sequence::{CommitGroupId, CommitSequence, EventSequence},
    tables::{CommitGroupAppend, MailboxTables},
};

/// How long a failed append waits before retrying the same group.
const APPEND_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(250);

/// What a commit-ordered listener needs from the sequencer: the lane's
/// fan-out and its head.
pub(crate) struct SequencerHandle<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    commit_head: Arc<AtomicU64>,
    fold_position: Arc<AtomicU64>,
    commit_sender: broadcast::Sender<CommitDelivery<P>>,
    backfill_request: mpsc::UnboundedSender<(CommitSequence, mpsc::Sender<CommitDelivery<P>>)>,
    backfill_buffer_size: usize,
    _task: OwnedTaskHandle,
}

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
    pub(crate) fn lane(&self) -> CommitLaneHandle<P> {
        CommitLaneHandle {
            commit_head: self.commit_head.clone(),
            commit_event_receiver: Some(self.commit_sender.subscribe()),
            backfill_request: self.backfill_request.clone(),
            backfill_buffer_size: self.backfill_buffer_size,
        }
    }

    /// The non-generic positions this sequencer publishes — deliberately
    /// decoupled from the listener plumbing above (which is generic over the
    /// payload type `P`), so a payload-agnostic caller like
    /// [`Subscription`](crate::out::Subscription) can hold one without a `P`
    /// parameter of its own.
    pub(crate) fn positions(&self) -> SequencerPositions {
        SequencerPositions {
            fold_position: self.fold_position.clone(),
        }
    }
}

/// Read-only positions the sequencer publishes, with no dependency on the
/// outbox's payload type. Backs the commit-lane [`await_caught_up`]
/// (crate::out::Subscription::await_caught_up) fence: `fold_position` is how
/// far the fold has *examined* the insert-lane stream (advanced for every
/// delivery, including placeholders and already-seen group members), which
/// is the position that fence must wait past — not the commit log's
/// `logged_through_sequence`, which only advances when a group is appended
/// and therefore stalls behind an open group or a quiet stream (see
/// obix-dev/handoff-commit-lane-consumer-gaps.md §4).
#[derive(Clone)]
pub(crate) struct SequencerPositions {
    fold_position: Arc<AtomicU64>,
}

impl SequencerPositions {
    /// Highest insert `EventSequence` the fold has examined — in-process,
    /// advances past placeholders, `logged_ahead` seeds and already-seen
    /// group members, none of which the durable `logged_through_sequence`
    /// watermark advances past.
    pub(crate) fn fold_position(&self) -> EventSequence {
        EventSequence::from(self.fold_position.load(Ordering::Acquire))
    }
}

/// What a [`CommitOrderedListener`](super::CommitOrderedListener) needs from
/// the commit lane.
pub struct CommitLaneHandle<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    commit_head: Arc<AtomicU64>,
    commit_event_receiver: Option<broadcast::Receiver<CommitDelivery<P>>>,
    backfill_request: mpsc::UnboundedSender<(CommitSequence, mpsc::Sender<CommitDelivery<P>>)>,
    backfill_buffer_size: usize,
}

impl<P> CommitLaneHandle<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn head(&self) -> CommitSequence {
        CommitSequence::from(self.commit_head.load(Ordering::Relaxed))
    }

    pub fn commit_event_stream(&mut self) -> BroadcastStream<CommitDelivery<P>> {
        BroadcastStream::new(
            self.commit_event_receiver
                .take()
                .expect("receiver already taken"),
        )
    }

    pub fn request_commit_backfill(
        &self,
        start_after: CommitSequence,
    ) -> ReceiverStream<CommitDelivery<P>> {
        let (tx, rx) = mpsc::channel(self.backfill_buffer_size);
        let _ = self.backfill_request.send((start_after, tx));
        ReceiverStream::new(rx)
    }
}

/// Folds the insert-ordered stream into commit order and materialises it
/// into the commit log.
///
/// One task per process, always running. It consumes a
/// [`PersistentOutboxListener`] from its stored cursor, so contiguity, gap
/// parking and lag recovery are the listener's job, not this one's. Every
/// process folds the same stream from the same state row and therefore
/// computes the same log, including `commit_seq`; the per-group cursor check
/// is what makes each group appended exactly once.
struct Sequencer<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pool: sqlx::PgPool,
    page: usize,
    head: CommitSequence,
    commit_head: Arc<AtomicU64>,
    /// Published unconditionally as the fold examines each insert-lane
    /// delivery — see [`SequencerPositions::fold_position`].
    fold_position: Arc<AtomicU64>,
    commit_sender: broadcast::Sender<CommitDelivery<P>>,
    /// Groups appended but whose later members the fold has not reached yet,
    /// mapped to their highest member. Dropped as the stream passes them.
    seen: HashMap<CommitGroupId, EventSequence>,
    /// Sequences already in the log at or above the resume cursor.
    ///
    /// A group appended before a crash can have members above the cursor the
    /// crash left behind; without this seed the resumed fold would append it
    /// a second time. Drained as the stream reaches each one.
    logged_ahead: BTreeSet<EventSequence>,
    _phantom: std::marker::PhantomData<Tables>,
}

impl<P, Tables> Sequencer<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
    Tables: MailboxTables,
{
    /// Fold one insert-lane delivery.
    async fn fold(&mut self, delivery: PersistentDelivery<P>) {
        let sequence = delivery.sequence();
        // INVARIANT: published before every branch below, including the
        // skips — fold_position must advance on placeholders too, or the
        // commit-lane fence stalls on an aborted tail.
        self.fold_position
            .store(u64::from(sequence), Ordering::Release);
        if self.logged_ahead.remove(&sequence) {
            return;
        }
        if !delivery.has_payload() {
            return;
        }
        let group = delivery.commit_group();
        if let Some(group_max) = self.seen.get(&group).copied() {
            if sequence >= group_max {
                self.seen.remove(&group);
            }
            return;
        }

        let append = self.append(group, sequence).await;
        if append.group_max > sequence {
            self.seen.insert(group, append.group_max);
        }
        if append.appended.is_empty() {
            self.deliver_tail().await;
            return;
        }
        for row in append.appended {
            let delivery = CommitDelivery::from(row);
            self.head = self.head.max(delivery.commit_sequence);
            let _ = self.commit_sender.send(delivery);
        }
        self.commit_head
            .store(u64::from(self.head), Ordering::Release);
    }

    /// Append one group, retrying the same group until the statement
    /// succeeds.
    ///
    /// Skipping on error would lose the group permanently: the cursor
    /// advances at the next group and a restart resumes above it. The
    /// statement is a no-op once the cursor has passed `at`, so retrying is
    /// always safe.
    async fn append(&self, group: CommitGroupId, at: EventSequence) -> CommitGroupAppend<P> {
        loop {
            match Tables::append_commit_group::<P>(&self.pool, group, at).await {
                Ok(append) => return append,
                Err(error) => {
                    record_append_failed(&error, u64::from(at));
                    tokio::time::sleep(APPEND_RETRY_INTERVAL).await;
                }
            }
        }
    }

    /// Read and broadcast whatever another process appended. Paging stops at
    /// a short page: the log is dense, so a short page means the head, never
    /// a gap to wait on.
    async fn deliver_tail(&mut self) {
        loop {
            let rows = match Tables::load_commit_ordered_page::<P>(&self.pool, self.head, self.page)
                .await
            {
                Ok(rows) => rows,
                Err(error) => {
                    record_tail_read_failed(&error, u64::from(self.head));
                    return;
                }
            };
            if rows.is_empty() {
                return;
            }
            let returned = rows.len();
            for row in rows {
                let delivery = CommitDelivery::from(row);
                self.head = self.head.max(delivery.commit_sequence);
                let _ = self.commit_sender.send(delivery);
            }
            self.commit_head
                .store(u64::from(self.head), Ordering::Release);
            if returned < self.page {
                return;
            }
        }
    }
}

/// Serve one commit-lane backfill request by paging the log.
///
/// No park, no probe, no gap report: the log is dense, so a short page means
/// the reader reached the head. A read error ends the request with the
/// listener's cursor unchanged, and its next poll asks again.
async fn serve_commit_backfill<P, Tables>(
    pool: sqlx::PgPool,
    page: usize,
    mut after: CommitSequence,
    sender: mpsc::Sender<CommitDelivery<P>>,
) where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
    Tables: MailboxTables,
{
    loop {
        // Don't spend a query without demand.
        match sender.reserve().await {
            Ok(permit) => drop(permit),
            Err(_) => return,
        }
        let rows = match Tables::load_commit_ordered_page::<P>(&pool, after, page).await {
            Ok(rows) => rows,
            Err(error) => {
                record_tail_read_failed(&error, u64::from(after));
                return;
            }
        };
        if rows.is_empty() {
            return;
        }
        let returned = rows.len();
        for row in rows {
            let delivery = CommitDelivery::from(row);
            after = after.max(delivery.commit_sequence);
            if sender.send(delivery).await.is_err() {
                return;
            }
        }
        if returned < page {
            return;
        }
    }
}

/// Start this process's sequencer. Always called from `Outbox::init`: a
/// process with no commit-ordered listener still folds and still appends,
/// so the log never lags a live process.
pub(crate) async fn spawn<P, Tables>(
    pool: &sqlx::PgPool,
    cache: CacheHandle<P>,
    buffer_size: usize,
    page: usize,
) -> Result<SequencerHandle<P>, sqlx::Error>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    let page = page.max(1);
    let restart = Tables::commit_log_restart_state(pool).await?;
    let logged_ahead: BTreeSet<EventSequence> = restart.logged_ahead.into_iter().collect();

    let (commit_sender, _) = broadcast::channel(buffer_size);
    let commit_head = Arc::new(AtomicU64::new(u64::from(restart.last_commit_seq)));
    let fold_position = Arc::new(AtomicU64::new(u64::from(restart.logged_through)));
    let (backfill_request, mut backfill_rx) = mpsc::unbounded_channel();
    let backfill_pool = pool.clone();

    let mut listener =
        PersistentOutboxListener::deliveries(cache, Some(restart.logged_through), buffer_size);
    let mut sequencer = Sequencer::<P, Tables> {
        pool: pool.clone(),
        page,
        head: restart.last_commit_seq,
        commit_head: commit_head.clone(),
        fold_position: fold_position.clone(),
        commit_sender: commit_sender.clone(),
        seen: HashMap::new(),
        logged_ahead,
        _phantom: std::marker::PhantomData,
    };

    let task = spawn_supervised("obix::commit_sequencer", async move {
        // INVARIANT: the published head is reconciled against the log before
        // the fold runs. The seeded sequences are skipped without appending,
        // so nothing else in the loop would correct a head that a peer has
        // already moved past, and a listener trusting it would never backfill.
        sequencer.deliver_tail().await;

        loop {
            tokio::select! {
                request = backfill_rx.recv() => {
                    match request {
                        Some((after, sender)) => {
                            tokio::spawn(serve_commit_backfill::<P, Tables>(
                                backfill_pool.clone(),
                                page,
                                after,
                                sender,
                            ));
                        }
                        None => break,
                    }
                }

                delivery = listener.next() => {
                    match delivery {
                        Some(delivery) => sequencer.fold(delivery).await,
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
        commit_head,
        fold_position,
        commit_sender,
        backfill_request,
        backfill_buffer_size: page,
        _task: OwnedTaskHandle::new(task),
    })
}

#[tracing::instrument(
    name = "obix.sequencer.append_failed",
    level = "warn",
    skip_all,
    fields(error = %error, sequence = sequence),
)]
fn record_append_failed(error: &sqlx::Error, sequence: u64) {}

#[tracing::instrument(
    name = "obix.sequencer.tail_read_failed",
    level = "warn",
    skip_all,
    fields(error = %error, head = head),
)]
fn record_tail_read_failed(error: &sqlx::Error, head: u64) {}

#[tracing::instrument(
    name = "obix.sequencer.stream_closed",
    level = "error",
    fields(otel.status_code = "ERROR"),
)]
fn record_stream_closed() {}
