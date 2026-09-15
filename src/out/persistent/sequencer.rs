use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{broadcast, mpsc, watch};

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use crate::{
    handle::{OwnedTaskHandle, spawn_supervised},
    out::{event::*, gap_fill::GapFillRequest, pg_notify::NotifyMessage},
    sequence::{CommitSequence, EventSequence},
    tables::MailboxTables,
};

/// What a commit-ordered listener needs from the sequencer: the lane's
/// fan-out and its head.
pub(crate) struct SequencerHandle<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub(crate) commit_head: Arc<AtomicU64>,
    pub(crate) commit_sender: broadcast::Sender<PersistentDelivery<P>>,
    pub(crate) backfill_request:
        mpsc::UnboundedSender<(CommitSequence, mpsc::Sender<PersistentDelivery<P>>)>,
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

/// Derives commit order and materialises it into the commit log.
///
/// One task per process, started only once a commit-ordered listener exists
/// there. Ticks are leaderless: each is a single statement that try-locks the
/// state row, so a loser skips rather than blocks and a process dying
/// mid-tick leaves committed state untouched.
struct Sequencer<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pool: sqlx::PgPool,
    page: usize,
    head: CommitSequence,
    commit_head: Arc<AtomicU64>,
    commit_sender: broadcast::Sender<PersistentDelivery<P>>,
    gap_fill_tx: mpsc::UnboundedSender<GapFillRequest>,
    /// Sequences at or below this were allocated before this process's cache
    /// loop started, so a hole below it is history rather than a live writer.
    init_head: u64,
    _phantom: std::marker::PhantomData<Tables>,
}

impl<P, Tables> Sequencer<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
    Tables: MailboxTables,
{
    /// Extend the log as far as the frontier allows.
    ///
    /// Stops on losing the state-row lock, on a hole blocking the scan short
    /// of its window, or on the scan watermark reaching the frontier. A page
    /// cap is not a stop: more remains below the frontier, so it ticks again.
    async fn tick_until_quiet(&mut self, frontier: EventSequence) {
        loop {
            let tick = match Tables::sequence_tick::<P>(&self.pool, frontier, self.page).await {
                Ok(Some(tick)) => tick,
                Ok(None) => return,
                Err(error) => {
                    record_tick_failed(&error, u64::from(frontier));
                    return;
                }
            };

            if !tick.appended.is_empty() {
                self.head = tick.head;
                self.commit_head
                    .store(u64::from(tick.head), Ordering::Release);
                for item in tick.appended {
                    let _ = self.commit_sender.send(PersistentDelivery::from(item));
                }
            }

            if tick.f_stop < tick.f_eff {
                self.report_hole(tick.f_stop).await;
                return;
            }
            if tick.scan_water >= frontier {
                return;
            }
        }
    }

    /// A hole stopped the scan. Below `init_head` it is history this process
    /// never watched, so the gap filler is told; its placeholders move the
    /// frontier and re-tick us. At or above `init_head` the frontier is
    /// contiguous by construction, so a hole there is a bug, not a gap.
    async fn report_hole(&self, f_stop: EventSequence) {
        let next_needed = u64::from(f_stop) + 1;
        if next_needed > self.init_head {
            record_unexpected_hole(next_needed, self.init_head);
            return;
        }
        let up_to = EventSequence::from(self.init_head.min(next_needed + self.page as u64));
        match Tables::missing_sequences(&self.pool, f_stop, up_to).await {
            Ok(missing) if !missing.is_empty() => {
                let _ = self.gap_fill_tx.send(GapFillRequest::Historical(missing));
            }
            Ok(_) => {}
            Err(error) => record_tick_failed(&error, next_needed),
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
            for item in rows {
                let delivery = PersistentDelivery::from(item);
                if let Some(commit_sequence) = delivery.commit_sequence() {
                    self.head = self.head.max(commit_sequence);
                }
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
    sender: mpsc::Sender<PersistentDelivery<P>>,
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
        for item in rows {
            let delivery = PersistentDelivery::from(item);
            if let Some(commit_sequence) = delivery.commit_sequence() {
                after = after.max(commit_sequence);
            }
            if sender.send(delivery).await.is_err() {
                return;
            }
        }
        if returned < page {
            return;
        }
    }
}

/// Start the sequencer for this process.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn<P, Tables>(
    pool: &sqlx::PgPool,
    page: usize,
    buffer_size: usize,
    init_head: u64,
    mut frontier_rx: watch::Receiver<EventSequence>,
    mut commit_notification_rx: mpsc::Receiver<NotifyMessage>,
    gap_fill_tx: mpsc::UnboundedSender<GapFillRequest>,
    idle_resync_interval: std::time::Duration,
    head: CommitSequence,
) -> SequencerHandle<P>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
    Tables: MailboxTables,
{
    let (commit_sender, _) = broadcast::channel(buffer_size);
    let commit_head = Arc::new(AtomicU64::new(u64::from(head)));
    let (backfill_request, mut backfill_rx) = mpsc::unbounded_channel();
    let backfill_pool = pool.clone();
    let backfill_page = page.max(1);

    let mut sequencer = Sequencer::<P, Tables> {
        pool: pool.clone(),
        page: page.max(1),
        head,
        commit_head: commit_head.clone(),
        commit_sender: commit_sender.clone(),
        gap_fill_tx,
        init_head,
        _phantom: std::marker::PhantomData,
    };

    let task = spawn_supervised("obix::commit_sequencer", async move {
        let frontier = *frontier_rx.borrow_and_update();
        sequencer.tick_until_quiet(frontier).await;
        sequencer.deliver_tail().await;

        loop {
            tokio::select! {
                request = backfill_rx.recv() => {
                    match request {
                        Some((after, sender)) => {
                            tokio::spawn(serve_commit_backfill::<P, Tables>(
                                backfill_pool.clone(),
                                backfill_page,
                                after,
                                sender,
                            ));
                        }
                        None => break,
                    }
                }

                changed = frontier_rx.changed() => {
                    if changed.is_err() {
                        record_frontier_closed();
                        break;
                    }
                    let frontier = *frontier_rx.borrow_and_update();
                    sequencer.tick_until_quiet(frontier).await;
                }

                message = commit_notification_rx.recv() => {
                    match message {
                        Some(_) => sequencer.deliver_tail().await,
                        None => {
                            record_notification_closed();
                            break;
                        }
                    }
                }

                _ = tokio::time::sleep(idle_resync_interval) => {
                    let frontier = *frontier_rx.borrow_and_update();
                    sequencer.tick_until_quiet(frontier).await;
                    sequencer.deliver_tail().await;
                }
            }
        }
    });

    SequencerHandle {
        commit_head,
        commit_sender,
        backfill_request,
        _task: OwnedTaskHandle::new(task),
    }
}

#[tracing::instrument(
    name = "obix.sequencer.tick_failed",
    level = "warn",
    skip_all,
    fields(error = %error, frontier = frontier),
)]
fn record_tick_failed(error: &sqlx::Error, frontier: u64) {}

#[tracing::instrument(
    name = "obix.sequencer.tail_read_failed",
    level = "warn",
    skip_all,
    fields(error = %error, head = head),
)]
fn record_tail_read_failed(error: &sqlx::Error, head: u64) {}

#[tracing::instrument(
    name = "obix.sequencer.unexpected_hole",
    level = "error",
    fields(otel.status_code = "ERROR"),
)]
fn record_unexpected_hole(sequence: u64, init_head: u64) {}

#[tracing::instrument(
    name = "obix.sequencer.frontier_closed",
    level = "error",
    fields(otel.status_code = "ERROR"),
)]
fn record_frontier_closed() {}

#[tracing::instrument(
    name = "obix.sequencer.notification_closed",
    level = "error",
    fields(otel.status_code = "ERROR"),
)]
fn record_notification_closed() {}
