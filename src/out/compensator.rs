use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{broadcast, mpsc};

use std::sync::Arc;

use crate::{
    handle::{OwnedTaskHandle, spawn_supervised},
    out::event::PersistentDelivery,
    sequence::EventSequence,
    tables::MailboxTables,
};

/// Per-process compensator for this process's own abandoned sequence
/// allocations — the reactive tier of the gap-fill design.
///
/// A publish's `INSERT .. RETURNING sequence` runs in `pre_commit`, so an
/// in-process allocation is only ever abandoned by a transaction failing
/// *after* the persist ran: a later commit hook or post-persist hook
/// erroring, or the COMMIT itself failing (serialization conflict,
/// connection loss). `PersistEvents`' `Drop` detects exactly that state
/// (persisted events never consumed by `post_commit`) and reports the
/// sequences here; this task inserts their placeholders and delivers them —
/// into the local cache-fill stream directly, and to other processes via a
/// `(min, max)` report to the debounced notifier. Stalls behind such a
/// rollback clear in milliseconds instead of waiting out the grace-gated
/// backstop.
///
/// Only the owning process compensates its own sequences, so this path has
/// no cross-node contention by construction and needs no fill lock. The
/// insert is `ON CONFLICT DO NOTHING`: if the "failed" commit actually
/// landed server-side (ambiguous commit), the placeholders no-op against
/// the real rows. It may also briefly park on the aborting transaction's
/// speculative-insertion lock — sqlx issues the ROLLBACK asynchronously —
/// a wait bounded by the rollback itself.
pub(crate) struct AbandonedCompensator {
    tx: mpsc::UnboundedSender<Vec<EventSequence>>,
    _handle: Arc<OwnedTaskHandle>,
}

impl Clone for AbandonedCompensator {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            _handle: self._handle.clone(),
        }
    }
}

impl AbandonedCompensator {
    /// How long a failed compensation insert waits before retrying, and how
    /// many attempts are made before the sequences are left to the
    /// grace-gated backstop.
    const RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);
    const MAX_ATTEMPTS: u32 = 5;

    pub fn spawn<P, Tables>(
        pool: &sqlx::PgPool,
        cache_fill_sender: broadcast::Sender<PersistentDelivery<P>>,
        notifier_tx: mpsc::UnboundedSender<(EventSequence, EventSequence)>,
    ) -> Self
    where
        P: Serialize + DeserializeOwned + Send + Sync + 'static,
        Tables: MailboxTables,
    {
        let (tx, rx) = mpsc::unbounded_channel();
        let handle = spawn_supervised(
            "obix::abandoned_compensator",
            Self::run::<P, Tables>(pool.clone(), cache_fill_sender, notifier_tx, rx),
        );
        Self {
            tx,
            _handle: Arc::new(OwnedTaskHandle::new(handle)),
        }
    }

    /// Sender for `PersistEvents::drop`'s abandoned-sequence reports;
    /// unbounded so the sync `Drop` never blocks.
    pub fn report_sender(&self) -> mpsc::UnboundedSender<Vec<EventSequence>> {
        self.tx.clone()
    }

    async fn run<P, Tables>(
        pool: sqlx::PgPool,
        cache_fill_sender: broadcast::Sender<PersistentDelivery<P>>,
        notifier_tx: mpsc::UnboundedSender<(EventSequence, EventSequence)>,
        mut rx: mpsc::UnboundedReceiver<Vec<EventSequence>>,
    ) where
        P: Serialize + DeserializeOwned + Send + Sync + 'static,
        Tables: MailboxTables,
    {
        while let Some(mut sequences) = rx.recv().await {
            while let Ok(more) = rx.try_recv() {
                sequences.extend(more);
            }

            let mut attempts = 0;
            loop {
                match Tables::fill_gaps::<P>(&pool, sequences.clone()).await {
                    Ok(placeholders) => {
                        let mut range: Option<(EventSequence, EventSequence)> = None;
                        for item in placeholders {
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
                        break;
                    }
                    Err(error) => {
                        attempts += 1;
                        record_compensation_failed(&error, attempts);
                        if attempts >= Self::MAX_ATTEMPTS {
                            // The backstop proves and fills them later.
                            break;
                        }
                        tokio::time::sleep(Self::RETRY_INTERVAL).await;
                    }
                }
            }
        }
    }
}

#[tracing::instrument(
    name = "obix.abandoned_compensator.fill_failed",
    level = "warn",
    skip_all,
    fields(error = %error, attempts = attempts),
)]
fn record_compensation_failed(error: &sqlx::Error, attempts: u32) {}
