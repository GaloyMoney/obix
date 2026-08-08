use tokio::sync::mpsc;

use std::{sync::Arc, time::Duration};

use crate::{
    handle::{OwnedTaskHandle, spawn_supervised},
    sequence::EventSequence,
    tables::MailboxTables,
};

/// Per-process debounced NOTIFY emitter for the persistent outbox.
///
/// Notify-bearing commits serialize on a cluster-wide lock, so persist
/// statements no longer embed `pg_notify`; `post_commit` reports each
/// committed batch's `(min, max)` here and one task emits at most one
/// `pg_notify` per debounce interval. Safe because notifications are hints
/// (listeners clamp and always fetch from the table): a lost hint costs
/// wake-up latency only, bounded by the cache loop's idle resync.
pub(crate) struct PersistentNotifier {
    tx: mpsc::UnboundedSender<(EventSequence, EventSequence)>,
    _handle: Arc<OwnedTaskHandle>,
}

impl Clone for PersistentNotifier {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            _handle: self._handle.clone(),
        }
    }
}

/// Same struct shape `handle_notification` in `persistent/cache.rs` parses.
#[derive(serde::Serialize)]
struct NotificationPayload {
    min_sequence: EventSequence,
    max_sequence: EventSequence,
}

impl PersistentNotifier {
    pub fn spawn<Tables: MailboxTables>(pool: &sqlx::PgPool, debounce: Duration) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let pool = pool.clone();
        let channel = Tables::persistent_outbox_events_channel();
        let handle = spawn_supervised(
            "obix::persistent_notifier",
            Self::run(pool, channel, debounce, rx),
        );
        Self {
            tx,
            _handle: Arc::new(OwnedTaskHandle::new(handle)),
        }
    }

    /// Sender for `post_commit`'s `(min, max)` batch reports; unbounded so
    /// the sync `post_commit` never blocks.
    pub fn report_sender(&self) -> mpsc::UnboundedSender<(EventSequence, EventSequence)> {
        self.tx.clone()
    }

    /// Fold reports into one `(min, max)` per tick and emit. A failed emit
    /// keeps the fold and retries next tick; exits when all senders drop.
    async fn run(
        pool: sqlx::PgPool,
        channel: &'static str,
        debounce: Duration,
        mut rx: mpsc::UnboundedReceiver<(EventSequence, EventSequence)>,
    ) {
        let mut pending: Option<(EventSequence, EventSequence)> = None;
        loop {
            if pending.is_none() {
                match rx.recv().await {
                    Some(report) => pending = Some(report),
                    None => return,
                }
            }
            Self::drain(&mut rx, &mut pending);
            tokio::time::sleep(debounce).await;
            Self::drain(&mut rx, &mut pending);

            let (min, max) = pending.expect("pending set before emit");
            match Self::emit(&pool, channel, min, max).await {
                Ok(()) => pending = None,
                Err(error) => record_notify_emit_failed(&error),
            }
        }
    }

    fn drain(
        rx: &mut mpsc::UnboundedReceiver<(EventSequence, EventSequence)>,
        pending: &mut Option<(EventSequence, EventSequence)>,
    ) {
        while let Ok((min, max)) = rx.try_recv() {
            *pending = Some(match *pending {
                Some((lo, hi)) => (lo.min(min), hi.max(max)),
                None => (min, max),
            });
        }
    }

    /// One round trip; `set_config(.., is_local => true)` scopes
    /// `synchronous_commit = off` to this statement — safe for a hint and
    /// keeps the NOTIFY lock hold sub-ms.
    async fn emit(
        pool: &sqlx::PgPool,
        channel: &str,
        min_sequence: EventSequence,
        max_sequence: EventSequence,
    ) -> Result<(), sqlx::Error> {
        let payload = serde_json::to_string(&NotificationPayload {
            min_sequence,
            max_sequence,
        })
        .expect("Could not serialize notification payload");
        sqlx::query("SELECT set_config('synchronous_commit', 'off', true), pg_notify($1, $2)")
            .bind(channel)
            .bind(payload)
            .execute(pool)
            .await?;
        Ok(())
    }
}

#[tracing::instrument(
    name = "obix.persistent_notifier.emit_failed",
    level = "warn",
    skip_all,
    fields(error = %error),
)]
fn record_notify_emit_failed(error: &sqlx::Error) {}
