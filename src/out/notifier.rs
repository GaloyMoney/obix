use tokio::sync::mpsc;

use std::{sync::Arc, time::Duration};

use crate::{
    handle::{OwnedTaskHandle, spawn_supervised},
    sequence::EventSequence,
    tables::MailboxTables,
};

/// Per-process debounced NOTIFY emitter for the persistent outbox.
///
/// Every notify-bearing commit serializes on a cluster-wide lock inside
/// PostgreSQL's `PreCommit_Notify`, held across the commit's WAL flush —
/// so persist statements no longer embed `pg_notify`. Instead the commit
/// hook's `post_commit` reports each committed batch's `(min, max)`
/// sequences here, and one background task coalesces reports and emits at
/// most one `pg_notify` per debounce interval, with the exact
/// `{min_sequence, max_sequence}` payload shape listeners already accept.
///
/// Safe because notifications are hints, not transport: listeners clamp
/// claims against the sequence's `last_value` and always fetch events from
/// the table. A lost hint costs wake-up latency only (bounded by the cache
/// loop's idle resync), never correctness — which is also why the emission
/// runs with `synchronous_commit = off`, shrinking its own lock hold to
/// sub-ms.
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

    /// Sender on which `PersistEvents::post_commit` reports the
    /// `(min, max)` sequences of a committed batch. Unbounded so the sync
    /// `post_commit` can send without blocking; bounded in practice by the
    /// drain-per-tick in [`run`](Self::run).
    pub fn report_sender(&self) -> mpsc::UnboundedSender<(EventSequence, EventSequence)> {
        self.tx.clone()
    }

    async fn run(
        pool: sqlx::PgPool,
        channel: &'static str,
        debounce: Duration,
        mut rx: mpsc::UnboundedReceiver<(EventSequence, EventSequence)>,
    ) {
        // The running fold of every not-yet-emitted report. Survives emit
        // failures (the range is retried next tick), so a report is only
        // ever dropped if the process dies — which the idle resync in other
        // processes covers.
        let mut pending: Option<(EventSequence, EventSequence)> = None;
        loop {
            if pending.is_none() {
                match rx.recv().await {
                    Some(report) => pending = Some(report),
                    // All senders dropped: the outbox is gone.
                    None => return,
                }
            }
            Self::drain(&mut rx, &mut pending);
            tokio::time::sleep(debounce).await;
            // Coalesce everything that arrived during the sleep.
            Self::drain(&mut rx, &mut pending);

            let (min, max) = pending.expect("pending set before emit");
            match Self::emit(&pool, channel, min, max).await {
                Ok(()) => pending = None,
                // Keep the folded range; retry next tick. SQL errors must
                // not kill the task (spawn_supervised covers panics).
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

    /// One round trip, no explicit transaction. `set_config(..,
    /// is_local => true)` scopes `synchronous_commit = off` to this
    /// statement's implicit transaction — a lost hint is harmless, so the
    /// relaxed durability carries zero risk and keeps the NOTIFY lock hold
    /// sub-ms. Runtime query: the channel name is per-`Tables` and there is
    /// no compile-time checking value in a `SELECT set_config, pg_notify`.
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
