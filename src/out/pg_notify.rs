use tokio::sync::mpsc;

use std::time::Duration;

use crate::{
    handle::{OwnedTaskHandle, spawn_supervised},
    tables::MailboxTables,
};

/// A message from the pg LISTEN pump to a cache loop.
pub(crate) enum NotifyMessage {
    /// A notification arrived on one of the subscribed channels.
    Notification(sqlx::postgres::PgNotification),
    /// The LISTEN connection was lost and has been re-established.
    /// Notifications sent while it was down are gone — the cache must
    /// resync against the tables instead of trusting the notify stream.
    Resync,
}

const INITIAL_RECONNECT_BACKOFF: Duration = Duration::from_millis(50);
const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(5);

pub async fn spawn_pg_listener<Tables>(
    pool: &sqlx::PgPool,
    persistent_notification_tx: mpsc::Sender<NotifyMessage>,
    ephemeral_notification_tx: mpsc::Sender<NotifyMessage>,
) -> Result<OwnedTaskHandle, sqlx::Error>
where
    Tables: MailboxTables,
{
    let pool = pool.clone();
    let persistent_channel = Tables::persistent_outbox_events_channel();
    let ephemeral_channel = Tables::ephemeral_outbox_events_channel();

    // The initial connection stays fail-fast: a broken setup should surface
    // at init, not as an endlessly-retrying background task.
    let mut listener = connect_and_listen(&pool, persistent_channel, ephemeral_channel).await?;

    let handle = spawn_supervised("obix::pg_listener", async move {
        loop {
            // `try_recv` (unlike `recv`) surfaces connection loss as
            // `Ok(None)` after sqlx has already reconnected and re-issued
            // LISTEN. `recv` would swallow that marker — and any notifications sent
            // while the connection was down would be silently lost with no
            // resync, stalling every consumer until the next notification.
            match listener.try_recv().await {
                Ok(Some(notification)) => {
                    // Route notification to appropriate channel with backpressure
                    let result = if notification.channel() == persistent_channel {
                        persistent_notification_tx
                            .send(NotifyMessage::Notification(notification))
                            .await
                    } else if notification.channel() == ephemeral_channel {
                        ephemeral_notification_tx
                            .send(NotifyMessage::Notification(notification))
                            .await
                    } else {
                        // Unknown channel, skip
                        continue;
                    };

                    // If send fails, receiver is dropped, so break
                    if let Err(e) = result {
                        record_forward_failed(&e);
                        break;
                    }
                }
                Ok(None) => {
                    // Connection was lost; sqlx already reconnected and
                    // re-issued LISTEN. Tell both caches to resync for
                    // whatever was notified during the gap.
                    record_connection_lost();
                    if send_resync(&persistent_notification_tx, &ephemeral_notification_tx)
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Err(error) => {
                    // Reconnecting inside `try_recv` failed (e.g. the pool
                    // was saturated at that moment). This pump must outlive
                    // transient failures: exiting here drops the notification
                    // senders, which kills the cache loops and leaves the
                    // outbox permanently deaf while every consumer still
                    // looks healthy. Rebuild the listener with capped
                    // backoff, then resync.
                    record_recv_error(&error);
                    let mut backoff = INITIAL_RECONNECT_BACKOFF;
                    listener = loop {
                        tokio::time::sleep(backoff).await;
                        match connect_and_listen(&pool, persistent_channel, ephemeral_channel).await
                        {
                            Ok(listener) => break listener,
                            Err(error) => {
                                record_reconnect_failed(&error);
                                backoff = (backoff * 2).min(MAX_RECONNECT_BACKOFF);
                            }
                        }
                    };
                    record_reconnected();
                    // Resync only after LISTEN is active again: anything
                    // published before this point is covered by the resync
                    // (the head re-read happens after), anything after by
                    // regular notifications.
                    if send_resync(&persistent_notification_tx, &ephemeral_notification_tx)
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }
        }
    });

    Ok(OwnedTaskHandle::new(handle))
}

async fn connect_and_listen(
    pool: &sqlx::PgPool,
    persistent_channel: &'static str,
    ephemeral_channel: &'static str,
) -> Result<sqlx::postgres::PgListener, sqlx::Error> {
    let mut listener = sqlx::postgres::PgListener::connect_with(pool).await?;
    listener
        .listen_all([persistent_channel, ephemeral_channel])
        .await?;
    Ok(listener)
}

async fn send_resync(
    persistent_notification_tx: &mpsc::Sender<NotifyMessage>,
    ephemeral_notification_tx: &mpsc::Sender<NotifyMessage>,
) -> Result<(), ()> {
    for tx in [persistent_notification_tx, ephemeral_notification_tx] {
        if let Err(e) = tx.send(NotifyMessage::Resync).await {
            record_forward_failed(&e);
            return Err(());
        }
    }
    Ok(())
}

#[tracing::instrument(
    name = "obix.pg_listener.recv_error",
    level = "error",
    skip_all,
    fields(otel.status_code = "ERROR", error = %error),
)]
fn record_recv_error(error: &sqlx::Error) {}

#[tracing::instrument(
    name = "obix.pg_listener.connection_lost",
    level = "error",
    fields(otel.status_code = "ERROR"),
)]
fn record_connection_lost() {}

#[tracing::instrument(
    name = "obix.pg_listener.reconnect_failed",
    level = "error",
    skip_all,
    fields(otel.status_code = "ERROR", error = %error),
)]
fn record_reconnect_failed(error: &sqlx::Error) {}

#[tracing::instrument(name = "obix.pg_listener.reconnected", level = "warn")]
fn record_reconnected() {}

#[tracing::instrument(
    name = "obix.pg_listener.forward_failed",
    level = "error",
    skip_all,
    fields(otel.status_code = "ERROR", error = %error),
)]
fn record_forward_failed<T>(error: &tokio::sync::mpsc::error::SendError<T>) {}
