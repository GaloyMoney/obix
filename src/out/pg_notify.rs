use crate::error::ObixFault;
use es_entity::errlanes::Laned;
use tokio::sync::mpsc;
use tracing::Instrument;

use std::time::Duration;

use crate::handle::{OwnedTaskHandle, spawn_supervised};

/// A message from the pg LISTEN pump to a cache loop.
pub(crate) enum NotifyMessage {
    /// A notification arrived on the subscribed channel.
    Notification(sqlx::postgres::PgNotification),
    /// The LISTEN connection was lost and has been re-established.
    /// Notifications sent while it was down are gone — the cache must
    /// resync against the tables instead of trusting the notify stream.
    Resync,
}

const INITIAL_RECONNECT_BACKOFF: Duration = Duration::from_millis(50);
const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(5);

pub async fn spawn_pg_listener(
    pool: &sqlx::PgPool,
    channel: &'static str,
    notification_tx: mpsc::Sender<NotifyMessage>,
) -> Result<OwnedTaskHandle, sqlx::Error> {
    let pool = pool.clone();

    // The initial connection stays fail-fast: a broken setup should surface
    // at init, not as an endlessly-retrying background task.
    let mut listener = connect_and_listen(&pool, channel).await?;

    let pump = async move {
        loop {
            // `try_recv` (unlike `recv`) surfaces connection loss as
            // `Ok(None)` after sqlx has already reconnected and re-issued
            // LISTEN. `recv` would swallow that marker — and any notifications sent
            // while the connection was down would be silently lost with no
            // resync, stalling every consumer until the next notification.
            match listener.try_recv().await {
                Ok(Some(notification)) => {
                    // A LISTEN connection can still receive stray channels.
                    if notification.channel() != channel {
                        continue;
                    }

                    // If send fails, receiver is dropped, so break
                    if let Err(e) = notification_tx
                        .send(NotifyMessage::Notification(notification))
                        .await
                    {
                        record_forward_failed(&e);
                        break;
                    }
                }
                Ok(None) => {
                    // Connection was lost; sqlx already reconnected and
                    // re-issued LISTEN. Tell the cache to resync for
                    // whatever was notified during the gap.
                    record_connection_lost();
                    if let Err(e) = notification_tx.send(NotifyMessage::Resync).await {
                        record_forward_failed(&e);
                        break;
                    }
                }
                Err(error) => {
                    // Reconnecting inside `try_recv` failed (e.g. the pool
                    // was saturated at that moment). This pump must outlive
                    // transient failures: exiting here drops the notification
                    // sender, which kills the cache loop and leaves the
                    // outbox permanently deaf while every consumer still
                    // looks healthy. Rebuild the listener with capped
                    // backoff, then resync.
                    record_recv_error(&error);
                    let mut backoff = INITIAL_RECONNECT_BACKOFF;
                    listener = loop {
                        tokio::time::sleep(backoff).await;
                        match connect_and_listen(&pool, channel).await {
                            Ok(listener) => break listener,
                            Err(error) => {
                                record_reconnect_failed(error);
                                backoff = (backoff * 2).min(MAX_RECONNECT_BACKOFF);
                            }
                        }
                    };
                    record_reconnected();
                    // Resync only after LISTEN is active again: anything
                    // published before this point is covered by the resync
                    // (the head re-read happens after), anything after by
                    // regular notifications.
                    if let Err(e) = notification_tx.send(NotifyMessage::Resync).await {
                        record_forward_failed(&e);
                        break;
                    }
                }
            }
        }
    }
    .instrument(tracing::info_span!("obix.pg_listener", channel));

    Ok(OwnedTaskHandle::new(spawn_supervised(
        "obix::pg_listener",
        pump,
    )))
}

async fn connect_and_listen(
    pool: &sqlx::PgPool,
    channel: &'static str,
) -> Result<sqlx::postgres::PgListener, sqlx::Error> {
    let mut listener = sqlx::postgres::PgListener::connect_with(pool).await?;
    listener.listen(channel).await?;
    Ok(listener)
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
    fields(
        otel.status_code = "ERROR",
        error = tracing::field::Empty,
        error.lane = tracing::field::Empty,
        error.code = tracing::field::Empty,
        error.level = tracing::field::Empty,
        exception.message = tracing::field::Empty,
        exception.type = tracing::field::Empty,
    ),
)]
fn record_reconnect_failed(fault: impl Into<ObixFault>) {
    fault.into().record(&tracing::Span::current());
}

#[tracing::instrument(name = "obix.pg_listener.reconnected", level = "warn")]
fn record_reconnected() {}

#[tracing::instrument(
    name = "obix.pg_listener.forward_failed",
    level = "error",
    skip_all,
    fields(otel.status_code = "ERROR", error = %error),
)]
fn record_forward_failed<T>(error: &tokio::sync::mpsc::error::SendError<T>) {}
