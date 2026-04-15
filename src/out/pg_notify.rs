use tokio::sync::mpsc;

use crate::{
    handle::{OwnedTaskHandle, spawn_supervised},
    tables::MailboxTables,
};

pub async fn spawn_pg_listener<Tables>(
    pool: &sqlx::PgPool,
    persistent_notification_tx: mpsc::Sender<sqlx::postgres::PgNotification>,
    ephemeral_notification_tx: mpsc::Sender<sqlx::postgres::PgNotification>,
) -> Result<OwnedTaskHandle, sqlx::Error>
where
    Tables: MailboxTables,
{
    let pool = pool.clone();
    let mut listener = sqlx::postgres::PgListener::connect_with(&pool).await?;
    listener
        .listen_all([
            Tables::persistent_outbox_events_channel(),
            Tables::ephemeral_outbox_events_channel(),
        ])
        .await?;

    let handle = spawn_supervised("obix::pg_listener", async move {
        loop {
            let notification = match listener.recv().await {
                Ok(notification) => notification,
                Err(e) => {
                    record_recv_error(&e);
                    break;
                }
            };

            // Route notification to appropriate channel with backpressure
            let result = if notification.channel() == Tables::persistent_outbox_events_channel() {
                persistent_notification_tx.send(notification).await
            } else if notification.channel() == Tables::ephemeral_outbox_events_channel() {
                ephemeral_notification_tx.send(notification).await
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
    });

    Ok(OwnedTaskHandle::new(handle))
}

#[tracing::instrument(
    name = "obix.pg_listener.recv_error",
    level = "error",
    skip_all,
    fields(otel.status_code = "ERROR", error = %error),
)]
fn record_recv_error(error: &sqlx::Error) {}

#[tracing::instrument(
    name = "obix.pg_listener.forward_failed",
    level = "error",
    skip_all,
    fields(otel.status_code = "ERROR", error = %error),
)]
fn record_forward_failed<T>(error: &tokio::sync::mpsc::error::SendError<T>) {}
