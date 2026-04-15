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
                    tracing::error_span!(
                        "obix.pg_listener.recv_error",
                        otel.status_code = "ERROR",
                        error = %e,
                    )
                    .in_scope(|| ());
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
                tracing::error_span!(
                    "obix.pg_listener.forward_failed",
                    otel.status_code = "ERROR",
                    error = %e,
                )
                .in_scope(|| ());
                break;
            }
        }
    });

    Ok(OwnedTaskHandle::new(handle))
}
