use tokio::sync::mpsc;

use crate::{handle::OwnedTaskHandle, tables::MailboxTables};

pub async fn spawn_pg_listener<Tables>(
    pool: &sqlx::PgPool,
    persistent_notification_tx: mpsc::UnboundedSender<sqlx::postgres::PgNotification>,
    ephemeral_notification_tx: mpsc::UnboundedSender<sqlx::postgres::PgNotification>,
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

    let handle = tokio::spawn(async move {
        while let Ok(notification) = listener.recv().await {
            let sent = if notification.channel() == Tables::persistent_outbox_events_channel() {
                persistent_notification_tx.send(notification).is_ok()
            } else if notification.channel() == Tables::ephemeral_outbox_events_channel() {
                ephemeral_notification_tx.send(notification).is_ok()
            } else {
                continue;
            };

            if !sent {
                break;
            }
        }
    });

    Ok(OwnedTaskHandle::new(handle))
}
