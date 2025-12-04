pub(crate) mod event;

use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::broadcast;

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use crate::{config::*, tables::*};
pub use event::*;

pub struct Outbox<P, Tables = DefaultMailboxTables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
    Tables: MailboxTables,
{
    event_sender: broadcast::Sender<OutboxEvent<P>>,
    highest_known_sequence: Arc<AtomicU64>,
    config: MailboxConfig,
    _phantom: std::marker::PhantomData<Tables>,
}

impl<P, Tables> Outbox<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    pub async fn init(pool: &sqlx::PgPool, config: MailboxConfig) -> Result<Self, sqlx::Error> {
        let pool = pool.clone();
        let (sender, _) = broadcast::channel(config.event_buffer_size);
        let highest_known_sequence = Arc::new(AtomicU64::from(
            <Tables as MailboxTables>::highest_known_persistent_sequence(&pool).await?,
        ));

        Self::spawn_pg_listeners(pool, sender.clone(), Arc::clone(&highest_known_sequence)).await?;
        Ok(Self {
            event_sender: sender,
            highest_known_sequence,
            config,
            _phantom: std::marker::PhantomData,
        })
    }

    async fn spawn_pg_listeners(
        pool: sqlx::PgPool,
        sender: broadcast::Sender<OutboxEvent<P>>,
        highest_known_sequence: Arc<AtomicU64>,
    ) -> Result<(), sqlx::Error> {
        let mut listener = sqlx::postgres::PgListener::connect_with(&pool).await?;
        listener.listen("persistent_outbox_events").await?;
        let persistent_sender = sender.clone();
        tokio::spawn(async move {
            loop {
                if let Ok(notification) = listener.recv().await
                    && let Ok(event) =
                        serde_json::from_str::<PersistentOutboxEvent<P>>(notification.payload())
                {
                    let new_highest_sequence = u64::from(event.sequence);
                    highest_known_sequence.fetch_max(new_highest_sequence, Ordering::AcqRel);
                    if persistent_sender.send(event.into()).is_err() {
                        break;
                    }
                }
            }
        });

        let mut listener = sqlx::postgres::PgListener::connect_with(&pool).await?;
        listener.listen("ephemeral_outbox_events").await?;
        tokio::spawn(async move {
            loop {
                if let Ok(notification) = listener.recv().await
                    && let Ok(event) =
                        serde_json::from_str::<EphemeralOutboxEvent<P>>(notification.payload())
                    && sender.send(event.into()).is_err()
                {
                    break;
                }
            }
        });
        Ok(())
    }
}
