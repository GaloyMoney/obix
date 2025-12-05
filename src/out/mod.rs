pub(crate) mod event;

use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::broadcast;

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use crate::{config::*, handle::OwnedTaskHandle, tables::*};
pub use event::*;

#[allow(dead_code)]
pub struct Outbox<P, Tables = DefaultMailboxTables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
    Tables: MailboxTables,
{
    event_sender: broadcast::Sender<OutboxEvent<P>>,
    highest_known_sequence: Arc<AtomicU64>,
    _listener_handle: Arc<OwnedTaskHandle>,
    _phantom: std::marker::PhantomData<Tables>,
}

impl<P, Tables> Clone for Outbox<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
    Tables: MailboxTables,
{
    fn clone(&self) -> Self {
        Self {
            event_sender: self.event_sender.clone(),
            highest_known_sequence: self.highest_known_sequence.clone(),
            _listener_handle: self._listener_handle.clone(),
            _phantom: self._phantom.clone(),
        }
    }
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
            Tables::highest_known_persistent_sequence(&pool).await?,
        ));

        let handle =
            Self::spawn_pg_listener(pool, sender.clone(), Arc::clone(&highest_known_sequence))
                .await?;

        Ok(Self {
            event_sender: sender,
            highest_known_sequence,
            _listener_handle: Arc::new(handle),
            _phantom: std::marker::PhantomData,
        })
    }

    pub async fn publish_persisted(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        event: impl Into<P>,
    ) -> Result<(), sqlx::Error> {
        self.publish_all_persisted(op, std::iter::once(event)).await
    }

    pub async fn publish_all_persisted(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        events: impl IntoIterator<Item = impl Into<P>>,
    ) -> Result<(), sqlx::Error> {
        Tables::persist_events(op, events.into_iter().map(Into::into)).await?;
        Ok(())
    }

    async fn spawn_pg_listener(
        pool: sqlx::PgPool,
        sender: broadcast::Sender<OutboxEvent<P>>,
        highest_known_sequence: Arc<AtomicU64>,
    ) -> Result<OwnedTaskHandle, sqlx::Error> {
        let mut listener = sqlx::postgres::PgListener::connect_with(&pool).await?;
        listener
            .listen_all([
                Tables::persistent_outbox_events_channel(),
                Tables::ephemeral_outbox_events_channel(),
            ])
            .await?;
        let handle = tokio::spawn(async move {
            loop {
                if let Ok(notification) = listener.recv().await {
                    if notification.channel() == Tables::persistent_outbox_events_channel()
                        && let Ok(event) =
                            serde_json::from_str::<PersistentOutboxEvent<P>>(notification.payload())
                    {
                        let new_highest_sequence = u64::from(event.sequence);
                        highest_known_sequence.fetch_max(new_highest_sequence, Ordering::AcqRel);
                        let _ = sender.send(event.into());
                    } else if let Ok(event) =
                        serde_json::from_str::<EphemeralOutboxEvent<P>>(notification.payload())
                    {
                        let _ = sender.send(event.into());
                    }
                }
            }
        });

        Ok(OwnedTaskHandle::new(handle))
    }
}
