use serde::{Serialize, de::DeserializeOwned};

use es_entity::hooks::HookOperation;

use crate::{
    out::{EphemeralEventType, EphemeralOutboxEvent, PersistentOutboxEvent},
    sequence::*,
};

#[cfg_attr(not(feature = "custom-tables"), derive(obix_macros::MailboxTables))]
#[cfg_attr(not(feature = "custom-tables"), obix(crate = "crate"))]
pub struct DefaultMailboxTables;

pub trait MailboxTables: Send + 'static {
    fn highest_known_persistent_sequence<'a>(
        op: impl es_entity::IntoOneTimeExecutor<'a>,
    ) -> impl Future<Output = Result<EventSequence, sqlx::Error>> + Send;

    fn persist_events<'a, P>(
        op: &mut HookOperation<'a>,
        events: impl Iterator<Item = P>,
    ) -> impl Future<Output = Result<Vec<PersistentOutboxEvent<P>>, sqlx::Error>> + Send
    where
        P: Serialize + DeserializeOwned + Send;

    fn load_next_page<P>(
        pool: &sqlx::PgPool,
        from_sequence: EventSequence,
        buffer_size: usize,
    ) -> impl Future<Output = Result<Vec<PersistentOutboxEvent<P>>, sqlx::Error>> + Send
    where
        P: Serialize + DeserializeOwned + Send;

    fn persist_ephemeral_event<'a, P>(
        op: impl es_entity::IntoOneTimeExecutor<'a>,
        event_type: EphemeralEventType,
        payload: P,
    ) -> impl Future<Output = Result<EphemeralOutboxEvent<P>, sqlx::Error>> + Send
    where
        P: Serialize + DeserializeOwned + Send;

    fn load_ephemeral_events<'a, P>(
        op: impl es_entity::IntoOneTimeExecutor<'a>,
    ) -> impl Future<Output = Result<Vec<EphemeralOutboxEvent<P>>, sqlx::Error>> + Send
    where
        P: Serialize + DeserializeOwned + Send;

    fn persistent_outbox_events_channel() -> &'static str;
    fn ephemeral_outbox_events_channel() -> &'static str;
}
