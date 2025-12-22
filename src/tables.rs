use serde::{Serialize, de::DeserializeOwned};

use es_entity::hooks::HookOperation;

use crate::{
    inbox::{InboxError, InboxEvent, InboxEventId, InboxEventStatus, InboxIdempotencyKey},
    out::{EphemeralEventType, EphemeralOutboxEvent, PersistentOutboxEvent},
    sequence::*,
};

#[cfg_attr(feature = "default-tables", derive(obix_macros::MailboxTables))]
#[cfg_attr(feature = "default-tables", obix(crate = "crate"))]
pub struct DefaultMailboxTables;

pub trait MailboxTables: Send + Sync + 'static {
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

    fn persist_ephemeral_event<P>(
        pool: &sqlx::PgPool,
        event_type: EphemeralEventType,
        payload: P,
    ) -> impl Future<Output = Result<EphemeralOutboxEvent<P>, sqlx::Error>> + Send
    where
        P: Serialize + DeserializeOwned + Send;

    fn load_ephemeral_events<P>(
        pool: &sqlx::PgPool,
        event_type_filter: Option<EphemeralEventType>,
    ) -> impl Future<Output = Result<Vec<EphemeralOutboxEvent<P>>, sqlx::Error>> + Send
    where
        P: Serialize + DeserializeOwned + Send;

    fn persistent_outbox_events_channel() -> &'static str;
    fn ephemeral_outbox_events_channel() -> &'static str;

    // === Inbox methods ===

    fn insert_inbox_event<P>(
        op: &mut impl es_entity::AtomicOperation,
        idempotency_key: &InboxIdempotencyKey,
        payload: &P,
    ) -> impl Future<Output = Result<Option<InboxEventId>, sqlx::Error>> + Send
    where
        P: Serialize + Send + Sync;

    fn find_inbox_event_by_id<P>(
        pool: &sqlx::PgPool,
        id: InboxEventId,
    ) -> impl Future<Output = Result<InboxEvent<P>, InboxError>> + Send
    where
        P: Serialize + DeserializeOwned + Send;

    fn update_inbox_event_status(
        pool: &sqlx::PgPool,
        id: InboxEventId,
        status: InboxEventStatus,
        error: Option<&str>,
    ) -> impl Future<Output = Result<(), sqlx::Error>> + Send;

    fn update_inbox_event_status_in_op(
        op: &mut impl es_entity::AtomicOperation,
        id: InboxEventId,
        status: InboxEventStatus,
        error: Option<&str>,
    ) -> impl Future<Output = Result<(), sqlx::Error>> + Send;

    fn list_inbox_events_by_status<P>(
        pool: &sqlx::PgPool,
        status: InboxEventStatus,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<InboxEvent<P>>, InboxError>> + Send
    where
        P: Serialize + DeserializeOwned + Send;
}
