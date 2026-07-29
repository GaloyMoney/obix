use serde::{Serialize, de::DeserializeOwned};

use es_entity::hooks::HookOperation;

use crate::{
    inbox::{InboxError, InboxEvent, InboxEventId, InboxEventStatus, InboxIdempotencyKey},
    out::{EphemeralEventType, EphemeralOutboxEvent, PersistentOutboxEvent},
    sequence::*,
};

#[derive(Clone)]
#[cfg_attr(feature = "default-tables", derive(obix_macros::MailboxTables))]
#[cfg_attr(feature = "default-tables", obix(crate = "crate"))]
pub struct DefaultMailboxTables;

/// Invoked from `MailboxTables` derive output when a stored persistent event
/// payload cannot be deserialized into the caller's event type (e.g. rows
/// written into a shared database by a different event enum). The payload is
/// treated as a placeholder (`None`) so the sequence still advances — a
/// single unknown event must neither crash nor stall the whole event
/// pipeline.
#[doc(hidden)]
#[tracing::instrument(
    name = "obix.tables.persistent_payload_deserialize_failed",
    level = "error",
    skip_all,
    fields(otel.status_code = "ERROR", error = %error, sequence = sequence)
)]
pub fn record_persistent_payload_deserialize_failed(error: &serde_json::Error, sequence: u64) {}

/// Same as [`record_persistent_payload_deserialize_failed`] but for the
/// tracing context envelope; the event is kept with no context attached.
#[doc(hidden)]
#[tracing::instrument(
    name = "obix.tables.tracing_context_deserialize_failed",
    level = "error",
    skip_all,
    fields(otel.status_code = "ERROR", error = %error)
)]
pub fn record_tracing_context_deserialize_failed(error: &serde_json::Error) {}

/// Invoked from `MailboxTables` derive output when a stored ephemeral event
/// payload cannot be deserialized; the event is dropped from the result.
#[doc(hidden)]
#[tracing::instrument(
    name = "obix.tables.ephemeral_payload_deserialize_failed",
    level = "error",
    skip_all,
    fields(otel.status_code = "ERROR", error = %error, event_type = %event_type)
)]
pub fn record_ephemeral_payload_deserialize_failed(error: &serde_json::Error, event_type: &str) {}

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

    /// Load the committed events in `(after_sequence, up_to_sequence]` with
    /// a plain SELECT. Unlike [`load_next_page`](Self::load_next_page) this
    /// never writes placeholder rows for sequence gaps — sequences absent
    /// from the result belong to in-flight transactions and are left to the
    /// grace-period gap fill.
    fn load_events_in_range<P>(
        pool: &sqlx::PgPool,
        after_sequence: EventSequence,
        up_to_sequence: EventSequence,
    ) -> impl Future<Output = Result<Vec<PersistentOutboxEvent<P>>, sqlx::Error>> + Send
    where
        P: Serialize + DeserializeOwned + Send;

    fn persist_ephemeral_event<P>(
        pool: &sqlx::PgPool,
        now: Option<chrono::DateTime<chrono::Utc>>,
        event_type: EphemeralEventType,
        payload: P,
    ) -> impl Future<Output = Result<EphemeralOutboxEvent<P>, sqlx::Error>> + Send
    where
        P: Serialize + DeserializeOwned + Send;

    fn persist_ephemeral_event_in_op<'a, P>(
        op: &mut HookOperation<'a>,
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

    fn find_inbox_event_by_id(
        pool: &sqlx::PgPool,
        id: InboxEventId,
    ) -> impl Future<Output = Result<InboxEvent, InboxError>> + Send;

    fn update_inbox_event_status(
        pool: &sqlx::PgPool,
        now: Option<chrono::DateTime<chrono::Utc>>,
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

    fn list_inbox_events_by_status(
        pool: &sqlx::PgPool,
        status: InboxEventStatus,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<InboxEvent>, InboxError>> + Send;
}
