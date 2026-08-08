use serde::{Serialize, de::DeserializeOwned};

use es_entity::hooks::HookOperation;

use crate::{
    inbox::{InboxError, InboxEvent, InboxEventId, InboxEventStatus, InboxIdempotencyKey},
    out::{
        DecodeFailure, EphemeralEventType, EphemeralOutboxEvent, OutboxEventId,
        PersistentOutboxEvent, UndecodableEventError,
    },
    sequence::*,
};

#[derive(Clone)]
#[cfg_attr(feature = "default-tables", derive(obix_macros::MailboxTables))]
#[cfg_attr(feature = "default-tables", obix(crate = "crate"))]
pub struct DefaultMailboxTables;

/// Decode one stored persistent row into a delivery item, invoked from
/// `MailboxTables` derive output. A `NULL` payload is a plain placeholder
/// (`Ok` with `payload: None`); a payload that does not decode into the
/// caller's event type (e.g. a variant the consumer does not know yet, or a
/// row written by a different event enum sharing the table) must neither
/// panic — a single poison row previously wedged the whole pipeline in a hot
/// panic/retry loop — nor be silently dropped: it becomes the `Err` arm
/// ([`UndecodableEventError`]), still occupying its sequence position, and
/// its fate is decided by consumer policy (see
/// [`OutboxEventHandler::handle_undecodable`](crate::OutboxEventHandler::handle_undecodable)).
#[doc(hidden)]
pub fn decode_persistent_event<P>(
    id: OutboxEventId,
    sequence: u64,
    recorded_at: chrono::DateTime<chrono::Utc>,
    tracing_context: Option<es_entity::context::TracingContext>,
    payload: Option<serde_json::Value>,
) -> Result<PersistentOutboxEvent<P>, UndecodableEventError>
where
    P: Serialize + DeserializeOwned + Send,
{
    let sequence = EventSequence::from(sequence);
    let payload = match payload {
        None => None,
        Some(raw) => match P::deserialize(&raw) {
            Ok(payload) => Some(payload),
            Err(error) => {
                record_persistent_payload_undecodable(&error, u64::from(sequence));
                return Err(UndecodableEventError {
                    id,
                    sequence,
                    recorded_at,
                    failure: DecodeFailure {
                        error: error.to_string(),
                        raw,
                    },
                });
            }
        },
    };
    Ok(PersistentOutboxEvent {
        id,
        sequence,
        payload,
        tracing_context,
        recorded_at,
    })
}

#[tracing::instrument(
    name = "obix.tables.persistent_payload_undecodable",
    level = "error",
    skip_all,
    fields(otel.status_code = "ERROR", error = %error, sequence = sequence)
)]
fn record_persistent_payload_undecodable(error: &serde_json::Error, sequence: u64) {}

/// Invoked from `MailboxTables` derive output when a stored ephemeral event
/// payload cannot be deserialized; the event is dropped from the result.
/// Unlike the persistent stream (ordered, guaranteed delivery — see
/// [`decode_persistent_payload`]) the ephemeral stream is best-effort
/// last-value by design, so dropping is the honest degradation.
#[doc(hidden)]
#[tracing::instrument(
    name = "obix.tables.ephemeral_payload_undecodable",
    level = "error",
    skip_all,
    fields(otel.status_code = "ERROR", error = %error, event_type = %event_type)
)]
pub fn record_ephemeral_payload_undecodable(error: &serde_json::Error, event_type: &str) {}

/// Invoked from `MailboxTables` derive output when a stored ephemeral row's
/// `event_type` cannot be deserialized (schema drift or foreign rows); the
/// event is dropped from the result, like an undecodable payload.
#[doc(hidden)]
#[tracing::instrument(
    name = "obix.tables.ephemeral_event_type_undecodable",
    level = "error",
    skip_all,
    fields(otel.status_code = "ERROR", error = %error, event_type = %event_type)
)]
pub fn record_ephemeral_event_type_undecodable(error: &serde_json::Error, event_type: &str) {}

/// Invoked from `MailboxTables` derive output when the tracing-context
/// envelope cannot be deserialized; the event is kept with no context
/// attached (the context is delivery metadata, not payload data).
#[doc(hidden)]
#[tracing::instrument(
    name = "obix.tables.tracing_context_undecodable",
    level = "error",
    skip_all,
    fields(otel.status_code = "ERROR", error = %error)
)]
pub fn record_tracing_context_undecodable(error: &serde_json::Error) {}

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

    /// [`persist_events`](Self::persist_events) variant whose insert
    /// statement carries the legacy in-transaction `pg_notify` hint. Used
    /// only for publishes onto operations without commit-hook support (a
    /// bare `sqlx::Transaction`): `post_commit` never runs there, so the
    /// debounced notifier cannot observe the commit — the transaction itself
    /// must carry the wake-up. Everything else goes through
    /// [`persist_events`](Self::persist_events), keeping the cluster-wide
    /// NOTIFY commit-serialization lock off the application commit path.
    fn persist_events_notifying<'a, P>(
        op: &mut HookOperation<'a>,
        events: impl Iterator<Item = P>,
    ) -> impl Future<Output = Result<Vec<PersistentOutboxEvent<P>>, sqlx::Error>> + Send
    where
        P: Serialize + DeserializeOwned + Send;

    /// Each item is one sequence position: `Ok` for a decoded event or a
    /// plain placeholder (`payload: None`), `Err` for a stored payload that
    /// does not decode into `P` — delivered, not dropped, so the page stays
    /// contiguous.
    fn load_next_page<P>(
        pool: &sqlx::PgPool,
        from_sequence: EventSequence,
        buffer_size: usize,
    ) -> impl Future<
        Output = Result<Vec<Result<PersistentOutboxEvent<P>, UndecodableEventError>>, sqlx::Error>,
    > + Send
    where
        P: Serialize + DeserializeOwned + Send;

    /// Load the committed events in `(after_sequence, up_to_sequence]` with
    /// a plain SELECT. Unlike [`load_next_page`](Self::load_next_page) this
    /// never writes placeholder rows for sequence gaps — sequences absent
    /// from the result belong to in-flight transactions and are left to the
    /// grace-period gap fill. Undecodable payloads are the `Err` items, as
    /// in [`load_next_page`](Self::load_next_page).
    fn load_events_in_range<P>(
        pool: &sqlx::PgPool,
        after_sequence: EventSequence,
        up_to_sequence: EventSequence,
    ) -> impl Future<
        Output = Result<Vec<Result<PersistentOutboxEvent<P>, UndecodableEventError>>, sqlx::Error>,
    > + Send
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

    /// Base name of the persistent outbox events table (honouring any table
    /// prefix). The partition maintainer derives child partition names
    /// (`{table}_p{k}`) and the sequence-object name (`{table}_sequence_seq`)
    /// from it.
    fn persistent_outbox_events_table() -> &'static str;

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
