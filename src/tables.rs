use es_entity::hooks::HookOperation;
use serde::{Serialize, de::DeserializeOwned};

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

/// Decode a stored row without losing its position when its payload is unknown.
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

#[tracing::instrument(name = "obix.tables.persistent_payload_undecodable", level = "error", skip_all,
    fields(otel.status_code = "ERROR", error = %error, sequence = sequence))]
fn record_persistent_payload_undecodable(error: &serde_json::Error, sequence: u64) {}

/// Ephemeral delivery is best-effort; unreadable last-value rows are skipped.
#[doc(hidden)]
#[tracing::instrument(name = "obix.tables.ephemeral_payload_undecodable", level = "error", skip_all,
    fields(otel.status_code = "ERROR", error = %error, event_type = %event_type))]
pub fn record_ephemeral_payload_undecodable(error: &serde_json::Error, event_type: &str) {}

#[doc(hidden)]
#[tracing::instrument(name = "obix.tables.ephemeral_event_type_undecodable", level = "error", skip_all,
    fields(otel.status_code = "ERROR", error = %error, event_type = %event_type))]
pub fn record_ephemeral_event_type_undecodable(error: &serde_json::Error, event_type: &str) {}

/// Invalid tracing metadata does not discard the message itself.
#[doc(hidden)]
#[tracing::instrument(name = "obix.tables.tracing_context_undecodable", level = "error", skip_all,
    fields(otel.status_code = "ERROR", error = %error))]
pub fn record_tracing_context_undecodable(error: &serde_json::Error) {}

pub type PersistentEventRows<P> = Vec<Result<PersistentOutboxEvent<P>, UndecodableEventError>>;

pub trait MailboxTables: Send + Sync + 'static {
    /// Committed head, shared by event and publication consumers.
    fn highest_known_persistent_sequence<'a>(
        op: impl es_entity::IntoOneTimeExecutor<'a>,
    ) -> impl Future<Output = Result<EventSequence, sqlx::Error>> + Send;

    /// Reserve positions transactionally and persist one chunk. The head lock
    /// keeps this operation's later chunks contiguous until finalization.
    fn persist_events<'a, P>(
        op: &mut HookOperation<'a>,
        events: impl Iterator<Item = P>,
    ) -> impl Future<Output = Result<Vec<PersistentOutboxEvent<P>>, sqlx::Error>> + Send
    where
        P: Serialize + DeserializeOwned + Send;

    /// Read at most `buffer_size` committed event positions after the cursor.
    fn load_next_page<P>(
        pool: &sqlx::PgPool,
        from_sequence: EventSequence,
        buffer_size: usize,
    ) -> impl Future<Output = Result<PersistentEventRows<P>, sqlx::Error>> + Send
    where
        P: Serialize + DeserializeOwned + Send;

    /// Read `(after_sequence, up_to_sequence]`; publication consumers supply
    /// sealed boundaries to retrieve the complete unit.
    fn load_events_in_range<P>(
        pool: &sqlx::PgPool,
        after_sequence: EventSequence,
        up_to_sequence: EventSequence,
    ) -> impl Future<Output = Result<PersistentEventRows<P>, sqlx::Error>> + Send
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
    fn persistent_outbox_events_table() -> &'static str;

    /// Job type of this outbox's keyed waker: `{persistent table}.keyed-waker`.
    /// Composed at macro-expansion time so the waker's `job::JobType` — which
    /// only accepts `&'static str` — needs no runtime formatting and no leak.
    const KEYED_WAKER_JOB_TYPE: &'static str;

    // === Keyed-subscriber subscription methods ===

    /// Insert a new subscription row, idempotently: a conflict on
    /// `(subscriber_type, key)` — an already-live subscription — resolves to
    /// success without overwriting the existing row's `start_after` or
    /// `wake_keys`. Re-subscribing an already-subscribed key must never
    /// silently rewind or fast-forward its birth frontier.
    fn insert_subscription_in_op(
        op: &mut impl es_entity::AtomicOperation,
        subscriber_type: &str,
        key: &str,
        wake_keys: &[String],
        instance_config: serde_json::Value,
        start_after: EventSequence,
    ) -> impl Future<Output = Result<(), sqlx::Error>> + Send;

    /// Delete a subscription row. Row absence is the tombstone: no job-kill
    /// API exists or is needed — the runner's next run-start row check (or a
    /// stray wake) observes the missing row and completes.
    fn delete_subscription_in_op(
        op: &mut impl es_entity::AtomicOperation,
        subscriber_type: &str,
        key: &str,
    ) -> impl Future<Output = Result<(), sqlx::Error>> + Send;

    /// Point-read one subscription's identity and terms by primary key.
    /// `None` means cancelled (or never subscribed) — the caller's row-is-truth
    /// check.
    fn find_subscription(
        pool: &sqlx::PgPool,
        subscriber_type: &str,
        key: &str,
    ) -> impl Future<Output = Result<Option<SubscriptionRow>, sqlx::Error>> + Send;

    /// Advance one subscription's mirrored cursor, in the caller's op so it
    /// shares the fate of the job checkpoint it copies. Monotonic: a lower
    /// value than the stored one is ignored rather than applied, so a write
    /// from a superseded generation cannot rewind it.
    fn update_subscription_checkpoint_in_op(
        op: &mut impl es_entity::AtomicOperation,
        subscriber_type: &str,
        key: &str,
        checkpoint: EventSequence,
    ) -> impl Future<Output = Result<(), sqlx::Error>> + Send;

    /// `(subscriber_type, key)` of the subscriptions whose mirrored cursor is
    /// below `below`, furthest behind first, capped at `limit` — the waker's
    /// catch-up scan.
    ///
    /// Spans every *registered* subscriber type because the waker does: one
    /// scan per pass for the whole outbox, not one per type. Ordering is what
    /// makes `limit` safe to apply — it sheds the members with the most
    /// slack, so a cap can bound the wake rate without ever starving the
    /// member closest to falling out of the cache.
    ///
    /// `subscriber_types` must filter in SQL, not afterwards: a row whose
    /// type is not registered here has no runner to advance its checkpoint,
    /// so it is permanently the furthest behind and would otherwise win every
    /// scan and consume the entire limit.
    fn subscriptions_behind(
        op: &mut impl es_entity::AtomicOperation,
        subscriber_types: &[String],
        below: EventSequence,
        limit: i64,
    ) -> impl Future<Output = Result<Vec<(String, String)>, sqlx::Error>> + Send;

    /// The `(subscriber_type, key)` of every subscription whose declared
    /// `wake_keys` contain a key the batch classified an event to for that
    /// same type — the waker's flush-time lookup, run **on the flush op** so
    /// the wakes it drives and the waker's own checkpoint commit atomically.
    /// Liveness-only: an over-approximating false positive here is a harmless
    /// empty wake, never a correctness gap.
    ///
    /// `subscriber_types` and `wake_keys` are **parallel arrays**: element `i`
    /// of each names one (type, wake key) pair to match. One query covers
    /// every registered type without losing per-type precision — a type is
    /// never matched against another type's keys.
    fn subscriptions_for_wake_keys(
        op: &mut impl es_entity::AtomicOperation,
        subscriber_types: &[String],
        wake_keys: &[String],
    ) -> impl Future<Output = Result<Vec<(String, String)>, sqlx::Error>> + Send;

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

/// One subscription's identity and terms, as stored — everything but the
/// primary key `(subscriber_type, key)` itself, which the caller already
/// knows from its own lookup.
#[derive(Debug, Clone)]
pub struct SubscriptionRow {
    pub wake_keys: Vec<String>,
    pub instance_config: serde_json::Value,
    pub start_after: EventSequence,
    pub created_at: chrono::DateTime<chrono::Utc>,
}
