use serde::{Deserialize, Serialize, de::DeserializeOwned};

use std::{borrow::Cow, sync::Arc};

use crate::sequence::*;
use crate::tables::CommitLogRow;

es_entity::entity_id! { OutboxEventId }

pub trait OutboxEventMarker<E>:
    serde::de::DeserializeOwned + serde::Serialize + Send + Sync + 'static + Unpin + From<E>
{
    fn as_event(&self) -> Option<&E>;
}
impl<T> OutboxEventMarker<T> for T
where
    T: serde::de::DeserializeOwned + serde::Serialize + Send + Sync + 'static + Unpin + From<T>,
{
    fn as_event(&self) -> Option<&T> {
        Some(self)
    }
}

pub enum OutboxEvent<P>
where
    P: Serialize + DeserializeOwned + Send,
{
    Persistent(Arc<PersistentOutboxEvent<P>>),
    Ephemeral(Arc<EphemeralOutboxEvent<P>>),
}
impl<P> Clone for OutboxEvent<P>
where
    P: Serialize + DeserializeOwned + Send,
{
    fn clone(&self) -> Self {
        match self {
            Self::Persistent(event) => Self::Persistent(Arc::clone(event)),
            Self::Ephemeral(event) => Self::Ephemeral(Arc::clone(event)),
        }
    }
}

impl<P> OutboxEvent<P>
where
    P: Serialize + DeserializeOwned + Send,
{
    pub fn as_event<E>(&self) -> Option<&E>
    where
        P: OutboxEventMarker<E>,
    {
        match self {
            Self::Persistent(e) => (**e).as_event::<E>(),
            Self::Ephemeral(e) => (**e).as_event::<E>(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct EphemeralEventType(Cow<'static, str>);
impl EphemeralEventType {
    pub const fn new(name: &'static str) -> Self {
        Self(Cow::Borrowed(name))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for EphemeralEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(bound(deserialize = "T: DeserializeOwned"))]
pub struct EphemeralOutboxEvent<T>
where
    T: Serialize + DeserializeOwned + Send,
{
    pub event_type: EphemeralEventType,
    pub payload: T,
    pub tracing_context: Option<es_entity::context::TracingContext>,
    pub recorded_at: chrono::DateTime<chrono::Utc>,
}

impl<T> EphemeralOutboxEvent<T>
where
    T: Serialize + DeserializeOwned + Send,
{
    pub fn as_event<E>(&self) -> Option<&E>
    where
        T: OutboxEventMarker<E>,
    {
        self.payload.as_event()
    }

    #[cfg(feature = "tracing")]
    pub fn inject_trace_parent(&self) {
        if let Some(context) = &self.tracing_context {
            context.inject_as_parent();
        }
    }
}

impl<P> From<EphemeralOutboxEvent<P>> for OutboxEvent<P>
where
    P: Serialize + DeserializeOwned + Send,
{
    fn from(event: EphemeralOutboxEvent<P>) -> Self {
        Self::Ephemeral(Arc::new(event))
    }
}

/// A stored payload that could not be decoded into the consumer's event type
/// (e.g. a variant the consumer's enum does not know yet, or a row written by
/// a different event enum sharing the table).
///
/// Carried inside [`UndecodableEventError`], which is how such an event
/// travels everywhere: the `Err` arm of the
/// [`MailboxTables`](crate::MailboxTables) load results, of the internal
/// delivery plumbing, and of the listener streams.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecodeFailure {
    /// The payload column exactly as stored.
    pub raw: serde_json::Value,
    /// The serde error message.
    pub error: String,
}

/// A persistent event whose stored payload could not be decoded into the
/// consumer's event type. Everything undecodable-shaped is this one type,
/// always in an `Err` arm: the
/// [`MailboxTables`](crate::MailboxTables) load results, the
/// [`PersistentOutboxListener`](crate::out::PersistentOutboxListener) /
/// [`AllOutboxListener`](crate::out::AllOutboxListener) stream items, and
/// the error the default
/// [`SingletonSubscriber::handle_undecodable`](crate::SingletonSubscriber::handle_undecodable)
/// fails the handler job with.
///
/// The `Err` item IS the delivery of that event — in order, in its sequence
/// position — so no consumer can pass over an undecodable payload without an
/// explicit decision: `?` (e.g. via `TryStreamExt::try_next`) fails loudly,
/// and matching the `Err` arm is the visible opt-out.
///
/// [`Display`](std::fmt::Display) prints the serde error only — the raw
/// payload is available on [`failure`](Self::failure) but never hits log
/// lines by accident.
#[derive(Debug, Clone, thiserror::Error)]
#[error("undecodable persistent outbox event {id} at sequence {sequence}: {}", failure.error)]
pub struct UndecodableEventError {
    pub id: OutboxEventId,
    pub sequence: EventSequence,
    pub recorded_at: chrono::DateTime<chrono::Utc>,
    /// The raw payload and the serde error.
    pub failure: DecodeFailure,
    /// The source transaction this event committed in.
    pub commit_group: CommitGroupId,
}

/// Internal transport for the persistent delivery plumbing (cache,
/// broadcast, backfill): an undecodable event rides it as the `Err` arm so
/// it still occupies its sequence position — the contiguity machinery treats
/// both arms alike. `Arc` on both arms keeps broadcast fan-out cheap; the
/// listeners unwrap to the public owned [`UndecodableEventError`] via
/// [`into_item`](Self::into_item) at their yield boundary.
pub(crate) struct PersistentDelivery<P>(
    Result<Arc<PersistentOutboxEvent<P>>, Arc<UndecodableEventError>>,
)
where
    P: Serialize + DeserializeOwned + Send;

impl<P> PersistentDelivery<P>
where
    P: Serialize + DeserializeOwned + Send,
{
    /// The sequence this delivery occupies, whichever arm it is.
    pub(crate) fn sequence(&self) -> EventSequence {
        match &self.0 {
            Ok(event) => event.sequence,
            Err(error) => error.sequence,
        }
    }

    /// The source transaction this delivery belongs to, whichever arm it is.
    pub(crate) fn commit_group(&self) -> CommitGroupId {
        match &self.0 {
            Ok(event) => event.commit_group,
            Err(error) => error.commit_group,
        }
    }

    /// Whether this delivery carries a payload — a placeholder does not, and
    /// never reaches the commit lane.
    pub(crate) fn has_payload(&self) -> bool {
        match &self.0 {
            Ok(event) => event.payload.is_some(),
            Err(_) => true,
        }
    }

    /// Unwrap into the public listener stream item, cloning the `Err` arm
    /// out of its transport `Arc`.
    ///
    /// The `Err` arm is the delivery of an undecodable event rather than a
    /// failure path, so boxing it to shrink the `Result` would change the
    /// public stream item type for every consumer.
    #[allow(clippy::result_large_err)]
    pub(crate) fn into_item(self) -> Result<Arc<PersistentOutboxEvent<P>>, UndecodableEventError> {
        match self.0 {
            Ok(event) => Ok(event),
            Err(error) => Err((*error).clone()),
        }
    }
}

/// Wrap a freshly loaded item into the internal delivery transport.
impl<P> From<Result<PersistentOutboxEvent<P>, UndecodableEventError>> for PersistentDelivery<P>
where
    P: Serialize + DeserializeOwned + Send,
{
    fn from(item: Result<PersistentOutboxEvent<P>, UndecodableEventError>) -> Self {
        Self(match item {
            Ok(event) => Ok(Arc::new(event)),
            Err(error) => Err(Arc::new(error)),
        })
    }
}

impl<P> Clone for PersistentDelivery<P>
where
    P: Serialize + DeserializeOwned + Send,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PersistentOutboxEvent<T>
where
    T: Serialize + DeserializeOwned + Send,
{
    pub id: OutboxEventId,
    pub sequence: EventSequence,
    /// `None` is a sequence placeholder: nothing to process at this
    /// sequence (a gap from a rolled-back transaction, or a payload
    /// explicitly NULLed by an operator). An *undecodable* payload never
    /// appears here — it travels as [`UndecodableEventError`], the `Err`
    /// arm of the load results and listener streams.
    #[serde(bound = "T: DeserializeOwned")]
    pub payload: Option<T>,
    pub tracing_context: Option<es_entity::context::TracingContext>,
    pub recorded_at: chrono::DateTime<chrono::Utc>,
    /// The source transaction this event committed in. Events sharing one
    /// value committed together; reported on both lanes.
    pub commit_group: CommitGroupId,
}

impl<T> Clone for PersistentOutboxEvent<T>
where
    T: Clone + Serialize + DeserializeOwned + Send,
{
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            sequence: self.sequence,
            payload: self.payload.clone(),
            tracing_context: self.tracing_context.clone(),
            recorded_at: self.recorded_at,
            commit_group: self.commit_group,
        }
    }
}

/// What the commit-ordered lane yields: one [`PersistentOutboxEvent`] plus
/// where it sits in commit order.
///
/// The position is a property of the lane, not of the event — the same event
/// on the insert lane has no commit sequence — so it is carried around the
/// event rather than on it.
pub struct CommitOrderedEnvelope<P>
where
    P: Serialize + DeserializeOwned + Send,
{
    pub commit_sequence: CommitSequence,
    /// The last event of its group — the point at which a batch may be
    /// flushed without splitting a source transaction.
    pub commit_boundary: bool,
    pub event: Result<Arc<PersistentOutboxEvent<P>>, UndecodableEventError>,
}

/// Internal transport for the commit lane's fan-out and backfill, mirroring
/// [`PersistentDelivery`] with the lane position attached.
pub(crate) struct CommitDelivery<P>
where
    P: Serialize + DeserializeOwned + Send,
{
    pub(crate) commit_sequence: CommitSequence,
    pub(crate) commit_boundary: bool,
    pub(crate) delivery: PersistentDelivery<P>,
}

impl<P> CommitDelivery<P>
where
    P: Serialize + DeserializeOwned + Send,
{
    pub(crate) fn into_item(self) -> CommitOrderedEnvelope<P> {
        CommitOrderedEnvelope {
            commit_sequence: self.commit_sequence,
            commit_boundary: self.commit_boundary,
            event: self.delivery.into_item(),
        }
    }
}

impl<P> Clone for CommitDelivery<P>
where
    P: Serialize + DeserializeOwned + Send,
{
    fn clone(&self) -> Self {
        Self {
            commit_sequence: self.commit_sequence,
            commit_boundary: self.commit_boundary,
            delivery: self.delivery.clone(),
        }
    }
}

impl<P> From<CommitLogRow<P>> for CommitDelivery<P>
where
    P: Serialize + DeserializeOwned + Send,
{
    fn from(row: CommitLogRow<P>) -> Self {
        Self {
            commit_sequence: row.commit_sequence,
            commit_boundary: row.commit_boundary,
            delivery: PersistentDelivery::from(row.event),
        }
    }
}

impl<P> From<PersistentOutboxEvent<P>> for OutboxEvent<P>
where
    P: Serialize + DeserializeOwned + Send,
{
    fn from(event: PersistentOutboxEvent<P>) -> Self {
        Self::Persistent(Arc::new(event))
    }
}

impl<T> PersistentOutboxEvent<T>
where
    T: Serialize + DeserializeOwned + Send,
{
    pub fn as_event<E>(&self) -> Option<&E>
    where
        T: OutboxEventMarker<E>,
    {
        if let Some(payload) = &self.payload {
            payload.as_event()
        } else {
            None
        }
    }

    #[cfg(feature = "tracing")]
    pub fn inject_trace_parent(&self) {
        if let Some(context) = &self.tracing_context {
            context.inject_as_parent();
        }
    }
}
