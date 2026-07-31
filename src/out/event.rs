use serde::{Deserialize, Serialize, de::DeserializeOwned};

use std::{borrow::Cow, sync::Arc};

use crate::sequence::*;

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
/// [`OutboxEventHandler::handle_undecodable`](crate::OutboxEventHandler::handle_undecodable)
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
}

/// Internal transport for the persistent delivery plumbing (cache,
/// broadcast, backfill): an undecodable event rides it as the `Err` arm so
/// it still occupies its sequence position — the contiguity machinery treats
/// both arms alike. `Arc` on both arms keeps broadcast fan-out cheap; the
/// listeners unwrap to the public owned [`UndecodableEventError`] at their
/// yield boundary.
pub(crate) type PersistentDelivery<P> =
    Result<Arc<PersistentOutboxEvent<P>>, Arc<UndecodableEventError>>;

/// The sequence a delivery occupies, whichever arm it is.
pub(crate) fn delivery_sequence<P>(delivery: &PersistentDelivery<P>) -> EventSequence
where
    P: Serialize + DeserializeOwned + Send,
{
    match delivery {
        Ok(event) => event.sequence,
        Err(error) => error.sequence,
    }
}

/// Wrap a freshly loaded item into the internal delivery transport.
pub(crate) fn into_delivery<P>(
    item: Result<PersistentOutboxEvent<P>, UndecodableEventError>,
) -> PersistentDelivery<P>
where
    P: Serialize + DeserializeOwned + Send,
{
    match item {
        Ok(event) => Ok(Arc::new(event)),
        Err(error) => Err(Arc::new(error)),
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
