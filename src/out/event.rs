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

/// Wire-level classification of an outbox event enum's variants into
/// *ephemeral* (at-most-once runtime broadcasts) and *persistent*
/// (checkpointed, replayable) events.
///
/// Derive it and mark the ephemeral variants with `#[obix(ephemeral)]`:
///
/// ```
/// use obix::OutboxEventKind;
/// use obix::out::OutboxEventKind as Kind;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Serialize, Deserialize, OutboxEventKind)]
/// #[serde(tag = "type")]
/// enum PriceEvent {
///     /// Ephemeral: live spot tick, never persisted.
///     #[obix(ephemeral)]
///     PriceUpdated { usd: f64 },
///     /// Persistent: the official EOD closing rate.
///     ClosingRateCaptured { usd: f64 },
/// }
///
/// assert_eq!(PriceEvent::EPHEMERAL_VARIANTS, &["PriceUpdated"]);
/// assert!(Kind::is_ephemeral(&PriceEvent::PriceUpdated { usd: 1.0 }));
/// assert!(!Kind::is_ephemeral(&PriceEvent::ClosingRateCaptured { usd: 1.0 }));
/// ```
///
/// Aggregates — enums whose variants wrap other event enums — delegate: an
/// unmarked single-field variant forwards [`is_ephemeral`](Self::is_ephemeral)
/// to the inner event, which therefore must implement `OutboxEventKind` too.
/// A variant marked `#[obix(ephemeral)]` is ephemeral as a whole, regardless
/// of its inner value (so its inner type needs no impl of its own):
///
/// ```
/// # use obix::OutboxEventKind;
/// # use obix::out::OutboxEventKind as Kind;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Debug, Serialize, Deserialize, OutboxEventKind)]
/// # #[serde(tag = "type")]
/// # enum PriceEvent {
/// #     #[obix(ephemeral)]
/// #     PriceUpdated { usd: f64 },
/// #     ClosingRateCaptured { usd: f64 },
/// # }
/// #[derive(Debug, Serialize, Deserialize, OutboxEventKind)]
/// #[serde(tag = "module")]
/// enum BankEvent {
///     Price(PriceEvent),
///     /// Ephemeral module: cache-invalidation pokes, never persisted.
///     #[obix(ephemeral)]
///     DomainConfig(DomainConfigEvent),
/// }
///
/// # #[derive(Debug, Serialize, Deserialize)]
/// # struct DomainConfigEvent;
/// assert!(Kind::is_ephemeral(&BankEvent::Price(PriceEvent::PriceUpdated { usd: 1.0 })));
/// assert!(Kind::is_ephemeral(&BankEvent::DomainConfig(DomainConfigEvent)));
/// assert_eq!(BankEvent::EPHEMERAL_VARIANTS, &["DomainConfig"]);
/// assert_eq!(
///     <BankEvent as Kind>::ephemeral_event_types(),
///     vec![("Price", "PriceUpdated"), ("DomainConfig", "*")],
/// );
/// ```
///
/// ## Tags
///
/// The tags in [`EPHEMERAL_VARIANTS`](Self::EPHEMERAL_VARIANTS) and
/// [`ephemeral_event_types`](Self::ephemeral_event_types) are the values
/// serde writes for the enum's internal tag — the variant name, or its
/// `#[serde(rename)]` / `rename_all` spelling — so they match the `oneOf`
/// discriminants of a generated JSON Schema directly.
///
/// ## Why
///
/// Ephemeral events are not replayable: consumers that persist them
/// silently lose rows whenever they were not listening. Classifying the
/// variants on the enum itself keeps exactly one machine-readable source of
/// truth for which events may be replicated downstream.
pub trait OutboxEventKind {
    /// The serde internal-tag values of this enum's variants that are marked
    /// `#[obix(ephemeral)]` — empty when every variant is persistent.
    const EPHEMERAL_VARIANTS: &'static [&'static str];

    /// Whether this value is an ephemeral event: a variant marked
    /// `#[obix(ephemeral)]` on this enum, or — for an unmarked single-field
    /// variant wrapping another event enum — an inner value that is itself
    /// ephemeral.
    fn is_ephemeral(&self) -> bool;

    /// The full ephemeral classification reachable from this enum, as
    /// `(tag, inner_tag)` pairs: `(tag, "*")` for variants marked ephemeral
    /// on this enum, and `(tag, inner_tag)` for each ephemeral variant of a
    /// single-field inner enum. Only one level of nesting is unfolded.
    ///
    /// Intended for consumers that classify by wire tag — e.g. filtering a
    /// generated JSON Schema down to its persistent variants.
    fn ephemeral_event_types() -> Vec<(&'static str, &'static str)> {
        Self::EPHEMERAL_VARIANTS
            .iter()
            .map(|tag| (*tag, "*"))
            .collect()
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

    /// Unwrap into the public listener stream item, cloning the `Err` arm
    /// out of its transport `Arc`.
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
