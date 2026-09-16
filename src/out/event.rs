use serde::{Deserialize, Serialize, de::DeserializeOwned};

use std::{borrow::Cow, sync::Arc};

use crate::out::lane::{CommitOrder, InsertOrder, Lane};
use crate::out::subscription::StreamPosition;
use crate::sequence::*;
use crate::tables::CommitLogRow;

es_entity::entity_id! { OutboxEventId }

pub trait OutboxEventMarker<E>:
    serde::de::DeserializeOwned + serde::Serialize + Send + Sync + 'static + Unpin + From<E>
{
    fn as_event(&self) -> Option<&E>;
}

/// A type that can itself be an outbox payload. Opting in is what makes
/// `T: OutboxEventMarker<T>` hold; it deliberately excludes wrappers such as
/// `Arc<PersistentOutboxEvent<_>>`, which would otherwise satisfy the
/// blanket impl below and shadow `PersistentOutboxEvent::as_event` /
/// `EphemeralOutboxEvent::as_event` at the call site.
///
/// `#[derive(OutboxEvent)]` implements this for you. A hand-rolled payload
/// type used directly as `P` (no wrapping enum) needs one line:
/// `impl OutboxPayload for MyPayload {}`.
pub trait OutboxPayload {}

impl<T> OutboxEventMarker<T> for T
where
    T: OutboxPayload
        + serde::de::DeserializeOwned
        + serde::Serialize
        + Send
        + Sync
        + 'static
        + Unpin
        + From<T>,
{
    fn as_event(&self) -> Option<&T> {
        Some(self)
    }
}

pub enum OutboxEvent<P>
where
    P: Serialize + DeserializeOwned + Send,
{
    Persistent(EventDelivery<P>),
    Ephemeral(Arc<EphemeralOutboxEvent<P>>),
}
impl<P> Clone for OutboxEvent<P>
where
    P: Serialize + DeserializeOwned + Send,
{
    fn clone(&self) -> Self {
        match self {
            Self::Persistent(event) => Self::Persistent(event.clone()),
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

    /// The insert lane's public stream item: the same two arms, each carrying
    /// the sequence it occupies.
    #[allow(clippy::result_large_err)]
    pub(crate) fn into_insert_item(
        self,
    ) -> Result<EventDelivery<P, InsertOrder>, UndecodableDelivery<InsertOrder>> {
        let sequence = self.sequence();
        Delivery::new(sequence, true, self.into_item()).transpose()
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

/// One item as delivered on lane `L`: the thing, plus where it sits.
///
/// The position rides *around* the value rather than on it because it belongs
/// to the delivery, not to the event — see the [lane module](crate::out::lane)
/// for why. Reading through a delivery is unchanged:
/// [`Deref`](std::ops::Deref) reaches the carried value, so an
/// [`EventDelivery`] behaves like the `Arc<PersistentOutboxEvent<P>>` it
/// carries (`event.payload`, `event.as_event::<E>()`, and passing `event`
/// where a `&PersistentOutboxEvent<P>` is expected all work).
///
/// Retaining the event past the invocation goes through
/// [`inner`](Self::inner): cloning the shared `Arc`'s refcount, never the
/// payload, so `P` need not be `Clone`.
pub struct Delivery<L, T>
where
    L: Lane,
{
    position: L::Position,
    /// Whether a flush here splits nothing. Meaningful on
    /// [`CommitOrder`](crate::out::CommitOrder), where it marks a source
    /// transaction's last event; on the insert lane every event is its own
    /// boundary, so this is always `true` and never exposed.
    boundary: bool,
    inner: T,
}

impl<L, T> Delivery<L, T>
where
    L: Lane,
{
    pub(crate) fn new(position: L::Position, boundary: bool, inner: T) -> Self {
        Self {
            position,
            boundary,
            inner,
        }
    }

    /// Where this delivery sits on its lane: an
    /// [`EventSequence`](crate::EventSequence) on the insert lane, a
    /// [`CommitSequence`](crate::CommitSequence) on the commit lane.
    pub fn position(&self) -> L::Position {
        self.position
    }

    /// The carried value — for an event, the shared [`Arc`] the outbox
    /// decoded once and broadcast to every subscriber.
    pub fn inner(&self) -> &T {
        &self.inner
    }

    /// Take the carried value, dropping the position.
    pub fn into_inner(self) -> T {
        self.inner
    }

    pub(crate) fn boundary(&self) -> bool {
        self.boundary
    }
}

impl<L, T, E> Delivery<L, Result<T, E>>
where
    L: Lane,
{
    /// Move the `Result` outside the delivery, so both arms keep the position
    /// they were delivered at — the shape every listener and the runner
    /// consume.
    pub(crate) fn transpose(self) -> Result<Delivery<L, T>, Delivery<L, E>> {
        let Self {
            position,
            boundary,
            inner,
        } = self;
        match inner {
            Ok(inner) => Ok(Delivery {
                position,
                boundary,
                inner,
            }),
            Err(inner) => Err(Delivery {
                position,
                boundary,
                inner,
            }),
        }
    }
}

impl<T> Delivery<CommitOrder, T> {
    /// Whether this is the last event of its source transaction — the point
    /// at which a batch may land without splitting a group.
    pub fn is_commit_boundary(&self) -> bool {
        self.boundary
    }
}

impl<L, T> std::ops::Deref for Delivery<L, T>
where
    L: Lane,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

// Manual: deriving would bound `L: Clone`, and a lane is a marker with no
// value to clone.
impl<L, T> Clone for Delivery<L, T>
where
    L: Lane,
    T: Clone,
{
    fn clone(&self) -> Self {
        Self {
            position: self.position,
            boundary: self.boundary,
            inner: self.inner.clone(),
        }
    }
}

impl<L, T> std::fmt::Debug for Delivery<L, T>
where
    L: Lane,
    T: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let position: StreamPosition = self.position.into();
        f.debug_struct("Delivery")
            .field("position", &position)
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

/// A persistent event as delivered on lane `L`.
pub type EventDelivery<P, L = InsertOrder> = Delivery<L, Arc<PersistentOutboxEvent<P>>>;

/// An undecodable persistent event as delivered on lane `L` — the `Err` arm
/// of every persistent stream, occupying the position the event would have
/// been delivered at.
pub type UndecodableDelivery<L = InsertOrder> = Delivery<L, UndecodableEventError>;

impl<L> std::fmt::Display for UndecodableDelivery<L>
where
    L: Lane,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let position: StreamPosition = self.position.into();
        write!(f, "{} (at {position})", self.inner)
    }
}

impl<L> std::error::Error for UndecodableDelivery<L>
where
    L: Lane,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.inner)
    }
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
    pub(crate) fn into_item(
        self,
    ) -> Result<EventDelivery<P, CommitOrder>, UndecodableDelivery<CommitOrder>> {
        Delivery::new(
            self.commit_sequence,
            self.commit_boundary,
            self.delivery.into_item(),
        )
        .transpose()
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
        Self::Persistent(Delivery::new(event.sequence, true, Arc::new(event)))
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
