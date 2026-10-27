use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::value::RawValue;

use std::borrow::Cow;

/// The key of an ephemeral event on the wire: one row per key, last value
/// wins.
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

/// A type that is published on the ephemeral stream. The key is the type's
/// identity on the wire: publish writes under it, `listen::<T>()` reads it.
/// One key per type, written once, here. Convention: `<crate>.<event>`.
///
/// ```
/// use obix::{EphemeralEvent, EphemeralEventType};
///
/// #[derive(serde::Serialize, serde::Deserialize)]
/// struct PriceUpdated {
///     cents: u64,
/// }
///
/// impl EphemeralEvent for PriceUpdated {
///     const EVENT_TYPE: EphemeralEventType = EphemeralEventType::new("price.updated");
/// }
/// ```
pub trait EphemeralEvent: Serialize + DeserializeOwned + Send + Sync + Unpin + 'static {
    const EVENT_TYPE: EphemeralEventType;
}

/// What a listener wants to see: one or more published types, decoded by key.
/// Every [`EphemeralEvent`] is the one-member selection of itself (blanket
/// impl). A union is a local enum whose variants each wrap a published type —
/// `#[derive(obix::EphemeralSelection)]`.
///
/// ```
/// use obix::{EphemeralEvent, EphemeralEventType, EphemeralSelection};
///
/// #[derive(serde::Serialize, serde::Deserialize)]
/// struct Uploaded(u64);
/// impl EphemeralEvent for Uploaded {
///     const EVENT_TYPE: EphemeralEventType = EphemeralEventType::new("doc.uploaded");
/// }
///
/// #[derive(serde::Serialize, serde::Deserialize)]
/// struct Failed(u64);
/// impl EphemeralEvent for Failed {
///     const EVENT_TYPE: EphemeralEventType = EphemeralEventType::new("doc.failed");
/// }
///
/// #[derive(EphemeralSelection)]
/// enum Outcome {
///     Uploaded(Uploaded),
///     Failed(Failed),
/// }
/// ```
///
/// The derive accepts only enums whose variants are tuple variants wrapping
/// exactly one published [`EphemeralEvent`] — a selection is closed and its
/// members are leaves. Anything else is rejected at the offending variant:
///
/// ```compile_fail
/// use obix::{EphemeralEvent, EphemeralEventType, EphemeralSelection};
///
/// #[derive(serde::Serialize, serde::Deserialize)]
/// struct Uploaded(u64);
/// impl EphemeralEvent for Uploaded {
///     const EVENT_TYPE: EphemeralEventType = EphemeralEventType::new("doc.uploaded");
/// }
///
/// #[derive(EphemeralSelection)]
/// enum Outcome {
///     Uploaded(Uploaded),
///     Unknown,
/// }
/// ```
pub trait EphemeralSelection: Send + Sync + Unpin + 'static + Sized {
    const EVENT_TYPES: &'static [EphemeralEventType];

    /// `None`: not one of `EVENT_TYPES`. `Some(Err)`: the key matched but the
    /// stored payload does not decode as this type.
    fn decode(
        event_type: &EphemeralEventType,
        payload: &RawValue,
    ) -> Option<Result<Self, serde_json::Error>>;
}

impl<T: EphemeralEvent> EphemeralSelection for T {
    const EVENT_TYPES: &'static [EphemeralEventType] = &[T::EVENT_TYPE];

    fn decode(
        event_type: &EphemeralEventType,
        payload: &RawValue,
    ) -> Option<Result<Self, serde_json::Error>> {
        (*event_type == T::EVENT_TYPE).then(|| serde_json::from_str(payload.get()))
    }
}

/// The stored row, undecoded. What the cache holds and what
/// [`listen_raw`](super::EphemeralOutbox::listen_raw) yields.
pub type RawPayload = Box<RawValue>;

#[derive(Debug, Serialize, Deserialize)]
#[serde(bound(deserialize = "P: DeserializeOwned"))]
pub struct EphemeralOutboxEvent<P> {
    pub event_type: EphemeralEventType,
    pub payload: P,
    pub tracing_context: Option<es_entity::context::TracingContext>,
    pub recorded_at: chrono::DateTime<chrono::Utc>,
}

impl<P> EphemeralOutboxEvent<P> {
    #[cfg(feature = "tracing")]
    pub fn inject_trace_parent(&self) {
        if let Some(context) = &self.tracing_context {
            context.inject_as_parent();
        }
    }
}
