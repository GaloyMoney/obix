use serde::{Deserialize, Serialize};

#[derive(
    sqlx::Type, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Copy, Clone, Serialize, Deserialize,
)]
#[serde(transparent)]
#[sqlx(transparent)]
pub struct EventSequence(i64);
impl EventSequence {
    pub const BEGIN: Self = EventSequence(0);
    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}

impl Default for EventSequence {
    fn default() -> Self {
        Self::BEGIN
    }
}

impl From<u64> for EventSequence {
    fn from(n: u64) -> Self {
        Self(n as i64)
    }
}

impl From<EventSequence> for u64 {
    fn from(EventSequence(n): EventSequence) -> Self {
        n as u64
    }
}

impl From<EventSequence> for std::sync::atomic::AtomicU64 {
    fn from(EventSequence(n): EventSequence) -> Self {
        std::sync::atomic::AtomicU64::new(n as u64)
    }
}
impl std::fmt::Display for EventSequence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
