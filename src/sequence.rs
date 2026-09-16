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

/// Position in the commit-ordered lane: dense and gap-free, unlike
/// [`EventSequence`], so a consumer's cursor needs no contiguity machinery.
///
/// Assigned by the sequencer, not by the database, and unrelated to an
/// event's [`EventSequence`] — the two lanes number the same events
/// differently.
#[derive(
    sqlx::Type, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Copy, Clone, Serialize, Deserialize,
)]
#[serde(transparent)]
#[sqlx(transparent)]
pub struct CommitSequence(i64);
impl CommitSequence {
    pub const BEGIN: Self = CommitSequence(0);
    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}

impl Default for CommitSequence {
    fn default() -> Self {
        Self::BEGIN
    }
}

impl From<u64> for CommitSequence {
    fn from(n: u64) -> Self {
        Self(n as i64)
    }
}

impl From<CommitSequence> for u64 {
    fn from(CommitSequence(n): CommitSequence) -> Self {
        n as u64
    }
}

impl From<CommitSequence> for i64 {
    fn from(CommitSequence(n): CommitSequence) -> Self {
        n
    }
}

impl std::fmt::Display for CommitSequence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Identity of the source transaction that published an event: its
/// PostgreSQL top-level transaction id, stamped by a column DEFAULT.
///
/// Events sharing one of these committed together. Deliberately not ordered:
/// commit order comes from the sequence of a group's first member, never
/// from comparing these ids.
#[derive(sqlx::Type, PartialEq, Eq, Hash, Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(transparent)]
#[sqlx(transparent)]
pub struct CommitGroupId(i64);

impl From<i64> for CommitGroupId {
    fn from(n: i64) -> Self {
        Self(n)
    }
}

impl From<CommitGroupId> for i64 {
    fn from(CommitGroupId(n): CommitGroupId) -> Self {
        n
    }
}

impl std::fmt::Display for CommitGroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
