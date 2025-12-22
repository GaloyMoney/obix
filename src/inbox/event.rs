// src/inbox/event.rs
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

es_entity::entity_id! { InboxEventId }

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InboxIdempotencyKey(String);

impl InboxIdempotencyKey {
    pub fn new(key: impl Into<String>) -> Self {
        Self(key.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<T: std::fmt::Display> From<T> for InboxIdempotencyKey {
    fn from(value: T) -> Self {
        Self(value.to_string())
    }
}

impl AsRef<str> for InboxIdempotencyKey {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<InboxEventId> for job::JobId {
    fn from(id: InboxEventId) -> Self {
        job::JobId::from(id.0)
    }
}

impl From<job::JobId> for InboxEventId {
    fn from(id: job::JobId) -> Self {
        let uuid: sqlx::types::Uuid = id.into();
        InboxEventId::from(uuid)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "InboxEventStatus", rename_all = "snake_case")]
pub enum InboxEventStatus {
    Pending,
    Processing,
    Completed,
    Failed,
}

impl InboxEventStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Processing => "processing",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

impl FromStr for InboxEventStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(Self::Pending),
            "processing" => Ok(Self::Processing),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            _ => Err(format!("Unknown inbox event status: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InboxEvent {
    pub id: InboxEventId,
    pub idempotency_key: Option<String>,
    pub payload: serde_json::Value,
    pub status: InboxEventStatus,
    pub error: Option<String>,
    pub recorded_at: DateTime<Utc>,
    pub processed_at: Option<DateTime<Utc>>,
}

impl InboxEvent {
    pub fn payload<T>(&self) -> Result<T, serde_json::Error>
    where
        T: DeserializeOwned,
    {
        serde_json::from_value(self.payload.clone())
    }
}
