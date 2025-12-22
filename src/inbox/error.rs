// src/inbox/error.rs
use thiserror::Error;

#[derive(Error, Debug)]
pub enum InboxError {
    #[error("InboxError - Sqlx: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("InboxError - EsEntity: {0}")]
    EsEntity(#[from] es_entity::EsEntityError),
    #[error("InboxError - Job: {0}")]
    Job(#[from] job::error::JobError),
    #[error("InboxError - NotFound: {0}")]
    NotFound(super::InboxEventId),
    #[error("InboxError - Deserialization: {0}")]
    Deserialization(#[from] serde_json::Error),
}
