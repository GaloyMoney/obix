#[derive(Debug, thiserror::Error)]
pub enum ArchiveError {
    #[error("archive database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("archive storage error: {0}")]
    Storage(String),
    #[error("archive codec error: {0}")]
    Codec(String),
    #[error("archive chunk overlaps existing manifest coverage: {0}")]
    OverlappingChunk(String),
    #[error("archive is not configured for this outbox")]
    NotConfigured,
}

impl ArchiveError {
    /// Wrap a backend error (GCS, S3, ...) returned by an
    /// [`EventArchiveStorage`](crate::archive::EventArchiveStorage) or
    /// [`ArchiveBoundaryProvider`](crate::archive::ArchiveBoundaryProvider)
    /// implementation.
    pub fn storage(error: impl std::fmt::Display) -> Self {
        Self::Storage(error.to_string())
    }
}
