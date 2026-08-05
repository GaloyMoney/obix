use async_trait::async_trait;
use bytes::Bytes;

use std::{collections::BTreeMap, sync::Mutex};

use super::ArchiveError;

/// Object-storage backend for the event archive.
///
/// Deliberately minimal — whole-file put/get is all the archive needs:
/// chunk files are bounded (see
/// [`ArchiveConfig::target_file_bytes`](crate::archive::ArchiveConfig)) and
/// always consumed start-to-finish. `put` must be idempotent for a given
/// path: after a crash the archiver re-exports and overwrites the same
/// path. obix ships no backend; consumers plug in their own (GCS, S3,
/// ...).
#[async_trait]
pub trait EventArchiveStorage: Send + Sync + 'static {
    async fn put(&self, path: &str, data: Bytes) -> Result<(), ArchiveError>;
    async fn get(&self, path: &str) -> Result<Bytes, ArchiveError>;
}

/// In-memory [`EventArchiveStorage`] for tests and local development.
#[derive(Debug, Default)]
pub struct InMemoryArchiveStorage {
    objects: Mutex<BTreeMap<String, Bytes>>,
}

impl InMemoryArchiveStorage {
    pub fn new() -> Self {
        Self::default()
    }

    /// Paths of all stored objects, in lexicographic order.
    pub fn list(&self) -> Vec<String> {
        self.objects
            .lock()
            .expect("storage lock poisoned")
            .keys()
            .cloned()
            .collect()
    }

    pub fn get_sync(&self, path: &str) -> Option<Bytes> {
        self.objects
            .lock()
            .expect("storage lock poisoned")
            .get(path)
            .cloned()
    }
}

#[async_trait]
impl EventArchiveStorage for InMemoryArchiveStorage {
    async fn put(&self, path: &str, data: Bytes) -> Result<(), ArchiveError> {
        self.objects
            .lock()
            .expect("storage lock poisoned")
            .insert(path.to_string(), data);
        Ok(())
    }

    async fn get(&self, path: &str) -> Result<Bytes, ArchiveError> {
        self.objects
            .lock()
            .expect("storage lock poisoned")
            .get(path)
            .cloned()
            .ok_or_else(|| ArchiveError::storage(format!("object not found: {path}")))
    }
}
