use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    decode_persistent_event,
    out::{OutboxEventId, PersistentOutboxEvent, UndecodableEventError},
    record_tracing_context_undecodable,
    sequence::EventSequence,
};

use super::{ArchiveError, RawExportRow};

/// One line of an archive JSONL file: exactly one sequence position,
/// mirroring the live table's semantics. `payload: null` is a sequence
/// placeholder (a gap from a rolled-back transaction); lines are
/// contiguous within a file — the exporter materializes sequence gaps as
/// placeholder lines so a chunk file is a complete history segment.
#[derive(Debug, Serialize, Deserialize)]
pub(super) struct ArchiveEventLine {
    pub id: OutboxEventId,
    pub sequence: EventSequence,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tracing_context: Option<serde_json::Value>,
    pub recorded_at: chrono::DateTime<chrono::Utc>,
}

impl ArchiveEventLine {
    pub fn from_row(row: &RawExportRow) -> Self {
        Self {
            id: row.id,
            sequence: row.sequence,
            payload: row.payload.clone(),
            tracing_context: row.tracing_context.clone(),
            recorded_at: row.recorded_at,
        }
    }

    /// A synthesized placeholder for a sequence gap (a transaction that
    /// never committed). `recorded_at` is meaningless for a gap; the
    /// export time is used.
    pub fn placeholder(
        sequence: EventSequence,
        recorded_at: chrono::DateTime<chrono::Utc>,
    ) -> Self {
        Self {
            id: OutboxEventId::new(),
            sequence,
            payload: None,
            tracing_context: None,
            recorded_at,
        }
    }

    pub fn write_to(&self, buf: &mut Vec<u8>) {
        serde_json::to_writer(&mut *buf, self).expect("archive line serialization failed");
        buf.push(b'\n');
    }

    /// Decode one line into a delivery item through the same decode path
    /// as live events: `Ok(None)` payload is a placeholder, an
    /// undecodable payload is the `Err` arm of the inner result.
    pub fn decode<P>(
        &self,
    ) -> Result<Result<PersistentOutboxEvent<P>, UndecodableEventError>, ArchiveError>
    where
        P: Serialize + DeserializeOwned + Send,
    {
        let tracing_context = self
            .tracing_context
            .clone()
            .filter(|v| !v.is_null())
            .and_then(|value| match serde_json::from_value(value) {
                Ok(context) => Some(context),
                Err(error) => {
                    record_tracing_context_undecodable(&error);
                    None
                }
            });
        Ok(decode_persistent_event(
            self.id,
            u64::from(self.sequence),
            self.recorded_at,
            tracing_context,
            self.payload.clone(),
        ))
    }

    pub fn parse(line: &[u8]) -> Result<Self, ArchiveError> {
        serde_json::from_slice(line)
            .map_err(|error| ArchiveError::Codec(format!("invalid archive line: {error}")))
    }
}

pub(super) fn gzip_encode(data: &[u8]) -> Result<bytes::Bytes, ArchiveError> {
    use flate2::{Compression as Level, write::GzEncoder};
    use std::io::Write;

    let mut encoder = GzEncoder::new(Vec::new(), Level::default());
    encoder
        .write_all(data)
        .and_then(|()| encoder.finish())
        .map(bytes::Bytes::from)
        .map_err(|error| ArchiveError::Codec(format!("gzip encode: {error}")))
}

pub(super) fn gzip_decode(data: bytes::Bytes) -> Result<bytes::Bytes, ArchiveError> {
    use flate2::read::GzDecoder;
    use std::io::Read;

    let mut decoded = Vec::new();
    GzDecoder::new(&data[..])
        .read_to_end(&mut decoded)
        .map_err(|error| ArchiveError::Codec(format!("gzip decode: {error}")))?;
    Ok(bytes::Bytes::from(decoded))
}
