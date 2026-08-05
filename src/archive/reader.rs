use bytes::Bytes;
use es_entity::clock::ClockHandle;
use serde::{Serialize, de::DeserializeOwned};

use std::sync::Arc;
use tokio::sync::mpsc;

use crate::{
    out::{OutboxEventId, PersistentDelivery, PersistentOutboxEvent, UndecodableEventError},
    sequence::EventSequence,
    tables::MailboxTables,
};

use super::{
    ArchiveChunk, ArchiveError, Compression, EventArchiveStorage, codec::ArchiveEventLine,
};

/// Serves pre-watermark history from object storage during backfill.
///
/// Archived deliveries stream straight to the requesting listener — they
/// deliberately do NOT enter the shared broadcast cache: old reads are
/// rare sequential replays, and warming the hot cache with a full-history
/// replay would just churn it.
#[derive(Clone)]
pub(crate) struct ArchiveReader {
    storage: Arc<dyn EventArchiveStorage>,
    clock: ClockHandle,
}

impl std::fmt::Debug for ArchiveReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArchiveReader").finish_non_exhaustive()
    }
}

impl ArchiveReader {
    pub(crate) fn new(storage: Arc<dyn EventArchiveStorage>, clock: ClockHandle) -> Self {
        Self { storage, clock }
    }

    /// Stream the archived deliveries in `(start_after, watermark]` to
    /// `sender`, in sequence order. Returns the last sequence sent.
    ///
    /// Files are read whole, one ahead: the next chunk is fetched while
    /// the current one is decoded, so storage latency does not sit on the
    /// critical path of a full-history replay.
    ///
    /// Holes in the archive coverage (e.g. history pruned before archiving
    /// existed, or a truncated file) are bridged with placeholder
    /// deliveries so the consumer's contiguity guarantee holds — a
    /// warn-level trace is emitted per hole.
    pub(crate) async fn stream_archived<P, Tables>(
        &self,
        pool: &sqlx::PgPool,
        start_after: EventSequence,
        watermark: EventSequence,
        sender: &mpsc::Sender<PersistentDelivery<P>>,
    ) -> Result<EventSequence, ArchiveError>
    where
        P: Serialize + DeserializeOwned + Send + Sync + 'static,
        Tables: MailboxTables,
    {
        let chunks = Tables::list_archive_chunks_from(pool, start_after).await?;
        // Ordered by min_sequence: chunks entirely above the watermark
        // (written by a concurrent archiver run) are not ours to serve.
        let mut chunks = chunks
            .into_iter()
            .take_while(|chunk| chunk.min_sequence <= watermark);
        let mut last_sent = start_after;

        let mut pending_fetch = chunks.next().map(|chunk| self.spawn_fetch(chunk));

        while let Some(fetch) = pending_fetch.take() {
            // Kick off the next file's fetch before decoding this one.
            pending_fetch = chunks.next().map(|chunk| self.spawn_fetch(chunk));

            let (chunk, data) = fetch
                .await
                .map_err(|e| ArchiveError::storage(format!("fetch task failed: {e}")))??;
            let data = Compression::decode_path(&chunk.path, data)?;

            let lines: Vec<&[u8]> = data.split(|&b| b == b'\n').collect();
            let mut last_nonempty = None;
            for (i, line) in lines.iter().enumerate() {
                if !line.is_empty() {
                    last_nonempty = Some(i);
                }
            }
            for (i, raw) in lines.into_iter().enumerate() {
                if raw.is_empty() {
                    continue;
                }
                let line = match ArchiveEventLine::parse(raw) {
                    Ok(line) => line,
                    // A torn final line (truncated write, operator edit):
                    // treat the chunk's tail as missing — the chunk-tail
                    // guard below bridges it with placeholders — rather
                    // than wedging every below-watermark consumer on the
                    // same line forever. Mid-file corruption still fails
                    // loud.
                    Err(error) if Some(i) == last_nonempty => {
                        record_archive_line_torn(&chunk.path, &error);
                        break;
                    }
                    Err(error) => return Err(error),
                };
                let item: Result<PersistentOutboxEvent<P>, UndecodableEventError> =
                    line.decode::<P>()?;
                let delivery = PersistentDelivery::from(item);
                let sequence = delivery.sequence();
                // Monotonic by construction: never emit at or below the
                // last sent sequence, even if the manifest ever holds
                // overlapping chunks — a re-covered sequence must not
                // rewind `last_sent` or replace an already-delivered real
                // event with a bridged placeholder downstream.
                if sequence <= last_sent || sequence > watermark {
                    continue;
                }
                match self.bridge_hole(last_sent, sequence, sender).await? {
                    Some(last) => last_sent = last,
                    // Listener is gone.
                    None => return Ok(last_sent),
                }
                if sender.send(delivery).await.is_err() {
                    return Ok(last_sent);
                }
                last_sent = sequence;
            }

            // Lines within a chunk are contiguous by construction, but
            // guard the chunk's own tail: a truncated file must not
            // silently skip sequences.
            let expected_end = chunk.max_sequence.min(watermark);
            if last_sent < expected_end {
                record_archive_chunk_short(&chunk.path, u64::from(last_sent));
                if self
                    .bridge_hole(last_sent, expected_end.next(), sender)
                    .await?
                    .is_none()
                {
                    return Ok(last_sent);
                }
                last_sent = expected_end;
            }
        }

        Ok(last_sent)
    }

    fn spawn_fetch(
        &self,
        chunk: ArchiveChunk,
    ) -> tokio::task::JoinHandle<Result<(ArchiveChunk, Bytes), ArchiveError>> {
        let storage = self.storage.clone();
        tokio::spawn(async move {
            let data = storage.get(&chunk.path).await?;
            Ok((chunk, data))
        })
    }

    /// Emit placeholder deliveries for the hole `(last_sent, next)` — a
    /// range with no archive coverage — preserving contiguity. Returns
    /// `Ok(Some(hole_end))`, `Ok(None)` if the listener is gone.
    async fn bridge_hole<P>(
        &self,
        last_sent: EventSequence,
        next: EventSequence,
        sender: &mpsc::Sender<PersistentDelivery<P>>,
    ) -> Result<Option<EventSequence>, ArchiveError>
    where
        P: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        if next <= last_sent.next() {
            return Ok(Some(last_sent));
        }
        record_archive_coverage_hole(u64::from(last_sent.next()), u64::from(next) - 1);
        let mut sequence = last_sent.next();
        while sequence < next {
            let event: PersistentOutboxEvent<P> = PersistentOutboxEvent {
                id: OutboxEventId::new(),
                sequence,
                payload: None,
                tracing_context: None,
                recorded_at: self.clock.now(),
            };
            if sender
                .send(PersistentDelivery::from(Ok::<_, UndecodableEventError>(
                    event,
                )))
                .await
                .is_err()
            {
                return Ok(None);
            }
            sequence = sequence.next();
        }
        Ok(Some(EventSequence::from(u64::from(next) - 1)))
    }
}

#[tracing::instrument(
    name = "obix.archive.coverage_hole",
    level = "warn",
    skip_all,
    fields(from_sequence = from_sequence, up_to_sequence = up_to_sequence)
)]
fn record_archive_coverage_hole(from_sequence: u64, up_to_sequence: u64) {}

#[tracing::instrument(
    name = "obix.archive.chunk_short",
    level = "warn",
    skip_all,
    fields(path = %path, last_sequence = last_sequence)
)]
fn record_archive_chunk_short(path: &str, last_sequence: u64) {}

#[tracing::instrument(
    name = "obix.archive.line_torn",
    level = "warn",
    skip_all,
    fields(path = %path, error = %error)
)]
fn record_archive_line_torn(path: &str, error: &ArchiveError) {}
