use bytes::Bytes;

use std::marker::PhantomData;

use crate::{sequence::EventSequence, tables::MailboxTables};

use super::{ArchiveChunk, ArchiveConfig, ArchiveError, Compression, codec::ArchiveEventLine};

/// What one [`EventArchiver::run_once`] did.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ArchiveRunReport {
    /// Settled spans swept during this run (bounded by
    /// [`ArchiveConfig::boundaries_per_run`](super::ArchiveConfig)).
    pub spans_archived: usize,
    /// JSONL files written to storage and recorded in the manifest.
    pub chunks_written: usize,
    /// Archive watermark after the run: the highest sequence no longer in
    /// postgres.
    pub watermark: EventSequence,
}

/// Sweeps settled spans of persistent events out of postgres into object
/// storage. Payload-type-agnostic: export works on the raw stored JSON;
/// decoding happens on read.
///
/// Progress is tracked by the `persistent_outbox_archive_chunks` manifest
/// table, so runs are idempotent and crash-safe at file granularity:
/// re-exporting a span overwrites the same deterministic path, and the
/// manifest insert + event deletion are a single statement.
pub struct EventArchiver<Tables> {
    pool: sqlx::PgPool,
    config: ArchiveConfig,
    _phantom: PhantomData<Tables>,
}

impl<Tables> EventArchiver<Tables>
where
    Tables: MailboxTables,
{
    pub fn new(pool: &sqlx::PgPool, config: ArchiveConfig) -> Self {
        Self {
            pool: pool.clone(),
            config,
            _phantom: PhantomData,
        }
    }

    /// Archive up to [`ArchiveConfig::boundaries_per_run`](super::ArchiveConfig)
    /// settled spans, oldest first.
    ///
    /// Concurrent runs are mutually excluded by a postgres advisory lock
    /// (held for the duration of the run): the job's `spawn_unique` only
    /// serializes one [`crate::job::JobType`], and two interleaved runs
    /// could export the same span into disagreeing, overlapping chunks.
    /// A run that cannot take the lock reports zero progress.
    pub async fn run_once(&self) -> Result<ArchiveRunReport, ArchiveError> {
        let mut watermark = Tables::archive_watermark(&self.pool)
            .await?
            .unwrap_or_default();
        let mut report = ArchiveRunReport {
            watermark,
            ..Default::default()
        };

        // Session-scoped lock on a dedicated connection (released by the
        // explicit unlock below, or by postgres if the session dies).
        let mut conn = self.pool.acquire().await?;
        // Keyed by the table prefix so independent outboxes (lana core,
        // cala, ...) never block each other.
        let lock_key = Tables::persistent_outbox_events_channel();
        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock(hashtext($1))")
            .bind(lock_key)
            .fetch_one(&mut *conn)
            .await?;
        if !acquired {
            record_archive_run_skipped_locked();
            return Ok(report);
        }
        let result = self.run_locked(&mut report, &mut watermark).await;
        let _ = sqlx::query("SELECT pg_advisory_unlock(hashtext($1))")
            .bind(lock_key)
            .execute(&mut *conn)
            .await;
        result?;
        Ok(report)
    }

    async fn run_locked(
        &self,
        report: &mut ArchiveRunReport,
        watermark: &mut EventSequence,
    ) -> Result<(), ArchiveError> {
        let boundaries = self.config.boundary.pending_boundaries(*watermark).await?;

        // Collect first: already-archived boundaries (e.g. an
        // idempotently republished settlement marker) are skipped — the
        // archive never rewinds. Sweep everything the single
        // `pending_boundaries` call returned (up to the pacing cap): the
        // listing query scans the whole retention window, so re-issuing
        // it once per span during a first catch-up is wasted work.
        let pending: Vec<_> = boundaries
            .into_iter()
            .filter(|b| b.up_to_sequence > *watermark)
            .take(self.config.boundaries_per_run)
            .collect();
        for boundary in pending {
            record_span_export_start(&boundary.label, *watermark, boundary.up_to_sequence);
            self.export_span(&boundary.label, *watermark, boundary.up_to_sequence, report)
                .await?;
            *watermark = boundary.up_to_sequence;
            report.spans_archived += 1;
            report.watermark = *watermark;
        }
        Ok(())
    }

    /// Export the contiguous span `(from, up_to]` as one or more JSONL
    /// files under `label`'s directory, recording + pruning each file
    /// before starting the next.
    async fn export_span(
        &self,
        label: &str,
        from: EventSequence,
        up_to: EventSequence,
        report: &mut ArchiveRunReport,
    ) -> Result<(), ArchiveError> {
        let placeholder_time = self.config.clock.now();
        let mut cursor = from;
        let mut writer: Option<ChunkWriter> = None;

        while cursor < up_to {
            let rows = Tables::load_raw_export_page(
                &self.pool,
                cursor,
                up_to,
                self.config.export_page_size,
            )
            .await?;
            let full_page = rows.len() == self.config.export_page_size;

            for row in &rows {
                // Materialize sequence gaps (transactions that never
                // committed) as placeholder lines so the file is
                // contiguous. The span is certified settled, so no gap
                // here can still be in flight.
                self.push_placeholders(
                    &mut writer,
                    label,
                    cursor,
                    row.sequence,
                    placeholder_time,
                    report,
                )
                .await?;
                let w = writer.get_or_insert_with(|| ChunkWriter::new(label));
                ArchiveEventLine::from_row(row).write_to(&mut w.buf);
                w.advance(row.sequence);
                self.maybe_roll(&mut writer, report).await?;
                cursor = row.sequence;
            }

            if !full_page {
                // No more live rows in the range: everything up to and
                // including `up_to` is a gap.
                self.push_placeholders(
                    &mut writer,
                    label,
                    cursor,
                    up_to.next(),
                    placeholder_time,
                    report,
                )
                .await?;
                cursor = up_to;
            }
        }

        self.flush(&mut writer, report).await?;
        Ok(())
    }

    /// Append placeholder lines for the gap `(after, before)`.
    async fn push_placeholders(
        &self,
        writer: &mut Option<ChunkWriter>,
        label: &str,
        after: EventSequence,
        before: EventSequence,
        recorded_at: chrono::DateTime<chrono::Utc>,
        report: &mut ArchiveRunReport,
    ) -> Result<(), ArchiveError> {
        let mut sequence = after.next();
        while sequence < before {
            let w = writer.get_or_insert_with(|| ChunkWriter::new(label));
            ArchiveEventLine::placeholder(sequence, recorded_at).write_to(&mut w.buf);
            w.advance(sequence);
            self.maybe_roll(writer, report).await?;
            sequence = sequence.next();
        }
        Ok(())
    }

    async fn maybe_roll(
        &self,
        writer: &mut Option<ChunkWriter>,
        report: &mut ArchiveRunReport,
    ) -> Result<(), ArchiveError> {
        if writer
            .as_ref()
            .is_some_and(|w| w.buf.len() >= self.config.target_file_bytes)
        {
            self.flush(writer, report).await?;
        }
        Ok(())
    }

    /// Write the buffered file to storage, then atomically record it in
    /// the manifest and delete its events from the live table (a single
    /// statement). Storage first: a crash before the manifest write
    /// re-exports and overwrites the same path.
    async fn flush(
        &self,
        writer: &mut Option<ChunkWriter>,
        report: &mut ArchiveRunReport,
    ) -> Result<(), ArchiveError> {
        let Some(w) = writer.take() else {
            return Ok(());
        };
        let (chunk, data) = w.finish(&self.config.path_prefix, self.config.compression);
        self.config.storage.put(&chunk.path, data).await?;
        Tables::record_archive_chunk(&self.pool, &chunk).await?;
        report.chunks_written += 1;
        Ok(())
    }
}

/// Accumulates one JSONL file. Rolled over (never mid-event) once it
/// exceeds `target_file_bytes`.
struct ChunkWriter {
    label: String,
    buf: Vec<u8>,
    min_sequence: Option<EventSequence>,
    max_sequence: EventSequence,
}

impl ChunkWriter {
    fn new(label: &str) -> Self {
        Self {
            label: label.to_string(),
            buf: Vec::new(),
            min_sequence: None,
            max_sequence: EventSequence::BEGIN,
        }
    }

    fn advance(&mut self, sequence: EventSequence) {
        self.min_sequence.get_or_insert(sequence);
        self.max_sequence = sequence;
    }

    /// Returns the manifest chunk and the bytes to store: the buffer,
    /// compressed per `compression`. `target_file_bytes` keeps bounding
    /// the uncompressed buffer — it is the memory bound on write and
    /// read; stored objects come out smaller when compressing.
    fn finish(self, path_prefix: &str, compression: Compression) -> (ArchiveChunk, Bytes) {
        let min_sequence = self.min_sequence.expect("flushed writer has lines");
        let chunk = ArchiveChunk {
            path: chunk_path(
                path_prefix,
                &self.label,
                min_sequence,
                self.max_sequence,
                compression,
            ),
            min_sequence,
            max_sequence: self.max_sequence,
        };
        let data = compression
            .encode(&self.buf)
            .expect("gzip encode of in-memory buffer cannot fail");
        (chunk, data)
    }
}

/// Deterministic path for a chunk:
/// `<prefix><label>/events-<min>-<max>.<ext>` where `<ext>` encodes the
/// compression (`jsonl` or `jsonl.gz`). Zero-padded so lexicographic
/// order matches sequence order. A typical span is a single file in its
/// directory; larger spans roll over into multiple contiguous files.
fn chunk_path(
    path_prefix: &str,
    label: &str,
    min_sequence: EventSequence,
    max_sequence: EventSequence,
    compression: Compression,
) -> String {
    format!(
        "{}{}/events-{:020}-{:020}.{}",
        path_prefix,
        label,
        u64::from(min_sequence),
        u64::from(max_sequence),
        compression.file_extension()
    )
}

#[tracing::instrument(
    name = "obix.archive.span_export_start",
    level = "info",
    skip_all,
    fields(label = %label, from = u64::from(from), up_to = u64::from(up_to))
)]
fn record_span_export_start(label: &str, from: EventSequence, up_to: EventSequence) {}

#[tracing::instrument(name = "obix.archive.run_skipped_locked", level = "info", skip_all)]
fn record_archive_run_skipped_locked() {}
