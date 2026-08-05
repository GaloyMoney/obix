use async_trait::async_trait;
use es_entity::clock::ClockHandle;

use std::marker::PhantomData;

use crate::{sequence::EventSequence, tables::MailboxTables};

use super::{ArchiveBoundary, ArchiveError};

/// Certifies which history is final and may be archived, and where each
/// settled span ends on the stream.
///
/// Implementations must only return boundaries whose history is *final* —
/// once a span is swept, its events are deleted from postgres. The default
/// [`DailyRetentionBoundary`] certifies purely by age. Deployments with a
/// settlement process (e.g. end-of-day) should instead certify from their
/// own marker on the stream — reading the sequence of a settlement event
/// keeps late settlement-processing events inside the span they belong to,
/// and a stalled settlement stalls archiving, which is the correct failure
/// semantics for history that is not yet final.
///
/// Contract: returned boundaries are oldest-first with strictly increasing
/// `up_to_sequence`, all greater than `after`. Already-archived boundaries
/// (`up_to_sequence <= after`) must be excluded — an idempotently
/// republished settlement marker must not rewind the archive.
///
/// A certified span must contain no in-flight transactions: a transaction
/// that has been assigned a sequence inside the span but commits *after*
/// the sweep lands its row below the archive watermark — masked by the
/// placeholder the sweep materialized for its sequence and invisible to
/// backfills, which resume above the watermark. Providers must certify
/// only spans old enough (or settlement-gated enough) that this cannot
/// happen; the resulting data loss is silent.
#[async_trait]
pub trait ArchiveBoundaryProvider: Send + Sync + 'static {
    async fn pending_boundaries(
        &self,
        after: EventSequence,
    ) -> Result<Vec<ArchiveBoundary>, ArchiveError>;
}

/// Age-based [`ArchiveBoundaryProvider`]: a UTC calendar date becomes
/// archivable once its end is at least `retention` in the past — every
/// archived event is at least `retention` old. Boundaries are drawn at
/// the highest sequence of each UTC calendar date of `recorded_at`.
/// Labels are the ISO dates (`"2026-07-20"`), so the archive is laid
/// out one directory per date.
///
/// Choose `retention` larger than the longest transaction lifetime of
/// the deployment (see the [`ArchiveBoundaryProvider`] contract); that
/// is effectively guaranteed at the default multi-day retentions.
///
/// `recorded_at` is insert time, so this is a heuristic — events written
/// with an overridden clock can be bucketed into a neighboring span. That
/// is cosmetic (it only affects which file an event lands in, never
/// ordering); deployments needing exact span semantics should plug in a
/// settlement-marker-based provider instead.
pub struct DailyRetentionBoundary<Tables> {
    pool: sqlx::PgPool,
    retention: chrono::Duration,
    clock: ClockHandle,
    _phantom: PhantomData<Tables>,
}

impl<Tables> DailyRetentionBoundary<Tables> {
    pub fn new(pool: &sqlx::PgPool, retention: chrono::Duration, clock: ClockHandle) -> Self {
        Self {
            pool: pool.clone(),
            retention,
            clock,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<Tables> ArchiveBoundaryProvider for DailyRetentionBoundary<Tables>
where
    Tables: MailboxTables,
{
    async fn pending_boundaries(
        &self,
        after: EventSequence,
    ) -> Result<Vec<ArchiveBoundary>, ArchiveError> {
        // A date is eligible only once its end — the start of the next
        // date — is at least `retention` in the past: `recorded_before`
        // is `now - retention` floored to the UTC date start. Every
        // archived event is therefore at least `retention` old (flooring
        // only makes the floor more conservative), which bounds the
        // in-flight-transaction race documented on
        // [`ArchiveBoundaryProvider`]: as long as no transaction stays
        // open longer than `retention`, no committed event can land
        // below the watermark after a sweep. Sub-day retentions behave
        // as expected — nothing is truncated to whole days.
        let recorded_before = (self.clock.now() - self.retention)
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .expect("midnight is a valid time")
            .and_utc();
        Ok(Tables::list_archivable_boundaries(&self.pool, after, recorded_before).await?)
    }
}
