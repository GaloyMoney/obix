use derive_builder::Builder;
use es_entity::clock::{Clock, ClockHandle};

pub const DEFAULT_PERSIST_EVENTS_BATCH_SIZE: usize = 5000;

/// How long the persistent cache waits before gap-filling a missing
/// sequence. Most gaps are in-flight transactions that committed out of
/// sequence-allocation order and resolve on their own within a few ms;
/// filling immediately makes the gap-fill upsert block on exactly those
/// uncommitted rows (ON CONFLICT speculative insertion) until they commit.
pub const DEFAULT_GAP_FILL_GRACE: std::time::Duration = std::time::Duration::from_millis(250);

/// Width, in `sequence` units, of each `persistent_outbox_events` partition.
///
/// A fixed schema constant, deliberately **not** runtime-configurable: it must
/// equal the range of the initial `p0` partition hard-coded in the migration
/// (`[0, 2_000_000)`), or the maintainer would create partitions that overlap
/// `p0`. Changing the size means editing both this constant and the migration
/// together.
///
/// Sized from real event data: outbox rows measure ~760 B each (payloads
/// average a few hundred bytes and rarely TOAST), so 2M rows ≈ ~1.5 GB per
/// partition — small enough that the hot partition stays cache-resident and a
/// per-partition vacuum is quick, while keeping the partition count low.
pub const DEFAULT_PARTITION_WIDTH: u64 = 2_000_000;

/// How many partitions ahead of the current sequence head the maintainer
/// keeps created — including on the initial synchronous `ensure` at
/// registration, so a fresh install starts with a multi-partition runway
/// (the migration itself ships only `p0` + `DEFAULT`). `premake * width` must
/// comfortably exceed the events produced between two maintainer ticks so the
/// head never reaches the last pre-made boundary (which would spill into the
/// `DEFAULT` partition). Empty partitions are cheap, so this errs generous:
/// 5 * 2M = 10M sequences of headroom by default.
pub const DEFAULT_PARTITION_PREMAKE: u64 = 5;

/// How often the partition maintainer wakes to pre-create partitions ahead of
/// the head. Each tick is idempotent (`CREATE ... IF NOT EXISTS`), so this is
/// a cheap steady-state poll; premake margin, not cadence, is the safety
/// budget against bursts.
pub const DEFAULT_PARTITION_MAINTAINER_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(3600);

#[derive(Clone, Builder)]
pub struct MailboxConfig {
    #[builder(default = "100")]
    pub event_buffer_size: usize,
    #[builder(default = "1000")]
    pub event_cache_size: usize,
    #[builder(default = "10")]
    pub event_cache_trim_percent: u8,
    #[builder(default = "DEFAULT_PERSIST_EVENTS_BATCH_SIZE")]
    pub persist_events_batch_size: usize,
    /// Grace period before the first proactive gap fill for a stalled
    /// broadcast sequence; see [`DEFAULT_GAP_FILL_GRACE`]. Retries after a
    /// fill attempt are unaffected (fixed 1s interval).
    #[builder(default = "DEFAULT_GAP_FILL_GRACE")]
    pub gap_fill_grace: std::time::Duration,
    /// How many partitions ahead of the head the maintainer keeps created;
    /// see [`DEFAULT_PARTITION_PREMAKE`]. (Partition *width* is the fixed
    /// [`DEFAULT_PARTITION_WIDTH`] constant, not configurable — it is coupled to
    /// the migration's `p0` range.)
    #[builder(default = "DEFAULT_PARTITION_PREMAKE")]
    pub partition_premake: u64,
    /// Poll interval of the partition maintainer job; see
    /// [`DEFAULT_PARTITION_MAINTAINER_INTERVAL`].
    #[builder(default = "DEFAULT_PARTITION_MAINTAINER_INTERVAL")]
    pub partition_maintainer_interval: std::time::Duration,
    #[builder(default = "Clock::handle().clone()")]
    pub clock: ClockHandle,
    /// Cold-storage archiving of old persistent events (see
    /// [`archive`](crate::archive)). When set, the outbox serves
    /// pre-watermark history reads from the archive, and
    /// [`Outbox::register_event_archiver`](crate::Outbox::register_event_archiver)
    /// can schedule the sweep job.
    #[builder(default)]
    pub archive: Option<crate::archive::ArchiveConfig>,
}

impl MailboxConfig {
    pub fn builder() -> MailboxConfigBuilder {
        MailboxConfigBuilder::default()
    }
}
