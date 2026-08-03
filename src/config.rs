use derive_builder::Builder;
use es_entity::clock::{Clock, ClockHandle};

pub const DEFAULT_PERSIST_EVENTS_BATCH_SIZE: usize = 5000;

/// How long the persistent cache waits before gap-filling a missing
/// sequence. Most gaps are in-flight transactions that committed out of
/// sequence-allocation order and resolve on their own within a few ms;
/// filling immediately makes the gap-fill upsert block on exactly those
/// uncommitted rows (ON CONFLICT speculative insertion) until they commit.
pub const DEFAULT_GAP_FILL_GRACE: std::time::Duration = std::time::Duration::from_millis(250);

/// Width, in `sequence` units, of each `persistent_outbox_events` partition
/// the maintainer pre-creates. Matches the initial `p0` partition shipped in
/// the migration (`[0, 10_000_000)`), so the first maintainer-created
/// partition (`p1`) tiles seamlessly onto it. Size to a few hours of peak
/// traffic in a real deployment.
pub const DEFAULT_PARTITION_WIDTH: u64 = 10_000_000;

/// How many partitions ahead of the current sequence head the maintainer
/// keeps created. `premake * width` must comfortably exceed the events
/// produced between two maintainer ticks so the head never reaches the last
/// pre-made boundary (which would spill into the `DEFAULT` partition).
pub const DEFAULT_PARTITION_PREMAKE: u64 = 2;

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
    /// Width in `sequence` units of each partition the maintainer pre-creates;
    /// see [`DEFAULT_PARTITION_WIDTH`].
    #[builder(default = "DEFAULT_PARTITION_WIDTH")]
    pub partition_width: u64,
    /// How many partitions ahead of the head the maintainer keeps created;
    /// see [`DEFAULT_PARTITION_PREMAKE`].
    #[builder(default = "DEFAULT_PARTITION_PREMAKE")]
    pub partition_premake: u64,
    /// Poll interval of the partition maintainer job; see
    /// [`DEFAULT_PARTITION_MAINTAINER_INTERVAL`].
    #[builder(default = "DEFAULT_PARTITION_MAINTAINER_INTERVAL")]
    pub partition_maintainer_interval: std::time::Duration,
    #[builder(default = "Clock::handle().clone()")]
    pub clock: ClockHandle,
}

impl MailboxConfig {
    pub fn builder() -> MailboxConfigBuilder {
        MailboxConfigBuilder::default()
    }
}
