use derive_builder::Builder;
use es_entity::clock::{Clock, ClockHandle};

/// SQL insert chunk size; never splits the surrounding publication.
pub const DEFAULT_PERSIST_EVENTS_BATCH_SIZE: usize = 5000;
/// Maximum rows in one event-consumer catch-up read and its delivery buffer.
pub const DEFAULT_BACKFILL_PAGE_SIZE: usize = 1000;
/// Coalescing window for post-commit cross-process notification hints.
pub const DEFAULT_NOTIFY_DEBOUNCE: std::time::Duration = std::time::Duration::from_millis(25);
/// Poll the committed head after this much silence to recover lost wake-ups.
pub const DEFAULT_IDLE_RESYNC_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);
/// Must equal the initial partition's range in the setup migration.
pub const DEFAULT_PARTITION_WIDTH: u64 = 2_000_000;
pub const DEFAULT_PARTITION_PREMAKE: u64 = 5;
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
    #[builder(default = "DEFAULT_BACKFILL_PAGE_SIZE")]
    pub backfill_page_size: usize,
    #[builder(default = "DEFAULT_NOTIFY_DEBOUNCE")]
    pub notify_debounce: std::time::Duration,
    #[builder(default = "DEFAULT_IDLE_RESYNC_INTERVAL")]
    pub idle_resync_interval: std::time::Duration,
    #[builder(default = "DEFAULT_PARTITION_PREMAKE")]
    pub partition_premake: u64,
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
