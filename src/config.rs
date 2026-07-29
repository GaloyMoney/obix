use derive_builder::Builder;
use es_entity::clock::{Clock, ClockHandle};

pub const DEFAULT_PERSIST_EVENTS_BATCH_SIZE: usize = 5000;

/// How long the persistent cache waits before gap-filling a missing
/// sequence. Most gaps are in-flight transactions that committed out of
/// sequence-allocation order and resolve on their own within a few ms;
/// filling immediately makes the gap-fill upsert block on exactly those
/// uncommitted rows (ON CONFLICT speculative insertion) until they commit.
pub const DEFAULT_GAP_FILL_GRACE: std::time::Duration = std::time::Duration::from_millis(250);

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
    #[builder(default = "Clock::handle().clone()")]
    pub clock: ClockHandle,
}

impl MailboxConfig {
    pub fn builder() -> MailboxConfigBuilder {
        MailboxConfigBuilder::default()
    }
}
