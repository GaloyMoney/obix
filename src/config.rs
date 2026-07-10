use derive_builder::Builder;
use es_entity::clock::{Clock, ClockHandle};

pub const DEFAULT_PERSIST_EVENTS_BATCH_SIZE: usize = 5000;

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
    #[builder(default = "Clock::handle().clone()")]
    pub clock: ClockHandle,
}

impl MailboxConfig {
    pub fn builder() -> MailboxConfigBuilder {
        MailboxConfigBuilder::default()
    }
}
