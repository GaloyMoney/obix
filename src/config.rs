use derive_builder::Builder;
use es_entity::clock::{Clock, ClockHandle};

#[derive(Clone, Builder)]
pub struct MailboxConfig {
    #[builder(default = "100")]
    pub event_buffer_size: usize,
    #[builder(default = "1000")]
    pub event_cache_size: usize,
    #[builder(default = "10")]
    pub event_cache_trim_percent: u8,
    #[builder(default = "Clock::handle().clone()")]
    pub clock: ClockHandle,
}

impl MailboxConfig {
    pub fn builder() -> MailboxConfigBuilder {
        MailboxConfigBuilder::default()
    }
}

impl Default for MailboxConfig {
    fn default() -> Self {
        Self {
            event_buffer_size: 100,
            event_cache_size: 1000,
            event_cache_trim_percent: 10,
            clock: Clock::handle().clone(),
        }
    }
}
