// TODO: need to update configs to use builder??
use es_entity::clock::{Clock, ClockHandle};

#[derive(Clone)]
pub struct MailboxConfig {
    pub event_buffer_size: usize,
    pub event_cache_size: usize,
    pub event_cache_trim_percent: u8,
    pub clock: ClockHandle,
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
