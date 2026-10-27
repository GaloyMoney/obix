use es_entity::clock::{Clock, ClockHandle};
use job::{JobType, RetrySettings};

#[derive(Clone)]
pub struct InboxConfig {
    pub job_type: JobType,
    pub retry_settings: RetrySettings,
    pub clock: ClockHandle,
}

impl InboxConfig {
    pub fn new(job_type: JobType) -> Self {
        Self {
            job_type,
            retry_settings: RetrySettings::default(),
            clock: Clock::handle().clone(),
        }
    }

    pub fn with_retry_settings(mut self, settings: RetrySettings) -> Self {
        self.retry_settings = settings;
        self
    }

    pub fn with_clock(mut self, clock: ClockHandle) -> Self {
        self.clock = clock;
        self
    }
}
