use es_entity::clock::{Clock, ClockHandle};

use std::time::Duration;

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

    /// Set max attempts before dead-lettering
    pub fn with_max_attempts(mut self, n: u32) -> Self {
        self.retry_settings.n_attempts = Some(n);
        self
    }

    /// Set backoff bounds
    pub fn with_backoff(mut self, min: Duration, max: Duration) -> Self {
        self.retry_settings.min_backoff = min;
        self.retry_settings.max_backoff = max;
        self
    }

    /// Set jitter percentage (0-100)
    pub fn with_jitter(mut self, pct: u8) -> Self {
        self.retry_settings.backoff_jitter_pct = pct;
        self
    }

    pub fn with_clock(mut self, clock: ClockHandle) -> Self {
        self.clock = clock;
        self
    }
}
