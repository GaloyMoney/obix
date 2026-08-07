use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use std::marker::PhantomData;

use job::{
    CurrentJob, Job, JobCompletion, JobInitializer, JobRunner, JobSpawner, JobType, RetrySettings,
};

use crate::tables::MailboxTables;

use super::{ArchiveConfig, EventArchiver};

const DEFAULT_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60 * 60);

/// Configuration for the archiver job registered via
/// [`Outbox::register_event_archiver`](crate::Outbox::register_event_archiver).
///
/// Each execution sweeps up to
/// [`ArchiveConfig::boundaries_per_run`](super::ArchiveConfig) settled
/// spans. While a run made progress (catch-up) the job reschedules
/// immediately; when there was nothing to do it reschedules after
/// `poll_interval`.
#[derive(Clone)]
pub struct OutboxArchiverJobConfig {
    pub job_type: JobType,
    pub retry_settings: RetrySettings,
    pub poll_interval: std::time::Duration,
}

impl OutboxArchiverJobConfig {
    pub fn new(job_type: JobType) -> Self {
        Self {
            job_type,
            retry_settings: RetrySettings::repeat_indefinitely(),
            poll_interval: DEFAULT_POLL_INTERVAL,
        }
    }

    pub fn with_retry_settings(mut self, settings: RetrySettings) -> Self {
        self.retry_settings = settings;
        self
    }

    /// How long an idle run waits before checking for newly settled
    /// history.
    pub fn with_poll_interval(mut self, interval: std::time::Duration) -> Self {
        self.poll_interval = interval;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct OutboxArchiverJobData {}

pub(crate) struct OutboxArchiverJobInitializer<Tables>
where
    Tables: MailboxTables,
{
    pool: sqlx::PgPool,
    archive: ArchiveConfig,
    job_type: JobType,
    retry_settings: RetrySettings,
    poll_interval: std::time::Duration,
    _phantom: PhantomData<Tables>,
}

impl<Tables> OutboxArchiverJobInitializer<Tables>
where
    Tables: MailboxTables,
{
    pub fn new(
        pool: &sqlx::PgPool,
        archive: ArchiveConfig,
        config: &OutboxArchiverJobConfig,
    ) -> Self {
        Self {
            pool: pool.clone(),
            archive,
            job_type: config.job_type.clone(),
            retry_settings: config.retry_settings.clone(),
            poll_interval: config.poll_interval,
            _phantom: PhantomData,
        }
    }
}

impl<Tables> JobInitializer for OutboxArchiverJobInitializer<Tables>
where
    Tables: MailboxTables,
{
    type Config = OutboxArchiverJobData;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn retry_on_error_settings(&self) -> RetrySettings {
        self.retry_settings.clone()
    }

    fn init(
        &self,
        _job: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(OutboxArchiverJobRunner::<Tables> {
            archiver: EventArchiver::<Tables>::new(&self.pool, self.archive.clone()),
            poll_interval: self.poll_interval,
        }))
    }
}

struct OutboxArchiverJobRunner<Tables>
where
    Tables: MailboxTables,
{
    archiver: EventArchiver<Tables>,
    poll_interval: std::time::Duration,
}

#[async_trait]
impl<Tables> JobRunner for OutboxArchiverJobRunner<Tables>
where
    Tables: MailboxTables,
{
    async fn run(
        &self,
        _current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        let report = self
            .archiver
            .run_once()
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
        if report.spans_archived > 0 {
            // Catch-up: keep sweeping until no settled spans remain.
            Ok(JobCompletion::RescheduleNow)
        } else {
            Ok(JobCompletion::RescheduleIn(self.poll_interval))
        }
    }
}
