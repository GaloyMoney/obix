//! The timer-driven partition maintainer job.
//!
//! Registered via
//! [`Outbox::register_partition_maintainer`](crate::out::Outbox::register_partition_maintainer),
//! it drives [`ensure_partitions`](super::ensure_partitions) on an internal
//! interval. The job stays resident and reschedules **only** when it falls out
//! of its loop — on shutdown (reschedule to resume after restart) or on an
//! `ensure_partitions` error (which propagates so the scheduler retries per the
//! configured [`RetrySettings`] — the alert). It carries no durable execution
//! state: each tick is idempotent off the current sequence head.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use job::{
    CurrentJob, Job, JobType, ResidentJobCompletion, ResidentJobInitializer, ResidentJobRunner,
    RetrySettings,
};

use super::Partitions;
use crate::tables::MailboxTables;

/// Registration surface for the partition maintainer, mirroring
/// [`OutboxEventJobConfig`](crate::out::OutboxEventJobConfig): the job-scheduling
/// details (type + retry policy). The width / premake / poll-interval come from
/// the [`MailboxConfig`](crate::MailboxConfig) the outbox was initialised with.
#[derive(Clone)]
pub struct PartitionMaintainerConfig {
    pub job_type: JobType,
    pub retry_settings: RetrySettings,
}

impl PartitionMaintainerConfig {
    pub fn new(job_type: JobType) -> Self {
        Self {
            job_type,
            retry_settings: RetrySettings::repeat_indefinitely(),
        }
    }

    pub fn with_retry_settings(mut self, settings: RetrySettings) -> Self {
        self.retry_settings = settings;
        self
    }
}

/// The maintainer holds no durable execution state: each tick is idempotent
/// off the current sequence head, so nothing needs to survive a restart.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct PartitionMaintainerJobData {}

pub(crate) struct PartitionMaintainerJobInitializer<Tables>
where
    Tables: MailboxTables,
{
    partitions: Partitions<Tables>,
    job_type: JobType,
    retry_settings: RetrySettings,
    interval: std::time::Duration,
}

impl<Tables> PartitionMaintainerJobInitializer<Tables>
where
    Tables: MailboxTables,
{
    pub fn new(
        partitions: Partitions<Tables>,
        config: &PartitionMaintainerConfig,
        interval: std::time::Duration,
    ) -> Self {
        Self {
            partitions,
            job_type: config.job_type.clone(),
            retry_settings: config.retry_settings.clone(),
            interval,
        }
    }
}

impl<Tables> ResidentJobInitializer for PartitionMaintainerJobInitializer<Tables>
where
    Tables: MailboxTables,
{
    type Config = PartitionMaintainerJobData;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn retry_on_error_settings(&self) -> RetrySettings {
        self.retry_settings.clone()
    }

    fn init(&self, _job: &Job) -> Result<Box<dyn ResidentJobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(PartitionMaintainerJobRunner::<Tables> {
            partitions: self.partitions.clone(),
            interval: self.interval,
        }))
    }
}

struct PartitionMaintainerJobRunner<Tables>
where
    Tables: MailboxTables,
{
    partitions: Partitions<Tables>,
    interval: std::time::Duration,
}

#[async_trait]
impl<Tables> ResidentJobRunner for PartitionMaintainerJobRunner<Tables>
where
    Tables: MailboxTables,
{
    async fn run(
        &self,
        mut current_job: CurrentJob,
    ) -> Result<ResidentJobCompletion, Box<dyn std::error::Error>> {
        // Re-run on an internal timer rather than round-tripping through the job
        // scheduler each tick: the job stays resident and only falls out of the
        // loop on shutdown (reschedule to resume after restart) or on an
        // `ensure` error (propagated below — the scheduler then retries per
        // `retry_settings`, which is the alert).
        loop {
            self.partitions
                .ensure()
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

            tokio::select! {
                biased;
                _ = current_job.shutdown_requested() => {
                    return Ok(ResidentJobCompletion::RescheduleNow);
                }
                _ = tokio::time::sleep(self.interval) => {}
            }
        }
    }
}
