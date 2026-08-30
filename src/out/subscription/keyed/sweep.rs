//! The wake plane, part 2: startup reconcile, repair, staleness bound.
//!
//! A single per-process-independent resident job per keyed-subscriber type:
//! on a timer, enumerate every currently-subscribed key and idempotently
//! respawn it. Registered alongside the [`router`](super::router) as a
//! distinct resident job rather than fused into it — folding a sweep timer
//! into the shared singleton runner loop would touch every registered
//! singleton subscriber in the crate, not just this one.
//!
//! Complements the router: a fresh subscription is Active from birth (see
//! [`Subscriptions::subscribe_in_op`](super::Subscriptions::subscribe_in_op))
//! so it needs no wake until its first dormancy; from then on the router's
//! liveness-only routing is the fast path, and the sweep is the backstop —
//! the startup reconcile (a fresh process has no router state yet), the
//! repair path (a routing-key bug or a lost wake), and the bound on
//! staleness under either.

use serde::{Deserialize, Serialize};
use std::{marker::PhantomData, time::Duration};

use job::{CurrentJob, Job, JobType, ResidentJobCompletion, ResidentJobInitializer, RetrySettings};

use super::{KeyMsg, KeyedSubscriberConfig, derived_job_type};
use crate::tables::MailboxTables;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(in crate::out) struct SweepJobData {}

pub(in crate::out) struct SweepJobInitializer<Tables> {
    pool: sqlx::PgPool,
    subscriber_type: JobType,
    spawner: job::KeyedJobSpawner<KeyMsg>,
    sweep_interval: Duration,
    _tables: PhantomData<Tables>,
}

impl<Tables> SweepJobInitializer<Tables> {
    pub(in crate::out) fn new(
        pool: sqlx::PgPool,
        spawner: job::KeyedJobSpawner<KeyMsg>,
        config: &KeyedSubscriberConfig,
    ) -> Self {
        Self {
            pool,
            subscriber_type: config.job_type.clone(),
            spawner,
            sweep_interval: config.sweep_interval,
            _tables: PhantomData,
        }
    }
}

impl<Tables: MailboxTables> ResidentJobInitializer for SweepJobInitializer<Tables> {
    type Config = SweepJobData;

    fn job_type(&self) -> JobType {
        derived_job_type(self.subscriber_type.as_str(), "sweep")
    }

    fn retry_on_error_settings(&self) -> RetrySettings {
        RetrySettings::repeat_indefinitely()
    }

    fn init(
        &self,
        _job: &Job,
    ) -> Result<Box<dyn job::ResidentJobRunner>, Box<dyn std::error::Error>> {
        Ok(Box::new(SweepJobRunner::<Tables> {
            pool: self.pool.clone(),
            subscriber_type: self.subscriber_type.clone(),
            spawner: self.spawner.clone(),
            sweep_interval: self.sweep_interval,
            _tables: PhantomData,
        }))
    }
}

struct SweepJobRunner<Tables> {
    pool: sqlx::PgPool,
    subscriber_type: JobType,
    spawner: job::KeyedJobSpawner<KeyMsg>,
    sweep_interval: Duration,
    _tables: PhantomData<Tables>,
}

impl<Tables: MailboxTables> SweepJobRunner<Tables> {
    async fn sweep_once(&self) -> Result<(), Box<dyn std::error::Error>> {
        let keys =
            Tables::list_subscription_keys(&self.pool, self.subscriber_type.as_str()).await?;
        if keys.is_empty() {
            return Ok(());
        }
        let specs = keys
            .into_iter()
            .map(|key| job::KeyedJobSpec::new(key.clone(), KeyMsg { key }))
            .collect();
        self.spawner.spawn_all(specs).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl<Tables: MailboxTables> job::ResidentJobRunner for SweepJobRunner<Tables> {
    async fn run(
        &self,
        mut current_job: CurrentJob,
    ) -> Result<ResidentJobCompletion, Box<dyn std::error::Error>> {
        // Sweep once immediately (the startup reconcile), then on every
        // subsequent interval.
        self.sweep_once().await?;
        loop {
            tokio::select! {
                biased;
                _ = current_job.shutdown_requested() => {
                    return Ok(ResidentJobCompletion::RescheduleNow);
                }
                _ = tokio::time::sleep(self.sweep_interval) => {
                    self.sweep_once().await?;
                }
            }
        }
    }
}
