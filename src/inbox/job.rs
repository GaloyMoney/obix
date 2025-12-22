use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub use job::Jobs;
use job::{
    CurrentJob, Job, JobCompletion, JobConfig, JobInitializer, JobRunner, JobType, RetrySettings,
};

use super::{InboxEvent, InboxEventId, InboxEventStatus};
use crate::tables::MailboxTables;

pub enum InboxResult {
    Complete,
    ReprocessNow,
    ReprocessIn(std::time::Duration),
}

pub trait InboxHandler: Send + Sync + 'static {
    fn handle(
        &self,
        event: &InboxEvent,
    ) -> impl std::future::Future<
        Output = Result<InboxResult, Box<dyn std::error::Error + Send + Sync>>,
    > + Send;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct InboxJobData<Tables> {
    pub inbox_event_id: InboxEventId,
    #[serde(skip)]
    pub(super) _phantom: std::marker::PhantomData<Tables>,
}

impl<Tables: MailboxTables> JobConfig for InboxJobData<Tables> {
    type Initializer = InboxJobInitializer<DummyHandler, Tables>;
}

pub(super) struct DummyHandler;
impl InboxHandler for DummyHandler {
    async fn handle(
        &self,
        _: &InboxEvent,
    ) -> Result<InboxResult, Box<dyn std::error::Error + Send + Sync>> {
        unreachable!()
    }
}

pub(super) struct InboxJobInitializer<H, Tables>
where
    H: InboxHandler,
    Tables: MailboxTables,
{
    pool: sqlx::PgPool,
    handler: Arc<H>,
    #[allow(dead_code)]
    job_type: JobType,
    #[allow(dead_code)]
    retry_settings: RetrySettings,
    _phantom: std::marker::PhantomData<Tables>,
}

impl<H, Tables> InboxJobInitializer<H, Tables>
where
    H: InboxHandler,
    Tables: MailboxTables,
{
    pub fn new(
        pool: &sqlx::PgPool,
        handler: H,
        job_type: JobType,
        retry_settings: RetrySettings,
    ) -> Self {
        Self {
            pool: pool.clone(),
            handler: Arc::new(handler),
            job_type,
            retry_settings,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<H, Tables> JobInitializer for InboxJobInitializer<H, Tables>
where
    H: InboxHandler,
    Tables: MailboxTables,
{
    fn job_type() -> JobType
    where
        Self: Sized,
    {
        JobType::new("inbox")
    }

    fn retry_on_error_settings() -> RetrySettings
    where
        Self: Sized,
    {
        RetrySettings::default()
    }

    fn init(&self, job: &Job) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        let config: InboxJobData<Tables> = job.config()?;

        Ok(Box::new(InboxJobRunner::<H, Tables> {
            pool: self.pool.clone(),
            handler: self.handler.clone(),
            inbox_event_id: config.inbox_event_id,
            _phantom: std::marker::PhantomData,
        }))
    }
}

struct InboxJobRunner<H, Tables>
where
    H: InboxHandler,
    Tables: MailboxTables,
{
    pool: sqlx::PgPool,
    handler: Arc<H>,
    inbox_event_id: InboxEventId,
    _phantom: std::marker::PhantomData<Tables>,
}

#[async_trait]
impl<H, Tables> JobRunner for InboxJobRunner<H, Tables>
where
    H: InboxHandler,
    Tables: MailboxTables,
{
    async fn run(
        &self,
        mut current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        if current_job.is_shutdown_requested() {
            return Ok(JobCompletion::RescheduleNow);
        }

        Tables::update_inbox_event_status(
            &self.pool,
            self.inbox_event_id,
            InboxEventStatus::Processing,
            None,
        )
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

        let event = Tables::find_inbox_event_by_id(&self.pool, self.inbox_event_id)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

        let result = self.handler.handle(&event).await;

        match result {
            Ok(InboxResult::Complete) => {
                Tables::update_inbox_event_status(
                    &self.pool,
                    self.inbox_event_id,
                    InboxEventStatus::Completed,
                    None,
                )
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
                Ok(JobCompletion::Complete)
            }
            Ok(InboxResult::ReprocessNow) => {
                Tables::update_inbox_event_status(
                    &self.pool,
                    self.inbox_event_id,
                    InboxEventStatus::Pending,
                    None,
                )
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
                Ok(JobCompletion::RescheduleNow)
            }
            Ok(InboxResult::ReprocessIn(duration)) => {
                Tables::update_inbox_event_status(
                    &self.pool,
                    self.inbox_event_id,
                    InboxEventStatus::Pending,
                    None,
                )
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
                Ok(JobCompletion::RescheduleIn(duration))
            }
            Err(e) => {
                Tables::update_inbox_event_status(
                    &self.pool,
                    self.inbox_event_id,
                    InboxEventStatus::Failed,
                    Some(&e.to_string()),
                )
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
                Err(e)
            }
        }
    }
}
