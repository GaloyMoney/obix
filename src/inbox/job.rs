use async_trait::async_trait;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::sync::Arc;

pub use job::Jobs;
use job::{
    CurrentJob, Job, JobCompletion, JobConfig, JobInitializer, JobRunner, JobType, RetrySettings,
};

use super::{InboxEvent, InboxEventId, InboxEventStatus};
use crate::tables::MailboxTables;

/// Result returned by inbox handlers
pub enum InboxResult {
    /// Event processed successfully
    Complete,
    /// Reprocess immediately (e.g., partial progress made)
    ReprocessNow,
    /// Reprocess after a delay
    ReprocessIn(std::time::Duration),
}

/// Trait for handling inbox events
pub trait InboxHandler<P>: Send + Sync + 'static
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn handle(
        &self,
        event: &InboxEvent<P>,
    ) -> impl std::future::Future<
        Output = Result<InboxResult, Box<dyn std::error::Error + Send + Sync>>,
    > + Send;
}

/// Job data stored in the job table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct InboxJobData<Tables> {
    pub inbox_event_id: InboxEventId,
    #[serde(skip)]
    pub(super) _phantom: std::marker::PhantomData<Tables>,
}

impl<Tables: MailboxTables> JobConfig for InboxJobData<Tables> {
    type Initializer = InboxJobInitializer<serde_json::Value, DummyHandler, Tables>;
}

/// Placeholder handler for JobConfig bound
pub(super) struct DummyHandler;
impl InboxHandler<serde_json::Value> for DummyHandler {
    async fn handle(
        &self,
        _: &InboxEvent<serde_json::Value>,
    ) -> Result<InboxResult, Box<dyn std::error::Error + Send + Sync>> {
        unreachable!()
    }
}

/// The job initializer registered with the Jobs service
pub(super) struct InboxJobInitializer<P, H, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
    H: InboxHandler<P>,
    Tables: MailboxTables,
{
    pool: sqlx::PgPool,
    handler: Arc<H>,
    #[allow(dead_code)]
    job_type: JobType,
    #[allow(dead_code)]
    retry_settings: RetrySettings,
    _phantom: std::marker::PhantomData<(P, Tables)>,
}

impl<P, H, Tables> InboxJobInitializer<P, H, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
    H: InboxHandler<P>,
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

impl<P, H, Tables> JobInitializer for InboxJobInitializer<P, H, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    H: InboxHandler<P>,
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

        Ok(Box::new(InboxJobRunner::<P, H, Tables> {
            pool: self.pool.clone(),
            handler: self.handler.clone(),
            inbox_event_id: config.inbox_event_id,
            _phantom: std::marker::PhantomData,
        }))
    }
}

/// The job runner that wraps the user's handler
struct InboxJobRunner<P, H, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
    H: InboxHandler<P>,
    Tables: MailboxTables,
{
    pool: sqlx::PgPool,
    handler: Arc<H>,
    inbox_event_id: InboxEventId,
    _phantom: std::marker::PhantomData<(P, Tables)>,
}

#[async_trait]
impl<P, H, Tables> JobRunner for InboxJobRunner<P, H, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    H: InboxHandler<P>,
    Tables: MailboxTables,
{
    async fn run(
        &self,
        mut current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        // Check for shutdown before doing any work
        if current_job.is_shutdown_requested() {
            return Ok(JobCompletion::RescheduleNow);
        }

        // Mark as processing
        Tables::update_inbox_event_status(
            &self.pool,
            self.inbox_event_id,
            InboxEventStatus::Processing,
            None,
        )
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

        // Load the event
        let event: InboxEvent<P> = Tables::find_inbox_event_by_id(&self.pool, self.inbox_event_id)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

        // Call the user's handler
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
                // Record the error - job crate handles retry/dead-letter
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
