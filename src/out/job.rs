use async_trait::async_trait;
use futures::StreamExt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::sync::Arc;

use job::{
    CurrentJob, Job, JobCompletion, JobInitializer, JobRunner, JobSpawner, JobType, RetrySettings,
};

use super::{Outbox, event::*};
use crate::{sequence::EventSequence, tables::MailboxTables};

pub trait OutboxEventHandler<P>: Send + Sync + 'static
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    fn handle_persistent(
        &self,
        op: &mut es_entity::DbOp<'_>,
        event: &PersistentOutboxEvent<P>,
    ) -> impl std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send
    {
        let _ = (op, event);
        async { Ok(()) }
    }

    fn handle_ephemeral(
        &self,
        event: &EphemeralOutboxEvent<P>,
    ) -> impl std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send
    {
        let _ = event;
        async { Ok(()) }
    }
}

#[derive(Clone)]
pub struct OutboxEventJobConfig {
    pub job_type: JobType,
    pub retry_settings: RetrySettings,
}

impl OutboxEventJobConfig {
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

#[derive(Default, Clone, Copy, Serialize, Deserialize)]
struct OutboxEventJobState {
    sequence: EventSequence,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(super) struct OutboxEventJobData {}

pub(super) struct OutboxEventJobInitializer<H, P, Tables>
where
    H: OutboxEventHandler<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    outbox: Outbox<P, Tables>,
    handler: Arc<H>,
    job_type: JobType,
    retry_settings: RetrySettings,
}

impl<H, P, Tables> OutboxEventJobInitializer<H, P, Tables>
where
    H: OutboxEventHandler<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    pub fn new(outbox: Outbox<P, Tables>, handler: H, config: &OutboxEventJobConfig) -> Self {
        Self {
            outbox,
            handler: Arc::new(handler),
            job_type: config.job_type.clone(),
            retry_settings: config.retry_settings.clone(),
        }
    }
}

impl<H, P, Tables> JobInitializer for OutboxEventJobInitializer<H, P, Tables>
where
    H: OutboxEventHandler<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    type Config = OutboxEventJobData;

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
        Ok(Box::new(OutboxEventJobRunner::<H, P, Tables> {
            outbox: self.outbox.clone(),
            handler: self.handler.clone(),
        }))
    }
}

pub struct EventHandlerContext<'a> {
    jobs: &'a mut ::job::Jobs,
}

impl<'a> EventHandlerContext<'a> {
    pub(super) fn new(jobs: &'a mut ::job::Jobs) -> Self {
        Self { jobs }
    }

    pub fn add_initializer<I>(&mut self, initializer: I) -> JobSpawner<I::Config>
    where
        I: JobInitializer,
        I::Config: Send + Sync + 'static,
    {
        self.jobs.add_initializer(initializer)
    }
}

struct OutboxEventJobRunner<H, P, Tables>
where
    H: OutboxEventHandler<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    outbox: Outbox<P, Tables>,
    handler: Arc<H>,
}

#[async_trait]
impl<H, P, Tables> JobRunner for OutboxEventJobRunner<H, P, Tables>
where
    H: OutboxEventHandler<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    async fn run(
        &self,
        mut current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        let mut state = current_job
            .execution_state::<OutboxEventJobState>()?
            .unwrap_or_default();

        let mut stream = self.outbox.listen_all(Some(state.sequence));

        loop {
            tokio::select! {
                biased;
                _ = current_job.shutdown_requested() => {
                    return Ok(JobCompletion::RescheduleNow);
                }
                event = stream.next() => {
                    match event {
                        Some(OutboxEvent::Persistent(e)) => {
                            let mut op = es_entity::DbOp::init_with_clock(
                                current_job.pool(),
                                current_job.clock(),
                            ).await?;
                            self.handler.handle_persistent(&mut op, &e).await
                                .map_err(|e| e as Box<dyn std::error::Error>)?;
                            state.sequence = e.sequence;
                            current_job.update_execution_state_in_op(&mut op, &state).await?;
                            op.commit().await?;
                        }
                        Some(OutboxEvent::Ephemeral(e)) => {
                            self.handler.handle_ephemeral(&e).await
                                .map_err(|e| e as Box<dyn std::error::Error>)?;
                        }
                        None => return Ok(JobCompletion::RescheduleNow),
                    }
                }
            }
        }
    }
}
