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

    /// Register a [`CommandJob`] and return a [`CommandJobSpawner`] for it.
    ///
    /// This is the ergonomic way to wire up command jobs. The returned spawner
    /// provides [`spawn_for_event`](CommandJobSpawner::spawn_for_event) which
    /// derives deterministic job IDs from event IDs.
    ///
    /// The command struct itself holds whatever outboxes or other dependencies
    /// it needs — construct it before calling this method.
    pub fn add_command_job<C: CommandJob>(
        &mut self,
        command: C,
    ) -> CommandJobSpawner<C::Config> {
        let initializer = CommandJobInitializer::new(command);
        let spawner = self.add_initializer(initializer);
        CommandJobSpawner::new(spawner)
    }
}

// ---------------------------------------------------------------------------
// Command Job abstraction
// ---------------------------------------------------------------------------

/// A one-time command job spawned by event handlers.
///
/// Command jobs do atomic work within a database transaction and can
/// publish outbox events as part of that transaction. The framework
/// handles transaction lifecycle — it begins a `DbOp`, passes it to
/// `run`, commits the `DbOp`, and returns `JobCompletion::Complete`.
///
/// The command struct itself holds whatever outboxes (or other deps) it
/// needs, injected at construction time. This makes command jobs
/// outbox-agnostic — a single command can publish to multiple outboxes.
#[async_trait]
pub trait CommandJob: Send + Sync + 'static {
    /// The configuration/payload type for this command job.
    type Config: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// The job type identifier for this command.
    fn job_type() -> JobType;

    /// Execute the command within a database transaction.
    ///
    /// The `op` is committed by the framework after `run` returns `Ok(())`.
    /// Use `outbox.publish_persisted_in_op(op, event)` to publish events
    /// atomically with the command's work.
    async fn run(
        &self,
        op: &mut es_entity::DbOp<'_>,
        config: &Self::Config,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// Auto-generated [`JobInitializer`] wrapper for a [`CommandJob`].
///
/// Created automatically by [`EventHandlerContext::add_command_job`].
/// You can also construct one manually and pass it to
/// [`EventHandlerContext::add_initializer`].
pub struct CommandJobInitializer<C>
where
    C: CommandJob,
{
    command: Arc<C>,
}

impl<C> CommandJobInitializer<C>
where
    C: CommandJob,
{
    pub fn new(command: C) -> Self {
        Self {
            command: Arc::new(command),
        }
    }
}

impl<C> JobInitializer for CommandJobInitializer<C>
where
    C: CommandJob,
{
    type Config = C::Config;

    fn job_type(&self) -> JobType {
        C::job_type()
    }

    fn init(
        &self,
        job: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        let config: C::Config = job.config()?;
        Ok(Box::new(CommandJobRunnerInner {
            command: self.command.clone(),
            config,
        }))
    }
}

struct CommandJobRunnerInner<C>
where
    C: CommandJob,
{
    command: Arc<C>,
    config: C::Config,
}

#[async_trait]
impl<C> JobRunner for CommandJobRunnerInner<C>
where
    C: CommandJob,
{
    async fn run(
        &self,
        current_job: CurrentJob,
    ) -> Result<JobCompletion, Box<dyn std::error::Error>> {
        let mut op = es_entity::DbOp::init_with_clock(
            current_job.pool(),
            current_job.clock(),
        )
        .await?;
        self.command
            .run(&mut op, &self.config)
            .await
            .map_err(|e| e as Box<dyn std::error::Error>)?;
        op.commit().await?;
        Ok(JobCompletion::Complete)
    }
}

/// A spawner for command jobs that derives deterministic job IDs from events.
///
/// Wraps a [`JobSpawner`] and provides [`spawn_for_event`](Self::spawn_for_event)
/// which derives the job ID from the event ID, ensuring replay-safe spawning.
#[derive(Clone)]
pub struct CommandJobSpawner<Config> {
    inner: JobSpawner<Config>,
}

impl<Config> CommandJobSpawner<Config>
where
    Config: Serialize + Send + Sync + 'static,
{
    pub fn new(inner: JobSpawner<Config>) -> Self {
        Self { inner }
    }

    /// Spawn a command job for the given event.
    ///
    /// The job ID is deterministically derived from the event ID, ensuring
    /// that replaying the same event does not create duplicate jobs.
    ///
    /// This method is idempotent: if a job with the derived ID already exists,
    /// it returns `Ok(())`.
    pub async fn spawn_for_event<P>(
        &self,
        event: &PersistentOutboxEvent<P>,
        config: Config,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        P: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        let job_id =
            job::JobId::from(es_entity::prelude::uuid::Uuid::from(event.id));
        match self.inner.spawn(job_id, config).await {
            Ok(_) => Ok(()),
            Err(job::error::JobError::DuplicateId) => Ok(()),
            Err(e) => Err(e.into()),
        }
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
