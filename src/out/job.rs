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
pub struct EventHandlerJobConfig {
    pub job_type: JobType,
    pub retry_settings: RetrySettings,
}

impl EventHandlerJobConfig {
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
    pub fn new(outbox: Outbox<P, Tables>, handler: H, config: &EventHandlerJobConfig) -> Self {
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

pub struct EventHandlerContext<'a, P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    jobs: &'a mut ::job::Jobs,
    outbox: Outbox<P, Tables>,
}

impl<'a, P, Tables> EventHandlerContext<'a, P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    pub(super) fn new(jobs: &'a mut ::job::Jobs, outbox: Outbox<P, Tables>) -> Self {
        Self { jobs, outbox }
    }

    pub fn outbox(&self) -> &Outbox<P, Tables> {
        &self.outbox
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
    /// provides [`spawn`](CommandJobSpawner::spawn) which uses the entity ID
    /// as a queue ID for per-entity concurrency control.
    ///
    /// The command struct itself holds whatever outboxes or other dependencies
    /// it needs — construct it before calling this method.
    pub fn add_command_job<C: CommandJob>(&mut self, command: C) -> CommandJobSpawner<C::Command> {
        let initializer = CommandJobInitializer::new(command);
        let spawner = self.add_initializer(initializer);
        CommandJobSpawner::new(spawner, C::entity_id)
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
///
/// Each command job is scoped to one entity (identified by
/// [`entity_id`](Self::entity_id)). The entity ID is used as a queue ID
/// to ensure at most one job per entity runs at a time.
#[async_trait]
pub trait CommandJob: Send + Sync + 'static {
    /// The command payload type for this command job.
    type Command: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// The job type identifier for this command.
    fn job_type() -> JobType;

    /// Return the entity ID that this command targets.
    ///
    /// Used as the queue ID, ensuring at most one job per entity runs at a
    /// time.
    fn entity_id(command: &Self::Command) -> &str;

    /// Execute the command within a database transaction.
    ///
    /// The `op` is committed by the framework after `run` returns `Ok(())`.
    /// Use `outbox.publish_persisted_in_op(op, event)` to publish events
    /// atomically with the command's work.
    async fn run(
        &self,
        op: &mut es_entity::DbOp<'_>,
        command: &Self::Command,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// Auto-generated [`JobInitializer`] wrapper for a [`CommandJob`].
///
/// Created automatically by [`EventHandlerContext::add_command_job`].
pub(crate) struct CommandJobInitializer<C>
where
    C: CommandJob,
{
    command: Arc<C>,
}

impl<C> CommandJobInitializer<C>
where
    C: CommandJob,
{
    pub(crate) fn new(command: C) -> Self {
        Self {
            command: Arc::new(command),
        }
    }
}

impl<C> JobInitializer for CommandJobInitializer<C>
where
    C: CommandJob,
{
    type Config = C::Command;

    fn job_type(&self) -> JobType {
        C::job_type()
    }

    fn init(
        &self,
        job: &Job,
        _: JobSpawner<Self::Config>,
    ) -> Result<Box<dyn JobRunner>, Box<dyn std::error::Error>> {
        let command: C::Command = job.config()?;
        Ok(Box::new(CommandJobRunnerInner {
            command_job: self.command.clone(),
            command,
        }))
    }
}

struct CommandJobRunnerInner<C>
where
    C: CommandJob,
{
    command_job: Arc<C>,
    command: C::Command,
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
        let mut op =
            es_entity::DbOp::init_with_clock(current_job.pool(), current_job.clock()).await?;
        self.command_job
            .run(&mut op, &self.command)
            .await
            .map_err(|e| e as Box<dyn std::error::Error>)?;
        op.commit().await?;
        Ok(JobCompletion::Complete)
    }
}

/// A spawner for command jobs tied to a specific entity.
///
/// Wraps a [`JobSpawner`] and provides [`spawn`](Self::spawn) which spawns a
/// job within the caller's transaction, using the entity ID as the queue ID to
/// ensure at most one job per entity runs at a time.
#[derive(Clone)]
pub struct CommandJobSpawner<Command> {
    inner: JobSpawner<Command>,
    entity_id_fn: fn(&Command) -> &str,
}

impl<Command> CommandJobSpawner<Command>
where
    Command: Serialize + Send + Sync + 'static,
{
    pub fn new(inner: JobSpawner<Command>, entity_id_fn: fn(&Command) -> &str) -> Self {
        Self {
            inner,
            entity_id_fn,
        }
    }

    /// Spawn a command job within the given transaction.
    ///
    /// The entity ID (extracted from the command) is used as the queue ID,
    /// so at most one job per entity runs at a time. The spawn is part of
    /// the caller's `op` — if the op is rolled back, the job is not created.
    pub async fn spawn(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        command: Command,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let entity_id = (self.entity_id_fn)(&command);
        let queue_id = entity_id.to_string();
        self.inner
            .spawn_with_queue_id_in_op(op, job::JobId::new(), command, queue_id)
            .await?;
        Ok(())
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
