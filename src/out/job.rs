use async_trait::async_trait;
use futures::StreamExt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use job::{
    CurrentJob, Job, JobCompletion, JobInitializer, JobRunner, JobSpawner, JobType, RetrySettings,
};

use super::{Outbox, event::*};
use crate::{sequence::EventSequence, tables::MailboxTables};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub trait OutboxEventHandler<E>: Send + Sync + 'static {
    fn handle(
        &self,
        op: &mut es_entity::DbOp<'_>,
        event: &E,
        meta: OutboxEventMeta,
        spawner: &CommandJobSpawner,
    ) -> impl std::future::Future<Output = Result<(), BoxError>> + Send;
}

/// A type-erased container of [`JobSpawner<T>`] instances, keyed by config type.
///
/// Passed to every [`OutboxEventHandler::handle`] invocation so handlers can spawn
/// downstream command jobs without carrying spawner fields on their struct.
///
/// # Panics
///
/// [`spawn`](Self::spawn) and [`spawn_in_op`](Self::spawn_in_op) panic if the
/// config type `T` was not registered via [`add_initializer`](Self::add_initializer).
/// This is always a programming error.
#[derive(Clone, Default)]
pub struct CommandJobSpawner {
    spawners: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl CommandJobSpawner {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a [`JobInitializer`] and store its [`JobSpawner`] for later use.
    pub fn add_initializer<I>(&mut self, jobs: &mut ::job::Jobs, initializer: I)
    where
        I: JobInitializer,
        I::Config: Send + Sync + 'static,
    {
        let spawner = jobs.add_initializer(initializer);
        self.spawners
            .insert(TypeId::of::<I::Config>(), Arc::new(spawner));
    }

    /// Spawn a downstream job in its own transaction.
    ///
    /// `T` must match the `Config` type of a previously-registered initializer.
    pub async fn spawn<T>(
        &self,
        id: impl Into<::job::JobId> + std::fmt::Debug + Send,
        config: T,
    ) -> Result<Job, ::job::error::JobError>
    where
        T: Serialize + Send + Sync + 'static,
    {
        self.get_spawner::<T>().spawn(id, config).await
    }

    /// Spawn a downstream job within an existing atomic operation.
    ///
    /// `T` must match the `Config` type of a previously-registered initializer.
    pub async fn spawn_in_op<T>(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        id: impl Into<::job::JobId> + std::fmt::Debug + Send,
        config: T,
    ) -> Result<Job, ::job::error::JobError>
    where
        T: Serialize + Send + Sync + 'static,
    {
        self.get_spawner::<T>().spawn_in_op(op, id, config).await
    }

    fn get_spawner<T: 'static>(&self) -> &JobSpawner<T> {
        let type_id = TypeId::of::<T>();
        let erased = self.spawners.get(&type_id).unwrap_or_else(|| {
            panic!(
                "CommandJobSpawner: no initializer registered for config type `{}`. \
                 Did you forget to call `add_initializer` or `with_job_initializer`?",
                std::any::type_name::<T>(),
            )
        });
        erased.downcast_ref::<JobSpawner<T>>().unwrap_or_else(|| {
            unreachable!(
                "TypeId matched but downcast failed for `{}` — this is a bug",
                std::any::type_name::<T>(),
            )
        })
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

// --- Type-erased event dispatch machinery ---

/// Object-safe trait for dispatching a full outbox event enum `P` to a typed handler.
///
/// Each implementation wraps an `OutboxEventHandler<E>` for a specific inner event type `E`,
/// calling `as_event::<E>()` to filter and then forwarding to the handler.
#[async_trait]
trait DispatchEventHandler<P>: Send + Sync + 'static
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Try to dispatch the event. Returns `true` if the event matched and was handled.
    /// Each dispatch creates and commits its own `DbOp` for transaction isolation.
    async fn dispatch(
        &self,
        pool: &sqlx::PgPool,
        clock: &es_entity::clock::ClockHandle,
        event: &P,
        meta: OutboxEventMeta,
        spawner: &CommandJobSpawner,
    ) -> Result<bool, BoxError>;
}

/// Wraps an `Arc<H>` implementing `OutboxEventHandler<E>` into a `DispatchEventHandler<P>`.
struct EventDispatcher<H, E, P> {
    handler: Arc<H>,
    _phantom: std::marker::PhantomData<(E, P)>,
}

#[async_trait]
impl<H, E, P> DispatchEventHandler<P> for EventDispatcher<H, E, P>
where
    H: OutboxEventHandler<E>,
    E: Send + Sync + 'static,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin + OutboxEventMarker<E>,
{
    async fn dispatch(
        &self,
        pool: &sqlx::PgPool,
        clock: &es_entity::clock::ClockHandle,
        event: &P,
        meta: OutboxEventMeta,
        spawner: &CommandJobSpawner,
    ) -> Result<bool, BoxError> {
        if let Some(inner) = event.as_event() {
            let mut op = es_entity::DbOp::init_with_clock(pool, clock).await?;
            self.handler.handle(&mut op, inner, meta, spawner).await?;
            op.commit().await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

// --- Event handler registration builder ---

/// Builder for registering an event handler for one or more event types.
///
/// Created by [`Outbox::register_event_handler`]. Use `.with_event::<E>()` to specify
/// which event types the handler should receive, then `.register().await` to finalize.
///
/// # Examples
///
/// Single event type:
/// ```ignore
/// outbox.register_event_handler(&mut jobs, config, handler)
///     .with_event::<PingEvent>()
///     .register()
///     .await?;
/// ```
///
/// Multiple event types:
/// ```ignore
/// outbox.register_event_handler(&mut jobs, config, handler)
///     .with_event::<PingEvent>()
///     .with_event::<PongEvent>()
///     .register()
///     .await?;
/// ```
pub struct EventHandlerRegistration<'a, H, P, Tables>
where
    H: Send + Sync + 'static,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    outbox: Outbox<P, Tables>,
    jobs: &'a mut ::job::Jobs,
    config: OutboxEventJobConfig,
    handler: Arc<H>,
    dispatchers: Vec<Box<dyn DispatchEventHandler<P>>>,
    registered_events: HashSet<TypeId>,
    cmd_spawner: CommandJobSpawner,
}

impl<'a, H, P, Tables> EventHandlerRegistration<'a, H, P, Tables>
where
    H: Send + Sync + 'static,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    pub(super) fn new(
        outbox: Outbox<P, Tables>,
        jobs: &'a mut ::job::Jobs,
        config: OutboxEventJobConfig,
        handler: H,
    ) -> Self {
        Self {
            outbox,
            jobs,
            config,
            handler: Arc::new(handler),
            dispatchers: Vec::new(),
            registered_events: HashSet::new(),
            cmd_spawner: CommandJobSpawner::new(),
        }
    }

    /// Register a [`JobInitializer`] whose [`JobSpawner`] will be available
    /// to the handler via the [`CommandJobSpawner`] parameter.
    pub fn with_job_initializer<I>(mut self, initializer: I) -> Self
    where
        I: JobInitializer,
        I::Config: Send + Sync + 'static,
    {
        self.cmd_spawner.add_initializer(self.jobs, initializer);
        self
    }

    /// Register the handler for event type `E`.
    ///
    /// The handler must implement `OutboxEventHandler<E>` and the outbox event enum `P`
    /// must implement `OutboxEventMarker<E>` (generated by the `OutboxEvent` derive macro).
    /// Both constraints are enforced at compile time — calling `with_event::<E>()` will
    /// fail to compile if the handler doesn't support `E` or the enum doesn't contain it.
    ///
    /// # Panics
    ///
    /// Panics if `with_event::<E>()` is called twice for the same event type `E`.
    /// Each event type may only be registered once per handler to prevent duplicate dispatch.
    pub fn with_event<E>(mut self) -> Self
    where
        H: OutboxEventHandler<E>,
        E: Send + Sync + 'static,
        P: OutboxEventMarker<E>,
    {
        let type_id = TypeId::of::<E>();
        assert!(
            self.registered_events.insert(type_id),
            "duplicate with_event::<{}>() call — each event type may only be registered once per handler",
            std::any::type_name::<E>()
        );
        self.dispatchers.push(Box::new(EventDispatcher::<H, E, P> {
            handler: Arc::clone(&self.handler),
            _phantom: std::marker::PhantomData,
        }));
        self
    }

    /// Finalize registration, spawning a background job to process events.
    pub async fn register(self) -> Result<(), BoxError> {
        assert!(
            !self.dispatchers.is_empty(),
            "register_event_handler requires at least one .with_event::<E>() call"
        );
        let initializer = EventHandlerJobInitializer::<P, Tables> {
            outbox: self.outbox,
            dispatchers: Arc::new(self.dispatchers),
            cmd_spawner: self.cmd_spawner,
            job_type: self.config.job_type.clone(),
            retry_settings: self.config.retry_settings.clone(),
        };
        let spawner = self.jobs.add_initializer(initializer);
        spawner
            .spawn_unique(::job::JobId::new(), OutboxEventJobData::default())
            .await?;
        Ok(())
    }
}

// --- Job initializer and runner (dispatcher-based, handles any number of event types) ---

struct EventHandlerJobInitializer<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    outbox: Outbox<P, Tables>,
    dispatchers: Arc<Vec<Box<dyn DispatchEventHandler<P>>>>,
    cmd_spawner: CommandJobSpawner,
    job_type: JobType,
    retry_settings: RetrySettings,
}

impl<P, Tables> JobInitializer for EventHandlerJobInitializer<P, Tables>
where
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
        Ok(Box::new(EventHandlerJobRunner::<P, Tables> {
            outbox: self.outbox.clone(),
            dispatchers: self.dispatchers.clone(),
            cmd_spawner: self.cmd_spawner.clone(),
        }))
    }
}

struct EventHandlerJobRunner<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    outbox: Outbox<P, Tables>,
    dispatchers: Arc<Vec<Box<dyn DispatchEventHandler<P>>>>,
    cmd_spawner: CommandJobSpawner,
}

#[async_trait]
impl<P, Tables> JobRunner for EventHandlerJobRunner<P, Tables>
where
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
                        Some(OutboxEvent::Persistent(ref e)) => {
                            if let Some(payload) = e.payload.as_ref() {
                                let meta = OutboxEventMeta::from_persistent(e);
                                for dispatcher in self.dispatchers.iter() {
                                    dispatcher.dispatch(
                                        current_job.pool(),
                                        current_job.clock(),
                                        payload,
                                        meta.clone(),
                                        &self.cmd_spawner,
                                    ).await
                                        .map_err(|e| e as Box<dyn std::error::Error>)?;
                                }
                            }
                            state.sequence = e.sequence;
                            let mut op = es_entity::DbOp::init_with_clock(
                                current_job.pool(),
                                current_job.clock(),
                            )
                            .await?;
                            current_job
                                .update_execution_state_in_op(&mut op, &state)
                                .await?;
                            op.commit().await?;
                        }
                        Some(OutboxEvent::Ephemeral(ref e)) => {
                            let meta = OutboxEventMeta::from_ephemeral(e);
                            for dispatcher in self.dispatchers.iter() {
                                dispatcher
                                    .dispatch(
                                        current_job.pool(),
                                        current_job.clock(),
                                        &e.payload,
                                        meta.clone(),
                                        &self.cmd_spawner,
                                    )
                                    .await
                                    .map_err(|e| e as Box<dyn std::error::Error>)?;
                            }
                        }
                        None => return Ok(JobCompletion::RescheduleNow),
                    }
                }
            }
        }
    }
}
