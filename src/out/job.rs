use async_trait::async_trait;
use futures::StreamExt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::any::TypeId;
use std::collections::HashSet;
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
    ) -> impl std::future::Future<Output = Result<(), BoxError>> + Send;
}

/// A builder context passed to the [`Outbox::register_event_handler_with`] closure.
///
/// Use [`add_initializer`](Self::add_initializer) to register downstream job
/// initializers. Each call returns a concrete, typed [`JobSpawner<I::Config>`]
/// that should be stored on the handler struct.
///
/// # Examples
///
/// ```ignore
/// outbox.register_event_handler_with(&mut jobs, config, |ctx| {
///     let fulfill = ctx.add_initializer(FulfillOrderInitializer { service: svc.clone() });
///     OnOrderPlaced { fulfill }
/// }).with_event::<OrderPlaced>()
///   .register().await?;
/// ```
pub struct EventHandlerContext<'a> {
    jobs: &'a mut ::job::Jobs,
}

impl<'a> EventHandlerContext<'a> {
    pub(super) fn new(jobs: &'a mut ::job::Jobs) -> Self {
        Self { jobs }
    }

    pub(super) fn into_jobs(self) -> &'a mut ::job::Jobs {
        self.jobs
    }

    /// Register a [`JobInitializer`] and return its [`JobSpawner`].
    ///
    /// The returned spawner should be stored on the handler struct so it can
    /// be used inside [`OutboxEventHandler::handle`] to spawn downstream jobs.
    pub fn add_initializer<I>(&mut self, initializer: I) -> JobSpawner<I::Config>
    where
        I: JobInitializer,
        I::Config: Send + Sync + 'static,
    {
        self.jobs.add_initializer(initializer)
    }
}

impl std::fmt::Debug for EventHandlerContext<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventHandlerContext")
            .finish_non_exhaustive()
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
    ) -> Result<bool, BoxError> {
        if let Some(inner) = event.as_event() {
            let mut op = es_entity::DbOp::init_with_clock(pool, clock).await?;
            self.handler.handle(&mut op, inner, meta).await?;
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
/// Created by [`Outbox::register_event_handler`] or
/// [`Outbox::register_event_handler_with`]. Use `.with_event::<E>()` to specify
/// which event types the handler should receive, then `.register().await` to finalize.
///
/// # Examples
///
/// Simple handler (no spawning):
/// ```ignore
/// outbox.register_event_handler(&mut jobs, config, handler)
///     .with_event::<PingEvent>()
///     .register()
///     .await?;
/// ```
///
/// Handler with downstream job spawning:
/// ```ignore
/// outbox.register_event_handler_with(&mut jobs, config, |ctx| {
///     let spawner = ctx.add_initializer(MyInitializer { ... });
///     MyHandler { spawner }
/// })
/// .with_event::<MyEvent>()
/// .register()
/// .await?;
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
        }
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
    #[must_use]
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
