use async_trait::async_trait;
use futures::StreamExt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
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

pub(super) struct OutboxEventJobInitializer<H, E, P, Tables>
where
    H: OutboxEventHandler<E>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin + OutboxEventMarker<E>,
    Tables: MailboxTables,
{
    outbox: Outbox<P, Tables>,
    handler: Arc<H>,
    job_type: JobType,
    retry_settings: RetrySettings,
    _phantom: std::marker::PhantomData<E>,
}

impl<H, E, P, Tables> OutboxEventJobInitializer<H, E, P, Tables>
where
    H: OutboxEventHandler<E>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin + OutboxEventMarker<E>,
    Tables: MailboxTables,
{
    pub fn new(outbox: Outbox<P, Tables>, handler: H, config: &OutboxEventJobConfig) -> Self {
        Self {
            outbox,
            handler: Arc::new(handler),
            job_type: config.job_type.clone(),
            retry_settings: config.retry_settings.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<H, E, P, Tables> JobInitializer for OutboxEventJobInitializer<H, E, P, Tables>
where
    H: OutboxEventHandler<E>,
    E: Send + Sync + 'static,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin + OutboxEventMarker<E>,
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
        Ok(Box::new(OutboxEventJobRunner::<H, E, P, Tables> {
            outbox: self.outbox.clone(),
            handler: self.handler.clone(),
            _phantom: std::marker::PhantomData,
        }))
    }
}

struct OutboxEventJobRunner<H, E, P, Tables>
where
    H: OutboxEventHandler<E>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin + OutboxEventMarker<E>,
    Tables: MailboxTables,
{
    outbox: Outbox<P, Tables>,
    handler: Arc<H>,
    _phantom: std::marker::PhantomData<E>,
}

#[async_trait]
impl<H, E, P, Tables> JobRunner for OutboxEventJobRunner<H, E, P, Tables>
where
    H: OutboxEventHandler<E>,
    E: Send + Sync + 'static,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin + OutboxEventMarker<E>,
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
                            if let Some(inner) = e.payload.as_ref().and_then(|p| p.as_event()) {
                                let meta = OutboxEventMeta::from_persistent(e);
                                let mut op = es_entity::DbOp::init_with_clock(
                                    current_job.pool(),
                                    current_job.clock(),
                                ).await?;
                                self.handler.handle(&mut op, inner, meta).await
                                    .map_err(|e| e as Box<dyn std::error::Error>)?;
                                op.commit().await?;
                            }
                            state.sequence = e.sequence;
                            let mut op = es_entity::DbOp::init_with_clock(
                                current_job.pool(),
                                current_job.clock(),
                            ).await?;
                            current_job.update_execution_state_in_op(&mut op, &state).await?;
                            op.commit().await?;
                        }
                        Some(OutboxEvent::Ephemeral(ref e)) => {
                            if let Some(inner) = e.payload.as_event() {
                                let meta = OutboxEventMeta::from_ephemeral(e);
                                let mut op = es_entity::DbOp::init_with_clock(
                                    current_job.pool(),
                                    current_job.clock(),
                                ).await?;
                                self.handler.handle(&mut op, inner, meta).await
                                    .map_err(|e| e as Box<dyn std::error::Error>)?;
                                op.commit().await?;
                            }
                        }
                        None => return Ok(JobCompletion::RescheduleNow),
                    }
                }
            }
        }
    }
}

// --- Multi-event handler support ---

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

/// Builder for registering a single handler struct that handles multiple event types.
///
/// Collects type-erased dispatchers for each event type, then produces a single background
/// job that processes the full event stream and routes to the appropriate `handle` method.
///
/// # Example
///
/// ```ignore
/// OutboxMultiEventHandler::new(my_handler)
///     .with_event::<PingEvent>()
///     .with_event::<PongEvent>()
/// ```
pub struct OutboxMultiEventHandler<H, P> {
    handler: Arc<H>,
    dispatchers: Vec<Box<dyn DispatchEventHandler<P>>>,
}

impl<H, P> OutboxMultiEventHandler<H, P>
where
    H: Send + Sync + 'static,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    pub fn new(handler: H) -> Self {
        Self {
            handler: Arc::new(handler),
            dispatchers: Vec::new(),
        }
    }

    /// Register the handler for an additional event type `E`.
    ///
    /// The handler must implement `OutboxEventHandler<E>` and `P` must implement
    /// `OutboxEventMarker<E>` (generated by the `OutboxEvent` derive macro).
    pub fn with_event<E>(mut self) -> Self
    where
        H: OutboxEventHandler<E>,
        E: Send + Sync + 'static,
        P: OutboxEventMarker<E>,
    {
        self.dispatchers.push(Box::new(EventDispatcher::<H, E, P> {
            handler: Arc::clone(&self.handler),
            _phantom: std::marker::PhantomData,
        }));
        self
    }
}

pub(super) struct MultiEventJobInitializer<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    outbox: Outbox<P, Tables>,
    dispatchers: Arc<Vec<Box<dyn DispatchEventHandler<P>>>>,
    job_type: JobType,
    retry_settings: RetrySettings,
}

impl<P, Tables> MultiEventJobInitializer<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    pub fn new<H>(
        outbox: Outbox<P, Tables>,
        multi_handler: OutboxMultiEventHandler<H, P>,
        config: &OutboxEventJobConfig,
    ) -> Self
    where
        H: Send + Sync + 'static,
    {
        Self {
            outbox,
            dispatchers: Arc::new(multi_handler.dispatchers),
            job_type: config.job_type.clone(),
            retry_settings: config.retry_settings.clone(),
        }
    }
}

impl<P, Tables> JobInitializer for MultiEventJobInitializer<P, Tables>
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
        Ok(Box::new(MultiEventJobRunner::<P, Tables> {
            outbox: self.outbox.clone(),
            dispatchers: self.dispatchers.clone(),
        }))
    }
}

struct MultiEventJobRunner<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    outbox: Outbox<P, Tables>,
    dispatchers: Arc<Vec<Box<dyn DispatchEventHandler<P>>>>,
}

#[async_trait]
impl<P, Tables> JobRunner for MultiEventJobRunner<P, Tables>
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
                                    if dispatcher.dispatch(
                                        current_job.pool(),
                                        current_job.clock(),
                                        payload,
                                        meta.clone(),
                                    ).await
                                        .map_err(|e| e as Box<dyn std::error::Error>)? {
                                        break;
                                    }
                                }
                            }
                            state.sequence = e.sequence;
                            let mut op = es_entity::DbOp::init_with_clock(
                                current_job.pool(),
                                current_job.clock(),
                            ).await?;
                            current_job.update_execution_state_in_op(&mut op, &state).await?;
                            op.commit().await?;
                        }
                        Some(OutboxEvent::Ephemeral(ref e)) => {
                            let meta = OutboxEventMeta::from_ephemeral(e);
                            for dispatcher in self.dispatchers.iter() {
                                if dispatcher
                                    .dispatch(
                                        current_job.pool(),
                                        current_job.clock(),
                                        &e.payload,
                                        meta.clone(),
                                    )
                                    .await
                                    .map_err(|e| e as Box<dyn std::error::Error>)?
                                {
                                    break;
                                }
                            }
                        }
                        None => return Ok(JobCompletion::RescheduleNow),
                    }
                }
            }
        }
    }
}
