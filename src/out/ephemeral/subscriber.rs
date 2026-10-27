use async_trait::async_trait;
use futures::StreamExt;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::sync::Arc;

use job::{
    CurrentJob, Job, JobType, ResidentJobCompletion, ResidentJobInitializer, ResidentJobRunner,
    RetrySettings,
};

use super::EphemeralOutbox;
use crate::out::event::EphemeralOutboxEvent;
use crate::tables::MailboxTables;

/// Handles the events of an [`EphemeralOutbox`].
///
/// Registered with
/// [`EphemeralOutbox::register_subscriber`], which runs it as a resident job.
/// There is no batch, checkpoint or execution state: the job never reads or
/// writes its execution state, and ephemeral events are not replayed.
///
/// Handed the shared [`Arc`] because an ephemeral event is broadcast to every
/// listener; there is rarely anything to retain.
pub trait EphemeralSubscriber<Q>: Send + Sync + 'static
where
    Q: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    fn handle(
        &self,
        event: &Arc<EphemeralOutboxEvent<Q>>,
    ) -> impl std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send;
}

/// Job identity and retry policy of an [`EphemeralSubscriber`].
#[derive(Clone)]
pub struct EphemeralSubscriberConfig {
    pub job_type: JobType,
    pub retry_settings: RetrySettings,
}

impl EphemeralSubscriberConfig {
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

/// What [`EphemeralOutbox::register_subscriber`] hands back: the job identity
/// only. There is no checkpoint, no position, no caught-up barrier — an
/// ephemeral subscription has nothing durable to read back.
#[derive(Debug, Clone)]
pub struct EphemeralSubscription {
    job_id: ::job::JobId,
}

impl EphemeralSubscription {
    pub(super) fn new(job_id: ::job::JobId) -> Self {
        Self { job_id }
    }

    pub fn job_id(&self) -> ::job::JobId {
        self.job_id
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(super) struct EphemeralSubscriberJobData {}

pub(super) struct EphemeralSubscriberJobInitializer<H, Q, Tables>
where
    H: EphemeralSubscriber<Q>,
    Q: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    outbox: EphemeralOutbox<Q, Tables>,
    handler: Arc<H>,
    job_type: JobType,
    retry_settings: RetrySettings,
}

impl<H, Q, Tables> EphemeralSubscriberJobInitializer<H, Q, Tables>
where
    H: EphemeralSubscriber<Q>,
    Q: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    pub(super) fn new(
        outbox: EphemeralOutbox<Q, Tables>,
        handler: H,
        config: &EphemeralSubscriberConfig,
    ) -> Self {
        Self {
            outbox,
            handler: Arc::new(handler),
            job_type: config.job_type.clone(),
            retry_settings: config.retry_settings.clone(),
        }
    }
}

impl<H, Q, Tables> ResidentJobInitializer for EphemeralSubscriberJobInitializer<H, Q, Tables>
where
    H: EphemeralSubscriber<Q>,
    Q: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    type Config = EphemeralSubscriberJobData;

    fn job_type(&self) -> JobType {
        self.job_type.clone()
    }

    fn retry_on_error_settings(&self) -> RetrySettings {
        self.retry_settings.clone()
    }

    fn init(
        &self,
        _job: &Job,
    ) -> Result<Box<dyn ResidentJobRunner>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Box::new(EphemeralSubscriberJobRunner::<H, Q, Tables> {
            outbox: self.outbox.clone(),
            handler: self.handler.clone(),
        }))
    }
}

struct EphemeralSubscriberJobRunner<H, Q, Tables>
where
    H: EphemeralSubscriber<Q>,
    Q: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    outbox: EphemeralOutbox<Q, Tables>,
    handler: Arc<H>,
}

#[async_trait]
impl<H, Q, Tables> ResidentJobRunner for EphemeralSubscriberJobRunner<H, Q, Tables>
where
    H: EphemeralSubscriber<Q>,
    Q: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    async fn run(
        &self,
        mut current_job: CurrentJob,
    ) -> Result<ResidentJobCompletion, Box<dyn std::error::Error + Send + Sync>> {
        let mut ephemeral = self.outbox.listen();
        loop {
            tokio::select! {
                biased;
                _ = current_job.shutdown_requested() => {
                    return Ok(ResidentJobCompletion::RescheduleNow);
                }
                event = ephemeral.next() => match event {
                    Some(event) => {
                        self.handler.handle(&event).await?;
                    }
                    None => return Ok(ResidentJobCompletion::RescheduleNow),
                },
            }
        }
    }
}
