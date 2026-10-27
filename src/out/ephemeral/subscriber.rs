use async_trait::async_trait;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use job::{
    CurrentJob, Job, JobType, ResidentJobCompletion, ResidentJobInitializer, ResidentJobRunner,
    RetrySettings,
};

use super::{EphemeralOutbox, EphemeralOutboxEvent, EphemeralOutboxListener};
use crate::tables::MailboxTables;

type Listen<P, Tables> =
    Arc<dyn Fn(&EphemeralOutbox<Tables>) -> EphemeralOutboxListener<P> + Send + Sync>;

/// Handles the events of an [`EphemeralOutbox`].
///
/// `P` is whatever the registration hands the handler: the selection `S` for
/// [`EphemeralOutbox::register_subscriber`], or
/// [`RawPayload`](super::RawPayload) for
/// [`EphemeralOutbox::register_subscriber_raw`]. Either runs it as a
/// resident job.
/// There is no batch, checkpoint or execution state: the job never reads or
/// writes its execution state, and ephemeral events are not replayed.
///
/// Handed the shared [`Arc`] because an ephemeral event is broadcast to every
/// listener; there is rarely anything to retain.
pub trait EphemeralSubscriber<P>: Send + Sync + 'static
where
    P: Send + Sync + 'static,
{
    fn handle(
        &self,
        event: &Arc<EphemeralOutboxEvent<P>>,
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
            retry_settings: RetrySettings::default(),
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

pub(super) struct EphemeralSubscriberJobInitializer<H, P, Tables>
where
    H: EphemeralSubscriber<P>,
    P: Send + Sync + 'static,
    Tables: MailboxTables,
{
    outbox: EphemeralOutbox<Tables>,
    handler: Arc<H>,
    listen: Listen<P, Tables>,
    job_type: JobType,
    retry_settings: RetrySettings,
}

impl<H, P, Tables> EphemeralSubscriberJobInitializer<H, P, Tables>
where
    H: EphemeralSubscriber<P>,
    P: Send + Sync + 'static,
    Tables: MailboxTables,
{
    pub(super) fn new(
        outbox: EphemeralOutbox<Tables>,
        handler: H,
        listen: Listen<P, Tables>,
        config: &EphemeralSubscriberConfig,
    ) -> Self {
        Self {
            outbox,
            handler: Arc::new(handler),
            listen,
            job_type: config.job_type.clone(),
            retry_settings: config.retry_settings.clone(),
        }
    }
}

impl<H, P, Tables> ResidentJobInitializer for EphemeralSubscriberJobInitializer<H, P, Tables>
where
    H: EphemeralSubscriber<P>,
    P: Send + Sync + 'static,
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
        Ok(Box::new(EphemeralSubscriberJobRunner::<H, P, Tables> {
            outbox: self.outbox.clone(),
            handler: self.handler.clone(),
            listen: self.listen.clone(),
        }))
    }
}

struct EphemeralSubscriberJobRunner<H, P, Tables>
where
    H: EphemeralSubscriber<P>,
    P: Send + Sync + 'static,
    Tables: MailboxTables,
{
    outbox: EphemeralOutbox<Tables>,
    handler: Arc<H>,
    listen: Listen<P, Tables>,
}

#[async_trait]
impl<H, P, Tables> ResidentJobRunner for EphemeralSubscriberJobRunner<H, P, Tables>
where
    H: EphemeralSubscriber<P>,
    P: Send + Sync + 'static,
    Tables: MailboxTables,
{
    async fn run(
        &self,
        mut current_job: CurrentJob,
    ) -> Result<ResidentJobCompletion, Box<dyn std::error::Error + Send + Sync>> {
        let mut ephemeral = (self.listen)(&self.outbox);
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
