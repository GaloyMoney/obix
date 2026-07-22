use async_trait::async_trait;
use futures::{FutureExt, StreamExt};
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

const DEFAULT_BATCH_SIZE: usize = 100;
const DEFAULT_BATCH_FLUSH_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(100);

#[derive(Clone)]
pub struct OutboxEventJobConfig {
    pub job_type: JobType,
    pub retry_settings: RetrySettings,
    pub batch_size: usize,
    pub batch_flush_timeout: std::time::Duration,
}

impl OutboxEventJobConfig {
    pub fn new(job_type: JobType) -> Self {
        Self {
            job_type,
            retry_settings: RetrySettings::repeat_indefinitely(),
            batch_size: DEFAULT_BATCH_SIZE,
            batch_flush_timeout: DEFAULT_BATCH_FLUSH_TIMEOUT,
        }
    }

    pub fn with_retry_settings(mut self, settings: RetrySettings) -> Self {
        self.retry_settings = settings;
        self
    }

    /// Maximum number of persistent events handled per transaction /
    /// checkpoint. `1` preserves the legacy one-transaction-per-event
    /// behavior.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size.max(1);
        self
    }

    /// Upper bound on how long a partially filled batch stays open before
    /// being committed.
    pub fn with_batch_flush_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.batch_flush_timeout = timeout;
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
    batch_size: usize,
    batch_flush_timeout: std::time::Duration,
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
            batch_size: config.batch_size,
            batch_flush_timeout: config.batch_flush_timeout,
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
            batch_size: self.batch_size,
            batch_flush_timeout: self.batch_flush_timeout,
        }))
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
    batch_size: usize,
    batch_flush_timeout: std::time::Duration,
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
        let mut batch: Vec<Arc<PersistentOutboxEvent<P>>> = Vec::with_capacity(self.batch_size);

        loop {
            let first = tokio::select! {
                biased;
                _ = current_job.shutdown_requested() => {
                    return Ok(JobCompletion::RescheduleNow);
                }
                event = stream.next() => event,
            };

            match first {
                Some(OutboxEvent::Persistent(e)) => batch.push(e),
                Some(OutboxEvent::Ephemeral(e)) => {
                    self.handler
                        .handle_ephemeral(&e)
                        .await
                        .map_err(|e| e as Box<dyn std::error::Error>)?;
                    continue;
                }
                None => return Ok(JobCompletion::RescheduleNow),
            }

            // Drain ready events into the batch without blocking. Flush when
            // the batch is full, the stream goes pending (idle streams never
            // wait for a full batch), or the flush timeout caps the batch
            // lifetime under a continuous feed.
            let batch_start = tokio::time::Instant::now();
            let mut flush_reason = "batch_full";
            let mut stream_closed = false;
            while batch.len() < self.batch_size {
                if batch_start.elapsed() >= self.batch_flush_timeout {
                    flush_reason = "flush_timeout";
                    break;
                }
                match stream.next().now_or_never() {
                    Some(Some(OutboxEvent::Persistent(e))) => batch.push(e),
                    Some(Some(OutboxEvent::Ephemeral(e))) => {
                        self.handler
                            .handle_ephemeral(&e)
                            .await
                            .map_err(|e| e as Box<dyn std::error::Error>)?;
                    }
                    Some(None) => {
                        flush_reason = "stream_closed";
                        stream_closed = true;
                        break;
                    }
                    None => {
                        flush_reason = "stream_pending";
                        break;
                    }
                }
            }

            self.process_batch(&mut current_job, &mut state, &mut batch, flush_reason)
                .await?;

            if stream_closed {
                return Ok(JobCompletion::RescheduleNow);
            }
        }
    }
}

impl<H, P, Tables> OutboxEventJobRunner<H, P, Tables>
where
    H: OutboxEventHandler<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    /// Handle all buffered events and checkpoint once, in a single op. On
    /// handler error the op is dropped (rolled back); the job retry replays
    /// the whole batch from the last committed checkpoint.
    #[tracing::instrument(
        name = "outbox.process_batch",
        skip(self, current_job, state, batch),
        fields(
            batch_size = batch.len() as u64,
            first_seq = batch.first().map(|e| u64::from(e.sequence)),
            last_seq = batch.last().map(|e| u64::from(e.sequence)),
            flush_reason = flush_reason,
        ),
        err
    )]
    async fn process_batch(
        &self,
        current_job: &mut CurrentJob,
        state: &mut OutboxEventJobState,
        batch: &mut Vec<Arc<PersistentOutboxEvent<P>>>,
        flush_reason: &'static str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut op =
            es_entity::DbOp::init_with_clock(current_job.pool(), current_job.clock()).await?;
        for event in batch.drain(..) {
            self.handler
                .handle_persistent(&mut op, &event)
                .await
                .map_err(|e| e as Box<dyn std::error::Error>)?;
            state.sequence = event.sequence;
        }
        current_job
            .update_execution_state_in_op(&mut op, state)
            .await?;
        op.commit().await?;
        Ok(())
    }
}
