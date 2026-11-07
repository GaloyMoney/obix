//! The tip's runner: the shared persistent loop with the [`Tip`] landing.

use async_trait::async_trait;
use es_entity::ResultExt;
use serde::{Serialize, de::DeserializeOwned};
use std::{marker::PhantomData, sync::Arc};

use job::{
    CurrentJob, Job, JobType, ResidentJobCompletion, ResidentJobInitializer, ResidentJobRunner,
    RetrySettings,
};

use super::{JoinableConfig, JoinableSubscriber, Scope};
use crate::out::Outbox;
use crate::out::ctx::*;
use crate::out::event::{EventDelivery, UndecodableDelivery};
use crate::out::lane::InsertOrder;
use crate::out::subscription::StreamPosition;
use crate::out::subscription::singleton::PersistentDispatch;
use crate::out::subscription::singleton::{LoopCadence, OutboxEventJobData, run_persistent_loop};
use crate::sequence::EventSequence;
use crate::tables::MailboxTables;

/// The tip's `joinable_tips` row: locked at the start of every tip landing and
/// by everything that fences against the tip, and written with the tip's
/// checkpoint.
pub(super) struct TipRow<Tables> {
    pub(super) subscriber_type: JobType,
    pub(super) _tables: PhantomData<fn() -> Tables>,
}

impl<Tables: MailboxTables> CheckpointMirror for TipRow<Tables> {
    fn mirror<'a>(
        &'a self,
        op: &'a mut es_entity::DbOp<'static>,
        checkpoint: EventSequence,
    ) -> futures::future::BoxFuture<'a, Result<(), sqlx::Error>> {
        Box::pin(async move {
            Tables::mirror_tip_checkpoint_in_op(op, self.subscriber_type.as_str(), checkpoint).await
        })
    }

    /// A tip has no row to retire; the [`Tip`] landing never calls this.
    fn retire<'a>(
        &'a self,
        _op: &'a mut es_entity::DbOp<'static>,
    ) -> futures::future::BoxFuture<'a, Result<(), sqlx::Error>> {
        Box::pin(async { Ok(()) })
    }
}

impl<Tables: MailboxTables> TipLock for TipRow<Tables> {
    fn lock<'a>(
        &'a self,
        op: &'a mut es_entity::DbOp<'static>,
    ) -> futures::future::BoxFuture<'a, Result<Option<EventSequence>, sqlx::Error>> {
        Box::pin(async move { Tables::lock_tip_in_op(op, self.subscriber_type.as_str()).await })
    }
}

struct TipFlusher<S, P> {
    subscriber: Arc<S>,
    _payload: PhantomData<fn() -> P>,
}

impl<S, P> ItemFlush<S::Batch, ()> for TipFlusher<S, P>
where
    S: JoinableSubscriber<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    fn flush_items<'a>(
        &'a self,
        op: &'a mut es_entity::DbOp<'static>,
        items: S::Batch,
        state: &'a OutboxEventJobState,
    ) -> BoxFuture<'a, Result<(), HandlerError>> {
        Box::pin(async move {
            let mut op = FlushOp::<InsertOrder>::new(op, state.sequence);
            self.subscriber.flush(&mut op, Scope::Tip, items).await
        })
    }

    fn position_of(&self, state: &OutboxEventJobState) -> StreamPosition {
        StreamPosition::Insert(state.sequence)
    }
}

pub(in crate::out) struct TipJobInitializer<S, P, Tables>
where
    S: JoinableSubscriber<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    outbox: Outbox<P, Tables>,
    subscriber: Arc<S>,
    job_type: JobType,
    retry_settings: RetrySettings,
    max_batch_size: usize,
    checkpoint_interval: std::time::Duration,
}

impl<S, P, Tables> TipJobInitializer<S, P, Tables>
where
    S: JoinableSubscriber<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    pub(in crate::out) fn new(
        outbox: Outbox<P, Tables>,
        subscriber: Arc<S>,
        job_type: JobType,
        config: &JoinableConfig,
    ) -> Self {
        Self {
            outbox,
            subscriber,
            job_type,
            retry_settings: config.tip.retry_settings.clone(),
            max_batch_size: config.tip.max_batch_size,
            checkpoint_interval: config.tip.checkpoint_interval,
        }
    }
}

impl<S, P, Tables> ResidentJobInitializer for TipJobInitializer<S, P, Tables>
where
    S: JoinableSubscriber<P>,
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
    ) -> Result<Box<dyn ResidentJobRunner>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Box::new(TipJobRunner::<S, P, Tables> {
            outbox: self.outbox.clone(),
            subscriber: self.subscriber.clone(),
            job_type: self.job_type.clone(),
            max_batch_size: self.max_batch_size,
            checkpoint_interval: self.checkpoint_interval,
        }))
    }
}

struct TipJobRunner<S, P, Tables>
where
    S: JoinableSubscriber<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    outbox: Outbox<P, Tables>,
    subscriber: Arc<S>,
    job_type: JobType,
    max_batch_size: usize,
    checkpoint_interval: std::time::Duration,
}

#[async_trait]
impl<S, P, Tables> ResidentJobRunner for TipJobRunner<S, P, Tables>
where
    S: JoinableSubscriber<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    async fn run(
        &self,
        current_job: CurrentJob,
    ) -> Result<ResidentJobCompletion, Box<dyn std::error::Error + Send + Sync>> {
        let state = decode_execution_state(&current_job)?.unwrap_or_default();
        let persistent = self
            .outbox
            .listen::<InsertOrder>(state.sequence)
            .narrow_rejected()?;

        let row = TipRow::<Tables> {
            subscriber_type: self.job_type.clone(),
            _tables: PhantomData,
        };
        let mut op =
            es_entity::DbOp::init_with_clock(current_job.pool(), current_job.clock()).await?;
        row.mirror(&mut op, state.sequence).await?;
        op.commit().await?;

        let flusher = TipFlusher::<S, P> {
            subscriber: self.subscriber.clone(),
            _payload: PhantomData,
        };
        run_persistent_loop(
            self,
            current_job,
            state,
            persistent,
            &flusher,
            Tip {
                mirror: &row,
                lock: &row,
            },
            LoopCadence {
                max_batch_size: self.max_batch_size,
                checkpoint_interval: self.checkpoint_interval,
            },
        )
        .await
    }
}

impl<'m, S, P, Tables> PersistentDispatch<P, InsertOrder, Tip<'m>> for TipJobRunner<S, P, Tables>
where
    S: JoinableSubscriber<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    type Batch = S::Batch;

    async fn dispatch<'inv>(
        &self,
        parts: CtxParts<'inv, Tip<'m>>,
        batch: &'inv mut S::Batch,
        _flusher: &'inv dyn ItemFlush<S::Batch, ()>,
        event: &EventDelivery<P>,
    ) -> Result<Outcome, HandlerError> {
        let ctx = JoinableEventCtx {
            batch,
            tracker: parts.tracker,
        };
        Ok(self.subscriber.handle(ctx, event).await?.outcome)
    }

    async fn undecodable(&self, error: &UndecodableDelivery) -> Result<(), HandlerError> {
        self.subscriber.handle_undecodable(error).await
    }
}
