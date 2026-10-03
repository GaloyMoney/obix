mod config;
mod error;
mod event;
mod job;

use es_entity::errlanes::{Fail, lanes};
use es_entity::{ResultExt, clock::ClockHandle};

use crate::error::ObixFault;
use serde::Serialize;

pub use config::*;
pub use error::*;
pub use event::*;
pub use job::{InboxHandler, InboxResult};

use crate::tables::MailboxTables;

#[derive(Clone)]
pub struct Inbox<Tables = crate::tables::DefaultMailboxTables> {
    pool: sqlx::PgPool,
    spawner: job::InboxJobSpawner<Tables>,
    clock: ClockHandle,
    _phantom: std::marker::PhantomData<Tables>,
}

impl<Tables> Inbox<Tables>
where
    Tables: MailboxTables,
{
    pub fn new<H>(
        pool: &sqlx::PgPool,
        jobs: &mut job::Jobs,
        config: InboxConfig,
        handler: H,
    ) -> Self
    where
        H: InboxHandler,
    {
        let initializer = job::InboxJobInitializer::<H, Tables>::new(
            pool,
            handler,
            config.job_type.clone(),
            config.retry_settings.clone(),
            config.clock.clone(),
        );

        let spawner = jobs.add_initializer(initializer);

        Self {
            pool: pool.clone(),
            spawner,
            _phantom: std::marker::PhantomData,
            clock: config.clock.clone(),
        }
    }

    pub async fn begin_op(&self) -> Result<es_entity::DbOp<'static>, sqlx::Error> {
        es_entity::DbOp::init_with_clock(&self.pool, &self.clock).await
    }

    pub async fn persist_and_queue_job<P>(
        &self,
        idempotency_key: impl Into<InboxIdempotencyKey>,
        event: P,
    ) -> Result<es_entity::Idempotent<InboxEventId>, ObixFault>
    where
        P: Serialize + Send + Sync,
    {
        let mut op = self.begin_op().await?;
        let res = self
            .persist_and_queue_job_in_op(&mut op, idempotency_key, event)
            .await?;
        op.commit().await?;
        Ok(res)
    }

    pub async fn persist_and_queue_job_in_op<P>(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        idempotency_key: impl Into<InboxIdempotencyKey>,
        event: P,
    ) -> Result<es_entity::Idempotent<InboxEventId>, ObixFault>
    where
        P: Serialize + Send + Sync,
    {
        let idempotency_key = idempotency_key.into();

        let Some(id) = Tables::insert_inbox_event(op, &idempotency_key, &event).await? else {
            return Ok(es_entity::Idempotent::AlreadyApplied);
        };

        let config = job::InboxJobData::<Tables> {
            inbox_event_id: id,
            _phantom: std::marker::PhantomData,
        };

        self.spawner
            .spawn_in_op(op, id, config)
            .await
            .narrow_rejected()?;

        Ok(es_entity::Idempotent::Executed(id))
    }

    /// Rejects with [`InboxRejection::NotFound`] when no event has that id.
    /// The only method that can.
    pub async fn find_event_by_id(
        &self,
        id: InboxEventId,
    ) -> Result<InboxEvent, Fail<InboxRejection, lanes!(Transient, Fatal)>> {
        Tables::maybe_find_inbox_event_by_id(&self.pool, id)
            .await?
            .ok_or(Fail::Rejected(InboxRejection::NotFound(id)))
    }

    pub async fn list_failed(&self, limit: usize) -> Result<Vec<InboxEvent>, ObixFault> {
        Ok(
            Tables::list_inbox_events_by_status(&self.pool, InboxEventStatus::Failed, limit)
                .await?,
        )
    }
}
