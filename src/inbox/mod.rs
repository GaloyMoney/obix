mod config;
mod error;
mod event;
mod job;

use serde::Serialize;

pub use config::*;
pub use error::*;
pub use event::*;
pub use job::{InboxHandler, InboxResult};

use crate::tables::MailboxTables;

#[derive(Clone)]
pub struct Inbox<Tables = crate::tables::DefaultMailboxTables> {
    pool: sqlx::PgPool,
    jobs: job::Jobs,
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
        );

        jobs.add_initializer(initializer);

        Self {
            pool: pool.clone(),
            jobs: jobs.clone(),
            _phantom: std::marker::PhantomData,
        }
    }

    pub async fn persist_and_process<P>(
        &self,
        idempotency_key: impl Into<InboxIdempotencyKey>,
        event: P,
    ) -> Result<es_entity::Idempotent<InboxEventId>, InboxError>
    where
        P: Serialize + Send + Sync,
    {
        let mut op = self.pool.begin().await?;
        let res = self
            .persist_and_process_in_op(&mut op, idempotency_key, event)
            .await?;
        op.commit().await?;
        Ok(res)
    }

    pub async fn persist_and_process_in_op<P>(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        idempotency_key: impl Into<InboxIdempotencyKey>,
        event: P,
    ) -> Result<es_entity::Idempotent<InboxEventId>, InboxError>
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
        self.jobs.create_and_spawn_in_op(op, id, config).await?;

        Ok(es_entity::Idempotent::Executed(id))
    }

    pub async fn find_event_by_id(&self, id: InboxEventId) -> Result<InboxEvent, InboxError> {
        Tables::find_inbox_event_by_id(&self.pool, id).await
    }

    pub async fn list_failed(&self, limit: usize) -> Result<Vec<InboxEvent>, InboxError> {
        Tables::list_inbox_events_by_status(&self.pool, InboxEventStatus::Failed, limit).await
    }
}
