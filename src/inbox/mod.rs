mod config;
mod error;
mod event;
mod job;

use serde::{Serialize, de::DeserializeOwned};

pub use config::*;
pub use error::*;
pub use event::*;
pub use job::{InboxHandler, InboxResult};

use crate::tables::MailboxTables;

#[derive(Clone)]
pub struct Inbox<P, Tables = crate::tables::DefaultMailboxTables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pool: sqlx::PgPool,
    jobs: job::Jobs,
    _phantom: std::marker::PhantomData<(P, Tables)>,
}

impl<P, Tables> Inbox<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    /// Initialize the inbox with a handler
    ///
    /// This registers the job initializer automatically.
    pub fn new<H>(
        pool: &sqlx::PgPool,
        jobs: &mut job::Jobs,
        config: InboxConfig,
        handler: H,
    ) -> Self
    where
        H: InboxHandler<P>,
    {
        let initializer = job::InboxJobInitializer::<P, H, Tables>::new(
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

    /// Push an event into the inbox and spawn a processing job
    pub async fn persist_and_process_in_op(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        event: impl Into<P>,
    ) -> Result<InboxEventId, InboxError> {
        let event = event.into();
        let id = Tables::insert_inbox_event(op, &event).await?;

        let config = job::InboxJobData::<Tables> {
            inbox_event_id: id,
            _phantom: std::marker::PhantomData,
        };
        self.jobs.create_and_spawn_in_op(op, id, config).await?;

        Ok(id)
    }

    /// Push with idempotency key - returns None if already exists
    pub async fn push_idempotent(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        idempotency_key: &str,
        event: impl Into<P>,
    ) -> Result<Option<InboxEventId>, InboxError> {
        let event = event.into();

        let Some(id) = Tables::insert_inbox_event_idempotent(op, idempotency_key, &event).await?
        else {
            return Ok(None);
        };

        let config = job::InboxJobData::<Tables> {
            inbox_event_id: id,
            _phantom: std::marker::PhantomData,
        };
        self.jobs.create_and_spawn_in_op(op, id, config).await?;

        Ok(Some(id))
    }

    /// Find an inbox event by ID
    pub async fn find_by_id(&self, id: InboxEventId) -> Result<InboxEvent<P>, InboxError> {
        Tables::find_inbox_event_by_id(&self.pool, id).await
    }

    /// List failed events (dead letters)
    pub async fn list_failed(&self, limit: usize) -> Result<Vec<InboxEvent<P>>, InboxError> {
        Tables::list_inbox_events_by_status(&self.pool, InboxEventStatus::Failed, limit).await
    }

    /// Retry a failed event
    pub async fn retry(&self, id: InboxEventId) -> Result<(), InboxError> {
        let mut op = es_entity::DbOp::init(&self.pool).await?;

        Tables::update_inbox_event_status(&self.pool, id, InboxEventStatus::Pending, None).await?;

        let config = job::InboxJobData::<Tables> {
            inbox_event_id: id,
            _phantom: std::marker::PhantomData,
        };
        self.jobs
            .create_and_spawn_in_op(&mut op, id, config)
            .await?;

        op.commit().await?;
        Ok(())
    }
}
