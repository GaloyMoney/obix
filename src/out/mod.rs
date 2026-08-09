mod all_listener;
mod compensator;
mod ctx;
mod ephemeral;
mod ephemeral_events_hook;
mod event;
mod job;
mod notifier;
mod op_cursor;
mod partition;
mod persist_events_hook;
mod persistent;
mod pg_notify;
mod post_persist_hook;

use es_entity::clock::ClockHandle;
use serde::{Serialize, de::DeserializeOwned};

use std::sync::Arc;

pub use self::ctx::{BatchOp, EventCtx, FlushError, FlushOp, Handled, IsolatedOp};
pub use self::job::{EventSubscription, OutboxEventHandler, OutboxEventJobConfig};
use crate::{config::*, handle::OwnedTaskHandle, sequence::EventSequence, tables::*};
pub use all_listener::AllOutboxListener;
use ephemeral::EphemeralOutboxEventCache;
pub use ephemeral::EphemeralOutboxListener;
pub use event::*;
use notifier::PersistentNotifier;
pub use op_cursor::{CursorError, OpCursor};
pub use partition::{PartitionMaintainerConfig, Partitions};
use persistent::PersistentOutboxEventCache;
pub use persistent::PersistentOutboxListener;
pub use post_persist_hook::PostPersistHook;

#[allow(dead_code)]
pub struct Outbox<P, Tables = DefaultMailboxTables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pool: sqlx::PgPool,
    event_buffer_size: usize,
    persist_events_batch_size: usize,
    partition_premake: u64,
    partition_maintainer_interval: std::time::Duration,
    persistent_cache: Arc<PersistentOutboxEventCache<P, Tables>>,
    ephemeral_cache: Arc<EphemeralOutboxEventCache<P, Tables>>,
    _pg_listener_handle: Arc<OwnedTaskHandle>,
    /// Per-process debounced NOTIFY emitter.
    notifier: PersistentNotifier,
    /// Per-process placeholder filler for this process's own abandoned
    /// (allocated-but-not-committed) sequences.
    compensator: compensator::AbandonedCompensator,
    clock: ClockHandle,
    /// Registered [`PostPersistHook`]s, shared across clones. Copy-on-write:
    /// registration swaps in a rebuilt slice, publishes snapshot it with a
    /// single `Arc` clone.
    post_persist_hooks: Arc<std::sync::RwLock<post_persist_hook::PostPersistHooks<P>>>,
}

impl<P, Tables> std::fmt::Debug for Outbox<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Outbox")
            .field("event_buffer_size", &self.event_buffer_size)
            .field("persist_events_batch_size", &self.persist_events_batch_size)
            .finish_non_exhaustive()
    }
}

impl<P, Tables> Clone for Outbox<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
    Tables: MailboxTables,
{
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            event_buffer_size: self.event_buffer_size,
            persist_events_batch_size: self.persist_events_batch_size,
            partition_premake: self.partition_premake,
            partition_maintainer_interval: self.partition_maintainer_interval,
            persistent_cache: self.persistent_cache.clone(),
            ephemeral_cache: self.ephemeral_cache.clone(),
            _pg_listener_handle: self._pg_listener_handle.clone(),
            notifier: self.notifier.clone(),
            compensator: self.compensator.clone(),
            clock: self.clock.clone(),
            post_persist_hooks: self.post_persist_hooks.clone(),
        }
    }
}

impl<P, Tables> Outbox<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    pub async fn init(pool: &sqlx::PgPool, config: MailboxConfig) -> Result<Self, sqlx::Error> {
        let pool = pool.clone();

        let (persistent_notification_tx, persistent_notification_rx) =
            tokio::sync::mpsc::channel(config.event_buffer_size);
        let (ephemeral_notification_tx, ephemeral_notification_rx) =
            tokio::sync::mpsc::channel(config.event_buffer_size);
        let pg_listener_handle = pg_notify::spawn_pg_listener::<Tables>(
            &pool,
            persistent_notification_tx,
            ephemeral_notification_tx,
        )
        .await?;

        // Spawned before the cache: backstop fills report the ranges they
        // placeholder-fill through the notifier so other processes resume
        // reactively instead of waiting out their own retry reads.
        let notifier = PersistentNotifier::spawn::<Tables>(&pool, config.notify_debounce);

        let persistent_cache = PersistentOutboxEventCache::init(
            &pool,
            &config,
            persistent_notification_rx,
            notifier.report_sender(),
        )
        .await?;
        let ephemeral_cache =
            EphemeralOutboxEventCache::init(&pool, &config, ephemeral_notification_rx).await?;

        let compensator = compensator::AbandonedCompensator::spawn::<P, Tables>(
            &pool,
            persistent_cache.cache_fill_sender(),
            notifier.report_sender(),
        );

        Ok(Self {
            pool,
            event_buffer_size: config.event_buffer_size,
            persist_events_batch_size: config.persist_events_batch_size,
            partition_premake: config.partition_premake,
            partition_maintainer_interval: config.partition_maintainer_interval,
            persistent_cache: Arc::new(persistent_cache),
            ephemeral_cache: Arc::new(ephemeral_cache),
            _pg_listener_handle: Arc::new(pg_listener_handle),
            notifier,
            compensator,
            clock: config.clock.clone(),
            post_persist_hooks: Arc::new(std::sync::RwLock::new(Vec::new().into())),
        })
    }

    /// Register an in-transaction callback invoked whenever this outbox's
    /// events are persisted — after the INSERT (sequences assigned), before
    /// the surrounding operation commits. See [`PostPersistHook`] for the
    /// full contract.
    ///
    /// Takes `&self`: consumers typically hold shared references to a
    /// long-lived outbox. Register at startup, before serving traffic —
    /// hooks are snapshotted when a publish first constructs its commit
    /// hook, so late registration affects only subsequent operations.
    pub fn add_post_persist_hook(&self, hook: impl PostPersistHook<P>) {
        let mut hooks = self
            .post_persist_hooks
            .write()
            .expect("post_persist_hooks lock poisoned");
        let mut rebuilt: Vec<Arc<dyn PostPersistHook<P>>> = hooks.iter().cloned().collect();
        rebuilt.push(Arc::new(hook));
        *hooks = rebuilt.into();
    }

    pub async fn begin_op(&self) -> Result<es_entity::DbOp<'static>, sqlx::Error> {
        es_entity::DbOp::init_with_clock(&self.pool, &self.clock).await
    }

    pub async fn publish_persisted_in_op(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        event: impl Into<P>,
    ) -> Result<(), sqlx::Error> {
        self.publish_all_persisted(op, std::iter::once(event)).await
    }

    pub async fn publish_all_persisted(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        events: impl IntoIterator<Item = impl Into<P>>,
    ) -> Result<(), sqlx::Error> {
        let post_persist_hooks = self
            .post_persist_hooks
            .read()
            .expect("post_persist_hooks lock poisoned")
            .clone();
        let hook = persist_events_hook::PersistEvents::<P, Tables>::new(
            self.persistent_cache.cache_fill_sender(),
            self.notifier.report_sender(),
            self.compensator.report_sender(),
            events,
            self.persist_events_batch_size,
            post_persist_hooks,
        );
        if let Err(hook) = op.add_commit_hook(hook) {
            use es_entity::hooks::CommitHook;
            hook.with_in_tx_notify()
                .force_execute_pre_commit(op)
                .await?;
        }
        Ok(())
    }

    pub async fn publish_ephemeral(
        &self,
        event_type: EphemeralEventType,
        event: impl Into<P>,
    ) -> Result<(), sqlx::Error> {
        let now = self.clock.manual_now();
        let event =
            Tables::persist_ephemeral_event(&self.pool, now, event_type, event.into()).await?;
        let _ = self
            .ephemeral_cache
            .cache_fill_sender()
            .send(Arc::new(event));
        Ok(())
    }

    pub async fn publish_ephemeral_in_op(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        event_type: EphemeralEventType,
        event: impl Into<P>,
    ) -> Result<(), sqlx::Error> {
        let hook = ephemeral_events_hook::PersistEphemeralEvents::<P, Tables>::new(
            self.ephemeral_cache.cache_fill_sender().clone(),
            event_type,
            event,
        );
        if let Err(hook) = op.add_commit_hook(hook) {
            use es_entity::hooks::CommitHook;
            hook.force_execute_pre_commit(op).await?;
        }
        Ok(())
    }

    /// Mark the current publish position of this outbox within `op`.
    ///
    /// Subsequent [`take_published_since`](Self::take_published_since) /
    /// [`map_published_since`](Self::map_published_since) reads with the
    /// returned cursor see only events published (buffered on the op's commit
    /// hook) after this call — e.g. the events generated by a specific
    /// downstream call that publishes onto the same op. See [`OpCursor`] for
    /// semantics and caveats.
    ///
    /// Fails with [`CursorError::HooksUnsupported`] if `op` does not support
    /// commit hooks (e.g. a bare `sqlx::Transaction`, which persists publishes
    /// immediately with no op-local buffer to position into) — rather than
    /// returning a cursor that would silently yield nothing. Support is checked
    /// via [`AtomicOperation::supports_hooks`](es_entity::AtomicOperation::supports_hooks),
    /// which is unambiguous where [`commit_hook`](es_entity::AtomicOperation::commit_hook)
    /// returning `None` is not (unsupported vs. supported-but-nothing-published-yet).
    pub fn cursor(
        &self,
        op: &impl es_entity::AtomicOperation,
    ) -> Result<OpCursor<P, Tables>, CursorError> {
        if !op.supports_hooks() {
            return Err(CursorError::HooksUnsupported);
        }
        let pos = op
            .commit_hook::<persist_events_hook::PersistEvents<P, Tables>>()
            .map(|hook| hook.pending().len())
            .unwrap_or(0);
        Ok(OpCursor::new(pos))
    }

    /// Events published to this outbox within `op` since `cursor`, as a
    /// borrowed slice; advances the cursor to the end of the buffer.
    ///
    /// The advance is visible at the call site through the `&mut` borrow. The
    /// returned slice borrows `op`, so nothing can publish while it is held —
    /// the snapshot is stable. Events are pre-persist payloads: they are only
    /// durable once `op.commit()` succeeds.
    pub fn take_published_since<'op>(
        &self,
        op: &'op impl es_entity::AtomicOperation,
        cursor: &mut OpCursor<P, Tables>,
    ) -> &'op [P] {
        let events: &'op [P] = op
            .commit_hook::<persist_events_hook::PersistEvents<P, Tables>>()
            .map(|hook| hook.pending())
            .unwrap_or(&[]);
        let start = cursor.pos.min(events.len());
        cursor.pos = events.len();
        &events[start..]
    }

    /// Filter-map the events published since `cursor` into owned values;
    /// advances the cursor.
    ///
    /// Convenience over [`take_published_since`](Self::take_published_since)
    /// for the republish use case: map this outbox's freshly published events
    /// into another outbox's payload type and publish them onto the same op —
    /// atomically, in the same transaction.
    pub fn map_published_since<T>(
        &self,
        op: &impl es_entity::AtomicOperation,
        cursor: &mut OpCursor<P, Tables>,
        f: impl FnMut(&P) -> Option<T>,
    ) -> Vec<T> {
        self.take_published_since(op, cursor)
            .iter()
            .filter_map(f)
            .collect()
    }

    /// Non-advancing read of the events published since `cursor`.
    ///
    /// Takes `&OpCursor` (shared) — the borrow mutability is what
    /// distinguishes peek from consume.
    pub fn peek_published_since<'op>(
        &self,
        op: &'op impl es_entity::AtomicOperation,
        cursor: &OpCursor<P, Tables>,
    ) -> &'op [P] {
        let events: &'op [P] = op
            .commit_hook::<persist_events_hook::PersistEvents<P, Tables>>()
            .map(|hook| hook.pending())
            .unwrap_or(&[]);
        &events[cursor.pos.min(events.len())..]
    }

    pub fn listen_persisted(
        &self,
        start_after: impl Into<Option<EventSequence>>,
    ) -> PersistentOutboxListener<P> {
        PersistentOutboxListener::new(
            self.persistent_cache.handle(),
            start_after,
            self.event_buffer_size,
        )
    }

    pub fn listen_ephemeral(&self) -> EphemeralOutboxListener<P> {
        EphemeralOutboxListener::new(self.ephemeral_cache.handle())
    }

    pub fn listen_all(
        &self,
        start_after: impl Into<Option<EventSequence>>,
    ) -> AllOutboxListener<P> {
        all_listener::AllOutboxListener::new(
            self.persistent_cache.handle(),
            self.ephemeral_cache.handle(),
            start_after,
            self.event_buffer_size,
        )
    }

    pub async fn register_event_handler<H>(
        &self,
        jobs: &mut ::job::Jobs,
        config: OutboxEventJobConfig,
        handler: H,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        H: OutboxEventHandler<P>,
    {
        let initializer =
            job::OutboxEventJobInitializer::<H, P, Tables>::new(self.clone(), handler, &config);
        let spawner = jobs.add_initializer(initializer);
        spawner
            .spawn_unique(::job::JobId::new(), job::OutboxEventJobData::default())
            .await?;
        Ok(())
    }

    /// Register the partition maintainer for `persistent_outbox_events`: a
    /// timer-scheduled job that pre-creates RANGE partitions ahead of the
    /// sequence head so inserts always route into an explicit partition rather
    /// than the `DEFAULT` backstop.
    ///
    /// A consumer that applies the partitioned migration but never registers
    /// the maintainer will, over time, pile everything into `DEFAULT` — still
    /// correct (reads see those rows), but it forfeits the per-partition
    /// vacuum/locality wins and eventually needs a
    /// [`Partitions::recover_default`] repair. **Register this at startup.**
    ///
    /// Runs one premake pass **synchronously before returning** (so traffic
    /// that starts immediately still routes into an explicit partition), then
    /// spawns the recurring job. Premake margin / poll interval come from the
    /// [`MailboxConfig`] this outbox was initialised with; partition width is
    /// the fixed [`DEFAULT_PARTITION_WIDTH`] constant (coupled to the
    /// migration's initial partition).
    pub async fn register_partition_maintainer(
        &self,
        jobs: &mut ::job::Jobs,
        config: PartitionMaintainerConfig,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let partitions = Partitions::<Tables>::new(&self.pool, self.partition_premake);

        // The synchronous write path must never wait on the async maintainer,
        // so premake covering the head BEFORE registering the job.
        partitions.ensure().await?;

        let initializer = partition::PartitionMaintainerJobInitializer::<Tables>::new(
            partitions,
            &config,
            self.partition_maintainer_interval,
        );
        let spawner = jobs.add_initializer(initializer);
        spawner
            .spawn_unique(
                ::job::JobId::new(),
                partition::PartitionMaintainerJobData::default(),
            )
            .await?;
        Ok(())
    }
}
