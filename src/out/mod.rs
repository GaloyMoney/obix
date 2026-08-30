mod all_listener;
mod ctx;
mod ephemeral;
mod ephemeral_events_hook;
mod event;
mod gap_fill;
mod notifier;
mod op_cursor;
mod partition;
mod persist_events_hook;
mod persistent;
mod pg_notify;
mod post_persist_hook;
mod subscription;

use es_entity::clock::ClockHandle;
use serde::{Serialize, de::DeserializeOwned};

use std::any::TypeId;
use std::sync::Arc;

pub use self::ctx::{
    EventCtx, FlushError, FlushOp, Handled, IsolatedOp, KeyedEventCtx, StagedEvent, StagedOp,
};
pub use self::subscription::keyed::{
    KeyedSubscriber, KeyedSubscriberConfig, Members, SubscriptionDef, SubscriptionMember,
    Subscriptions, WakeKey,
};
pub use self::subscription::singleton::{
    OutboxEventJobConfig, SingletonSubscriber, StreamSelection,
};
pub use self::subscription::{
    Subscription, SubscriptionError, SubscriptionSnapshot, SubscriptionStreamStatus,
};
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
    /// Per-process gap filler — the only component that writes
    /// placeholder rows (rollback compensation, stall episodes,
    /// historical fills).
    gap_filler: gap_fill::GapFiller,
    clock: ClockHandle,
    /// Registered [`PostPersistHook`]s, shared across clones. Copy-on-write:
    /// registration swaps in a rebuilt slice, publishes snapshot it with a
    /// single `Arc` clone.
    post_persist_hooks: Arc<std::sync::RwLock<post_persist_hook::PostPersistHooks<P>>>,
    /// TypeIds of upstream outboxes' `PersistEvents` hooks that this
    /// outbox's persist hook must run after within a shared commit pass.
    /// Copy-on-write; snapshotted into each constructed hook at publish,
    /// like `post_persist_hooks`.
    persist_after: Arc<std::sync::RwLock<Arc<[TypeId]>>>,
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
            gap_filler: self.gap_filler.clone(),
            clock: self.clock.clone(),
            post_persist_hooks: self.post_persist_hooks.clone(),
            persist_after: self.persist_after.clone(),
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

        let notifier = PersistentNotifier::spawn::<Tables>(&pool, config.notify_debounce);

        // The gap-fill channel exists before either side of it: the cache
        // loop sends stall/historical requests, the GapFiller task (which
        // needs the cache's fill sender) serves them.
        let (gap_fill_tx, gap_fill_rx) = tokio::sync::mpsc::unbounded_channel();

        let persistent_cache = PersistentOutboxEventCache::init(
            &pool,
            &config,
            persistent_notification_rx,
            gap_fill_tx.clone(),
        )
        .await?;
        let ephemeral_cache =
            EphemeralOutboxEventCache::init(&pool, &config, ephemeral_notification_rx).await?;

        let gap_filler = gap_fill::GapFiller::spawn::<P, Tables>(
            &pool,
            gap_fill_rx,
            gap_fill_tx,
            persistent_cache.cache_fill_sender(),
            notifier.report_sender(),
            &config,
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
            gap_filler,
            clock: config.clock.clone(),
            post_persist_hooks: Arc::new(std::sync::RwLock::new(Vec::new().into())),
            persist_after: Arc::new(std::sync::RwLock::new(Vec::new().into())),
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

    /// Declare that this outbox's persist commit hook must run after
    /// `upstream`'s whenever both are registered on the same operation.
    ///
    /// Use when a [`PostPersistHook`] on `upstream` republishes into this
    /// outbox: with the ordering declared, the repost merges into this
    /// outbox's still-pending commit hook — one INSERT batch, one notify —
    /// instead of appending a fresh re-entrant generation whenever this
    /// outbox happened to publish earlier in the operation. Without a
    /// shared operation, or when `upstream` never publishes in it, this
    /// declaration has no effect.
    ///
    /// `upstream` is used only to name its concrete hook type; nothing is
    /// stored from it. Call at startup next to the hook registration —
    /// snapshot-at-publish semantics, same as
    /// [`add_post_persist_hook`](Self::add_post_persist_hook). Idempotent.
    ///
    /// Declaring a cycle (A after B and B after A, directly or
    /// transitively) is an error caught at commit time: a pass containing
    /// the cycle fails loudly with a protocol error and rolls back.
    pub fn persist_after<P2, Tables2>(&self, _upstream: &Outbox<P2, Tables2>)
    where
        P2: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
        Tables2: MailboxTables,
    {
        let dep = TypeId::of::<persist_events_hook::PersistEvents<P2, Tables2>>();
        let mut deps = self
            .persist_after
            .write()
            .expect("persist_after lock poisoned");
        if deps.contains(&dep) {
            return;
        }
        let mut rebuilt: Vec<TypeId> = deps.iter().copied().collect();
        rebuilt.push(dep);
        *deps = rebuilt.into();
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
        let persist_after = self
            .persist_after
            .read()
            .expect("persist_after lock poisoned")
            .clone();
        let hook = persist_events_hook::PersistEvents::<P, Tables>::new(
            self.persistent_cache.cache_fill_sender(),
            self.notifier.report_sender(),
            self.gap_filler.report_sender(),
            events,
            self.persist_events_batch_size,
            post_persist_hooks,
            persist_after,
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

    /// Register `handler` as a resident job consuming this outbox — a
    /// singleton subscriber: one instance, permanent, subscribed from
    /// registration (its subscription is implicit).
    ///
    /// Returns a [`Subscription`]: the handler's committed checkpoint, its
    /// position against the stream frontier, and the
    /// [`await_caught_up`](Subscription::await_caught_up) barrier. Callers
    /// that only want the handler running can discard it with `.await?;`.
    ///
    /// Registration is idempotent per job type: registering the same job type
    /// twice resolves to the already-persisted job, so both calls hand back
    /// handles with the same [`job_id`](Subscription::job_id).
    pub async fn register_singleton_subscriber<H>(
        &self,
        jobs: &mut ::job::Jobs,
        config: OutboxEventJobConfig,
        handler: H,
    ) -> Result<Subscription<P, Tables>, Box<dyn std::error::Error + Send + Sync>>
    where
        H: SingletonSubscriber<P>,
    {
        let initializer = subscription::singleton::OutboxEventJobInitializer::<H, P, Tables>::new(
            self.clone(),
            handler,
            &config,
        );
        let spawner = jobs.add_resident_initializer(initializer);
        let handle = spawner
            .spawn(subscription::singleton::OutboxEventJobData::default())
            .await?;
        Ok(Subscription::new(handle, self.pool.clone()))
    }

    /// Register a keyed subscriber type: per-entity consumers, created and
    /// destroyed transactionally with the entity via the returned
    /// [`Subscriptions`] capability's `subscribe_in_op`/`cancel_in_op`, each
    /// with its own durable cursor, costing nothing while idle.
    ///
    /// A passivated member is revived by the wake plane: the waker matches
    /// each event's [`wake_keys`](subscription::keyed::SubscriptionDef::wake_keys)
    /// against the sets subscriptions declared, and a periodic sweep backstops
    /// it (`config.sweep_interval`). A fresh subscription is Active from birth
    /// regardless, so neither is on the critical path for first delivery —
    /// they only govern re-wake latency after a member has gone Dormant.
    ///
    /// Must be called **before** [`::job::Jobs::start_poll`].
    pub async fn register_keyed_subscriber<D: subscription::keyed::SubscriptionDef<P>>(
        &self,
        jobs: &mut ::job::Jobs,
        config: subscription::keyed::KeyedSubscriberConfig,
        def: D,
    ) -> Result<Subscriptions<D, P, Tables>, Box<dyn std::error::Error + Send + Sync>> {
        let def = Arc::new(def);
        let initializer = subscription::keyed::KeyedSubscriberJobInitializer::<D, P, Tables>::new(
            self.clone(),
            def.clone(),
            &config,
        );
        let spawner = jobs.add_keyed_initializer(initializer);

        // Wake plane: the waker (event-driven, liveness-only wakes) is a
        // singleton subscriber built entirely on the existing
        // register_singleton_subscriber machinery, plus a periodic sweep
        // (startup reconcile / repair / staleness bound) as its own resident
        // job. A fresh subscription is Active from birth regardless, so
        // neither is on the critical path for first delivery.
        let waker = subscription::keyed::waker_handler::<D, P, Tables>(
            def,
            config.job_type.clone(),
            spawner.clone(),
        );
        self.register_singleton_subscriber(
            jobs,
            OutboxEventJobConfig::new(subscription::keyed::waker_job_type(
                config.job_type.as_str(),
            )),
            waker,
        )
        .await?;

        let sweep = subscription::keyed::SweepJobInitializer::<Tables>::new(
            self.pool.clone(),
            spawner.clone(),
            &config,
        );
        jobs.add_resident_initializer(sweep)
            .spawn(subscription::keyed::SweepJobData::default())
            .await?;

        Ok(Subscriptions::new(
            self.pool.clone(),
            self.clock.clone(),
            config.job_type,
            jobs.clone(),
            spawner,
        ))
    }

    /// The highest sequence the persistent outbox has handed out — the
    /// stream frontier.
    ///
    /// Read from the sequence generator's `last_value`, so it includes
    /// sequences already assigned to transactions that have not committed
    /// yet, and needs no table scan. This is the same value
    /// [`SubscriptionSnapshot::stream_status`] compares a handler's checkpoint
    /// against.
    pub async fn highest_known_persistent_sequence(&self) -> Result<EventSequence, sqlx::Error> {
        subscription::read_frontier::<Tables>(&self.pool).await
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
        let spawner = jobs.add_resident_initializer(initializer);
        spawner
            .spawn(partition::PartitionMaintainerJobData::default())
            .await?;
        Ok(())
    }
}
