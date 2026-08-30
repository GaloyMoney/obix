//! The wake plane: liveness-only, event-driven wakes.
//!
//! **One waker per outbox, not per subscriber type.** It is an internal
//! singleton subscriber — built entirely on the EXISTING
//! [`SingletonSubscriber`]/[`EventCtx`]/[`FlushOp`] machinery, no fork — that
//! classifies every persistent event through every registered type's
//! [`SubscriptionDef::wake_keys`], collects the matches per batch, and at
//! flush time looks up which subscribed keys care and idempotently respawns
//! them, all on the flush op so the wakes and the waker's own checkpoint
//! commit atomically: a crash cannot checkpoint past events whose wakes were
//! lost.
//!
//! Registering one waker per type would have meant N independent full passes
//! over the persistent stream — every event read, decoded and checkpointed N
//! times — to answer a question that is one classification per type over a
//! single pass. Erasure is free here because [`SubscriptionDef`] already
//! promises `wake_keys` is synchronous, DB-free and cheap.
//!
//! Liveness-only by construction: a false-positive match costs one harmless
//! empty spawn (resolves to the live holder, or an empty lookup for an
//! unsubscribed key — the majority case). Wake keys must never gate
//! delivery, so they are never consulted by the per-key runner itself — only
//! by this waker, to decide who to wake. The periodic backstop that covers
//! a *missed* wake is [`sweep`](super::sweep).

use serde::{Serialize, de::DeserializeOwned};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use job::JobType;

use super::{KeyMsg, SubscriptionDef, WakeKey, derived_job_type};
use crate::out::ctx::{EventCtx, FlushOp, Handled};
use crate::out::event::PersistentOutboxEvent;
use crate::out::subscription::singleton::SingletonSubscriber;
use crate::tables::MailboxTables;

/// One registered keyed-subscriber type, with its [`SubscriptionDef`] erased
/// so a single waker can hold every type registered on one outbox.
///
/// Object-safe because everything the waker needs from a def is synchronous
/// and owned: `wake_keys` returns owned keys, and the spawner and subscriber
/// type are plain values. Nothing here touches the database.
pub(in crate::out) trait WakeRoute<P>: Send + Sync + 'static
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    fn subscriber_type(&self) -> &str;
    fn wake_keys(&self, event: &PersistentOutboxEvent<P>) -> Vec<WakeKey>;
    fn spawner(&self) -> &job::KeyedJobSpawner<KeyMsg>;
}

struct TypedWakeRoute<D, P>
where
    D: SubscriptionDef<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    def: Arc<D>,
    subscriber_type: JobType,
    spawner: job::KeyedJobSpawner<KeyMsg>,
    _marker: std::marker::PhantomData<fn() -> P>,
}

impl<D, P> WakeRoute<P> for TypedWakeRoute<D, P>
where
    D: SubscriptionDef<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    fn subscriber_type(&self) -> &str {
        self.subscriber_type.as_str()
    }

    fn wake_keys(&self, event: &PersistentOutboxEvent<P>) -> Vec<WakeKey> {
        self.def.wake_keys(event).into_iter().collect()
    }

    fn spawner(&self) -> &job::KeyedJobSpawner<KeyMsg> {
        &self.spawner
    }
}

/// The set of registered types the waker classifies against.
///
/// Written only by
/// [`register_keyed_subscriber`](crate::out::Outbox::register_keyed_subscriber),
/// which the API requires to be called before `Jobs::start_poll`, so by the
/// time the waker job runs this is effectively read-only and uncontended.
pub(in crate::out) type WakeRoutes<P> = Arc<RwLock<Vec<Arc<dyn WakeRoute<P>>>>>;

/// Erase one registered type into a [`WakeRoute`].
pub(in crate::out) fn wake_route<D, P>(
    def: Arc<D>,
    subscriber_type: JobType,
    spawner: job::KeyedJobSpawner<KeyMsg>,
) -> Arc<dyn WakeRoute<P>>
where
    D: SubscriptionDef<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    Arc::new(TypedWakeRoute {
        def,
        subscriber_type,
        spawner,
        _marker: std::marker::PhantomData,
    })
}

pub(in crate::out) struct WakerHandler<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    routes: WakeRoutes<P>,
    _marker: std::marker::PhantomData<fn() -> Tables>,
}

/// The waker's job type, scoped to the outbox rather than to any one
/// subscriber type: `{persistent table}.keyed-waker`, `'static`-leaked once
/// at registration (see [`derived_job_type`]). Two outboxes in one process
/// have different persistent tables and so cannot collide.
pub(in crate::out) fn waker_job_type<Tables: MailboxTables>() -> JobType {
    derived_job_type(Tables::persistent_outbox_events_table(), "keyed-waker")
}

/// Construct the waker handler — a plain [`SingletonSubscriber`], registered
/// via [`crate::out::Outbox::register_singleton_subscriber`] like any other.
pub(in crate::out) fn waker_handler<P, Tables>(routes: WakeRoutes<P>) -> WakerHandler<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    WakerHandler {
        routes,
        _marker: std::marker::PhantomData,
    }
}

impl<P, Tables> SingletonSubscriber<P> for WakerHandler<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    /// Wake keys accumulated per registered type, indexed by that type's
    /// position in [`WakeRoutes`] — an index rather than the type's name so
    /// the batch holds no strings it would only have to look up again.
    type Batch = HashMap<usize, HashSet<WakeKey>>;

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, Self::Batch>,
        event: &PersistentOutboxEvent<P>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        // Scoped so the guard cannot be held across the return: classifying
        // is synchronous by `SubscriptionDef`'s contract, and a lock guard
        // alive across an await would make this future non-`Send`.
        let matched: Vec<(usize, Vec<WakeKey>)> = {
            let routes = self.routes.read().expect("wake routes poisoned");
            routes
                .iter()
                .enumerate()
                .filter_map(|(idx, route)| {
                    let keys = route.wake_keys(event);
                    (!keys.is_empty()).then_some((idx, keys))
                })
                .collect()
        };

        if matched.is_empty() {
            return Ok(ctx.skip());
        }
        Ok(ctx.collect_with(move |batch| {
            for (idx, keys) in matched {
                batch.entry(idx).or_default().extend(keys);
            }
        }))
    }

    async fn flush(
        &self,
        op: &mut FlushOp<'_>,
        items: Self::Batch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if items.is_empty() {
            return Ok(());
        }
        // Snapshot (cheap — one `Arc` clone per registered type) so no lock
        // guard is alive across the awaits below.
        let routes: Vec<Arc<dyn WakeRoute<P>>> =
            self.routes.read().expect("wake routes poisoned").clone();

        for (idx, keys) in items {
            let Some(route) = routes.get(idx) else {
                continue;
            };
            let wake_keys: Vec<String> = keys.into_iter().map(|k| k.0).collect();
            let subscribed =
                Tables::subscription_keys_for_wake_keys(op, route.subscriber_type(), &wake_keys)
                    .await?;
            if subscribed.is_empty() {
                continue;
            }
            let specs = subscribed
                .into_iter()
                .map(|key| job::KeyedJobSpec::new(key.clone(), KeyMsg { key }))
                .collect();
            route.spawner().spawn_all_in_op(op, specs).await?;
        }
        Ok(())
    }
}
