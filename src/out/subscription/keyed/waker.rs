//! The wake plane: liveness-only, event-driven wakes.
//!
//! An internal singleton subscriber — built entirely on the EXISTING
//! [`SingletonSubscriber`]/[`EventCtx`]/[`FlushOp`] machinery, no fork — that
//! classifies every persistent event into wake keys, collects them per
//! batch, and at flush time looks up which subscribed keys care and
//! idempotently respawns them, all on the flush op so the wakes and the
//! waker's own checkpoint commit atomically: a crash cannot checkpoint past
//! events whose wakes were lost.
//!
//! Liveness-only by construction: a false-positive match costs one harmless
//! empty spawn (resolves to the live holder, or an empty lookup for an
//! unsubscribed key — the majority case). Wake keys must never gate
//! delivery, so they are never consulted by the per-key runner itself — only
//! by this waker, to decide who to wake. The periodic backstop that covers
//! a *missed* wake is [`sweep`](super::sweep).

use serde::{Serialize, de::DeserializeOwned};
use std::{marker::PhantomData, sync::Arc};

use job::JobType;

use super::{KeyMsg, SubscriptionDef, WakeKey, derived_job_type};
use crate::out::ctx::{EventCtx, FlushOp, Handled};
use crate::out::event::PersistentOutboxEvent;
use crate::out::subscription::singleton::SingletonSubscriber;
use crate::tables::MailboxTables;

pub(in crate::out) struct WakerHandler<D, P, Tables>
where
    D: SubscriptionDef<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    def: Arc<D>,
    subscriber_type: JobType,
    spawner: job::KeyedJobSpawner<KeyMsg>,
    _marker: PhantomData<fn() -> (P, Tables)>,
}

/// Build the waker's job type: `{base}.waker`, `'static`-leaked once at
/// registration (see [`derived_job_type`]).
pub(in crate::out) fn waker_job_type(base: &str) -> JobType {
    derived_job_type(base, "waker")
}

/// Construct the waker handler — a plain [`SingletonSubscriber`], registered
/// via [`crate::out::Outbox::register_singleton_subscriber`] like any other.
pub(in crate::out) fn waker_handler<D, P, Tables>(
    def: Arc<D>,
    subscriber_type: JobType,
    spawner: job::KeyedJobSpawner<KeyMsg>,
) -> WakerHandler<D, P, Tables>
where
    D: SubscriptionDef<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    WakerHandler {
        def,
        subscriber_type,
        spawner,
        _marker: PhantomData,
    }
}

impl<D, P, Tables> SingletonSubscriber<P> for WakerHandler<D, P, Tables>
where
    D: SubscriptionDef<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    type Batch = std::collections::HashSet<WakeKey>;

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, Self::Batch>,
        event: &PersistentOutboxEvent<P>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        let keys: Vec<WakeKey> = self.def.wake_keys(event).into_iter().collect();
        if keys.is_empty() {
            return Ok(ctx.skip());
        }
        Ok(ctx.collect_with(move |batch| batch.extend(keys)))
    }

    async fn flush(
        &self,
        op: &mut FlushOp<'_>,
        items: Self::Batch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if items.is_empty() {
            return Ok(());
        }
        let wake_keys: Vec<String> = items.into_iter().map(|r| r.0).collect();
        let keys =
            Tables::subscription_keys_for_wake_keys(op, self.subscriber_type.as_str(), &wake_keys)
                .await?;
        if keys.is_empty() {
            return Ok(());
        }
        let specs = keys
            .into_iter()
            .map(|key| job::KeyedJobSpec::new(key.clone(), KeyMsg { key }))
            .collect();
        self.spawner.spawn_all_in_op(op, specs).await?;
        Ok(())
    }
}
