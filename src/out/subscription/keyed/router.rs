//! The wake plane, part 1: liveness-only, event-driven wakes.
//!
//! An internal singleton subscriber — built entirely on the EXISTING
//! [`SingletonSubscriber`]/[`EventCtx`]/[`FlushOp`] machinery, no fork — that
//! classifies every persistent event into routing keys, collects them per
//! batch, and at flush time looks up which subscribed keys care and
//! idempotently respawns them, all on the flush op so the wakes and the
//! router's own checkpoint commit atomically: a crash cannot checkpoint past
//! events whose wakes were lost.
//!
//! Liveness-only by construction: a false-positive routing match costs one
//! harmless empty spawn (resolves to the live holder, or an empty lookup for
//! an unsubscribed key — the majority case). Routing must never gate
//! delivery, so it is never consulted by the per-key runner itself — only by
//! this router, to decide who to wake. The periodic backstop that covers a
//! *missed* wake is [`sweep`](super::sweep).

use serde::{Serialize, de::DeserializeOwned};
use std::{marker::PhantomData, sync::Arc};

use job::JobType;

use super::{KeyMsg, RoutingKey, SubscriptionDef, derived_job_type};
use crate::out::ctx::{EventCtx, FlushOp, Handled};
use crate::out::event::PersistentOutboxEvent;
use crate::out::subscription::singleton::SingletonSubscriber;
use crate::tables::MailboxTables;

pub(in crate::out) struct RouterHandler<D, P, Tables>
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

/// Build the router's job type: `{base}.router`, `'static`-leaked once at
/// registration (see [`derived_job_type`]).
pub(in crate::out) fn router_job_type(base: &str) -> JobType {
    derived_job_type(base, "router")
}

/// Construct the router handler — a plain [`SingletonSubscriber`], registered
/// via [`crate::out::Outbox::register_singleton_subscriber`] like any other.
pub(in crate::out) fn router_handler<D, P, Tables>(
    def: Arc<D>,
    subscriber_type: JobType,
    spawner: job::KeyedJobSpawner<KeyMsg>,
) -> RouterHandler<D, P, Tables>
where
    D: SubscriptionDef<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    RouterHandler {
        def,
        subscriber_type,
        spawner,
        _marker: PhantomData,
    }
}

impl<D, P, Tables> SingletonSubscriber<P> for RouterHandler<D, P, Tables>
where
    D: SubscriptionDef<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    type Batch = std::collections::HashSet<RoutingKey>;

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, Self::Batch>,
        event: &PersistentOutboxEvent<P>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        let keys: Vec<RoutingKey> = self.def.routing_key(event).into_iter().collect();
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
        let routing_keys: Vec<String> = items.into_iter().map(|r| r.0).collect();
        let keys = Tables::subscription_keys_for_routing_keys(
            op,
            self.subscriber_type.as_str(),
            &routing_keys,
        )
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
