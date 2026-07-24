use es_entity::hooks::{BoxFuture, HookOperation};
use serde::{Serialize, de::DeserializeOwned};

use std::sync::Arc;

use crate::out::event::PersistentOutboxEvent;

/// Shared snapshot of an outbox's registered post-persist hooks.
pub(crate) type PostPersistHooks<P> = Arc<[Arc<dyn PostPersistHook<P>>]>;

/// In-transaction callback invoked after this outbox's events are INSERTed
/// (sequences assigned) but before the surrounding operation commits.
///
/// Register on an outbox via
/// [`Outbox::add_post_persist_hook`](super::Outbox::add_post_persist_hook).
///
/// # Contract
///
/// - Runs inside the source transaction. An `Err` rolls back EVERYTHING —
///   including the write that produced the events. Keep implementations
///   total; this is a feature (invariant vetoes) and a hazard (a mapping bug
///   aborts the producing write).
/// - Sees persisted events — `id`, `sequence`, `recorded_at`, payload — so
///   provenance is built in.
/// - Exactly-once *with* the source data: fires iff the transaction commits;
///   a rollback unwinds both.
/// - Invoked once per persisted chunk (bounded memory for large publishes —
///   see [`MailboxConfig::persist_events_batch_size`](crate::MailboxConfig)).
///   Implementations must not assume they see an operation's events in one
///   call.
/// - The [`HookOperation`] cannot register commit hooks, so a hook cannot
///   schedule further commit hooks. It CAN publish to another outbox — that
///   takes the immediate-insert path (same transaction) and invokes that
///   outbox's own post-persist hooks. Chains (A→B→C) compose; it is the
///   implementor's responsibility not to register hooks that form a cycle
///   (A→B→A), which would recurse until the stack overflows.
/// - Fires on every persist path, including publishes on operations without
///   commit-hook support (e.g. a bare `sqlx::Transaction`), where events are
///   inserted immediately — the hook then fires during the publish call
///   itself rather than at commit time.
/// - Registration is snapshot-at-publish: registering a hook affects only
///   subsequently-constructed commit hooks (in practice: subsequent
///   operations). Register at startup before serving traffic; this is not a
///   dynamic-reconfiguration facility.
/// - Ephemeral events are out of scope — persistent outbox only.
pub trait PostPersistHook<P>: Send + Sync + 'static
where
    P: Serialize + DeserializeOwned + Send,
{
    fn on_persisted<'a>(
        &'a self,
        op: &'a mut HookOperation<'_>,
        events: &'a [PersistentOutboxEvent<P>],
    ) -> BoxFuture<'a, Result<(), sqlx::Error>>;
}
