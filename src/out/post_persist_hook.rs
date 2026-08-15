use es_entity::hooks::{BoxFuture, HookOperation};
use serde::{Serialize, de::DeserializeOwned};

use std::sync::Arc;

use crate::out::event::PersistentOutboxEvent;

/// Shared snapshot of an outbox's registered post-persist hooks.
pub(crate) type PostPersistHooks<P> = Arc<[Arc<dyn PostPersistHook<P>>]>;

/// In-transaction callback invoked after this outbox's events are inserted
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
/// - A hook CAN publish to another outbox. `on_persisted` runs from inside a
///   real commit pass, so the [`HookOperation`] it is handed can register
///   further commit hooks: the repost's own `PersistEvents` joins the **tail
///   of this same pass**, giving the destination outbox its full lifecycle
///   (`pre_commit`, `post_commit`, `on_rollback`) atomically in the same
///   transaction. Chains (A→B→C) compose; a cycle (A→B→A) is bounded by
///   es-entity's [`MAX_HOOK_GENERATIONS`](es_entity::hooks::MAX_HOOK_GENERATIONS)
///   (8) and fails the commit loudly — still the implementor's job to avoid.
///   See [`es_entity::hooks`] for the re-entrant registration contract.
///   Whether the repost merges into the destination's still-pending commit
///   hook or appends a fresh generation depends on registration order,
///   which is otherwise an accident of application call order — declare
///   [`Outbox::persist_after`](super::Outbox::persist_after) on the
///   destination to pin it: the destination's persist hook then always
///   defers behind the source's while the source is still pending, so the
///   repost merges into one INSERT batch, one notify, one broadcast,
///   regardless of which outbox published first in program order.
/// - Fires on every persist path, including publishes on operations without
///   commit-hook support at all (e.g. a bare `sqlx::Transaction`), where
///   events are inserted immediately — the hook then fires during the
///   publish call itself rather than at commit time.
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
