//! The rejections and fault wrappers obix's carriers name, and the rules
//! that decide which carrier a method gets. Re-exported from the paths
//! downstream code already uses (`obix::`, `obix::out::`, `obix::inbox::`).
//!
//! Carriers themselves are not named here: there are no `FooError` aliases
//! (rule 7) — every signature spells its own `Fail`/`Fault`. A rejection
//! returned by exactly one module's API may live beside it instead
//! ([`InboxRejection`](crate::inbox::InboxRejection), in `src/inbox`).
//!
//! # The rule
//!
//! 0. **Implementor boundaries stay broad.** Anything a user of obix
//!    implements (every async method on [`SingletonSubscriber`](crate::out::SingletonSubscriber),
//!    [`KeyedSubscriber`](crate::out::KeyedSubscriber), [`InboxHandler`](crate::inbox::InboxHandler),
//!    [`SubscriptionDef`](crate::out::SubscriptionDef)) returns
//!    `Box<dyn std::error::Error + Send + Sync>`, and whatever the owning
//!    trait already fixes elsewhere (`sqlx::Error` on
//!    [`PostPersistHook`](crate::out::PostPersistHook), by es-entity). obix
//!    never requires a laned error from an implementor. A laned error an
//!    implementor *chooses* to return travels through unchanged inside the
//!    box, and job's `Fault::classify` finds it in the chain — that is how a
//!    handler reaches job's dispositions (a congestion reschedule that does
//!    not spend an attempt, `terminal_on_fatal`). A boxed `Fail::Rejected`
//!    has no surviving marker and simply retries per policy: a handler
//!    resolves its own rejections in its body (skip, ack, or map) rather
//!    than propagating them.
//! 1. **A rejection exists only where a caller can branch on it in code.**
//!    Rejections are scoped to the methods that can produce them.
//! 2. **A method that cannot reject returns [`ObixFault`]**, never a `Fail`
//!    with an empty rejection and never a boxed error.
//! 3. **Foreign errors stay raw on a `pub fn` only when they are the only
//!    error that site can produce**, or when a trait pins them (the two
//!    boundaries below). Everywhere else a foreign error is laned at birth.
//! 4. **Stored data that does not decode is `Fatal(CorruptState)`, not
//!    `Fatal(Invariant)`.** [`CouldNotDecodeStored`] overrides the serde
//!    default wherever the bytes came out of Postgres.
//! 5. **Library code never inspects a `Fatal`'s payload and never asks
//!    callers to.** Where a caller needs to know, the API offers a value
//!    instead (`Jobs::keyed_handle` returning `Option`,
//!    `Subscriptions::subscription` rejecting).
//! 6. **Classify once, where the error is born; box once, at the trait
//!    boundary.**
//! 7. **Every `Fail` signature spells its carrier.** No `pub type FooError
//!    = Fail<..>` aliases: a reader of a signature must see which rejection
//!    it is being handed, since that is exactly what they have to branch on.
//!    The cost is a long `Result` type; the thing bought is that rule 1 is
//!    *visible* at every method rather than hidden behind a name. The
//!    fault-only carrier is the sole exception, and has one name —
//!    [`ObixFault`] — precisely because it has no rejection to hide.
//!
//! # Two boundaries pinned by es-entity, not obix's to move
//!
//! - `es_entity::hooks::CommitHook::pre_commit` returns `sqlx::Error`. That
//!   fixes [`PostPersistHook::on_persisted`](crate::out::PostPersistHook::on_persisted),
//!   `CheckpointMirror::mirror`, `Outbox::publish_persisted_in_op` and
//!   `Outbox::publish_all_persisted` (a hook is documented to call back into
//!   both from inside its own `on_persisted`). A broader hook error type is
//!   an es-entity change, not an obix one.
//! - `MailboxTables` is generated into downstream crates via
//!   `#[derive(MailboxTables)]` and is the storage layer; **every** method on
//!   it returns `sqlx::Error`, with no exceptions. obix classifies one level
//!   up, exactly as es-entity's own repo layer does, which is also where
//!   absence becomes a rejection ([`crate::inbox::InboxRejection::NotFound`]
//!   at `Inbox::find_event_by_id`, over the trait's plain `Option`).
//!   Nothing the trait reads needs an obix-authored decode error:
//!   `inbox_events.status` is a Postgres enum that sqlx decodes itself, and
//!   a label this binary does not know arrives as `ColumnDecode`, which
//!   errlanes already lanes as `Fatal(CorruptState)` — rule 4 without a
//!   wrapper.
//!
//! # Which carrier each public method returns
//!
//! | Carrier | Returned by |
//! |---|---|
//! | [`ObixFault`] | every method that cannot reject: `Outbox::init`, `begin_op`, `publish_ephemeral*`, `highest_known_persistent_sequence`, `register_keyed_subscriber`, `register_partition_maintainer`, `Partitions::ensure`, `recover_default`, `Subscriptions::subscribe_in_op`/`cancel`/`cancel_in_op`, `Inbox::persist_and_queue_job*`/`list_failed` |
//! | `Fail<CommitLaneDisabled, lanes!(Transient, Fatal)>` | `Outbox::frontier`, `Outbox::register_singleton_subscriber` |
//! | `Fail<SubscriptionRejection, lanes!(Transient, Fatal)>` | `Subscription::load`/`await_position`/`await_caught_up`, `Subscriptions::subscription` |
//! | `Fail<InboxRejection, lanes!(Transient, Fatal)>` | `Inbox::find_event_by_id` — the only method that can reject it |
//! | bare rejection | `Outbox::listen`/`listen_commit_ordered` (`CommitLaneDisabled`) |
//! | `sqlx::Error` | `Outbox::publish_persisted_in_op`, `Outbox::publish_all_persisted` (hook-pinned) |
//! | raw `serde_json::Error` | `InboxEvent::payload` (the only error that site can produce) |

use es_entity::errlanes::{Fault, lanes};

/// The fault-only carrier: what every method that cannot reject returns
/// (rule 2).
///
/// The one alias obix keeps, and the one exempt from rule 7 — it hides
/// nothing. There is no rejection type for a name to swallow, and the lanes
/// are the same two everywhere, so `ObixFault` says as much as the shape it
/// stands for. A `Fail` carrier is a different matter and always spells
/// itself.
pub type ObixFault = Fault<lanes!(Transient, Fatal)>;

#[cfg(test)]
mod tests {
    use super::*;

    /// A raw sqlx fault never rejects — it enters the fault lanes through
    /// errlanes' own blanket classification, with no obix-specific
    /// conversion in between. Each module's own rejections and fault
    /// wrappers are tested where they live.
    #[test]
    fn a_raw_sqlx_fault_classifies_rather_than_rejects() {
        let err: ObixFault = sqlx::Error::PoolTimedOut.into();
        assert!(err.is_transient());
        let err: ObixFault = sqlx::Error::RowNotFound.into();
        assert!(err.is_fatal());
    }
}
