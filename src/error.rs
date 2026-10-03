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
//!   at `Inbox::find_event_by_id`, over the trait's plain `Option`) and where
//!   an unreadable `inbox_events.status` becomes `Fatal(CorruptState)` (the
//!   trait hands it over as a `sqlx::Error::ColumnDecode` carrying
//!   [`CouldNotDecodeStored::InboxStatus`] — see
//!   [`decode_inbox_status`](crate::decode_inbox_status)).
//!
//! # Which carrier each public method returns
//!
//! | Carrier | Returned by |
//! |---|---|
//! | [`ObixFault`] | every method that cannot reject: `Outbox::init`, `begin_op`, `publish_ephemeral*`, `highest_known_persistent_sequence`, `register_keyed_subscriber`, `register_partition_maintainer`, `Partitions::ensure`, `recover_default`, `Subscriptions::subscribe_in_op`/`cancel`/`cancel_in_op`, `Inbox::persist_and_queue_job*`/`list_failed` |
//! | `Fail<CommitLaneDisabled, lanes!(Transient, Fatal)>` | `Outbox::frontier`, `Outbox::register_singleton_subscriber` |
//! | `Fail<SubscriptionRejection, lanes!(Transient, Fatal)>` | `Subscription::load`/`await_position`/`await_caught_up`, `Subscriptions::subscription` |
//! | `Fail<InboxRejection, lanes!(Transient, Fatal)>` | `Inbox::find_event_by_id` — the only method that can reject it |
//! | bare rejection | `Outbox::cursor` (`CursorError`), `WakeKeys::try_from` (`SubscribeError`), `Outbox::listen`/`listen_commit_ordered` (`CommitLaneDisabled`) |
//! | `sqlx::Error` | `Outbox::publish_persisted_in_op`, `Outbox::publish_all_persisted` (hook-pinned) |
//! | raw `serde_json::Error` | `InboxEvent::payload` (the only error that site can produce) |

use es_entity::errlanes::{self, Fault, lanes};

use crate::out::Ordering;

/// The fault-only carrier: what every method that cannot reject returns
/// (rule 2).
///
/// The one alias obix keeps, and the one exempt from rule 7 — it hides
/// nothing. There is no rejection type for a name to swallow, and the lanes
/// are the same two everywhere, so `ObixFault` says as much as the shape it
/// stands for. A `Fail` carrier is a different matter and always spells
/// itself.
pub type ObixFault = Fault<lanes!(Transient, Fatal)>;

// --- bare rejections (no fault lanes at the sites that return them) ---

/// The commit lane is off for this outbox. Raised at registration, before
/// any job is spawned, so a consumer that needs the lane fails at startup.
///
/// Purely caller-correctable (enable the lane in config), so it is a bare
/// [`errlanes::Rejection`] — both on its own, where it is returned directly
/// throughout [`out::lane`](crate::out), and lifted into
/// `Fail<CommitLaneDisabled, lanes!(Transient, Fatal)>` where a frontier
/// read or a registration can also fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq, errlanes::Rejection)]
#[rejection(code = "OBIX_COMMIT_LANE_DISABLED")]
#[error(
    "the commit lane is disabled on this outbox — set MailboxConfig::commit_lane = CommitLane::Enabled"
)]
pub struct CommitLaneDisabled;

/// Error returned by [`Outbox::cursor`](crate::out::Outbox::cursor).
///
/// Purely caller-correctable — the caller passed an op of the wrong kind —
/// so this is a bare [`errlanes::Rejection`] rather than a `Fail`/`Fault`:
/// nothing about obtaining a cursor can fail in any other way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, errlanes::Rejection)]
pub enum CursorError {
    /// The operation does not support commit hooks, so there is no op-local
    /// publish buffer to position into. Use an op that supports hooks — e.g.
    /// one from [`Outbox::begin_op`](crate::out::Outbox::begin_op) — rather
    /// than a bare `sqlx::Transaction`.
    #[error("OpCursor requires an operation that supports commit hooks; this operation does not")]
    #[rejection(code = "OBIX_CURSOR_HOOKS_UNSUPPORTED")]
    HooksUnsupported,
}

/// Why a set of wake keys could not be accepted.
///
/// Purely caller-correctable — the caller built a bad set of wake keys — so
/// this is a bare [`errlanes::Rejection`] rather than a `Fail`/`Fault`:
/// nothing else can go wrong here.
#[derive(Debug, errlanes::Rejection)]
pub enum SubscribeError {
    /// A runtime-built collection of wake keys turned out to be empty. See
    /// [`WakeKeys`](crate::out::WakeKeys) for why an empty set is
    /// unrepresentable at the call site and how to build one from runtime
    /// data.
    #[error("a subscription must declare at least one wake key")]
    #[rejection(code = "OBIX_SUBSCRIBE_EMPTY_WAKE_KEYS")]
    EmptyWakeKeys,
}

// --- rejection families ---
//
// [`InboxRejection`](crate::inbox::InboxRejection) is the exception: it is
// defined in `crate::inbox`, beside the only API that returns it.

/// Caller-correctable outcomes of the checkpoint read-back and the
/// caught-up barrier. Everything else — a raw sqlx fault, an undecodable
/// stored execution state, a stored checkpoint on the wrong lane, the
/// handler job's own failure — travels as `Transient`/`Fatal` in
/// `Fail<SubscriptionRejection, lanes!(Transient, Fatal)>` instead of as a
/// variant here.
#[derive(Debug, errlanes::Rejection)]
pub enum SubscriptionRejection {
    /// A keyed member's job could not be resolved from
    /// `(subscriber_type, key)` — no job of that type has ever been spawned
    /// under the key. Distinct from a cancelled subscription, whose job rows
    /// outlive the `subscriptions` row.
    #[error("no job for ({subscriber_type}, {key})")]
    #[rejection(code = "OBIX_SUBSCRIPTION_NO_SUCH_JOB")]
    NoSuchJob {
        subscriber_type: String,
        key: String,
    },
    /// `Subscriptions::subscription(key)`: no subscriptions row has ever
    /// existed for this key.
    #[error("no subscription has ever existed for ({subscriber_type}, {key})")]
    #[rejection(code = "OBIX_SUBSCRIPTION_NO_SUCH_SUBSCRIPTION")]
    NoSuchSubscription {
        subscriber_type: String,
        key: String,
    },
    /// [`Subscription::await_position`](crate::out::Subscription::await_position) —
    /// or [`await_caught_up`](crate::out::Subscription::await_caught_up),
    /// which delegates to it — hit its deadline. Carries the observed lag so
    /// the caller can alert with real numbers instead of reporting a bare
    /// timeout.
    #[error("checkpoint {checkpoint} behind target {target} after {waited:?}")]
    #[rejection(code = "OBIX_SUBSCRIPTION_CAUGHT_UP_TIMEOUT")]
    CaughtUpTimeout {
        checkpoint: crate::out::StreamPosition,
        target: crate::out::StreamPosition,
        waited: std::time::Duration,
    },
}

// --- fault wrappers: the kind says what the operator looks at ---

/// A subscription checkpointed on one stream lane was opened under the
/// other [`Ordering`]. The remedy is a code change (register a new job
/// type), so this is configuration, not a caller outcome — it enters the
/// subscription carrier as `Fatal(Config)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, errlanes::Classify)]
#[classify(fatal(Config))]
#[error(
    "subscription is checkpointed on the {stored} lane but registered with Ordering::{configured}; switching lanes is unsupported — register a new job type instead"
)]
pub struct LaneMismatch {
    pub stored: Ordering,
    pub configured: Ordering,
}

/// Persisted state that no longer decodes. One variant per stored value so
/// the operator-facing chain names which row to look at; the serde error (or
/// nothing, for a non-serde decode) is the variant's source and appears
/// exactly once below it.
///
/// `Key`'s message deliberately does not include the stored key string —
/// it is caller data, and a `Fatal`'s `exception.message` is operator-facing
/// (see the display-discipline note on [`errlanes::Rejection`]).
/// `InboxStatus`'s message does include the stored string: the status
/// column holds a bounded set of obix-authored tokens, not caller data.
#[derive(Debug, errlanes::Classify)]
pub enum CouldNotDecodeStored {
    #[classify(fatal(CorruptState))]
    #[error("could not decode a subscriber job's execution state")]
    ExecutionState(#[source] serde_json::Error),
    #[classify(fatal(CorruptState))]
    #[error("could not decode a keyed subscription's instance_config")]
    InstanceConfig(#[source] serde_json::Error),
    #[classify(fatal(CorruptState))]
    #[error("could not parse a keyed subscription's persisted key for {job_type}")]
    Key { job_type: ::job::JobType },
    #[classify(fatal(CorruptState))]
    #[error("inbox_events.status holds an unknown value: {0}")]
    InboxStatus(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inbox::InboxRejection;
    use es_entity::errlanes::{Fail, FatalKind, Fault, Rejection, lanes};
    use std::error::Error as _;

    #[test]
    fn commit_lane_disabled_has_a_stable_code() {
        let code: &'static str = CommitLaneDisabled.code().into();
        assert_eq!(code, "OBIX_COMMIT_LANE_DISABLED");
    }

    #[test]
    fn cursor_error_has_a_stable_code() {
        let code: &'static str = CursorError::HooksUnsupported.code().into();
        assert_eq!(code, "OBIX_CURSOR_HOOKS_UNSUPPORTED");
    }

    #[test]
    fn subscribe_error_has_a_stable_code() {
        let code: &'static str = SubscribeError::EmptyWakeKeys.code().into();
        assert_eq!(code, "OBIX_SUBSCRIBE_EMPTY_WAKE_KEYS");
    }

    #[test]
    fn subscription_rejection_codes_are_stable() {
        let no_such_job: &'static str = SubscriptionRejection::NoSuchJob {
            subscriber_type: String::new(),
            key: String::new(),
        }
        .code()
        .into();
        assert_eq!(no_such_job, "OBIX_SUBSCRIPTION_NO_SUCH_JOB");

        let no_such_subscription: &'static str = SubscriptionRejection::NoSuchSubscription {
            subscriber_type: String::new(),
            key: String::new(),
        }
        .code()
        .into();
        assert_eq!(
            no_such_subscription,
            "OBIX_SUBSCRIPTION_NO_SUCH_SUBSCRIPTION"
        );

        let timeout: &'static str = SubscriptionRejection::CaughtUpTimeout {
            checkpoint: crate::out::StreamPosition::Insert(crate::EventSequence::BEGIN),
            target: crate::out::StreamPosition::Insert(crate::EventSequence::BEGIN),
            waited: std::time::Duration::ZERO,
        }
        .code()
        .into();
        assert_eq!(timeout, "OBIX_SUBSCRIPTION_CAUGHT_UP_TIMEOUT");
    }

    /// `LaneMismatch` is configuration, not a caller outcome: it enters the
    /// subscription carrier as `Fatal(Config)`, never `Rejected`.
    #[test]
    fn lane_mismatch_is_fatal_config() {
        let mismatch = LaneMismatch {
            stored: Ordering::Insert,
            configured: Ordering::Commit,
        };
        let err: Fail<SubscriptionRejection, lanes!(Transient, Fatal)> = mismatch.into();
        match err {
            Fail::Fatal(f) => {
                assert_eq!(f.kind, FatalKind::Config);
                assert!(f.source().unwrap().downcast_ref::<LaneMismatch>().is_some());
            }
            other => panic!("expected Fatal(Config), got {other:?}"),
        }
    }

    /// Each `CouldNotDecodeStored` variant converts into `Fatal(CorruptState)`,
    /// names the stored value in its own message, and keeps the serde error
    /// (when it has one) as its source exactly once.
    #[test]
    fn execution_state_decode_failure_is_fatal_corrupt_state() {
        let decode_failure =
            serde_json::from_value::<u64>(serde_json::json!("not a number")).unwrap_err();
        let err: ObixFault = CouldNotDecodeStored::ExecutionState(decode_failure).into();
        match err {
            Fault::Fatal(fatal) => {
                assert_eq!(fatal.kind, FatalKind::CorruptState);
                let wrapper = fatal.source().expect("the wrapper is the Fatal's source");
                assert_eq!(
                    wrapper.to_string(),
                    "could not decode a subscriber job's execution state"
                );
                assert!(wrapper.source().is_some(), "serde error stays in the chain");
            }
            other => panic!("expected Fatal(CorruptState), got {other:?}"),
        }
    }

    #[test]
    fn instance_config_decode_failure_is_fatal_corrupt_state() {
        let decode_failure =
            serde_json::from_value::<u64>(serde_json::json!("not a number")).unwrap_err();
        let err: ObixFault = CouldNotDecodeStored::InstanceConfig(decode_failure).into();
        assert!(matches!(err, Fault::Fatal(f) if f.kind == FatalKind::CorruptState));
    }

    /// The key string is caller data, so it must not appear in the
    /// message — only in the `job_type` the operator can already see.
    #[test]
    fn key_decode_failure_is_fatal_corrupt_state_and_hides_no_message_it_should_not() {
        let err: ObixFault = CouldNotDecodeStored::Key {
            job_type: ::job::JobType::new("test-job-type"),
        }
        .into();
        match err {
            Fault::Fatal(fatal) => {
                assert_eq!(fatal.kind, FatalKind::CorruptState);
                let wrapper = fatal.source().expect("the wrapper is the Fatal's source");
                assert!(wrapper.to_string().contains("test-job-type"));
            }
            other => panic!("expected Fatal(CorruptState), got {other:?}"),
        }
    }

    #[test]
    fn inbox_status_decode_failure_is_fatal_corrupt_state() {
        let err: Fail<InboxRejection, lanes!(Transient, Fatal)> =
            CouldNotDecodeStored::InboxStatus("bogus".to_string()).into();
        match err {
            Fail::Fatal(fatal) => {
                assert_eq!(fatal.kind, FatalKind::CorruptState);
                assert!(fatal.source().unwrap().to_string().contains("bogus"));
            }
            other => panic!("expected Fatal(CorruptState), got {other:?}"),
        }
    }

    /// A raw sqlx fault never rejects — it enters every one of these fault
    /// lanes through errlanes' own blanket classification, with no
    /// obix-specific conversion in between.
    #[test]
    fn a_raw_sqlx_fault_classifies_rather_than_rejects() {
        let err: ObixFault = sqlx::Error::PoolTimedOut.into();
        assert!(err.is_transient());
        let err: Fail<InboxRejection, lanes!(Transient, Fatal)> = sqlx::Error::RowNotFound.into();
        assert!(err.is_fatal());
    }
}
