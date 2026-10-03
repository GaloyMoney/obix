//! The outbox's own rejections and fault wrappers. The rules they follow —
//! and `ObixFault`, the carrier they share with the rest of the crate — are
//! in `src/error.rs`.

use es_entity::errlanes;

use super::Ordering;

// --- bare rejections (no fault lanes at the sites that return them) ---

/// The commit lane is off for this outbox. Raised at registration, before
/// any job is spawned, so a consumer that needs the lane fails at startup.
///
/// Purely caller-correctable (enable the lane in config), so it is a bare
/// [`errlanes::Rejection`] — both on its own, where [`Lane`](super::Lane)'s
/// own methods return it directly, and lifted into
/// `Fail<CommitLaneDisabled, lanes!(Transient, Fatal)>` where a frontier
/// read or a registration can also fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq, errlanes::Rejection)]
#[rejection(code = "OBIX_COMMIT_LANE_DISABLED")]
#[error(
    "the commit lane is disabled on this outbox — set MailboxConfig::commit_lane = CommitLane::Enabled"
)]
pub struct CommitLaneDisabled;

// --- rejection families ---

/// Caller-correctable outcomes of the checkpoint read-back and the
/// caught-up barrier. Everything else — a raw sqlx fault, an undecodable
/// stored execution state, a stored checkpoint on the wrong lane, the
/// handler job's own failure — travels as `Transient`/`Fatal` in
/// `Fail<SubscriptionRejection, lanes!(Transient, Fatal)>` instead of as a
/// variant here.
#[derive(Debug, errlanes::Rejection)]
pub enum SubscriptionRejection {
    /// A keyed member's job could not be resolved from
    /// `(subscriber_type, key)` — no job row of that type is visible under
    /// the key.
    ///
    /// In practice this means reading through the [`Subscription`] that
    /// [`subscribe_in_op`] returned before the enclosing transaction has
    /// committed: that handle comes back from inside the caller's op, while
    /// the lookup reads on the pool. Remedy: commit, then read. Past the
    /// commit the `subscriptions` row and the job row are atomic (one op
    /// inserts and spawns), and a cancelled subscription's job rows outlive
    /// its `subscriptions` row — so no other sequence leaves a subscription
    /// without a job.
    ///
    /// [`Subscription`]: super::Subscription
    /// [`subscribe_in_op`]: super::Subscriptions::subscribe_in_op
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
    /// [`Subscription::await_position`](super::Subscription::await_position) —
    /// or [`await_caught_up`](super::Subscription::await_caught_up),
    /// which delegates to it — hit its deadline. Carries the observed lag so
    /// the caller can alert with real numbers instead of reporting a bare
    /// timeout.
    #[error("checkpoint {checkpoint} behind target {target} after {waited:?}")]
    #[rejection(code = "OBIX_SUBSCRIPTION_CAUGHT_UP_TIMEOUT")]
    CaughtUpTimeout {
        checkpoint: super::StreamPosition,
        target: super::StreamPosition,
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
/// Every variant here exists to *override* a classification: errlanes lanes
/// a bare `serde_json::Error` as `Fatal(Invariant)`, and these bytes came
/// out of Postgres, so they are `Fatal(CorruptState)` instead (rule 4). A
/// decode failure that already classifies correctly on its own — anything
/// sqlx itself refuses to decode — needs no variant here.
///
/// `Key`'s message deliberately does not include the stored key string —
/// it is caller data, and a `Fatal`'s `exception.message` is operator-facing
/// (see the display-discipline note on [`errlanes::Rejection`]).
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ObixFault;
    use es_entity::errlanes::{Fail, FatalKind, Fault, Rejection, ResultExt, lanes};
    use std::error::Error as _;

    #[test]
    fn commit_lane_disabled_has_a_stable_code() {
        let code: &'static str = CommitLaneDisabled.code().into();
        assert_eq!(code, "OBIX_COMMIT_LANE_DISABLED");
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
            checkpoint: super::super::StreamPosition::Insert(crate::EventSequence::BEGIN),
            target: super::super::StreamPosition::Insert(crate::EventSequence::BEGIN),
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

    /// Why `.widen::<ObixFault>()` at the keyed runner's decode sites is not
    /// ceremony: a `Classify` wrapper carries its classification in its
    /// `impl`, not in the value, so one boxed raw is neither a lane payload
    /// nor a blessed foreign type — the boundary's `Fault::classify` walks
    /// straight past it to the `serde_json::Error` underneath and lanes
    /// *that*, as `Fatal(Invariant)`. The `Fatal(CorruptState)` override only
    /// survives if the wrapper reaches a carrier first (rule 6).
    #[test]
    fn a_decode_wrapper_must_reach_a_carrier_before_it_reaches_a_box() {
        fn undecodable() -> CouldNotDecodeStored {
            CouldNotDecodeStored::ExecutionState(
                serde_json::from_value::<u64>(serde_json::json!("not a number")).unwrap_err(),
            )
        }
        fn boxed(e: impl std::error::Error + Send + Sync + 'static) -> ObixFault {
            let boxed: Box<dyn std::error::Error + Send + Sync> = Box::new(e);
            Fault::classify(&*boxed).narrow_denied()
        }

        assert!(
            matches!(boxed(undecodable()), Fault::Fatal(f) if f.kind == FatalKind::Invariant),
            "boxing the wrapper raw loses the override — so naming the carrier \
             at the call site is load-bearing, not ceremony",
        );

        // Through the verb the runner actually uses, the kind survives.
        let laned = Err::<(), _>(undecodable())
            .widen::<ObixFault>()
            .expect_err("still an error");
        assert!(matches!(boxed(laned), Fault::Fatal(f) if f.kind == FatalKind::CorruptState));
    }

    /// The converse, and why obix's own storage calls inside those same
    /// boxed methods need no conversion: a raw `sqlx::Error` is recovered
    /// from the box with the identical lane and kind, because
    /// `Fault::classify` reads the same table `From<sqlx::Error>` does.
    #[test]
    fn a_raw_sqlx_error_keeps_its_lane_across_a_box() {
        for error in [
            sqlx::Error::PoolTimedOut,
            sqlx::Error::RowNotFound,
            sqlx::Error::Protocol("synthesized".into()),
        ] {
            let eager: ObixFault = clone_shape(&error).into();
            let boxed: Box<dyn std::error::Error + Send + Sync> = Box::new(error);
            let recovered = Fault::classify(&*boxed).narrow_denied();
            assert_eq!(recovered.lane(), eager.lane());
            match (recovered, eager) {
                (Fault::Fatal(a), Fault::Fatal(b)) => assert_eq!(a.kind, b.kind),
                (Fault::Transient(a), Fault::Transient(b)) => assert_eq!(a.kind, b.kind),
                (a, b) => panic!("lane mismatch: {a:?} vs {b:?}"),
            }
        }
    }

    /// `sqlx::Error` is not `Clone`, and the test above needs the same
    /// variant twice.
    fn clone_shape(error: &sqlx::Error) -> sqlx::Error {
        match error {
            sqlx::Error::PoolTimedOut => sqlx::Error::PoolTimedOut,
            sqlx::Error::RowNotFound => sqlx::Error::RowNotFound,
            sqlx::Error::Protocol(m) => sqlx::Error::Protocol(m.clone()),
            other => panic!("unhandled shape {other:?}"),
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

    /// The same override reached through a `Fail` carrier rather than a
    /// `Fault` — the subscription reads have a rejection lane beside these
    /// faults, and a fault wrapper must land in the fault lanes there too.
    #[test]
    fn a_stored_decode_failure_is_fatal_corrupt_state_in_a_fail_carrier_too() {
        let decode_failure =
            serde_json::from_value::<u64>(serde_json::json!("not a number")).unwrap_err();
        let err: Fail<SubscriptionRejection, lanes!(Transient, Fatal)> =
            CouldNotDecodeStored::ExecutionState(decode_failure).into();
        assert!(matches!(err, Fail::Fatal(f) if f.kind == FatalKind::CorruptState));
    }
}
