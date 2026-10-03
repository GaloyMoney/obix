/// Caller-correctable outcomes of an inbox operation. See
/// [`crate::error::InboxRejection`]'s doc for the full rationale.
pub use crate::error::InboxRejection;

pub use crate::error::InboxError;

#[cfg(test)]
mod tests {
    use super::super::InboxEventId;
    use crate::error::{CouldNotDecodeStored, InboxError, InboxRejection};
    use es_entity::errlanes::{Fail, FatalKind};
    use std::error::Error as _;

    #[test]
    fn not_found_enters_inbox_error_as_rejected() {
        let id = InboxEventId::new();
        let err: InboxError = InboxRejection::NotFound(id).into();
        assert!(matches!(err, Fail::Rejected(InboxRejection::NotFound(found)) if found == id));
    }

    /// A raw sqlx fault never rejects — it enters InboxError's fault lanes
    /// through errlanes' own blanket classification, with no inbox-specific
    /// conversion in between.
    #[test]
    fn a_raw_sqlx_fault_classifies_rather_than_rejects() {
        let err: InboxError = sqlx::Error::PoolTimedOut.into();
        assert!(err.is_transient());
    }

    /// job's own id-choosing spawn rejects with `JobRejection::DuplicateId`
    /// only when the caller handed it an id it already used for a
    /// different job; after a *fresh* `insert_inbox_event` that can only
    /// mean the inbox's own id generation collided with a job the inbox
    /// itself spawned earlier, which is an invariant, not something a
    /// caller of `persist_and_queue_job*` can correct. See
    /// `Inbox::persist_and_queue_job_in_op`, which `narrow_rejected`s the
    /// job service's `JobError` into the fault lanes rather than lifting it
    /// into `InboxRejection`.
    #[test]
    fn a_duplicate_job_id_after_a_fresh_insert_is_fatal_invariant() {
        let job_id = ::job::JobId::new();
        let job_error: ::job::JobError =
            es_entity::errlanes::Fail::Rejected(::job::JobRejection::DuplicateId(job_id));
        let fault = job_error.narrow_rejected();
        match fault {
            es_entity::errlanes::Fault::Fatal(f) => {
                assert_eq!(f.kind, FatalKind::Invariant);
                assert!(
                    f.source()
                        .unwrap()
                        .downcast_ref::<::job::JobRejection>()
                        .is_some()
                );
            }
            other => panic!("expected Fatal(Invariant), got {other:?}"),
        }
        // Sanity: a `CouldNotDecodeStored` fault that bare-`?`s into
        // `InboxError` is unaffected by this narrowing path — the two are
        // independent entry points into the same fault lanes.
        let _: InboxError = CouldNotDecodeStored::InboxStatus("bogus".into()).into();
    }
}
