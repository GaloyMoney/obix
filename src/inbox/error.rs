use es_entity::errlanes;

use super::InboxEventId;

/// Caller-correctable outcomes of an inbox operation. Everything else —
/// sqlx faults, an undecodable stored payload, the handler job's own
/// failure — travels as `Transient`/`Fatal` in the carrier
/// `Fail<InboxRejection, lanes!(Transient, Fatal)>` instead of as a variant
/// here.
///
/// Defined beside the API that returns it; the rules every obix rejection
/// follows are in `src/error.rs`.
#[derive(Debug, errlanes::Rejection)]
pub enum InboxRejection {
    #[error("inbox event {0} not found")]
    #[rejection(code = "OBIX_INBOX_EVENT_NOT_FOUND")]
    NotFound(InboxEventId),
}

#[cfg(test)]
mod tests {
    use super::{InboxEventId, InboxRejection};
    use crate::error::{CouldNotDecodeStored, ObixFault};
    use es_entity::errlanes::{Fail, FatalKind, Fault, Rejection, lanes};
    use std::error::Error as _;

    #[test]
    fn inbox_rejection_codes_are_stable() {
        let id = InboxEventId::new();
        let not_found: &'static str = InboxRejection::NotFound(id).code().into();
        assert_eq!(not_found, "OBIX_INBOX_EVENT_NOT_FOUND");
    }

    #[test]
    fn not_found_enters_the_carrier_as_rejected() {
        let id = InboxEventId::new();
        let err: Fail<InboxRejection, lanes!(Transient, Fatal)> =
            InboxRejection::NotFound(id).into();
        assert!(matches!(err, Fail::Rejected(InboxRejection::NotFound(found)) if found == id));
    }

    /// A raw sqlx fault never rejects — it enters the inbox's fault lanes
    /// through errlanes' own blanket classification, with no inbox-specific
    /// conversion in between.
    #[test]
    fn a_raw_sqlx_fault_classifies_rather_than_rejects() {
        let err: Fail<InboxRejection, lanes!(Transient, Fatal)> = sqlx::Error::PoolTimedOut.into();
        assert!(err.is_transient());
    }

    /// Rule 4 survives the storage layer staying on `sqlx::Error`: an
    /// unreadable `inbox_events.status` is handed over as a `ColumnDecode`,
    /// which errlanes lanes as `Fatal(CorruptState)` one level up, with the
    /// named wrapper still in the chain to say which column it was.
    #[test]
    fn an_unreadable_stored_status_stays_fatal_corrupt_state_through_sqlx() {
        let err = crate::decode_inbox_status("bogus").expect_err("not a known status");
        assert!(matches!(err, sqlx::Error::ColumnDecode { .. }));

        let fault: ObixFault = err.into();
        let Fault::Fatal(fatal) = fault else {
            panic!("expected Fatal(CorruptState), got {fault:?}")
        };
        assert_eq!(fatal.kind, FatalKind::CorruptState);

        let mut link = fatal.source();
        while let Some(e) = link {
            if let Some(wrapper) = e.downcast_ref::<CouldNotDecodeStored>() {
                assert!(wrapper.to_string().contains("bogus"));
                return;
            }
            link = e.source();
        }
        panic!("the named wrapper must stay in the chain");
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
        let job_error: ::job::JobError = Fail::Rejected(::job::JobRejection::DuplicateId(job_id));
        let fault = job_error.narrow_rejected();
        match fault {
            Fault::Fatal(f) => {
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
        // Sanity: a `CouldNotDecodeStored` fault that bare-`?`s into the
        // inbox's carrier is unaffected by this narrowing path — the two are
        // independent entry points into the same fault lanes.
        let _: Fail<InboxRejection, lanes!(Transient, Fatal)> =
            CouldNotDecodeStored::InboxStatus("bogus".into()).into();
    }
}
