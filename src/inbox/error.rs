/// Caller-correctable outcomes of an inbox operation. Everything else —
/// sqlx faults, an undecodable stored payload, the handler job's own
/// failure — travels as `Transient`/`Fatal` in [`InboxError`] instead of as
/// a variant here.
///
/// The job service's own errors are deliberately narrowed into the fault
/// lanes rather than lifted into this rejection: a failure reading or
/// awaiting the handler job is an inbox-internal plumbing problem, not one
/// of the inbox's own domain outcomes.
#[derive(Debug, es_entity::errlanes::Rejection)]
pub enum InboxRejection {
    #[error("InboxError - NotFound: {0}")]
    #[rejection(code = "OBIX_INBOX_EVENT_NOT_FOUND")]
    NotFound(super::InboxEventId),
    #[error("InboxError - InvalidStatus: {0}")]
    #[rejection(code = "OBIX_INBOX_INVALID_STATUS")]
    InvalidStatus(String),
}

pub type InboxError =
    es_entity::errlanes::Fail<InboxRejection, es_entity::errlanes::lanes!(Transient, Fatal)>;

#[cfg(test)]
mod tests {
    use super::*;
    use es_entity::errlanes::{Fail, Rejection};

    #[test]
    fn not_found_enters_inbox_error_as_rejected() {
        let id = super::super::InboxEventId::new();
        let err: InboxError = InboxRejection::NotFound(id).into();
        assert!(matches!(err, Fail::Rejected(InboxRejection::NotFound(found)) if found == id));
    }

    #[test]
    fn rejection_codes_are_stable() {
        let id = super::super::InboxEventId::new();
        let not_found: &'static str = InboxRejection::NotFound(id).code().into();
        assert_eq!(not_found, "OBIX_INBOX_EVENT_NOT_FOUND");

        let invalid: &'static str = InboxRejection::InvalidStatus("bogus".to_string())
            .code()
            .into();
        assert_eq!(invalid, "OBIX_INBOX_INVALID_STATUS");
    }

    /// A raw sqlx fault never rejects — it enters InboxError's fault lanes
    /// through errlanes' own blanket classification, with no inbox-specific
    /// conversion in between.
    #[test]
    fn a_raw_sqlx_fault_classifies_rather_than_rejects() {
        let err: InboxError = sqlx::Error::PoolTimedOut.into();
        assert!(err.is_transient());
    }
}
