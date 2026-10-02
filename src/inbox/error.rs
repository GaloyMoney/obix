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
