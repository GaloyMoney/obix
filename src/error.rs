//! obix's fault-only carrier.

use es_entity::errlanes::{Fault, lanes};

/// The fault-only carrier: what every obix method that cannot reject returns.
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
