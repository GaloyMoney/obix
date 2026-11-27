//! obix's fault-only carrier.

use es_entity::errlanes::{self, Fault, lanes};

/// The fault-only carrier: what every obix method that cannot reject returns.
pub type ObixFault = Fault<lanes!(Transient, Fatal)>;

#[derive(Debug, errlanes::Classify)]
#[error("obix migration failed")]
#[classify(fatal(Config), from)]
pub(crate) struct Migrate(#[source] sqlx::migrate::MigrateError);

#[cfg(test)]
mod tests {
    use super::*;
    use es_entity::errlanes::FatalKind;
    use std::error::Error as _;

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

    /// The wrapper names the migration stage while the `MigrateError` below it
    /// says which migration failed, so an operator sees both in one chain.
    #[test]
    fn a_migration_failure_is_fatal_config_keeping_the_driver_error() {
        let err: ObixFault = Migrate(sqlx::migrate::MigrateError::VersionMissing(7)).into();
        match err {
            Fault::Fatal(fatal) => {
                assert_eq!(fatal.kind, FatalKind::Config);
                let wrapper = fatal.source().expect("the wrapper is the Fatal's source");
                assert_eq!(wrapper.to_string(), "obix migration failed");
                let driver = wrapper
                    .source()
                    .expect("the MigrateError is the wrapper's source");
                assert!(
                    driver.to_string().contains("migration 7"),
                    "driver error lost from the chain, got {driver}"
                );
            }
            other => panic!("expected Fatal(Config), got {other:?}"),
        }
    }
}
