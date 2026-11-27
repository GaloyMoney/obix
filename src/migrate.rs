//! Embedded migrations for obix's outbox tables.
//!
//! Each [`MailboxTables`] implementation records its migrations in its own
//! tracking table (`T::MIGRATION_TABLE`: `_sqlx_migrations_obix` for the
//! default tables, `_sqlx_migrations_obix_{tbl_prefix}` for prefixed ones),
//! so multiple obix instances can coexist in one Postgres database.
//!
//! # Existing databases
//!
//! If obix's tables were previously applied via copied SQL recorded in
//! `_sqlx_migrations` (or another consumer's tracking table), the obix
//! tracking table starts empty and running these migrations fails with
//! "already exists": recreate the tables, or seed/skip the record (e.g.
//! `sqlx::migrate::Migrator::skip`).

use std::borrow::Cow;

use es_entity::errlanes::ResultExt;

#[cfg(feature = "default-tables")]
use crate::DefaultMailboxTables;
use crate::{MailboxTables, error::ObixFault};

const SETUP_SQL: &str = include_str!("../migrations_templates/20251204130225_obix_setup.sql");
const SETUP_VERSION: i64 = 20251204130225;
const SETUP_DESCRIPTION: &str = "obix setup";

fn migrator_for(table_prefix: &'static str, table_name: &'static str) -> sqlx::migrate::Migrator {
    use sqlx::{
        AssertSqlSafe, SqlSafeStr,
        migrate::{Migration, MigrationType, Migrator},
    };

    // The prefix comes from a `MailboxTables` implementation (a compile-time
    // literal validated by the derive), never from user input; each table set
    // gets its own migration so two outboxes can coexist in one database.
    let sql = SETUP_SQL.replace("{{prefix}}", table_prefix);
    let migrations = vec![Migration::new(
        SETUP_VERSION,
        Cow::Borrowed(SETUP_DESCRIPTION),
        MigrationType::Simple,
        AssertSqlSafe(sql).into_sql_str(),
        false,
    )];
    let mut migrator = Migrator::with_migrations(migrations);
    migrator.dangerous_set_table_name(table_name);
    migrator
}

/// The migrator for `T`'s tables only, recording them in `T::MIGRATION_TABLE`.
pub fn migrator<T: MailboxTables>() -> sqlx::migrate::Migrator {
    migrator_for(T::TABLE_PREFIX, T::MIGRATION_TABLE)
}

/// [`migrator`] for [`DefaultMailboxTables`] (tracking table
/// `_sqlx_migrations_obix`).
#[cfg(feature = "default-tables")]
pub static MIGRATOR: std::sync::LazyLock<sqlx::migrate::Migrator> =
    std::sync::LazyLock::new(migrator::<DefaultMailboxTables>);

/// Run `T`'s obix migrations, then the `job` crate's migrations (tracked in
/// `_sqlx_migrations_job`).
pub async fn run<T: MailboxTables>(pool: &sqlx::PgPool) -> Result<(), ObixFault> {
    migrator::<T>()
        .run(pool)
        .await
        .classify::<crate::error::Migrate>()?;
    job::MIGRATOR
        .run(pool)
        .await
        .classify::<crate::error::Migrate>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn only_migration(migrator: &sqlx::migrate::Migrator) -> &sqlx::migrate::Migration {
        let mut migrations = migrator.iter();
        let migration = migrations.next().expect("exactly one migration");
        assert!(migrations.next().is_none(), "exactly one migration");
        migration
    }

    #[cfg(feature = "default-tables")]
    #[test]
    fn default_tables_render_unprefixed_sql() {
        assert_eq!(DefaultMailboxTables::TABLE_PREFIX, "");
        assert_eq!(
            DefaultMailboxTables::MIGRATION_TABLE,
            "_sqlx_migrations_obix"
        );
        assert_eq!(MIGRATOR.table_name, "_sqlx_migrations_obix");

        let migration = only_migration(&MIGRATOR);
        assert_eq!(migration.version, SETUP_VERSION);
        assert_eq!(migration.description.as_ref(), SETUP_DESCRIPTION);
        let sql = migration.sql.as_str();
        assert!(!sql.contains("{{prefix}}"));
        for name in [
            "CREATE TABLE persistent_outbox_events (",
            "CREATE INDEX idx_persistent_outbox_events_commit_xid",
            "  ON persistent_outbox_events (commit_xid);",
            "CREATE TABLE persistent_outbox_events_p0 PARTITION OF persistent_outbox_events",
            "CREATE TABLE persistent_outbox_events_default",
            "  PARTITION OF persistent_outbox_events DEFAULT;",
            "CREATE TABLE persistent_outbox_commit_checkpoints (",
            "CREATE TABLE ephemeral_outbox_events (",
            "CREATE FUNCTION notify_ephemeral_outbox_events()",
            "    'ephemeral_outbox_events',",
            "CREATE TRIGGER ephemeral_outbox_events_notify",
            "  AFTER INSERT OR UPDATE ON ephemeral_outbox_events",
            "  FOR EACH ROW EXECUTE FUNCTION notify_ephemeral_outbox_events();",
            "CREATE TABLE inbox_events (",
            "CREATE INDEX idx_inbox_events_status ON inbox_events(status)",
            "CREATE TABLE subscriptions (",
            "CREATE INDEX idx_subscriptions_checkpoint ON subscriptions (checkpoint);",
            "CREATE INDEX idx_subscriptions_wake_keys ON subscriptions USING GIN (wake_keys);",
            "CREATE TABLE retired_subscriptions (",
            "CREATE TABLE joinable_tips (",
            "CREATE TABLE joinable_catch_ups (",
        ] {
            assert!(sql.contains(name), "missing unprefixed object: {name}");
        }
        assert!(
            sql.contains("CREATE TYPE InboxEventStatus AS ENUM"),
            "InboxEventStatus must be shared, not prefixed"
        );
        assert!(
            sql.contains("status InboxEventStatus NOT NULL DEFAULT 'pending'"),
            "column type must remain unprefixed"
        );
        assert!(
            !sql.contains("cala_"),
            "default rendering must be unprefixed"
        );
    }

    #[test]
    fn prefixed_tables_render_prefixed_sql_with_a_distinct_checksum() {
        let migrator = migrator_for("cala_", "_sqlx_migrations_obix_cala");
        assert_eq!(migrator.table_name, "_sqlx_migrations_obix_cala");

        let migration = only_migration(&migrator);
        assert_eq!(migration.version, SETUP_VERSION);
        assert_eq!(migration.description.as_ref(), SETUP_DESCRIPTION);
        let sql = migration.sql.as_str();
        assert!(!sql.contains("{{prefix}}"));
        for name in [
            "CREATE TABLE cala_persistent_outbox_events (",
            "CREATE INDEX cala_idx_persistent_outbox_events_commit_xid",
            "  ON cala_persistent_outbox_events (commit_xid);",
            "CREATE TABLE cala_persistent_outbox_events_p0 PARTITION OF cala_persistent_outbox_events",
            "CREATE TABLE cala_persistent_outbox_events_default",
            "  PARTITION OF cala_persistent_outbox_events DEFAULT;",
            "CREATE TABLE cala_persistent_outbox_commit_checkpoints (",
            "CREATE TABLE cala_ephemeral_outbox_events (",
            "CREATE FUNCTION cala_notify_ephemeral_outbox_events()",
            "    'cala_ephemeral_outbox_events',",
            "CREATE TRIGGER cala_ephemeral_outbox_events_notify",
            "  AFTER INSERT OR UPDATE ON cala_ephemeral_outbox_events",
            "  FOR EACH ROW EXECUTE FUNCTION cala_notify_ephemeral_outbox_events();",
            "CREATE TABLE cala_inbox_events (",
            "CREATE INDEX cala_idx_inbox_events_status ON cala_inbox_events(status)",
            "CREATE TABLE cala_subscriptions (",
            "CREATE INDEX cala_idx_subscriptions_checkpoint ON cala_subscriptions (checkpoint);",
            "CREATE INDEX cala_idx_subscriptions_wake_keys ON cala_subscriptions USING GIN (wake_keys);",
            "CREATE TABLE cala_retired_subscriptions (",
            "CREATE TABLE cala_joinable_tips (",
            "CREATE TABLE cala_joinable_catch_ups (",
        ] {
            assert!(sql.contains(name), "missing prefixed object: {name}");
        }
        assert!(
            sql.contains("CREATE TYPE InboxEventStatus AS ENUM"),
            "InboxEventStatus must be shared, not prefixed"
        );
        assert!(
            sql.contains("  status InboxEventStatus NOT NULL DEFAULT 'pending',"),
            "column type must remain unprefixed"
        );

        let default_migrator = migrator_for("", "_sqlx_migrations_obix");
        let default = only_migration(&default_migrator);
        assert_ne!(
            migration.checksum, default.checksum,
            "prefix changes the SQL, so the checksum must change too"
        );
    }
}
