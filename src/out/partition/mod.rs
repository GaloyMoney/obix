//! Partition maintenance for the RANGE-partitioned `persistent_outbox_events`
//! table (Stage 1 of the partitioning plan).
//!
//! The table is partitioned `BY RANGE (sequence)` with a `DEFAULT` backstop.
//! Two independent guarantees keep the synchronous write path total:
//!
//!   * the **`DEFAULT` partition** (shipped in the migration) means an INSERT
//!     can never fail to route — worst case a row lands in `DEFAULT`, which is
//!     still read normally; and
//!   * the **maintainer job** ([`job`]) keeps an explicit partition covering
//!     `[head, head + premake * width]` so `DEFAULT` stays empty in steady
//!     state.
//!
//! Correctness of the write path may **not** depend on the async maintainer —
//! the `DEFAULT` partition is what makes that true. The maintainer is a pure
//! *shape* optimisation: falling behind degrades layout (rows in `DEFAULT`),
//! never correctness. DDL is therefore kept entirely out of the commit path
//! (it takes locks and would wreck commit latency).
//!
//! [`Partitions`] holds the DDL primitives ([`ensure`](Partitions::ensure),
//! [`recover_default`](Partitions::recover_default)); the timer-driven job that
//! drives them lives in [`job`].

mod job;

pub use job::PartitionMaintainerConfig;
pub(crate) use job::{PartitionMaintainerJobData, PartitionMaintainerJobInitializer};

use std::marker::PhantomData;

use crate::config::DEFAULT_PARTITION_WIDTH;
use crate::tables::MailboxTables;

/// Per-partition storage parameters, applied on every partition
/// [`Partitions`] creates. These are **not** inherited from the parent on
/// `PARTITION OF`, so they must be set on each `CREATE` — kept in lock-step
/// with the `p0` partition shipped in the migration. A fixed insert *threshold*
/// (not the default 0.2 scale factor) keeps each partition vacuumed at a steady
/// cadence as it grows; `autovacuum_freeze_min_age = 0` freezes on the first
/// insert-driven vacuum, defusing anti-wraparound on an append-only table.
const PARTITION_STORAGE_PARAMS: &str = "autovacuum_vacuum_insert_scale_factor = 0.0, \
     autovacuum_vacuum_insert_threshold = 50000, \
     autovacuum_freeze_min_age = 0, \
     fillfactor = 100";

/// Maintains the RANGE partitions of `persistent_outbox_events`: caches the
/// pool and `premake` margin so the registration premake and the maintainer job
/// share one configured handle. Cheap to clone. Partition *width* is the fixed
/// [`DEFAULT_PARTITION_WIDTH`] constant (coupled to the migration's `p0`), not a
/// per-instance parameter.
pub struct Partitions<Tables = crate::tables::DefaultMailboxTables> {
    pool: sqlx::PgPool,
    premake: u64,
    _phantom: PhantomData<Tables>,
}

/// Serialize partition-creating sessions cluster-wide for the duration of
/// `tx`: `CREATE TABLE IF NOT EXISTS ... PARTITION OF` is **not**
/// concurrency-safe — two sessions that both observe the partition missing can
/// race, and the loser gets `DuplicateTable` / a catalog `unique_violation`.
/// Registration runs [`Partitions::ensure`] synchronously on every instance at
/// startup, so without this lock multi-instance deploys could fail
/// registration on some nodes and leave no maintainer running. Keyed on the
/// table name so different parents don't block each other; hash collisions
/// only cause spurious (harmless) serialization.
async fn partition_ddl_lock(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    table: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("obix:partition-ddl:{table}"))
        .execute(&mut **tx)
        .await?;
    Ok(())
}

impl<Tables> Clone for Partitions<Tables> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            premake: self.premake,
            _phantom: PhantomData,
        }
    }
}

impl<Tables> Partitions<Tables>
where
    Tables: MailboxTables,
{
    /// `premake` is how many partitions ahead of the head to keep created.
    pub fn new(pool: &sqlx::PgPool, premake: u64) -> Self {
        Self {
            pool: pool.clone(),
            premake,
            _phantom: PhantomData,
        }
    }

    /// Pre-create the explicit partitions covering
    /// `[head, head + premake * width]`.
    ///
    /// Idempotent — every partition is created with `CREATE TABLE IF NOT
    /// EXISTS`, so re-running is cheap and safe. Called both synchronously at
    /// registration (before serving traffic) and on every maintainer tick.
    /// Concurrent callers (multi-instance startup, a tick overlapping an
    /// operator repair) are serialized on a cluster-wide advisory lock
    /// (`partition_ddl_lock`), because `IF NOT EXISTS` alone does not make
    /// concurrent `PARTITION OF` creation safe.
    ///
    /// The partition covering index `k` spans `[k * width, (k + 1) * width)` and
    /// is named `{table}_p{k}`, tiling seamlessly onto the migration's
    /// `{table}_p0` (`[0, width)`).
    ///
    /// If rows have already spilled into `DEFAULT` (the maintainer fell behind),
    /// the `CREATE` for that range **fails** — Postgres validates that `DEFAULT`
    /// holds no rows in the new range and errors. `IF NOT EXISTS` does not help
    /// there: it guards "the partition exists", not "`DEFAULT` stole the range".
    /// That failure is intentional: it surfaces the stall as a failing job (the
    /// alert) rather than being silently absorbed. Run
    /// [`recover_default`](Self::recover_default) to repair.
    pub async fn ensure(&self) -> Result<(), sqlx::Error> {
        let table = Tables::persistent_outbox_events_table();
        let head = u64::from(Tables::highest_known_persistent_sequence(&self.pool).await?);
        let first = head / DEFAULT_PARTITION_WIDTH;
        let mut tx = self.pool.begin().await?;
        partition_ddl_lock(&mut tx, table).await?;
        for k in first..=first + self.premake {
            let lo = k * DEFAULT_PARTITION_WIDTH;
            let hi = (k + 1) * DEFAULT_PARTITION_WIDTH;
            let ddl = format!(
                "CREATE TABLE IF NOT EXISTS {table}_p{k} PARTITION OF {table} \
                 FOR VALUES FROM ({lo}) TO ({hi}) WITH ({PARTITION_STORAGE_PARAMS})",
            );
            sqlx::query(&ddl).execute(&mut *tx).await?;
        }
        tx.commit().await
    }

    /// Repair a non-empty `DEFAULT` partition: rows landed there because the
    /// maintainer fell behind. Nothing is broken while they sit (writes succeed,
    /// reads see them, gap-fill routes there, replay is intact) — this is a
    /// *layout* repair, not a *data* repair.
    ///
    /// Moves every stranded row into freshly-created explicit partitions in
    /// **one transaction**, so the parent's `MAX(sequence)` never regresses and
    /// no replaying reader sees a transient gap. Cost: it holds `ACCESS
    /// EXCLUSIVE` for the row move, so concurrent writes **block** (not fail) for
    /// its duration, which scales with the strand size (= maintainer downtime ×
    /// event rate). Therefore alert on `DEFAULT` row-count > 0 and run this while
    /// the strand is tiny (a sub-second stall).
    ///
    /// This is **not** invoked automatically by the maintainer (Stage 1
    /// decision: runbook + alert first, automate only if it recurs). It is
    /// exposed for operators and exercised by the test suite. Idempotent: a
    /// no-op when `DEFAULT` is already empty.
    pub async fn recover_default(&self) -> Result<(), sqlx::Error> {
        let table = Tables::persistent_outbox_events_table();
        let default_child = format!("{table}_default");
        let default_old = format!("{table}_default_old");

        // Clean up the artifact of an earlier repair that crashed before its
        // final DROP (only possible from a run predating the in-transaction
        // DROP below): it would otherwise collide with the RENAME below and
        // block the repair entirely. Such a leftover is guaranteed drained —
        // the row move commits in the same transaction as the rename — so
        // dropping it loses nothing.
        sqlx::query(&format!("DROP TABLE IF EXISTS {default_old}"))
            .execute(&self.pool)
            .await?;

        // Range of stranded sequences. `MIN`/`MAX` are NULL when DEFAULT is
        // empty — nothing to repair.
        let bounds = sqlx::query(&format!(
            "SELECT MIN(sequence) AS lo, MAX(sequence) AS hi FROM {default_child}"
        ))
        .fetch_one(&self.pool)
        .await?;
        use sqlx::Row;
        let (Some(min_seq), Some(max_seq)) = (
            bounds.try_get::<Option<i64>, _>("lo")?,
            bounds.try_get::<Option<i64>, _>("hi")?,
        ) else {
            return Ok(());
        };
        let min_k = (min_seq as u64) / DEFAULT_PARTITION_WIDTH;
        // Cover the stranded rows AND stay `premake` partitions ahead of the
        // head (the top of the strand is the head), so the maintainer's next
        // tick has nothing to do and steady state resumes immediately.
        let max_k = (max_seq as u64) / DEFAULT_PARTITION_WIDTH + self.premake;

        // One transaction: detach DEFAULT (so the explicit CREATEs have no
        // default to validate against and no overlap), create the covering
        // partitions and a fresh DEFAULT, then move the stranded rows back into
        // the parent — where they route into the new explicit partitions. A
        // two-phase (detach, then move online) would leave the top-of-log rows
        // detached during the move and MAX(sequence) would regress. The
        // advisory lock serializes the CREATEs against concurrent `ensure`
        // ticks (see `partition_ddl_lock`).
        let mut tx = self.pool.begin().await?;
        partition_ddl_lock(&mut tx, table).await?;
        sqlx::query(&format!(
            "ALTER TABLE {table} DETACH PARTITION {default_child}"
        ))
        .execute(&mut *tx)
        .await?;
        sqlx::query(&format!(
            "ALTER TABLE {default_child} RENAME TO {table}_default_old"
        ))
        .execute(&mut *tx)
        .await?;
        for k in min_k..=max_k {
            let lo = k * DEFAULT_PARTITION_WIDTH;
            let hi = (k + 1) * DEFAULT_PARTITION_WIDTH;
            sqlx::query(&format!(
                "CREATE TABLE IF NOT EXISTS {table}_p{k} PARTITION OF {table} \
                 FOR VALUES FROM ({lo}) TO ({hi}) WITH ({PARTITION_STORAGE_PARAMS})",
            ))
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query(&format!(
            "CREATE TABLE {table}_default PARTITION OF {table} DEFAULT"
        ))
        .execute(&mut *tx)
        .await?;
        // `SELECT *` supplies `sequence` explicitly (no nextval), preserving
        // every stranded row's position.
        sqlx::query(&format!(
            "WITH moved AS (DELETE FROM {default_old} RETURNING *) \
             INSERT INTO {table} SELECT * FROM moved"
        ))
        .execute(&mut *tx)
        .await?;
        // Drop the drained artifact in the same transaction: atomic with the
        // repair, so a crash can never leave `{table}_default_old` behind to
        // collide with the next repair's RENAME.
        sqlx::query(&format!("DROP TABLE {default_old}"))
            .execute(&mut *tx)
            .await?;
        tx.commit().await
    }
}
