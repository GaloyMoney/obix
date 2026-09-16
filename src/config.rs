use derive_builder::Builder;
use es_entity::clock::{Clock, ClockHandle};
use serde::{Deserialize, Serialize};

pub const DEFAULT_PERSIST_EVENTS_BATCH_SIZE: usize = 5000;

/// How long the persistent cache waits before the first gap-fill attempt
/// for a stalled broadcast sequence. Most gaps are in-flight transactions
/// that committed out of sequence-allocation order and resolve on their own
/// within a few ms (and this process's own commit-failed allocations are
/// compensated reactively, never waiting for a fill attempt at all);
/// attempting immediately wastes a page re-read per gap. At high posting
/// rates (hundreds of sequences/s) there is nearly always an in-flight
/// frontier gap, so a sub-second grace is guaranteed-too-short and turns
/// the fill into a permanent background load — hence seconds, not
/// milliseconds. Attempts are read-only until the missing sequences are
/// *provably abandoned* (every transaction that could have produced them
/// has ended — see `MailboxTables::abandonment_proof_passed`), so a fill
/// can never collide with a live writer regardless of this setting.
///
/// This grace no longer paces draining a backlog of *committed* rows — the
/// feeder drains those from memory, or reads them at DB speed. It governs the
/// hole case exactly as before.
pub const DEFAULT_GAP_FILL_GRACE: std::time::Duration = std::time::Duration::from_secs(2);

/// Ceiling on the rows a single backfill page read may return, and the width
/// of a gap-fill stall episode's window (an episode's re-read is a page read,
/// so one episode covers what one page read can see).
///
/// A query-shape knob, distinct from `event_cache_size` (how far behind a
/// listener may fall and still be served from memory) and `event_buffer_size`
/// (how much may be in flight): this bounds how long a catch-up read holds a
/// pooled connection and how large a result set it materialises. Keep it well
/// under `event_cache_size`, so a listener that consumes a page lands back
/// inside the memory window instead of paging forever.
///
/// It is also the depth of the channel a backfill delivers through, so it
/// bounds how far the reader may run ahead of its consumer — one page in
/// flight while the next is fetched. Sizing that channel by `event_buffer_size`
/// instead would let the reader outrun a slow consumer by a whole second
/// buffer's worth of events, all of it read from the database and then evicted.
pub const DEFAULT_BACKFILL_PAGE_SIZE: usize = 1000;

/// Maximum number of placeholder rows a single gap-fill query may insert.
/// Bounds the worst case (a mass rollback or long outage leaving thousands
/// of lost sequences) to a small, predictable statement instead of one
/// giant insert; the fixed 1s retry cadence picks up the remainder, so a
/// cap delays recovery of a pathological backlog without ever losing
/// sequences.
///
/// This caps one *insert*, not an episode: the episode window is the
/// [`backfill page`](DEFAULT_BACKFILL_PAGE_SIZE), so a gap wider than this cap
/// fills across several passes of one episode — one grace period, one marker,
/// batches at the loop cadence.
pub const DEFAULT_GAP_FILL_BATCH_LIMIT: usize = 1000;

/// How long the per-process notifier coalesces committed-batch reports
/// before emitting one `pg_notify` wake-up hint. Notify-bearing commits
/// serialize on a cluster-wide lock, so app transactions no longer notify;
/// this bounds the added cross-process wake-up latency (in-process delivery
/// is unaffected).
pub const DEFAULT_NOTIFY_DEBOUNCE: std::time::Duration = std::time::Duration::from_millis(25);

/// How long the persistent cache goes without authoritative progress (a
/// newly-seen committed row or a confirmed head read) before polling the
/// sequence head (the O(1) `last_value` query). Backstops lost wake-ups: a
/// writer crashing between commit and notify, a dead remote notifier, or
/// external writers that never notify.
pub const DEFAULT_IDLE_RESYNC_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);

/// Width, in `sequence` units, of each `persistent_outbox_events` partition.
///
/// A fixed schema constant, deliberately **not** runtime-configurable: it must
/// equal the range of the initial `p0` partition hard-coded in the migration
/// (`[0, 2_000_000)`), or the maintainer would create partitions that overlap
/// `p0`. Changing the size means editing both this constant and the migration
/// together.
///
/// Sized from real event data: outbox rows measure ~760 B each (payloads
/// average a few hundred bytes and rarely TOAST), so 2M rows ≈ ~1.5 GB per
/// partition — small enough that the hot partition stays cache-resident and a
/// per-partition vacuum is quick, while keeping the partition count low.
pub const DEFAULT_PARTITION_WIDTH: u64 = 2_000_000;

/// How many partitions ahead of the current sequence head the maintainer
/// keeps created — including on the initial synchronous `ensure` at
/// registration, so a fresh install starts with a multi-partition runway
/// (the migration itself ships only `p0` + `DEFAULT`). `premake * width` must
/// comfortably exceed the events produced between two maintainer ticks so the
/// head never reaches the last pre-made boundary (which would spill into the
/// `DEFAULT` partition). Empty partitions are cheap, so this errs generous:
/// 5 * 2M = 10M sequences of headroom by default.
pub const DEFAULT_PARTITION_PREMAKE: u64 = 5;

/// How often the partition maintainer wakes to pre-create partitions ahead of
/// the head. Each tick is idempotent (`CREATE ... IF NOT EXISTS`), so this is
/// a cheap steady-state poll; premake margin, not cadence, is the safety
/// budget against bursts.
pub const DEFAULT_PARTITION_MAINTAINER_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(3600);

/// How many groups the commit-lane fold emits between sparse checkpoints.
///
/// The commit order is computed, not stored, so a checkpoint is only a
/// shortcut: it bounds how far a restarting process re-folds before it is
/// live, and how far a lagging subscriber's backfill re-folds below the
/// position it actually wants (the overshoot is read but never sent). The
/// cost is one small row per interval per process — against one locked
/// INSERT per source transaction under the superseded materialised log.
pub const DEFAULT_COMMIT_CHECKPOINT_EVERY: usize = 1_000;

/// Longest the commit-lane fold goes without a checkpoint while it is
/// emitting, regardless of group count; see
/// [`DEFAULT_COMMIT_CHECKPOINT_EVERY`]. Bounds the re-fold on a quiet
/// outbox, where the group count alone would leave the last checkpoint far
/// behind.
pub const DEFAULT_COMMIT_CHECKPOINT_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(5);

/// Whether this outbox runs the commit-ordered lane.
///
/// The lane costs one commit-log append per source transaction plus a fold
/// over every event, in every process that runs the outbox, so it is opt-in:
/// an embedder that never consumes commit order (cala's embedded outbox, say)
/// should not pay for it.
///
/// # Enabling later is safe, and costs only time
///
/// Placement is a pure function of the persisted table — a group is placed at
/// first sight of its lowest member, over a contiguous gap-filled stream — so
/// the commit log a sequencer computes does not depend on *when* it ran. A
/// process that enables the lane in year two resumes from
/// `persistent_outbox_commit_log_state.logged_through_sequence` (`0` on a
/// database where the lane never ran) and sequences the full history into the
/// same order a sequencer running from day one would have produced, page by
/// page, resumable at any point. A `CommitOrder` subscriber registered
/// alongside simply trails the fold.
///
/// The two things that make that possible stay on regardless of this setting:
/// the `commit_group` stamped on every event, and the commit log's partitions
/// kept in lock-step by the maintainer.
///
/// obix ships no retention. If an operator has dropped event partitions, a
/// from-zero enable gap-fills the dropped range with placeholders; seed the
/// floor once before enabling on such a database —
/// `UPDATE persistent_outbox_commit_log_state SET logged_through_sequence =
/// <first retained sequence − 1> WHERE singleton AND logged_through_sequence = 0`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitLane {
    /// No sequencer runs in this process for this outbox. Registering a
    /// [`CommitOrder`](crate::CommitOrder) subscriber or calling
    /// `listen_commit_ordered` fails with [`CommitLaneDisabled`].
    #[default]
    Disabled,
    /// This process sequences the commit lane and can host `CommitOrder`
    /// subscribers.
    Enabled,
}

/// Why a lane's frontier could not be read.
///
/// Two lanes, two sources: the insert lane reads the sequence generator and
/// can fail like any query; the commit lane reads this process's fold head,
/// which does not exist when the lane is off.
#[derive(Debug, thiserror::Error)]
pub enum FrontierError {
    #[error("FrontierError - Sqlx: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("FrontierError - {0}")]
    CommitLaneDisabled(#[from] CommitLaneDisabled),
}

/// The commit lane is off for this outbox.
///
/// Raised at registration — before any job is spawned — and by
/// `listen_commit_ordered`, so a consumer that needs the lane fails loudly at
/// startup rather than stalling on a stream that will never advance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "the commit lane is disabled on this outbox — set MailboxConfig::commit_lane = CommitLane::Enabled"
)]
pub struct CommitLaneDisabled;

#[derive(Clone, Builder)]
pub struct MailboxConfig {
    #[builder(default = "100")]
    pub event_buffer_size: usize,
    #[builder(default = "1000")]
    pub event_cache_size: usize,
    #[builder(default = "10")]
    pub event_cache_trim_percent: u8,
    #[builder(default = "DEFAULT_PERSIST_EVENTS_BATCH_SIZE")]
    pub persist_events_batch_size: usize,
    /// Rows a single backfill page read may return; see
    /// [`DEFAULT_BACKFILL_PAGE_SIZE`]. The reader waits for the consumer to
    /// have room before issuing a read, but the read itself is this size.
    #[builder(default = "DEFAULT_BACKFILL_PAGE_SIZE")]
    pub backfill_page_size: usize,
    /// Grace period before the first proactive gap-fill attempt for a
    /// stalled broadcast sequence; see [`DEFAULT_GAP_FILL_GRACE`]. Retries
    /// after a fill attempt are unaffected (fixed 1s interval).
    #[builder(default = "DEFAULT_GAP_FILL_GRACE")]
    pub gap_fill_grace: std::time::Duration,
    /// Maximum placeholder rows a single gap-fill query may insert; see
    /// [`DEFAULT_GAP_FILL_BATCH_LIMIT`]. The remainder is picked up by
    /// subsequent attempts — sequences are never skipped, only filled
    /// later.
    #[builder(default = "DEFAULT_GAP_FILL_BATCH_LIMIT")]
    pub gap_fill_batch_limit: usize,
    /// Coalescing window of the per-process debounced notifier; see
    /// [`DEFAULT_NOTIFY_DEBOUNCE`]. Deliberately no per-commit escape hatch.
    #[builder(default = "DEFAULT_NOTIFY_DEBOUNCE")]
    pub notify_debounce: std::time::Duration,
    /// Progress-silence threshold before the persistent cache polls the
    /// sequence head; see [`DEFAULT_IDLE_RESYNC_INTERVAL`].
    #[builder(default = "DEFAULT_IDLE_RESYNC_INTERVAL")]
    pub idle_resync_interval: std::time::Duration,
    /// How many partitions ahead of the head the maintainer keeps created;
    /// see [`DEFAULT_PARTITION_PREMAKE`]. (Partition *width* is the fixed
    /// [`DEFAULT_PARTITION_WIDTH`] constant, not configurable — it is coupled to
    /// the migration's `p0` range.)
    #[builder(default = "DEFAULT_PARTITION_PREMAKE")]
    pub partition_premake: u64,
    /// Poll interval of the partition maintainer job; see
    /// [`DEFAULT_PARTITION_MAINTAINER_INTERVAL`].
    #[builder(default = "DEFAULT_PARTITION_MAINTAINER_INTERVAL")]
    pub partition_maintainer_interval: std::time::Duration,
    /// Whether this outbox runs the commit-ordered lane; see [`CommitLane`].
    /// Defaults to [`Disabled`](CommitLane::Disabled).
    #[builder(default)]
    pub commit_lane: CommitLane,
    /// Groups between sparse checkpoints of the commit-lane fold; see
    /// [`DEFAULT_COMMIT_CHECKPOINT_EVERY`]. Ignored when the lane is
    /// `Disabled`.
    #[builder(default = "DEFAULT_COMMIT_CHECKPOINT_EVERY")]
    pub commit_checkpoint_every: usize,
    /// Longest the commit-lane fold goes without a checkpoint while
    /// emitting; see [`DEFAULT_COMMIT_CHECKPOINT_INTERVAL`].
    #[builder(default = "DEFAULT_COMMIT_CHECKPOINT_INTERVAL")]
    pub commit_checkpoint_interval: std::time::Duration,
    #[builder(default = "Clock::handle().clone()")]
    pub clock: ClockHandle,
}

impl MailboxConfig {
    pub fn builder() -> MailboxConfigBuilder {
        MailboxConfigBuilder::default()
    }
}
