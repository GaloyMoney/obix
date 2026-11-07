//! Joinable subscriptions — one handler run by a **tip** (the live writer)
//! and by any number of **catch-up lanes** that join it at a fence.
//!
//! A consumer that folds many units in one pass cannot add a unit with
//! history without a second cursor: the tip is at `C`, the new unit needs the
//! stream from an earlier position. obix owns that seam. A lane is born at an
//! earlier position, consumes through the same handler, and ends in a single
//! landing at a **fence** `F` such that no tip landing straddles `F`: every
//! tip landing either began after the fenced landing committed, or committed
//! before the fence was read. The handler learns which side of the fence each
//! landing is on from [`Scope`]; it never decides it.
//!
//! The tip's landing takes the tip row's lock as the first statement of its
//! transaction, before [`flush`](JoinableSubscriber::flush) runs. Lane birth
//! and the fenced landing take the same lock. A commit-time lock would not do:
//! a tip landing could decide coverage on a snapshot from before the fence and
//! commit after it. The placement at landing **entry** is the whole point, and
//! it is obix's to enforce, not the handler's.

mod runner;

use es_entity::errlanes::{Fail, lanes};
use serde::{Serialize, de::DeserializeOwned};
use std::sync::{Arc, OnceLock};

use job::{JobType, Jobs, RetrySettings};

use crate::error::ObixFault;
use crate::out::Outbox;
use crate::out::ctx::{FlushOp, Handled, JoinableEventCtx};
use crate::out::error::JoinableFault;
use crate::out::event::{EventDelivery, UndecodableDelivery};
use crate::out::lane::InsertOrder;
use crate::out::subscription::singleton::{OutboxEventJobConfig, OutboxEventJobData};
use crate::out::subscription::{Subscription, SubscriptionRejection, SubscriptionSnapshot};
use crate::sequence::EventSequence;
use crate::tables::{DefaultMailboxTables, MailboxTables};

const DEFAULT_LANE_MAX_BATCH_SIZE: usize = 100;

/// A handler run by a tip and by catch-up lanes.
///
/// # Contract
///
/// 1. [`flush`](Self::flush) must tolerate whole-batch replay, as everywhere.
/// 2. `flush` may be called with an **empty batch** on a lane's fenced
///    landing: the lane caught up on skips alone and the runner lands anyway.
///    Liveness is the runner's job, not the handler's.
/// 3. Seam state is read **on `op`** — the handler's own columns written by a
///    lane's fenced `flush`, or [`JoinableHandle::lanes_in_op`] — never from
///    the pool or from a cache across landings. Any read on the landing op is
///    after the entry lock by construction; a pool read is not.
/// 4. Coverage: apply an event to a unit only if `sequence > fence` once
///    joined, and never while the unit's lane is live. With a lane ahead of
///    the tip, the fence is the lane's own position and the tip's next
///    landings contain events at or below it that belong to the lane.
///
/// One handler instance serves both runners; the unit arrives with the
/// landing. [`handle`](Self::handle) is the same on both sides, and its ctx
/// offers [`skip`](JoinableEventCtx::skip) and
/// [`collect_with`](JoinableEventCtx::collect_with) only.
pub trait JoinableSubscriber<P>: Send + Sync + 'static
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    /// What one catch-up lane covers, persisted as the lane's instance
    /// config.
    type Unit: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Accumulator for events resolved via
    /// [`collect_with`](JoinableEventCtx::collect_with).
    type Batch: Default + Send + 'static;

    /// Identical on the tip and on every lane.
    fn handle<'inv>(
        &self,
        ctx: JoinableEventCtx<'inv, Self::Batch>,
        event: &EventDelivery<P>,
    ) -> impl std::future::Future<
        Output = Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>>,
    > + Send;

    /// One landing. `scope` says whose, and — for a lane — whether this is
    /// its final landing and where it ends.
    fn flush(
        &self,
        op: &mut FlushOp<'_, InsertOrder>,
        scope: Scope<'_, Self::Unit>,
        batch: Self::Batch,
    ) -> impl std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send;

    /// Same default and semantics as
    /// [`SingletonSubscriber::handle_undecodable`](crate::out::SingletonSubscriber::handle_undecodable):
    /// fails with the error as-is, parking the cursor before the poison event.
    fn handle_undecodable(
        &self,
        error: &UndecodableDelivery,
    ) -> impl std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send
    {
        let error = error.inner().clone();
        async move { Err(error.into()) }
    }
}

/// Whose landing a [`JoinableSubscriber::flush`] is.
#[derive(Debug)]
pub enum Scope<'a, U> {
    /// The live writer.
    Tip,
    /// One catch-up lane. `fence` is `Some(F)` on that lane's final landing
    /// only: every event at or below `F` has been handled by this lane, and
    /// the tip has not committed any landing past `F`.
    CatchUp {
        unit: &'a U,
        fence: Option<EventSequence>,
    },
}

/// Tuning for one joinable registration.
#[derive(Clone)]
pub struct JoinableConfig {
    /// Batch size, checkpoint interval and retry of the tip, as for a
    /// singleton. Its `job_type` is ignored: a joinable's identity is the job
    /// type its [`JoinableHandle`] was addressed with.
    pub tip: OutboxEventJobConfig,
    /// Backstop on how many collected events may share one lane batch.
    /// Default 100.
    pub lane_max_batch_size: usize,
    /// Backoff for a lane whose handler errors. Default
    /// [`RetrySettings::default`].
    pub lane_retry_settings: RetrySettings,
    /// Cap on how many lanes run concurrently on one process. `None` defers
    /// to the job crate's own default.
    pub max_concurrent_lanes_per_process: Option<usize>,
    /// A fenced landing drains at most this many sequences past the lane's
    /// position. If the tip is further ahead under the lock, the landing is an
    /// ordinary one (fence `None`, lock held for this landing only) and the
    /// lane keeps catching up. Bounds the tip stall by construction;
    /// correctness never depends on it. Defaults to
    /// [`lane_max_batch_size`](Self::lane_max_batch_size).
    pub max_fence_gap: u64,
}

impl JoinableConfig {
    pub fn new(tip: OutboxEventJobConfig) -> Self {
        Self {
            tip,
            lane_max_batch_size: DEFAULT_LANE_MAX_BATCH_SIZE,
            lane_retry_settings: RetrySettings::default(),
            max_concurrent_lanes_per_process: None,
            max_fence_gap: DEFAULT_LANE_MAX_BATCH_SIZE as u64,
        }
    }

    pub fn with_lane_max_batch_size(mut self, lane_max_batch_size: usize) -> Self {
        self.lane_max_batch_size = lane_max_batch_size.max(1);
        self.max_fence_gap = self.lane_max_batch_size as u64;
        self
    }

    pub fn with_lane_retry_settings(mut self, settings: RetrySettings) -> Self {
        self.lane_retry_settings = settings;
        self
    }

    pub fn with_max_concurrent_lanes_per_process(mut self, n: usize) -> Self {
        self.max_concurrent_lanes_per_process = Some(n);
        self
    }

    pub fn with_max_fence_gap(mut self, max_fence_gap: u64) -> Self {
        self.max_fence_gap = max_fence_gap;
        self
    }
}

struct Registered<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    tip: Subscription<P, InsertOrder, Tables>,
}

struct Shared<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    outbox: Outbox<P, Tables>,
    job_type: JobType,
    registered: OnceLock<Registered<P, Tables>>,
}

/// An address for one joinable subscription, keyed by the tip's job type.
///
/// Cheap to clone and usable before [`spawn`](Self::spawn) — it holds no
/// registration of its own, so a service that starts catch-ups can hold it
/// without depending on the handler that depends on the service. It is
/// parameterised by the **unit**, not the handler type, for that reason.
/// Calls that need the registration fail with a `Fatal` fault until `spawn`
/// has run.
pub struct JoinableHandle<P, U, Tables = DefaultMailboxTables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    shared: Arc<Shared<P, Tables>>,
    _unit: std::marker::PhantomData<fn() -> U>,
}

impl<P, U, Tables> Clone for JoinableHandle<P, U, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            _unit: std::marker::PhantomData,
        }
    }
}

impl<P, Tables> Outbox<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    /// An address for the joinable subscription whose tip runs as
    /// `job_type`. An address, not a registration: usable before
    /// [`spawn`](JoinableHandle::spawn), `Clone`, cheap.
    pub fn joinable<U>(&self, job_type: JobType) -> JoinableHandle<P, U, Tables> {
        JoinableHandle {
            shared: Arc::new(Shared {
                outbox: self.clone(),
                job_type,
                registered: OnceLock::new(),
            }),
            _unit: std::marker::PhantomData,
        }
    }
}

impl<P, U, Tables> JoinableHandle<P, U, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    /// Register the tip for `handler`. Must run before
    /// [`Jobs::start_poll`](job::Jobs::start_poll), like the other
    /// registrations.
    ///
    /// Idempotent per job type, as
    /// [`register_singleton_subscriber`](Outbox::register_singleton_subscriber)
    /// is.
    #[es_entity::errlanes::instrument(name = "obix.joinable.spawn", skip_all)]
    pub async fn spawn<S>(
        &self,
        jobs: &mut Jobs,
        config: JoinableConfig,
        handler: S,
    ) -> Result<(), ObixFault>
    where
        S: JoinableSubscriber<P, Unit = U>,
    {
        let shared = &self.shared;
        Tables::ensure_tip_row(shared.outbox.pool(), shared.job_type.as_str()).await?;

        let initializer = runner::TipJobInitializer::<S, P, Tables>::new(
            shared.outbox.clone(),
            handler,
            shared.job_type.clone(),
            &config,
        );
        let spawner = jobs.add_resident_initializer(initializer);
        let job = spawner.spawn(OutboxEventJobData::default()).await?;

        let tip = Subscription::new(job, shared.outbox.pool().clone(), None);
        let _ = shared.registered.set(Registered { tip });
        Ok(())
    }

    fn registered(&self) -> Result<&Registered<P, Tables>, JoinableFault> {
        self.shared
            .registered
            .get()
            .ok_or_else(|| JoinableFault::NotSpawned {
                job_type: self.shared.job_type.clone(),
            })
    }

    /// The tip's checkpoint, frontier and job status.
    #[es_entity::errlanes::instrument(name = "obix.joinable.load", skip_all)]
    pub async fn load(
        &self,
    ) -> Result<SubscriptionSnapshot, Fail<SubscriptionRejection, lanes!(Transient, Fatal)>> {
        self.registered()?.tip.load().await
    }
}
