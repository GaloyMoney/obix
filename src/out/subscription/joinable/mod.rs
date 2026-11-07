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

mod lane;
mod runner;

use es_entity::errlanes::{Fail, ResultExt, lanes};
use serde::{Serialize, de::DeserializeOwned};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use job::{JobType, Jobs, RetrySettings};

use self::lane::{LaneJobInitializer, LaneMsg};
use crate::error::ObixFault;
use crate::out::Outbox;
use crate::out::ctx::{FlushOp, Handled, JoinableEventCtx};
use crate::out::error::{CouldNotDecodeStored as Undecodable, JoinableFault};
use crate::out::event::{EventDelivery, UndecodableDelivery};
use crate::out::lane::InsertOrder;
use crate::out::subscription::singleton::{OutboxEventJobConfig, OutboxEventJobData};
use crate::out::subscription::{
    INITIAL_POLL_INTERVAL, MAX_POLL_INTERVAL, Subscription, SubscriptionRejection,
    SubscriptionSnapshot,
};
use crate::sequence::EventSequence;
use crate::tables::{DefaultMailboxTables, MailboxTables};

es_entity::entity_id! { LaneId }

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
    jobs: Jobs,
    lane_job_type: JobType,
    lane_spawner: job::KeyedJobSpawner<LaneMsg>,
}

/// The lanes of one joinable as of a landing op, from
/// [`JoinableHandle::lanes_in_op`].
#[derive(Debug, Clone)]
pub struct Lanes<U> {
    /// Lanes that have not joined yet: the tip must not apply their units.
    pub catching_up: Vec<(LaneId, U)>,
    /// Lanes that joined at a fence at or above the `since` asked for: the
    /// tip applies their units from the event after the fence.
    pub joined: Vec<(LaneId, U, EventSequence)>,
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
    /// Register the tip (a resident job under the handle's job type) and the
    /// lane job type (a keyed job under `"<job type>.lane"`) for `handler`.
    /// Must run before [`Jobs::start_poll`](job::Jobs::start_poll), like the
    /// other registrations.
    ///
    /// Idempotent per job type, as
    /// [`register_singleton_subscriber`](Outbox::register_singleton_subscriber)
    /// is. The lane job type's name is leaked once per call: the job crate
    /// only accepts `&'static str`.
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

        let handler = Arc::new(handler);
        let tip_initializer = runner::TipJobInitializer::<S, P, Tables>::new(
            shared.outbox.clone(),
            handler.clone(),
            shared.job_type.clone(),
            &config,
        );
        let tip_spawner = jobs.add_resident_initializer(tip_initializer);

        let lane_job_type = JobType::new(Box::leak(
            format!("{}.lane", shared.job_type).into_boxed_str(),
        ));
        let lane_initializer = LaneJobInitializer::<S, P, Tables>::new(
            shared.outbox.clone(),
            handler,
            shared.job_type.clone(),
            lane_job_type.clone(),
            &config,
        );
        let lane_spawner = jobs.add_keyed_initializer(lane_initializer);

        let job = tip_spawner.spawn(OutboxEventJobData::default()).await?;
        let tip = Subscription::new(job, shared.outbox.pool().clone(), None);
        let _ = shared.registered.set(Registered {
            tip,
            jobs: jobs.clone(),
            lane_job_type,
            lane_spawner,
        });
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

    /// Re-derive a lane handle from a stored id.
    pub fn lane(&self, id: LaneId) -> CatchUpLane<P, Tables> {
        CatchUpLane {
            shared: self.shared.clone(),
            id,
        }
    }

    /// Lane state as of `op`. Inside a tip `flush` this is read under the
    /// entry lock and is the contract for consumers that keep no seam state of
    /// their own: skip units in `catching_up`, apply `> fence` for units in
    /// `joined`. `joined` holds joins with `fence >= since`; a tip passes its
    /// previous checkpoint, which bounds the list by construction.
    #[es_entity::errlanes::instrument(name = "obix.joinable.lanes_in_op", skip_all)]
    pub async fn lanes_in_op(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        since: EventSequence,
    ) -> Result<Lanes<U>, ObixFault>
    where
        U: DeserializeOwned,
    {
        let registered = self.registered()?;
        let rows = {
            let fut: std::pin::Pin<
                Box<
                    dyn std::future::Future<Output = Result<crate::tables::LaneRows, sqlx::Error>>
                        + Send
                        + '_,
                >,
            > = Box::pin(Tables::lanes_in_op(
                &mut *op,
                self.shared.job_type.as_str(),
                registered.lane_job_type.as_str(),
                since,
            ));
            fut.await?
        };
        let lane_job_type = &registered.lane_job_type;
        let decode = |key: String, stored: serde_json::Value| -> Result<(LaneId, U), ObixFault> {
            let id: LaneId = key
                .parse()
                .ok()
                .ok_or(Undecodable::Key {
                    job_type: lane_job_type.clone(),
                })
                .into_fault()?;
            let unit: U = serde_json::from_value(stored)
                .map_err(Undecodable::InstanceConfig)
                .into_fault()?;
            Ok((id, unit))
        };
        let mut lanes = Lanes {
            catching_up: Vec::with_capacity(rows.live.len()),
            joined: Vec::with_capacity(rows.joined.len()),
        };
        for (key, stored) in rows.live {
            lanes.catching_up.push(decode(key, stored)?);
        }
        for (key, stored, fence) in rows.joined {
            let (id, unit) = decode(key, stored)?;
            lanes.joined.push((id, unit, fence));
        }
        Ok(lanes)
    }

    /// Start a catch-up for `unit` from `start_after` (exclusive), in the
    /// caller's op — the one that creates the unit. Takes the tip's lock
    /// exclusively as the first statement on `op`, so a tip landing sees the
    /// unit and its lane together or not at all, and holds it until `op`
    /// commits. A birth above the stream frontier is rejected with
    /// [`SubscriptionRejection::BirthAheadOfFrontier`].
    #[es_entity::errlanes::instrument(name = "obix.joinable.join_in_op", skip_all)]
    pub async fn join_in_op(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        unit: U,
        start_after: EventSequence,
    ) -> Result<CatchUpLane<P, Tables>, Fail<SubscriptionRejection, lanes!(Transient, Fatal)>>
    where
        U: Serialize,
    {
        let registered = self.registered()?;
        let tip_type = self.shared.job_type.as_str();

        {
            let fut: std::pin::Pin<
                Box<
                    dyn std::future::Future<Output = Result<Option<EventSequence>, sqlx::Error>>
                        + Send
                        + '_,
                >,
            > = Box::pin(Tables::lock_tip_in_op(&mut *op, tip_type));
            fut.await?;
        }

        let id = LaneId::new();
        let key = id.to_string();
        let lane_type = registered.lane_job_type.as_str();
        let retired = {
            let fut: std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<bool, sqlx::Error>> + Send + '_>,
            > = Box::pin(Tables::subscription_retired_in_op(
                &mut *op, lane_type, &key,
            ));
            fut.await?
        };
        if retired {
            return Err(Fail::Rejected(SubscriptionRejection::KeyRetired {
                subscriber_type: lane_type.to_string(),
                key,
            }));
        }
        let instance_config = serde_json::to_value(&unit)?;
        let frontier = {
            let fut: std::pin::Pin<
                Box<
                    dyn std::future::Future<Output = Result<EventSequence, sqlx::Error>>
                        + Send
                        + '_,
                >,
            > = Box::pin(Tables::highest_known_persistent_sequence(&mut *op));
            fut.await?
        };
        if start_after > frontier {
            return Err(Fail::Rejected(
                SubscriptionRejection::BirthAheadOfFrontier {
                    start_after,
                    frontier,
                },
            ));
        }

        Tables::insert_subscription_in_op(
            op,
            lane_type,
            &key,
            &[LANE_WAKE_KEY.to_string()],
            instance_config,
            start_after,
        )
        .await?;
        registered
            .lane_spawner
            .spawn_in_op(op, key.clone(), LaneMsg { lane: key })
            .await?;

        Ok(CatchUpLane {
            shared: self.shared.clone(),
            id,
        })
    }
}

/// The wake key every lane row carries: the table requires one, and a lane
/// never passivates, so nothing ever consults it.
const LANE_WAKE_KEY: &str = "lane";

/// One catch-up lane: an address derived from its id, cheap to clone and to
/// re-derive with [`JoinableHandle::lane`].
pub struct CatchUpLane<P, Tables = DefaultMailboxTables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    shared: Arc<Shared<P, Tables>>,
    id: LaneId,
}

impl<P, Tables> Clone for CatchUpLane<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            id: self.id,
        }
    }
}

impl<P, Tables> CatchUpLane<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    pub fn id(&self) -> LaneId {
        self.id
    }

    fn registered(&self) -> Result<&Registered<P, Tables>, JoinableFault> {
        self.shared
            .registered
            .get()
            .ok_or_else(|| JoinableFault::NotSpawned {
                job_type: self.shared.job_type.clone(),
            })
    }

    /// The lane's checkpoint, frontier and job status. A joined lane's final
    /// execution state stays readable.
    #[es_entity::errlanes::instrument(name = "obix.joinable.lane_load", skip_all)]
    pub async fn load(
        &self,
    ) -> Result<SubscriptionSnapshot, Fail<SubscriptionRejection, lanes!(Transient, Fatal)>> {
        let registered = self.registered()?;
        Subscription::<P, InsertOrder, Tables>::new_keyed(
            registered.jobs.clone(),
            registered.lane_job_type.clone(),
            self.id.to_string(),
            self.shared.outbox.pool().clone(),
        )
        .load()
        .await
    }

    /// Block until the lane has joined; returns the fence. Polls the join
    /// record as [`Subscription::await_position`] polls a checkpoint, and
    /// reports its deadline as [`SubscriptionRejection::LaneFenceTimeout`].
    /// A cancelled lane never joins, so it ends in the same timeout.
    #[es_entity::errlanes::instrument(
        name = "obix.joinable.await_fenced",
        skip_all,
        fields(timeout_ms = timeout.as_millis())
    )]
    pub async fn await_fenced(
        &self,
        timeout: Duration,
    ) -> Result<EventSequence, Fail<SubscriptionRejection, lanes!(Transient, Fatal)>> {
        self.registered()?;
        let key = self.id.to_string();
        let start = tokio::time::Instant::now();
        let deadline = start + timeout;
        let mut interval = INITIAL_POLL_INTERVAL;
        loop {
            if let Some(fence) = Tables::lane_fence(
                self.shared.outbox.pool(),
                self.shared.job_type.as_str(),
                &key,
            )
            .await?
            {
                return Ok(fence);
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Err(Fail::Rejected(SubscriptionRejection::LaneFenceTimeout {
                    lane: self.id,
                    waited: now.duration_since(start),
                }));
            }
            tokio::time::sleep(interval.min(deadline - now)).await;
            interval = (interval * 2).min(MAX_POLL_INTERVAL);
        }
    }

    /// Cancel the lane on the caller's own operation — atomic with, e.g.,
    /// deletion of the unit it covers. Row absence ends the lane: the
    /// runner's next run-start check completes it. The lane's record has no
    /// join sequence, so it never appears among a tip's joined lanes.
    #[es_entity::errlanes::instrument(name = "obix.joinable.cancel_in_op", skip_all)]
    pub async fn cancel_in_op(
        &self,
        op: &mut impl es_entity::AtomicOperation,
    ) -> Result<(), ObixFault> {
        let registered = self.registered()?;
        let key = self.id.to_string();
        let fut: std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<(), sqlx::Error>> + Send + '_>,
        > = Box::pin(Tables::retire_lane_in_op(
            &mut *op,
            self.shared.job_type.as_str(),
            registered.lane_job_type.as_str(),
            &key,
            None,
        ));
        fut.await?;
        Ok(())
    }
}
