//! Joinable subscriptions — one handler run by a **tip** (the live writer)
//! and by any number of **catch-up lanes** that catch up and then join it.
//!
//! A consumer that folds many units in one pass cannot add a unit with
//! history without a second cursor: the tip is at `C`, the new unit needs the
//! stream from an earlier position. obix owns that seam. A lane is opened at an
//! earlier position, consumes through the same handler, and ends in a single
//! **join landing** that joins the tip at a sequence `F`. The join is a fence
//! on the tip's landings — no tip landing straddles `F`: every tip landing
//! either began after the join landing committed, or committed before `F` was
//! read. The handler learns which side of the join each landing is on from
//! [`Scope`]; it never decides it.
//!
//! The tip's landing takes the tip row's lock as the first statement of its
//! transaction, before [`flush`](JoinableSubscriber::flush) runs. Opening a
//! lane and the join landing take the same lock. A commit-time lock would not
//! do: a tip landing could decide coverage on a snapshot from before the join
//! and commit after it. The placement at landing **entry** is the whole point, and
//! it is obix's to enforce, not the handler's.

mod lane;
mod runner;

use es_entity::errlanes::{Fail, ResultExt, lanes};
use serde::{Serialize, de::DeserializeOwned};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use job::{JobType, Jobs, RetrySettings};

use self::lane::{LaneJobInitializer, LaneMsg};
use crate::error::ObixFault;
use crate::out::Outbox;
use crate::out::ctx::{FlushOp, Handled, JoinableEventCtx};
use crate::out::error::{CouldNotDecodeStored as Undecodable, JoinableFault};
use crate::out::event::{EventDelivery, UndecodableDelivery};
use crate::out::lane::InsertOrder;
use crate::out::subscription::singleton::OutboxEventJobData;
use crate::out::subscription::{
    INITIAL_POLL_INTERVAL, MAX_POLL_INTERVAL, Subscription, SubscriptionRejection,
    SubscriptionSnapshot,
};
use crate::sequence::EventSequence;
use crate::tables::{DefaultMailboxTables, MailboxTables};

es_entity::entity_id! { LaneId }

const DEFAULT_MAX_BATCH_SIZE: usize = 100;
const DEFAULT_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(5);

/// A handler run by a tip and by catch-up lanes.
///
/// # Contract
///
/// 1. [`flush`](Self::flush) must tolerate whole-batch replay, as everywhere.
/// 2. `flush` may be called with an **empty batch** on a lane's join
///    landing: the lane caught up on skips alone and the runner lands anyway.
///    Liveness is the runner's job, not the handler's.
/// 3. Seam state is read **on `op`** — the handler's own columns written by a
///    lane's joining `flush`, or [`JoinableHandle::lanes_in_op`] — never from
///    the pool or from a cache across landings. Any read on the landing op is
///    after the entry lock by construction; a pool read is not.
/// 4. Coverage: apply an event to a unit only if its sequence is past the one
///    the unit joined at, and never while the unit's lane is live. With a lane
///    ahead of the tip, it joins at its own position and the tip's next
///    landings contain events at or below it that belong to the lane.
///
/// One handler instance serves both runners, and both of its methods are told
/// whose work they are doing through a [`Scope`]: a lane's
/// [`handle`](Self::handle) can skip events that are not its unit's, and
/// [`flush`](Self::flush) folds for the tip or for one unit. The ctx offers
/// [`skip`](JoinableEventCtx::skip) and
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

    /// One event, on the tip or on a lane. `scope` says which: a lane's
    /// handler may skip what is not its unit's. The same scope reaches the
    /// `flush` the event's batch lands in.
    fn handle<'inv>(
        &self,
        ctx: JoinableEventCtx<'inv, Self::Batch>,
        scope: Scope<'_, Self::Unit>,
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

/// Whose work a [`JoinableSubscriber`] call is: the tip's, or one catch-up
/// lane's, and for a lane whether this is the landing in which it joins.
///
/// `handle` and `flush` receive the same scope for one landing: events a lane
/// handles under [`Join`](Self::Join) are the ones its join landing's drain
/// pulled, and they land in the `flush` that carries the same `join`.
#[derive(Debug)]
pub enum Scope<'a, U> {
    /// The live writer.
    Tip,
    /// One catch-up lane, still catching up: an ordinary landing of its own.
    CatchUp { unit: &'a U },
    /// One catch-up lane's final landing, in which it joins the tip at
    /// `join`: every event at or below `join` has been handled by this lane,
    /// and the tip has not committed any landing past `join`.
    Join { unit: &'a U, join: EventSequence },
}

// Manual `Clone`/`Copy`: a scope borrows its unit, so it is `Copy` whatever
// `U` is — deriving would wrongly bound `U`.
impl<U> Clone for Scope<'_, U> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<U> Copy for Scope<'_, U> {}

/// Tuning for one joinable registration: the tip's dials, as for a
/// singleton's [`OutboxEventJobConfig`](crate::out::OutboxEventJobConfig),
/// and the lanes'.
#[derive(Clone)]
pub struct JoinableSubscriberConfig {
    /// The tip's job type, and the joinable's identity: the job type an
    /// address from [`Outbox::joinable`] names. Lanes run under
    /// `"<job_type>.lane"`.
    pub job_type: JobType,
    /// Backoff for the tip when its handler errors. Default
    /// [`RetrySettings::default`].
    pub retry_settings: RetrySettings,
    /// Backstop on how many collected events may share one tip batch.
    /// Default 100.
    pub max_batch_size: usize,
    /// Maximum staleness of a persisted checkpoint over skip-only stretches,
    /// for the tip and for every lane. Default 5s.
    pub checkpoint_interval: Duration,
    /// Backstop on how many collected events may share one lane batch.
    /// Default 100.
    pub lane_max_batch_size: usize,
    /// Backoff for a lane whose handler errors. Default
    /// [`RetrySettings::default`].
    pub lane_retry_settings: RetrySettings,
    /// Cap on how many lanes run concurrently on one process. `None` defers
    /// to the job crate's own default.
    pub max_concurrent_lanes_per_process: Option<usize>,
    /// A join landing drains at most this many sequences past the lane's
    /// position. If the tip is further ahead under the lock, the landing is an
    /// ordinary one ([`Scope::CatchUp`], lock held for this landing only) and
    /// the lane keeps catching up. Bounds the tip stall by construction;
    /// correctness never depends on it. Defaults to
    /// [`lane_max_batch_size`](Self::lane_max_batch_size).
    pub max_join_gap: u64,
}

impl JoinableSubscriberConfig {
    pub fn new(job_type: JobType) -> Self {
        Self {
            job_type,
            retry_settings: RetrySettings::default(),
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
            checkpoint_interval: DEFAULT_CHECKPOINT_INTERVAL,
            lane_max_batch_size: DEFAULT_MAX_BATCH_SIZE,
            lane_retry_settings: RetrySettings::default(),
            max_concurrent_lanes_per_process: None,
            max_join_gap: DEFAULT_MAX_BATCH_SIZE as u64,
        }
    }

    pub fn with_retry_settings(mut self, settings: RetrySettings) -> Self {
        self.retry_settings = settings;
        self
    }

    pub fn with_max_batch_size(mut self, max_batch_size: usize) -> Self {
        self.max_batch_size = max_batch_size.max(1);
        self
    }

    pub fn with_checkpoint_interval(mut self, interval: Duration) -> Self {
        self.checkpoint_interval = interval;
        self
    }

    /// Also resets [`max_join_gap`](Self::max_join_gap) to the new size.
    pub fn with_lane_max_batch_size(mut self, lane_max_batch_size: usize) -> Self {
        self.lane_max_batch_size = lane_max_batch_size.max(1);
        self.max_join_gap = self.lane_max_batch_size as u64;
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

    pub fn with_max_join_gap(mut self, max_join_gap: u64) -> Self {
        self.max_join_gap = max_join_gap;
        self
    }
}

/// One registered joinable: what [`Outbox::register_joinable_subscriber`]
/// stores on the outbox and every address for its job type resolves to.
pub(in crate::out) struct Registered<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    tip: Subscription<P, InsertOrder, Tables>,
    jobs: Jobs,
    lane_job_type: JobType,
    lane_spawner: job::KeyedJobSpawner<LaneMsg>,
}

/// The joinables registered on one outbox, by the tip's job type. Shared
/// across the outbox's clones, like its other registries.
pub(in crate::out) type Registrations<P, Tables> =
    Arc<RwLock<HashMap<JobType, Arc<Registered<P, Tables>>>>>;

/// The lanes of one joinable as of a landing op, from
/// [`JoinableHandle::lanes_in_op`].
#[derive(Debug, Clone)]
pub struct Lanes<U> {
    /// Lanes that have not joined yet: the tip must not apply their units.
    pub catching_up: Vec<(LaneId, U)>,
    /// Lanes that joined at a sequence at or above the `since` asked for: the
    /// tip applies their units from the event after that sequence.
    pub joined: Vec<(LaneId, U, EventSequence)>,
}

/// An address for one joinable subscription, keyed by the tip's job type.
///
/// An address, not a registration: it names the job type and resolves the
/// registration on the outbox at each call, so it can be minted before
/// [`register_joinable_subscriber`](Outbox::register_joinable_subscriber)
/// runs and any address for the job type works once it has. A service that
/// starts catch-ups can therefore hold one without depending on the handler
/// that depends on the service, which is also why it is parameterised by the
/// **unit** and not the handler type. Cheap to clone. Calls that need the
/// registration fail with a `Fatal` fault until it exists.
pub struct JoinableHandle<P, U, Tables = DefaultMailboxTables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    outbox: Outbox<P, Tables>,
    job_type: JobType,
    _unit: std::marker::PhantomData<fn() -> U>,
}

impl<P, U, Tables> Clone for JoinableHandle<P, U, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
    Tables: MailboxTables,
{
    fn clone(&self) -> Self {
        Self {
            outbox: self.outbox.clone(),
            job_type: self.job_type.clone(),
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
    /// [`register_joinable_subscriber`](Self::register_joinable_subscriber),
    /// `Clone`, cheap.
    pub fn joinable<U>(&self, job_type: JobType) -> JoinableHandle<P, U, Tables> {
        JoinableHandle {
            outbox: self.clone(),
            job_type,
            _unit: std::marker::PhantomData,
        }
    }

    /// The body of
    /// [`register_joinable_subscriber`](Self::register_joinable_subscriber).
    pub(in crate::out) async fn register_joinable<S>(
        &self,
        jobs: &mut Jobs,
        config: JoinableSubscriberConfig,
        handler: S,
    ) -> Result<JoinableHandle<P, S::Unit, Tables>, ObixFault>
    where
        S: JoinableSubscriber<P>,
    {
        let job_type = config.job_type.clone();
        Tables::ensure_tip_row(self.pool(), job_type.as_str()).await?;

        let handler = Arc::new(handler);
        let tip_initializer =
            runner::TipJobInitializer::<S, P, Tables>::new(self.clone(), handler.clone(), &config);
        let tip_spawner = jobs.add_resident_initializer(tip_initializer);

        // Leaked once per registration: the job crate only accepts `&'static str`.
        let lane_job_type = JobType::new(Box::leak(format!("{job_type}.lane").into_boxed_str()));
        let lane_initializer = LaneJobInitializer::<S, P, Tables>::new(
            self.clone(),
            handler,
            lane_job_type.clone(),
            &config,
        );
        let lane_spawner = jobs.add_keyed_initializer(lane_initializer);

        let job = tip_spawner.spawn(OutboxEventJobData::default()).await?;
        let tip = Subscription::new(job, self.pool().clone(), None);
        self.joinable_registrations
            .write()
            .expect("joinable registrations poisoned")
            .entry(job_type.clone())
            .or_insert_with(|| {
                Arc::new(Registered {
                    tip,
                    jobs: jobs.clone(),
                    lane_job_type,
                    lane_spawner,
                })
            });
        Ok(self.joinable(job_type))
    }

    fn joinable_registration(
        &self,
        job_type: &JobType,
    ) -> Result<Arc<Registered<P, Tables>>, JoinableFault> {
        self.joinable_registrations
            .read()
            .expect("joinable registrations poisoned")
            .get(job_type)
            .cloned()
            .ok_or_else(|| JoinableFault::NotRegistered {
                job_type: job_type.clone(),
            })
    }
}

impl<P, U, Tables> JoinableHandle<P, U, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    fn registered(&self) -> Result<Arc<Registered<P, Tables>>, JoinableFault> {
        self.outbox.joinable_registration(&self.job_type)
    }

    /// The tip as a [`Subscription`]: its checkpoint, frontier, job status
    /// and the `await_position`/`await_caught_up` barriers, as
    /// [`register_singleton_subscriber`](Outbox::register_singleton_subscriber)
    /// hands back for a singleton.
    pub fn tip(&self) -> Result<Subscription<P, InsertOrder, Tables>, ObixFault> {
        Ok(self.registered()?.tip.clone())
    }

    /// The tip's checkpoint, frontier and job status:
    /// [`tip()`](Self::tip)`.load()`.
    #[es_entity::errlanes::instrument(name = "obix.joinable.load", skip_all)]
    pub async fn load(
        &self,
    ) -> Result<SubscriptionSnapshot, Fail<SubscriptionRejection, lanes!(Transient, Fatal)>> {
        self.registered()?.tip.load().await
    }

    /// Re-derive a lane handle from a stored id. Pure: nothing is checked
    /// until the lane is used.
    pub fn lane(&self, id: LaneId) -> CatchUpLane<P, Tables> {
        CatchUpLane {
            outbox: self.outbox.clone(),
            job_type: self.job_type.clone(),
            id,
        }
    }

    /// Lane state as of `op`. Inside a tip `flush` this is read under the
    /// entry lock and is the contract for consumers that keep no seam state of
    /// their own: skip units in `catching_up`, apply past the join sequence
    /// for units in `joined`. `joined` holds joins at or above `since`; a tip passes its
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
                self.job_type.as_str(),
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
        for (key, stored, join) in rows.joined {
            let (id, unit) = decode(key, stored)?;
            lanes.joined.push((id, unit, join));
        }
        Ok(lanes)
    }

    /// Start a catch-up for `unit` from `start_after` (exclusive), in the
    /// caller's op — the one that creates the unit. Takes the tip's lock
    /// exclusively as the first statement on `op`, so a tip landing sees the
    /// unit and its lane together or not at all, and holds it until `op`
    /// commits. A birth above the stream frontier is rejected with
    /// [`SubscriptionRejection::BirthAheadOfFrontier`].
    #[es_entity::errlanes::instrument(name = "obix.joinable.start_catch_up_in_op", skip_all)]
    pub async fn start_catch_up_in_op(
        &self,
        op: &mut impl es_entity::AtomicOperation,
        unit: U,
        start_after: EventSequence,
    ) -> Result<CatchUpLane<P, Tables>, Fail<SubscriptionRejection, lanes!(Transient, Fatal)>>
    where
        U: Serialize,
    {
        let registered = self.registered()?;
        let tip_type = self.job_type.as_str();

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

        Ok(self.lane(id))
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
    outbox: Outbox<P, Tables>,
    job_type: JobType,
    id: LaneId,
}

impl<P, Tables> Clone for CatchUpLane<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static,
    Tables: MailboxTables,
{
    fn clone(&self) -> Self {
        Self {
            outbox: self.outbox.clone(),
            job_type: self.job_type.clone(),
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

    fn registered(&self) -> Result<Arc<Registered<P, Tables>>, JoinableFault> {
        self.outbox.joinable_registration(&self.job_type)
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
            self.outbox.pool().clone(),
        )
        .load()
        .await
    }

    /// Block until the lane has joined; returns the sequence it joined at. Polls the join
    /// record as [`Subscription::await_position`] polls a checkpoint, and
    /// reports its deadline as [`SubscriptionRejection::LaneJoinTimeout`].
    /// A cancelled lane never joins, so it ends in the same timeout.
    #[es_entity::errlanes::instrument(
        name = "obix.joinable.await_join",
        skip_all,
        fields(timeout_ms = timeout.as_millis())
    )]
    pub async fn await_join(
        &self,
        timeout: Duration,
    ) -> Result<EventSequence, Fail<SubscriptionRejection, lanes!(Transient, Fatal)>> {
        self.registered()?;
        let key = self.id.to_string();
        let start = tokio::time::Instant::now();
        let deadline = start + timeout;
        let mut interval = INITIAL_POLL_INTERVAL;
        loop {
            if let Some(join) =
                Tables::lane_join_sequence(self.outbox.pool(), self.job_type.as_str(), &key).await?
            {
                return Ok(join);
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Err(Fail::Rejected(SubscriptionRejection::LaneJoinTimeout {
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
    /// runner's next run-start check completes it, and a join landing
    /// already waiting for the tip's lock finds the row gone and ends the
    /// lane without flushing. The lane's record has no join sequence, so it
    /// never appears among a tip's joined lanes. Cancelling a lane that has
    /// already joined is a no-op: its row is gone and its join record stands.
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
            self.job_type.as_str(),
            registered.lane_job_type.as_str(),
            &key,
            None,
        ));
        fut.await?;
        Ok(())
    }

    /// [`cancel_in_op`](Self::cancel_in_op), standalone.
    #[es_entity::errlanes::instrument(name = "obix.joinable.cancel", skip_all)]
    pub async fn cancel(&self) -> Result<(), ObixFault> {
        let mut op = self.outbox.begin_op().await?;
        self.cancel_in_op(&mut op).await?;
        op.commit().await?;
        Ok(())
    }
}
