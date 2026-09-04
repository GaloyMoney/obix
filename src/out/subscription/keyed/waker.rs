//! The wake plane: liveness-only, event-driven wakes.
//!
//! **One waker per outbox, not per subscriber type.** It is an internal
//! singleton subscriber — built entirely on the EXISTING
//! [`SingletonSubscriber`]/[`EventCtx`]/[`FlushOp`] machinery, no fork — that
//! classifies every persistent event through every registered type's
//! [`SubscriptionDef::wake_keys`], collects the matches per batch, and at
//! flush time looks up which subscribed keys care and idempotently respawns
//! them, all on the flush op so the wakes and the waker's own checkpoint
//! commit atomically: a crash cannot checkpoint past events whose wakes were
//! lost.
//!
//! Registering one waker per type would have meant N independent full passes
//! over the persistent stream — every event read, decoded and checkpointed N
//! times — to answer a question that is one classification per type over a
//! single pass. Erasure is free here because [`SubscriptionDef`] already
//! promises `wake_keys` is synchronous, DB-free and cheap.
//!
//! Liveness-only by construction: a false-positive match costs one harmless
//! wake (pulls a live holder's deadline forward, or an empty lookup for an
//! unsubscribed key — the majority case). Wake keys must never gate
//! delivery, so they are never consulted by the per-key runner itself — only
//! by this waker, to decide who to wake.
//!
//! # Two wake paths, two service levels
//!
//! **Wake-key match** — instant and uncapped. A matching event is a real
//! arrival for a specific subscription; delaying or shedding it would break
//! the latency contract the whole mechanism exists to provide.
//!
//! **Catch-up** — capped and cascading. A Dormant member whose cursor is
//! drifting toward the bottom of the in-memory cache will, if left alone,
//! eventually resume with a paged cold read from disk: one `SELECT` per
//! `backfill_page_size` events it fell behind. Waking it while its backlog
//! is still resident turns that into a memory read. Unlike a wake-key match
//! this can select *every* subscription at once, so it is bounded — see
//! [`CATCH_UP_WAKE_LIMIT`].
//!
//! The catch-up path needs no timer and no signal from the cache, which is
//! what lets the whole wake plane live in a `SingletonSubscriber` and its
//! shared runner loop: eviction pressure is a function of how far the stream
//! has advanced, and this handler sees every event. It is also the reason
//! there is no periodic reconciler — a clock-driven pass fires hardest when
//! the stream is quiet, which is precisely when nothing needs waking.

use serde::{Serialize, de::DeserializeOwned};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use job::JobType;

use super::{KeyMsg, SubscriptionDef, WakeKey};
use es_entity::AtomicOperation as _;

use crate::out::StreamSelection;
use crate::out::ctx::{EventCtx, FlushOp, Handled};
use crate::out::event::PersistentOutboxEvent;
use crate::out::subscription::singleton::SingletonSubscriber;
use crate::sequence::EventSequence;
use crate::tables::MailboxTables;

/// One registered keyed-subscriber type, with its [`SubscriptionDef`] erased
/// so a single waker can hold every type registered on one outbox.
///
/// Object-safe because everything the waker needs from a def is synchronous
/// and owned: `wake_keys` returns owned keys, and the spawner and subscriber
/// type are plain values. Nothing here touches the database.
pub(in crate::out) trait WakeRoute<P>: Send + Sync + 'static
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    fn subscriber_type(&self) -> &str;
    fn wake_keys(&self, event: &PersistentOutboxEvent<P>) -> Vec<WakeKey>;
    fn spawner(&self) -> &job::KeyedJobSpawner<KeyMsg>;
}

struct TypedWakeRoute<D, P>
where
    D: SubscriptionDef<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    def: Arc<D>,
    subscriber_type: JobType,
    spawner: job::KeyedJobSpawner<KeyMsg>,
    _marker: std::marker::PhantomData<fn() -> P>,
}

impl<D, P> WakeRoute<P> for TypedWakeRoute<D, P>
where
    D: SubscriptionDef<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    fn subscriber_type(&self) -> &str {
        self.subscriber_type.as_str()
    }

    fn wake_keys(&self, event: &PersistentOutboxEvent<P>) -> Vec<WakeKey> {
        self.def.wake_keys(event).into_iter().collect()
    }

    fn spawner(&self) -> &job::KeyedJobSpawner<KeyMsg> {
        &self.spawner
    }
}

/// The set of registered types the waker classifies against.
///
/// Written only by
/// [`register_keyed_subscriber`](crate::out::Outbox::register_keyed_subscriber),
/// which the API requires to be called before `Jobs::start_poll`, so by the
/// time the waker job runs this is effectively read-only and uncontended.
pub(in crate::out) type WakeRoutes<P> = Arc<RwLock<Vec<Arc<dyn WakeRoute<P>>>>>;

/// Erase one registered type into a [`WakeRoute`].
pub(in crate::out) fn wake_route<D, P>(
    def: Arc<D>,
    subscriber_type: JobType,
    spawner: job::KeyedJobSpawner<KeyMsg>,
) -> Arc<dyn WakeRoute<P>>
where
    D: SubscriptionDef<P>,
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
{
    Arc::new(TypedWakeRoute {
        def,
        subscriber_type,
        spawner,
        _marker: std::marker::PhantomData,
    })
}

/// Most a single catch-up pass may wake.
///
/// Not configurable, and deliberately so: it bounds a *repair* rate, not a
/// throughput knob, and the ordering is what makes a cap safe at all — the
/// scan returns the furthest-behind members first, so a pass sheds the ones
/// with the most cache slack left and the next pass takes the next slice.
/// Members therefore cascade rather than storm, prioritised by how close
/// they are to falling out of the cache instead of by an arbitrary
/// randomised schedule.
const CATCH_UP_WAKE_LIMIT: i64 = 64;

/// Fraction of the cache a member may fall behind before a catch-up wake:
/// wake at three quarters, leaving the last quarter as the margin it has to
/// actually drain the backlog from memory before the events it still needs
/// are evicted.
const CATCH_UP_TRIGGER_NUMERATOR: u64 = 3;
const CATCH_UP_TRIGGER_DENOMINATOR: u64 = 4;

pub(in crate::out) struct WakerHandler<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    routes: WakeRoutes<P>,
    /// How far behind the head a member may fall before a catch-up wake, and
    /// how far the stream must advance between scans — both derived from
    /// `MailboxConfig::event_cache_size`.
    catch_up_lag: u64,
    catch_up_stride: u64,
    /// Head sequence at the last catch-up scan that actually **committed**.
    /// The stream's own advance is the clock here: this handler is a
    /// `SingletonSubscriber` with no timer branch, and it does not need one —
    /// eviction pressure is a function of how far the stream has moved, which
    /// is exactly what it observes.
    ///
    /// Advanced from a commit hook rather than when the pass is decided.
    /// Deciding happens in `handle_persistent`, long before the batch flush
    /// commits, and this handler is shared across runner retries — so a
    /// claim taken at decision time would survive a rolled-back flush and
    /// suppress the retry's scan, leaving members at the cache cliff waiting
    /// out another full stride. Erring the other way merely rescans.
    last_catch_up: Arc<std::sync::atomic::AtomicU64>,
    _marker: std::marker::PhantomData<fn() -> Tables>,
}

/// The waker's job type, scoped to the outbox rather than to any one
/// subscriber type: [`MailboxTables::KEYED_WAKER_JOB_TYPE`], i.e.
/// `{persistent table}.keyed-waker`. Two outboxes in one process have
/// different persistent tables and so cannot collide.
pub(in crate::out) fn waker_job_type<Tables: MailboxTables>() -> JobType {
    JobType::new(Tables::KEYED_WAKER_JOB_TYPE)
}

/// Construct the waker handler — a plain [`SingletonSubscriber`], registered
/// via [`crate::out::Outbox::register_singleton_subscriber`] like any other.
///
/// `event_cache_size` is the configured depth of the in-memory persistent
/// cache, which is all the catch-up scan needs to know about it: the cache
/// exposes no low-water mark and needs no eviction signal, because the
/// waker already observes the head and can derive the pressure from it.
/// That derivation is approximate — the retained window floats either side
/// of the configured size and the head counts pre-commit allocations — and
/// the quarter-cache margin absorbs the imprecision.
pub(in crate::out) fn waker_handler<P, Tables>(
    routes: WakeRoutes<P>,
    event_cache_size: usize,
) -> WakerHandler<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    let cache_size = event_cache_size as u64;
    WakerHandler {
        routes,
        catch_up_lag: (cache_size * CATCH_UP_TRIGGER_NUMERATOR / CATCH_UP_TRIGGER_DENOMINATOR)
            .max(1),
        catch_up_stride: (cache_size / CATCH_UP_TRIGGER_DENOMINATOR).max(1),
        last_catch_up: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        _marker: std::marker::PhantomData,
    }
}

impl<P, Tables> SingletonSubscriber<P> for WakerHandler<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    // Classification reads persistent events only; the ephemeral stream
    // would just spend the runner's turns on deliveries that skip.
    const SUBSCRIPTION: StreamSelection = StreamSelection::PersistentOnly;

    type Batch = WakeBatch;

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, Self::Batch>,
        event: &PersistentOutboxEvent<P>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        // Scoped so the guard cannot be held across the return: classifying
        // is synchronous by `SubscriptionDef`'s contract, and a lock guard
        // alive across an await would make this future non-`Send`.
        let matched: Vec<(usize, Vec<WakeKey>)> = {
            let routes = self.routes.read().expect("wake routes poisoned");
            routes
                .iter()
                .enumerate()
                .filter_map(|(idx, route)| {
                    let keys = route.wake_keys(event);
                    (!keys.is_empty()).then_some((idx, keys))
                })
                .collect()
        };
        let catch_up_head = self.catch_up_due(event.sequence).then_some(event.sequence);

        if matched.is_empty() && catch_up_head.is_none() {
            return Ok(ctx.skip());
        }
        Ok(ctx.collect_with(move |batch| {
            for (idx, keys) in matched {
                batch.per_route.entry(idx).or_default().extend(keys);
            }
            // Keep the highest head of the batch: however many events came
            // due while it was open, one scan at flush covers all of them.
            if let Some(head) = catch_up_head {
                batch.catch_up_head = Some(
                    batch
                        .catch_up_head
                        .map_or(head, |current| current.max(head)),
                );
            }
        }))
    }

    async fn flush(
        &self,
        op: &mut FlushOp<'_>,
        items: Self::Batch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let WakeBatch {
            per_route,
            catch_up_head,
        } = items;
        if per_route.is_empty() && catch_up_head.is_none() {
            return Ok(());
        }
        // Snapshot (cheap — one `Arc` clone per registered type) so no lock
        // guard is alive across the awaits below.
        let routes: Vec<Arc<dyn WakeRoute<P>>> =
            self.routes.read().expect("wake routes poisoned").clone();
        let route_idx: HashMap<&str, usize> = routes
            .iter()
            .enumerate()
            .map(|(idx, r)| (r.subscriber_type(), idx))
            .collect();

        // Both paths select `(subscriber_type, key)` and both feed this, so
        // the whole flush issues one lookup per path and one spawn call per
        // registered type — never one per key, and never one per key twice
        // for a member both paths select.
        // Key -> whether the wake may pull a live holder forward: a wake-key
        // match is a real arrival and may, a catch-up wake may not.
        let mut to_wake: HashMap<usize, HashMap<String, bool>> = HashMap::new();

        // The wake-key path, as one query zipping every type's matches
        // together (see `subscriptions_for_wake_keys`).
        let (types, wake_keys): (Vec<String>, Vec<String>) = per_route
            .into_iter()
            .filter_map(|(idx, keys)| routes.get(idx).map(|route| (route, keys)))
            .flat_map(|(route, keys)| {
                keys.into_iter()
                    .map(|key| (route.subscriber_type().to_string(), key.0))
            })
            .unzip();
        for (subscriber_type, key) in Tables::subscriptions_for_wake_keys(op, &types, &wake_keys)
            .await?
            .into_iter()
        {
            if let Some(idx) = route_idx.get(subscriber_type.as_str()) {
                to_wake.entry(*idx).or_default().insert(key, true);
            }
        }

        // The catch-up pass: whoever has fallen far enough behind that a
        // wake now serves them from memory instead of a paged cold read.
        // Uncapped by design on the wake-key path above — a matching event is
        // a real arrival and must not be delayed — but capped here, because
        // this path can select every subscription at once.
        if let Some(head) = catch_up_head {
            let below = EventSequence::from(u64::from(head).saturating_sub(self.catch_up_lag));
            let registered: Vec<String> = routes
                .iter()
                .map(|r| r.subscriber_type().to_string())
                .collect();
            let behind =
                Tables::subscriptions_behind(op, &registered, below, CATCH_UP_WAKE_LIMIT).await?;
            for (subscriber_type, key) in behind {
                if let Some(idx) = route_idx.get(subscriber_type.as_str()) {
                    to_wake.entry(*idx).or_default().entry(key).or_insert(false);
                }
            }
            // Claimed only if all of this lands: the hook runs after commit.
            let _ = op.add_commit_hook(CatchUpClaimed {
                cell: self.last_catch_up.clone(),
                head: u64::from(head),
            });
        }

        for (idx, keys) in to_wake {
            let Some(route) = routes.get(idx) else {
                continue;
            };
            self.spawn_all(op, route, keys.into_iter().collect())
                .await?;
        }
        Ok(())
    }
}

/// What one waker batch accumulated: wake-key matches per registered type,
/// plus whether a catch-up scan came due while the batch was open.
#[derive(Default)]
pub(in crate::out) struct WakeBatch {
    /// Wake keys accumulated per registered type, indexed by that type's
    /// position in [`WakeRoutes`] — an index rather than the type's name so
    /// the batch holds no strings it would only have to look up again.
    per_route: HashMap<usize, HashSet<WakeKey>>,
    /// Set when the stream advanced far enough for a catch-up scan; carries
    /// the head it came due at, from which the floor is derived at flush.
    catch_up_head: Option<EventSequence>,
}

/// Advances the waker's catch-up clock, but only once the flush that ran the
/// scan has actually committed — so a rolled-back batch re-scans on retry
/// instead of silently skipping a pass.
struct CatchUpClaimed {
    cell: Arc<std::sync::atomic::AtomicU64>,
    head: u64,
}

impl es_entity::hooks::CommitHook for CatchUpClaimed {
    fn post_commit(self) {
        self.cell
            .fetch_max(self.head, std::sync::atomic::Ordering::Relaxed);
    }
}

impl<P, Tables> WakerHandler<P, Tables>
where
    P: Serialize + DeserializeOwned + Send + Sync + 'static + Unpin,
    Tables: MailboxTables,
{
    /// Whether the stream has advanced a full stride since the last scan
    /// that committed. A pure predicate: nothing is claimed here, because
    /// nothing is durable here — see [`last_catch_up`](Self::last_catch_up).
    fn catch_up_due(&self, head: EventSequence) -> bool {
        use std::sync::atomic::Ordering;

        let head = u64::from(head);
        // Nothing can be behind the floor yet, and on a young stream the
        // subtraction would saturate to zero and scan for nobody anyway.
        if head.checked_sub(self.catch_up_lag).is_none() {
            return false;
        }
        let last = self.last_catch_up.load(Ordering::Relaxed);
        head >= last.saturating_add(self.catch_up_stride)
    }

    async fn spawn_all(
        &self,
        op: &mut FlushOp<'_>,
        route: &Arc<dyn WakeRoute<P>>,
        keys: Vec<(String, bool)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if keys.is_empty() {
            return Ok(());
        }
        let specs = keys
            .into_iter()
            .map(|(key, pull_forward)| {
                let spec = job::KeyedJobSpec::new(key.clone(), KeyMsg { key });
                if pull_forward {
                    spec.force_reschedule()
                } else {
                    spec
                }
            })
            .collect();
        route.spawner().spawn_all_in_op(op, specs).await?;
        Ok(())
    }
}
