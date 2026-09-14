//! Contracts for [`obix::Subscription`] — the checkpoint read-back returned
//! by `Outbox::register_singleton_subscriber` and the caught-up barrier built on it.

mod helpers;

use std::sync::Arc;
use std::time::Duration;

use obix::{
    EventCtx, EventSequence, FlushOp, Handled, MailboxConfig, OutboxEventJobConfig,
    SingletonSubscriber, Subscription, SubscriptionError, SubscriptionSnapshot,
    SubscriptionStreamStatus, out::Outbox,
};
use serde::{Deserialize, Serialize};
use serial_test::file_serial;
use tokio::sync::Mutex;

use helpers::{TestTables, init_pool, wipeout_outbox_job_tables, wipeout_outbox_tables};

const JOB_TYPE: &str = "test-registered-handler";

/// Short enough that a skip-only handler's lazy checkpoint lands inside a
/// test's patience, rather than at the 5s production default.
const TEST_CHECKPOINT_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum TestEvent {
    Ping(u64),
    /// Published by [`RepublishingHandler`] from inside its own flush, onto
    /// the outbox it consumes — the self-publishing tail of semantics 5.
    Echo(u64),
}

/// Pure observer: records deliveries and skips, so the checkpoint only ever
/// advances through the lazy (interval-bounded) path.
struct SkippingObserver {
    received: Arc<Mutex<Vec<u64>>>,
}

impl SingletonSubscriber<TestEvent> for SkippingObserver {
    type Batch = ();

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv>,
        event: &obix::out::PersistentOutboxEvent<TestEvent>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(TestEvent::Ping(n)) = &event.payload {
            self.received.lock().await.push(*n);
        }
        Ok(ctx.skip())
    }
}

/// Always fails, so its job crash-loops on the first event forever — the
/// shape of a handler parked on a poison event.
struct PoisonHandler;

const POISON_ERROR: &str = "poison-handler-always-fails";

impl SingletonSubscriber<TestEvent> for PoisonHandler {
    type Batch = ();

    async fn handle_persistent<'inv>(
        &self,
        _ctx: EventCtx<'inv>,
        _event: &obix::out::PersistentOutboxEvent<TestEvent>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        Err(POISON_ERROR.into())
    }
}

/// Collects every `Ping` and, on flush, publishes a matching `Echo` back onto
/// the SAME outbox — inside the batch transaction that commits the
/// checkpoint. `Echo`s are skipped rather than collected, so the cascade
/// terminates after one round.
struct RepublishingHandler {
    outbox: Outbox<TestEvent, TestTables>,
    echoed: Arc<Mutex<Vec<u64>>>,
}

impl SingletonSubscriber<TestEvent> for RepublishingHandler {
    type Batch = Vec<u64>;

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, Vec<u64>>,
        event: &obix::out::PersistentOutboxEvent<TestEvent>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        match &event.payload {
            Some(TestEvent::Ping(n)) => {
                let n = *n;
                Ok(ctx.collect_with(move |batch| batch.push(n)))
            }
            _ => Ok(ctx.skip()),
        }
    }

    async fn flush(
        &self,
        op: &mut FlushOp<'_>,
        items: Vec<u64>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for n in items {
            self.outbox
                .publish_persisted_in_op(op, TestEvent::Echo(n))
                .await?;
            self.echoed.lock().await.push(n);
        }
        Ok(())
    }
}

async fn init_jobs(pool: &sqlx::PgPool) -> anyhow::Result<job::Jobs> {
    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    Ok(job::Jobs::init(job_config).await?)
}

async fn init_outbox(pool: &sqlx::PgPool) -> anyhow::Result<Outbox<TestEvent, TestTables>> {
    wipeout_outbox_tables(pool).await?;
    wipeout_outbox_job_tables(pool, JOB_TYPE).await?;

    Ok(Outbox::<TestEvent, TestTables>::init(
        pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?)
}

fn test_config() -> OutboxEventJobConfig {
    OutboxEventJobConfig::new(job::JobType::new(JOB_TYPE))
        .with_checkpoint_interval(TEST_CHECKPOINT_INTERVAL)
}

/// Retry fast enough that a crash-looping job cycles several times inside a
/// test, instead of at the production backoff.
fn fast_retry_settings() -> job::RetrySettings {
    let mut settings = job::RetrySettings::repeat_indefinitely();
    settings.min_backoff = Duration::from_millis(50);
    settings.max_backoff = Duration::from_millis(100);
    settings.backoff_jitter_pct = 0;
    settings
}

async fn register<H: SingletonSubscriber<TestEvent>>(
    outbox: &Outbox<TestEvent, TestTables>,
    jobs: &mut job::Jobs,
    handler: H,
) -> anyhow::Result<Subscription<TestEvent, TestTables>> {
    register_with(outbox, jobs, test_config(), handler).await
}

async fn register_with<H: SingletonSubscriber<TestEvent>>(
    outbox: &Outbox<TestEvent, TestTables>,
    jobs: &mut job::Jobs,
    config: OutboxEventJobConfig,
    handler: H,
) -> anyhow::Result<Subscription<TestEvent, TestTables>> {
    outbox
        .register_singleton_subscriber(jobs, config, handler)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
}

async fn publish_pings(
    outbox: &Outbox<TestEvent, TestTables>,
    range: std::ops::RangeInclusive<u64>,
) -> anyhow::Result<()> {
    let mut op = outbox.begin_op().await?;
    for n in range {
        outbox
            .publish_persisted_in_op(&mut op, TestEvent::Ping(n))
            .await?;
    }
    op.commit().await?;
    Ok(())
}

/// Load through an owned handle. Taking the handle by value keeps the future
/// free of borrows, which is what lets it cross a `tokio::spawn` boundary
/// (an inline `async move` block hits rust-lang/rust#100013 here).
async fn load_owned(
    handle: Subscription<TestEvent, TestTables>,
) -> Result<SubscriptionSnapshot, SubscriptionError> {
    handle.load().await
}

/// Poll `f` until it holds or `timeout` elapses.
async fn eventually<F, Fut>(timeout: Duration, mut f: F) -> anyhow::Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<bool>>,
{
    let start = std::time::Instant::now();
    loop {
        if f().await? {
            return Ok(());
        }
        if start.elapsed() >= timeout {
            anyhow::bail!("condition did not hold within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Contract 1 — BEGIN-on-missing: a registered handler whose job has never
/// run has no persisted execution state, and that reads as honest full lag
/// rather than a spurious "caught up".
#[tokio::test]
#[file_serial]
async fn checkpoint_reads_begin_before_the_handler_runs() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let handle = register(
        &outbox,
        &mut jobs,
        SkippingObserver {
            received: Arc::new(Mutex::new(Vec::new())),
        },
    )
    .await?;

    // Deliberately no `jobs.start_poll()`.
    assert_eq!(handle.load().await?.checkpoint(), EventSequence::BEGIN);

    publish_pings(&outbox, 1..=3).await?;

    let snapshot = handle.load().await?;
    assert_eq!(snapshot.checkpoint(), EventSequence::BEGIN);
    assert_eq!(snapshot.frontier(), EventSequence::from(3u64));
    assert_eq!(snapshot.lag(), 3);
    assert!(!snapshot.is_caught_up());
    assert_eq!(
        snapshot.stream_status(),
        SubscriptionStreamStatus {
            checkpoint: EventSequence::BEGIN,
            frontier: EventSequence::from(3u64),
        }
    );

    Ok(())
}

/// Contract 2 — the checkpoint trails applied state and converges on the
/// frontier once the backlog drains; it never runs ahead of it.
#[tokio::test]
#[file_serial]
async fn checkpoint_trails_applied_state_and_converges() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let received = Arc::new(Mutex::new(Vec::new()));
    let handle = register(
        &outbox,
        &mut jobs,
        SkippingObserver {
            received: received.clone(),
        },
    )
    .await?;

    jobs.start_poll().await?;
    publish_pings(&outbox, 1..=3).await?;

    let frontier = outbox.highest_known_persistent_sequence().await?;
    assert_eq!(frontier, EventSequence::from(3u64));

    eventually(Duration::from_secs(10), || {
        let handle = handle.clone();
        async move { Ok(handle.load().await?.checkpoint() >= frontier) }
    })
    .await?;

    assert_eq!(*received.lock().await, vec![1, 2, 3]);
    // Nothing published since, so the checkpoint must sit exactly on the
    // frontier — never past it.
    let snapshot = handle.load().await?;
    assert_eq!(snapshot.checkpoint(), frontier);
    assert!(snapshot.is_caught_up());

    Ok(())
}

/// Contract 3 — duplicate-registration identity: registering the same job
/// type twice resolves to the one persisted job, so both handles observe it.
#[tokio::test]
#[file_serial]
async fn duplicate_registration_yields_the_same_job_id() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let first = register(
        &outbox,
        &mut jobs,
        SkippingObserver {
            received: Arc::new(Mutex::new(Vec::new())),
        },
    )
    .await?;
    let second = register(
        &outbox,
        &mut jobs,
        SkippingObserver {
            received: Arc::new(Mutex::new(Vec::new())),
        },
    )
    .await?;

    assert_eq!(first.job_id(), second.job_id());

    Ok(())
}

/// Contract 4 — `stream_status` reads the checkpoint BEFORE the frontier, so
/// an advance racing the pair can only overstate lag. A caller acting on
/// `is_caught_up` therefore never acts on an optimistic reading.
#[tokio::test]
#[file_serial]
async fn stream_status_never_understates_lag() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let handle = register(
        &outbox,
        &mut jobs,
        SkippingObserver {
            received: Arc::new(Mutex::new(Vec::new())),
        },
    )
    .await?;

    jobs.start_poll().await?;
    publish_pings(&outbox, 1..=5).await?;

    // Sampled while a publisher races the reader: whatever pair comes back,
    // the checkpoint may never exceed the frontier read after it.
    for _ in 0..20 {
        let snapshot = handle.load().await?;
        assert!(
            snapshot.checkpoint() <= snapshot.frontier(),
            "checkpoint {} led frontier {}",
            snapshot.checkpoint(),
            snapshot.frontier()
        );
        assert_eq!(snapshot.is_caught_up(), snapshot.lag() == 0);
        publish_pings(&outbox, 6..=6).await?;
    }

    eventually(Duration::from_secs(10), || {
        let handle = handle.clone();
        async move { Ok(handle.load().await?.is_caught_up()) }
    })
    .await?;

    Ok(())
}

/// Contract 5 — the handle retains no borrow of `Jobs`: it stays usable once
/// the service value is gone, and is portable across tasks.
#[tokio::test]
#[file_serial]
async fn handle_retains_no_jobs_borrow() -> anyhow::Result<()> {
    fn assert_portable<T: Send + Sync + Clone + 'static>() {}
    assert_portable::<Subscription<TestEvent, TestTables>>();

    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let handle = register(
        &outbox,
        &mut jobs,
        SkippingObserver {
            received: Arc::new(Mutex::new(Vec::new())),
        },
    )
    .await?;

    jobs.start_poll().await?;
    publish_pings(&outbox, 1..=2).await?;

    let frontier = outbox.highest_known_persistent_sequence().await?;
    eventually(Duration::from_secs(10), || {
        let handle = handle.clone();
        async move { Ok(handle.load().await?.checkpoint() >= frontier) }
    })
    .await?;

    // The poller stops here; the handle keeps reading committed state.
    drop(jobs);

    // Spawning is the regression guard for the boxed frontier read: awaiting
    // `highest_known_persistent_sequence`'s opaque future directly makes
    // these `Send` bounds higher-ranked and fails to compile here.
    let snapshot = tokio::spawn(load_owned(handle.clone())).await??;
    assert_eq!(snapshot.checkpoint(), frontier);

    let spawned_outbox = outbox.clone();
    let spawned_frontier =
        tokio::spawn(async move { spawned_outbox.highest_known_persistent_sequence().await })
            .await??;
    assert_eq!(spawned_frontier, frontier);

    Ok(())
}

/// Contract 6 — fence semantics: everything published before the call is
/// applied by the time the barrier returns.
#[tokio::test]
#[file_serial]
async fn await_caught_up_fences_a_backlog() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let received = Arc::new(Mutex::new(Vec::new()));
    let handle = register(
        &outbox,
        &mut jobs,
        SkippingObserver {
            received: received.clone(),
        },
    )
    .await?;

    publish_pings(&outbox, 1..=25).await?;
    let frontier_at_call = outbox.highest_known_persistent_sequence().await?;
    assert_eq!(frontier_at_call, EventSequence::from(25u64));

    jobs.start_poll().await?;
    handle.await_caught_up(Duration::from_secs(60)).await?;

    // The barrier's guarantee: applied, not merely delivered — and one load
    // answers every question about the handler.
    let snapshot = handle.load().await?;
    assert!(snapshot.checkpoint() >= frontier_at_call);
    assert!(
        !snapshot.job_status().is_terminal(),
        "a resident handler job should still be live, got {:?}",
        snapshot.job_status()
    );
    assert_eq!(received.lock().await.len(), 25);

    Ok(())
}

/// Contract 10 — a wedged handler is diagnosable. Handler jobs retry
/// indefinitely, so one crash-looping on a poison event never goes terminal:
/// `job_status` keeps saying "alive" while the checkpoint is frozen, and a
/// barrier over it times out looking exactly like a slow handler.
/// `last_error` is what separates the two.
#[tokio::test]
#[file_serial]
async fn wedged_handler_is_distinguishable_from_a_slow_one() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let handle = register_with(
        &outbox,
        &mut jobs,
        test_config().with_retry_settings(fast_retry_settings()),
        PoisonHandler,
    )
    .await?;

    publish_pings(&outbox, 1..=3).await?;
    jobs.start_poll().await?;

    // The failure is recorded while the job is still retrying — under a
    // terminal-only error surface this stays `None` forever.
    eventually(Duration::from_secs(10), || {
        let handle = handle.clone();
        async move { Ok(handle.load().await?.last_error().is_some()) }
    })
    .await?;

    let snapshot = handle.load().await?;
    assert!(
        snapshot
            .last_error()
            .is_some_and(|e| e.contains(POISON_ERROR)),
        "expected the handler's own error, got {:?}",
        snapshot.last_error()
    );
    // Alive by every other measure: never terminal, checkpoint parked before
    // the poison event. That pair is the wedge.
    assert!(!snapshot.job_status().is_terminal());
    assert_eq!(snapshot.checkpoint(), EventSequence::BEGIN);
    assert!(!snapshot.is_caught_up());

    // The barrier reports a plain timeout — identical in shape to a merely
    // backlogged handler — so the diagnosis has to come from the snapshot.
    match handle.await_caught_up(Duration::from_millis(200)).await {
        Err(SubscriptionError::CaughtUpTimeout { checkpoint, .. }) => {
            assert_eq!(checkpoint, EventSequence::BEGIN);
        }
        other => anyhow::bail!("expected CaughtUpTimeout, got {other:?}"),
    }
    assert!(handle.load().await?.last_error().is_some());

    Ok(())
}

/// Contract 9 — `await_sequence` fences on a caller-chosen target, and
/// `await_caught_up` is its special case over the call-time frontier.
#[tokio::test]
#[file_serial]
async fn await_sequence_fences_on_a_caller_chosen_target() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let handle = register(
        &outbox,
        &mut jobs,
        SkippingObserver {
            received: Arc::new(Mutex::new(Vec::new())),
        },
    )
    .await?;

    publish_pings(&outbox, 1..=5).await?;
    jobs.start_poll().await?;

    let target = EventSequence::from(3u64);
    handle
        .await_sequence(target, Duration::from_secs(60))
        .await?;
    assert!(handle.load().await?.checkpoint() >= target);

    // A target the stream has not reached is not an error — it is simply a
    // wait the handler cannot satisfy yet, and it times out honestly.
    let beyond = EventSequence::from(999u64);
    match handle.await_sequence(beyond, Duration::ZERO).await {
        Err(SubscriptionError::CaughtUpTimeout {
            checkpoint, target, ..
        }) => {
            assert_eq!(target, beyond);
            assert!(checkpoint < beyond);
        }
        other => anyhow::bail!("expected CaughtUpTimeout, got {other:?}"),
    }

    // Already-satisfied targets return without waiting.
    handle
        .await_sequence(EventSequence::BEGIN, Duration::ZERO)
        .await?;

    Ok(())
}

/// Contract 7 — the timeout is honest: a handler that never runs produces an
/// alertable error carrying the real lag, not a silent hang.
#[tokio::test]
#[file_serial]
async fn await_caught_up_times_out_with_real_numbers() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let handle = register(
        &outbox,
        &mut jobs,
        SkippingObserver {
            received: Arc::new(Mutex::new(Vec::new())),
        },
    )
    .await?;

    // Deliberately no `jobs.start_poll()`.
    publish_pings(&outbox, 1..=3).await?;

    match handle.await_caught_up(Duration::ZERO).await {
        Err(SubscriptionError::CaughtUpTimeout {
            checkpoint, target, ..
        }) => {
            assert_eq!(checkpoint, EventSequence::BEGIN);
            // `await_caught_up`'s target is the call-time frontier.
            assert_eq!(target, EventSequence::from(3u64));
        }
        other => anyhow::bail!("expected CaughtUpTimeout, got {other:?}"),
    }

    let timeout = Duration::from_millis(300);
    let started = std::time::Instant::now();
    match handle.await_caught_up(timeout).await {
        Err(SubscriptionError::CaughtUpTimeout { waited, .. }) => {
            assert!(waited >= timeout, "waited {waited:?} < timeout {timeout:?}");
        }
        other => anyhow::bail!("expected CaughtUpTimeout, got {other:?}"),
    }
    assert!(started.elapsed() >= timeout);

    Ok(())
}

/// Contract 8 — per-call anchoring (semantics 5): a handler that publishes
/// onto the outbox it consumes leaves a tail behind the frontier its own
/// fence sampled. Each barrier anchors to its own call-time frontier, so
/// fences still terminate and compose sequentially.
#[tokio::test]
#[file_serial]
async fn await_caught_up_anchors_to_the_call_time_frontier() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let mut jobs = init_jobs(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let echoed = Arc::new(Mutex::new(Vec::new()));
    let handle = register(
        &outbox,
        &mut jobs,
        RepublishingHandler {
            outbox: outbox.clone(),
            echoed: echoed.clone(),
        },
    )
    .await?;

    publish_pings(&outbox, 1..=3).await?;
    let pings_frontier = outbox.highest_known_persistent_sequence().await?;
    assert_eq!(pings_frontier, EventSequence::from(3u64));

    jobs.start_poll().await?;

    // Terminates despite the handler extending the stream as it drains — the
    // frontier is sampled once, at call time.
    handle.await_caught_up(Duration::from_secs(60)).await?;
    assert!(handle.load().await?.checkpoint() >= pings_frontier);

    // The self-publishing tail really happened: the stream grew past the
    // frontier this fence anchored to.
    assert_eq!(*echoed.lock().await, vec![1, 2, 3]);
    assert!(outbox.highest_known_persistent_sequence().await? > pings_frontier);

    // A second fence anchors to the new frontier and drains the tail. (That
    // the FIRST fence leaves observable lag is inherently timing-dependent —
    // it is exactly the caveat this contract documents — so what is asserted
    // is the part consumers rely on: sequential fences compose and converge.)
    handle.await_caught_up(Duration::from_secs(60)).await?;
    eventually(Duration::from_secs(10), || {
        let handle = handle.clone();
        async move { Ok(handle.load().await?.is_caught_up()) }
    })
    .await?;

    Ok(())
}
