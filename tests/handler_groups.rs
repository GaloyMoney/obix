mod helpers;

use std::sync::Arc;

use obix::{
    EventCtx, FlushOp, Handled, HandlerGroupName, MailboxConfig, OutboxEventHandler,
    OutboxEventJobConfig, out::Outbox,
};
use serde::{Deserialize, Serialize};
use serial_test::file_serial;
use tokio::sync::Mutex;

use helpers::{TestTables, init_pool, wipeout_outbox_job_tables, wipeout_outbox_tables};

const GROUP: HandlerGroupName = HandlerGroupName::new("test-handler-group");
const MEMBER_A: &str = "test-group-member-a";
const MEMBER_B: &str = "test-group-member-b";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum TestEvent {
    Ping(u64),
}

/// Records what it saw and skips — no transaction on its behalf.
struct Observer {
    received: Arc<Mutex<Vec<u64>>>,
}

impl OutboxEventHandler<TestEvent> for Observer {
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

/// Collects into a `Vec` and flushes the whole accumulator into a table inside
/// the landing transaction — so its rows are durable exactly when the group's
/// checkpoint is.
struct Collector {
    tag: &'static str,
    fail_flush_at: Option<u64>,
}

impl OutboxEventHandler<TestEvent> for Collector {
    type Batch = Vec<u64>;

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv, Vec<u64>>,
        event: &obix::out::PersistentOutboxEvent<TestEvent>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        match &event.payload {
            Some(TestEvent::Ping(n)) => Ok(ctx.collect(*n)),
            None => Ok(ctx.skip()),
        }
    }

    async fn flush(
        &self,
        op: &mut FlushOp<'_>,
        items: Vec<u64>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(poison) = self.fail_flush_at
            && items.contains(&poison)
        {
            return Err("collector flush failed".into());
        }
        use es_entity::AtomicOperation;
        for n in items {
            sqlx::query(
                "INSERT INTO test_group_effects (tag, n) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            )
            .bind(self.tag)
            .bind(n as i64)
            .execute(op.as_executor())
            .await?;
        }
        Ok(())
    }
}

/// Fails on one specific event, so the group must eject it and keep going.
struct Poisoned {
    poison: u64,
    received: Arc<Mutex<Vec<u64>>>,
}

impl OutboxEventHandler<TestEvent> for Poisoned {
    type Batch = ();

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv>,
        event: &obix::out::PersistentOutboxEvent<TestEvent>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(TestEvent::Ping(n)) = &event.payload {
            if *n == self.poison {
                return Err("poisoned event".into());
            }
            self.received.lock().await.push(*n);
        }
        Ok(ctx.skip())
    }
}

/// Isolates one specific event, fencing it from the pending group batch.
struct Isolator {
    isolate_at: u64,
}

impl OutboxEventHandler<TestEvent> for Isolator {
    type Batch = ();

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv>,
        event: &obix::out::PersistentOutboxEvent<TestEvent>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(TestEvent::Ping(n)) = &event.payload
            && *n == self.isolate_at
        {
            let op = ctx.consume_isolated().await?;
            return Ok(op.commit());
        }
        Ok(ctx.skip())
    }
}

// ── harness ─────────────────────────────────────────────────────────────────

async fn reset(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    wipeout_outbox_tables(pool).await?;
    for job_type in [GROUP.as_str(), MEMBER_A, MEMBER_B] {
        wipeout_outbox_job_tables(pool, job_type).await?;
    }
    sqlx::query("DROP TABLE IF EXISTS test_group_effects")
        .execute(pool)
        .await?;
    sqlx::query("CREATE TABLE test_group_effects (tag TEXT NOT NULL, n BIGINT NOT NULL, PRIMARY KEY (tag, n))")
        .execute(pool)
        .await?;
    Ok(())
}

async fn init_outbox(pool: &sqlx::PgPool) -> anyhow::Result<Outbox<TestEvent, TestTables>> {
    Ok(Outbox::<TestEvent, TestTables>::init(
        pool,
        MailboxConfig::builder()
            .build()
            .expect("Couldn't build MailboxConfig"),
    )
    .await?)
}

fn member(job_type: &'static str) -> OutboxEventJobConfig {
    OutboxEventJobConfig::new(job::JobType::new(job_type)).in_group(&GROUP)
}

async fn jobs_for(pool: &sqlx::PgPool) -> anyhow::Result<job::Jobs> {
    let config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    Ok(job::Jobs::init(config).await?)
}

async fn publish(outbox: &Outbox<TestEvent, TestTables>, ns: &[u64]) -> anyhow::Result<()> {
    let mut op = outbox.begin_op().await?;
    for n in ns {
        outbox
            .publish_persisted_in_op(&mut op, TestEvent::Ping(*n))
            .await?;
    }
    op.commit().await?;
    Ok(())
}

async fn wait_for(
    received: &Mutex<Vec<u64>>,
    n: usize,
    timeout: std::time::Duration,
) -> anyhow::Result<()> {
    let start = std::time::Instant::now();
    loop {
        if received.lock().await.len() >= n {
            return Ok(());
        }
        if start.elapsed() > timeout {
            anyhow::bail!(
                "timeout waiting for {n} deliveries, got {:?}",
                received.lock().await
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// The group's persisted per-member checkpoint map.
async fn group_state(
    pool: &sqlx::PgPool,
) -> anyhow::Result<Option<std::collections::BTreeMap<String, i64>>> {
    let row: Option<(Option<serde_json::Value>,)> = sqlx::query_as(
        "SELECT je.execution_state_json FROM job_executions je \
         JOIN jobs j ON j.id = je.id WHERE j.job_type = $1",
    )
    .bind(GROUP.as_str())
    .fetch_optional(pool)
    .await?;
    Ok(row.and_then(|(json,)| json).and_then(|json| {
        json.get("members")
            .and_then(|m| serde_json::from_value(m.clone()).ok())
    }))
}

async fn wait_for_member_checkpoint(
    pool: &sqlx::PgPool,
    member: &str,
    expected: i64,
) -> anyhow::Result<()> {
    let start = std::time::Instant::now();
    loop {
        if let Some(members) = group_state(pool).await?
            && members.get(member) == Some(&expected)
        {
            return Ok(());
        }
        if start.elapsed() > std::time::Duration::from_secs(10) {
            anyhow::bail!(
                "timeout waiting for {member} checkpoint {expected}, at {:?}",
                group_state(pool).await?
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

async fn effect_rows(pool: &sqlx::PgPool, tag: &str) -> anyhow::Result<Vec<i64>> {
    let rows: Vec<(i64,)> =
        sqlx::query_as("SELECT n FROM test_group_effects WHERE tag = $1 ORDER BY n")
            .bind(tag)
            .fetch_all(pool)
            .await?;
    Ok(rows.into_iter().map(|(n,)| n).collect())
}

async fn running_jobs(pool: &sqlx::PgPool, job_types: &[&str]) -> anyhow::Result<i64> {
    let (count,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM job_executions je JOIN jobs j ON j.id = je.id \
         WHERE j.job_type = ANY($1)",
    )
    .bind(job_types)
    .fetch_one(pool)
    .await?;
    Ok(count)
}

// ── contracts ───────────────────────────────────────────────────────────────

/// Two members, one job row: the whole point of grouping.
#[tokio::test]
#[file_serial]
async fn group_members_share_one_job() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    reset(&pool).await?;
    let mut jobs = jobs_for(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let a = Arc::new(Mutex::new(Vec::new()));
    let b = Arc::new(Mutex::new(Vec::new()));
    outbox
        .register_event_handler(
            &mut jobs,
            member(MEMBER_A),
            Observer {
                received: a.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    outbox
        .register_event_handler(
            &mut jobs,
            member(MEMBER_B),
            Observer {
                received: b.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    jobs.start_poll().await?;

    publish(&outbox, &[1, 2, 3]).await?;
    wait_for(&a, 3, std::time::Duration::from_secs(10)).await?;
    wait_for(&b, 3, std::time::Duration::from_secs(10)).await?;

    // Both members saw every event, in order...
    assert_eq!(*a.lock().await, vec![1, 2, 3]);
    assert_eq!(*b.lock().await, vec![1, 2, 3]);
    // ...through exactly one job, and none of their own.
    assert_eq!(running_jobs(&pool, &[GROUP.as_str()]).await?, 1);
    assert_eq!(running_jobs(&pool, &[MEMBER_A, MEMBER_B]).await?, 0);

    Ok(())
}

/// One landing carries every member's flush and every member's checkpoint.
#[tokio::test]
#[file_serial]
async fn one_landing_carries_every_members_work_and_checkpoint() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    reset(&pool).await?;
    let mut jobs = jobs_for(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    for (job_type, tag) in [(MEMBER_A, "a"), (MEMBER_B, "b")] {
        outbox
            .register_event_handler(
                &mut jobs,
                member(job_type),
                Collector {
                    tag,
                    fail_flush_at: None,
                },
            )
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
    }
    jobs.start_poll().await?;

    publish(&outbox, &[1, 2, 3]).await?;
    wait_for_member_checkpoint(&pool, MEMBER_A, 3).await?;

    // Both members' collected items landed, and both checkpoints advanced —
    // out of a single execution-state row.
    assert_eq!(effect_rows(&pool, "a").await?, vec![1, 2, 3]);
    assert_eq!(effect_rows(&pool, "b").await?, vec![1, 2, 3]);
    let members = group_state(&pool).await?.expect("group state written");
    assert_eq!(members.get(MEMBER_A), Some(&3));
    assert_eq!(members.get(MEMBER_B), Some(&3));

    Ok(())
}

/// A member's `consume_isolated` fence must land the *whole* pending group
/// batch first — including a sibling's collected items — not just its own.
#[tokio::test]
#[file_serial]
async fn isolation_fence_lands_siblings_collected_items() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    reset(&pool).await?;
    let mut jobs = jobs_for(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    // A collects everything; B isolates on 3. A's items for 1..=3 must be
    // durable before B's isolated op runs.
    outbox
        .register_event_handler(
            &mut jobs,
            member(MEMBER_A),
            Collector {
                tag: "a",
                fail_flush_at: None,
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    outbox
        .register_event_handler(&mut jobs, member(MEMBER_B), Isolator { isolate_at: 3 })
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    jobs.start_poll().await?;

    publish(&outbox, &[1, 2, 3, 4]).await?;
    wait_for_member_checkpoint(&pool, MEMBER_B, 4).await?;

    assert_eq!(effect_rows(&pool, "a").await?, vec![1, 2, 3, 4]);
    Ok(())
}

/// A member that cannot handle an event is parked before it; its siblings
/// advance past it rather than stalling behind it.
#[tokio::test]
#[file_serial]
async fn failing_member_is_ejected_and_siblings_advance() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    reset(&pool).await?;
    let mut jobs = jobs_for(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let healthy = Arc::new(Mutex::new(Vec::new()));
    let poisoned = Arc::new(Mutex::new(Vec::new()));
    outbox
        .register_event_handler(
            &mut jobs,
            member(MEMBER_A),
            Poisoned {
                poison: 3,
                received: poisoned.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    outbox
        .register_event_handler(
            &mut jobs,
            member(MEMBER_B),
            Observer {
                received: healthy.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    jobs.start_poll().await?;

    publish(&outbox, &[1, 2, 3, 4, 5]).await?;

    // The checkpoint is the deterministic signal; the delivery vector is only
    // stable once it lands.
    wait_for_member_checkpoint(&pool, MEMBER_B, 5).await?;

    // The healthy sibling gets everything, including events past the poison.
    // Ejection rewinds the in-flight batch and replays it without the offender,
    // so the sibling legitimately re-sees the events it had already handled in
    // memory but not yet committed — whole-batch replay is the DSL's contract.
    // What must hold is that it sees every event, in order, with none skipped.
    let seen = healthy.lock().await.clone();
    let mut first_seen = Vec::new();
    for n in &seen {
        if !first_seen.contains(n) {
            first_seen.push(*n);
        }
    }
    assert_eq!(
        first_seen,
        vec![1, 2, 3, 4, 5],
        "actual deliveries: {seen:?}"
    );

    // The ejected member's key is retained, parked at its last *committed*
    // sequence — which here is BEGIN, since it had only skipped and the lazy
    // checkpoint had not yet fired. What matters is that the park is strictly
    // before the poison event, so a restart re-reads it and nothing is skipped.
    let members = group_state(&pool).await?.expect("group state written");
    let parked = *members
        .get(MEMBER_A)
        .expect("ejected member's checkpoint is retained, not dropped");
    assert!(parked < 3, "parked at {parked}, must be before the poison");

    // And it stops there: no delivery past the event it could not handle.
    assert!(
        !poisoned.lock().await.iter().any(|n| *n >= 3),
        "ejected member must not receive events past its parked checkpoint: {:?}",
        poisoned.lock().await
    );

    Ok(())
}

/// Moving a handler from solo to grouped keeps its position: it must not
/// replay the outbox from the beginning.
#[tokio::test]
#[file_serial]
async fn grouping_adopts_the_solo_checkpoint() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    reset(&pool).await?;

    // Phase 1: run MEMBER_A as an ordinary solo handler and let it checkpoint.
    let seen_solo = Arc::new(Mutex::new(Vec::new()));
    {
        let mut jobs = jobs_for(&pool).await?;
        let outbox = init_outbox(&pool).await?;
        outbox
            .register_event_handler(
                &mut jobs,
                OutboxEventJobConfig::new(job::JobType::new(MEMBER_A)),
                Observer {
                    received: seen_solo.clone(),
                },
            )
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        jobs.start_poll().await?;
        publish(&outbox, &[1, 2, 3]).await?;
        wait_for(&seen_solo, 3, std::time::Duration::from_secs(10)).await?;
        // Let the lazy skip-only checkpoint reach the database.
        let start = std::time::Instant::now();
        loop {
            let row: Option<(Option<serde_json::Value>,)> = sqlx::query_as(
                "SELECT je.execution_state_json FROM job_executions je \
                 JOIN jobs j ON j.id = je.id WHERE j.job_type = $1",
            )
            .bind(MEMBER_A)
            .fetch_optional(&pool)
            .await?;
            let seq = row
                .and_then(|(json,)| json)
                .and_then(|json| json.get("sequence").and_then(|s| s.as_i64()));
            if seq == Some(3) {
                break;
            }
            if start.elapsed() > std::time::Duration::from_secs(10) {
                anyhow::bail!("solo handler never checkpointed at 3 (at {seq:?})");
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        jobs.shutdown().await?;
    }

    // Phase 2: same handler, now grouped. It must resume at 3.
    let seen_grouped = Arc::new(Mutex::new(Vec::new()));
    let mut jobs = jobs_for(&pool).await?;
    let outbox = init_outbox(&pool).await?;
    outbox
        .register_event_handler(
            &mut jobs,
            member(MEMBER_A),
            Observer {
                received: seen_grouped.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    jobs.start_poll().await?;

    publish(&outbox, &[4, 5]).await?;
    wait_for(&seen_grouped, 2, std::time::Duration::from_secs(10)).await?;

    // Only the new events — the first three were not replayed.
    assert_eq!(*seen_grouped.lock().await, vec![4, 5]);
    // And the legacy solo execution row is gone, so it can never resurrect.
    assert_eq!(running_jobs(&pool, &[MEMBER_A]).await?, 0);

    Ok(())
}

/// Registering into a group after its job started would silently deliver
/// nothing — it must be an error instead.
#[tokio::test]
#[file_serial]
async fn late_registration_is_rejected() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    reset(&pool).await?;
    let mut jobs = jobs_for(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    let a = Arc::new(Mutex::new(Vec::new()));
    outbox
        .register_event_handler(
            &mut jobs,
            member(MEMBER_A),
            Observer {
                received: a.clone(),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    jobs.start_poll().await?;
    // Wait until the group job has actually started and snapshotted membership.
    publish(&outbox, &[1]).await?;
    wait_for(&a, 1, std::time::Duration::from_secs(10)).await?;

    let err = outbox
        .register_event_handler(
            &mut jobs,
            member(MEMBER_B),
            Observer {
                received: Arc::new(Mutex::new(Vec::new())),
            },
        )
        .await
        .expect_err("registering into a started group must fail");
    assert!(
        err.to_string().contains("after the group job started"),
        "unexpected error: {err}"
    );

    Ok(())
}

/// Batch size governs the shared landing, so members disagreeing about it is
/// an error rather than a silently folded value.
#[tokio::test]
#[file_serial]
async fn conflicting_group_settings_are_rejected() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    reset(&pool).await?;
    let mut jobs = jobs_for(&pool).await?;
    let outbox = init_outbox(&pool).await?;

    outbox
        .register_event_handler(
            &mut jobs,
            member(MEMBER_A).with_max_batch_size(10),
            Observer {
                received: Arc::new(Mutex::new(Vec::new())),
            },
        )
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let err = outbox
        .register_event_handler(
            &mut jobs,
            member(MEMBER_B).with_max_batch_size(99),
            Observer {
                received: Arc::new(Mutex::new(Vec::new())),
            },
        )
        .await
        .expect_err("conflicting max_batch_size must fail");
    assert!(
        err.to_string().contains("max_batch_size"),
        "unexpected error: {err}"
    );

    Ok(())
}
