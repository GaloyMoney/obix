mod helpers;

use es_entity::clock::ClockHandle;
use serde::{Deserialize, Serialize};
use serial_test::file_serial;
use tokio::sync::Mutex;

use std::sync::Arc;

use helpers::{init_inbox, init_inbox_with_clock, init_pool, wait_for_inbox_status};
use obix::{
    InboxEventStatus,
    inbox::{InboxEvent, InboxHandler, InboxResult},
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum TestInboxEvent {
    DoWork(u64),
    FailOnce(String),
}

struct TestHandler {
    received: Arc<Mutex<Vec<TestInboxEvent>>>,
}

impl InboxHandler for TestHandler {
    async fn handle(
        &self,
        event: &InboxEvent,
    ) -> Result<InboxResult, Box<dyn std::error::Error + Send + Sync>> {
        let payload: TestInboxEvent = event.payload()?;
        self.received.lock().await.push(payload);
        Ok(InboxResult::Complete)
    }
}

struct ReprocessHandler {
    execution_times: Arc<Mutex<Vec<chrono::DateTime<chrono::Utc>>>>,
    clock: ClockHandle,
}

struct ScrubbingHandler {
    received: Arc<Mutex<Vec<TestInboxEvent>>>,
}

impl InboxHandler for ScrubbingHandler {
    async fn handle(
        &self,
        event: &InboxEvent,
    ) -> Result<InboxResult, Box<dyn std::error::Error + Send + Sync>> {
        let payload: TestInboxEvent = event.payload()?;
        self.received.lock().await.push(payload);
        Ok(InboxResult::CompleteAndScrub)
    }
}

impl InboxHandler for ReprocessHandler {
    async fn handle(
        &self,
        event: &InboxEvent,
    ) -> Result<InboxResult, Box<dyn std::error::Error + Send + Sync>> {
        let current_time = self.clock.now();
        self.execution_times.lock().await.push(current_time);

        let _payload: TestInboxEvent = event.payload()?;

        // First execution: request reprocess in 30 seconds
        if self.execution_times.lock().await.len() == 1 {
            Ok(InboxResult::ReprocessIn(std::time::Duration::from_secs(30)))
        } else {
            // Second execution: complete
            Ok(InboxResult::Complete)
        }
    }
}

#[tokio::test]
#[file_serial]
async fn inbox_processes_event() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let received = Arc::new(Mutex::new(Vec::new()));

    let inbox = init_inbox(
        &pool,
        &mut jobs,
        TestHandler {
            received: received.clone(),
        },
    )
    .await?;

    jobs.start_poll().await?;

    let mut op = es_entity::DbOp::init(&pool).await?;
    let event_id = inbox
        .persist_and_queue_job_in_op(&mut op, "test-event-1", TestInboxEvent::DoWork(42))
        .await?
        .expect("Event should be created");
    op.commit().await?;

    // Wait for the event to be processed (max 5 seconds)
    wait_for_inbox_status(
        &inbox,
        event_id,
        InboxEventStatus::Completed,
        std::time::Duration::from_secs(5),
    )
    .await?;

    let events = received.lock().await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0], TestInboxEvent::DoWork(42));

    let event = inbox.find_event_by_id(event_id).await?;
    assert_eq!(event.status, InboxEventStatus::Completed);

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn inbox_scrub_and_job_completion_are_atomic() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    remove_terminal_failure_trigger(&pool).await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .poller_config(job::JobPollerConfig {
            job_lost_interval: std::time::Duration::from_secs(2),
            ..Default::default()
        })
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;
    let received = Arc::new(Mutex::new(Vec::new()));
    let inbox = init_inbox(
        &pool,
        &mut jobs,
        ScrubbingHandler {
            received: received.clone(),
        },
    )
    .await?;

    let original_payload = TestInboxEvent::DoWork(42);
    let event_id = inbox
        .persist_and_queue_job("scrub-recovery-event", original_payload.clone())
        .await?
        .expect("Event should be created");
    install_terminal_failure_trigger(&pool, event_id).await?;
    jobs.start_poll().await?;

    wait_for_terminal_failure(&pool, std::time::Duration::from_secs(3)).await?;

    let event = inbox.find_event_by_id(event_id).await?;
    assert_eq!(event.status, InboxEventStatus::Processing);
    assert_eq!(event.payload, serde_json::to_value(&original_payload)?);
    assert_eq!(event.processed_at, None);

    wait_for_inbox_status(
        &inbox,
        event_id,
        InboxEventStatus::Completed,
        std::time::Duration::from_secs(8),
    )
    .await?;

    assert_eq!(
        received.lock().await.as_slice(),
        &[original_payload.clone(), original_payload]
    );
    let event = inbox.find_event_by_id(event_id).await?;
    assert_eq!(event.id, event_id);
    assert_eq!(
        event.idempotency_key.as_deref(),
        Some("scrub-recovery-event")
    );
    assert_eq!(event.status, InboxEventStatus::Completed);
    assert_eq!(event.payload, serde_json::Value::Null);
    assert_eq!(event.error, None);
    assert!(event.processed_at.is_some());

    remove_terminal_failure_trigger(&pool).await?;
    Ok(())
}

async fn install_terminal_failure_trigger(
    pool: &sqlx::PgPool,
    event_id: obix::inbox::InboxEventId,
) -> anyhow::Result<()> {
    sqlx::query("CREATE SEQUENCE obix_test_terminal_failure_seq")
        .execute(pool)
        .await?;
    sqlx::query(
        r#"
        CREATE FUNCTION obix_test_fail_first_terminal_transition()
        RETURNS trigger AS $$
        BEGIN
            IF nextval('obix_test_terminal_failure_seq') = 1 THEN
                RAISE EXCEPTION 'expected terminal transition failure';
            END IF;
            RETURN OLD;
        END;
        $$ LANGUAGE plpgsql
        "#,
    )
    .execute(pool)
    .await?;
    sqlx::query(&format!(
        r#"
        CREATE TRIGGER obix_test_fail_first_terminal_transition
        BEFORE DELETE ON job_executions
        FOR EACH ROW WHEN (OLD.id = '{}')
        EXECUTE FUNCTION obix_test_fail_first_terminal_transition()
        "#,
        job::JobId::from(event_id)
    ))
    .execute(pool)
    .await?;
    Ok(())
}

async fn wait_for_terminal_failure(
    pool: &sqlx::PgPool,
    timeout: std::time::Duration,
) -> anyhow::Result<()> {
    let started_at = std::time::Instant::now();
    loop {
        let failure_attempted: bool =
            sqlx::query_scalar("SELECT is_called FROM obix_test_terminal_failure_seq")
                .fetch_one(pool)
                .await?;
        if failure_attempted {
            return Ok(());
        }
        if started_at.elapsed() >= timeout {
            anyhow::bail!("timed out waiting for the forced terminal transition failure");
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

async fn remove_terminal_failure_trigger(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    sqlx::query(
        "DROP TRIGGER IF EXISTS obix_test_fail_first_terminal_transition ON job_executions",
    )
    .execute(pool)
    .await?;
    sqlx::query("DROP FUNCTION IF EXISTS obix_test_fail_first_terminal_transition()")
        .execute(pool)
        .await?;
    sqlx::query("DROP SEQUENCE IF EXISTS obix_test_terminal_failure_seq")
        .execute(pool)
        .await?;
    Ok(())
}

#[tokio::test]
#[file_serial]
async fn inbox_duplicate_idempotency_key() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let received = Arc::new(Mutex::new(Vec::new()));

    let inbox = init_inbox(
        &pool,
        &mut jobs,
        TestHandler {
            received: received.clone(),
        },
    )
    .await?;

    jobs.start_poll().await?;

    let mut op = es_entity::DbOp::init(&pool).await?;
    let first = inbox
        .persist_and_queue_job_in_op(&mut op, "unique-key-1", TestInboxEvent::DoWork(1))
        .await?;
    op.commit().await?;
    assert!(first.did_execute());
    let first_id = first.unwrap();

    let mut op = es_entity::DbOp::init(&pool).await?;
    let second = inbox
        .persist_and_queue_job_in_op(&mut op, "unique-key-1", TestInboxEvent::DoWork(1))
        .await?;
    op.commit().await?;
    assert!(second.was_already_applied());

    // Wait for the first event to be processed (max 5 seconds)
    wait_for_inbox_status(
        &inbox,
        first_id,
        InboxEventStatus::Completed,
        std::time::Duration::from_secs(5),
    )
    .await?;

    let events = received.lock().await;
    assert_eq!(events.len(), 1);

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn inbox_multiple_events() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let received = Arc::new(Mutex::new(Vec::new()));

    let inbox = init_inbox(
        &pool,
        &mut jobs,
        TestHandler {
            received: received.clone(),
        },
    )
    .await?;

    jobs.start_poll().await?;

    let mut event_ids = Vec::new();
    for i in 0..5 {
        let mut op = es_entity::DbOp::init(&pool).await?;
        let event_id = inbox
            .persist_and_queue_job_in_op(&mut op, format!("event-{}", i), TestInboxEvent::DoWork(i))
            .await?
            .expect("Event should be created");
        op.commit().await?;
        event_ids.push(event_id);
    }

    for event_id in event_ids {
        wait_for_inbox_status(
            &inbox,
            event_id,
            InboxEventStatus::Completed,
            std::time::Duration::from_secs(5),
        )
        .await?;
    }

    let events = received.lock().await;
    assert_eq!(events.len(), 5);

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn inbox_reprocess_in_with_artificial_clock() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let (clock, controller) = ClockHandle::manual();
    let initial_time = clock.now();

    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .clock(clock.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let execution_times = Arc::new(Mutex::new(Vec::new()));
    let execution_times_clone = execution_times.clone();

    let inbox = init_inbox_with_clock(
        &pool,
        &mut jobs,
        ReprocessHandler {
            execution_times: execution_times_clone,
            clock: clock.clone(),
        },
        clock.clone(),
    )
    .await?;

    jobs.start_poll().await?;

    let mut op = inbox.begin_op().await?;
    let event_id = inbox
        .persist_and_queue_job_in_op(&mut op, "reprocess-test", TestInboxEvent::DoWork(42))
        .await?
        .expect("Event should be created");
    op.commit().await?;

    // Wait for the first processing (handler returns ReprocessIn), then for the
    // runner to settle the event back to Pending. Poll rather than race a fixed
    // sleep: on a loaded CI runner the job can still be mid-run (status
    // Processing) after any fixed delay, which flaked the status assertion.
    wait_for_executions(&execution_times, 1, std::time::Duration::from_secs(5)).await?;
    assert_eq!(execution_times.lock().await[0], initial_time);

    // Event should be Pending (scheduled for reprocessing), not Completed.
    wait_for_inbox_status(
        &inbox,
        event_id,
        InboxEventStatus::Pending,
        std::time::Duration::from_secs(5),
    )
    .await?;

    // Advance clock by 20 seconds (not enough - needs 30s)
    controller.advance(std::time::Duration::from_secs(20)).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Should NOT have executed again yet
    let times_len = {
        let times = execution_times.lock().await;
        times.len()
    };
    assert_eq!(times_len, 1);

    // Advance clock by another 11 seconds (total 31s - past the 30s threshold);
    // the reprocess fires. Poll for the second execution rather than racing.
    controller.advance(std::time::Duration::from_secs(11)).await;
    wait_for_executions(&execution_times, 2, std::time::Duration::from_secs(5)).await?;
    let delay = {
        let times = execution_times.lock().await;
        times[1] - times[0]
    };
    assert!(delay >= chrono::Duration::seconds(31));

    wait_for_inbox_status(
        &inbox,
        event_id,
        InboxEventStatus::Completed,
        std::time::Duration::from_secs(5),
    )
    .await?;

    Ok(())
}

/// Poll until `execution_times` holds at least `n` entries, or time out.
/// Positive state assertions must poll rather than race a fixed sleep — a loaded
/// CI runner can lag arbitrarily behind wall-clock.
async fn wait_for_executions<T>(
    execution_times: &Arc<Mutex<Vec<T>>>,
    n: usize,
    timeout: std::time::Duration,
) -> anyhow::Result<()> {
    let start = std::time::Instant::now();
    loop {
        let len = execution_times.lock().await.len();
        if len >= n {
            return Ok(());
        }
        if start.elapsed() >= timeout {
            anyhow::bail!("timed out waiting for {n} executions (have {len})");
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}
