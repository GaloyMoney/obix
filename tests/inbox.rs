mod helpers;

use es_entity::clock::{ArtificialClockConfig, ClockHandle};
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

    let (clock, controller) = ClockHandle::artificial(ArtificialClockConfig::manual());
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

    // Wait for first processing (should return ReprocessIn)
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Verify first execution happened
    let (times_len, first_execution_time) = {
        let times = execution_times.lock().await;
        (times.len(), times[0])
    };
    assert_eq!(times_len, 1);
    assert_eq!(first_execution_time, initial_time);

    // Event should be Pending (scheduled for reprocessing), not Completed
    let event = inbox.find_event_by_id(event_id).await?;
    let event_status = event.status;
    assert_eq!(event_status, InboxEventStatus::Pending);

    // Advance clock by 20 seconds (not enough - needs 30s)
    controller.advance(std::time::Duration::from_secs(20)).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Should NOT have executed again yet
    let times_len = {
        let times = execution_times.lock().await;
        times.len()
    };
    assert_eq!(times_len, 1);

    // Advance clock by another 11 seconds (total 31s - past the 30s threshold)
    controller.advance(std::time::Duration::from_secs(11)).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Now it should have executed again
    let (times_len, delay) = {
        let times = execution_times.lock().await;
        (times.len(), times[1] - times[0])
    };
    assert_eq!(times_len, 2);
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
