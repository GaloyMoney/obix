mod helpers;

use std::sync::Arc;
use tokio::sync::Mutex;

use obix::{
    InboxEventStatus,
    inbox::{InboxEvent, InboxHandler, InboxResult},
};
use serde::{Deserialize, Serialize};
use serial_test::file_serial;

use helpers::{init_inbox, init_pool, wait_for_inbox_status};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum TestInboxEvent {
    DoWork(u64),
    FailOnce(String),
}

struct TestHandler {
    received: Arc<Mutex<Vec<TestInboxEvent>>>,
}

impl InboxHandler<TestInboxEvent> for TestHandler {
    async fn handle(
        &self,
        event: &InboxEvent<TestInboxEvent>,
    ) -> Result<InboxResult, Box<dyn std::error::Error + Send + Sync>> {
        self.received.lock().await.push(event.payload.clone());
        Ok(InboxResult::Complete)
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
        .persist_and_process_in_op(&mut op, "test-event-1", TestInboxEvent::DoWork(42))
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
        .persist_and_process_in_op(&mut op, "unique-key-1", TestInboxEvent::DoWork(1))
        .await?;
    op.commit().await?;
    assert!(first.is_some());
    let first_id = first.unwrap();

    let mut op = es_entity::DbOp::init(&pool).await?;
    let second = inbox
        .persist_and_process_in_op(&mut op, "unique-key-1", TestInboxEvent::DoWork(1))
        .await?;
    op.commit().await?;
    assert!(second.is_none());

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
            .persist_and_process_in_op(&mut op, format!("event-{}", i), TestInboxEvent::DoWork(i))
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
