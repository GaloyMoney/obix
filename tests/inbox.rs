mod helpers;

use std::sync::Arc;
use tokio::sync::Mutex;

use obix::{
    InboxEventStatus,
    inbox::{InboxEvent, InboxHandler, InboxResult},
};
use serde::{Deserialize, Serialize};
use serial_test::file_serial;

use helpers::{init_inbox, init_pool};

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
        .persist_and_process_in_op(&mut op, TestInboxEvent::DoWork(42))
        .await?;
    op.commit().await?;

    tokio::time::sleep(std::time::Duration::from_millis(3000)).await;

    let events = received.lock().await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0], TestInboxEvent::DoWork(42));

    let event = inbox.find_by_id(event_id).await?;
    assert_eq!(event.status, InboxEventStatus::Completed);

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn inbox_idempotent_push() -> anyhow::Result<()> {
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
        .push_idempotent(&mut op, "unique-key-1", TestInboxEvent::DoWork(1))
        .await?;
    op.commit().await?;
    assert!(first.is_some());

    let mut op = es_entity::DbOp::init(&pool).await?;
    let second = inbox
        .push_idempotent(&mut op, "unique-key-1", TestInboxEvent::DoWork(1))
        .await?;
    op.commit().await?;
    assert!(second.is_none());

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

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

    for i in 0..5 {
        let mut op = es_entity::DbOp::init(&pool).await?;
        inbox
            .persist_and_process_in_op(&mut op, TestInboxEvent::DoWork(i))
            .await?;
        op.commit().await?;
    }

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let events = received.lock().await;
    assert_eq!(events.len(), 5);

    Ok(())
}
