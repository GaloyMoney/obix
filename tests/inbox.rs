use std::sync::Arc;
use tokio::sync::Mutex;

use obix::{
    Inbox, InboxEventStatus,
    inbox::{InboxConfig, InboxEvent, InboxHandler, InboxResult},
};
use serde::{Deserialize, Serialize};
use serial_test::file_serial;

#[derive(obix::MailboxTables)]
pub struct TestTables;

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

pub async fn init_pool() -> anyhow::Result<sqlx::PgPool> {
    let pg_host = std::env::var("PG_HOST").unwrap_or("localhost".to_string());
    let pg_con = format!("postgres://user:password@{pg_host}:5432/pg");
    let pool = sqlx::PgPool::connect(&pg_con).await?;
    wipeout_tables(&pool).await?;
    Ok(pool)
}

async fn wipeout_tables(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    sqlx::query!("TRUNCATE inbox_events").execute(pool).await?;
    sqlx::query!("DELETE FROM jobs WHERE job_type = 'test-inbox'")
        .execute(pool)
        .await?;
    Ok(())
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

    let inbox = Inbox::<TestInboxEvent, TestTables>::new(
        &pool,
        &mut jobs,
        InboxConfig::new(job::JobType::new("test-inbox")),
        TestHandler {
            received: received.clone(),
        },
    );

    // Start job polling
    jobs.start_poll().await?;

    // Push an event
    let mut op = es_entity::DbOp::init(&pool).await?;
    let event_id = inbox
        .persist_and_process_in_op(&mut op, TestInboxEvent::DoWork(42))
        .await?;
    op.commit().await?;

    // Wait for processing
    tokio::time::sleep(std::time::Duration::from_millis(3000)).await;

    // Verify handler was called
    let events = received.lock().await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0], TestInboxEvent::DoWork(42));

    // Verify event status is completed
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

    let inbox = Inbox::<TestInboxEvent, TestTables>::new(
        &pool,
        &mut jobs,
        InboxConfig::new(job::JobType::new("test-inbox")),
        TestHandler {
            received: received.clone(),
        },
    );

    jobs.start_poll().await?;

    // Push with idempotency key
    let mut op = es_entity::DbOp::init(&pool).await?;
    let first = inbox
        .push_idempotent(&mut op, "unique-key-1", TestInboxEvent::DoWork(1))
        .await?;
    op.commit().await?;
    assert!(first.is_some());

    // Push again with same key - should return None
    let mut op = es_entity::DbOp::init(&pool).await?;
    let second = inbox
        .push_idempotent(&mut op, "unique-key-1", TestInboxEvent::DoWork(1))
        .await?;
    op.commit().await?;
    assert!(second.is_none());

    // Wait for processing
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Should only have processed once
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

    let inbox = Inbox::<TestInboxEvent, TestTables>::new(
        &pool,
        &mut jobs,
        InboxConfig::new(job::JobType::new("test-inbox")),
        TestHandler {
            received: received.clone(),
        },
    );

    jobs.start_poll().await?;

    // Push multiple events
    for i in 0..5 {
        let mut op = es_entity::DbOp::init(&pool).await?;
        inbox
            .persist_and_process_in_op(&mut op, TestInboxEvent::DoWork(i))
            .await?;
        op.commit().await?;
    }

    // Wait for processing
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Verify all events were processed
    let events = received.lock().await;
    assert_eq!(events.len(), 5);

    Ok(())
}
