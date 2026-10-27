mod helpers;

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::StreamExt;
use obix::{
    EphemeralConfig, EphemeralEvent, EphemeralEventType, EphemeralOutbox, EphemeralSubscriber,
    EphemeralSubscriberConfig, MailboxConfig, RawPayload,
};
use serde::{Deserialize, Serialize};
use serial_test::file_serial;
use tokio::sync::Mutex;

use helpers::{
    Other, Status, StatusOrTick, TestTables, Tick, init_ephemeral, init_outbox, init_pool,
    wipeout_outbox_job_tables,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum DurableEvent {
    Saved(u64),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum FleetingEvent {
    Tick(u64),
}

impl EphemeralEvent for FleetingEvent {
    const EVENT_TYPE: EphemeralEventType = EphemeralEventType::new("test.fleeting");
}

/// A pool whose connections are attributable to this test run alone, so
/// LISTEN connections can be counted without other tests perturbing the number.
async fn init_named_pool() -> anyhow::Result<(sqlx::PgPool, String)> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let name = format!("obix-ephemeral-test-{}-{nanos}", std::process::id());
    let options = sqlx::postgres::PgConnectOptions::from_str(&std::env::var("PG_CON")?)?
        .application_name(&name);
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_with(options)
        .await?;
    Ok((pool, name))
}

async fn listen_connections(
    observer: &sqlx::PgPool,
    application_name: &str,
) -> anyhow::Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1 AND query LIKE 'LISTEN%'",
    )
    .bind(application_name)
    .fetch_one(observer)
    .await?)
}

async fn row_count(pool: &sqlx::PgPool, table: &str) -> anyhow::Result<i64> {
    Ok(sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
        .fetch_one(pool)
        .await?)
}

#[tokio::test]
#[file_serial]
async fn the_two_handles_deliver_their_own_payload_types_independently() -> anyhow::Result<()> {
    let observer = init_pool().await?;
    let (pool, application_name) = init_named_pool().await?;

    let outbox = init_outbox::<DurableEvent>(&pool, MailboxConfig::builder().build()).await?;
    assert_eq!(
        listen_connections(&observer, &application_name).await?,
        1,
        "a persistent-only process opens exactly one LISTEN connection"
    );

    let ephemeral = init_ephemeral(&pool, EphemeralConfig::builder().build()).await?;
    assert_eq!(
        listen_connections(&observer, &application_name).await?,
        2,
        "a process holding both handles opens two LISTEN connections"
    );

    let mut persistent_listener = outbox.listen_persisted(None);
    let mut ephemeral_listener = ephemeral.listen::<FleetingEvent>();

    let mut op = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut op, DurableEvent::Saved(1))
        .await?;
    op.commit().await?;
    ephemeral.publish(FleetingEvent::Tick(2)).await?;

    let persisted = tokio::time::timeout(Duration::from_secs(5), persistent_listener.next())
        .await?
        .expect("persistent stream is open")?;
    assert_eq!(persisted.payload, Some(DurableEvent::Saved(1)));

    let ephemeral_event = tokio::time::timeout(Duration::from_secs(5), ephemeral_listener.next())
        .await?
        .expect("ephemeral stream is open");
    assert_eq!(ephemeral_event.event_type, FleetingEvent::EVENT_TYPE);
    assert_eq!(ephemeral_event.payload, FleetingEvent::Tick(2));

    assert!(
        tokio::time::timeout(Duration::from_millis(500), persistent_listener.next())
            .await
            .is_err(),
        "the persistent listener must not see the ephemeral event"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(500), ephemeral_listener.next())
            .await
            .is_err(),
        "the ephemeral listener must not see the persistent event"
    );

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn publish_in_op_shares_the_persistent_ops_fate() -> anyhow::Result<()> {
    let pool = init_pool().await?;

    let outbox = init_outbox::<DurableEvent>(&pool, MailboxConfig::builder().build()).await?;
    let ephemeral = init_ephemeral(&pool, EphemeralConfig::builder().build()).await?;

    let mut persistent_listener = outbox.listen_persisted(None);
    let mut ephemeral_listener = ephemeral.listen::<StatusOrTick>();

    let mut rolled_back = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut rolled_back, DurableEvent::Saved(1))
        .await?;
    ephemeral.publish_in_op(&mut rolled_back, Status(1)).await?;
    ephemeral.publish_in_op(&mut rolled_back, Tick(2)).await?;
    drop(rolled_back);

    assert!(
        tokio::time::timeout(Duration::from_millis(500), persistent_listener.next())
            .await
            .is_err(),
        "a rolled-back op must publish no persistent event"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(500), ephemeral_listener.next())
            .await
            .is_err(),
        "a rolled-back op must publish no ephemeral event"
    );
    assert_eq!(row_count(&pool, "persistent_outbox_events").await?, 0);
    assert_eq!(row_count(&pool, "ephemeral_outbox_events").await?, 0);

    // Two ephemeral publishes in one op: the second merges into the first's
    // pending commit hook.
    let mut committed = outbox.begin_op().await?;
    outbox
        .publish_persisted_in_op(&mut committed, DurableEvent::Saved(3))
        .await?;
    ephemeral.publish_in_op(&mut committed, Status(10)).await?;
    ephemeral.publish_in_op(&mut committed, Tick(20)).await?;
    committed.commit().await?;

    let persisted = tokio::time::timeout(Duration::from_secs(5), persistent_listener.next())
        .await?
        .expect("persistent stream is open")?;
    assert_eq!(persisted.payload, Some(DurableEvent::Saved(3)));

    let mut received = Vec::new();
    for _ in 0..2 {
        let event = tokio::time::timeout(Duration::from_secs(5), ephemeral_listener.next())
            .await?
            .expect("ephemeral stream is open");
        let event = Arc::into_inner(event).expect("the listener holds the only reference");
        received.push((event.event_type, event.payload));
    }
    received.sort_by_key(|(event_type, _)| event_type.to_string());
    assert_eq!(
        received,
        vec![
            (Status::EVENT_TYPE, StatusOrTick::Status(Status(10))),
            (Tick::EVENT_TYPE, StatusOrTick::Tick(Tick(20))),
        ]
    );
    assert_eq!(row_count(&pool, "persistent_outbox_events").await?, 1);
    assert_eq!(row_count(&pool, "ephemeral_outbox_events").await?, 2);

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn two_ephemeral_handles_each_hear_an_externally_written_row() -> anyhow::Result<()> {
    let observer = init_pool().await?;
    let (pool, application_name) = init_named_pool().await?;

    let first = init_ephemeral(&pool, EphemeralConfig::builder().build()).await?;
    assert_eq!(listen_connections(&observer, &application_name).await?, 1);
    let second = init_ephemeral(&pool, EphemeralConfig::builder().build()).await?;
    assert_eq!(
        listen_connections(&observer, &application_name).await?,
        2,
        "each EphemeralOutbox holds its own LISTEN connection"
    );

    let mut first_listener = first.listen::<FleetingEvent>();
    let mut second_listener = second.listen::<FleetingEvent>();

    sqlx::query("INSERT INTO ephemeral_outbox_events (event_type, payload) VALUES ($1, $2)")
        .bind(FleetingEvent::EVENT_TYPE.as_str())
        .bind(serde_json::json!({"Tick": 5}))
        .execute(&observer)
        .await?;

    for listener in [&mut first_listener, &mut second_listener] {
        let event = tokio::time::timeout(Duration::from_secs(5), listener.next())
            .await?
            .expect("ephemeral stream is open");
        assert_eq!(event.event_type, FleetingEvent::EVENT_TYPE);
        assert_eq!(event.payload, FleetingEvent::Tick(5));
    }

    Ok(())
}

/// A downstream-shaped consumer: it holds an `EphemeralOutbox` with no
/// payload parameter, publishes a leaf and listens on a derived selection.
struct Downstream {
    ephemeral: EphemeralOutbox<TestTables>,
}

impl Downstream {
    async fn announce(&self, n: u64) -> anyhow::Result<()> {
        self.ephemeral.publish(Status(n)).await?;
        Ok(())
    }

    fn hear(&self) -> obix::EphemeralOutboxListener<StatusOrTick> {
        self.ephemeral.listen::<StatusOrTick>()
    }
}

#[tokio::test]
#[file_serial]
async fn a_consumer_without_a_payload_parameter_publishes_and_hears_a_selection()
-> anyhow::Result<()> {
    let pool = init_pool().await?;
    let downstream = Downstream {
        ephemeral: init_ephemeral(&pool, EphemeralConfig::builder().build()).await?,
    };

    let mut listener = downstream.hear();
    downstream.announce(3).await?;

    let event = tokio::time::timeout(Duration::from_secs(5), listener.next())
        .await?
        .expect("ephemeral stream is open");
    assert_eq!(event.payload, StatusOrTick::Status(Status(3)));

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn selection_backfill_is_filtered_to_its_keys() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let ephemeral = init_ephemeral(&pool, EphemeralConfig::builder().build()).await?;

    ephemeral.publish(Status(1)).await?;
    ephemeral.publish(Tick(2)).await?;
    ephemeral.publish(Other(3)).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut single = ephemeral.listen::<Status>();
    let backfilled = tokio::time::timeout(Duration::from_secs(5), single.next())
        .await?
        .expect("ephemeral stream is open");
    assert_eq!(backfilled.payload, Status(1));
    assert!(
        tokio::time::timeout(Duration::from_millis(500), single.next())
            .await
            .is_err(),
        "listen::<Status>() backfills exactly one row"
    );

    let mut pair = ephemeral.listen::<StatusOrTick>();
    let mut backfilled = Vec::new();
    for _ in 0..2 {
        let event = tokio::time::timeout(Duration::from_secs(5), pair.next())
            .await?
            .expect("ephemeral stream is open");
        backfilled.push(event.event_type.to_string());
    }
    backfilled.sort();
    assert_eq!(backfilled, vec!["test.status", "test.tick"]);
    assert!(
        tokio::time::timeout(Duration::from_millis(500), pair.next())
            .await
            .is_err(),
        "listen::<StatusOrTick>() backfills exactly two rows"
    );

    Ok(())
}

#[tokio::test]
#[file_serial]
async fn listener_skips_a_row_it_cannot_decode() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let ephemeral = init_ephemeral(&pool, EphemeralConfig::builder().build()).await?;

    let mut live = ephemeral.listen::<Status>();

    sqlx::query("INSERT INTO ephemeral_outbox_events (event_type, payload) VALUES ($1, $2)")
        .bind(Status::EVENT_TYPE.as_str())
        .bind(serde_json::json!({"not": "a status"}))
        .execute(&pool)
        .await?;
    // The hint + fetch path puts the undecodable row in the cache and
    // broadcasts it; a listener created now backfills it.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut late = ephemeral.listen::<Status>();

    ephemeral.publish(Status(7)).await?;

    for listener in [&mut live, &mut late] {
        let event = tokio::time::timeout(Duration::from_secs(5), listener.next())
            .await?
            .expect("the stream does not end on an undecodable row");
        assert_eq!(
            event.payload,
            Status(7),
            "nothing is yielded before Status(7)"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(300), listener.next())
                .await
                .is_err(),
            "the stream is still open"
        );
    }

    Ok(())
}

const TICK_KEYS: &[EphemeralEventType] = &[Tick::EVENT_TYPE];

#[tokio::test]
#[file_serial]
async fn listen_raw_yields_undecoded_rows_for_the_keys_asked() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    let ephemeral = init_ephemeral(&pool, EphemeralConfig::builder().build()).await?;

    let mut raw = ephemeral.listen_raw(TICK_KEYS);

    ephemeral.publish(Status(1)).await?;
    ephemeral.publish(Tick(5)).await?;

    let event = tokio::time::timeout(Duration::from_secs(5), raw.next())
        .await?
        .expect("ephemeral stream is open");
    assert_eq!(event.event_type, Tick::EVENT_TYPE);
    assert_eq!(event.payload.get(), "5");
    assert!(
        tokio::time::timeout(Duration::from_millis(500), raw.next())
            .await
            .is_err(),
        "nothing arrives for Status"
    );

    Ok(())
}

const STATUS_AND_TICK_KEYS: &[EphemeralEventType] = &[Status::EVENT_TYPE, Tick::EVENT_TYPE];

struct KeyRecorder {
    seen: Arc<Mutex<Vec<String>>>,
}

impl EphemeralSubscriber<RawPayload> for KeyRecorder {
    async fn handle(
        &self,
        event: &Arc<obix::EphemeralOutboxEvent<RawPayload>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.seen.lock().await.push(event.event_type.to_string());
        Ok(())
    }
}

const RAW_JOB_TYPE: &str = "test-ephemeral-raw";

#[tokio::test]
#[file_serial]
async fn raw_subscriber_hears_the_union_of_its_keys() -> anyhow::Result<()> {
    let pool = init_pool().await?;
    wipeout_outbox_job_tables(&pool, RAW_JOB_TYPE).await?;
    let job_config = job::JobSvcConfig::builder()
        .pool(pool.clone())
        .build()
        .unwrap();
    let mut jobs = job::Jobs::init(job_config).await?;

    let ephemeral = init_ephemeral(&pool, EphemeralConfig::builder().build()).await?;
    let seen = Arc::new(Mutex::new(Vec::new()));
    ephemeral
        .register_subscriber_raw(
            &mut jobs,
            EphemeralSubscriberConfig::new(job::JobType::new(RAW_JOB_TYPE)),
            STATUS_AND_TICK_KEYS,
            KeyRecorder { seen: seen.clone() },
        )
        .await?;
    jobs.start_poll().await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    ephemeral.publish(Status(1)).await?;
    ephemeral.publish(Tick(2)).await?;
    ephemeral.publish(Other(3)).await?;

    let start = std::time::Instant::now();
    loop {
        if seen.lock().await.len() >= 2 {
            break;
        }
        if start.elapsed() > Duration::from_secs(5) {
            anyhow::bail!("timeout waiting for the raw subscriber");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut seen = seen.lock().await.clone();
    seen.sort();
    assert_eq!(seen, vec!["test.status", "test.tick"]);

    let _ = jobs.shutdown().await;
    Ok(())
}
