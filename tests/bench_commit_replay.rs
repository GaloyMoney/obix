//! Benchmark: replay a commit-lane backlog under a few backfill page settings,
//! and print a comparison table with the rows each statement kind read.
//! `#[ignore]`d; run it with `make bench-replay`.
//!
//! Parameters (environment): `OBIX_BENCH_EVENTS` (default 100000),
//! `OBIX_BENCH_ROUNDS` (3) and `OBIX_BENCH_GROUP_SIZES` (`1,1,1,5,20`, cycled).
//!
//! The consumer does nothing but count. Each round times from registering the
//! subscriber at `BEGIN` until it has received every event. Timings are
//! reported, never asserted; the span counters (rows read per statement kind)
//! are the deterministic part.

mod helpers;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::stream::StreamExt;
use obix::{CommitLane, CommitSequence, MailboxConfig, out::Outbox};
use serde::{Deserialize, Serialize};
use serial_test::file_serial;

use helpers::{TestTables, init_pool, wipeout_commit_log, wipeout_outbox_tables};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
enum TestEvent {
    Ping { n: u64 },
}

// ---- span counters ---------------------------------------------------------

#[derive(Default, Clone, Copy)]
struct Totals {
    statements: u64,
    rows: u64,
}

struct SpanData {
    name: &'static str,
    fold: Option<String>,
    rows: u64,
}

/// Sums the `rows` field per span name (and `fold` label), for the `obix.`
/// spans only. Needs nothing beyond `tracing` itself.
#[derive(Clone, Default)]
struct Counters {
    next_id: Arc<AtomicU64>,
    spans: Arc<Mutex<HashMap<u64, SpanData>>>,
}

#[derive(Default)]
struct Fields {
    rows: Option<u64>,
    fold: Option<String>,
}

impl tracing::field::Visit for Fields {
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if field.name() == "rows" {
            self.rows = Some(value);
        }
    }
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "fold" {
            self.fold = Some(value.to_string());
        }
    }
    fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
}

impl tracing::Subscriber for Counters {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.is_span() && metadata.name().starts_with("obix.")
    }

    fn new_span(&self, attrs: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let mut fields = Fields::default();
        attrs.record(&mut fields);
        self.spans.lock().unwrap().insert(
            id,
            SpanData {
                name: attrs.metadata().name(),
                fold: fields.fold,
                rows: fields.rows.unwrap_or(0),
            },
        );
        tracing::span::Id::from_u64(id)
    }

    fn record(&self, span: &tracing::span::Id, values: &tracing::span::Record<'_>) {
        let mut fields = Fields::default();
        values.record(&mut fields);
        if let (Some(rows), Some(data)) = (
            fields.rows,
            self.spans.lock().unwrap().get_mut(&span.into_u64()),
        ) {
            data.rows = rows;
        }
    }

    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, _: &tracing::Event<'_>) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

impl Counters {
    fn reset(&self) {
        self.spans.lock().unwrap().clear();
    }

    /// Totals for one span name, optionally restricted to one `fold` label.
    fn totals(&self, name: &str, fold: Option<&str>) -> Totals {
        let mut totals = Totals::default();
        for data in self.spans.lock().unwrap().values() {
            if data.name == name && fold.is_none_or(|f| data.fold.as_deref() == Some(f)) {
                totals.statements += 1;
                totals.rows += data.rows;
            }
        }
        totals
    }
}

const PAGE_SPAN: &str = "obix.persistent_cache.backfill_page";
const GROUP_SPAN: &str = "obix.sequencer.group_fetch";
const ORDER_SPAN: &str = "obix.sequencer.group_order_page";

// ---- parameters ------------------------------------------------------------

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn group_sizes() -> Vec<u64> {
    std::env::var("OBIX_BENCH_GROUP_SIZES")
        .ok()
        .map(|v| v.split(',').filter_map(|s| s.trim().parse().ok()).collect())
        .filter(|v: &Vec<u64>| !v.is_empty() && v.iter().all(|s| *s > 0))
        .unwrap_or_else(|| vec![1, 1, 1, 5, 20])
}

#[derive(Clone)]
struct Variant {
    name: String,
    page: usize,
    read_ahead: usize,
}

fn variant(name: &str, page: usize, read_ahead: usize) -> Variant {
    Variant {
        name: name.to_string(),
        page,
        read_ahead,
    }
}

/// The first variant is the baseline the ratios are taken against.
fn variants() -> Vec<Variant> {
    vec![
        variant("page=1000", 1000, 1),
        variant("page=1000 read_ahead=2", 1000, 2),
        variant("page=5000", 5000, 1),
    ]
}

fn config(v: &Variant) -> MailboxConfig {
    MailboxConfig::builder()
        .commit_lane(CommitLane::Enabled)
        .backfill_page_size(v.page)
        .backfill_read_ahead(v.read_ahead)
        .build()
}

// ---- data and rounds -------------------------------------------------------

async fn publish_backlog(
    outbox: &Outbox<TestEvent, TestTables>,
    events: usize,
    sizes: &[u64],
) -> anyhow::Result<(usize, usize)> {
    let (mut published, mut groups) = (0usize, 0usize);
    while published < events {
        let size = sizes[groups % sizes.len()].min((events - published) as u64);
        let mut op = outbox.begin_op().await?;
        for _ in 0..size {
            outbox
                .publish_persisted_in_op(
                    &mut op,
                    TestEvent::Ping {
                        n: published as u64,
                    },
                )
                .await?;
            published += 1;
        }
        op.commit().await?;
        groups += 1;
    }
    Ok((published, groups))
}

async fn max_checkpoint(pool: &sqlx::PgPool) -> anyhow::Result<i64> {
    Ok(sqlx::query_scalar(
        "SELECT COALESCE(max(commit_seq), 0)::bigint FROM persistent_outbox_commit_checkpoints",
    )
    .fetch_one(pool)
    .await?)
}

/// One timed replay from `BEGIN`; returns the elapsed time.
async fn replay(outbox: &Outbox<TestEvent, TestTables>, total: usize) -> anyhow::Result<Duration> {
    let started = Instant::now();
    let mut listener = outbox.listen_commit_ordered(CommitSequence::BEGIN)?;
    let mut count = 0usize;
    let mut last = 0i64;
    while count < total {
        let item = tokio::time::timeout(Duration::from_secs(300), listener.next())
            .await
            .map_err(|_| anyhow::anyhow!("stalled after {count} of {total}"))?
            .expect("the stream stays open")?;
        last = i64::from(item.position());
        count += 1;
    }
    let elapsed = started.elapsed();
    assert_eq!(last, total as i64, "positions must be dense to the tip");
    Ok(elapsed)
}

struct Measured {
    times: Vec<Duration>,
    page: Totals,
    group: Totals,
    order: Totals,
    live_group: Totals,
}

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Warm,
    Cold,
}

async fn run_round(
    pool: &sqlx::PgPool,
    counters: &Counters,
    mode: Mode,
    v: &Variant,
    total: usize,
) -> anyhow::Result<(Duration, Totals, Totals, Totals, Totals)> {
    match mode {
        Mode::Warm => {
            let outbox = Outbox::<TestEvent, TestTables>::init(pool, config(v)).await?;
            // Let this process's own live fold reach the tip first, so exactly
            // one fold works during the replay.
            tokio::time::sleep(Duration::from_secs(2)).await;
            counters.reset();
            let elapsed = replay(&outbox, total).await?;
            Ok(snapshot(counters, elapsed))
        }
        Mode::Cold => {
            wipeout_commit_log(pool).await?;
            counters.reset();
            let outbox = Outbox::<TestEvent, TestTables>::init(pool, config(v)).await?;
            let elapsed = replay(&outbox, total).await?;
            Ok(snapshot(counters, elapsed))
        }
    }
}

fn snapshot(counters: &Counters, elapsed: Duration) -> (Duration, Totals, Totals, Totals, Totals) {
    (
        elapsed,
        counters.totals(PAGE_SPAN, None),
        counters.totals(GROUP_SPAN, Some("backfill")),
        counters.totals(ORDER_SPAN, None),
        counters.totals(GROUP_SPAN, Some("live")),
    )
}

fn median(times: &[Duration]) -> Duration {
    let mut sorted = times.to_vec();
    sorted.sort();
    sorted[sorted.len() / 2]
}

fn print_table(mode: &str, total: usize, rows: &[(Variant, Measured)]) {
    println!("\n### {mode} — {total} events\n");
    println!(
        "| variant | min s | median s | min vs baseline | page rows/ev | group rows/ev | order rows/ev | live group rows/ev | page stmts | group stmts | order stmts |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|---|");
    let per_event = |rows: u64| rows as f64 / total as f64;
    for (v, m) in rows {
        let min = m.times.iter().min().unwrap().as_secs_f64();
        let base = rows
            .first()
            .map(|(_, b)| b.times.iter().min().unwrap().as_secs_f64())
            .unwrap_or(min);
        println!(
            "| {} | {:.2} | {:.2} | {:.2}x | {:.2} | {:.2} | {:.2} | {:.2} | {} | {} | {} |",
            v.name,
            min,
            median(&m.times).as_secs_f64(),
            min / base,
            per_event(m.page.rows),
            per_event(m.group.rows),
            per_event(m.order.rows),
            per_event(m.live_group.rows),
            m.page.statements,
            m.group.statements,
            m.order.statements,
        );
    }
}

#[tokio::test]
#[ignore = "benchmark: run with `make bench-replay`"]
#[file_serial]
async fn bench_commit_replay() -> anyhow::Result<()> {
    let counters = Counters::default();
    tracing::subscriber::set_global_default(counters.clone())?;

    let events = env_usize("OBIX_BENCH_EVENTS", 100_000);
    let rounds = env_usize("OBIX_BENCH_ROUNDS", 3).max(1);
    let sizes = group_sizes();
    let pool = init_pool().await?;

    wipeout_outbox_tables(&pool).await?;
    let (total, groups) = {
        let outbox = Outbox::<TestEvent, TestTables>::init(&pool, config(&variants()[0])).await?;
        let started = Instant::now();
        let published = publish_backlog(&outbox, events, &sizes).await?;
        println!(
            "published {} events in {} groups in {:.1}s",
            published.0,
            published.1,
            started.elapsed().as_secs_f64()
        );
        // The live fold has reached the tip once a checkpoint is near the head.
        let near_head = (total_slack(&sizes)) as i64;
        let deadline = Instant::now() + Duration::from_secs(300);
        while max_checkpoint(&pool).await? < published.0 as i64 - near_head {
            anyhow::ensure!(Instant::now() < deadline, "no checkpoint near the head");
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        published
    };
    println!(
        "{groups} groups; max checkpoint {}",
        max_checkpoint(&pool).await?
    );

    let all = variants();
    for mode in [Mode::Warm, Mode::Cold] {
        let mut measured: Vec<(Variant, Measured)> = all
            .iter()
            .map(|v| {
                (
                    v.clone(),
                    Measured {
                        times: vec![],
                        page: Totals::default(),
                        group: Totals::default(),
                        order: Totals::default(),
                        live_group: Totals::default(),
                    },
                )
            })
            .collect();
        // Rounds are interleaved across variants.
        for round in 0..rounds {
            for (v, m) in measured.iter_mut() {
                let (elapsed, page, group, order, live_group) =
                    run_round(&pool, &counters, mode, v, total).await?;
                eprintln!(
                    "round {round} {} {}: {:.2}s",
                    if mode == Mode::Warm { "warm" } else { "cold" },
                    v.name,
                    elapsed.as_secs_f64()
                );
                m.times.push(elapsed);
                (m.page, m.group, m.order, m.live_group) = (page, group, order, live_group);
                if mode == Mode::Warm {
                    // Exact: the order scan bypasses the page stream, and
                    // nothing else reads it once the live fold sits at the tip.
                    assert_eq!(page.rows, 0, "{}: backfill read payload pages", v.name);
                    assert!(
                        order.rows >= total as u64,
                        "{}: the order scan read nothing",
                        v.name
                    );
                }
            }
        }
        print_table(
            if mode == Mode::Warm { "Warm" } else { "Cold" },
            total,
            &measured,
        );
    }

    explain_order_scan(&pool, total as i64).await?;
    Ok(())
}

/// Checkpoints land every 1000 groups by default; "near the head" is within
/// that many groups' worth of events.
fn total_slack(sizes: &[u64]) -> u64 {
    let avg = sizes.iter().sum::<u64>() as f64 / sizes.len() as f64;
    (1000.0 * avg * 2.0).ceil() as u64
}

async fn explain_order_scan(pool: &sqlx::PgPool, total: i64) -> anyhow::Result<()> {
    let lines: Vec<String> = sqlx::query_scalar(
        "EXPLAIN (ANALYZE, BUFFERS)
         SELECT sequence, commit_xid, (payload IS NOT NULL) AS has_payload
         FROM persistent_outbox_events
         WHERE sequence > $1::bigint AND sequence <= $1::bigint + $2::bigint
         ORDER BY sequence",
    )
    .bind(total / 2)
    .bind(1000i64)
    .fetch_all(pool)
    .await?;
    println!("\n### EXPLAIN (ANALYZE, BUFFERS) of the order scan\n\n```");
    for line in lines {
        println!("{line}");
    }
    println!("```");
    Ok(())
}
