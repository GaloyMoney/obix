# Obix
[![Crates.io](https://img.shields.io/crates/v/obix)](https://crates.io/crates/obix)
[![Documentation](https://docs.rs/obix/badge.svg)](https://docs.rs/obix)
[![Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Unsafe Rust forbidden](https://img.shields.io/badge/unsafe-forbidden-success.svg)](https://github.com/rust-secure-code/safety-dance/)

Implementation of the outbox pattern backed by PostgreSQL and [sqlx](https://docs.rs/sqlx/latest/sqlx/).

## Features

- Transactional outbox pattern for reliable event publishing
- Persistent events stored in PostgreSQL with sequential ordering
- Ephemeral events for transient state updates
- Real-time event delivery via PostgreSQL NOTIFY/LISTEN
- Database-verified delivery: notifications carry only hints (sequence ranges / event type + timestamp), never payloads — a forged or snooped `pg_notify` can neither inject events nor leak payloads
- Event caching for efficient replay and new listener catchup
- Automatic backfill from database for events not in cache
- Large payload handling with automatic database fallback

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
obix = "0.1"
```

### Basic Example

```rust
use serde::{Deserialize, Serialize};
use obix::{EventSequence, MailboxConfig, out::Outbox};
use futures::stream::StreamExt;

// Define your event types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum MyEvent {
    UserRegistered { user_id: String, email: String },
    OrderPlaced { order_id: String, amount: i64 },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Connect to database
    let pool = sqlx::PgPool::connect("postgresql://user:pass@localhost/db").await?;

    // Initialize outbox (uses default tables from migration)
    let outbox = Outbox::<MyEvent>::init(&pool, MailboxConfig::builder().build().expect("Couldn't build MailboxConfig")).await?;

    // Start listening for events
    let mut listener = outbox.listen_persisted(None);

    // Spawn task to handle events
    tokio::spawn(async move {
        while let Some(event) = listener.next().await {
            println!("Received event: {:?}", event.payload);
            // Process event...
        }
    });

    // Publish a persistent event within a transaction
    let mut op = outbox.begin_op().await?;
    outbox.publish_persisted_in_op(
        &mut op,
        MyEvent::UserRegistered {
            user_id: "123".to_string(),
            email: "user@example.com".to_string(),
        },
    ).await?;
    op.commit().await?;

    // Publish an ephemeral event (only latest per type is kept)
    let event_type = obix::out::EphemeralEventType::new("user_online_status");
    outbox.publish_ephemeral(
        event_type,
        MyEvent::UserRegistered {
            user_id: "123".to_string(),
            email: "user@example.com".to_string(),
        },
    ).await?;

    // Listen to ephemeral events
    let mut ephemeral_listener = outbox.listen_ephemeral();
    while let Some(event) = ephemeral_listener.next().await {
        println!("Ephemeral event: {:?}", event.payload);
    }

    Ok(())
}
```

### Setup

The outbox pattern requires two PostgreSQL tables (`persistent_outbox_events` and `ephemeral_outbox_events`). You must apply the migration to create these tables before using the library.

> **Breaking change:** `persistent_outbox_events` is **range-partitioned by `sequence`** (primary key on `sequence`, `id` demoted to a plain column). Upgrading from a pre-partitioning version is a breaking schema change with no in-place data migration — recreate the table from the shipped migration. See [Partition maintenance](#partition-maintenance) below.

#### Option 1: Copy the migration file

Copy the migration file into your project's migrations directory:
```bash
cp ./migrations/20251204130225_obix_setup.sql <path>/<to>/<your>/<project>/migrations/
```

Then run your migrations as usual with sqlx:
```rust
sqlx::migrate!("./migrations").run(&pool).await?;
```

#### Option 2: Use a custom table prefix

If you need to avoid table name conflicts or want to namespace your outbox tables, you can define custom tables with a prefix:

```rust
#[derive(obix::MailboxTables)]
#[obix(tbl_prefix = "myapp")]
struct MyAppTables;

// Initialize with custom tables
let outbox = Outbox::<MyEvent, MyAppTables>::init(&pool, MailboxConfig::builder().build().expect("Couldn't build MailboxConfig")).await?;
```

When using a custom prefix, you'll need to create a modified migration with your prefix. For example, with prefix `myapp`, the tables would be named `myapp_persistent_outbox_events` and `myapp_ephemeral_outbox_events`.

You can copy the default migration and add your prefix to all table names, sequence names, and channel names in the SQL.

### Partition maintenance

`persistent_outbox_events` is range-partitioned by `sequence`. The migration ships an initial partition covering the first 2 million sequences (~1.5 GB) plus an always-present `DEFAULT` partition, so an insert can **never** fail to route. To keep explicit partitions created ahead of the sequence head (and `DEFAULT` empty), register the partition maintainer once at startup:

```rust
use obix::PartitionMaintainerConfig;

outbox
    .register_partition_maintainer(
        &mut jobs,
        PartitionMaintainerConfig::new(job::JobType::new("outbox-partition-maintainer")),
    )
    .await?;
```

How many partitions to keep ahead and the maintainer's poll interval come from `MailboxConfig` (`partition_premake` / `partition_maintainer_interval`). Partition width is deliberately **not** configurable: it is the fixed `DEFAULT_PARTITION_WIDTH` constant, coupled to the initial partition's range in the shipped migration.

**Registration is optional and the outbox never breaks without it.** If you never register the maintainer, everything keeps working: once `sequence` passes the initial partition's upper bound, new rows land in the `DEFAULT` partition, which is still read, gap-filled, and replayed exactly like any other partition. You only forfeit the per-partition vacuum/freeze/cache-locality benefits (equivalent to the pre-partitioning single-table behaviour). A non-empty `DEFAULT` is a *layout* concern, not a correctness one — alert on it, and use [`Partitions::recover_default`](https://docs.rs/obix) to fold any stranded rows back into explicit partitions in a single transaction (never regressing `MAX(sequence)`).

### Event Types

**Persistent Events**: Stored in the database with sequential ordering, guaranteed delivery, and replay capability. Use for critical business events that must be processed reliably.

**Ephemeral Events**: Persisted to the database to enable replication across multiple runtime instances, but only the latest event per event type is kept. Later events of the same type replace earlier ones via database UPSERT. Use for current state updates like online status, real-time metrics, or any state that only needs the most recent value.

### Listening to Events

```rust
// Listen to persistent events from the beginning
let mut listener = outbox.listen_persisted(EventSequence::BEGIN);

// Listen to persistent events from a specific sequence
let mut listener = outbox.listen_persisted(EventSequence::from(42));

// Listen to new persistent events only
let mut listener = outbox.listen_persisted(None);

// Listen to ephemeral events
let mut listener = outbox.listen_ephemeral();

// Listen to all events (persistent + ephemeral)
let mut listener = outbox.listen_all(None);
```

## Errors and telemetry

obix's public methods return one of four `errlanes` carriers. Every `Fail`
signature spells itself — there are no `FooError` aliases — so a reader sees
which rejection they are handed, which is exactly what they have to branch
on. `ObixFault`, the fault-only carrier, is the one alias: it has no
rejection to hide. The rejections and fault wrappers the carriers name are
documented in `src/error.rs` (`InboxRejection` beside the inbox API that
returns it) and re-exported from `obix::`, `obix::out::` and
`obix::inbox::`:

| carrier | returned by |
|---|---|
| `ObixFault` (`= Fault<lanes!(Transient, Fatal)>`) | every method that cannot reject: `Outbox::init`/`begin_op`/`publish_ephemeral*`/`highest_known_persistent_sequence`/`register_keyed_subscriber`/`register_partition_maintainer`, `Partitions::ensure`/`recover_default`, `Subscriptions::subscribe_in_op`/`cancel`/`cancel_in_op`, `Inbox::persist_and_queue_job*`/`list_failed` |
| `Fail<CommitLaneDisabled, lanes!(Transient, Fatal)>` | `Outbox::frontier`, `Outbox::register_singleton_subscriber` |
| `Fail<SubscriptionRejection, lanes!(Transient, Fatal)>` | `Subscription::load`/`await_position`/`await_caught_up`, `Subscriptions::subscription` |
| `Fail<InboxRejection, lanes!(Transient, Fatal)>` | `Inbox::find_event_by_id` |

`ObixFault` is `obix::ObixFault`; `Fail` and `lanes!` come from `errlanes`,
re-exported as `obix::prelude::es_entity::errlanes` — a downstream signature
that spells a `Fail` carrier names them from there.

A handful of methods return a bare `errlanes::Rejection` on its own
(`WakeKeys::try_from` → `SubscribeError`,
`Outbox::listen`/`listen_commit_ordered` → `CommitLaneDisabled`), and two
methods stay on `sqlx::Error` because `es_entity::hooks::CommitHook::pre_commit`
pins it: `Outbox::publish_persisted_in_op` and `Outbox::publish_all_persisted`.

The `MailboxTables` storage trait is `sqlx::Error` throughout — it classifies
nothing. Absence comes back as `Option` and becomes
`InboxRejection::NotFound` at `Inbox::find_event_by_id`, and a column sqlx
cannot decode comes back as `sqlx::Error::ColumnDecode`, which errlanes lanes
as `Fatal(CorruptState)` one level up.

Stored data that fails to decode (a subscriber job's execution state, a keyed
subscription's `instance_config` or persisted key, an inbox event's status
column) always classifies as `Fatal(CorruptState)`, never the serde default
of `Fatal(Invariant)` — the same override job makes for its own persisted
execution state, via the named wrapper `CouldNotDecodeStored`.

### For implementors

Anything *you* implement — `SingletonSubscriber`, `KeyedSubscriber`,
`InboxHandler`, `SubscriptionDef`'s `Subscriber` — returns
`Box<dyn std::error::Error + Send + Sync>` from every fallible method, and
`PostPersistHook::on_persisted` returns `sqlx::Error` (pinned by es-entity).
obix never asks an implementor to classify or lane an error. If you return a
laned `errlanes::Fail`/`Fault` (or let one propagate through `?`) it travels
through the box unchanged, and job's `Fault::classify` finds it in the
chain — which is how a handler reaches job's dispositions: a `Transient`
congestion kind reschedules without spending an attempt, and
`terminal_on_fatal` (a `RetrySettings` field you already pass through
`OutboxEventJobConfig` / `KeyedSubscriberConfig` / `InboxConfig`, forced off
for resident types — singleton subscribers and the partition maintainer) acts
on a `Fatal`. A boxed `Fail::Rejected` has **no surviving marker** and simply
retries per policy, so resolve your own rejections inside the handler body
(skip, ack, or map them) rather than propagating them.

### Boundary spans

Every laned boundary is instrumented with `#[es_entity::errlanes::instrument]`
rather than `#[tracing::instrument(.., err)]`: the attribute declares and
records `error`, `error.lane`, `error.code`, `error.level`,
`exception.message` and `exception.type` from the returned `Fail`/`Fault`
itself, so the lane is on the span before the error is ever logged.
`PostPersistHook`'s two sqlx-pinned functions (`flush_batch`,
`persist_checkpoint` on the batch op) keep plain `#[tracing::instrument(err)]`
— job's finalizer is the boundary that disposes of the handler's own error
and records its lane. Background loops (the pg listener, the debounced
notifier, the persistent cache, feeder and sequencer, the gap filler) report
a fault's lane on their own `warn`-level spans without changing retry
behaviour — a dropped-table `Fatal(Config)` and a `Transient(ConnectionLost)`
used to look identical in the logs; they no longer do.

This requires the `es-entity/errlanes-tracing` feature, which is on
unconditionally (not gated behind obix's own optional `tracing` feature,
which only adds span/trace-context propagation on top).

## License

Licensed under the Apache License, Version 2.0.
