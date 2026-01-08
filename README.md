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

## License

Licensed under the Apache License, Version 2.0.
