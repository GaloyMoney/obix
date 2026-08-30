#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![cfg_attr(feature = "fail-on-warnings", deny(clippy::all))]
#![forbid(unsafe_code)]

pub mod prelude {
    pub use es_entity;
    pub use serde;
    pub use serde_json;
    pub use sqlx;
}

mod config;
mod handle;
pub mod inbox;
pub mod out;
mod sequence;
mod tables;

#[cfg(feature = "test-utils")]
pub mod test_utils;

pub use config::{
    DEFAULT_BACKFILL_PAGE_SIZE, DEFAULT_GAP_FILL_GRACE, DEFAULT_IDLE_RESYNC_INTERVAL,
    DEFAULT_NOTIFY_DEBOUNCE, DEFAULT_PARTITION_MAINTAINER_INTERVAL, DEFAULT_PARTITION_PREMAKE,
    DEFAULT_PARTITION_WIDTH, DEFAULT_PERSIST_EVENTS_BATCH_SIZE, MailboxConfig,
};
pub use inbox::{
    Inbox, InboxConfig, InboxError, InboxEvent, InboxEventId, InboxEventStatus, InboxHandler,
    InboxIdempotencyKey, InboxResult,
};
pub use obix_macros::{MailboxTables, OutboxEvent};
pub use out::{
    BatchOp, CursorError, DecodeFailure, EventCtx, FlushError, FlushOp, Handled, IsolatedOp,
    OpCursor, Outbox, OutboxEventJobConfig, PartitionMaintainerConfig, Partitions, PostPersistHook,
    SingletonSubscriber, StreamSelection, Subscription, SubscriptionError, SubscriptionSnapshot,
    SubscriptionStreamStatus, UndecodableEventError,
};
pub use sequence::EventSequence;
pub use tables::MailboxTables;
#[doc(hidden)]
pub use tables::{
    decode_persistent_event, record_ephemeral_event_type_undecodable,
    record_ephemeral_payload_undecodable, record_tracing_context_undecodable,
};
