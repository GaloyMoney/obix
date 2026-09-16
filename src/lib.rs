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
    CommitLane, CommitLaneDisabled, DEFAULT_BACKFILL_PAGE_SIZE, DEFAULT_COMMIT_CHECKPOINT_EVERY,
    DEFAULT_COMMIT_CHECKPOINT_INTERVAL, DEFAULT_GAP_FILL_GRACE, DEFAULT_IDLE_RESYNC_INTERVAL,
    DEFAULT_NOTIFY_DEBOUNCE, DEFAULT_PARTITION_MAINTAINER_INTERVAL, DEFAULT_PARTITION_PREMAKE,
    DEFAULT_PARTITION_WIDTH, DEFAULT_PERSIST_EVENTS_BATCH_SIZE, FrontierError, MailboxConfig,
};

pub use inbox::{
    Inbox, InboxConfig, InboxError, InboxEvent, InboxEventId, InboxEventStatus, InboxHandler,
    InboxIdempotencyKey, InboxResult,
};
pub use obix_macros::{MailboxTables, OutboxEvent};
pub use out::{
    CommitOrder, CursorError, DecodeFailure, Delivery, EventCtx, EventDelivery, FlushError,
    FlushOp, Handled, InsertOrder, IsolatedOp, KeyedEventCtx, KeyedSubscriber,
    KeyedSubscriberConfig, Lane, OpCursor, Ordering, Outbox, OutboxEventJobConfig,
    PartitionMaintainerConfig, Partitions, PostPersistHook, SingletonSubscriber, StagedOp,
    StreamPosition, StreamSelection, SubscribeError, Subscription, SubscriptionDef,
    SubscriptionError, SubscriptionSnapshot, SubscriptionStreamStatus, Subscriptions, Suspended,
    UndecodableDelivery, UndecodableEventError, WakeKey, WakeKeys,
};
pub use sequence::{CommitGroupId, CommitSequence, EventSequence};
#[doc(hidden)]
pub use tables::CommitCheckpoint;
pub use tables::MailboxTables;
pub use tables::{PersistentEventRows, SubscriptionRow};
#[doc(hidden)]
pub use tables::{
    decode_persistent_event, record_ephemeral_event_type_undecodable,
    record_ephemeral_payload_undecodable, record_tracing_context_undecodable,
};
