#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![cfg_attr(feature = "fail-on-warnings", deny(clippy::all))]
#![forbid(unsafe_code)]

pub mod prelude {
    pub use es_entity;
    pub use serde;
    pub use serde_json;
    pub use sqlx;
    #[cfg(feature = "tracing")]
    pub use tracing;
}

mod config;
mod handle;
pub mod inbox;
pub mod out;
mod sequence;
mod tables;

#[cfg(feature = "test-utils")]
pub mod test_utils;

pub use config::MailboxConfig;
pub use inbox::{
    Inbox, InboxConfig, InboxError, InboxEvent, InboxEventId, InboxEventStatus, InboxHandler,
    InboxIdempotencyKey, InboxResult,
};
pub use obix_macros::{MailboxTables, OutboxEvent};
pub use out::{
    EventHandlerRegistration, Outbox, OutboxEventHandler, OutboxEventJobConfig, OutboxEventMeta,
};
pub use sequence::EventSequence;
pub use tables::MailboxTables;
