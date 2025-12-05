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
pub mod out;
mod sequence;
mod tables;

pub use obix_macros::MailboxTables;
pub use sequence::EventSequence;
pub use tables::MailboxTables;
