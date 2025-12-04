#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![cfg_attr(feature = "fail-on-warnings", deny(clippy::all))]
#![forbid(unsafe_code)]

mod config;
pub mod out;
mod sequence;
mod tables;

pub use obix_macros::MailboxTables;
pub use tables::MailboxTables;
