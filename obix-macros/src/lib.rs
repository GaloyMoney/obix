#![cfg_attr(feature = "fail-on-warnings", deny(warnings))]
#![cfg_attr(feature = "fail-on-warnings", deny(clippy::all))]
#![forbid(unsafe_code)]

mod outbox_event;
mod outbox_event_kind;
mod tables;

use proc_macro::TokenStream;
use syn::parse_macro_input;

#[proc_macro_derive(MailboxTables, attributes(obix))]
pub fn mailbox_tables_derive(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as syn::DeriveInput);
    match tables::derive(ast) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.write_errors().into(),
    }
}

#[proc_macro_derive(OutboxEvent, attributes(obix))]
pub fn outbox_event_derive(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as syn::DeriveInput);
    match outbox_event::derive(ast) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.write_errors().into(),
    }
}

/// Derives [`obix::out::OutboxEventKind`], classifying an event enum's
/// variants as *ephemeral* (at-most-once runtime broadcasts) via the
/// `#[obix(ephemeral)]` marker.
///
/// See the trait documentation for usage and semantics.
#[proc_macro_derive(OutboxEventKind, attributes(obix))]
pub fn outbox_event_kind_derive(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as syn::DeriveInput);
    match outbox_event_kind::derive(&ast) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}
