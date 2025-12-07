use darling::{FromDeriveInput, ToTokens};
use proc_macro2::TokenStream;
use quote::{TokenStreamExt, quote};

#[derive(Debug, Clone, FromDeriveInput)]
#[darling(attributes(obix))]
pub struct MailboxTables {
    ident: syn::Ident,
    #[darling(default, rename = "tbl_prefix")]
    prefix: Option<syn::LitStr>,
    #[darling(default = "default_crate_name", rename = "crate")]
    crate_name: syn::LitStr,
}
fn default_crate_name() -> syn::LitStr {
    syn::LitStr::new("obix", proc_macro2::Span::call_site())
}

pub fn derive(ast: syn::DeriveInput) -> darling::Result<proc_macro2::TokenStream> {
    let tables = MailboxTables::from_derive_input(&ast)?;
    Ok(quote!(#tables))
}

impl ToTokens for MailboxTables {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let ident = &self.ident;
        let crate_name: syn::Path = self.crate_name.parse().expect("invalid crate path");
        #[cfg(feature = "tracing")]
        let (extract_tracing, set_context) = (
            quote! {
                let tracing_context = es_entity::context::TracingContext::current();
                let tracing_json =
                    serde_json::to_value(&tracing_context).expect("Could not serialize tracing context");
            },
            quote! { tracing_context: tracing_context.clone() },
        );
        #[cfg(not(feature = "tracing"))]
        let (extract_tracing, set_context) = (
            quote! {
                let tracing_json = None::<serde_json::Value>;
            },
            quote! {},
        );

        let table_prefix = self
            .prefix
            .as_ref()
            .map(|p| format!("{}_", p.value()))
            .unwrap_or_default();

        let persistent_outbox_events_channel = format!("{}persistent_outbox_events", table_prefix);
        let ephemeral_outbox_events_channel = format!("{}ephemeral_outbox_events", table_prefix);
        let highest_known_query = format!(
            "SELECT COALESCE(MAX(sequence), 0) AS \"max!\" FROM {}persistent_outbox_events",
            table_prefix
        );

        let persist_events_query = format!(
            r#"WITH new_events AS (
                   INSERT INTO {}persistent_outbox_events (payload, tracing_context, recorded_at)
                   SELECT unnest($1::jsonb[]) AS payload, $2::jsonb AS tracing_context, COALESCE($3::timestamptz, NOW()) AS recorded_at
                   RETURNING id, sequence, recorded_at
               )
               SELECT * FROM new_events"#,
            table_prefix
        );

        let persist_ephemeral_events_query = format!(
            r#"
            INSERT INTO {}ephemeral_outbox_events (event_type, payload, tracing_context, recorded_at)
            VALUES ($1, $2, $3, COALESCE($4::timestamptz, NOW()))
            ON CONFLICT (event_type) DO UPDATE
            SET payload = EXCLUDED.payload,
                tracing_context = EXCLUDED.tracing_context,
                recorded_at = COALESCE($4::timestamptz, NOW())
            RETURNING recorded_at"#,
            table_prefix
        );

        tokens.append_all(quote! {
            impl #crate_name::MailboxTables for #ident {
                fn persistent_outbox_events_channel() -> &'static str {
                    #persistent_outbox_events_channel
                }
                fn ephemeral_outbox_events_channel() -> &'static str {
                    #ephemeral_outbox_events_channel
                }

                fn highest_known_persistent_sequence<'a>(
                    op: impl #crate_name::prelude::es_entity::IntoOneTimeExecutor<'a>,
                ) -> impl std::future::Future<Output = Result<#crate_name::EventSequence, #crate_name::prelude::sqlx::Error>> + Send {
                    let executor = op.into_executor();
                    async {
                        let row = executor
                            .fetch_one(sqlx::query!(
                                    #highest_known_query
                            ))
                            .await?;
                        Ok(#crate_name::EventSequence::from(row.max as u64))
                    }
                }

                fn persist_events<'a, P>(
                    op: &mut #crate_name::prelude::es_entity::hooks::HookOperation<'a>,
                    events: impl Iterator<Item = P>,
                ) -> impl std::future::Future<Output = Result<Vec<#crate_name::out::PersistentOutboxEvent<P>>, #crate_name::prelude::sqlx::Error>> + Send
                where
                    P: #crate_name::prelude::serde::Serialize + #crate_name::prelude::serde::de::DeserializeOwned + Send,
                {
                    use #crate_name::prelude::es_entity::AtomicOperation;

                    let now = op.now();

                    let mut payloads = Vec::new();
                    let serialized_events = events
                        .map(|e| {
                            let serialized_event =
                                #crate_name::prelude::serde_json::to_value(&e).expect("Could not serialize payload");
                            payloads.push(e);
                            serialized_event
                        })
                    .collect::<Vec<_>>();

                    #extract_tracing

                    async move {
                        if payloads.is_empty() {
                            return Ok(Vec::new());
                        }
                        let rows = sqlx::query!(
                            #persist_events_query,
                            &serialized_events as _,
                            tracing_json,
                            now
                        ).fetch_all(op.as_executor()).await?;
                        let events = rows
                            .into_iter()
                            .zip(payloads.into_iter())
                            .map(|(row, payload)| #crate_name::out::PersistentOutboxEvent {
                                id: #crate_name::out::OutboxEventId::from(row.id),
                                sequence: #crate_name::EventSequence::from(row.sequence as u64),
                                recorded_at: row.recorded_at,
                                payload: Some(payload),
                                #set_context
                            })
                        .collect::<Vec<_>>();
                        Ok(events)
                    }
                }

                fn persist_ephemeral_event<'a, P>(
                    op: impl #crate_name::prelude::es_entity::IntoOneTimeExecutor<'a>,
                    event_type: #crate_name::out::EphemeralEventType,
                    payload: P,
                ) -> impl std::future::Future<Output = Result<#crate_name::out::EphemeralOutboxEvent<P>, sqlx::Error>> + Send
                where
                    P: #crate_name::prelude::serde::Serialize + #crate_name::prelude::serde::de::DeserializeOwned + Send {
                    let executor = op.into_executor();
                    let now = executor.now();

                    let serialized_payload =
                        #crate_name::prelude::serde_json::to_value(&payload).expect("Could not serialize payload");

                    #extract_tracing
                
                    async move {
                        let row = executor.fetch_one(sqlx::query!(
                            #persist_ephemeral_events_query,
                            event_type.as_str(),
                            serialized_payload,
                            tracing_json,
                            now
                        )).await?;
                
                        Ok(#crate_name::out::EphemeralOutboxEvent {
                            event_type,
                            payload,
                            recorded_at: row.recorded_at,
                            #set_context
                        })
                    }
                }
            }
        });
    }
}
