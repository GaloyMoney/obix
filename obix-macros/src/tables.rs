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
        let (extract_tracing, set_context, deserialize_context) = (
            quote! {
                let tracing_context = es_entity::context::TracingContext::current();
                let tracing_json =
                    serde_json::to_value(&tracing_context).expect("Could not serialize tracing context");
            },
            quote! { tracing_context: tracing_context.clone(), },
            quote! {
                let tracing_context = row.tracing_context
                    .filter(|v| !v.is_null())
                    .map(|p| {
                        #crate_name::prelude::serde_json::from_value(p)
                            .expect("Could not deserialize tracing context")
                    });
            },
        );
        #[cfg(not(feature = "tracing"))]
        let (extract_tracing, set_context, deserialize_context) = (
            quote! {
                let tracing_json = None::<serde_json::Value>;
            },
            quote! { tracing_context: None::<es_entity::context::TracingContext>, },
            quote! { let tracing_context = None::<es_entity::context::TracingContext>; },
        );

        let table_prefix = self
            .prefix
            .as_ref()
            .map(|p| format!("{}_", p.value()))
            .unwrap_or_default();

        // === Outbox queries ===
        let persistent_outbox_events_channel = format!("{}persistent_outbox_events", table_prefix);
        let ephemeral_outbox_events_channel = format!("{}ephemeral_outbox_events", table_prefix);

        let highest_known_query = format!(
            "SELECT CASE WHEN is_called THEN last_value ELSE 0 END AS \"last_returned!: i64\"
FROM {}persistent_outbox_events_sequence_seq",
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

        let load_next_page_query = format!(
            r#"
            WITH max_sequence AS (
                SELECT COALESCE(MAX(sequence), 0) AS max FROM {}persistent_outbox_events
            )
            SELECT
              g.seq AS "sequence!: i64",
              e.id AS "id?",
              e.payload AS "payload?",
              e.tracing_context AS "tracing_context?",
              e.recorded_at AS "recorded_at?"
            FROM
                generate_series(LEAST($1 + 1, (SELECT max FROM max_sequence)),
                  LEAST($1 + $2, (SELECT max FROM max_sequence)))
                AS g(seq)
            LEFT JOIN
                {}persistent_outbox_events e ON g.seq = e.sequence
            WHERE
                g.seq > $1
            ORDER BY
                g.seq ASC
            LIMIT $2"#,
            table_prefix, table_prefix
        );

        let load_ephemeral_events_query_all = format!(
            r#"
            SELECT event_type, payload, tracing_context, recorded_at
            FROM {}ephemeral_outbox_events
            ORDER BY recorded_at"#,
            table_prefix
        );

        let load_ephemeral_events_query_filtered = format!(
            r#"
            SELECT event_type, payload, tracing_context, recorded_at
            FROM {}ephemeral_outbox_events
            WHERE event_type = $1
            ORDER BY recorded_at"#,
            table_prefix
        );

        let fill_gaps_query = format!(
            r#"
            INSERT INTO {}persistent_outbox_events (sequence)
            SELECT unnest($1::bigint[]) AS sequence
            ON CONFLICT (sequence) DO UPDATE
            SET sequence = EXCLUDED.sequence
            RETURNING id, sequence AS "sequence!: i64", payload, tracing_context, recorded_at"#,
            table_prefix
        );

        // === Inbox queries ===
        let insert_inbox_event_query = format!(
            r#"INSERT INTO {tbl}inbox_events (id, idempotency_key, payload, recorded_at)
            VALUES ($1, $2, $3, COALESCE($4::timestamptz, NOW()))
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING id"#,
            tbl = table_prefix
        );

        let find_inbox_event_by_id_query = format!(
            r#"SELECT id, idempotency_key, payload, status::text AS "status!", error, recorded_at, processed_at
            FROM {tbl}inbox_events
            WHERE id = $1"#,
            tbl = table_prefix
        );

        let update_inbox_event_status_query = format!(
            r#"UPDATE {tbl}inbox_events
            SET status = $2,
                error = $3,
                processed_at = CASE WHEN $2 = 'completed'::InboxEventStatus THEN NOW() ELSE processed_at END
            WHERE id = $1"#,
            tbl = table_prefix
        );

        let list_inbox_events_by_status_query = format!(
            r#"SELECT id, idempotency_key, payload, status::text AS "status!", error, recorded_at, processed_at
            FROM {tbl}inbox_events
            WHERE status = $1
            ORDER BY recorded_at ASC
            LIMIT $2"#,
            tbl = table_prefix
        );

        tokens.append_all(quote! {
            impl #crate_name::MailboxTables for #ident {
                // === Outbox channel names ===

                fn persistent_outbox_events_channel() -> &'static str {
                    #persistent_outbox_events_channel
                }

                fn ephemeral_outbox_events_channel() -> &'static str {
                    #ephemeral_outbox_events_channel
                }

                // === Outbox methods ===

                fn highest_known_persistent_sequence<'a>(
                    op: impl #crate_name::prelude::es_entity::IntoOneTimeExecutor<'a>,
                ) -> impl std::future::Future<Output = Result<#crate_name::EventSequence, #crate_name::prelude::sqlx::Error>> + Send {
                    let executor = op.into_executor();
                    async {
                        let row = executor
                            .fetch_one(sqlx::query!(#highest_known_query))
                            .await?;
                        Ok(#crate_name::EventSequence::from(row.last_returned as u64))
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

                    let now = op.maybe_now();

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

                fn persist_ephemeral_event<P>(
                    pool: &#crate_name::prelude::sqlx::PgPool,
                    event_type: #crate_name::out::EphemeralEventType,
                    payload: P,
                ) -> impl std::future::Future<Output = Result<#crate_name::out::EphemeralOutboxEvent<P>, sqlx::Error>> + Send
                where
                    P: #crate_name::prelude::serde::Serialize + #crate_name::prelude::serde::de::DeserializeOwned + Send
                {
                    let serialized_payload =
                        #crate_name::prelude::serde_json::to_value(&payload).expect("Could not serialize payload");

                    #extract_tracing

                    async move {
                        let row = sqlx::query!(
                            #persist_ephemeral_events_query,
                            event_type.as_str(),
                            serialized_payload,
                            tracing_json,
                            None::<chrono::DateTime<chrono::Utc>>
                        ).fetch_one(pool).await?;

                        Ok(#crate_name::out::EphemeralOutboxEvent {
                            event_type,
                            payload,
                            recorded_at: row.recorded_at,
                            #set_context
                        })
                    }
                }

                fn load_next_page<P>(
                    pool: &#crate_name::prelude::sqlx::PgPool,
                    from_sequence: #crate_name::EventSequence,
                    buffer_size: usize,
                ) -> impl std::future::Future<Output = Result<Vec<#crate_name::out::PersistentOutboxEvent<P>>, sqlx::Error>> + Send
                where
                    P: #crate_name::prelude::serde::Serialize + #crate_name::prelude::serde::de::DeserializeOwned + Send
                {
                    let pool = pool.clone();

                    async move {
                        let rows = sqlx::query!(
                            #load_next_page_query,
                            from_sequence as #crate_name::EventSequence,
                            buffer_size as i64,
                        ).fetch_all(&pool).await?;

                        let mut events = Vec::new();
                        let mut empty_ids = Vec::new();

                        for row in rows {
                            if row.id.is_none() {
                                empty_ids.push(row.sequence);
                                continue;
                            }
                            #deserialize_context
                            events.push(#crate_name::out::PersistentOutboxEvent {
                                id: #crate_name::out::OutboxEventId::from(row.id.expect("already checked")),
                                sequence: #crate_name::EventSequence::from(row.sequence as u64),
                                payload: row
                                    .payload
                                    .map(|p| #crate_name::prelude::serde_json::from_value(p).expect("Could not deserialize payload")),
                                recorded_at: row.recorded_at.unwrap_or_default(),
                                #set_context
                            });
                        }

                        if !empty_ids.is_empty() {
                            let gap_rows = sqlx::query!(
                                #fill_gaps_query,
                                &empty_ids as _
                            ).fetch_all(&pool).await?;

                            for row in gap_rows {
                                #deserialize_context
                                events.push(#crate_name::out::PersistentOutboxEvent {
                                    id: #crate_name::out::OutboxEventId::from(row.id),
                                    sequence: #crate_name::EventSequence::from(row.sequence as u64),
                                    payload: row
                                        .payload
                                        .map(|p| #crate_name::prelude::serde_json::from_value(p).expect("Could not deserialize payload")),
                                    recorded_at: row.recorded_at,
                                    #set_context
                                });
                            }
                            events.sort_by(|a, b| a.sequence.cmp(&b.sequence));
                        }

                        Ok(events)
                    }
                }

                fn load_ephemeral_events<P>(
                    pool: &#crate_name::prelude::sqlx::PgPool,
                    event_type_filter: Option<#crate_name::out::EphemeralEventType>,
                ) -> impl std::future::Future<Output = Result<Vec<#crate_name::out::EphemeralOutboxEvent<P>>, sqlx::Error>> + Send
                where
                    P: #crate_name::prelude::serde::Serialize + #crate_name::prelude::serde::de::DeserializeOwned + Send
                {
                    let pool = pool.clone();

                    async move {
                        type RowData = (String, #crate_name::prelude::serde_json::Value, Option<#crate_name::prelude::serde_json::Value>, chrono::DateTime<chrono::Utc>);

                        let rows: Vec<RowData> = if let Some(event_type) = event_type_filter {
                            sqlx::query!(
                                #load_ephemeral_events_query_filtered,
                                event_type.as_str()
                            )
                            .fetch_all(&pool)
                            .await?
                            .into_iter()
                            .map(|row| (row.event_type, row.payload, row.tracing_context, row.recorded_at))
                            .collect()
                        } else {
                            sqlx::query!(
                                #load_ephemeral_events_query_all
                            )
                            .fetch_all(&pool)
                            .await?
                            .into_iter()
                            .map(|row| (row.event_type, row.payload, row.tracing_context, row.recorded_at))
                            .collect()
                        };

                        let events = rows
                            .into_iter()
                            .map(|(event_type_str, payload_json, tracing_context_json, recorded_at)| {
                                let payload = #crate_name::prelude::serde_json::from_value(payload_json)
                                    .expect("Couldn't deserialize payload");
                                let event_type = #crate_name::prelude::serde_json::from_value(
                                    #crate_name::prelude::serde_json::Value::String(event_type_str)
                                ).expect("Couldn't deserialize event_type");

                                let row = {
                                    struct TempRow {
                                        tracing_context: Option<#crate_name::prelude::serde_json::Value>,
                                    }
                                    TempRow {
                                        tracing_context: tracing_context_json,
                                    }
                                };
                                #deserialize_context

                                #crate_name::out::EphemeralOutboxEvent {
                                    event_type,
                                    payload,
                                    #set_context
                                    recorded_at,
                                }
                            })
                            .collect::<Vec<_>>();
                        Ok(events)
                    }
                }

                // === Inbox methods ===

                fn insert_inbox_event<P>(
                    op: &mut impl #crate_name::prelude::es_entity::AtomicOperation,
                    idempotency_key: &#crate_name::inbox::InboxIdempotencyKey,
                    payload: &P,
                ) -> impl std::future::Future<Output = Result<Option<#crate_name::inbox::InboxEventId>, #crate_name::prelude::sqlx::Error>> + Send
                where
                    P: #crate_name::prelude::serde::Serialize + Send + Sync
                {
                    use #crate_name::prelude::es_entity::AtomicOperation;

                    let id = #crate_name::inbox::InboxEventId::new();
                    let serialized_payload =
                        #crate_name::prelude::serde_json::to_value(payload).expect("Could not serialize payload");
                    let idempotency_key = idempotency_key.as_str().to_string();
                    let now = op.maybe_now();

                    async move {
                        let result = sqlx::query!(
                            #insert_inbox_event_query,
                            id as #crate_name::inbox::InboxEventId,
                            idempotency_key,
                            serialized_payload,
                            now
                        )
                        .fetch_optional(op.as_executor())
                        .await?;

                        Ok(result.map(|row| #crate_name::inbox::InboxEventId::from(row.id)))
                    }
                }

                fn find_inbox_event_by_id<P>(
                    pool: &#crate_name::prelude::sqlx::PgPool,
                    id: #crate_name::inbox::InboxEventId,
                ) -> impl std::future::Future<Output = Result<#crate_name::inbox::InboxEvent<P>, #crate_name::inbox::InboxError>> + Send
                where
                    P: #crate_name::prelude::serde::Serialize + #crate_name::prelude::serde::de::DeserializeOwned + Send
                {
                    let pool = pool.clone();

                    async move {
                        let row = sqlx::query!(
                            #find_inbox_event_by_id_query,
                            id as #crate_name::inbox::InboxEventId
                        )
                        .fetch_optional(&pool)
                        .await?
                        .ok_or(#crate_name::inbox::InboxError::NotFound(id))?;

                        let payload = #crate_name::prelude::serde_json::from_value(row.payload)?;
                        let status: #crate_name::inbox::InboxEventStatus = row.status.parse()
                            .expect("Invalid inbox event status in database");

                        Ok(#crate_name::inbox::InboxEvent {
                            id: #crate_name::inbox::InboxEventId::from(row.id),
                            idempotency_key: row.idempotency_key,
                            payload,
                            status,
                            error: row.error,
                            recorded_at: row.recorded_at,
                            processed_at: row.processed_at,
                        })
                    }
                }

                fn list_inbox_events_by_status<P>(
                    pool: &#crate_name::prelude::sqlx::PgPool,
                    status: #crate_name::inbox::InboxEventStatus,
                    limit: usize,
                ) -> impl std::future::Future<Output = Result<Vec<#crate_name::inbox::InboxEvent<P>>, #crate_name::inbox::InboxError>> + Send
                where
                    P: #crate_name::prelude::serde::Serialize + #crate_name::prelude::serde::de::DeserializeOwned + Send
                {
                    let pool = pool.clone();

                    async move {
                        let rows = sqlx::query!(
                            #list_inbox_events_by_status_query,
                            status as #crate_name::inbox::InboxEventStatus,
                            limit as i64
                        )
                        .fetch_all(&pool)
                        .await?;

                        let events = rows
                            .into_iter()
                            .map(|row| {
                                let payload = #crate_name::prelude::serde_json::from_value(row.payload)
                                    .expect("Could not deserialize payload");
                                let status: #crate_name::inbox::InboxEventStatus = row.status.parse()
                                    .expect("Invalid inbox event status in database");

                                #crate_name::inbox::InboxEvent {
                                    id: #crate_name::inbox::InboxEventId::from(row.id),
                                    idempotency_key: row.idempotency_key,
                                    payload,
                                    status,
                                    error: row.error,
                                    recorded_at: row.recorded_at,
                                    processed_at: row.processed_at,
                                }
                            })
                            .collect();

                        Ok(events)
                    }
                }

                fn update_inbox_event_status(
                    pool: &#crate_name::prelude::sqlx::PgPool,
                    id: #crate_name::inbox::InboxEventId,
                    status: #crate_name::inbox::InboxEventStatus,
                    error: Option<&str>,
                ) -> impl std::future::Future<Output = Result<(), #crate_name::prelude::sqlx::Error>> + Send
                {
                    let error = error.map(|s| s.to_string());

                    async move {
                        sqlx::query!(
                            #update_inbox_event_status_query,
                            id as #crate_name::inbox::InboxEventId,
                            status as #crate_name::inbox::InboxEventStatus,
                            error
                        )
                        .execute(pool)
                        .await?;
                        Ok(())
                    }
                }

                fn update_inbox_event_status_in_op(
                    op: &mut impl #crate_name::prelude::es_entity::AtomicOperation,
                    id: #crate_name::inbox::InboxEventId,
                    status: #crate_name::inbox::InboxEventStatus,
                    error: Option<&str>,
                ) -> impl std::future::Future<Output = Result<(), #crate_name::prelude::sqlx::Error>> + Send
                {
                    use #crate_name::prelude::es_entity::AtomicOperation;

                    let error = error.map(|s| s.to_string());

                    async move {
                        sqlx::query!(
                            #update_inbox_event_status_query,
                            id as #crate_name::inbox::InboxEventId,
                            status as #crate_name::inbox::InboxEventStatus,
                            error
                        )
                        .execute(op.as_executor())
                        .await?;
                        Ok(())
                    }
                }
            }
        });
    }
}
