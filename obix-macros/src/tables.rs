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
    tables.validate_prefix()?;
    Ok(quote!(#tables))
}

impl MailboxTables {
    /// The prefix is interpolated verbatim into SQL identifiers (table,
    /// sequence and channel names) and into the `pg_notify('<channel>', ...)`
    /// string literal of the generated persist query. Restricting it to a
    /// plain identifier charset makes SQL injection through the derive
    /// attribute unrepresentable; the length bound keeps the longest derived
    /// name — `{prefix}_persistent_outbox_events_sequence_seq`, i.e.
    /// prefix + 38 bytes — inside PostgreSQL's 63-byte identifier limit.
    fn validate_prefix(&self) -> darling::Result<()> {
        let Some(prefix) = &self.prefix else {
            return Ok(());
        };
        let value = prefix.value();
        let valid = !value.is_empty()
            && value.len() <= 25
            && value
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            && value.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
        if valid {
            Ok(())
        } else {
            Err(darling::Error::custom(format!(
                "invalid obix tbl_prefix `{value}`: must be 1-25 characters, start with an \
                 ASCII letter or underscore, and contain only ASCII letters, digits and \
                 underscores (it is interpolated into generated SQL identifiers)"
            ))
            .with_span(prefix))
        }
    }
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
                    .and_then(|p| {
                        match #crate_name::prelude::serde_json::from_value(p) {
                            Ok(context) => Some(context),
                            Err(error) => {
                                #crate_name::record_tracing_context_undecodable(&error);
                                None
                            }
                        }
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

        // Base table name for the partition maintainer. Same string the
        // channel builds, but exposed under its own method so the maintainer
        // reads "the table" rather than "the notify channel"; it derives child
        // partition names (`{table}_p{k}`) and the sequence-object name
        // (`{table}_sequence_seq`) from it.
        let persistent_outbox_events_table = format!("{}persistent_outbox_events", table_prefix);

        let highest_known_query = format!(
            "SELECT CASE WHEN is_called THEN last_value ELSE 0 END AS \"last_returned!: i64\"
FROM {}persistent_outbox_events_sequence_seq",
            table_prefix
        );

        // Notifications ride the insert statement itself: one
        // {min_sequence, max_sequence} pg_notify per statement, aggregated
        // from the rows the CTE already materializes — no per-row trigger,
        // no row_to_json of payloads, no transition table. Listeners fetch
        // the notified range back with a SELECT-only scan
        // (load_events_in_range).
        //
        // INVARIANT: `notified` must remain referenced by the outer query
        // (the LEFT JOIN below). Postgres never executes an unreferenced
        // SELECT CTE — notifications would silently stop and cross-process
        // delivery would degrade to the grace-period gap-fill path
        // (events_via_pg_notify hangs if this regresses). HAVING COUNT(*) > 0
        // suppresses the aggregate's empty-input row so an empty insert
        // notifies nothing.
        let persist_events_query = format!(
            r#"WITH new_events AS (
                   INSERT INTO {tbl}persistent_outbox_events (payload, tracing_context, recorded_at)
                   SELECT unnest($1::jsonb[]) AS payload, $2::jsonb AS tracing_context, COALESCE($3::timestamptz, NOW()) AS recorded_at
                   RETURNING id, sequence, recorded_at
               ),
               notified AS (
                   SELECT pg_notify(
                       '{channel}',
                       json_build_object('min_sequence', MIN(sequence), 'max_sequence', MAX(sequence))::TEXT
                   )
                   FROM new_events
                   HAVING COUNT(*) > 0
               )
               SELECT ne.id AS "id!", ne.sequence AS "sequence!", ne.recorded_at AS "recorded_at!"
               FROM new_events ne
               LEFT JOIN notified ON TRUE
               ORDER BY ne.sequence"#,
            tbl = table_prefix,
            channel = persistent_outbox_events_channel,
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

        // Bounded range scan over the `sequence` index: O(page) instead of the
        // previous generate_series + LEFT JOIN, which planned as a hash join
        // over a full seq scan of the (append-only, unpruned) events table on
        // every poll. The single-row `m` side always yields MAX(sequence) so
        // the caller can compute the gap range even when the page is empty;
        // sequence gaps within the page are detected caller-side and filled
        // via fill_gaps_query, preserving the old placeholder semantics.
        let load_next_page_query = format!(
            r#"
            SELECT
              m.max_sequence AS "max_sequence!: i64",
              e.sequence AS "sequence?: i64",
              e.id AS "id?",
              e.payload AS "payload?",
              e.tracing_context AS "tracing_context?",
              e.recorded_at AS "recorded_at?"
            FROM (
                SELECT COALESCE(MAX(sequence), 0) AS max_sequence
                FROM {}persistent_outbox_events
            ) m
            LEFT JOIN LATERAL (
                SELECT sequence, id, payload, tracing_context, recorded_at
                FROM {}persistent_outbox_events
                WHERE sequence > $1
                  AND sequence <= $1 + $2
                ORDER BY sequence ASC
                LIMIT $2
            ) e ON true
            ORDER BY e.sequence ASC"#,
            table_prefix, table_prefix
        );

        let load_events_in_range_query = format!(
            r#"
            SELECT id, sequence, payload, tracing_context, recorded_at
            FROM {}persistent_outbox_events
            WHERE sequence > $1
              AND sequence <= $2
            ORDER BY sequence ASC"#,
            table_prefix
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
                processed_at = CASE WHEN $2 = 'completed'::InboxEventStatus THEN COALESCE($4::timestamptz, NOW()) ELSE processed_at END
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

                fn persistent_outbox_events_table() -> &'static str {
                    #persistent_outbox_events_table
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
                    now: Option<chrono::DateTime<chrono::Utc>>,
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
                            now
                        ).fetch_one(pool).await?;

                        Ok(#crate_name::out::EphemeralOutboxEvent {
                            event_type,
                            payload,
                            recorded_at: row.recorded_at,
                            #set_context
                        })
                    }
                }

                fn persist_ephemeral_event_in_op<'a, P>(
                    op: &mut #crate_name::prelude::es_entity::hooks::HookOperation<'a>,
                    event_type: #crate_name::out::EphemeralEventType,
                    payload: P,
                ) -> impl std::future::Future<Output = Result<#crate_name::out::EphemeralOutboxEvent<P>, sqlx::Error>> + Send
                where
                    P: #crate_name::prelude::serde::Serialize + #crate_name::prelude::serde::de::DeserializeOwned + Send
                {
                    use #crate_name::prelude::es_entity::AtomicOperation;

                    let now = op.maybe_now();
                    let serialized_payload =
                        #crate_name::prelude::serde_json::to_value(&payload).expect("Could not serialize payload");

                    #extract_tracing

                    async move {
                        let row = sqlx::query!(
                            #persist_ephemeral_events_query,
                            event_type.as_str(),
                            serialized_payload,
                            tracing_json,
                            now
                        ).fetch_one(op.as_executor()).await?;

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
                ) -> impl std::future::Future<Output = Result<Vec<Result<#crate_name::out::PersistentOutboxEvent<P>, #crate_name::out::UndecodableEventError>>, sqlx::Error>> + Send
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

                        let max_sequence = rows
                            .first()
                            .map(|r| r.max_sequence)
                            .unwrap_or_else(|| u64::from(from_sequence) as i64);

                        let mut events = Vec::new();
                        let mut present = std::collections::HashSet::new();

                        for row in rows {
                            let Some(sequence) = row.sequence else {
                                continue;
                            };
                            present.insert(sequence);
                            #deserialize_context
                            events.push(#crate_name::decode_persistent_event(
                                #crate_name::out::OutboxEventId::from(row.id.expect("matched row has id")),
                                sequence as u64,
                                row.recorded_at.unwrap_or_default(),
                                tracing_context,
                                row.payload,
                            ));
                        }

                        // Fill sequence gaps in the page with placeholder rows,
                        // preserving contiguity for consumers (same semantics as
                        // the old generate_series + LEFT JOIN page).
                        let from = u64::from(from_sequence) as i64;
                        let end = std::cmp::min(from + buffer_size as i64, max_sequence);
                        let empty_ids: Vec<i64> = ((from + 1)..=end)
                            .filter(|s| !present.contains(s))
                            .collect();

                        if !empty_ids.is_empty() {
                            let gap_rows = sqlx::query!(
                                #fill_gaps_query,
                                &empty_ids as _
                            ).fetch_all(&pool).await?;

                            for row in gap_rows {
                                #deserialize_context
                                events.push(#crate_name::decode_persistent_event(
                                    #crate_name::out::OutboxEventId::from(row.id),
                                    row.sequence as u64,
                                    row.recorded_at,
                                    tracing_context,
                                    row.payload,
                                ));
                            }
                            // Gap-fill rows were appended after the page rows, so
                            // re-establish the ascending order consumers rely on.
                            events.sort_by_key(|item| match item {
                                Ok(event) => event.sequence,
                                Err(error) => error.sequence,
                            });
                        }

                        Ok(events)
                    }
                }

                fn load_events_in_range<P>(
                    pool: &#crate_name::prelude::sqlx::PgPool,
                    after_sequence: #crate_name::EventSequence,
                    up_to_sequence: #crate_name::EventSequence,
                ) -> impl std::future::Future<Output = Result<Vec<Result<#crate_name::out::PersistentOutboxEvent<P>, #crate_name::out::UndecodableEventError>>, sqlx::Error>> + Send
                where
                    P: #crate_name::prelude::serde::Serialize + #crate_name::prelude::serde::de::DeserializeOwned + Send
                {
                    let pool = pool.clone();

                    async move {
                        let rows = sqlx::query!(
                            #load_events_in_range_query,
                            after_sequence as #crate_name::EventSequence,
                            up_to_sequence as #crate_name::EventSequence,
                        ).fetch_all(&pool).await?;

                        let events = rows
                            .into_iter()
                            .map(|row| {
                                #deserialize_context
                                #crate_name::decode_persistent_event(
                                    #crate_name::out::OutboxEventId::from(row.id),
                                    row.sequence as u64,
                                    row.recorded_at,
                                    tracing_context,
                                    row.payload,
                                )
                            })
                            .collect();
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
                            .filter_map(|(event_type_str, payload_json, tracing_context_json, recorded_at)| {
                                let payload = match #crate_name::prelude::serde_json::from_value(payload_json) {
                                    Ok(payload) => payload,
                                    Err(error) => {
                                        #crate_name::record_ephemeral_payload_undecodable(&error, &event_type_str);
                                        return None;
                                    }
                                };
                                let event_type = match #crate_name::prelude::serde_json::from_value(
                                    #crate_name::prelude::serde_json::Value::String(event_type_str.clone())
                                ) {
                                    Ok(event_type) => event_type,
                                    Err(error) => {
                                        #crate_name::record_ephemeral_event_type_undecodable(&error, &event_type_str);
                                        return None;
                                    }
                                };

                                let row = {
                                    struct TempRow {
                                        tracing_context: Option<#crate_name::prelude::serde_json::Value>,
                                    }
                                    TempRow {
                                        tracing_context: tracing_context_json,
                                    }
                                };
                                #deserialize_context

                                Some(#crate_name::out::EphemeralOutboxEvent {
                                    event_type,
                                    payload,
                                    #set_context
                                    recorded_at,
                                })
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

                fn find_inbox_event_by_id(
                    pool: &#crate_name::prelude::sqlx::PgPool,
                    id: #crate_name::inbox::InboxEventId,
                ) -> impl std::future::Future<Output = Result<#crate_name::inbox::InboxEvent, #crate_name::inbox::InboxError>> + Send
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

                        let status: #crate_name::inbox::InboxEventStatus = row.status.parse()
                            .map_err(#crate_name::inbox::InboxError::InvalidStatus)?;

                        Ok(#crate_name::inbox::InboxEvent {
                            id: #crate_name::inbox::InboxEventId::from(row.id),
                            idempotency_key: row.idempotency_key,
                            payload: row.payload,
                            status,
                            error: row.error,
                            recorded_at: row.recorded_at,
                            processed_at: row.processed_at,
                        })
                    }
                }

                fn list_inbox_events_by_status(
                    pool: &#crate_name::prelude::sqlx::PgPool,
                    status: #crate_name::inbox::InboxEventStatus,
                    limit: usize,
                ) -> impl std::future::Future<Output = Result<Vec<#crate_name::inbox::InboxEvent>, #crate_name::inbox::InboxError>> + Send
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
                                let status: #crate_name::inbox::InboxEventStatus = row.status.parse()
                                    .map_err(#crate_name::inbox::InboxError::InvalidStatus)?;

                                Ok(#crate_name::inbox::InboxEvent {
                                    id: #crate_name::inbox::InboxEventId::from(row.id),
                                    idempotency_key: row.idempotency_key,
                                    payload: row.payload,
                                    status,
                                    error: row.error,
                                    recorded_at: row.recorded_at,
                                    processed_at: row.processed_at,
                                })
                            })
                            .collect::<Result<Vec<_>, #crate_name::inbox::InboxError>>()?;

                        Ok(events)
                    }
                }

                fn update_inbox_event_status(
                    pool: &#crate_name::prelude::sqlx::PgPool,
                    now: Option<chrono::DateTime<chrono::Utc>>,
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
                            error,
                            now
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
                    let now = op.maybe_now();

                    async move {
                        sqlx::query!(
                            #update_inbox_event_status_query,
                            id as #crate_name::inbox::InboxEventId,
                            status as #crate_name::inbox::InboxEventStatus,
                            error,
                            now
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
