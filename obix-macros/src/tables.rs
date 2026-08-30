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

        // No pg_notify here: notify-bearing commits serialize on a
        // cluster-wide lock, so cross-process wake-up moved to the
        // per-process debounced notifier (`src/out/notifier.rs`).
        let persist_events_query = format!(
            r#"WITH new_events AS (
                   INSERT INTO {tbl}persistent_outbox_events (payload, tracing_context, recorded_at)
                   SELECT unnest($1::jsonb[]) AS payload, $2::jsonb AS tracing_context, COALESCE($3::timestamptz, NOW()) AS recorded_at
                   RETURNING id, sequence, recorded_at
               )
               SELECT ne.id AS "id!", ne.sequence AS "sequence!", ne.recorded_at AS "recorded_at!"
               FROM new_events ne
               ORDER BY ne.sequence"#,
            tbl = table_prefix,
        );

        // Kept only for publishes onto operations without commit-hook
        // support (bare `sqlx::Transaction`): post_commit never runs there,
        // so the insert statement itself must carry the {min, max} hint.
        //
        // INVARIANT: `notified` must remain referenced by the outer query
        // (the LEFT JOIN below) — Postgres never executes an unreferenced
        // SELECT CTE (events_via_pg_notify hangs if this regresses).
        // HAVING COUNT(*) > 0 keeps an empty insert from notifying.
        let persist_events_notifying_query = format!(
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

        // Bounded range scan over the `sequence` index: O(page), SELECT-only.
        // Deliberately NO MAX(sequence) anchor and NO placeholder writes: the
        // previous anchor was a Merge Append over every partition's index with
        // visibility checks on the never-all-visible tail (hundreds of ms on a
        // large table, on every page read), and it only existed to serve the
        // in-query gap fill that has moved to `fill_gaps_query` — age-gated
        // and batch-capped caller-side (`out::persistent::cache`).
        let load_next_page_query = format!(
            r#"
            SELECT sequence AS "sequence!: i64", id AS "id!", payload, tracing_context, recorded_at AS "recorded_at!"
            FROM {}persistent_outbox_events
            WHERE sequence > $1
              AND sequence <= $1 + $2
            ORDER BY sequence ASC
            LIMIT $2"#,
            table_prefix
        );

        // The contiguous run above `$1`, cut at the first gap. `win` finds the
        // cut index-only: in a contiguous run `sequence = $1 + rn` exactly, so
        // the first row failing that is the first gap.
        //
        // The cut MUST stay a scalar subquery. As an InitPlan it is evaluated
        // once and usable as an index bound, so the payload scan stops at the
        // gap; written as a joined CTE (`FROM t e, stop WHERE ...`) the planner
        // demotes it to a Join Filter and walks the whole tail of the table —
        // 5,327 buffers / 24.6 ms against 43 / 0.3 ms here. The redundant
        // `sequence <= $1 + $2` bounds the scan and prunes partitions
        // independently of the InitPlan. Re-check the plan (including the
        // generic one) if you touch this.
        let load_next_contiguous_page_query = format!(
            r#"
            WITH win AS (
                SELECT sequence, ROW_NUMBER() OVER (ORDER BY sequence) AS rn
                FROM {tbl}persistent_outbox_events
                WHERE sequence > $1
                  AND sequence <= $1 + $2
            )
            SELECT e.sequence AS "sequence!: i64", e.id AS "id!", e.payload,
                   e.tracing_context, e.recorded_at AS "recorded_at!"
            FROM {tbl}persistent_outbox_events e
            WHERE e.sequence > $1
              AND e.sequence <= $1 + $2
              AND e.sequence < (
                  SELECT COALESCE(MIN(sequence), $1 + $2 + 1)
                  FROM win
                  WHERE sequence <> $1 + rn
              )
            ORDER BY e.sequence ASC"#,
            tbl = table_prefix,
        );

        // Single index probe for a parked reader: has the sequence it is
        // blocked on landed yet?
        let sequence_present_query = format!(
            r#"
            SELECT EXISTS (
                SELECT 1 FROM {tbl}persistent_outbox_events WHERE sequence = $1
            ) AS "present!""#,
            tbl = table_prefix,
        );

        // The holes in `(after, up_to]`, without fetching payloads.
        //
        // Must stay `EXCEPT`, not `WHERE NOT EXISTS (...)`: the anti-join form
        // plans as a Nested Loop Anti Join paying an index probe per generated
        // sequence — 4,501 buffers on a 1,500-wide range against 8 here.
        let missing_sequences_query = format!(
            r#"
            SELECT g AS "sequence!: i64"
            FROM generate_series($1::bigint + 1, $2::bigint) g
            EXCEPT
            SELECT sequence FROM {tbl}persistent_outbox_events
            WHERE sequence > $1 AND sequence <= $2
            ORDER BY 1"#,
            tbl = table_prefix,
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

        // DO NOTHING, not DO UPDATE: a conflict means the sequence already
        // has a committed row (real event or earlier placeholder) and must
        // not be rewritten — the old upsert-to-return-rows trick generated a
        // dead tuple per already-committed sequence per fill. RETURNING
        // therefore yields only the placeholders actually inserted; rows
        // that committed concurrently reach consumers through the normal
        // post-commit broadcast/notification path or the next page read.
        let fill_gaps_query = format!(
            r#"
            INSERT INTO {}persistent_outbox_events (sequence)
            SELECT unnest($1::bigint[]) AS sequence
            ON CONFLICT (sequence) DO NOTHING
            RETURNING id, sequence AS "sequence!: i64", payload, tracing_context, recorded_at"#,
            table_prefix
        );

        // One statement, one round trip, auto-commit: assigns this
        // connection a real xid (the abandonment marker — every write
        // transaction that had begun before this statement holds a smaller
        // xid) and reads the sequence's allocation head alongside it. The
        // deliberate xid burn is negligible: markers are taken once per
        // gap-fill episode, and episodes only exist while a stall persists.
        let abandonment_marker_query = format!(
            r#"
            SELECT pg_current_xact_id()::text AS "marker!",
                   (SELECT CASE WHEN is_called THEN last_value ELSE 0 END
                    FROM {}persistent_outbox_events_sequence_seq) AS "head!: i64""#,
            table_prefix
        );

        // The xmin horizon has passed the marker once every transaction
        // with an older xid has ended — at that point a sequence known to
        // be allocated before the marker, and still absent from the table,
        // is provably abandoned. (Deliberately NOT the snapshot-xmax
        // variant: snapshot xmax is one past the highest *completed* xid,
        // and a transaction active at marker time can hold an xid at or
        // above it — that check can pass while the gap's writer still
        // runs.)
        let abandonment_proof_query =
            r#"SELECT pg_snapshot_xmin(pg_current_snapshot()) > $1::text::xid8 AS "passed!""#
                .to_string();

        // Cluster-wide dedup of backstop fills: losers of the try-lock skip
        // entirely (the winner's rows are committed by the time the lock
        // releases, so a later page read delivers them). The key is derived
        // from the (prefixed) table name so co-hosted outboxes never
        // contend with each other.
        let fill_gaps_lock_query = format!(
            r#"SELECT pg_try_advisory_xact_lock(hashtextextended('{}persistent_outbox_events_gap_fill', 0)) AS "locked!""#,
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

        // === Keyed-subscriber subscription queries ===

        // DO NOTHING, not DO UPDATE: re-subscribing an already-live key must
        // resolve to the ORIGINAL row (identity + birth frontier), never
        // rewrite it with a freshly-sampled `start_after` — the whole
        // from-birth-delivery guarantee rests on `start_after` being sampled
        // exactly once, at the subscription's true birth.
        let insert_subscription_query = format!(
            r#"
            INSERT INTO {tbl}subscriptions (subscriber_type, key, wake_keys, instance_config, start_after, created_at)
            VALUES ($1, $2, $3, $4, $5, COALESCE($6::timestamptz, NOW()))
            ON CONFLICT (subscriber_type, key) DO NOTHING"#,
            tbl = table_prefix
        );

        // Row absence IS the tombstone: no job-kill API exists or is needed.
        let delete_subscription_query = format!(
            r#"DELETE FROM {tbl}subscriptions WHERE subscriber_type = $1 AND key = $2"#,
            tbl = table_prefix
        );

        let find_subscription_query = format!(
            r#"
            SELECT wake_keys, instance_config, start_after AS "start_after!: i64", created_at
            FROM {tbl}subscriptions
            WHERE subscriber_type = $1 AND key = $2"#,
            tbl = table_prefix
        );

        let list_subscription_keys_query = format!(
            r#"SELECT key FROM {tbl}subscriptions WHERE subscriber_type = $1 ORDER BY key"#,
            tbl = table_prefix
        );

        // The waker's flush-time lookup: liveness-only, so an
        // over-approximating false positive here is a harmless empty wake,
        // never a correctness gap.
        // The `$2::varchar[]` cast is required, not decorative: Postgres has
        // no `varchar[] && text[]` operator (unlike scalar varchar/text,
        // array element types do not implicitly cast for `&&`), and sqlx's
        // compile-time check does not catch it — DESCRIBE happily infers
        // `text[]` for an untyped array parameter, so an uncast `$2` compiles
        // clean and fails at EXECUTE time on every call, on live data only.
        let subscription_keys_for_wake_keys_query = format!(
            r#"SELECT key FROM {tbl}subscriptions WHERE subscriber_type = $1 AND wake_keys && $2::varchar[]"#,
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

                fn persist_events_notifying<'a, P>(
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
                            #persist_events_notifying_query,
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

                fn load_next_contiguous_page<P>(
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
                            #load_next_contiguous_page_query,
                            from_sequence as #crate_name::EventSequence,
                            buffer_size as i64,
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

                fn sequence_present(
                    pool: &#crate_name::prelude::sqlx::PgPool,
                    sequence: #crate_name::EventSequence,
                ) -> impl std::future::Future<Output = Result<bool, #crate_name::prelude::sqlx::Error>> + Send
                {
                    let pool = pool.clone();

                    async move {
                        let row = sqlx::query!(
                            #sequence_present_query,
                            sequence as #crate_name::EventSequence,
                        ).fetch_one(&pool).await?;
                        Ok(row.present)
                    }
                }

                fn missing_sequences(
                    pool: &#crate_name::prelude::sqlx::PgPool,
                    after_sequence: #crate_name::EventSequence,
                    up_to_sequence: #crate_name::EventSequence,
                ) -> impl std::future::Future<Output = Result<Vec<#crate_name::EventSequence>, #crate_name::prelude::sqlx::Error>> + Send
                {
                    let pool = pool.clone();

                    async move {
                        let rows = sqlx::query!(
                            #missing_sequences_query,
                            after_sequence as #crate_name::EventSequence,
                            up_to_sequence as #crate_name::EventSequence,
                        ).fetch_all(&pool).await?;

                        Ok(rows
                            .into_iter()
                            .map(|row| #crate_name::EventSequence::from(row.sequence as u64))
                            .collect())
                    }
                }

                fn fill_gaps<P>(
                    pool: &#crate_name::prelude::sqlx::PgPool,
                    sequences: Vec<#crate_name::EventSequence>,
                ) -> impl std::future::Future<Output = Result<Vec<Result<#crate_name::out::PersistentOutboxEvent<P>, #crate_name::out::UndecodableEventError>>, sqlx::Error>> + Send
                where
                    P: #crate_name::prelude::serde::Serialize + #crate_name::prelude::serde::de::DeserializeOwned + Send
                {
                    let pool = pool.clone();

                    async move {
                        if sequences.is_empty() {
                            return Ok(Vec::new());
                        }
                        let sequences = sequences
                            .into_iter()
                            .map(|s| u64::from(s) as i64)
                            .collect::<Vec<_>>();
                        let rows = sqlx::query!(
                            #fill_gaps_query,
                            &sequences as _
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

                fn fill_gaps_deduped<P>(
                    pool: &#crate_name::prelude::sqlx::PgPool,
                    sequences: Vec<#crate_name::EventSequence>,
                ) -> impl std::future::Future<Output = Result<Option<Vec<Result<#crate_name::out::PersistentOutboxEvent<P>, #crate_name::out::UndecodableEventError>>>, sqlx::Error>> + Send
                where
                    P: #crate_name::prelude::serde::Serialize + #crate_name::prelude::serde::de::DeserializeOwned + Send
                {
                    let pool = pool.clone();

                    async move {
                        if sequences.is_empty() {
                            return Ok(Some(Vec::new()));
                        }
                        let sequences = sequences
                            .into_iter()
                            .map(|s| u64::from(s) as i64)
                            .collect::<Vec<_>>();

                        let mut tx = pool.begin().await?;
                        let locked = sqlx::query!(#fill_gaps_lock_query)
                            .fetch_one(&mut *tx)
                            .await?
                            .locked;
                        if !locked {
                            tx.rollback().await?;
                            return Ok(None);
                        }
                        let rows = sqlx::query!(
                            #fill_gaps_query,
                            &sequences as _
                        ).fetch_all(&mut *tx).await?;
                        tx.commit().await?;

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
                        Ok(Some(events))
                    }
                }

                fn abandonment_marker(
                    pool: &#crate_name::prelude::sqlx::PgPool,
                ) -> impl std::future::Future<Output = Result<(String, #crate_name::EventSequence), #crate_name::prelude::sqlx::Error>> + Send
                {
                    let pool = pool.clone();

                    async move {
                        let row = sqlx::query!(#abandonment_marker_query)
                            .fetch_one(&pool)
                            .await?;
                        Ok((row.marker, #crate_name::EventSequence::from(row.head as u64)))
                    }
                }

                fn abandonment_proof_passed(
                    pool: &#crate_name::prelude::sqlx::PgPool,
                    marker: &str,
                ) -> impl std::future::Future<Output = Result<bool, #crate_name::prelude::sqlx::Error>> + Send
                {
                    let pool = pool.clone();
                    let marker = marker.to_string();

                    async move {
                        let row = sqlx::query!(#abandonment_proof_query, marker)
                            .fetch_one(&pool)
                            .await?;
                        Ok(row.passed)
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

                // === Keyed-subscriber subscription methods ===

                fn insert_subscription_in_op(
                    op: &mut impl #crate_name::prelude::es_entity::AtomicOperation,
                    subscriber_type: &str,
                    key: &str,
                    wake_keys: &[String],
                    instance_config: #crate_name::prelude::serde_json::Value,
                    start_after: #crate_name::EventSequence,
                ) -> impl std::future::Future<Output = Result<(), #crate_name::prelude::sqlx::Error>> + Send {
                    use #crate_name::prelude::es_entity::AtomicOperation;

                    let subscriber_type = subscriber_type.to_string();
                    let key = key.to_string();
                    let wake_keys = wake_keys.to_vec();
                    let now = op.maybe_now();

                    async move {
                        sqlx::query!(
                            #insert_subscription_query,
                            subscriber_type,
                            key,
                            &wake_keys as _,
                            instance_config,
                            start_after as #crate_name::EventSequence,
                            now
                        )
                        .execute(op.as_executor())
                        .await?;
                        Ok(())
                    }
                }

                fn delete_subscription_in_op(
                    op: &mut impl #crate_name::prelude::es_entity::AtomicOperation,
                    subscriber_type: &str,
                    key: &str,
                ) -> impl std::future::Future<Output = Result<(), #crate_name::prelude::sqlx::Error>> + Send {
                    use #crate_name::prelude::es_entity::AtomicOperation;

                    let subscriber_type = subscriber_type.to_string();
                    let key = key.to_string();

                    async move {
                        sqlx::query!(#delete_subscription_query, subscriber_type, key)
                            .execute(op.as_executor())
                            .await?;
                        Ok(())
                    }
                }

                fn find_subscription(
                    pool: &#crate_name::prelude::sqlx::PgPool,
                    subscriber_type: &str,
                    key: &str,
                ) -> impl std::future::Future<Output = Result<Option<#crate_name::SubscriptionRow>, #crate_name::prelude::sqlx::Error>> + Send {
                    let pool = pool.clone();
                    let subscriber_type = subscriber_type.to_string();
                    let key = key.to_string();

                    async move {
                        let row = sqlx::query!(#find_subscription_query, subscriber_type, key)
                            .fetch_optional(&pool)
                            .await?;

                        Ok(row.map(|row| #crate_name::SubscriptionRow {
                            wake_keys: row.wake_keys,
                            instance_config: row.instance_config,
                            start_after: #crate_name::EventSequence::from(row.start_after as u64),
                            created_at: row.created_at,
                        }))
                    }
                }

                fn list_subscription_keys(
                    pool: &#crate_name::prelude::sqlx::PgPool,
                    subscriber_type: &str,
                ) -> impl std::future::Future<Output = Result<Vec<String>, #crate_name::prelude::sqlx::Error>> + Send {
                    let pool = pool.clone();
                    let subscriber_type = subscriber_type.to_string();

                    async move {
                        let rows = sqlx::query!(#list_subscription_keys_query, subscriber_type)
                            .fetch_all(&pool)
                            .await?;
                        Ok(rows.into_iter().map(|row| row.key).collect())
                    }
                }

                fn subscription_keys_for_wake_keys(
                    op: &mut impl #crate_name::prelude::es_entity::AtomicOperation,
                    subscriber_type: &str,
                    wake_keys: &[String],
                ) -> impl std::future::Future<Output = Result<Vec<String>, #crate_name::prelude::sqlx::Error>> + Send {
                    use #crate_name::prelude::es_entity::AtomicOperation;

                    let subscriber_type = subscriber_type.to_string();
                    let wake_keys = wake_keys.to_vec();

                    async move {
                        if wake_keys.is_empty() {
                            return Ok(Vec::new());
                        }
                        let rows = sqlx::query!(
                            #subscription_keys_for_wake_keys_query,
                            subscriber_type,
                            &wake_keys as _,
                        )
                        .fetch_all(op.as_executor())
                        .await?;
                        Ok(rows.into_iter().map(|row| row.key).collect())
                    }
                }
            }
        });
    }
}
