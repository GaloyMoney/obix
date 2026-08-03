-- Persistent outbox events — RANGE-partitioned by `sequence`.
--
-- Stage 1 of the partitioning plan: partition-by-sequence only, nothing
-- dropped, so the gapless-sequence + replay-from-zero contract is fully
-- intact. Total on-disk size is unchanged; the wins are per-operation cost,
-- vacuum trajectory and index locality.
--
--   * Partition key is `sequence` specifically. `fill_gaps_query` and the
--     persist path arbitrate on `ON CONFLICT (sequence)`, and a partitioned
--     parent's PK/unique constraint MUST include the partition key. Making
--     `sequence` the PRIMARY KEY turns the per-partition unique index into
--     the global `ON CONFLICT` arbiter for free, and simultaneously removes
--     the random-UUID PK index (the ~8x WAL-amplification win).
--   * `id` is demoted to a plain, unindexed column: it is only ever
--     RETURNING-ed, never a query predicate, but is retained for the public
--     `OutboxEventId` on `PersistentOutboxEvent`.
--   * `BIGSERIAL` preserves the sequence object name
--     `persistent_outbox_events_sequence_seq`, which `highest_known_query`
--     reads by name — do NOT rename it. The single shared sequence is what
--     guarantees global monotonic ordering across partitions.
--   * The DEFAULT partition is a structural backstop so an INSERT can never
--     fail to route (SQLSTATE 23514) into a caller's business commit. The
--     partition-maintainer job (see `src/out/partition`) keeps an explicit
--     partition covering [head, head + margin] so DEFAULT stays empty.
CREATE TABLE persistent_outbox_events (
  id UUID NOT NULL DEFAULT gen_random_uuid(),
  sequence BIGSERIAL,
  payload JSONB COMPRESSION lz4,
  tracing_context JSONB COMPRESSION lz4,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (sequence)
) PARTITION BY RANGE (sequence);

-- Initial explicit partition, deliberately wide so ordinary test volumes
-- never need the maintainer running. Per-partition storage params are set on
-- the CREATE (they are NOT inherited from the parent on PARTITION OF):
-- insert-driven autovacuum at a fixed threshold keeps each partition vacuumed
-- at a steady cadence, and autovacuum_freeze_min_age = 0 freezes on the first
-- insert-driven vacuum, defusing anti-wraparound on an append-only table.
-- (The per-table reloption is `autovacuum_freeze_min_age`; the bare
-- `vacuum_freeze_min_age` is a GUC, not a storage parameter, and Postgres
-- rejects it in WITH (...).)
CREATE TABLE persistent_outbox_events_p0 PARTITION OF persistent_outbox_events
  FOR VALUES FROM (0) TO (10000000)
  WITH (autovacuum_vacuum_insert_scale_factor = 0.0,
        autovacuum_vacuum_insert_threshold = 50000,
        autovacuum_freeze_min_age = 0,
        fillfactor = 100);

-- Always-empty backstop: guarantees INSERT routing never fails even if the
-- maintainer falls behind. Rows landing here are still read normally; a
-- non-empty DEFAULT is a layout repair (see `recover_default_partition`),
-- not a correctness failure.
CREATE TABLE persistent_outbox_events_default
  PARTITION OF persistent_outbox_events DEFAULT;

-- Ephemeral outbox events
CREATE TABLE ephemeral_outbox_events (
  event_type VARCHAR NOT NULL UNIQUE,
  payload JSONB NOT NULL,
  tracing_context JSONB,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE FUNCTION notify_ephemeral_outbox_events() RETURNS TRIGGER AS $$
DECLARE
  payload TEXT;
  payload_size INTEGER;
BEGIN
  payload := row_to_json(NEW);
  payload_size := octet_length(payload);
  IF payload_size > 8000 THEN
    payload := json_build_object(
      'event_type', NEW.event_type,
      'payload', NULL,
      'payload_omitted', true,
      'tracing_context', NEW.tracing_context,
      'recorded_at', NEW.recorded_at
    )::TEXT;
  END IF;
  PERFORM pg_notify('ephemeral_outbox_events', payload);
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ephemeral_outbox_events_notify
  AFTER INSERT OR UPDATE ON ephemeral_outbox_events
  FOR EACH ROW EXECUTE FUNCTION notify_ephemeral_outbox_events();

-- Inbox events
DO $$ BEGIN
    CREATE TYPE InboxEventStatus AS ENUM ('pending', 'processing', 'completed', 'failed');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

CREATE TABLE inbox_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  idempotency_key VARCHAR UNIQUE,
  payload JSONB NOT NULL,
  status InboxEventStatus NOT NULL DEFAULT 'pending',
  error VARCHAR,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  processed_at TIMESTAMPTZ
);

CREATE INDEX idx_inbox_events_status ON inbox_events(status)
  WHERE status IN ('pending', 'processing', 'failed');
