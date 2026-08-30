-- Persistent outbox events: a RANGE-partitioned table.
--
-- Partitioned by `sequence` with a DEFAULT catch-all partition, so an insert
-- can never fail to route. The primary key is `sequence` (a partitioned
-- table's PK must include the partition key); `id` is a plain column.
-- Partitions are pre-created ahead of the head by the maintainer job in
-- `src/out/partition`.
CREATE TABLE persistent_outbox_events (
  id UUID NOT NULL DEFAULT gen_random_uuid(),
  sequence BIGSERIAL,
  payload JSONB,
  tracing_context JSONB,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (sequence)
) PARTITION BY RANGE (sequence);

-- Initial partition. Its range MUST equal DEFAULT_PARTITION_WIDTH (a fixed
-- constant) so maintainer-created partitions tile onto it without overlapping.
-- Storage params are set per-partition (not inherited via PARTITION OF).
CREATE TABLE persistent_outbox_events_p0 PARTITION OF persistent_outbox_events
  FOR VALUES FROM (0) TO (2000000)
  WITH (autovacuum_vacuum_insert_scale_factor = 0.0,
        autovacuum_vacuum_insert_threshold = 50000,
        autovacuum_freeze_min_age = 0,
        fillfactor = 100);

-- Always-empty backstop so INSERT routing never fails if the maintainer falls
-- behind. Rows landing here are still read normally; draining them is a layout
-- repair (`Partitions::recover_default`), not a correctness failure.
CREATE TABLE persistent_outbox_events_default
  PARTITION OF persistent_outbox_events DEFAULT;

-- Ephemeral outbox events
CREATE TABLE ephemeral_outbox_events (
  event_type VARCHAR NOT NULL UNIQUE,
  payload JSONB NOT NULL,
  tracing_context JSONB,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- SECURITY: the ephemeral notification is a hint, not a transport.
--
-- PostgreSQL performs no authorization on LISTEN/NOTIFY channels: any role
-- able to connect to the database could LISTEN and harvest payloads with no
-- table grant, or pg_notify a forged event consumers would accept. The
-- notification therefore carries only {event_type, recorded_at}; listeners
-- always fetch the payload from the table with their own credentials
-- (recorded_at lets them skip the fetch when their cache is already current).
CREATE FUNCTION notify_ephemeral_outbox_events() RETURNS TRIGGER AS $$
BEGIN
  PERFORM pg_notify(
    'ephemeral_outbox_events',
    json_build_object('event_type', NEW.event_type, 'recorded_at', NEW.recorded_at)::TEXT
  );
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

-- Keyed-subscriber subscriptions: one row per (subscriber_type, key)
-- identity.
--
-- Row presence IS the subscription: absence means cancelled. This table
-- holds identity and terms only (key, routing keys, instance config, birth
-- frontier); execution and progress (liveness, generations, attempts,
-- watermark) live entirely in the job crate's own tables, addressed by
-- (subscriber_type, key) through job's keyed-job machinery. Readers here
-- must never join against job-crate tables for routing (schema boundary).
--
-- `routing_keys` is a set, not a scalar: a subscription's identity is its
-- `key`, so watching several stream partitions cannot be expressed as extra
-- rows. Matching is set-overlap on both sides — an event classifies to a set
-- of routing keys, a subscription declares the set it watches, and the
-- router wakes on intersection.
CREATE TABLE subscriptions (
  subscriber_type  VARCHAR NOT NULL,
  key              VARCHAR NOT NULL,
  routing_keys     VARCHAR[] NOT NULL,
  instance_config  JSONB NOT NULL,
  start_after      BIGINT NOT NULL,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (subscriber_type, key)
);

-- Backs the router's flush-time lookup: `WHERE subscriber_type = $1 AND
-- routing_keys && $2::varchar[]`. The cast is load-bearing — Postgres has no
-- implicit varchar[]/text[] cast for `&&`, and sqlx infers text[] for an
-- untyped array parameter.
CREATE INDEX idx_subscriptions_routing_keys ON subscriptions USING GIN (routing_keys);
