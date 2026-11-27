CREATE TABLE {{prefix}}persistent_outbox_events (
  id UUID NOT NULL DEFAULT gen_random_uuid(),
  sequence BIGSERIAL,
  payload JSONB,
  tracing_context JSONB,
  event_context JSONB,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  commit_xid BIGINT NOT NULL DEFAULT pg_current_xact_id()::text::bigint,
  PRIMARY KEY (sequence)
) PARTITION BY RANGE (sequence);

CREATE INDEX {{prefix}}idx_persistent_outbox_events_commit_xid
  ON {{prefix}}persistent_outbox_events (commit_xid);

CREATE TABLE {{prefix}}persistent_outbox_events_p0 PARTITION OF {{prefix}}persistent_outbox_events
  FOR VALUES FROM (0) TO (2000000)
  WITH (autovacuum_vacuum_insert_scale_factor = 0.0,
        autovacuum_vacuum_insert_threshold = 50000,
        autovacuum_freeze_min_age = 0,
        fillfactor = 100);

CREATE TABLE {{prefix}}persistent_outbox_events_default
  PARTITION OF {{prefix}}persistent_outbox_events DEFAULT;

CREATE TABLE {{prefix}}persistent_outbox_commit_checkpoints (
  sequence    BIGINT PRIMARY KEY,
  commit_seq  BIGINT NOT NULL UNIQUE,
  open_groups JSONB  NOT NULL,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE {{prefix}}ephemeral_outbox_events (
  event_type VARCHAR NOT NULL UNIQUE,
  payload JSONB NOT NULL,
  tracing_context JSONB,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE FUNCTION {{prefix}}notify_ephemeral_outbox_events() RETURNS TRIGGER AS $$
BEGIN
  PERFORM pg_notify(
    '{{prefix}}ephemeral_outbox_events',
    json_build_object('event_type', NEW.event_type, 'recorded_at', NEW.recorded_at)::TEXT
  );
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER {{prefix}}ephemeral_outbox_events_notify
  AFTER INSERT OR UPDATE ON {{prefix}}ephemeral_outbox_events
  FOR EACH ROW EXECUTE FUNCTION {{prefix}}notify_ephemeral_outbox_events();

-- Idempotent so that multiple scoped obix instances can be used simultaneously.
DO $$ BEGIN
    CREATE TYPE InboxEventStatus AS ENUM ('pending', 'processing', 'completed', 'failed');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

CREATE TABLE {{prefix}}inbox_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  idempotency_key VARCHAR UNIQUE,
  payload JSONB NOT NULL,
  status InboxEventStatus NOT NULL DEFAULT 'pending',
  error VARCHAR,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  processed_at TIMESTAMPTZ
);
CREATE INDEX {{prefix}}idx_inbox_events_status ON {{prefix}}inbox_events(status)
  WHERE status IN ('pending', 'processing', 'failed');

CREATE TABLE {{prefix}}subscriptions (
  subscriber_type  VARCHAR NOT NULL,
  key              VARCHAR NOT NULL,
  wake_keys        VARCHAR[] NOT NULL CHECK (cardinality(wake_keys) > 0),
  instance_config  JSONB NOT NULL,
  start_after      BIGINT NOT NULL,
  checkpoint       BIGINT NOT NULL,
  dormant          BOOLEAN NOT NULL DEFAULT FALSE,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (subscriber_type, key)
);
CREATE INDEX {{prefix}}idx_subscriptions_checkpoint ON {{prefix}}subscriptions (checkpoint);
CREATE INDEX {{prefix}}idx_subscriptions_wake_keys ON {{prefix}}subscriptions USING GIN (wake_keys);

CREATE TABLE {{prefix}}retired_subscriptions (
  subscriber_type  VARCHAR NOT NULL,
  key              VARCHAR NOT NULL,
  retired_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (subscriber_type, key)
);

CREATE TABLE {{prefix}}joinable_tips (
  subscriber_type  VARCHAR PRIMARY KEY,
  checkpoint       BIGINT,
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE {{prefix}}joinable_catch_ups (
  subscriber_type     VARCHAR NOT NULL,
  key                 VARCHAR NOT NULL,
  instance_config     JSONB NOT NULL,
  joined_at_sequence  BIGINT,
  ended_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (subscriber_type, key)
);
