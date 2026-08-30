-- Keyed-subscriber subscriptions: one row per (subscriber_type, key)
-- identity.
--
-- Row presence IS the subscription: absence means cancelled. This table
-- holds identity and terms only (key, routing keys, instance config, birth
-- frontier); execution and progress (liveness, generations, attempts,
-- watermark) live entirely in the job crate's own tables, addressed by
-- (subscriber_type, key) through job's keyed-job machinery. Readers here
-- must never join against job-crate tables for routing (schema boundary).
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
-- routing_keys && $2`.
CREATE INDEX idx_subscriptions_routing_keys ON subscriptions USING GIN (routing_keys);
