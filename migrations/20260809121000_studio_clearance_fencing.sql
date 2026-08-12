ALTER TABLE studio_clearance_requests
  ADD COLUMN IF NOT EXISTS claim_owner VARCHAR(128),
  ADD COLUMN IF NOT EXISTS cancellation_requested_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS decisions JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS failure_class VARCHAR(64);

CREATE INDEX IF NOT EXISTS idx_studio_clearance_claim
  ON studio_clearance_requests(state, created_at);

CREATE TABLE IF NOT EXISTS studio_clearance_attempts (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  request_id UUID NOT NULL REFERENCES studio_clearance_requests(public_id),
  attempt_number INTEGER NOT NULL CHECK (attempt_number BETWEEN 1 AND 2),
  state VARCHAR(24) NOT NULL CHECK (state IN ('claimed','running','verifying','uncertain','succeeded','failed','cancelled')),
  claim_token UUID NOT NULL,
  fence_token UUID NOT NULL,
  lease_expires_at TIMESTAMPTZ NOT NULL,
  heartbeat_at TIMESTAMPTZ NOT NULL,
  effect_started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(tenant_id, request_id, attempt_number)
);

CREATE TABLE IF NOT EXISTS studio_clearance_events (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  request_id UUID NOT NULL REFERENCES studio_clearance_requests(public_id),
  sequence BIGINT NOT NULL,
  event_type VARCHAR(48) NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  occurred_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(tenant_id, request_id, sequence)
);

CREATE OR REPLACE FUNCTION reject_studio_clearance_event_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'studio clearance events are append-only';
END $$;

DROP TRIGGER IF EXISTS trg_studio_clearance_events_immutable ON studio_clearance_events;
CREATE TRIGGER trg_studio_clearance_events_immutable
BEFORE UPDATE OR DELETE ON studio_clearance_events
FOR EACH ROW EXECUTE FUNCTION reject_studio_clearance_event_mutation();
