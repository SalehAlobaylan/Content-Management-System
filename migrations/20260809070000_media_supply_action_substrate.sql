-- Pods Supply Continuity: immutable action preview, approval, attempt, and
-- event substrate. This migration admits no executor by itself; only static
-- CMS descriptors and owner adapters added in later code may use these rows.

CREATE TABLE IF NOT EXISTS media_supply_action_previews (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  action_key VARCHAR(128) NOT NULL,
  action_version VARCHAR(32) NOT NULL,
  target_type VARCHAR(64) NOT NULL,
  target_id UUID NOT NULL,
  evidence_digest VARCHAR(128) NOT NULL,
  policy_digest VARCHAR(128) NOT NULL,
  preflight_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  planned_effects JSONB NOT NULL DEFAULT '{}'::jsonb,
  affected_subjects JSONB NOT NULL DEFAULT '[]'::jsonb,
  deep_links JSONB NOT NULL DEFAULT '[]'::jsonb,
  state VARCHAR(24) NOT NULL CHECK (state IN ('active','consumed','invalidated')),
  expires_at TIMESTAMPTZ NOT NULL,
  created_by VARCHAR(128) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_media_supply_action_previews_active
  ON media_supply_action_previews(tenant_id, state, expires_at, created_at DESC);

CREATE TABLE IF NOT EXISTS media_supply_action_requests (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  preview_id UUID NOT NULL,
  episode_id UUID,
  action_key VARCHAR(128) NOT NULL,
  action_version VARCHAR(32) NOT NULL,
  target_type VARCHAR(64) NOT NULL,
  target_id UUID NOT NULL,
  execution_owner VARCHAR(64) NOT NULL CHECK (execution_owner IN ('cms','aggregation_dispatcher','aggregation_receipt')),
  idempotency_key VARCHAR(255) NOT NULL,
  state VARCHAR(32) NOT NULL CHECK (state IN ('awaiting_approval','queued','claimed','running','verifying','succeeded','failed','cancelled','uncertain')),
  approved_by VARCHAR(128),
  approval_evidence_digest VARCHAR(128),
  approved_at TIMESTAMPTZ,
  claim_owner VARCHAR(128),
  claim_token UUID,
  claim_epoch BIGINT NOT NULL DEFAULT 0,
  claim_expires_at TIMESTAMPTZ,
  cancellation_requested_at TIMESTAMPTZ,
  before_effects JSONB,
  planned_effects JSONB NOT NULL DEFAULT '{}'::jsonb,
  after_effects JSONB,
  verified_effects JSONB,
  affected_subjects JSONB NOT NULL DEFAULT '[]'::jsonb,
  deep_links JSONB NOT NULL DEFAULT '[]'::jsonb,
  failure_class VARCHAR(64),
  finished_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, preview_id),
  UNIQUE (tenant_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS idx_media_supply_action_requests_claim
  ON media_supply_action_requests(state, claim_expires_at, created_at ASC);
CREATE INDEX IF NOT EXISTS idx_media_supply_action_requests_target
  ON media_supply_action_requests(tenant_id, target_type, target_id, created_at DESC);

CREATE TABLE IF NOT EXISTS media_supply_action_attempts (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  action_request_id UUID NOT NULL,
  attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
  state VARCHAR(32) NOT NULL CHECK (state IN ('claimed','running','verifying','succeeded','failed','cancelled','uncertain')),
  fence_token UUID NOT NULL,
  owner_protocol VARCHAR(64) NOT NULL,
  started_at TIMESTAMPTZ,
  effect_started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, action_request_id, attempt_number),
  UNIQUE (tenant_id, action_request_id, fence_token)
);

CREATE TABLE IF NOT EXISTS media_supply_action_events (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  action_request_id UUID NOT NULL,
  attempt_id UUID,
  sequence BIGINT NOT NULL CHECK (sequence > 0),
  event_key VARCHAR(255) NOT NULL,
  event_type VARCHAR(48) NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  occurred_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, event_key),
  UNIQUE (tenant_id, action_request_id, sequence)
);
CREATE INDEX IF NOT EXISTS idx_media_supply_action_events_cursor
  ON media_supply_action_events(tenant_id, action_request_id, sequence ASC);

CREATE OR REPLACE FUNCTION reject_media_supply_action_event_mutation()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION 'media supply action events are append-only';
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS media_supply_action_events_append_only ON media_supply_action_events;
CREATE TRIGGER media_supply_action_events_append_only
BEFORE UPDATE OR DELETE ON media_supply_action_events
FOR EACH ROW EXECUTE FUNCTION reject_media_supply_action_event_mutation();
