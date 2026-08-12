-- Pods Supply Continuity: durable, read-only attention episodes.
--
-- This migration adds no action request, scheduler, queue, or provider state.
-- Episodes persist CMS-derived evidence so a repeated handoff/delivery problem
-- is visible without treating an enqueue acknowledgement as success.

CREATE TABLE IF NOT EXISTS media_supply_episodes (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  fingerprint VARCHAR(128) NOT NULL,
  first_failed_boundary VARCHAR(64) NOT NULL,
  verdict VARCHAR(64) NOT NULL,
  severity VARCHAR(16) NOT NULL CHECK (severity IN ('info','warning','major','critical')),
  owner VARCHAR(128) NOT NULL,
  state VARCHAR(24) NOT NULL CHECK (state IN ('open','recovering','resolved')),
  summary TEXT NOT NULL,
  affected_subjects JSONB NOT NULL DEFAULT '[]'::jsonb,
  evidence_digest VARCHAR(128) NOT NULL,
  evidence_completeness VARCHAR(24) NOT NULL CHECK (evidence_completeness IN ('complete','partial','unavailable')),
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  first_seen_at TIMESTAMPTZ NOT NULL,
  last_seen_at TIMESTAMPTZ NOT NULL,
  slo_deadline_at TIMESTAMPTZ,
  resolved_at TIMESTAMPTZ,
  resolution_proof JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Only one active episode can own one deterministic tenant/fingerprint pair.
-- Resolved history remains immutable and does not block a later recurrence.
CREATE UNIQUE INDEX IF NOT EXISTS uq_media_supply_active_episode
  ON media_supply_episodes(tenant_id, fingerprint)
  WHERE state IN ('open','recovering');
CREATE INDEX IF NOT EXISTS idx_media_supply_episodes_attention
  ON media_supply_episodes(tenant_id, state, last_seen_at DESC, public_id DESC);

CREATE TABLE IF NOT EXISTS media_supply_episode_events (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  episode_id UUID NOT NULL,
  event_key VARCHAR(255) NOT NULL,
  event_type VARCHAR(32) NOT NULL CHECK (event_type IN ('opened','observed','recovering','resolved')),
  evidence_digest VARCHAR(128) NOT NULL,
  evaluation JSONB NOT NULL DEFAULT '{}'::jsonb,
  occurred_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, event_key)
);
CREATE INDEX IF NOT EXISTS idx_media_supply_episode_events_cursor
  ON media_supply_episode_events(tenant_id, occurred_at DESC, public_id DESC);
CREATE INDEX IF NOT EXISTS idx_media_supply_episode_events_episode
  ON media_supply_episode_events(tenant_id, episode_id, occurred_at DESC);

CREATE OR REPLACE FUNCTION reject_media_supply_episode_event_mutation()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION 'media supply episode events are append-only';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS media_supply_episode_events_append_only ON media_supply_episode_events;
CREATE TRIGGER media_supply_episode_events_append_only
BEFORE UPDATE OR DELETE ON media_supply_episode_events
FOR EACH ROW EXECUTE FUNCTION reject_media_supply_episode_event_mutation();
