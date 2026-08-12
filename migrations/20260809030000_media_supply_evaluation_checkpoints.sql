-- Pods Supply Continuity: bounded evaluator liveness proof.
--
-- One row per explicit tenant records the latest observation outcome. This is
-- deliberately a checkpoint, not an unbounded scheduler ledger: immutable
-- attention history remains in media_supply_episode_events. The table cannot
-- authorize source runs, providers, queues, retries, or Operator plans.

CREATE TABLE IF NOT EXISTS media_supply_evaluation_checkpoints (
  id BIGSERIAL PRIMARY KEY,
  tenant_id VARCHAR(64) NOT NULL UNIQUE,
  last_trigger VARCHAR(16) NOT NULL CHECK (last_trigger IN ('scheduled','manual')),
  last_outcome VARCHAR(32) NOT NULL CHECK (last_outcome IN ('evaluated','disabled','control_unavailable','record_failed')),
  last_observed_at TIMESTAMPTZ NOT NULL,
  last_evaluated_at TIMESTAMPTZ,
  evaluation_digest VARCHAR(128),
  last_failure_class VARCHAR(64),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (
    (last_outcome = 'evaluated' AND last_evaluated_at IS NOT NULL AND evaluation_digest IS NOT NULL AND last_failure_class IS NULL)
    OR
    (last_outcome <> 'evaluated' AND last_failure_class IS NOT NULL)
  )
);

CREATE INDEX IF NOT EXISTS idx_media_supply_evaluation_checkpoints_observed
  ON media_supply_evaluation_checkpoints(last_observed_at DESC);
