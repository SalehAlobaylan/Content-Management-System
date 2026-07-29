-- Slice 9: bounded Low-Space Purge & Reseed state. Sources and durable source
-- checkpoints remain outside this schema and are never mutation targets.
ALTER TABLE feed_recovery_plans ADD COLUMN IF NOT EXISTS purge_manifest JSONB;
ALTER TABLE feed_recovery_plans ADD COLUMN IF NOT EXISTS manifest_frozen_at TIMESTAMPTZ;
ALTER TABLE feed_recovery_runs ADD COLUMN IF NOT EXISTS destructive_manifest JSONB;
ALTER TABLE feed_recovery_runs ADD COLUMN IF NOT EXISTS destructive_lane VARCHAR(16);
ALTER TABLE feed_recovery_runs ADD COLUMN IF NOT EXISTS expected_empty BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE feed_recovery_runs ADD COLUMN IF NOT EXISTS recovery_artifact_ref TEXT;

ALTER TABLE news_ingest_tombstones ALTER COLUMN retirement_action_id DROP NOT NULL;
ALTER TABLE news_ingest_tombstones ADD COLUMN IF NOT EXISTS recovery_run_id BIGINT REFERENCES feed_recovery_runs(id) ON DELETE RESTRICT;
ALTER TABLE news_ingest_tombstones ADD COLUMN IF NOT EXISTS recovery_run_public_id UUID;
CREATE INDEX IF NOT EXISTS idx_news_tombstones_recovery_run ON news_ingest_tombstones(recovery_run_id, manifest_hash);

ALTER TABLE feed_availability_states DROP CONSTRAINT IF EXISTS feed_availability_states_state_check;
ALTER TABLE feed_availability_states ADD CONSTRAINT feed_availability_states_state_check
  CHECK (state IN ('normal','refreshing','partial','expected_empty'));
