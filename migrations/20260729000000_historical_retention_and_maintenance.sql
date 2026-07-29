-- Slice 5: immutable historical-retirement scope and operator-only readiness evidence.
CREATE TABLE IF NOT EXISTS retention_historical_manifests (
  id BIGSERIAL PRIMARY KEY, public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  run_id BIGINT NOT NULL REFERENCES retention_runs(id) ON DELETE RESTRICT,
  action_id BIGINT UNIQUE REFERENCES retention_actions(id) ON DELETE RESTRICT,
  tenant_id VARCHAR(64) NOT NULL, policy_version INTEGER NOT NULL, timezone VARCHAR(64) NOT NULL,
  manifest_hash CHAR(64) NOT NULL UNIQUE,
  state VARCHAR(24) NOT NULL CHECK (state IN ('prepared','approved','executed','blocked','expired')),
  content_ids JSONB NOT NULL, story_ids JSONB NOT NULL, evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  content_count INTEGER NOT NULL, story_count INTEGER NOT NULL, estimated_bytes BIGINT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL, approved_at TIMESTAMPTZ, approved_by VARCHAR(255),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_retention_historical_manifests_state ON retention_historical_manifests(tenant_id,state,created_at DESC);
CREATE TABLE IF NOT EXISTS retention_historical_recovery_artifacts (
  id BIGSERIAL PRIMARY KEY, public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  action_id BIGINT NOT NULL UNIQUE REFERENCES retention_actions(id) ON DELETE RESTRICT,
  manifest_id BIGINT NOT NULL UNIQUE REFERENCES retention_historical_manifests(id) ON DELETE RESTRICT,
  tenant_id VARCHAR(64) NOT NULL, artifact_key TEXT NOT NULL UNIQUE, sha256 CHAR(64) NOT NULL,
  compressed_bytes BIGINT NOT NULL, uncompressed_bytes BIGINT NOT NULL,
  state VARCHAR(24) NOT NULL, expires_at TIMESTAMPTZ NOT NULL, verified_at TIMESTAMPTZ,
  deleted_at TIMESTAMPTZ, last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_retention_historical_recovery_artifacts_expiry
  ON retention_historical_recovery_artifacts(tenant_id, state, expires_at);
CREATE TABLE IF NOT EXISTS retention_maintenance_reports (
  id BIGSERIAL PRIMARY KEY, public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL, database_bytes BIGINT NOT NULL, target_bytes BIGINT NOT NULL,
  sparse_use_count BIGINT NOT NULL, state VARCHAR(24) NOT NULL,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
