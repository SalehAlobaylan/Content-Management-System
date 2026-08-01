CREATE TABLE IF NOT EXISTS operator_shadow_enrollments (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  user_id VARCHAR(255) NOT NULL,
  state VARCHAR(16) NOT NULL DEFAULT 'active' CHECK (state IN ('active','paused','revoked')),
  enrolled_by VARCHAR(255) NOT NULL,
  reason VARCHAR(500) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(tenant_id, user_id)
);

ALTER TABLE operator_shadow_runs
  ADD COLUMN IF NOT EXISTS actor_id VARCHAR(255) NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS access_version_hash CHAR(64) NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_operator_shadow_enrollments_active
  ON operator_shadow_enrollments(state, tenant_id);
