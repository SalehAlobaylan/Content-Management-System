-- Slice 7: Feed Recovery safety foundation. These tables establish durable
-- planning/approval identity and live generation namespaces; they do not
-- enable destructive recovery execution.

CREATE TABLE IF NOT EXISTS feed_recovery_plans (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  lane VARCHAR(16) NOT NULL CHECK (lane IN ('news','media','both')),
  level VARCHAR(24) NOT NULL CHECK (level IN ('repair','rotate','purge_reseed')),
  capacity_mode VARCHAR(24) NOT NULL CHECK (capacity_mode IN ('safe_cutover','low_space_reset')),
  state VARCHAR(32) NOT NULL DEFAULT 'draft',
  plan_hash CHAR(64) NOT NULL,
  manifest_hash CHAR(64) NOT NULL,
  target_count INT NOT NULL DEFAULT 0 CHECK (target_count >= 0),
  source_checksum CHAR(64) NOT NULL,
  source_count INT NOT NULL DEFAULT 0 CHECK (source_count >= 0),
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  policy_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
  no_full_rollback BOOLEAN NOT NULL DEFAULT false,
  expires_at TIMESTAMPTZ NOT NULL,
  created_by VARCHAR(255) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_feed_recovery_plans_tenant_created ON feed_recovery_plans(tenant_id, created_at DESC);

CREATE TABLE IF NOT EXISTS feed_recovery_runs (
  id BIGSERIAL PRIMARY KEY, public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  plan_id BIGINT NOT NULL REFERENCES feed_recovery_plans(id) ON DELETE RESTRICT,
  tenant_id VARCHAR(64) NOT NULL, lane VARCHAR(16) NOT NULL,
  correlation_id UUID NOT NULL DEFAULT gen_random_uuid(), phase VARCHAR(40) NOT NULL,
  not_before TIMESTAMPTZ, rollback_deadline TIMESTAMPTZ, verification_due_at TIMESTAMPTZ,
  active_generation_id UUID, candidate_generation_id UUID, outcome VARCHAR(32), error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_feed_recovery_runs_tenant_phase ON feed_recovery_runs(tenant_id, phase, updated_at DESC);

CREATE TABLE IF NOT EXISTS feed_recovery_actions (
  id BIGSERIAL PRIMARY KEY, public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  run_id BIGINT NOT NULL REFERENCES feed_recovery_runs(id) ON DELETE RESTRICT,
  action_type VARCHAR(64) NOT NULL, state VARCHAR(32) NOT NULL, idempotency_key VARCHAR(255) NOT NULL,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb, error TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(run_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS feed_recovery_approvals (
  id BIGSERIAL PRIMARY KEY, public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  plan_id BIGINT NOT NULL REFERENCES feed_recovery_plans(id) ON DELETE RESTRICT,
  tenant_id VARCHAR(64) NOT NULL, actor VARCHAR(255) NOT NULL, plan_hash CHAR(64) NOT NULL,
  manifest_hash CHAR(64) NOT NULL, target_count INT NOT NULL, phrase_proof_hash CHAR(64) NOT NULL,
  reauth_jti VARCHAR(128) NOT NULL UNIQUE, no_full_rollback BOOLEAN NOT NULL DEFAULT false,
  approved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), consumed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS feed_recovery_artifacts (
  id BIGSERIAL PRIMARY KEY, public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  plan_id BIGINT NOT NULL REFERENCES feed_recovery_plans(id) ON DELETE RESTRICT,
  tenant_id VARCHAR(64) NOT NULL, artifact_type VARCHAR(48) NOT NULL, artifact_key TEXT NOT NULL,
  sha256 CHAR(64) NOT NULL, byte_size BIGINT NOT NULL DEFAULT 0, state VARCHAR(24) NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(plan_id, artifact_type), UNIQUE(artifact_key)
);

CREATE TABLE IF NOT EXISTS feed_availability_states (
  tenant_id VARCHAR(64) NOT NULL, lane VARCHAR(16) NOT NULL CHECK (lane IN ('news','media')),
  state VARCHAR(16) NOT NULL DEFAULT 'normal' CHECK (state IN ('normal','refreshing','partial')),
  recovery_run_id BIGINT REFERENCES feed_recovery_runs(id) ON DELETE SET NULL,
  message_key VARCHAR(64), retry_after_seconds INT, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY(tenant_id, lane)
);

CREATE TABLE IF NOT EXISTS feed_generations (
  public_id UUID PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id VARCHAR(64) NOT NULL,
  lane VARCHAR(16) NOT NULL CHECK (lane IN ('news','media')), state VARCHAR(16) NOT NULL CHECK (state IN ('building','candidate','active','rollback','retired')),
  previous_generation_id UUID REFERENCES feed_generations(public_id) ON DELETE RESTRICT,
  build_watermark TIMESTAMPTZ NOT NULL DEFAULT NOW(), caught_up_at TIMESTAMPTZ, cutover_at TIMESTAMPTZ,
  rollback_deadline TIMESTAMPTZ, verification JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_feed_generations_tenant_lane_state ON feed_generations(tenant_id, lane, state);

CREATE TABLE IF NOT EXISTS feed_generation_heads (
  tenant_id VARCHAR(64) NOT NULL, lane VARCHAR(16) NOT NULL CHECK (lane IN ('news','media')),
  active_generation_id UUID REFERENCES feed_generations(public_id) ON DELETE RESTRICT,
  candidate_generation_id UUID REFERENCES feed_generations(public_id) ON DELETE SET NULL,
  generation BIGINT NOT NULL DEFAULT 1 CHECK (generation > 0), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY(tenant_id, lane)
);

CREATE TABLE IF NOT EXISTS feed_generation_memberships (
  generation_id UUID NOT NULL REFERENCES feed_generations(public_id) ON DELETE CASCADE,
  member_type VARCHAR(16) NOT NULL CHECK (member_type IN ('story','feed_unit')),
  member_id UUID NOT NULL, attached_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY(generation_id, member_type, member_id)
);
CREATE INDEX IF NOT EXISTS idx_feed_generation_membership_member ON feed_generation_memberships(member_type, member_id);

ALTER TABLE consumer_feed_sessions ADD COLUMN IF NOT EXISTS generation BIGINT NOT NULL DEFAULT 1;
