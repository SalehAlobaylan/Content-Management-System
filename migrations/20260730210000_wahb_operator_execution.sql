-- Immutable action plans and execution ledger. No foreign key cascades from
-- conversations or investigations reach these audit records.

CREATE TABLE IF NOT EXISTS operator_action_plans (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  actor_id VARCHAR(255) NOT NULL,
  tool_key VARCHAR(150) NOT NULL,
  tool_version VARCHAR(50) NOT NULL,
  state VARCHAR(32) NOT NULL CHECK (state IN ('draft','contextualizing','awaiting_approval','blocked','expired','approved','claimed','running','verifying','succeeded','partial','failed','cancelled','rolling_back','rolled_back','rollback_failed')),
  risk_tier VARCHAR(24) NOT NULL CHECK (risk_tier IN ('read','routine','high_impact')),
  canonical_plan JSONB NOT NULL,
  evidence_fingerprint CHAR(64) NOT NULL,
  access_version VARCHAR(200) NOT NULL,
  digest CHAR(64) NOT NULL,
  signature CHAR(64) NOT NULL,
  idempotency_key VARCHAR(255) NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  approved_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(tenant_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS idx_operator_action_plans_claim
  ON operator_action_plans(state, expires_at) WHERE state IN ('approved','claimed','running','verifying');

CREATE TABLE IF NOT EXISTS operator_action_steps (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  plan_id BIGINT NOT NULL REFERENCES operator_action_plans(id) ON DELETE RESTRICT,
  tenant_id VARCHAR(64) NOT NULL,
  ordinal INTEGER NOT NULL CHECK (ordinal > 0),
  state VARCHAR(32) NOT NULL,
  tool_key VARCHAR(150) NOT NULL,
  targets JSONB NOT NULL,
  arguments JSONB NOT NULL,
  branch JSONB NOT NULL DEFAULT '{}'::jsonb,
  before_state JSONB,
  after_state JSONB,
  verified_state JSONB,
  claim_token UUID,
  claim_expires_at TIMESTAMPTZ,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(plan_id, ordinal)
);
CREATE INDEX IF NOT EXISTS idx_operator_action_steps_claim
  ON operator_action_steps(state, claim_expires_at);

CREATE TABLE IF NOT EXISTS operator_plan_approvals (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  plan_id BIGINT NOT NULL REFERENCES operator_action_plans(id) ON DELETE RESTRICT,
  tenant_id VARCHAR(64) NOT NULL,
  actor_id VARCHAR(255) NOT NULL,
  access_version VARCHAR(200) NOT NULL,
  plan_digest CHAR(64) NOT NULL,
  confirmation_tier VARCHAR(24) NOT NULL CHECK (confirmation_tier IN ('routine','high_impact')),
  confirmation_proof_hash CHAR(64),
  expires_at TIMESTAMPTZ NOT NULL,
  consumed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(plan_id, actor_id, plan_digest)
);

CREATE TABLE IF NOT EXISTS operator_plan_events (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  plan_id BIGINT NOT NULL REFERENCES operator_action_plans(id) ON DELETE RESTRICT,
  step_id BIGINT REFERENCES operator_action_steps(id) ON DELETE RESTRICT,
  tenant_id VARCHAR(64) NOT NULL,
  sequence BIGINT NOT NULL CHECK (sequence > 0),
  event_type VARCHAR(48) NOT NULL,
  actor_type VARCHAR(16) NOT NULL CHECK (actor_type IN ('admin','worker','system')),
  actor_id VARCHAR(255),
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(plan_id, sequence)
);
CREATE INDEX IF NOT EXISTS idx_operator_plan_events_tenant_created
  ON operator_plan_events(tenant_id, created_at DESC);
