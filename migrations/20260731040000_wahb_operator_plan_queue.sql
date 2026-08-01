-- Durable CMS-owned execution queue for approved Operator plans. The worker
-- never receives a browser credential and every job is bound to one immutable
-- signed plan.
CREATE TABLE IF NOT EXISTS operator_action_jobs (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  plan_id BIGINT NOT NULL REFERENCES operator_action_plans(id) ON DELETE RESTRICT,
  tenant_id VARCHAR(64) NOT NULL,
  state VARCHAR(32) NOT NULL DEFAULT 'queued',
  attempt_count INTEGER NOT NULL DEFAULT 0,
  available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  claim_token UUID,
  claim_expires_at TIMESTAMPTZ,
  last_error_class VARCHAR(128),
  completed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(plan_id)
);

CREATE INDEX IF NOT EXISTS idx_operator_action_jobs_due
  ON operator_action_jobs(state, available_at)
  WHERE state IN ('queued', 'authorizing');

CREATE INDEX IF NOT EXISTS idx_operator_action_jobs_lease
  ON operator_action_jobs(state, claim_expires_at)
  WHERE state IN ('authorizing', 'claimed', 'running', 'verifying');

CREATE INDEX IF NOT EXISTS idx_operator_action_plans_queued
  ON operator_action_plans(state, expires_at)
  WHERE state IN ('queued', 'claimed', 'running', 'verifying');
