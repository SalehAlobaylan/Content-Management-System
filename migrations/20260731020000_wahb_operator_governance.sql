-- Wahb Operator governance history. These records are deliberately separate
-- from action plans: schedules are read-only and shares never grant evidence
-- or execution authority.

CREATE TABLE IF NOT EXISTS operator_schedule_events (
  id BIGSERIAL PRIMARY KEY,
  schedule_id BIGINT NOT NULL REFERENCES operator_schedules(id) ON DELETE RESTRICT,
  tenant_id VARCHAR(64) NOT NULL,
  sequence BIGINT NOT NULL CHECK (sequence > 0),
  event_type VARCHAR(48) NOT NULL,
  actor_id VARCHAR(255),
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(schedule_id, sequence)
);
CREATE INDEX IF NOT EXISTS idx_operator_schedule_events_tenant_created
  ON operator_schedule_events(tenant_id, created_at DESC);

CREATE TABLE IF NOT EXISTS operator_investigation_shares (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  investigation_id BIGINT NOT NULL REFERENCES operator_investigations(id) ON DELETE RESTRICT,
  tenant_id VARCHAR(64) NOT NULL,
  recipient_id VARCHAR(255) NOT NULL,
  created_by VARCHAR(255) NOT NULL,
  state VARCHAR(16) NOT NULL DEFAULT 'active' CHECK (state IN ('active','revoked')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  revoked_at TIMESTAMPTZ,
  UNIQUE(investigation_id, recipient_id)
);
CREATE INDEX IF NOT EXISTS idx_operator_investigation_shares_recipient
  ON operator_investigation_shares(tenant_id, recipient_id, state);
