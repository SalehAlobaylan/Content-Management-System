-- Durable worker claim lease. A crashed replica can only recover an expired
-- claim through the same plan-event ledger; no in-memory lock is authoritative.
ALTER TABLE operator_action_plans
  ADD COLUMN IF NOT EXISTS claim_token UUID,
  ADD COLUMN IF NOT EXISTS claim_expires_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_operator_action_plans_lease
  ON operator_action_plans(state, claim_expires_at)
  WHERE state = 'claimed';
