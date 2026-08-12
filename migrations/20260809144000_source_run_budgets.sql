ALTER TABLE source_run_requests
  ADD COLUMN IF NOT EXISTS workload_cap INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS item_cap INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS byte_cap BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS provider_call_cap INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS reserved_workload INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS reserved_items INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS reserved_bytes BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS reserved_provider_calls INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS consumed_workload INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS consumed_items INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS consumed_bytes BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS consumed_provider_calls INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS released_workload INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS released_items INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS released_bytes BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS released_provider_calls INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS budget_state VARCHAR(24) NOT NULL DEFAULT 'legacy_unknown',
  ADD COLUMN IF NOT EXISTS budget_settled_at TIMESTAMPTZ;

ALTER TABLE source_run_requests
  DROP CONSTRAINT IF EXISTS source_run_requests_budget_state_check;
ALTER TABLE source_run_requests
  ADD CONSTRAINT source_run_requests_budget_state_check
  CHECK (budget_state IN ('legacy_unknown','reserved','settled','exceeded')) NOT VALID;
ALTER TABLE source_run_requests VALIDATE CONSTRAINT source_run_requests_budget_state_check;

ALTER TABLE source_run_requests
  DROP CONSTRAINT IF EXISTS source_run_requests_budget_nonnegative_check;
ALTER TABLE source_run_requests
  ADD CONSTRAINT source_run_requests_budget_nonnegative_check CHECK (
    workload_cap >= 0 AND item_cap >= 0 AND byte_cap >= 0 AND provider_call_cap >= 0 AND
    reserved_workload >= 0 AND reserved_items >= 0 AND reserved_bytes >= 0 AND reserved_provider_calls >= 0 AND
    consumed_workload >= 0 AND consumed_items >= 0 AND consumed_bytes >= 0 AND consumed_provider_calls >= 0 AND
    released_workload >= 0 AND released_items >= 0 AND released_bytes >= 0 AND released_provider_calls >= 0
  ) NOT VALID;
ALTER TABLE source_run_requests VALIDATE CONSTRAINT source_run_requests_budget_nonnegative_check;

CREATE INDEX IF NOT EXISTS idx_source_run_requests_active_budget
  ON source_run_requests(tenant_id, budget_state, state)
  WHERE budget_state = 'reserved';
