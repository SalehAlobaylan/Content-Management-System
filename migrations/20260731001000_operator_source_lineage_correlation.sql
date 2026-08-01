-- Future registered Operator actions may trigger a source run, but that
-- authority must remain a durable optional correlation on the CMS request,
-- never an inferred queue/job relationship.
ALTER TABLE source_run_requests
  ADD COLUMN IF NOT EXISTS operator_plan_id UUID,
  ADD COLUMN IF NOT EXISTS operator_step_id UUID;

CREATE INDEX IF NOT EXISTS idx_source_run_requests_operator_correlation
  ON source_run_requests(tenant_id, operator_plan_id, operator_step_id)
  WHERE operator_plan_id IS NOT NULL;
