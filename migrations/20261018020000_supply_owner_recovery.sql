-- Exact signed-plan handoffs survive a crash before the Operator observes them.
ALTER TABLE media_supply_action_requests
  ADD COLUMN operator_plan_id UUID,
  ADD COLUMN operator_step_id UUID,
  ADD CONSTRAINT media_supply_operator_correlation_pair CHECK ((operator_plan_id IS NULL) = (operator_step_id IS NULL));
CREATE UNIQUE INDEX media_supply_operator_step_once
  ON media_supply_action_requests (tenant_id, operator_plan_id, operator_step_id)
  WHERE operator_plan_id IS NOT NULL;
CREATE INDEX media_supply_operator_monitor
  ON media_supply_action_requests (updated_at, public_id)
  WHERE operator_plan_id IS NOT NULL;
-- Reviewed windows rotate by last review, while immutable creation order remains available.
CREATE INDEX artifact_coverage_fair_queue ON artifact_coverage_requests (owner, updated_at, created_at, public_id) WHERE state='queued';
CREATE INDEX atomization_work_fair_queue ON atomization_work_requests (updated_at, created_at, public_id) WHERE state='queued';
CREATE INDEX studio_clearance_fair_queue ON studio_clearance_requests (updated_at, created_at, public_id) WHERE state='queued';
CREATE INDEX media_supply_action_fair_queue ON media_supply_action_requests (execution_owner, updated_at, created_at, public_id) WHERE state='queued';
