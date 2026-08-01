-- Approval atomically moves an Operator plan into the durable queue. The
-- original execution table predates that lifecycle state and its check
-- constraint therefore rejected every approval at the database boundary.
-- Keep the historical states for compatibility and add the CMS-owned queued
-- state used by the worker/outbox contract.
ALTER TABLE operator_action_plans
  DROP CONSTRAINT IF EXISTS operator_action_plans_state_check;

ALTER TABLE operator_action_plans
  ADD CONSTRAINT operator_action_plans_state_check CHECK (
    state IN (
      'draft',
      'contextualizing',
      'awaiting_approval',
      'blocked',
      'expired',
      'approved',
      'queued',
      'claimed',
      'running',
      'verifying',
      'succeeded',
      'partial',
      'failed',
      'cancelled',
      'rolling_back',
      'rolled_back',
      'rollback_failed'
    )
  );
