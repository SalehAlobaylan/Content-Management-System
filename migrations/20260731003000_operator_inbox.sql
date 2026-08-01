-- The Operations inbox is a short-lived, creator-bound view over durable
-- investigations. Reading an item never changes evidence, plans, approvals,
-- or execution history.

ALTER TABLE operator_investigations
  ADD COLUMN IF NOT EXISTS read_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_operator_investigations_inbox
  ON operator_investigations(tenant_id, actor_id, state, read_at, updated_at DESC)
  WHERE state IN ('backgrounded', 'running', 'completed', 'failed');
