-- Conversation-first Operator workspace. Existing rows remain readable; new
-- links let the Console reconstruct a case without weakening the plan ledger.
ALTER TABLE operator_threads ADD COLUMN IF NOT EXISTS pinned_at TIMESTAMPTZ;
ALTER TABLE operator_threads ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS idx_operator_threads_workspace
  ON operator_threads(tenant_id, creator_id, archived_at, pinned_at DESC, last_activity_at DESC);

ALTER TABLE operator_messages ADD COLUMN IF NOT EXISTS message_kind VARCHAR(32) NOT NULL DEFAULT 'admin_note';
ALTER TABLE operator_messages ADD COLUMN IF NOT EXISTS investigation_id BIGINT REFERENCES operator_investigations(id) ON DELETE SET NULL;
ALTER TABLE operator_messages ADD COLUMN IF NOT EXISTS plan_id BIGINT REFERENCES operator_action_plans(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS idx_operator_messages_case
  ON operator_messages(thread_id, investigation_id, plan_id, created_at);

ALTER TABLE operator_action_plans ADD COLUMN IF NOT EXISTS investigation_id BIGINT REFERENCES operator_investigations(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS idx_operator_action_plans_investigation
  ON operator_action_plans(investigation_id, created_at DESC);
