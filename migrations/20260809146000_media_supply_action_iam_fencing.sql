-- Pods Supply Continuity: approval-time IAM identity/version fencing. Existing
-- action rows remain readable but have no authority to start a new effect.

ALTER TABLE media_supply_action_previews
  ADD COLUMN IF NOT EXISTS created_access_version VARCHAR(128);

ALTER TABLE media_supply_action_requests
  ADD COLUMN IF NOT EXISTS approval_access_version VARCHAR(128);

CREATE INDEX IF NOT EXISTS idx_media_supply_action_requests_approval_access
  ON media_supply_action_requests(tenant_id, approved_by, approval_access_version)
  WHERE state IN ('queued', 'claimed', 'running', 'verifying', 'uncertain');

COMMENT ON COLUMN media_supply_action_previews.created_access_version IS
  'Fresh IAM snapshot version held by the actor when the immutable preview was created.';
COMMENT ON COLUMN media_supply_action_requests.approval_access_version IS
  'Fresh IAM snapshot version rechecked at claim and immediately before any action effect.';
