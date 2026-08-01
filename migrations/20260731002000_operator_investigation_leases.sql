-- Durable Operator investigations must survive a CMS restart without replaying
-- browser credentials or raw prompt text. The request envelope is redacted by
-- CMS before insert; the lease lets exactly one worker resume it at a time.

ALTER TABLE operator_investigations
  ADD COLUMN IF NOT EXISTS request JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS claim_token UUID,
  ADD COLUMN IF NOT EXISTS claim_expires_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_operator_investigations_claimable
  ON operator_investigations(state, claim_expires_at, updated_at)
  WHERE state IN ('backgrounded', 'running');
