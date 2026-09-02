ALTER TABLE content_stage_requests
  ADD COLUMN IF NOT EXISTS priority smallint NOT NULL DEFAULT 0;

ALTER TABLE content_stage_requests
  ADD CONSTRAINT content_stage_requests_priority_check
  CHECK (priority BETWEEN 0 AND 100);

CREATE INDEX IF NOT EXISTS idx_content_stage_requests_claim_priority
  ON content_stage_requests (lane, owner, state, priority DESC, created_at, public_id)
  WHERE cancellation_requested_at IS NULL;
