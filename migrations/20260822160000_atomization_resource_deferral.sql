-- Resource scarcity is a scheduling state, not an invalid long-form parent.
ALTER TABLE atomization_work_requests
  ADD COLUMN IF NOT EXISTS not_before_at timestamptz;

CREATE INDEX IF NOT EXISTS idx_atomization_work_claim_ready
  ON atomization_work_requests(state, not_before_at, created_at)
  WHERE state = 'queued';
