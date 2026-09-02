-- Run the digest-bound legacy source-run repair before this migration. The
-- migration refuses to guess which duplicate active request is authoritative.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM source_run_requests
    WHERE state IN ('requested', 'accepted', 'running', 'verification_required')
    GROUP BY tenant_id, content_source_id
    HAVING COUNT(*) > 1
  ) THEN
    RAISE EXCEPTION 'circulation qualification requires source-run repair preview/apply first';
  END IF;
END $$;

UPDATE content_sources
SET next_due_at = now(), updated_at = now()
WHERE is_active = TRUE
  AND category IN ('news', 'media')
  AND next_due_at IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_source_run_requests_one_active_source
  ON source_run_requests (tenant_id, content_source_id)
  WHERE state IN ('requested', 'accepted', 'running', 'verification_required');

CREATE INDEX IF NOT EXISTS idx_source_run_requests_active_expiry
  ON source_run_requests (expires_at, deadline_at)
  WHERE state IN ('requested', 'accepted', 'running', 'verification_required');
