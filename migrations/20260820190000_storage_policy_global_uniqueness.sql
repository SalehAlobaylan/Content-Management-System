BEGIN;

-- Preserve complete recovery evidence before consolidating historic global
-- rows. The newest updated row is already the deterministic runtime winner.
CREATE TABLE IF NOT EXISTS storage_policy_duplicate_archive (
    original_policy_id BIGINT PRIMARY KEY,
    archived_policy JSONB NOT NULL,
    archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    archive_reason TEXT NOT NULL
);

WITH ranked AS (
    SELECT id,
           ROW_NUMBER() OVER (ORDER BY updated_at DESC NULLS LAST, id DESC) AS position
    FROM storage_policies
    WHERE tenant_id IS NULL
), duplicates AS (
    SELECT p.*
    FROM storage_policies p
    JOIN ranked r ON r.id = p.id
    WHERE r.position > 1
)
INSERT INTO storage_policy_duplicate_archive (original_policy_id, archived_policy, archive_reason)
SELECT id, to_jsonb(duplicates), 'duplicate_global_policy_consolidation'
FROM duplicates
ON CONFLICT (original_policy_id) DO NOTHING;

WITH ranked AS (
    SELECT id,
           ROW_NUMBER() OVER (ORDER BY updated_at DESC NULLS LAST, id DESC) AS position
    FROM storage_policies
    WHERE tenant_id IS NULL
)
DELETE FROM storage_policies p
USING ranked r
WHERE p.id = r.id AND r.position > 1;

DROP INDEX IF EXISTS idx_storage_policy_tenant;
CREATE UNIQUE INDEX idx_storage_policy_tenant
    ON storage_policies (tenant_id) NULLS NOT DISTINCT;

COMMIT;
