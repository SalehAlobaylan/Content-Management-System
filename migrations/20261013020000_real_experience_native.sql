-- Additive native collection scope. Existing evidence remains Web and native
-- admission stays off until an operator explicitly enables its client.
ALTER TABLE experience_events ADD COLUMN IF NOT EXISTS client_platform VARCHAR(8) NOT NULL DEFAULT 'web';
ALTER TABLE experience_events ADD COLUMN IF NOT EXISTS os_major INTEGER NOT NULL DEFAULT 0;
ALTER TABLE experience_metric_rollups ADD COLUMN IF NOT EXISTS client_platform VARCHAR(8) NOT NULL DEFAULT 'web';
ALTER TABLE experience_incidents ADD COLUMN IF NOT EXISTS client_platform VARCHAR(8) NOT NULL DEFAULT 'web';
ALTER TABLE experience_suppressions ADD COLUMN IF NOT EXISTS client_platform VARCHAR(8) NOT NULL DEFAULT 'web';
ALTER TABLE experience_policies ADD COLUMN IF NOT EXISTS enabled_clients VARCHAR(32) NOT NULL DEFAULT 'web';
DROP INDEX IF EXISTS idx_experience_rollups_identity;
CREATE UNIQUE INDEX idx_experience_rollups_identity ON experience_metric_rollups
 (tenant_id,bucket_start,resolution,metric_key,surface,cohort_dim,cohort_val,client_platform);
CREATE INDEX IF NOT EXISTS idx_experience_events_client_window ON experience_events
 (tenant_id,client_platform,surface,received_at,id);
