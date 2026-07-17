-- Lifecycle indexes require tenant scope.  Do not backfill or infer ownership
-- here: later tenant-invariant migrations validate and enforce that boundary.
ALTER TABLE content_items ADD COLUMN IF NOT EXISTS tenant_id varchar(64);
ALTER TABLE stories ADD COLUMN IF NOT EXISTS tenant_id varchar(64);
ALTER TABLE topics ADD COLUMN IF NOT EXISTS tenant_id varchar(64);
ALTER TABLE topic_proposals ADD COLUMN IF NOT EXISTS tenant_id varchar(64);
ALTER TABLE discovery_profiles ADD COLUMN IF NOT EXISTS tenant_id varchar(64);
