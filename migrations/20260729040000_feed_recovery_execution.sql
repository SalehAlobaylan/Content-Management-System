-- Slice 8: resumable, non-destructive Feed Recovery execution evidence.
ALTER TABLE feed_recovery_runs ADD COLUMN IF NOT EXISTS cancel_deadline TIMESTAMPTZ;
ALTER TABLE feed_recovery_runs ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ;
ALTER TABLE feed_recovery_runs ADD COLUMN IF NOT EXISTS claim_token UUID;
ALTER TABLE feed_recovery_runs ADD COLUMN IF NOT EXISTS claim_expires_at TIMESTAMPTZ;
ALTER TABLE feed_recovery_runs ADD COLUMN IF NOT EXISTS lane_lease VARCHAR(16);
ALTER TABLE feed_integrity_runs ADD COLUMN IF NOT EXISTS correlation_id UUID;
ALTER TABLE feed_integrity_runs ADD COLUMN IF NOT EXISTS trigger_ref VARCHAR(128);
ALTER TABLE system_autopilot_runs ADD COLUMN IF NOT EXISTS correlation_id UUID;
ALTER TABLE system_autopilot_runs ADD COLUMN IF NOT EXISTS trigger_ref VARCHAR(128);
CREATE INDEX IF NOT EXISTS idx_feed_recovery_runs_claim ON feed_recovery_runs(tenant_id, claim_expires_at);
