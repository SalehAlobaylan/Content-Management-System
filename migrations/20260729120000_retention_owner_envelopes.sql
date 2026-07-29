-- Freeze the cross-service owner contract at approval time. Existing rows are
-- backfilled with a bounded 24-hour expiry so they cannot become evergreen.
ALTER TABLE retention_owner_requests
    ADD COLUMN IF NOT EXISTS allowed_action_classes JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS max_bytes BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS max_items INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS max_actions INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS correlation_id UUID,
    ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() + INTERVAL '24 hours');

CREATE INDEX IF NOT EXISTS idx_retention_owner_requests_expiry
    ON retention_owner_requests(tenant_id, status, expires_at);
