-- Reports can be created by a verified account or an opaque app installation.
-- The account UUID stays private; the scope is used only for rate limiting and
-- idempotent mobile delivery.
ALTER TABLE moderation_reports
    ADD COLUMN IF NOT EXISTS reporter_scope VARCHAR(96);

UPDATE moderation_reports
SET reporter_scope = 'user:' || reporter_id::text
WHERE reporter_scope IS NULL;

ALTER TABLE moderation_reports
    ALTER COLUMN reporter_id DROP NOT NULL,
    ALTER COLUMN reporter_scope SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_moderation_reports_anonymous_rate
    ON moderation_reports(tenant_id, reporter_scope, created_at DESC);

-- Keep the original table intact for safe forward migration. Its UUID primary
-- key cannot represent installation reporters, so new writes use this scope
-- keyed table.
CREATE TABLE IF NOT EXISTS consumer_moderation_idempotency_v2 (
    reporter_scope VARCHAR(96) NOT NULL,
    endpoint VARCHAR(120) NOT NULL,
    idempotency_key VARCHAR(160) NOT NULL,
    request_digest CHAR(64) NOT NULL,
    report_id UUID NOT NULL REFERENCES moderation_reports(public_id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (reporter_scope, endpoint, idempotency_key)
);
