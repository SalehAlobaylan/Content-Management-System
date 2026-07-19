CREATE TABLE IF NOT EXISTS moderation_reports (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    tenant_id VARCHAR(64) NOT NULL DEFAULT 'default',
    reporter_id UUID NOT NULL,
    target_type VARCHAR(16) NOT NULL CHECK (target_type IN ('content', 'comment')),
    target_id UUID NOT NULL,
    reason VARCHAR(64) NOT NULL CHECK (reason IN ('harmful_inappropriate', 'misinformation', 'copyright', 'broken_media', 'incorrect_language_translation', 'other')),
    detail TEXT,
    status VARCHAR(24) NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'resolved', 'dismissed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_moderation_reports_queue
    ON moderation_reports(tenant_id, status, created_at DESC);

CREATE TABLE IF NOT EXISTS user_blocks (
    id BIGSERIAL PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL DEFAULT 'default',
    user_id UUID NOT NULL,
    blocked_user_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT user_blocks_not_self CHECK (user_id <> blocked_user_id),
    CONSTRAINT idx_user_blocks_identity UNIQUE (tenant_id, user_id, blocked_user_id)
);

CREATE INDEX IF NOT EXISTS idx_user_blocks_reader
    ON user_blocks(tenant_id, user_id);

CREATE TABLE IF NOT EXISTS consumer_moderation_idempotency (
    reporter_id UUID NOT NULL,
    endpoint VARCHAR(120) NOT NULL,
    idempotency_key VARCHAR(160) NOT NULL,
    request_digest CHAR(64) NOT NULL,
    report_id UUID NOT NULL REFERENCES moderation_reports(public_id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (reporter_id, endpoint, idempotency_key)
);
