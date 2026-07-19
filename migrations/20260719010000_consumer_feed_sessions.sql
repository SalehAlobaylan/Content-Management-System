-- A six-hour server-owned snapshot prevents ranking rebuilds or cursor drift
-- from reshuffling an active consumer session. It contains CMS response
-- snapshots only; clients still enforce local restoration and never write it.
CREATE TABLE IF NOT EXISTS consumer_feed_sessions (
    id UUID PRIMARY KEY,
    identity_scope VARCHAR(320) NOT NULL,
    feed_type VARCHAR(24) NOT NULL CHECK (feed_type IN ('foryou')),
    snapshot_json JSONB NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_consumer_feed_sessions_identity_expiry
    ON consumer_feed_sessions(identity_scope, feed_type, expires_at DESC);
