-- Feed Recovery remediation: durable tenant/lane leases with fencing.

CREATE TABLE IF NOT EXISTS feed_recovery_lane_leases (
    tenant_id VARCHAR(64) NOT NULL,
    lane VARCHAR(16) NOT NULL CHECK (lane IN ('news', 'media')),
    run_id BIGINT NOT NULL REFERENCES feed_recovery_runs(id) ON DELETE RESTRICT,
    fencing_token UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, lane)
);
CREATE INDEX IF NOT EXISTS idx_feed_recovery_lane_leases_expiry
    ON feed_recovery_lane_leases(expires_at);
