-- Feed Integrity / Experience QA base system. Applied deliberately; never by startup patch.
CREATE TABLE IF NOT EXISTS feed_integrity_policies (
    id BIGSERIAL PRIMARY KEY, tenant_id VARCHAR(64) NOT NULL DEFAULT 'default',
    scheduled_enabled BOOLEAN NOT NULL DEFAULT FALSE, light_interval_minutes INTEGER NOT NULL DEFAULT 15,
    deep_interval_hours INTEGER NOT NULL DEFAULT 24, confirm_runs INTEGER NOT NULL DEFAULT 2,
    resolve_runs INTEGER NOT NULL DEFAULT 3, flap_cycles_24h INTEGER NOT NULL DEFAULT 3,
    edge_pages_per_feed INTEGER NOT NULL DEFAULT 3, probe_url_budget INTEGER NOT NULL DEFAULT 40,
    probe_concurrency INTEGER NOT NULL DEFAULT 2, probe_timeout_ms INTEGER NOT NULL DEFAULT 5000,
    foryou_latency_budget_ms INTEGER NOT NULL DEFAULT 1500, news_latency_budget_ms INTEGER NOT NULL DEFAULT 2000,
    thin_slide_floor DOUBLE PRECISION NOT NULL DEFAULT 0.80,
    expected_min_foryou_units INTEGER NOT NULL DEFAULT 1, expected_min_news_slides INTEGER NOT NULL DEFAULT 1,
    paused_until TIMESTAMP, last_light_run_at TIMESTAMP, last_deep_run_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(), updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_feed_integrity_policy_tenant ON feed_integrity_policies(tenant_id);

CREATE TABLE IF NOT EXISTS feed_integrity_runs (
    id BIGSERIAL PRIMARY KEY, public_id UUID NOT NULL DEFAULT gen_random_uuid(), tenant_id VARCHAR(64) NOT NULL DEFAULT 'default',
    trigger VARCHAR(24) NOT NULL, tier VARCHAR(16) NOT NULL, status VARCHAR(24) NOT NULL, headline VARCHAR(32) NOT NULL,
    started_at TIMESTAMP NOT NULL, finished_at TIMESTAMP, summary TEXT, feed_results JSONB, counts JSONB,
    created_by VARCHAR(255), error TEXT, error_class VARCHAR(48) NOT NULL DEFAULT 'none',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(), updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_feed_integrity_runs_public_id ON feed_integrity_runs(public_id);
CREATE INDEX IF NOT EXISTS idx_feed_integrity_runs_tenant_started ON feed_integrity_runs(tenant_id, started_at DESC);

CREATE TABLE IF NOT EXISTS feed_integrity_findings (
    id BIGSERIAL PRIMARY KEY, public_id UUID NOT NULL DEFAULT gen_random_uuid(),
    run_id BIGINT NOT NULL REFERENCES feed_integrity_runs(id) ON DELETE CASCADE, tenant_id VARCHAR(64) NOT NULL DEFAULT 'default',
    lane VARCHAR(16) NOT NULL, check_key VARCHAR(80) NOT NULL, axis VARCHAR(16) NOT NULL, feed VARCHAR(16) NOT NULL,
    variant VARCHAR(32) NOT NULL DEFAULT 'default', target_type VARCHAR(24), target_ref TEXT,
    candidate_count INTEGER NOT NULL DEFAULT 0, status VARCHAR(32) NOT NULL, severity VARCHAR(16), evidence JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_feed_integrity_findings_public_id ON feed_integrity_findings(public_id);
CREATE INDEX IF NOT EXISTS idx_feed_integrity_findings_run ON feed_integrity_findings(run_id);
CREATE INDEX IF NOT EXISTS idx_feed_integrity_findings_tenant_check ON feed_integrity_findings(tenant_id, check_key, created_at DESC);

CREATE TABLE IF NOT EXISTS feed_integrity_episodes (
    id BIGSERIAL PRIMARY KEY, public_id UUID NOT NULL DEFAULT gen_random_uuid(), tenant_id VARCHAR(64) NOT NULL DEFAULT 'default',
    check_key VARCHAR(80) NOT NULL, axis VARCHAR(16) NOT NULL, feed VARCHAR(16) NOT NULL, variant VARCHAR(32) NOT NULL DEFAULT 'default',
    scope TEXT NOT NULL DEFAULT 'feed', status VARCHAR(32) NOT NULL, severity VARCHAR(16) NOT NULL, summary TEXT,
    affected_trend JSONB, evidence JSONB, attribution JSONB, first_detected_at TIMESTAMP NOT NULL, last_seen_at TIMESTAMP NOT NULL,
    recovering_since TIMESTAMP, resolved_at TIMESTAMP, closed_by VARCHAR(255), close_reason_class VARCHAR(32), close_notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(), updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_feed_integrity_episodes_public_id ON feed_integrity_episodes(public_id);
CREATE INDEX IF NOT EXISTS idx_feed_integrity_episodes_open_identity ON feed_integrity_episodes(tenant_id, check_key, feed, variant, scope) WHERE status IN ('open','recovering');

CREATE TABLE IF NOT EXISTS feed_integrity_suppressions (
    id BIGSERIAL PRIMARY KEY, public_id UUID NOT NULL DEFAULT gen_random_uuid(), tenant_id VARCHAR(64) NOT NULL DEFAULT 'default',
    check_key VARCHAR(80) NOT NULL, feed VARCHAR(16), variant VARCHAR(32), scope TEXT, reason TEXT NOT NULL,
    starts_at TIMESTAMP NOT NULL, expires_at TIMESTAMP NOT NULL, created_by VARCHAR(255), revoked_at TIMESTAMP, revoked_by VARCHAR(255),
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_feed_integrity_suppressions_public_id ON feed_integrity_suppressions(public_id);
CREATE INDEX IF NOT EXISTS idx_feed_integrity_suppressions_active ON feed_integrity_suppressions(tenant_id, check_key, expires_at);

INSERT INTO feed_integrity_policies (tenant_id) VALUES ('default') ON CONFLICT (tenant_id) DO NOTHING;
