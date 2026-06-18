-- Feeds Finding — auto news-source discovery (Slice 1).
--
-- discovery_profiles: admin-defined interests that drive discovery sweeps.
-- source_suggestions:  candidate sources awaiting admin review; approving one
--                      creates a content_sources row, rejecting keeps the row so
--                      the unique (tenant_id, canonical_key) blocks re-suggestion.
-- content_sources.discovery_profile_id links an approved source back to the
-- profile that discovered it, so the News Feeds hub can group by interest.
-- (Dev gets all of this via GORM AutoMigrate; this migration is for prod.)

CREATE TABLE IF NOT EXISTS discovery_profiles (
    id                       BIGSERIAL PRIMARY KEY,
    public_id                UUID NOT NULL DEFAULT gen_random_uuid(),
    tenant_id                VARCHAR(64) NOT NULL DEFAULT 'default',
    name                     VARCHAR(255) NOT NULL,
    description              TEXT,
    keywords                 TEXT[],
    languages                TEXT[],
    enabled                  BOOLEAN NOT NULL DEFAULT TRUE,
    max_suggestions_per_run  INTEGER NOT NULL DEFAULT 10,
    last_run_at              TIMESTAMP,
    created_at               TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_discovery_profiles_public_id ON discovery_profiles (public_id);
CREATE INDEX IF NOT EXISTS idx_discovery_profiles_tenant ON discovery_profiles (tenant_id);

CREATE TABLE IF NOT EXISTS source_suggestions (
    id                  BIGSERIAL PRIMARY KEY,
    public_id           UUID NOT NULL DEFAULT gen_random_uuid(),
    tenant_id           VARCHAR(64) NOT NULL DEFAULT 'default',
    profile_id          BIGINT REFERENCES discovery_profiles(id) ON DELETE SET NULL,
    name                VARCHAR(255) NOT NULL,
    type                VARCHAR(20) NOT NULL,
    feed_url            TEXT NOT NULL,
    site_url            TEXT,
    image_url           TEXT,
    language            VARCHAR(16),
    canonical_key       TEXT NOT NULL,
    confidence          DOUBLE PRECISION NOT NULL DEFAULT 0,
    relevance_score     DOUBLE PRECISION,
    health              JSONB,
    sample_items        JSONB,
    discovered_via      VARCHAR(20),
    status              VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    reject_reason       TEXT,
    approved_source_id  BIGINT,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_source_suggestions_public_id ON source_suggestions (public_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_source_suggestions_tenant_canonical ON source_suggestions (tenant_id, canonical_key);
CREATE INDEX IF NOT EXISTS idx_source_suggestions_tenant_status ON source_suggestions (tenant_id, status);
CREATE INDEX IF NOT EXISTS idx_source_suggestions_profile ON source_suggestions (profile_id);

ALTER TABLE content_sources
    ADD COLUMN IF NOT EXISTS discovery_profile_id BIGINT;
CREATE INDEX IF NOT EXISTS idx_content_sources_discovery_profile ON content_sources (discovery_profile_id);
