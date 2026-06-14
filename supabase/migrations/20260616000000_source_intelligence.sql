-- Feeds Finding Slice 4 — Source Intelligence Graph.
--
-- source_candidates: persistent ledger of candidate news domains + their graph
--   signals (citations, co-citations, PageRank authority, trend, composite).
-- source_edges:      the directed citation graph (from_host -> to_host).
-- discovery_configs: + intelligence toggle/interval/threshold + 6 signal weights.
-- (Dev gets all of this via GORM AutoMigrate; this migration is for prod.)

CREATE TABLE IF NOT EXISTS source_candidates (
    id                 BIGSERIAL PRIMARY KEY,
    public_id          UUID NOT NULL DEFAULT gen_random_uuid(),
    tenant_id          VARCHAR(64) NOT NULL DEFAULT 'default',
    domain             VARCHAR(255) NOT NULL,
    canonical_key      TEXT,
    resolved_feed_url  TEXT,
    feed_valid         BOOLEAN NOT NULL DEFAULT FALSE,
    last_resolved_at   TIMESTAMP,
    citation_count     INTEGER NOT NULL DEFAULT 0,
    cocitation_count   INTEGER NOT NULL DEFAULT 0,
    authority_score    DOUBLE PRECISION NOT NULL DEFAULT 0,
    trend              VARCHAR(12) NOT NULL DEFAULT 'flat',
    composite_score    DOUBLE PRECISION NOT NULL DEFAULT 0,
    status             VARCHAR(16) NOT NULL DEFAULT 'candidate',
    discovered_via     TEXT[],
    sample_titles      JSONB,
    feed_health        JSONB,
    evidence           JSONB,
    first_seen_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    last_seen_at       TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_source_candidates_public_id ON source_candidates (public_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_source_candidates_tenant_domain ON source_candidates (tenant_id, domain);
CREATE INDEX IF NOT EXISTS idx_source_candidates_status ON source_candidates (status);

CREATE TABLE IF NOT EXISTS source_edges (
    id         BIGSERIAL PRIMARY KEY,
    tenant_id  VARCHAR(64) NOT NULL DEFAULT 'default',
    from_host  VARCHAR(255) NOT NULL,
    to_host    VARCHAR(255) NOT NULL,
    weight     INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_source_edges_tenant_pair ON source_edges (tenant_id, from_host, to_host);

ALTER TABLE source_suggestions
    ADD COLUMN IF NOT EXISTS evidence JSONB;

ALTER TABLE discovery_configs
    ADD COLUMN IF NOT EXISTS intelligence_enabled       BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS graph_build_interval_hours INTEGER NOT NULL DEFAULT 24,
    ADD COLUMN IF NOT EXISTS promotion_threshold        DOUBLE PRECISION NOT NULL DEFAULT 0.50,
    ADD COLUMN IF NOT EXISTS weight_citation            DOUBLE PRECISION NOT NULL DEFAULT 0.20,
    ADD COLUMN IF NOT EXISTS weight_cocitation          DOUBLE PRECISION NOT NULL DEFAULT 0.20,
    ADD COLUMN IF NOT EXISTS weight_authority           DOUBLE PRECISION NOT NULL DEFAULT 0.20,
    ADD COLUMN IF NOT EXISTS weight_relevance           DOUBLE PRECISION NOT NULL DEFAULT 0.25,
    ADD COLUMN IF NOT EXISTS weight_health              DOUBLE PRECISION NOT NULL DEFAULT 0.10,
    ADD COLUMN IF NOT EXISTS weight_novelty             DOUBLE PRECISION NOT NULL DEFAULT 0.05;
