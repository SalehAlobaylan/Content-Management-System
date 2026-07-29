-- Slice 4: immutable Month in Review policy revisions and archive revisions.
CREATE TABLE IF NOT EXISTS monthly_review_policy_versions (
  id BIGSERIAL PRIMARY KEY, public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL, version INTEGER NOT NULL, state VARCHAR(24) NOT NULL,
  config JSONB NOT NULL, reason TEXT, created_by VARCHAR(255), previous_id BIGINT,
  effective_at TIMESTAMPTZ NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, version)
);
CREATE TABLE IF NOT EXISTS monthly_review_policy_heads (
  tenant_id VARCHAR(64) PRIMARY KEY, policy_id BIGINT NOT NULL REFERENCES monthly_review_policy_versions(id) ON DELETE RESTRICT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS news_month_archives (
  id BIGSERIAL PRIMARY KEY, public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL, month_start DATE NOT NULL, timezone VARCHAR(64) NOT NULL,
  revision INTEGER NOT NULL, supersedes_id BIGINT REFERENCES news_month_archives(id) ON DELETE RESTRICT,
  policy_version_id BIGINT NOT NULL REFERENCES monthly_review_policy_versions(id) ON DELETE RESTRICT,
  state VARCHAR(24) NOT NULL CHECK (state IN ('building','composed','verified','finalized','failed')),
  limited_coverage BOOLEAN NOT NULL DEFAULT FALSE, headline TEXT NOT NULL, introduction TEXT NOT NULL,
  sections JSONB NOT NULL, headline_ar TEXT NOT NULL, introduction_ar TEXT NOT NULL, sections_ar JSONB NOT NULL,
  selection_manifest JSONB NOT NULL, selection_hash CHAR(64) NOT NULL,
  composition_hash CHAR(64) NOT NULL, qualified_count INTEGER NOT NULL, selected_count INTEGER NOT NULL,
  verification JSONB NOT NULL DEFAULT '{}'::jsonb, built_at TIMESTAMPTZ NOT NULL,
  verified_at TIMESTAMPTZ, finalized_at TIMESTAMPTZ, finalized_by VARCHAR(255),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, month_start, revision)
);
CREATE TABLE IF NOT EXISTS news_month_archive_heads (
  tenant_id VARCHAR(64) NOT NULL, month_start DATE NOT NULL,
  archive_id BIGINT NOT NULL REFERENCES news_month_archives(id) ON DELETE RESTRICT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), PRIMARY KEY (tenant_id, month_start)
);
CREATE TABLE IF NOT EXISTS news_month_archive_stories (
  id BIGSERIAL PRIMARY KEY, public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  archive_id BIGINT NOT NULL REFERENCES news_month_archives(id) ON DELETE RESTRICT,
  position INTEGER NOT NULL, section VARCHAR(120) NOT NULL, original_story_id UUID NOT NULL,
  lead_content_id UUID NOT NULL REFERENCES content_items(public_id) ON DELETE RESTRICT,
  label TEXT NOT NULL, snapshot JSONB NOT NULL, importance_score DOUBLE PRECISION NOT NULL,
  engagement_score DOUBLE PRECISION NOT NULL, final_score DOUBLE PRECISION NOT NULL,
  selection_evidence JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (archive_id, position)
);
CREATE INDEX IF NOT EXISTS idx_news_month_archives_tenant_month ON news_month_archives(tenant_id, month_start DESC);
CREATE TABLE IF NOT EXISTS news_engagement_monthly_rollups (
  id BIGSERIAL PRIMARY KEY, tenant_id VARCHAR(64) NOT NULL, month_start DATE NOT NULL,
  story_id UUID NOT NULL, interaction_type VARCHAR(32) NOT NULL,
  total_count INTEGER NOT NULL, unique_actor_count INTEGER NOT NULL,
  impression_count BIGINT NOT NULL, excluded_event_count INTEGER NOT NULL DEFAULT 0,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, month_start, story_id, interaction_type)
);
CREATE TABLE IF NOT EXISTS monthly_review_story_overrides (
  id BIGSERIAL PRIMARY KEY, public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL, month_start DATE NOT NULL, story_id UUID NOT NULL,
  decision VARCHAR(16) NOT NULL CHECK (decision IN ('include','exclude')),
  reason TEXT NOT NULL, created_by VARCHAR(255) NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, month_start, story_id)
);
