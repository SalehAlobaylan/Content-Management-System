ALTER TABLE content_sources
  ADD COLUMN IF NOT EXISTS last_upstream_observed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_no_change_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_content_sources_provider_checkpoint
  ON content_sources(tenant_id, category, last_provider_success_at DESC);
