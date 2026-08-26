-- Mobile delivery v3 is additive.  Existing v1/v2 playback metadata remains
-- readable until a generation-safe repair replaces it.

ALTER TABLE content_items
  ADD COLUMN IF NOT EXISTS rendition_digest CHAR(64) NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS delivery_class VARCHAR(32) NOT NULL DEFAULT 'audio_first_visual'
    CHECK (delivery_class IN ('audio_only','audio_first_visual','visual_dependent'));

ALTER TABLE media_artifact_manifests
  DROP CONSTRAINT IF EXISTS media_artifact_manifests_artifact_role_check;
ALTER TABLE media_artifact_manifests
  ADD CONSTRAINT media_artifact_manifests_artifact_role_check
  CHECK (artifact_role IN (
    'source','analysis_audio','chapter_media','chapter_hls','thumbnail','transcript_segment',
    'playback_audio','playback_mp4','delivery_audio','delivery_progressive',
    'hls_master','hls_access_master','hls_playlist','hls_init','hls_segment'
  ));

-- An access master is a small, immutable, validated playlist that limits a
-- package to a mobile-safe subset of the canonical CMAF ladder.
CREATE TABLE IF NOT EXISTS media_hls_access_points (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  package_id UUID NOT NULL REFERENCES media_hls_packages(public_id) ON DELETE RESTRICT,
  quality_tier VARCHAR(16) NOT NULL CHECK (quality_tier IN ('standard','high')),
  manifest_id UUID NOT NULL REFERENCES media_artifact_manifests(public_id) ON DELETE RESTRICT,
  max_height INTEGER NOT NULL,
  max_bandwidth_kbps INTEGER NOT NULL,
  validation_digest CHAR(64) NOT NULL,
  state VARCHAR(24) NOT NULL CHECK (state IN ('verified','active','superseded','failed')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, package_id, quality_tier)
);
CREATE INDEX IF NOT EXISTS idx_media_hls_access_points_package
  ON media_hls_access_points(tenant_id, package_id, state);

-- A compact, redacted playback-health ledger lets consumer clients improve
-- inventory/repair prioritisation without becoming repair initiators.
CREATE TABLE IF NOT EXISTS media_playback_health_receipts (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  content_item_id UUID NOT NULL,
  rendition_generation_id UUID,
  rendition_id VARCHAR(128) NOT NULL DEFAULT '',
  failure_class VARCHAR(32) NOT NULL CHECK (failure_class IN ('load','decode','stall','seek','manifest','fallback_exhausted')),
  platform VARCHAR(16) NOT NULL DEFAULT '',
  app_build VARCHAR(64) NOT NULL DEFAULT '',
  network_class VARCHAR(16) NOT NULL DEFAULT '',
  identity_key CHAR(64) NOT NULL,
  idempotency_key VARCHAR(128) NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, identity_key, idempotency_key)
);
CREATE INDEX IF NOT EXISTS idx_media_playback_health_recent
  ON media_playback_health_receipts(tenant_id, content_item_id, created_at DESC);
