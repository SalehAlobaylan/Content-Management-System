-- Audio-first delivery contracts.  This migration is intentionally additive:
-- old MP4/HLS metadata remains readable while new media uses typed variants.

ALTER TABLE content_items
  ADD COLUMN IF NOT EXISTS visual_available BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS rendition_set_version INTEGER NOT NULL DEFAULT 1;

ALTER TABLE quality_profiles
  ADD COLUMN IF NOT EXISTS media_kind VARCHAR(16) NOT NULL DEFAULT 'video'
    CHECK (media_kind IN ('video','audio')),
  ADD COLUMN IF NOT EXISTS max_width INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS max_frame_rate NUMERIC(6,2) NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS video_profile VARCHAR(32) NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS video_level VARCHAR(32) NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS audio_channels INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS audio_sample_rate_hz INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS quality_tier VARCHAR(16) NOT NULL DEFAULT 'standard'
    CHECK (quality_tier IN ('data_saver','standard','high','source')),
  ADD COLUMN IF NOT EXISTS profile_purpose VARCHAR(16) NOT NULL DEFAULT 'delivery'
    CHECK (profile_purpose IN ('delivery','analysis','archive','repair')),
  ADD COLUMN IF NOT EXISTS passthrough_allowed BOOLEAN NOT NULL DEFAULT TRUE,
  ADD COLUMN IF NOT EXISTS remux_allowed BOOLEAN NOT NULL DEFAULT TRUE,
  ADD COLUMN IF NOT EXISTS schema_version INTEGER NOT NULL DEFAULT 1;

CREATE TABLE IF NOT EXISTS media_delivery_policies (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64),
  source_type VARCHAR(20),
  media_kind VARCHAR(16) NOT NULL CHECK (media_kind IN ('video','audio')),
  suitability VARCHAR(40),
  name VARCHAR(96) NOT NULL DEFAULT '',
  schema_version INTEGER NOT NULL DEFAULT 1,
  policy_digest CHAR(64) NOT NULL DEFAULT '',
  primary_mode VARCHAR(16) NOT NULL DEFAULT 'progressive' CHECK (primary_mode IN ('audio','progressive','hls')),
  short_form_delivery BOOLEAN NOT NULL DEFAULT TRUE,
  allow_native_audio BOOLEAN NOT NULL DEFAULT TRUE,
  allow_mp4_fallback BOOLEAN NOT NULL DEFAULT TRUE,
  allow_hls BOOLEAN NOT NULL DEFAULT TRUE,
  require_adaptive_hls BOOLEAN NOT NULL DEFAULT FALSE,
  allow_passthrough BOOLEAN NOT NULL DEFAULT TRUE,
  allow_remux BOOLEAN NOT NULL DEFAULT TRUE,
  generate_audio_alternate BOOLEAN NOT NULL DEFAULT TRUE,
  generate_progressive_fallback BOOLEAN NOT NULL DEFAULT TRUE,
  preserve_video BOOLEAN NOT NULL DEFAULT TRUE,
  hls_segment_duration_sec INTEGER NOT NULL DEFAULT 6,
  hls_segment_format VARCHAR(16) NOT NULL DEFAULT 'cmaf' CHECK (hls_segment_format IN ('cmaf')),
  hls_min_variants INTEGER NOT NULL DEFAULT 2,
  rollout_state VARCHAR(16) NOT NULL DEFAULT 'shadow' CHECK (rollout_state IN ('shadow','active','disabled')),
  created_by VARCHAR(128) NOT NULL DEFAULT '',
  updated_by VARCHAR(128) NOT NULL DEFAULT '',
  max_delivery_height INTEGER NOT NULL DEFAULT 720,
  max_delivery_bitrate_kbps INTEGER NOT NULL DEFAULT 0,
  cache_profile VARCHAR(32) NOT NULL DEFAULT 'immutable-media',
  active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_media_delivery_policy_scope
  ON media_delivery_policies (
    COALESCE(tenant_id, ''), COALESCE(source_type, ''), media_kind,
    COALESCE(suitability, ''), short_form_delivery
  );

CREATE TABLE IF NOT EXISTS media_delivery_policy_variants (
  id BIGSERIAL PRIMARY KEY,
  policy_id BIGINT NOT NULL REFERENCES media_delivery_policies(id) ON DELETE CASCADE,
  quality_profile_id BIGINT REFERENCES quality_profiles(id) ON DELETE SET NULL,
  rendition_type VARCHAR(32) NOT NULL CHECK (rendition_type IN ('audio_primary','audio_alternate','progressive_primary','progressive_fallback','hls_audio','hls_video_variant','analysis_audio')),
  quality_tier VARCHAR(16) NOT NULL CHECK (quality_tier IN ('data_saver','standard','high','source')),
  priority INTEGER NOT NULL DEFAULT 100,
  required BOOLEAN NOT NULL DEFAULT TRUE,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (policy_id, rendition_type, quality_tier)
);

-- One explicit, active default makes policy resolution deterministic after the
-- cutover. Scoped policies may override it, but no worker has to infer a
-- ladder from a file extension or an undocumented environment variable.
INSERT INTO media_delivery_policies (
  media_kind, name, schema_version, policy_digest, primary_mode,
  short_form_delivery, allow_native_audio, allow_mp4_fallback, allow_hls,
  require_adaptive_hls, allow_passthrough, allow_remux,
  generate_audio_alternate, generate_progressive_fallback, preserve_video,
  hls_segment_duration_sec, hls_segment_format, hls_min_variants,
  rollout_state, max_delivery_height, active
)
SELECT 'video', 'default-audio-first-cmaf-v1', 1,
  encode(digest('default-audio-first-cmaf-v1:360-600,540-1200,720-2500:aac-128:6', 'sha256'), 'hex'),
  'hls', TRUE, TRUE, TRUE, TRUE, FALSE, TRUE, TRUE, TRUE, TRUE, TRUE,
  6, 'cmaf', 2, 'active', 720, TRUE
WHERE NOT EXISTS (
  SELECT 1 FROM media_delivery_policies
  WHERE tenant_id IS NULL AND source_type IS NULL AND media_kind = 'video'
    AND suitability IS NULL AND short_form_delivery = TRUE
);

INSERT INTO media_delivery_policy_variants (policy_id, rendition_type, quality_tier, priority, required, enabled)
SELECT p.id, v.rendition_type, v.quality_tier, v.priority, v.required, TRUE
FROM media_delivery_policies p
CROSS JOIN (VALUES
  ('audio_primary', 'standard', 10, TRUE),
  ('progressive_fallback', 'standard', 20, TRUE),
  ('hls_audio', 'standard', 30, TRUE),
  ('hls_video_variant', 'data_saver', 40, TRUE),
  ('hls_video_variant', 'standard', 50, TRUE),
  ('hls_video_variant', 'high', 60, TRUE)
) AS v(rendition_type, quality_tier, priority, required)
WHERE p.name = 'default-audio-first-cmaf-v1'
ON CONFLICT (policy_id, rendition_type, quality_tier) DO NOTHING;

ALTER TABLE media_artifact_manifests
  ADD COLUMN IF NOT EXISTS package_manifest_id UUID REFERENCES media_artifact_manifests(public_id) ON DELETE RESTRICT,
  ADD COLUMN IF NOT EXISTS cache_control VARCHAR(255) NOT NULL DEFAULT '';

ALTER TABLE media_artifact_manifests
  DROP CONSTRAINT IF EXISTS media_artifact_manifests_artifact_role_check;
ALTER TABLE media_artifact_manifests
  ADD CONSTRAINT media_artifact_manifests_artifact_role_check
  CHECK (artifact_role IN (
    'source','analysis_audio','chapter_media','chapter_hls','thumbnail','transcript_segment',
    'playback_audio','playback_mp4','delivery_audio','delivery_progressive','hls_master','hls_playlist','hls_init','hls_segment'
  ));
CREATE INDEX IF NOT EXISTS idx_media_artifact_manifests_package
  ON media_artifact_manifests(tenant_id, package_manifest_id, artifact_role);

CREATE TABLE IF NOT EXISTS media_rendition_generations (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  content_item_id UUID NOT NULL,
  generation_number INTEGER NOT NULL,
  source_manifest_id UUID REFERENCES media_artifact_manifests(public_id) ON DELETE RESTRICT,
  route_decision JSONB NOT NULL DEFAULT '{}'::jsonb,
  route_digest CHAR(64) NOT NULL,
  probe_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
  probe_digest CHAR(64) NOT NULL,
  policy_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
  policy_digest CHAR(64) NOT NULL,
  rendition_set JSONB NOT NULL DEFAULT '[]'::jsonb,
  rendition_digest CHAR(64),
  state VARCHAR(24) NOT NULL CHECK (state IN ('planning','running','verifying','active','superseded','failed','uncertain')),
  attempt_id UUID,
  fence_token UUID,
  terminal_proof JSONB NOT NULL DEFAULT '{}'::jsonb,
  activation_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, content_item_id, generation_number)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_active_media_rendition_generation
  ON media_rendition_generations(tenant_id, content_item_id) WHERE state = 'active';

ALTER TABLE content_items
  ADD COLUMN IF NOT EXISTS active_media_rendition_generation_id UUID
    REFERENCES media_rendition_generations(public_id) ON DELETE RESTRICT;
CREATE INDEX IF NOT EXISTS idx_content_items_active_media_rendition_generation
  ON content_items(active_media_rendition_generation_id);

CREATE TABLE IF NOT EXISTS media_hls_packages (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  rendition_generation_id UUID NOT NULL REFERENCES media_rendition_generations(public_id) ON DELETE RESTRICT,
  master_manifest_id UUID NOT NULL REFERENCES media_artifact_manifests(public_id) ON DELETE RESTRICT,
  progressive_manifest_id UUID REFERENCES media_artifact_manifests(public_id) ON DELETE RESTRICT,
  variant_count INTEGER NOT NULL DEFAULT 0,
  state VARCHAR(24) NOT NULL CHECK (state IN ('uploading','verifying','verified','active','failed','uncertain')),
  validation_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  validation_digest CHAR(64),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, rendition_generation_id)
);

CREATE TABLE IF NOT EXISTS user_playback_preferences (
  id BIGSERIAL PRIMARY KEY,
  tenant_id VARCHAR(64) NOT NULL DEFAULT 'default',
  user_id UUID NOT NULL,
  audio_quality VARCHAR(16) NOT NULL DEFAULT 'standard'
    CHECK (audio_quality IN ('data_saver','standard','high')),
  streaming_quality VARCHAR(16) NOT NULL DEFAULT 'auto'
    CHECK (streaming_quality IN ('auto','data_saver','standard','high')),
  allow_cellular_high_quality BOOLEAN NOT NULL DEFAULT FALSE,
  prefer_audio_when_available BOOLEAN NOT NULL DEFAULT TRUE,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, user_id)
);
