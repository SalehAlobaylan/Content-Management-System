-- Metadata-first media acquisition. Discovery may persist preview metadata
-- without admitting an expensive media effect; CMS remains the authority that
-- releases manual items into the durable Pods stage scheduler.

CREATE TABLE IF NOT EXISTS media_acquisition_configs (
  id bigserial PRIMARY KEY,
  tenant_id varchar(64) NOT NULL UNIQUE,
  default_mode varchar(16) NOT NULL DEFAULT 'automatic'
    CHECK (default_mode IN ('automatic', 'manual')),
  updated_by varchar(128) NOT NULL DEFAULT 'system',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE content_sources
  ADD COLUMN IF NOT EXISTS media_acquisition_mode varchar(16);

ALTER TABLE content_sources
  DROP CONSTRAINT IF EXISTS content_sources_media_acquisition_mode_check;
ALTER TABLE content_sources
  ADD CONSTRAINT content_sources_media_acquisition_mode_check
  CHECK (media_acquisition_mode IS NULL OR media_acquisition_mode IN ('automatic', 'manual'));

ALTER TABLE content_stage_requests
  DROP CONSTRAINT IF EXISTS content_stage_requests_state_check;
ALTER TABLE content_stage_requests
  ADD CONSTRAINT content_stage_requests_state_check CHECK (state IN (
    'awaiting_approval', 'blocked', 'queued', 'claimed', 'running',
    'verifying', 'verified', 'deferred', 'uncertain', 'reconciling',
    'failed', 'cancelled', 'superseded'
  ));

CREATE INDEX IF NOT EXISTS idx_content_stage_requests_manual_admission
  ON content_stage_requests (tenant_id, stage, state, created_at)
  WHERE state IN ('awaiting_approval', 'blocked');

-- Older manifests represented dependency waiting as queued. Make the new
-- state explicit without disturbing running/claimed effects.
UPDATE content_stage_requests AS child
SET state = 'blocked', not_before_at = NULL, updated_at = now()
WHERE child.state IN ('queued', 'deferred')
  AND (
    (child.stage IN ('news_story_classification', 'news_llm_metadata') AND EXISTS (
      SELECT 1 FROM content_stage_requests AS parent
      WHERE parent.tenant_id = child.tenant_id
        AND parent.content_item_id = child.content_item_id
        AND parent.processing_generation = child.processing_generation
        AND parent.stage = 'news_text_embedding'
        AND parent.state <> 'verified'
    ))
    OR (child.stage IN ('pods_transcript', 'pods_image_embedding') AND EXISTS (
      SELECT 1 FROM content_stage_requests AS parent
      WHERE parent.tenant_id = child.tenant_id
        AND parent.content_item_id = child.content_item_id
        AND parent.processing_generation = child.processing_generation
        AND parent.stage = 'pods_media_artifacts'
        AND parent.state <> 'verified'
    ))
    OR (child.stage IN ('pods_atomization', 'pods_caption_reembedding') AND EXISTS (
      SELECT 1 FROM content_stage_requests AS parent
      WHERE parent.tenant_id = child.tenant_id
        AND parent.content_item_id = child.content_item_id
        AND parent.processing_generation = child.processing_generation
        AND parent.stage = 'pods_transcript'
        AND parent.state <> 'verified'
    ))
    OR (child.stage = 'pods_llm_metadata' AND EXISTS (
      SELECT 1 FROM content_stage_requests AS parent
      WHERE parent.tenant_id = child.tenant_id
        AND parent.content_item_id = child.content_item_id
        AND parent.processing_generation = child.processing_generation
        AND parent.stage = 'pods_text_embedding'
        AND parent.state <> 'verified'
    ))
  );

INSERT INTO media_acquisition_configs (tenant_id, default_mode)
VALUES ('default', 'automatic')
ON CONFLICT (tenant_id) DO NOTHING;
