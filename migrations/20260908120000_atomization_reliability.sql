-- Apply with Pods claim admission paused. No historical ownership is inferred.
ALTER TABLE media_artifact_manifests ADD COLUMN IF NOT EXISTS unit_fence_token UUID;
ALTER TABLE media_artifact_manifests ADD COLUMN IF NOT EXISTS outer_fence_token UUID;
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'atomization_manifest_nested_authority') THEN
    ALTER TABLE media_artifact_manifests ADD CONSTRAINT atomization_manifest_nested_authority
      CHECK (atomization_chapter_unit_id IS NULL OR
        (atomization_generation_id IS NOT NULL AND parent_content_item_id IS NOT NULL
         AND attempt_id IS NOT NULL AND unit_fence_token IS NOT NULL AND outer_fence_token IS NOT NULL)) NOT VALID;
  END IF;
END $$;
ALTER TABLE atomization_generations ADD COLUMN IF NOT EXISTS content_stage_request_id UUID REFERENCES content_stage_requests(public_id);
ALTER TABLE atomization_generations ADD COLUMN IF NOT EXISTS processing_generation BIGINT NOT NULL DEFAULT 0;
ALTER TABLE atomization_generations ADD COLUMN IF NOT EXISTS plan JSONB NOT NULL DEFAULT '[]'::jsonb;
CREATE UNIQUE INDEX IF NOT EXISTS atomization_generation_stage_identity
 ON atomization_generations(tenant_id, content_stage_request_id)
 WHERE content_stage_request_id IS NOT NULL;
ALTER TABLE atomization_chapter_units ADD COLUMN IF NOT EXISTS effect_started_at TIMESTAMPTZ;
ALTER TABLE transcription_segment_units ADD COLUMN IF NOT EXISTS effect_started_at TIMESTAMPTZ;
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'nested_manifest_fence_shape') THEN
    ALTER TABLE media_artifact_manifests ADD CONSTRAINT nested_manifest_fence_shape
      CHECK (transcription_segment_unit_id IS NULL OR
        (transcription_generation_id IS NOT NULL AND content_item_id IS NOT NULL
         AND attempt_id IS NOT NULL AND unit_fence_token IS NOT NULL AND outer_fence_token IS NOT NULL)) NOT VALID;
  END IF;
END $$;
CREATE TABLE IF NOT EXISTS pods_episode_execution_slot (
 singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK(singleton),
 tenant_id VARCHAR(64), root_content_item_id UUID, processing_generation BIGINT NOT NULL DEFAULT 0,
 epoch BIGINT NOT NULL DEFAULT 0, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 CHECK ((tenant_id IS NULL) = (root_content_item_id IS NULL))
);
INSERT INTO pods_episode_execution_slot(singleton) VALUES(TRUE) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS pods_episode_dispositions (
 tenant_id VARCHAR(64) NOT NULL, root_content_item_id UUID NOT NULL,
 processing_generation BIGINT NOT NULL, disposition VARCHAR(32) NOT NULL,
 reason TEXT NOT NULL DEFAULT '', updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 PRIMARY KEY(tenant_id,root_content_item_id,processing_generation),
 CHECK(disposition IN ('active','parked_transcript','parked_review','parked_failed','reconciling','published','cancelled'))
);
