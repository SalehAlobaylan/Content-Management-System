-- Additive only. Apply before deploying draft-aware workers and Console.
ALTER TABLE atomization_generations ADD COLUMN plan_origin TEXT NOT NULL DEFAULT 'unavailable' CHECK (plan_origin IN ('unavailable','provider','contextual','manual'));
CREATE TABLE media_chapter_drafts (
 public_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 tenant_id VARCHAR(64) NOT NULL,
 parent_content_item_id UUID NOT NULL,
 revision BIGINT NOT NULL CHECK (revision > 0),
 base_processing_generation BIGINT NOT NULL,
 input_fingerprint TEXT NOT NULL,
 transcript_digest TEXT NOT NULL,
 policy_digest TEXT NOT NULL,
 plan JSONB NOT NULL CHECK (jsonb_typeof(plan)='array'),
 provenance TEXT NOT NULL DEFAULT 'manual',
 author TEXT NOT NULL,
 validation JSONB NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 applied_request_id UUID,
 applied_processing_generation BIGINT,
 application_key UUID,
 applied_at TIMESTAMPTZ,
 UNIQUE (tenant_id,parent_content_item_id,revision),
 UNIQUE (tenant_id,public_id),
 FOREIGN KEY (tenant_id,parent_content_item_id) REFERENCES content_items(tenant_id,public_id),
 FOREIGN KEY (tenant_id,applied_request_id) REFERENCES content_stage_requests(tenant_id,public_id),
 CHECK ((applied_request_id IS NULL) = (application_key IS NULL))
);
CREATE UNIQUE INDEX media_chapter_draft_application_key ON media_chapter_drafts(tenant_id,application_key) WHERE application_key IS NOT NULL;
CREATE UNIQUE INDEX media_chapter_draft_request ON media_chapter_drafts(tenant_id,applied_request_id) WHERE applied_request_id IS NOT NULL;
CREATE TABLE media_worker_capabilities (
 name TEXT PRIMARY KEY,
 version INTEGER NOT NULL,
 observed_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX media_journey_checkpoints ON content_stage_events(tenant_id,request_id,sequence DESC) WHERE event_type LIKE 'checkpoint:%';
