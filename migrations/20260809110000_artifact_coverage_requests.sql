-- M4: CMS-owned, tenant-fenced artifact recovery commands.
CREATE TABLE IF NOT EXISTS artifact_coverage_requests (
 id bigserial PRIMARY KEY, public_id uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE, tenant_id varchar(64) NOT NULL, content_item_id uuid NOT NULL, item_updated_at timestamptz NOT NULL,
 artifact varchar(48) NOT NULL CHECK (artifact IN ('transcript','image_embedding','text_embedding','llm_metadata')), owner varchar(16) NOT NULL CHECK (owner IN ('media','enrichment')),
 state varchar(24) NOT NULL CHECK (state IN ('queued','claimed','running','verifying','succeeded','failed','cancelled','uncertain')),
 evidence_digest varchar(128) NOT NULL, input_digest varchar(128) NOT NULL, idempotency_key varchar(255) NOT NULL, claim_token uuid, fence_token uuid, claim_expires_at timestamptz,
 cancellation_requested_at timestamptz, accepted_at timestamptz, verified_at timestamptz, terminal_proof jsonb NOT NULL DEFAULT '{}'::jsonb, created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz,
 UNIQUE(tenant_id,idempotency_key));
CREATE UNIQUE INDEX IF NOT EXISTS uq_artifact_coverage_active ON artifact_coverage_requests(tenant_id,content_item_id,item_updated_at,artifact) WHERE state IN ('queued','claimed','running','verifying','uncertain');
CREATE INDEX IF NOT EXISTS idx_artifact_coverage_claim ON artifact_coverage_requests(owner,state,claim_expires_at,created_at);
CREATE TABLE IF NOT EXISTS artifact_coverage_events (id bigserial PRIMARY KEY, public_id uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE, tenant_id varchar(64) NOT NULL, request_id uuid NOT NULL, sequence bigint NOT NULL, event_type varchar(48) NOT NULL, payload jsonb NOT NULL DEFAULT '{}'::jsonb, occurred_at timestamptz NOT NULL DEFAULT now(), UNIQUE(tenant_id,request_id,sequence));
CREATE OR REPLACE FUNCTION reject_artifact_coverage_event_mutation() RETURNS trigger AS $$ BEGIN RAISE EXCEPTION 'artifact coverage events are append-only'; END; $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS artifact_coverage_events_append_only ON artifact_coverage_events;
CREATE TRIGGER artifact_coverage_events_append_only BEFORE UPDATE OR DELETE ON artifact_coverage_events FOR EACH ROW EXECUTE FUNCTION reject_artifact_coverage_event_mutation();
