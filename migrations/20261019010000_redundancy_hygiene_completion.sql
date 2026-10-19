-- Redundancy-owned tables only. Explicit migration; startup never repairs data.
-- Earlier deployed schemas predate these fields despite the legacy ledger entry.
ALTER TABLE redundancy_policies ADD COLUMN IF NOT EXISTS emit_circulation_recs BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE redundancy_policies ADD COLUMN IF NOT EXISTS confirm_rules JSONB NOT NULL DEFAULT '{}';
ALTER TABLE redundancy_runs ADD COLUMN IF NOT EXISTS error_class VARCHAR(48);
ALTER TABLE redundancy_policies ADD COLUMN IF NOT EXISTS lane_settings JSONB NOT NULL DEFAULT '{}';
ALTER TABLE redundancy_policies ADD COLUMN IF NOT EXISTS scan_state JSONB NOT NULL DEFAULT '{}';
ALTER TABLE redundancy_fingerprints ADD COLUMN IF NOT EXISTS signature JSONB NOT NULL DEFAULT '[]';
-- Historical Go models inserted zero UUIDs instead of using database defaults.
UPDATE redundancy_runs SET public_id=gen_random_uuid() WHERE public_id='00000000-0000-0000-0000-000000000000';
UPDATE redundancy_pairs SET public_id=gen_random_uuid() WHERE public_id='00000000-0000-0000-0000-000000000000';
UPDATE redundancy_families SET public_id=gen_random_uuid() WHERE public_id='00000000-0000-0000-0000-000000000000';
UPDATE redundancy_actions SET public_id=gen_random_uuid() WHERE public_id='00000000-0000-0000-0000-000000000000';
UPDATE redundancy_actions SET idempotency_key=NULL WHERE idempotency_key='';
UPDATE redundancy_family_members m SET role=CASE WHEN m.content_item_id=f.canonical_content_item_id THEN 'canonical' ELSE 'redundant' END
FROM redundancy_families f WHERE f.id=m.family_id AND f.status='active' AND m.ended_at IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_redundancy_one_canonical ON redundancy_family_members(family_id) WHERE ended_at IS NULL AND role='canonical';
CREATE OR REPLACE FUNCTION redundancy_normalized_title(value TEXT) RETURNS TEXT LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS $$
 SELECT trim(regexp_replace(regexp_replace(lower(coalesce(value,'')), '\[[^]]*\]|\([^)]*\)', ' ', 'g'), '[^[:alnum:][:space:]]', ' ', 'g'));
$$;
-- Index-only additions; no rewrite/backfill of the content library.
CREATE INDEX IF NOT EXISTS idx_redundancy_duration_block ON content_items(tenant_id,duration_sec,public_id) WHERE parent_content_item_id IS NULL AND type IN ('VIDEO','PODCAST');
CREATE INDEX IF NOT EXISTS idx_redundancy_title_block ON content_items USING gin (redundancy_normalized_title(title) gin_trgm_ops) WHERE parent_content_item_id IS NULL AND type IN ('VIDEO','PODCAST');
CREATE INDEX IF NOT EXISTS idx_redundancy_transcript_latest ON transcripts(content_item_id,created_at DESC,id DESC);
