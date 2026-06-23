-- Speed up admin enrichment polling/count paths.
-- Canonical migrations only: do not mirror this under supabase/migrations.

CREATE INDEX IF NOT EXISTS idx_content_items_ready_media_missing_transcript
    ON content_items (type, created_at DESC)
    WHERE status = 'READY'
      AND transcript_id IS NULL
      AND type IN ('VIDEO', 'PODCAST');

CREATE INDEX IF NOT EXISTS idx_content_items_ready_missing_embedding
    ON content_items (type, created_at DESC)
    WHERE status = 'READY'
      AND embedding IS NULL;

CREATE INDEX IF NOT EXISTS idx_content_items_ready_missing_sparse
    ON content_items (type, created_at DESC)
    WHERE status = 'READY'
      AND embedding IS NOT NULL
      AND embedding_sparse IS NULL;

CREATE INDEX IF NOT EXISTS idx_content_items_ready_missing_image_embedding
    ON content_items (type, created_at DESC)
    WHERE status = 'READY'
      AND thumbnail_url IS NOT NULL
      AND image_embedding IS NULL;

CREATE INDEX IF NOT EXISTS idx_content_items_admin_ready_type_created
    ON content_items (status, type, created_at DESC)
    WHERE status != 'ARCHIVED';

CREATE INDEX IF NOT EXISTS idx_news_circulation_policies_enabled_automation
    ON news_circulation_policies (tenant_id)
    WHERE automation_enabled = true OR autopilot_enabled = true;
