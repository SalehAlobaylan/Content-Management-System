-- Retention cockpit/run preview indexes.
-- These are narrow partial indexes: they cover only the state predicates used
-- by the bounded News preview and pending-work guard, without duplicating the
-- large content payloads or vectors in the index.

CREATE INDEX IF NOT EXISTS idx_stories_retention_window
    ON stories (tenant_id, last_member_at, public_id)
    WHERE news_retention_state = 'full' OR news_retention_state IS NULL;

CREATE INDEX IF NOT EXISTS idx_content_items_story_retention_pending
    ON content_items (tenant_id, story_id)
    WHERE status IN ('PENDING', 'PROCESSING');
