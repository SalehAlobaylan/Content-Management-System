ALTER TABLE content_items
    ADD COLUMN IF NOT EXISTS content_language VARCHAR(16);

CREATE INDEX IF NOT EXISTS idx_content_items_content_language
    ON content_items (tenant_id, content_language)
    WHERE content_language IS NOT NULL;
