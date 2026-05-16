ALTER TABLE content_items
ADD COLUMN IF NOT EXISTS author_id UUID;

CREATE INDEX IF NOT EXISTS idx_content_items_author_id ON content_items(author_id);
