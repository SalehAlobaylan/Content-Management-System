DROP INDEX IF EXISTS idx_content_items_author_id;

ALTER TABLE content_items
DROP COLUMN IF EXISTS author_id;
