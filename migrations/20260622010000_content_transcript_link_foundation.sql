-- The transcript relationship previously existed only through GORM's dev
-- schema inference.  It is required by the following canonical index.
ALTER TABLE content_items
    ADD COLUMN IF NOT EXISTS transcript_id uuid;

CREATE INDEX IF NOT EXISTS idx_content_items_transcript_id
    ON content_items (transcript_id);
