-- Phase 13 Slice 1: NEWS-first data model.
--
-- content_items.type becomes the KIND (NEWS / VIDEO / PODCAST). NEWS items
-- carry a `format` sub-classification (ARTICLE / TWEET / COMMENT) describing the
-- original content shape. Legacy ARTICLE/TWEET/COMMENT rows migrate to
-- type='NEWS' with `format` preserving the old type. VIDEO/PODCAST are
-- unchanged (format stays NULL).
--
-- Destructive in the sense that it rewrites `type` for existing news rows —
-- apply against prod only with explicit approval.

-- 1. Add the format sub-classification column.
ALTER TABLE content_items ADD COLUMN IF NOT EXISTS format varchar(20);

-- 2. Migrate legacy news kinds → NEWS + format.
UPDATE content_items
SET    format = type,
       type   = 'NEWS'
WHERE  type IN ('ARTICLE', 'TWEET', 'COMMENT');

-- 3. Composite index for the news-feed queries that filter by (type, format).
CREATE INDEX IF NOT EXISTS idx_content_items_type_format
    ON content_items (type, format);
