-- Topic-centric news management at scale.
--
-- The admin news manager filters and bulk-rotates content by topic using
-- PostgreSQL array membership (`? = ANY(topic_tags)`) and aggregates topics with
-- `UNNEST(topic_tags) GROUP BY tag`. A GIN index on the text[] column keeps the
-- membership filters (topic detail list + bulk-by-topic over thousands of rows)
-- index-backed instead of sequential scans.
--
-- `= ANY(col)` is planned as `col @> ARRAY[value]`, which a GIN index on the
-- array serves directly.

CREATE INDEX IF NOT EXISTS idx_content_items_topic_tags
    ON content_items USING GIN (topic_tags);
