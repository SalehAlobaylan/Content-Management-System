-- Media Atomization: parent/child feed units, chapter review metadata, and
-- format-agnostic playback renditions.

ALTER TABLE content_items
    ADD COLUMN IF NOT EXISTS parent_content_item_id uuid,
    ADD COLUMN IF NOT EXISTS is_feed_unit boolean NOT NULL DEFAULT true,
    ADD COLUMN IF NOT EXISTS feed_visibility varchar(24) NOT NULL DEFAULT 'visible',
    ADD COLUMN IF NOT EXISTS chapter_index integer,
    ADD COLUMN IF NOT EXISTS chapter_start_ms integer,
    ADD COLUMN IF NOT EXISTS chapter_end_ms integer,
    ADD COLUMN IF NOT EXISTS chapter_confidence double precision,
    ADD COLUMN IF NOT EXISTS chaptering_status varchar(32),
    ADD COLUMN IF NOT EXISTS duration_bucket varchar(8),
    ADD COLUMN IF NOT EXISTS source_episode_id varchar(255),
    ADD COLUMN IF NOT EXISTS playback_url text,
    ADD COLUMN IF NOT EXISTS playback_type varchar(16),
    ADD COLUMN IF NOT EXISTS fallback_playback_url text,
    ADD COLUMN IF NOT EXISTS has_video boolean,
    ADD COLUMN IF NOT EXISTS media_renditions jsonb;

CREATE INDEX IF NOT EXISTS idx_content_items_parent ON content_items (parent_content_item_id);
CREATE INDEX IF NOT EXISTS idx_content_items_feed_unit ON content_items (is_feed_unit);
CREATE INDEX IF NOT EXISTS idx_content_items_feed_visibility ON content_items (feed_visibility);
CREATE INDEX IF NOT EXISTS idx_content_items_chaptering_status ON content_items (chaptering_status);
CREATE INDEX IF NOT EXISTS idx_content_items_duration_bucket ON content_items (duration_bucket);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'content_items_parent_content_item_id_fkey'
          AND conrelid = 'content_items'::regclass
    ) THEN
        ALTER TABLE content_items
            ADD CONSTRAINT content_items_parent_content_item_id_fkey
            FOREIGN KEY (parent_content_item_id) REFERENCES content_items (public_id) ON DELETE SET NULL;
    END IF;
END $$;

ALTER TABLE chapters
    ADD COLUMN IF NOT EXISTS end_ms integer,
    ADD COLUMN IF NOT EXISTS status varchar(24) NOT NULL DEFAULT 'draft',
    ADD COLUMN IF NOT EXISTS confidence double precision,
    ADD COLUMN IF NOT EXISTS context_label text,
    ADD COLUMN IF NOT EXISTS boundary_reason text,
    ADD COLUMN IF NOT EXISTS standalone_score double precision,
    ADD COLUMN IF NOT EXISTS contains_sponsor_intro boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS needs_review_reason text,
    ADD COLUMN IF NOT EXISTS duration_bucket varchar(8),
    ADD COLUMN IF NOT EXISTS child_content_item_id uuid;

CREATE INDEX IF NOT EXISTS idx_chapters_status ON chapters (status);
CREATE INDEX IF NOT EXISTS idx_chapters_child_content ON chapters (child_content_item_id);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'chapters_child_content_item_id_fkey'
          AND conrelid = 'chapters'::regclass
    ) THEN
        ALTER TABLE chapters
            ADD CONSTRAINT chapters_child_content_item_id_fkey
            FOREIGN KEY (child_content_item_id) REFERENCES content_items (public_id) ON DELETE SET NULL;
    END IF;
END $$;

-- Existing media rows predate atomization. Keep short/unknown-duration rows
-- playable during migration, but never leave known long-form parents visible
-- as For You feed units; they must be atomized into child chapters first.
UPDATE content_items
SET playback_url = COALESCE(playback_url, media_url),
    playback_type = COALESCE(playback_type, 'mp4'),
    fallback_playback_url = COALESCE(fallback_playback_url, media_url),
    has_video = COALESCE(has_video, true)
WHERE type IN ('VIDEO', 'PODCAST')
  AND media_url IS NOT NULL
  AND media_url <> '';

UPDATE content_items
SET is_feed_unit = false,
    feed_visibility = 'hidden',
    chaptering_status = COALESCE(chaptering_status, 'waiting_transcript')
WHERE type IN ('VIDEO', 'PODCAST')
  AND parent_content_item_id IS NULL
  AND duration_sec IS NOT NULL
  AND duration_sec > 2400;
