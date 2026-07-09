-- Rename the existing event-cluster "topics" model to "stories".
-- This frees "topics" for the canonical user preference/catalog vocabulary.

DO $$
BEGIN
    IF to_regclass('public.topics') IS NOT NULL
       AND to_regclass('public.stories') IS NULL THEN
        ALTER TABLE topics RENAME TO stories;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'topics_pkey'
    ) AND NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'stories_pkey'
    ) THEN
        ALTER TABLE stories RENAME CONSTRAINT topics_pkey TO stories_pkey;
    END IF;
END $$;

DO $$
BEGIN
    IF to_regclass('public.topics_id_seq') IS NOT NULL
       AND to_regclass('public.stories_id_seq') IS NULL THEN
        ALTER SEQUENCE topics_id_seq RENAME TO stories_id_seq;
        ALTER TABLE stories ALTER COLUMN id SET DEFAULT nextval('stories_id_seq'::regclass);
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'content_items'
          AND column_name = 'topic_id'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'content_items'
          AND column_name = 'story_id'
    ) THEN
        ALTER TABLE content_items RENAME COLUMN topic_id TO story_id;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'rss_feeds'
          AND column_name = 'topic_id'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'rss_feeds'
          AND column_name = 'story_id'
    ) THEN
        ALTER TABLE rss_feeds RENAME COLUMN topic_id TO story_id;
    END IF;
END $$;

DO $$
BEGIN
    IF to_regclass('public.idx_topics_public_id') IS NOT NULL
       AND to_regclass('public.idx_stories_public_id') IS NULL THEN
        ALTER INDEX idx_topics_public_id RENAME TO idx_stories_public_id;
    END IF;
    IF to_regclass('public.idx_topics_tenant_label') IS NOT NULL
       AND to_regclass('public.idx_stories_tenant_label') IS NULL THEN
        ALTER INDEX idx_topics_tenant_label RENAME TO idx_stories_tenant_label;
    END IF;
    IF to_regclass('public.idx_topics_tenant') IS NOT NULL
       AND to_regclass('public.idx_stories_tenant') IS NULL THEN
        ALTER INDEX idx_topics_tenant RENAME TO idx_stories_tenant;
    END IF;
    IF to_regclass('public.idx_topics_last_member_at') IS NOT NULL
       AND to_regclass('public.idx_stories_last_member_at') IS NULL THEN
        ALTER INDEX idx_topics_last_member_at RENAME TO idx_stories_last_member_at;
    END IF;
    IF to_regclass('public.idx_topics_summary_built_at') IS NOT NULL
       AND to_regclass('public.idx_stories_summary_built_at') IS NULL THEN
        ALTER INDEX idx_topics_summary_built_at RENAME TO idx_stories_summary_built_at;
    END IF;
    IF to_regclass('public.topics_embedding_idx') IS NOT NULL
       AND to_regclass('public.stories_embedding_idx') IS NULL THEN
        ALTER INDEX topics_embedding_idx RENAME TO stories_embedding_idx;
    END IF;
    IF to_regclass('public.idx_content_items_topic_id') IS NOT NULL
       AND to_regclass('public.idx_content_items_story_id') IS NULL THEN
        ALTER INDEX idx_content_items_topic_id RENAME TO idx_content_items_story_id;
    END IF;
    IF to_regclass('public.idx_rss_feeds_topic_id') IS NOT NULL
       AND to_regclass('public.idx_rss_feeds_story_id') IS NULL THEN
        ALTER INDEX idx_rss_feeds_topic_id RENAME TO idx_rss_feeds_story_id;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'content_items_topic_id_fkey'
    ) AND NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'content_items_story_id_fkey'
    ) THEN
        ALTER TABLE content_items
            RENAME CONSTRAINT content_items_topic_id_fkey TO content_items_story_id_fkey;
    END IF;

    IF EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'rss_feeds_topic_id_fkey'
    ) AND NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'rss_feeds_story_id_fkey'
    ) THEN
        ALTER TABLE rss_feeds
            RENAME CONSTRAINT rss_feeds_topic_id_fkey TO rss_feeds_story_id_fkey;
    END IF;
END $$;
