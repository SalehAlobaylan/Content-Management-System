-- Reconcile databases that were interrupted during the story/topic vocabulary
-- split, then make all preference state explicitly tenant scoped.

DO $$
BEGIN
    -- A previous development-only recovery could archive legacy event topics
    -- after creating stories. Preserve every historical content/feed reference.
    IF to_regclass('public.legacy_event_topics') IS NOT NULL
       AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'content_items' AND column_name = 'topic_id')
       AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'content_items' AND column_name = 'story_id') THEN
        UPDATE content_items ci
        SET story_id = ci.topic_id
        WHERE ci.story_id IS NULL
          AND ci.topic_id IS NOT NULL
          AND EXISTS (SELECT 1 FROM stories s WHERE s.public_id = ci.topic_id);
    END IF;

    IF to_regclass('public.legacy_event_topics') IS NOT NULL
       AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'rss_feeds' AND column_name = 'topic_id')
       AND EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'rss_feeds' AND column_name = 'story_id') THEN
        UPDATE rss_feeds rf
        SET story_id = rf.topic_id
        WHERE rf.story_id IS NULL
          AND rf.topic_id IS NOT NULL
          AND EXISTS (SELECT 1 FROM stories s WHERE s.public_id = rf.topic_id);
    END IF;
END $$;

ALTER TABLE user_topic_prefs ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64);
ALTER TABLE user_topic_affinity ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64);
ALTER TABLE user_category_affinity ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(64);

UPDATE user_topic_prefs SET tenant_id = 'default' WHERE tenant_id IS NULL OR tenant_id = '';
UPDATE user_topic_affinity SET tenant_id = 'default' WHERE tenant_id IS NULL OR tenant_id = '';
UPDATE user_category_affinity SET tenant_id = 'default' WHERE tenant_id IS NULL OR tenant_id = '';

ALTER TABLE user_topic_prefs ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE user_topic_affinity ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE user_category_affinity ALTER COLUMN tenant_id SET NOT NULL;

ALTER TABLE user_topic_prefs DROP CONSTRAINT IF EXISTS user_topic_prefs_pkey;
ALTER TABLE user_topic_affinity DROP CONSTRAINT IF EXISTS user_topic_affinity_pkey;
ALTER TABLE user_category_affinity DROP CONSTRAINT IF EXISTS user_category_affinity_pkey;
ALTER TABLE user_topic_prefs ADD PRIMARY KEY (tenant_id, user_id, topic_id);
ALTER TABLE user_topic_affinity ADD PRIMARY KEY (tenant_id, user_id, topic_id);
ALTER TABLE user_category_affinity ADD PRIMARY KEY (tenant_id, user_id, category_slug);

CREATE INDEX IF NOT EXISTS idx_user_topic_prefs_tenant_user ON user_topic_prefs (tenant_id, user_id);
CREATE INDEX IF NOT EXISTS idx_user_topic_affinity_tenant_user ON user_topic_affinity (tenant_id, user_id);
CREATE INDEX IF NOT EXISTS idx_user_category_affinity_tenant_user ON user_category_affinity (tenant_id, user_id);

-- A usable initial picker catalog. Centroids are hydrated by the CMS topic
-- heartbeat through Enrichment, then the full catalog remap is scheduled.
INSERT INTO topics (tenant_id, slug, label_ar, label_en, category_slug, active, featured, created_from)
VALUES
    ('default', 'politics', 'سياسة', 'Politics', 'politics', TRUE, TRUE, 'seeded'),
    ('default', 'economy', 'اقتصاد', 'Economy', 'economy', TRUE, TRUE, 'seeded'),
    ('default', 'sports', 'رياضة', 'Sports', 'sports', TRUE, TRUE, 'seeded'),
    ('default', 'technology', 'تقنية', 'Technology', 'technology', TRUE, TRUE, 'seeded'),
    ('default', 'culture', 'ثقافة', 'Culture', 'culture', TRUE, TRUE, 'seeded'),
    ('default', 'society', 'مجتمع', 'Society', 'society', TRUE, TRUE, 'seeded'),
    ('default', 'religion', 'دين', 'Religion', 'religion', TRUE, TRUE, 'seeded'),
    ('default', 'health', 'صحة', 'Health', 'health', TRUE, TRUE, 'seeded'),
    ('default', 'science', 'علوم', 'Science', 'science', TRUE, TRUE, 'seeded'),
    ('default', 'environment', 'بيئة', 'Environment', 'environment', TRUE, TRUE, 'seeded'),
    ('default', 'general', 'عام', 'General', 'general', TRUE, TRUE, 'seeded')
ON CONFLICT (tenant_id, slug) DO UPDATE
SET active = TRUE, featured = TRUE, updated_at = now();
