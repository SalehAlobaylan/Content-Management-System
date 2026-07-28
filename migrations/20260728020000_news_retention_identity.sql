-- News compact-identity contract. Columns default existing/new NEWS rows to
-- full fidelity; no content is compacted by this migration.

ALTER TABLE content_items
    ADD COLUMN IF NOT EXISTS news_retention_state VARCHAR(24),
    ADD COLUMN IF NOT EXISTS news_feed_role VARCHAR(24),
    ADD COLUMN IF NOT EXISTS news_representative_ordinal SMALLINT,
    ADD COLUMN IF NOT EXISTS news_compacted_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS news_retention_expires_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS news_compaction_hash CHAR(64);

UPDATE content_items
SET news_retention_state = 'full',
    news_feed_role = 'full_member'
WHERE type = 'NEWS'
  AND (news_retention_state IS NULL OR news_feed_role IS NULL);

ALTER TABLE content_items
    DROP CONSTRAINT IF EXISTS chk_content_news_retention_state,
    ADD CONSTRAINT chk_content_news_retention_state CHECK (
        (type = 'NEWS'
            AND news_retention_state IN ('full', 'compact', 'archive_anchor')
            AND news_feed_role IN ('full_member', 'lead', 'representative', 'protected_only'))
        OR
        (type <> 'NEWS'
            AND news_retention_state IS NULL
            AND news_feed_role IS NULL
            AND news_representative_ordinal IS NULL
            AND news_compacted_at IS NULL
            AND news_retention_expires_at IS NULL
            AND news_compaction_hash IS NULL)
    ),
    DROP CONSTRAINT IF EXISTS chk_content_news_representative_ordinal,
    ADD CONSTRAINT chk_content_news_representative_ordinal CHECK (
        (news_feed_role = 'representative' AND news_representative_ordinal BETWEEN 1 AND 3)
        OR (news_feed_role <> 'representative' AND news_representative_ordinal IS NULL)
        OR news_feed_role IS NULL
    ),
    DROP CONSTRAINT IF EXISTS chk_content_news_protected_state,
    ADD CONSTRAINT chk_content_news_protected_state CHECK (
        news_feed_role <> 'protected_only'
        OR news_retention_state IN ('compact', 'archive_anchor')
    );

CREATE UNIQUE INDEX IF NOT EXISTS idx_content_items_one_retained_lead
    ON content_items(tenant_id, story_id)
    WHERE news_feed_role = 'lead';
CREATE UNIQUE INDEX IF NOT EXISTS idx_content_items_representative_order
    ON content_items(tenant_id, story_id, news_representative_ordinal)
    WHERE news_feed_role = 'representative';
CREATE INDEX IF NOT EXISTS idx_content_items_news_retention_candidates
    ON content_items(tenant_id, story_id, published_at)
    WHERE type = 'NEWS' AND news_retention_state = 'full';

ALTER TABLE stories
    ADD COLUMN IF NOT EXISTS news_retention_state VARCHAR(24) NOT NULL DEFAULT 'full',
    ADD COLUMN IF NOT EXISTS news_compacted_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS retained_lead_content_id UUID,
    ADD COLUMN IF NOT EXISTS original_member_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS retained_member_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS original_source_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS retained_source_count INTEGER NOT NULL DEFAULT 0;

UPDATE stories story
SET original_member_count = counts.member_count,
    retained_member_count = counts.member_count,
    original_source_count = counts.source_count,
    retained_source_count = counts.source_count
FROM (
    SELECT story_id,
           COUNT(*)::integer AS member_count,
           COUNT(DISTINCT COALESCE(NULLIF(source_name, ''), source::text))::integer AS source_count
    FROM content_items
    WHERE type = 'NEWS' AND story_id IS NOT NULL
    GROUP BY story_id
) counts
WHERE story.public_id = counts.story_id
  AND story.news_retention_state = 'full';

ALTER TABLE stories
    DROP CONSTRAINT IF EXISTS chk_stories_news_retention_state,
    ADD CONSTRAINT chk_stories_news_retention_state CHECK (
        news_retention_state IN ('full', 'compact', 'archive_anchor')
    ),
    DROP CONSTRAINT IF EXISTS chk_stories_retention_counts,
    ADD CONSTRAINT chk_stories_retention_counts CHECK (
        original_member_count >= 0
        AND retained_member_count >= 0
        AND retained_member_count <= original_member_count
        AND original_source_count >= 0
        AND retained_source_count >= 0
        AND retained_source_count <= original_source_count
    ),
    DROP CONSTRAINT IF EXISTS chk_stories_compact_lead,
    ADD CONSTRAINT chk_stories_compact_lead CHECK (
        news_retention_state = 'full' OR retained_lead_content_id IS NOT NULL
    ),
    DROP CONSTRAINT IF EXISTS fk_stories_retained_lead,
    ADD CONSTRAINT fk_stories_retained_lead
        FOREIGN KEY (retained_lead_content_id)
        REFERENCES content_items(public_id)
        ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED;

CREATE OR REPLACE FUNCTION enforce_story_retained_lead()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    lead_tenant VARCHAR(64);
    lead_story UUID;
    lead_role VARCHAR(24);
    lead_state VARCHAR(24);
BEGIN
    IF NEW.news_retention_state = 'full' THEN
        RETURN NEW;
    END IF;

    SELECT tenant_id, story_id, news_feed_role, news_retention_state
    INTO lead_tenant, lead_story, lead_role, lead_state
    FROM content_items
    WHERE public_id = NEW.retained_lead_content_id;

    IF lead_tenant IS DISTINCT FROM NEW.tenant_id
       OR lead_story IS DISTINCT FROM NEW.public_id
       OR lead_role <> 'lead'
       OR lead_state NOT IN ('compact', 'archive_anchor') THEN
        RAISE EXCEPTION 'retained lead must belong to the same tenant/story and be a compact/archive lead';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_enforce_story_retained_lead ON stories;
CREATE CONSTRAINT TRIGGER trg_enforce_story_retained_lead
AFTER INSERT OR UPDATE OF news_retention_state, retained_lead_content_id, tenant_id
ON stories
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION enforce_story_retained_lead();
