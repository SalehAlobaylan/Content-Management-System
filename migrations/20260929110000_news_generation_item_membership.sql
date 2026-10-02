-- News feed generations must fence content identities as well as stories.
-- Story-only membership allows an item assigned to an active story to leak into
-- the active view even when the item itself belongs to a shadow build.
-- wahb:large-table-backfill: operator-maintenance
-- The existing-generation backfill joins content_items to story memberships;
-- schedule and observe it as an explicit database migration.

ALTER TABLE feed_generation_memberships
    DROP CONSTRAINT IF EXISTS feed_generation_memberships_member_type_check;
ALTER TABLE feed_generation_memberships
    ADD CONSTRAINT feed_generation_memberships_member_type_check
    CHECK (member_type IN ('story', 'feed_unit', 'news_item'));

CREATE INDEX IF NOT EXISTS idx_feed_generation_news_item
    ON feed_generation_memberships(generation_id, member_id)
    WHERE member_type = 'news_item';

-- Backfill the exact item set for every already materialized News generation.
-- The active generation remains behaviorally identical to its story membership
-- while future candidate generations can contain a narrower item population.
INSERT INTO feed_generation_memberships (generation_id, member_type, member_id)
SELECT story_membership.generation_id, 'news_item', content.public_id
FROM feed_generation_memberships story_membership
JOIN feed_generations generation
  ON generation.public_id = story_membership.generation_id
 AND generation.lane = 'news'
JOIN content_items content
  ON content.tenant_id = generation.tenant_id
 AND content.story_id = story_membership.member_id
WHERE story_membership.member_type = 'story'
  AND content.type = 'NEWS'
  AND content.status = 'READY'
  AND (COALESCE(content.news_retention_state, 'full') = 'full'
       OR content.news_feed_role IN ('lead', 'representative'))
ON CONFLICT DO NOTHING;

CREATE OR REPLACE FUNCTION enforce_content_reset_lifecycle_membership_fence()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    generation_tenant TEXT;
    generation_lane TEXT;
    lifecycle_lane TEXT;
    membership_type TEXT;
    membership_id UUID;
    generation_id UUID;
    lock_key TEXT;
    item RECORD;
    item_found BOOLEAN := FALSE;
BEGIN
    IF TG_OP = 'UPDATE' AND (
        OLD.generation_id IS DISTINCT FROM NEW.generation_id
        OR OLD.member_type IS DISTINCT FROM NEW.member_type
        OR OLD.member_id IS DISTINCT FROM NEW.member_id
    ) THEN
        RAISE EXCEPTION 'feed generation membership identity is immutable; delete and reinsert through the feed owner'
            USING ERRCODE = 'P0001';
    END IF;

    IF TG_OP = 'DELETE' THEN
        membership_type := OLD.member_type;
        membership_id := OLD.member_id;
        generation_id := OLD.generation_id;
    ELSE
        membership_type := NEW.member_type;
        membership_id := NEW.member_id;
        generation_id := NEW.generation_id;
    END IF;

    SELECT generation.tenant_id,
           generation.lane,
           CASE WHEN generation.lane = 'media' THEN 'pods' ELSE 'news' END
      INTO generation_tenant, generation_lane, lifecycle_lane
      FROM feed_generations generation
     WHERE generation.public_id = generation_id;
    IF NOT FOUND THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;

    IF membership_type IN ('feed_unit', 'news_item') THEN
        SELECT content.public_id, content.tenant_id, content.content_source_id, content.type
          INTO item
          FROM content_items content
         WHERE content.tenant_id = generation_tenant AND content.public_id = membership_id;
        IF NOT FOUND THEN
            IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
            IF membership_type = 'news_item' THEN
                RAISE EXCEPTION 'News item membership requires an existing tenant content item'
                    USING ERRCODE = 'P0001';
            END IF;
            RETURN NEW;
        END IF;
        IF membership_type = 'news_item'
           AND (generation_lane <> 'news' OR item.type <> 'NEWS') THEN
            RAISE EXCEPTION 'News item membership must reference News content in a News generation'
                USING ERRCODE = 'P0001';
        END IF;
        IF membership_type = 'feed_unit'
           AND (generation_lane <> 'media' OR item.type NOT IN ('VIDEO', 'PODCAST')) THEN
            RAISE EXCEPTION 'feed unit membership must reference Pods content in a media generation'
                USING ERRCODE = 'P0001';
        END IF;
        PERFORM assert_content_reset_item_allowed(
            item.tenant_id,
            content_reset_lifecycle_lane(item.type),
            item.content_source_id,
            item.public_id,
            'feed_membership'
        );
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;

    IF membership_type <> 'story' THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;

    IF generation_lane <> 'news' THEN
        RAISE EXCEPTION 'story membership must belong to a News generation'
            USING ERRCODE = 'P0001';
    END IF;

    FOR lock_key IN
        SELECT DISTINCT scopes.lock_key
        FROM content_items content
        CROSS JOIN LATERAL content_reset_lifecycle_item_lock_keys(
            content.tenant_id,
            content_reset_lifecycle_lane(content.type),
            content.content_source_id,
            content.public_id
        ) AS scopes(lock_key)
        WHERE content.tenant_id = generation_tenant
          AND content.story_id = membership_id
          AND content.type = 'NEWS'
        ORDER BY scopes.lock_key
    LOOP
        PERFORM pg_advisory_xact_lock_shared(hashtextextended(lock_key, 0));
    END LOOP;

    FOR item IN
        SELECT content.public_id, content.tenant_id, content.content_source_id, content.type
          FROM content_items content
         WHERE content.tenant_id = generation_tenant
           AND content.story_id = membership_id
           AND content.type = 'NEWS'
         ORDER BY content.public_id
    LOOP
        item_found := TRUE;
        PERFORM assert_content_reset_item_allowed(
            item.tenant_id,
            content_reset_lifecycle_lane(item.type),
            item.content_source_id,
            item.public_id,
            'feed_membership'
        );
    END LOOP;
    IF NOT item_found THEN
        PERFORM assert_content_reset_lane_membership_allowed(generation_tenant, lifecycle_lane);
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$;

-- A News snapshot is also bound to its active feed generation. Head switches
-- invalidate every cached window before a request can serve an old namespace.
CREATE OR REPLACE FUNCTION invalidate_news_snapshot_on_feed_head_change()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    active_changed BOOLEAN := FALSE;
    window_name TEXT;
BEGIN
    IF NEW.lane <> 'news' THEN
        RETURN NEW;
    END IF;
    IF TG_OP = 'INSERT' THEN
        active_changed := NEW.active_generation_id IS NOT NULL;
    ELSE
        active_changed := OLD.active_generation_id IS DISTINCT FROM NEW.active_generation_id;
    END IF;
    IF NOT active_changed THEN
        RETURN NEW;
    END IF;

    FOREACH window_name IN ARRAY ARRAY['today', 'week', 'month'] LOOP
        INSERT INTO news_snapshot_generations (tenant_id, "window", generation, updated_at)
        VALUES (NEW.tenant_id, window_name, 2, NOW())
        ON CONFLICT (tenant_id, "window") DO UPDATE
        SET generation = news_snapshot_generations.generation + 1,
            updated_at = NOW();
    END LOOP;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS feed_generation_head_news_snapshot_invalidation ON feed_generation_heads;
CREATE TRIGGER feed_generation_head_news_snapshot_invalidation
AFTER INSERT OR UPDATE ON feed_generation_heads
FOR EACH ROW EXECUTE FUNCTION invalidate_news_snapshot_on_feed_head_change();

-- Keep exact News item membership in the same transaction as the CMS-owned
-- story and content changes. This also makes the additive contract safe across
-- rolling writer deployments: a legacy story-membership writer still causes
-- the exact eligible item set to be materialized.
CREATE OR REPLACE FUNCTION sync_news_items_from_story_membership()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    v_generation_tenant TEXT;
    v_generation_id UUID;
    v_story_id UUID;
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.member_type <> 'story' THEN RETURN OLD; END IF;
        v_generation_id := OLD.generation_id;
        v_story_id := OLD.member_id;
    ELSE
        IF NEW.member_type <> 'story' THEN RETURN NEW; END IF;
        v_generation_id := NEW.generation_id;
        v_story_id := NEW.member_id;
    END IF;

    SELECT tenant_id INTO v_generation_tenant
    FROM feed_generations
    WHERE public_id = v_generation_id AND lane = 'news';
    IF NOT FOUND THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;

    IF TG_OP = 'DELETE' THEN
        DELETE FROM feed_generation_memberships item_membership
        USING content_items content
        WHERE item_membership.generation_id = v_generation_id
          AND item_membership.member_type = 'news_item'
          AND item_membership.member_id = content.public_id
          AND content.tenant_id = v_generation_tenant
          AND content.story_id = v_story_id;
        RETURN OLD;
    END IF;

    INSERT INTO feed_generation_memberships (generation_id, member_type, member_id)
    SELECT v_generation_id, 'news_item', content.public_id
    FROM content_items content
    WHERE content.tenant_id = v_generation_tenant
      AND content.type = 'NEWS'
      AND content.status = 'READY'
      AND content.story_id = v_story_id
      AND (COALESCE(content.news_retention_state, 'full') = 'full'
           OR content.news_feed_role IN ('lead', 'representative'))
    ON CONFLICT DO NOTHING;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS feed_generation_story_exact_news_membership ON feed_generation_memberships;
CREATE TRIGGER feed_generation_story_exact_news_membership
AFTER INSERT OR DELETE ON feed_generation_memberships
FOR EACH ROW EXECUTE FUNCTION sync_news_items_from_story_membership();

CREATE OR REPLACE FUNCTION sync_news_item_generation_membership()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    content_id UUID;
    content_tenant TEXT;
    content_type TEXT;
    content_status TEXT;
    content_story_id UUID;
    retention_state TEXT;
    feed_role TEXT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        content_id := OLD.public_id;
        content_tenant := OLD.tenant_id;
        DELETE FROM feed_generation_memberships
        WHERE member_type = 'news_item' AND member_id = content_id
          AND generation_id IN (
              SELECT generation.public_id FROM feed_generations generation
              WHERE generation.tenant_id = content_tenant AND generation.lane = 'news'
          );
        RETURN OLD;
    END IF;

    content_id := NEW.public_id;
    content_tenant := NEW.tenant_id;
    content_type := NEW.type;
    content_status := NEW.status;
    content_story_id := NEW.story_id;
    retention_state := COALESCE(NEW.news_retention_state, 'full');
    feed_role := NEW.news_feed_role;

    IF content_type <> 'NEWS' OR content_status <> 'READY' OR content_story_id IS NULL
       OR NOT (retention_state = 'full' OR feed_role IN ('lead', 'representative')) THEN
        DELETE FROM feed_generation_memberships
        WHERE member_type = 'news_item' AND member_id = content_id
          AND generation_id IN (
              SELECT generation.public_id FROM feed_generations generation
              WHERE generation.tenant_id = content_tenant AND generation.lane = 'news'
          );
        RETURN NEW;
    END IF;

    DELETE FROM feed_generation_memberships existing
    WHERE existing.member_type = 'news_item'
      AND existing.member_id = content_id
      AND existing.generation_id IN (
          SELECT generation.public_id FROM feed_generations generation
          WHERE generation.tenant_id = content_tenant AND generation.lane = 'news'
      )
      AND NOT EXISTS (
          SELECT 1 FROM feed_generation_memberships story_membership
          WHERE story_membership.generation_id = existing.generation_id
            AND story_membership.member_type = 'story'
            AND story_membership.member_id = content_story_id
      );

    INSERT INTO feed_generation_memberships (generation_id, member_type, member_id)
    SELECT story_membership.generation_id, 'news_item', content_id
    FROM feed_generation_memberships story_membership
    JOIN feed_generations generation
      ON generation.public_id = story_membership.generation_id
     AND generation.tenant_id = content_tenant
     AND generation.lane = 'news'
    WHERE story_membership.member_type = 'story'
      AND story_membership.member_id = content_story_id
    ON CONFLICT DO NOTHING;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS content_item_exact_news_generation_membership ON content_items;
CREATE TRIGGER content_item_exact_news_generation_membership
AFTER INSERT OR DELETE OR UPDATE OF story_id, status, news_retention_state, news_feed_role
ON content_items
FOR EACH ROW EXECUTE FUNCTION sync_news_item_generation_membership();

-- Repeat the backfill after installing both synchronization triggers. This
-- closes the migration window for rows changed while the first bounded copy ran.
INSERT INTO feed_generation_memberships (generation_id, member_type, member_id)
SELECT story_membership.generation_id, 'news_item', content.public_id
FROM feed_generation_memberships story_membership
JOIN feed_generations generation
  ON generation.public_id = story_membership.generation_id
 AND generation.lane = 'news'
JOIN content_items content
  ON content.tenant_id = generation.tenant_id
 AND content.story_id = story_membership.member_id
WHERE story_membership.member_type = 'story'
  AND content.type = 'NEWS'
  AND content.status = 'READY'
  AND (COALESCE(content.news_retention_state, 'full') = 'full'
       OR content.news_feed_role IN ('lead', 'representative'))
ON CONFLICT DO NOTHING;
