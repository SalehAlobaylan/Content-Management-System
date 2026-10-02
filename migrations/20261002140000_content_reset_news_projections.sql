-- Each News generation owns its presentation and centroid. Canonical story
-- UUIDs remain stable for survivors; staging never updates the live projection.
CREATE TABLE news_feed_story_projections (
    tenant_id VARCHAR(64) NOT NULL,
    generation_id UUID NOT NULL REFERENCES feed_generations(public_id) ON DELETE RESTRICT,
    story_id UUID NOT NULL REFERENCES stories(public_id) ON DELETE RESTRICT,
    projection JSONB NOT NULL CHECK (jsonb_typeof(projection)='object'),
    member_hash CHAR(64) NOT NULL CHECK (member_hash ~ '^[0-9a-f]{64}$'),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (generation_id, story_id),
    CHECK (projection->>'tenant_id' IS NOT DISTINCT FROM tenant_id AND projection->>'public_id' IS NOT DISTINCT FROM story_id::text)
);
CREATE INDEX news_feed_story_projections_tenant ON news_feed_story_projections(tenant_id,generation_id);
CREATE FUNCTION guard_news_feed_story_projection() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM feed_generations g JOIN stories s ON s.public_id=NEW.story_id
       WHERE g.public_id=NEW.generation_id AND g.tenant_id=NEW.tenant_id AND g.lane='news'
         AND s.tenant_id=NEW.tenant_id) THEN
       RAISE EXCEPTION 'News projection tenant/lane binding is invalid';
    END IF;
    IF TG_OP='UPDATE' AND ROW(NEW.tenant_id,NEW.generation_id,NEW.story_id)
       IS DISTINCT FROM ROW(OLD.tenant_id,OLD.generation_id,OLD.story_id) THEN
       RAISE EXCEPTION 'News projection identity cannot change';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER news_feed_story_projection_guard BEFORE INSERT OR UPDATE ON news_feed_story_projections
FOR EACH ROW EXECUTE FUNCTION guard_news_feed_story_projection();
