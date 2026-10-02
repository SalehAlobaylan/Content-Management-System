-- Defense in depth for writes which do not pass through feedstate helpers.
-- These checks do not replace candidate story projections or private delivery.
CREATE OR REPLACE FUNCTION enforce_content_reset_staged_membership()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    serving_generation feed_generations%ROWTYPE;
    source_instance source_item_instances%ROWTYPE;
    item_tenant TEXT;
    parent_id UUID;
BEGIN
    IF NEW.member_type NOT IN ('news_item','feed_unit') THEN RETURN NEW; END IF;
    SELECT * INTO serving_generation FROM feed_generations WHERE public_id=NEW.generation_id;
    IF NOT FOUND THEN RAISE EXCEPTION 'Content Reset membership generation is missing'; END IF;
    SELECT tenant_id,parent_content_item_id INTO item_tenant,parent_id
      FROM content_items WHERE public_id=NEW.member_id;
    IF NOT FOUND OR item_tenant IS DISTINCT FROM serving_generation.tenant_id THEN
        RAISE EXCEPTION 'Content Reset membership requires a same-tenant content instance';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'content-reset-instance/v1/' || item_tenant || '/' || COALESCE(parent_id,NEW.member_id)::text,0));
    SELECT * INTO source_instance FROM source_item_instances
      WHERE tenant_id=item_tenant AND content_item_id=NEW.member_id;
    IF NOT FOUND AND parent_id IS NOT NULL THEN
        SELECT * INTO source_instance FROM source_item_instances
          WHERE tenant_id=item_tenant AND content_item_id=parent_id;
    END IF;
    IF source_instance.state='staged' AND NOT (
        serving_generation.purpose='content_reset' AND serving_generation.state='candidate'
        AND serving_generation.content_reset_campaign_id=source_instance.campaign_id
    ) THEN
        RAISE EXCEPTION 'Content Reset staged instance cannot enter a live or unrelated view';
    END IF;
    IF source_instance.state='retired' AND source_instance.content_item_id=NEW.member_id THEN
        RAISE EXCEPTION 'Content Reset retired instance cannot gain serving membership';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS content_reset_staged_membership_guard ON feed_generation_memberships;
CREATE TRIGGER content_reset_staged_membership_guard
BEFORE INSERT ON feed_generation_memberships
FOR EACH ROW EXECUTE FUNCTION enforce_content_reset_staged_membership();

CREATE OR REPLACE FUNCTION enforce_content_reset_staged_instance_admission()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.state<>'staged' THEN RETURN NEW; END IF;
    IF NEW.campaign_id IS NULL THEN RAISE EXCEPTION 'Staged content requires a campaign'; END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'content-reset-instance/v1/' || NEW.tenant_id || '/' || NEW.content_item_id::text,0));
    IF EXISTS (
        SELECT 1 FROM feed_generation_memberships membership
        JOIN feed_generations generation ON generation.public_id=membership.generation_id
        JOIN content_items item ON item.public_id=membership.member_id AND item.tenant_id=generation.tenant_id
        WHERE item.tenant_id=NEW.tenant_id
          AND (item.public_id=NEW.content_item_id OR item.parent_content_item_id=NEW.content_item_id)
          AND membership.member_type IN ('news_item','feed_unit')
          AND (generation.purpose<>'content_reset' OR generation.state<>'candidate'
               OR generation.content_reset_campaign_id IS DISTINCT FROM NEW.campaign_id)
    ) THEN RAISE EXCEPTION 'Staged instance already has unrelated serving membership'; END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS content_reset_staged_instance_admission_guard ON source_item_instances;
CREATE TRIGGER content_reset_staged_instance_admission_guard
BEFORE INSERT ON source_item_instances
FOR EACH ROW EXECUTE FUNCTION enforce_content_reset_staged_instance_admission();
