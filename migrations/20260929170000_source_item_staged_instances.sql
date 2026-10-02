-- Replacement materialization does not change the live source identity.
-- Publication promotes a staged instance in the same transaction as feed heads.
ALTER TABLE source_item_instances DROP CONSTRAINT IF EXISTS source_item_instances_state_check;
ALTER TABLE source_item_instances ADD CONSTRAINT source_item_instances_state_check
    CHECK (state IN ('staged','active','superseded','retired'));
CREATE UNIQUE INDEX IF NOT EXISTS uq_source_item_staged_identity
    ON source_item_instances(tenant_id, identity_id) WHERE state='staged';
CREATE UNIQUE INDEX IF NOT EXISTS uq_source_item_content_instance
    ON source_item_instances(tenant_id, content_item_id);

ALTER TABLE content_reset_reconstruction_grants
    ADD COLUMN IF NOT EXISTS base_instance_generation INTEGER;
UPDATE content_reset_reconstruction_grants
SET base_instance_generation = replacement_instance_generation - 1
WHERE base_instance_generation IS NULL;
ALTER TABLE content_reset_reconstruction_grants
    ALTER COLUMN base_instance_generation SET NOT NULL;
-- Resolve the historical expression constraint by its definition rather than
-- relying on PostgreSQL's numbering of anonymous CHECK constraints.
DO $$ DECLARE constraint_name TEXT; BEGIN
    FOR constraint_name IN
        SELECT conname FROM pg_constraint
        WHERE conrelid='content_reset_reconstruction_grants'::regclass AND contype='c'
          AND pg_get_constraintdef(oid) LIKE '%grant_kind%new_identity%replacement_instance_generation%'
    LOOP EXECUTE format('ALTER TABLE content_reset_reconstruction_grants DROP CONSTRAINT %I', constraint_name); END LOOP;
END $$;
ALTER TABLE content_reset_reconstruction_grants
    ADD CONSTRAINT content_reset_grant_base_generation CHECK (
        base_instance_generation >= 0 AND replacement_instance_generation > base_instance_generation
        AND ((grant_kind='new_identity') = (base_instance_generation=0))
    );
CREATE UNIQUE INDEX IF NOT EXISTS uq_content_reset_reserved_instance_generation
    ON content_reset_reconstruction_grants(tenant_id,identity_id,replacement_instance_generation);

CREATE OR REPLACE FUNCTION retire_source_item_instance_before_content_delete()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE source_item_instances
       SET state='retired', retired_at=COALESCE(retired_at,NOW())
     WHERE tenant_id=OLD.tenant_id AND content_item_id=OLD.public_id
       AND state IN ('staged','active','superseded');
    RETURN OLD;
END;
$$;

CREATE OR REPLACE FUNCTION protect_source_item_instance_identity()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN RAISE EXCEPTION 'Source item instance identities cannot be deleted'; END IF;
    IF ROW(NEW.id,NEW.public_id,NEW.tenant_id,NEW.identity_id,NEW.instance_generation,
           NEW.content_item_id,NEW.source_observation_id,NEW.upstream_fingerprint,NEW.provider_version,NEW.campaign_id,NEW.created_at)
       IS DISTINCT FROM ROW(OLD.id,OLD.public_id,OLD.tenant_id,OLD.identity_id,OLD.instance_generation,
           OLD.content_item_id,OLD.source_observation_id,OLD.upstream_fingerprint,OLD.provider_version,OLD.campaign_id,OLD.created_at) THEN
        RAISE EXCEPTION 'Source item instance provenance is immutable';
    END IF;
    IF NEW.state IS DISTINCT FROM OLD.state AND NOT (
        (OLD.state='staged' AND NEW.state IN ('active','retired')) OR
        (OLD.state='active' AND NEW.state IN ('superseded','retired')) OR
        (OLD.state='superseded' AND NEW.state IN ('active','retired'))
    ) THEN RAISE EXCEPTION 'Source item instance transition is not permitted'; END IF;
    IF OLD.retired_at IS NOT NULL AND NEW.retired_at IS DISTINCT FROM OLD.retired_at THEN
        RAISE EXCEPTION 'Source item retirement is permanent';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS source_item_instance_identity_guard ON source_item_instances;
CREATE TRIGGER source_item_instance_identity_guard BEFORE UPDATE OR DELETE ON source_item_instances
FOR EACH ROW EXECUTE FUNCTION protect_source_item_instance_identity();
