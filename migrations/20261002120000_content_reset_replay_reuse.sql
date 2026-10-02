-- Read references for repeated provider observations. Reconstruction grants
-- retain their one-use and exact original normalization-unit binding.
CREATE TABLE content_reset_replay_reuses (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    tenant_id VARCHAR(64) NOT NULL,
    campaign_id BIGINT NOT NULL,
    revision_id BIGINT NOT NULL,
    grant_id BIGINT REFERENCES content_reset_reconstruction_grants(id) ON DELETE RESTRICT,
    observation_id UUID NOT NULL REFERENCES source_upstream_observations(public_id) ON DELETE RESTRICT,
    source_run_request_id UUID NOT NULL,
    execution_unit_id UUID NOT NULL,
    content_item_id UUID NOT NULL,
    instance_generation INTEGER NOT NULL CHECK (instance_generation > 0),
    fingerprint CHAR(64) NOT NULL CHECK (fingerprint ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, observation_id),
    FOREIGN KEY (tenant_id, campaign_id, revision_id)
        REFERENCES content_reset_revisions(tenant_id, campaign_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, source_run_request_id)
        REFERENCES source_run_requests(tenant_id, public_id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, execution_unit_id)
        REFERENCES source_run_execution_units(tenant_id, public_id) ON DELETE RESTRICT
);
CREATE FUNCTION assert_content_reset_replay_reuse_binding() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
      SELECT 1 FROM source_upstream_observations observation
      JOIN source_run_execution_units unit ON unit.tenant_id=observation.tenant_id
        AND unit.source_run_request_id=observation.source_run_request_id AND unit.content_source_id=observation.content_source_id
      JOIN source_item_instances instance ON instance.tenant_id=observation.tenant_id AND instance.content_item_id=NEW.content_item_id
      JOIN source_item_identities identity ON identity.tenant_id=instance.tenant_id AND identity.id=instance.identity_id
        AND identity.content_source_id=observation.content_source_id AND identity.upstream_item_id=observation.upstream_item_id
      JOIN content_reset_replay_pages page ON page.tenant_id=unit.tenant_id AND page.source_run_request_id=unit.source_run_request_id
      JOIN content_reset_replay_branches branch ON branch.tenant_id=page.tenant_id AND branch.public_id=page.branch_id
      WHERE observation.tenant_id=NEW.tenant_id AND observation.public_id=NEW.observation_id
        AND observation.source_run_request_id=NEW.source_run_request_id AND unit.public_id=NEW.execution_unit_id
        AND lower(observation.upstream_fingerprint)=NEW.fingerprint AND instance.upstream_fingerprint=NEW.fingerprint
        AND instance.instance_generation=NEW.instance_generation AND branch.campaign_id=NEW.campaign_id AND branch.revision_id=NEW.revision_id
        AND (
          (NEW.grant_id IS NOT NULL AND instance.state='staged' AND instance.campaign_id=NEW.campaign_id AND EXISTS (
            SELECT 1 FROM content_reset_reconstruction_grants grant_row WHERE grant_row.id=NEW.grant_id
              AND grant_row.tenant_id=NEW.tenant_id AND grant_row.campaign_id=NEW.campaign_id AND grant_row.revision_id=NEW.revision_id
              AND grant_row.state='consumed' AND grant_row.identity_id=instance.identity_id
              AND grant_row.replacement_content_item_id=NEW.content_item_id AND grant_row.replacement_instance_generation=NEW.instance_generation
          )) OR (NEW.grant_id IS NULL AND instance.state='active' AND identity.current_content_item_id=NEW.content_item_id
            AND identity.current_instance_generation=NEW.instance_generation AND NOT EXISTS (
              SELECT 1 FROM content_reset_targets target WHERE target.tenant_id=NEW.tenant_id AND target.revision_id=NEW.revision_id
                AND target.content_item_id=NEW.content_item_id AND target.disposition='selected'
            ))
        )
    ) THEN RAISE EXCEPTION 'Content Reset replay reuse does not match its native observation/instance'; END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER content_reset_replay_reuse_binding_guard BEFORE INSERT ON content_reset_replay_reuses
FOR EACH ROW EXECUTE FUNCTION assert_content_reset_replay_reuse_binding();
CREATE TRIGGER content_reset_replay_reuses_immutable
BEFORE UPDATE OR DELETE ON content_reset_replay_reuses
FOR EACH ROW EXECUTE FUNCTION reject_content_reset_execution_evidence_mutation();
