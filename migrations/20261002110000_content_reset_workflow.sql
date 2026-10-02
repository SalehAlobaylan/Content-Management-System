CREATE TABLE content_reset_workflows (
    id BIGSERIAL PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    campaign_id BIGINT NOT NULL,
    revision_id BIGINT NOT NULL,
    manifest_hash CHAR(64) NOT NULL CHECK (manifest_hash ~ '^[0-9a-f]{64}$'),
    prepared_command_id UUID NOT NULL REFERENCES content_reset_steps(public_id) ON DELETE RESTRICT,
    phase VARCHAR(48) NOT NULL,
    source_cursor INTEGER NOT NULL DEFAULT 0 CHECK (source_cursor >= 0),
    target_cursor BIGINT NOT NULL DEFAULT 0 CHECK (target_cursor >= 0),
    branch_cursor BIGINT NOT NULL DEFAULT 0 CHECK (branch_cursor >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, campaign_id),
    FOREIGN KEY (tenant_id, campaign_id, revision_id)
        REFERENCES content_reset_revisions(tenant_id, campaign_id, id) ON DELETE RESTRICT
);

CREATE FUNCTION protect_content_reset_workflow_identity()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN RAISE EXCEPTION 'Content Reset workflow cannot be deleted'; END IF;
    IF ROW(NEW.id, NEW.tenant_id, NEW.campaign_id, NEW.revision_id, NEW.manifest_hash, NEW.prepared_command_id, NEW.created_at)
       IS DISTINCT FROM ROW(OLD.id, OLD.tenant_id, OLD.campaign_id, OLD.revision_id, OLD.manifest_hash, OLD.prepared_command_id, OLD.created_at)
       OR NEW.source_cursor < OLD.source_cursor OR NEW.target_cursor < OLD.target_cursor THEN
        RAISE EXCEPTION 'Content Reset workflow identity/cursors cannot be rewritten';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER content_reset_workflow_identity_guard
BEFORE UPDATE OR DELETE ON content_reset_workflows
FOR EACH ROW EXECUTE FUNCTION protect_content_reset_workflow_identity();
