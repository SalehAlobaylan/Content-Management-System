-- Durable approval, operator CAS decisions, and owner-step dependencies.
-- No operation is enabled and no existing content is mutated by this migration.
CREATE UNIQUE INDEX IF NOT EXISTS uq_content_reset_revision_campaign_identity
    ON content_reset_revisions(tenant_id, campaign_id, id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_content_reset_step_campaign_identity
    ON content_reset_steps(tenant_id, campaign_id, revision_id, id);

CREATE TABLE IF NOT EXISTS content_reset_executions (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    tenant_id VARCHAR(64) NOT NULL,
    campaign_id BIGINT NOT NULL,
    revision_id BIGINT NOT NULL,
    manifest_hash CHAR(64) NOT NULL CHECK (manifest_hash ~ '^[0-9a-f]{64}$'),
    version BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
    phase VARCHAR(48) NOT NULL CHECK (length(btrim(phase)) > 0),
    pause_requested BOOLEAN NOT NULL DEFAULT FALSE,
    approved_by VARCHAR(255) NOT NULL,
    approved_at TIMESTAMPTZ NOT NULL,
    approval_expires_at TIMESTAMPTZ NOT NULL CHECK (approval_expires_at > approved_at),
    started_at TIMESTAMPTZ,
    irreversible_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    cleanup_not_before TIMESTAMPTZ,
    contract_hash CHAR(64) NOT NULL CHECK (contract_hash ~ '^[0-9a-f]{64}$'),
    contract JSONB NOT NULL CHECK (jsonb_typeof(contract) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, campaign_id),
    FOREIGN KEY (tenant_id, campaign_id, revision_id)
        REFERENCES content_reset_revisions(tenant_id, campaign_id, id) ON DELETE RESTRICT,
    CHECK (published_at IS NULL OR started_at IS NOT NULL),
    CHECK (irreversible_at IS NULL OR started_at IS NOT NULL),
    CHECK (cleanup_not_before IS NULL OR (published_at IS NOT NULL AND cleanup_not_before >= published_at))
);

CREATE TABLE IF NOT EXISTS content_reset_decisions (
    id BIGSERIAL PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    campaign_id BIGINT NOT NULL,
    idempotency_key VARCHAR(128) NOT NULL CHECK (length(idempotency_key) BETWEEN 8 AND 128),
    action VARCHAR(48) NOT NULL,
    actor_id VARCHAR(255) NOT NULL,
    request_hash CHAR(64) NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    proof_hash CHAR(64),
    response JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, campaign_id, idempotency_key),
    FOREIGN KEY (tenant_id, campaign_id) REFERENCES content_reset_campaigns(tenant_id, id) ON DELETE RESTRICT
);
-- Issuer + JTI is globally one-use, including across tenants and campaigns.
CREATE UNIQUE INDEX IF NOT EXISTS uq_content_reset_decision_proof
    ON content_reset_decisions(proof_hash) WHERE proof_hash IS NOT NULL;

CREATE TABLE IF NOT EXISTS content_reset_step_dependencies (
    tenant_id VARCHAR(64) NOT NULL,
    campaign_id BIGINT NOT NULL,
    revision_id BIGINT NOT NULL,
    step_id BIGINT NOT NULL,
    requires_id BIGINT NOT NULL,
    PRIMARY KEY (tenant_id, campaign_id, revision_id, step_id, requires_id),
    CHECK (requires_id < step_id),
    FOREIGN KEY (tenant_id, campaign_id, revision_id, step_id)
        REFERENCES content_reset_steps(tenant_id, campaign_id, revision_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, campaign_id, revision_id, requires_id)
        REFERENCES content_reset_steps(tenant_id, campaign_id, revision_id, id) ON DELETE RESTRICT
);

CREATE OR REPLACE FUNCTION protect_content_reset_execution_identity()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN RAISE EXCEPTION 'Content Reset executions cannot be deleted'; END IF;
    IF ROW(NEW.id, NEW.public_id, NEW.tenant_id, NEW.campaign_id, NEW.revision_id,
           NEW.manifest_hash, NEW.approved_by, NEW.approved_at, NEW.approval_expires_at,
           NEW.contract_hash, NEW.contract, NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.id, OLD.public_id, OLD.tenant_id, OLD.campaign_id, OLD.revision_id,
           OLD.manifest_hash, OLD.approved_by, OLD.approved_at, OLD.approval_expires_at,
           OLD.contract_hash, OLD.contract, OLD.created_at) THEN
        RAISE EXCEPTION 'Content Reset approval identity is immutable';
    END IF;
    IF NEW.version < OLD.version OR NEW.version > OLD.version + 1 THEN
        RAISE EXCEPTION 'Content Reset control version must advance monotonically';
    END IF;
    IF (OLD.started_at IS NOT NULL AND NEW.started_at IS DISTINCT FROM OLD.started_at)
       OR (OLD.irreversible_at IS NOT NULL AND NEW.irreversible_at IS DISTINCT FROM OLD.irreversible_at)
       OR (OLD.published_at IS NOT NULL AND NEW.published_at IS DISTINCT FROM OLD.published_at)
       OR (OLD.cleanup_not_before IS NOT NULL AND NEW.cleanup_not_before IS DISTINCT FROM OLD.cleanup_not_before) THEN
        RAISE EXCEPTION 'Content Reset execution milestones cannot be rewritten';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS content_reset_execution_identity_guard ON content_reset_executions;
CREATE TRIGGER content_reset_execution_identity_guard
BEFORE UPDATE OR DELETE ON content_reset_executions
FOR EACH ROW EXECUTE FUNCTION protect_content_reset_execution_identity();

CREATE OR REPLACE FUNCTION reject_content_reset_execution_evidence_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Content Reset decisions and step dependencies are immutable';
END;
$$;
DROP TRIGGER IF EXISTS content_reset_decisions_immutable ON content_reset_decisions;
CREATE TRIGGER content_reset_decisions_immutable
BEFORE UPDATE OR DELETE ON content_reset_decisions
FOR EACH ROW EXECUTE FUNCTION reject_content_reset_execution_evidence_mutation();

CREATE OR REPLACE FUNCTION protect_content_reset_step_settlement()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.state IN ('succeeded','cancelled') AND
       ROW(NEW.state, NEW.receipt, NEW.attempt_count, NEW.lease_token, NEW.lease_until, NEW.last_error)
       IS DISTINCT FROM ROW(OLD.state, OLD.receipt, OLD.attempt_count, OLD.lease_token, OLD.lease_until, OLD.last_error) THEN
        RAISE EXCEPTION 'Content Reset terminal owner receipt is immutable';
    END IF;
    IF NEW.state IS DISTINCT FROM OLD.state AND NOT (
        (OLD.state = 'pending' AND NEW.state IN ('claimed','blocked','cancelled')) OR
        (OLD.state = 'claimed' AND NEW.state IN ('succeeded','blocked','failed','outcome_unknown')) OR
        (OLD.state = 'outcome_unknown' AND NEW.state IN ('succeeded','blocked','failed'))
    ) THEN
        RAISE EXCEPTION 'Content Reset step transition is not permitted';
    END IF;
    IF NEW.state = 'claimed' AND (NEW.lease_token IS NULL OR NEW.lease_until IS NULL) THEN
        RAISE EXCEPTION 'Content Reset claimed step requires a lease';
    END IF;
    IF NEW.state = 'succeeded' AND (NEW.receipt IS NULL OR NEW.receipt->>'state' IS DISTINCT FROM 'succeeded'
       OR NEW.receipt->>'command_id' IS DISTINCT FROM NEW.public_id::text
       OR NEW.receipt->>'command_hash' IS NULL OR NEW.receipt->>'command_hash' !~ '^[0-9a-f]{64}$') THEN
        RAISE EXCEPTION 'Content Reset success requires a bound owner receipt';
    END IF;
    IF NEW.attempt_count < OLD.attempt_count OR NEW.attempt_count > OLD.attempt_count + 1 THEN
        RAISE EXCEPTION 'Content Reset owner attempt counter is invalid';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS content_reset_step_settlement_guard ON content_reset_steps;
CREATE TRIGGER content_reset_step_settlement_guard
BEFORE UPDATE ON content_reset_steps
FOR EACH ROW EXECUTE FUNCTION protect_content_reset_step_settlement();

CREATE OR REPLACE FUNCTION protect_content_reset_dependency_admission()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE target_state TEXT;
BEGIN
    SELECT state INTO target_state FROM content_reset_steps
    WHERE tenant_id=NEW.tenant_id AND campaign_id=NEW.campaign_id
      AND revision_id=NEW.revision_id AND id=NEW.step_id FOR UPDATE;
    IF target_state IS DISTINCT FROM 'pending' THEN
        RAISE EXCEPTION 'Content Reset dependencies must be frozen before claim';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS content_reset_dependency_admission_guard ON content_reset_step_dependencies;
CREATE TRIGGER content_reset_dependency_admission_guard
BEFORE INSERT ON content_reset_step_dependencies
FOR EACH ROW EXECUTE FUNCTION protect_content_reset_dependency_admission();
DROP TRIGGER IF EXISTS content_reset_dependencies_immutable ON content_reset_step_dependencies;
CREATE TRIGGER content_reset_dependencies_immutable
BEFORE UPDATE OR DELETE ON content_reset_step_dependencies
FOR EACH ROW EXECUTE FUNCTION reject_content_reset_execution_evidence_mutation();
