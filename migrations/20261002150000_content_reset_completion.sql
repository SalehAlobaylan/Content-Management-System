-- Content Reset completion: durable publication/rollback/cleanup milestones,
-- delegated Pods retirement binding, and execution completion timestamps.
--
-- Milestones are immutable approvals. The owning worker may only transition an
-- active milestone to consumed (or an operator may revoke it while unused).
-- None of these rows grants an effect by itself; the typed owner contract still
-- revalidates the campaign, revision, environment and exact live fences.

CREATE TABLE IF NOT EXISTS content_reset_milestones (
    id bigserial PRIMARY KEY,
    public_id uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    tenant_id varchar(64) NOT NULL,
    campaign_id bigint NOT NULL REFERENCES content_reset_campaigns(id),
    revision_id bigint NOT NULL REFERENCES content_reset_revisions(id),
    kind varchar(24) NOT NULL CHECK (kind IN ('publication', 'rollback')),
    state varchar(16) NOT NULL CHECK (state IN ('active', 'consumed', 'revoked')),
    manifest_hash char(64) NOT NULL,
    payload jsonb NOT NULL,
    payload_hash char(64) NOT NULL,
    approved_by varchar(255) NOT NULL,
    approved_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    consumed_at timestamptz,
    revoked_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((state = 'consumed') = (consumed_at IS NOT NULL)),
    CHECK ((state = 'revoked') = (revoked_at IS NOT NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_content_reset_active_milestone
    ON content_reset_milestones (tenant_id, campaign_id, revision_id, kind)
    WHERE state = 'active';

CREATE INDEX IF NOT EXISTS idx_content_reset_milestones_campaign
    ON content_reset_milestones (tenant_id, campaign_id, kind, state);

CREATE OR REPLACE FUNCTION enforce_content_reset_milestone_identity()
RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'content_reset_milestone_immutable';
    END IF;
    IF NEW.public_id <> OLD.public_id
        OR NEW.tenant_id <> OLD.tenant_id
        OR NEW.campaign_id <> OLD.campaign_id
        OR NEW.revision_id <> OLD.revision_id
        OR NEW.kind <> OLD.kind
        OR NEW.manifest_hash <> OLD.manifest_hash
        OR NEW.payload_hash <> OLD.payload_hash
        OR NEW.payload <> OLD.payload THEN
        RAISE EXCEPTION 'content_reset_milestone_identity_immutable';
    END IF;
    IF OLD.state <> 'active' AND NEW.state <> OLD.state THEN
        RAISE EXCEPTION 'content_reset_milestone_terminal';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS enforce_content_reset_milestone_identity ON content_reset_milestones;
CREATE TRIGGER enforce_content_reset_milestone_identity
    BEFORE UPDATE OR DELETE ON content_reset_milestones
    FOR EACH ROW EXECUTE FUNCTION enforce_content_reset_milestone_identity();

ALTER TABLE content_reset_executions
    ADD COLUMN IF NOT EXISTS rolled_back_at timestamptz,
    ADD COLUMN IF NOT EXISTS completed_at timestamptz,
    ADD COLUMN IF NOT EXISTS cleanup_authorized_until timestamptz;

-- A campaign may compose bounded delegated Pods reset runs. The immutable parent
-- binding prevents an ordinary standalone plan from claiming campaign authority.
ALTER TABLE pods_reset_runs
    ADD COLUMN IF NOT EXISTS content_reset_campaign_id bigint NULL REFERENCES content_reset_campaigns(id),
    ADD COLUMN IF NOT EXISTS content_reset_revision_id bigint NULL REFERENCES content_reset_revisions(id),
    ADD COLUMN IF NOT EXISTS content_reset_lane varchar(8) NULL,
    ADD COLUMN IF NOT EXISTS content_reset_batch integer NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_pods_reset_campaign_batch
    ON pods_reset_runs (content_reset_campaign_id, content_reset_revision_id, content_reset_lane, content_reset_batch)
    WHERE content_reset_campaign_id IS NOT NULL;

-- Delegated runs terminate through the same Pods-reset executor; this index keeps
-- campaign retirement accounting bounded.
CREATE INDEX IF NOT EXISTS idx_pods_reset_campaign_binding
    ON pods_reset_runs (content_reset_campaign_id, state);

-- One immutable handoff per replay branch. The live source schedule is advanced
-- only after every branch page has settled provider evidence; the recorded
-- boundary is the maximum provider observation time, never a guessed cursor.
CREATE TABLE IF NOT EXISTS content_reset_replay_handoffs (
    id bigserial PRIMARY KEY,
    public_id uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    tenant_id varchar(64) NOT NULL,
    campaign_id bigint NOT NULL REFERENCES content_reset_campaigns(id),
    revision_id bigint NOT NULL REFERENCES content_reset_revisions(id),
    branch_id uuid NOT NULL,
    content_source_id uuid NOT NULL,
    pages integer NOT NULL,
    observed_until timestamptz NOT NULL,
    spec_hash char(64) NOT NULL,
    command_id uuid NOT NULL,
    handed_off_at timestamptz NOT NULL,
    UNIQUE (tenant_id, branch_id)
);

CREATE OR REPLACE FUNCTION enforce_content_reset_replay_handoff_immutable()
RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'content_reset_replay_handoff_immutable';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS enforce_content_reset_replay_handoff_immutable ON content_reset_replay_handoffs;
CREATE TRIGGER enforce_content_reset_replay_handoff_immutable
    BEFORE UPDATE OR DELETE ON content_reset_replay_handoffs
    FOR EACH ROW EXECUTE FUNCTION enforce_content_reset_replay_handoff_immutable();

-- Release qualification registry. A row records that one exact owner contract
-- version passed its qualification suite in a named environment. Application
-- code additionally requires its own code-owned expected qualification version,
-- so a stale or unreviewed row cannot enable an effect. No row is seeded here:
-- execution stays disabled until the qualification suite is run and recorded.
CREATE TABLE IF NOT EXISTS content_reset_owner_qualifications (
    id bigserial PRIMARY KEY,
    public_id uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    contract_owner varchar(64) NOT NULL,
    contract_effect varchar(64) NOT NULL,
    contract_target_type varchar(32) NOT NULL,
    contract_version varchar(32) NOT NULL,
    qualification_version varchar(96) NOT NULL,
    environment_hash char(64) NOT NULL,
    evidence_hash char(64) NOT NULL,
    qualified_by varchar(255) NOT NULL,
    qualified_at timestamptz NOT NULL,
    revoked_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_content_reset_owner_qualification
    ON content_reset_owner_qualifications (contract_owner, contract_effect, contract_target_type, contract_version)
    WHERE revoked_at IS NULL;

CREATE OR REPLACE FUNCTION enforce_content_reset_owner_qualification_immutable()
RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'content_reset_owner_qualification_immutable';
    END IF;
    IF NEW.contract_owner <> OLD.contract_owner
        OR NEW.contract_effect <> OLD.contract_effect
        OR NEW.contract_target_type <> OLD.contract_target_type
        OR NEW.contract_version <> OLD.contract_version
        OR NEW.qualification_version <> OLD.qualification_version
        OR NEW.environment_hash <> OLD.environment_hash
        OR NEW.evidence_hash <> OLD.evidence_hash THEN
        RAISE EXCEPTION 'content_reset_owner_qualification_identity_immutable';
    END IF;
    IF OLD.revoked_at IS NOT NULL AND NEW.revoked_at IS DISTINCT FROM OLD.revoked_at THEN
        RAISE EXCEPTION 'content_reset_owner_qualification_terminal';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS enforce_content_reset_owner_qualification_immutable ON content_reset_owner_qualifications;
CREATE TRIGGER enforce_content_reset_owner_qualification_immutable
    BEFORE UPDATE OR DELETE ON content_reset_owner_qualifications
    FOR EACH ROW EXECUTE FUNCTION enforce_content_reset_owner_qualification_immutable();

-- An approved rollback demotes a published replacement back to staged. The
-- session fence is set only inside the admitted rollback transaction, is scoped
-- to the exact campaign's instances, and never authorizes reversing a permanent
-- retirement (retired rows stay terminal).
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
        (OLD.state='superseded' AND NEW.state IN ('active','retired')) OR
        (OLD.state='active' AND NEW.state='staged' AND NEW.campaign_id IS NOT NULL
            AND current_setting('wahb.content_reset.rollback_campaign', true) IS NOT NULL
            AND current_setting('wahb.content_reset.rollback_campaign', true) = NEW.campaign_id::text)
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

-- `closed_partial` is a supported terminal campaign state for an explicitly
-- abandoned run that keeps its published replacement serving.
DO $$ DECLARE constraint_name TEXT; BEGIN
    FOR constraint_name IN
        SELECT conname FROM pg_constraint
        WHERE conrelid='content_reset_campaigns'::regclass AND contype='c'
          AND pg_get_constraintdef(oid) LIKE '%planning%previewed%blocked%approved%executing%'
    LOOP EXECUTE format('ALTER TABLE content_reset_campaigns DROP CONSTRAINT %I', constraint_name); END LOOP;
END $$;
ALTER TABLE content_reset_campaigns ADD CONSTRAINT content_reset_campaigns_state_check
    CHECK (state IN ('planning','previewed','blocked','approved','executing','partial','published','cleanup_pending','complete','cancelled','closed_partial'));
