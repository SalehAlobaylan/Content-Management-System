-- Separate completed provider effects from pending consumer delivery.
-- Existing rows remain blocking: release timestamps default to NULL.
-- Apply through the canonical transactional migration owner: the two
-- exclusion indexes and proof guards must become visible together.
ALTER TABLE source_run_requests ADD COLUMN provider_effects_released_at TIMESTAMPTZ;
ALTER TABLE source_run_attempts ADD COLUMN provider_effects_released_at TIMESTAMPTZ;
CREATE UNIQUE INDEX IF NOT EXISTS uq_content_reset_release_attempt_identity
    ON source_run_attempts(tenant_id, public_id);
CREATE TABLE content_reset_provider_releases (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    tenant_id VARCHAR(64) NOT NULL,
    page_id UUID NOT NULL,
    source_run_request_id UUID NOT NULL,
    source_run_attempt_id UUID NOT NULL,
    command_id UUID NOT NULL UNIQUE REFERENCES content_reset_steps(public_id) ON DELETE RESTRICT,
    proof JSONB NOT NULL CHECK (jsonb_typeof(proof)='object'),
    proof_hash CHAR(64) NOT NULL,
    released_at TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, page_id),
    UNIQUE (tenant_id, source_run_request_id),
    UNIQUE (tenant_id, source_run_attempt_id),
    FOREIGN KEY (tenant_id, page_id) REFERENCES content_reset_replay_pages(tenant_id, public_id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, source_run_request_id) REFERENCES source_run_requests(tenant_id, public_id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, source_run_attempt_id) REFERENCES source_run_attempts(tenant_id, public_id) ON DELETE RESTRICT
);
CREATE TRIGGER content_reset_provider_releases_immutable
BEFORE UPDATE OR DELETE ON content_reset_provider_releases
FOR EACH ROW EXECUTE FUNCTION reject_content_reset_execution_evidence_mutation();

CREATE FUNCTION guard_content_reset_provider_release() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE bound BOOLEAN;
BEGIN
    SELECT EXISTS (
        SELECT 1 FROM content_reset_replay_pages p
        JOIN content_reset_replay_branches b ON b.tenant_id=p.tenant_id AND b.public_id=p.branch_id
        JOIN content_reset_replay_checkpoints k ON k.tenant_id=p.tenant_id AND k.page_id=p.public_id
        JOIN source_run_requests r ON r.tenant_id=p.tenant_id AND r.public_id=p.source_run_request_id
        JOIN source_run_attempts a ON a.tenant_id=r.tenant_id AND a.source_run_request_id=r.public_id
        JOIN source_run_execution_units root ON root.tenant_id=a.tenant_id AND root.public_id=a.root_execution_unit_id
        JOIN content_reset_steps step ON step.tenant_id=b.tenant_id AND step.campaign_id=b.campaign_id AND step.revision_id=b.revision_id
        JOIN content_reset_campaigns campaign ON campaign.tenant_id=b.tenant_id AND campaign.id=b.campaign_id
        JOIN content_reset_revisions revision ON revision.tenant_id=b.tenant_id AND revision.id=b.revision_id AND revision.campaign_id=campaign.id
        JOIN content_reset_executions execution ON execution.tenant_id=b.tenant_id AND execution.campaign_id=campaign.id AND execution.revision_id=revision.id
        WHERE p.tenant_id=NEW.tenant_id AND p.public_id=NEW.page_id
          AND r.public_id=NEW.source_run_request_id AND a.public_id=NEW.source_run_attempt_id
          AND r.purpose='content_reset_replay' AND r.state='verification_required'
          AND r.manifest_state='sealed' AND r.expected_page_count=1
          AND a.state='verification_required' AND a.provider_effects_released_at IS NULL
          AND root.unit_type='coordinator' AND root.state='verification_required'
          AND root.source_run_request_id=r.public_id AND root.source_run_attempt_id=a.public_id
          AND r.root_execution_unit_id=root.public_id
          AND campaign.state='executing' AND campaign.operation='fresh_start' AND campaign.current_revision=revision.revision
          AND revision.manifest_hash=b.manifest_hash AND execution.manifest_hash=b.manifest_hash
          AND execution.started_at IS NOT NULL AND execution.published_at IS NULL
          AND step.public_id=NEW.command_id AND step.owner='cms/source-run' AND step.effect_type='release_provider_slot'
          AND step.target_id=b.public_id::text AND step.state='claimed' AND step.lease_until>NOW()
          AND step.command->'parameters'->>'page_id'=p.public_id::text
          AND step.command->>'manifest_hash'=b.manifest_hash
          AND r.expected_unit_count=(SELECT COUNT(*) FROM source_run_execution_units u WHERE u.tenant_id=r.tenant_id AND u.source_run_request_id=r.public_id)
          AND NOT EXISTS (SELECT 1 FROM source_run_execution_units u
                          WHERE u.tenant_id=r.tenant_id AND u.source_run_request_id=r.public_id
                            AND (u.source_run_attempt_id<>a.public_id OR
                                 (u.public_id<>root.public_id AND
                                  (u.state<>'succeeded' OR u.terminal_outcome IS NULL OR u.terminal_outcome NOT IN ('new_items','no_change')))))
    ) INTO bound;
    IF NOT bound THEN RAISE EXCEPTION 'Provider release requires a sealed completed replay graph and a current owner command'; END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER content_reset_provider_release_guard BEFORE INSERT ON content_reset_provider_releases
FOR EACH ROW EXECUTE FUNCTION guard_content_reset_provider_release();

CREATE FUNCTION guard_content_reset_release_proof_commit() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM source_run_requests r
        JOIN source_run_attempts a ON a.tenant_id=r.tenant_id AND a.source_run_request_id=r.public_id
        WHERE r.tenant_id=NEW.tenant_id AND r.public_id=NEW.source_run_request_id
          AND a.public_id=NEW.source_run_attempt_id
          AND r.provider_effects_released_at=NEW.released_at AND a.provider_effects_released_at=NEW.released_at
    ) THEN
        RAISE EXCEPTION 'Provider release proof must commit with both release stamps';
    END IF;
    RETURN NEW;
END;
$$;
CREATE CONSTRAINT TRIGGER content_reset_release_proof_commit AFTER INSERT ON content_reset_provider_releases
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION guard_content_reset_release_proof_commit();

CREATE FUNCTION guard_content_reset_release_stamp() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE bound BOOLEAN;
BEGIN
    IF OLD.provider_effects_released_at IS NOT NULL AND NEW.provider_effects_released_at IS DISTINCT FROM OLD.provider_effects_released_at THEN
        RAISE EXCEPTION 'Provider release cannot be renewed or removed';
    END IF;
    IF TG_TABLE_NAME='source_run_attempts' AND OLD.provider_effects_released_at IS NOT NULL THEN
        IF NEW.state IN ('authorized','claimed','running')
           OR (NEW.tenant_id,NEW.public_id,NEW.source_run_request_id,NEW.content_source_id,NEW.fence_token,NEW.root_execution_unit_id)
              IS DISTINCT FROM
              (OLD.tenant_id,OLD.public_id,OLD.source_run_request_id,OLD.content_source_id,OLD.fence_token,OLD.root_execution_unit_id) THEN
            RAISE EXCEPTION 'Released replay attempt identity and closed effects cannot be reopened';
        END IF;
    END IF;
    IF NEW.provider_effects_released_at IS NULL OR OLD.provider_effects_released_at IS NOT NULL THEN RETURN NEW; END IF;
    SELECT EXISTS (
        SELECT 1 FROM content_reset_provider_releases proof
        JOIN source_run_requests r ON r.tenant_id=proof.tenant_id AND r.public_id=proof.source_run_request_id
        JOIN source_run_attempts a ON a.tenant_id=proof.tenant_id AND a.public_id=proof.source_run_attempt_id
        WHERE proof.tenant_id=NEW.tenant_id AND proof.released_at=NEW.provider_effects_released_at
          AND r.purpose='content_reset_replay'
          AND r.provider_effects_released_at=proof.released_at AND a.provider_effects_released_at=proof.released_at
          AND ((TG_TABLE_NAME='source_run_requests' AND r.public_id=NEW.public_id)
            OR (TG_TABLE_NAME='source_run_attempts' AND a.public_id=NEW.public_id))
    ) INTO bound;
    IF NOT bound THEN RAISE EXCEPTION 'Provider release stamps require the same immutable proof on request and attempt'; END IF;
    RETURN NEW;
END;
$$;
CREATE CONSTRAINT TRIGGER content_reset_request_release_stamp AFTER UPDATE ON source_run_requests
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION guard_content_reset_release_stamp();
CREATE CONSTRAINT TRIGGER content_reset_attempt_release_stamp AFTER UPDATE ON source_run_attempts
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION guard_content_reset_release_stamp();

CREATE FUNCTION fence_released_content_reset_unit() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE released BOOLEAN;
BEGIN
    IF TG_OP='DELETE' THEN
        SELECT provider_effects_released_at IS NOT NULL INTO released FROM source_run_attempts
        WHERE tenant_id=OLD.tenant_id AND public_id=OLD.source_run_attempt_id;
        IF released THEN RAISE EXCEPTION 'Released replay graph evidence cannot be deleted'; END IF;
        RETURN OLD;
    END IF;
    SELECT provider_effects_released_at IS NOT NULL INTO released FROM source_run_attempts
    WHERE tenant_id=NEW.tenant_id AND public_id=NEW.source_run_attempt_id;
    IF TG_OP='UPDATE' AND NOT COALESCE(released,FALSE) THEN
        SELECT provider_effects_released_at IS NOT NULL INTO released FROM source_run_attempts
        WHERE tenant_id=OLD.tenant_id AND public_id=OLD.source_run_attempt_id;
    END IF;
    IF released THEN
        IF TG_OP='INSERT' THEN RAISE EXCEPTION 'Released replay attempt cannot admit new units'; END IF;
        IF NEW.effect_started_at IS DISTINCT FROM OLD.effect_started_at
           OR NEW.execution_lease_token IS DISTINCT FROM OLD.execution_lease_token
           OR NEW.execution_lease_epoch IS DISTINCT FROM OLD.execution_lease_epoch
           OR NEW.state IN ('authorized','accepted','running')
           OR (NEW.tenant_id,NEW.public_id,NEW.source_run_request_id,NEW.source_run_attempt_id,NEW.content_source_id,NEW.parent_unit_id,NEW.unit_type,NEW.unit_key,NEW.page_id,NEW.batch_id,NEW.job_id,NEW.attempt_fence_token,NEW.declared_child_count,NEW.declared_child_digest)
              IS DISTINCT FROM
              (OLD.tenant_id,OLD.public_id,OLD.source_run_request_id,OLD.source_run_attempt_id,OLD.content_source_id,OLD.parent_unit_id,OLD.unit_type,OLD.unit_key,OLD.page_id,OLD.batch_id,OLD.job_id,OLD.attempt_fence_token,OLD.declared_child_count,OLD.declared_child_digest) THEN
            RAISE EXCEPTION 'Released replay effects cannot be reopened';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER content_reset_released_unit_fence BEFORE INSERT OR UPDATE OR DELETE ON source_run_execution_units
FOR EACH ROW EXECUTE FUNCTION fence_released_content_reset_unit();

CREATE FUNCTION fence_released_content_reset_request() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.provider_effects_released_at IS NOT NULL THEN
        IF NEW.manifest_state<>'sealed' OR NEW.state IN ('requested','accepted','running')
           OR (NEW.expected_unit_count,NEW.expected_page_count,NEW.expected_batch_count,NEW.root_execution_unit_id,NEW.manifest_version,NEW.manifest_sealed_at)
              IS DISTINCT FROM (OLD.expected_unit_count,OLD.expected_page_count,OLD.expected_batch_count,OLD.root_execution_unit_id,OLD.manifest_version,OLD.manifest_sealed_at) THEN
            RAISE EXCEPTION 'Released replay request cannot reopen its manifest';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER content_reset_released_request_fence BEFORE UPDATE ON source_run_requests
FOR EACH ROW EXECUTE FUNCTION fence_released_content_reset_request();

-- Replace BOTH exclusions together. Pending consumer verification remains
-- active/auditable; only proof-backed releases yield provider admission.
DROP INDEX uq_source_run_requests_one_active_source;
CREATE UNIQUE INDEX uq_source_run_requests_one_active_source
  ON source_run_requests(tenant_id,content_source_id)
  WHERE state IN ('requested','accepted','running','verification_required') AND provider_effects_released_at IS NULL;
DROP INDEX uq_source_run_active_provider_attempt;
CREATE UNIQUE INDEX uq_source_run_active_provider_attempt
  ON source_run_attempts(tenant_id,content_source_id)
  WHERE state IN ('authorized','claimed','running','verification_required') AND provider_effects_released_at IS NULL;
