-- Explicit, immutable replay branches. No live source pointer/index is changed.
CREATE UNIQUE INDEX IF NOT EXISTS uq_content_reset_replay_source_identity
    ON content_sources(tenant_id, public_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_content_reset_replay_request_identity
    ON source_run_requests(tenant_id, public_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_content_reset_replay_receipt_identity
    ON source_run_receipts(tenant_id, public_id);
CREATE TABLE content_reset_replay_branches (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    tenant_id VARCHAR(64) NOT NULL,
    campaign_id BIGINT NOT NULL,
    revision_id BIGINT NOT NULL,
    content_source_id UUID NOT NULL,
    manifest_hash CHAR(64) NOT NULL,
    spec JSONB NOT NULL CHECK (jsonb_typeof(spec) = 'object'),
    spec_hash CHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, public_id),
    UNIQUE (tenant_id, revision_id, content_source_id),
    FOREIGN KEY (tenant_id, campaign_id, revision_id)
        REFERENCES content_reset_revisions(tenant_id, campaign_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, content_source_id)
        REFERENCES content_sources(tenant_id, public_id) ON DELETE RESTRICT
);
CREATE TABLE content_reset_replay_pages (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    tenant_id VARCHAR(64) NOT NULL,
    branch_id UUID NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal BETWEEN 1 AND 100),
    source_run_request_id UUID NOT NULL,
    input_cursor TEXT NOT NULL CHECK (octet_length(input_cursor) <= 4096),
    input_cursor_hash CHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, public_id),
    UNIQUE (tenant_id, source_run_request_id),
    UNIQUE (tenant_id, branch_id, ordinal),
    UNIQUE (tenant_id, branch_id, input_cursor_hash),
    FOREIGN KEY (tenant_id, branch_id)
        REFERENCES content_reset_replay_branches(tenant_id, public_id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, source_run_request_id)
        REFERENCES source_run_requests(tenant_id, public_id) ON DELETE RESTRICT
);
CREATE TABLE content_reset_replay_checkpoints (
    id BIGSERIAL PRIMARY KEY,
    public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    tenant_id VARCHAR(64) NOT NULL,
    page_id UUID NOT NULL,
    provider_receipt_id UUID NOT NULL,
    next_cursor TEXT NOT NULL CHECK (octet_length(next_cursor) <= 4096),
    next_cursor_hash CHAR(64) NOT NULL,
    exhausted BOOLEAN NOT NULL,
    observed INTEGER NOT NULL CHECK (observed BETWEEN 0 AND 100),
    admitted INTEGER NOT NULL CHECK (admitted BETWEEN 0 AND 100),
    outside_window INTEGER NOT NULL CHECK (outside_window BETWEEN 0 AND 100),
    observed_bytes BIGINT NOT NULL CHECK (observed_bytes BETWEEN 0 AND 8388608),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (observed = admitted + outside_window),
    CHECK (exhausted = (next_cursor = '')),
    UNIQUE (tenant_id, page_id),
    UNIQUE (tenant_id, provider_receipt_id),
    FOREIGN KEY (tenant_id, page_id)
        REFERENCES content_reset_replay_pages(tenant_id, public_id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, provider_receipt_id)
        REFERENCES source_run_receipts(tenant_id, public_id) ON DELETE RESTRICT
);
CREATE TRIGGER content_reset_replay_branches_immutable
BEFORE UPDATE OR DELETE ON content_reset_replay_branches
FOR EACH ROW EXECUTE FUNCTION reject_content_reset_execution_evidence_mutation();
CREATE TRIGGER content_reset_replay_pages_immutable
BEFORE UPDATE OR DELETE ON content_reset_replay_pages
FOR EACH ROW EXECUTE FUNCTION reject_content_reset_execution_evidence_mutation();
CREATE TRIGGER content_reset_replay_checkpoints_immutable
BEFORE UPDATE OR DELETE ON content_reset_replay_checkpoints
FOR EACH ROW EXECUTE FUNCTION reject_content_reset_execution_evidence_mutation();

-- One bounded fetch child per replay request, even if an old worker tries to
-- expand provider pagination. Ordinary source-run graphs remain unaffected.
CREATE FUNCTION guard_content_reset_replay_unit() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE replay BOOLEAN;
BEGIN
    SELECT purpose = 'content_reset_replay' INTO replay FROM source_run_requests
    WHERE tenant_id = NEW.tenant_id AND public_id = NEW.source_run_request_id;
    IF replay AND NEW.unit_type = 'fetch_page' THEN
        IF NEW.page_id <> 'initial' OR NEW.unit_key <> 'fetch:initial' THEN
            RAISE EXCEPTION 'Content Reset replay requires one bounded initial page per request';
        END IF;
        PERFORM pg_advisory_xact_lock(hashtextextended('content-reset-page/v1/' || NEW.tenant_id || '/' || NEW.source_run_request_id::text, 0));
        IF EXISTS (SELECT 1 FROM source_run_execution_units
                   WHERE tenant_id = NEW.tenant_id AND source_run_request_id = NEW.source_run_request_id
                     AND unit_type = 'fetch_page' AND public_id <> NEW.public_id) THEN
            RAISE EXCEPTION 'Content Reset replay request already has its provider page';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER content_reset_replay_unit_guard BEFORE INSERT ON source_run_execution_units
FOR EACH ROW EXECUTE FUNCTION guard_content_reset_replay_unit();

-- Request insertion and page binding commit atomically. Recheck the current
-- row at commit because budgets/state can change inside its insertion tx.
CREATE FUNCTION guard_content_reset_replay_request() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE bound BOOLEAN;
BEGIN
    IF NEW.purpose <> 'content_reset_replay' THEN RETURN NEW; END IF;
    SELECT EXISTS (
        SELECT 1 FROM source_run_requests r
        JOIN content_reset_replay_pages p ON p.tenant_id=r.tenant_id AND p.source_run_request_id=r.public_id
        JOIN content_reset_replay_branches b ON b.tenant_id=p.tenant_id AND b.public_id=p.branch_id
        JOIN content_reset_campaigns c ON c.tenant_id=b.tenant_id AND c.id=b.campaign_id
        JOIN content_reset_revisions v ON v.tenant_id=b.tenant_id AND v.id=b.revision_id AND v.campaign_id=c.id
        WHERE r.tenant_id=NEW.tenant_id AND r.public_id=NEW.public_id
          AND r.content_source_id=b.content_source_id AND r.operator_plan_id=c.public_id
          AND r.metadata->>'content_reset_campaign_id'=c.public_id::text
          AND r.metadata->>'content_reset_revision_id'=v.public_id::text
          AND r.metadata->>'content_reset_manifest_hash'=b.manifest_hash
          AND r.metadata->'content_reset_replay'->>'branchId'=b.public_id::text
          AND r.metadata->'content_reset_replay'->>'pageId'=p.public_id::text
          AND r.metadata->'content_reset_replay'->>'specHash'=b.spec_hash
          AND r.metadata->'content_reset_replay'->>'ordinal'=p.ordinal::text
          AND COALESCE(r.metadata->'content_reset_replay'->>'inputCursor','')=p.input_cursor
    ) INTO bound;
    IF NOT bound THEN RAISE EXCEPTION 'Content Reset replay request lacks its immutable branch/page binding'; END IF;
    RETURN NEW;
END;
$$;
CREATE CONSTRAINT TRIGGER content_reset_replay_request_guard AFTER INSERT ON source_run_requests
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION guard_content_reset_replay_request();

CREATE FUNCTION protect_content_reset_replay_request_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.purpose = 'content_reset_replay' OR NEW.purpose = 'content_reset_replay' THEN
        IF (NEW.public_id,NEW.tenant_id,NEW.content_source_id,NEW.purpose,NEW.lane,
            NEW.metadata,NEW.operator_plan_id,NEW.operator_step_id,NEW.idempotency_key,
            NEW.policy_fingerprint,NEW.argument_fingerprint,NEW.cadence_window_start,
            NEW.item_cap,NEW.byte_cap,NEW.provider_call_cap,NEW.workload_cap)
           IS DISTINCT FROM
           (OLD.public_id,OLD.tenant_id,OLD.content_source_id,OLD.purpose,OLD.lane,
            OLD.metadata,OLD.operator_plan_id,OLD.operator_step_id,OLD.idempotency_key,
            OLD.policy_fingerprint,OLD.argument_fingerprint,OLD.cadence_window_start,
            OLD.item_cap,OLD.byte_cap,OLD.provider_call_cap,OLD.workload_cap) THEN
            RAISE EXCEPTION 'Content Reset replay request identity and bounds are immutable';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER content_reset_replay_request_identity_guard BEFORE UPDATE ON source_run_requests
FOR EACH ROW EXECUTE FUNCTION protect_content_reset_replay_request_identity();

CREATE FUNCTION guard_content_reset_replay_page_binding() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE previous_cursor TEXT;
BEGIN
    IF NEW.ordinal = 1 THEN
        IF NEW.input_cursor <> '' THEN RAISE EXCEPTION 'Initial replay page cannot have a cursor'; END IF;
    ELSE
        SELECT checkpoint.next_cursor INTO previous_cursor
        FROM content_reset_replay_pages page
        JOIN content_reset_replay_checkpoints checkpoint ON checkpoint.tenant_id=page.tenant_id AND checkpoint.page_id=page.public_id
        WHERE page.tenant_id=NEW.tenant_id AND page.branch_id=NEW.branch_id
          AND page.ordinal=NEW.ordinal-1 AND checkpoint.exhausted=FALSE;
        IF previous_cursor IS NULL OR previous_cursor <> NEW.input_cursor THEN
            RAISE EXCEPTION 'Replay continuation requires its preceding verified observation checkpoint';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER content_reset_replay_page_binding_guard BEFORE INSERT ON content_reset_replay_pages
FOR EACH ROW EXECUTE FUNCTION guard_content_reset_replay_page_binding();

CREATE FUNCTION guard_content_reset_replay_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE bound BOOLEAN;
BEGIN
    SELECT EXISTS (
        SELECT 1 FROM content_reset_replay_pages p
        JOIN content_reset_replay_branches b ON b.tenant_id=p.tenant_id AND b.public_id=p.branch_id
        JOIN source_run_receipts r ON r.tenant_id=p.tenant_id AND r.source_run_request_id=p.source_run_request_id
        JOIN source_run_execution_units u ON u.tenant_id=r.tenant_id AND u.public_id=r.execution_unit_id
        WHERE p.tenant_id=NEW.tenant_id AND p.public_id=NEW.page_id AND r.public_id=NEW.provider_receipt_id
          AND r.content_source_id=b.content_source_id AND r.stage='fetch' AND r.event_type='provider_terminal'
          AND r.final_page=TRUE AND r.page_id='initial' AND r.outcome IN ('new_items','no_change')
          AND u.state='succeeded' AND u.unit_type='fetch_page'
          AND r.payload->'content_reset_replay'->>'branch_id'=b.public_id::text
          AND r.payload->'content_reset_replay'->>'page_id'=p.public_id::text
          AND r.payload->'content_reset_replay'->>'spec_hash'=b.spec_hash
          AND (r.payload->'content_reset_replay'->>'complete')::boolean=TRUE
          AND (r.payload->'content_reset_replay'->>'observed')::integer=NEW.observed
          AND (r.payload->'content_reset_replay'->>'admitted')::integer=NEW.admitted
          AND (r.payload->'content_reset_replay'->>'outside_window')::integer=NEW.outside_window
          AND (r.payload->'content_reset_replay'->>'observed_bytes')::bigint=NEW.observed_bytes
          AND r.payload->'content_reset_replay'->>'next_cursor'=NEW.next_cursor
          AND (r.payload->'content_reset_replay'->>'exhausted')::boolean=NEW.exhausted
    ) INTO bound;
    IF NOT bound THEN RAISE EXCEPTION 'Replay checkpoint does not match complete native provider evidence'; END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER content_reset_replay_checkpoint_guard BEFORE INSERT ON content_reset_replay_checkpoints
FOR EACH ROW EXECUTE FUNCTION guard_content_reset_replay_checkpoint();
