-- WP-10: a pre-effect lost acknowledgement adopts the same effect identity.
ALTER TABLE artifact_coverage_attempts
  ADD COLUMN IF NOT EXISTS adoption_count integer NOT NULL DEFAULT 0 CHECK (adoption_count >= 0),
  ADD COLUMN IF NOT EXISTS last_adopted_at timestamptz;

CREATE TABLE IF NOT EXISTS artifact_coverage_budget_reservations (
  id bigserial PRIMARY KEY,
  public_id uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id varchar(64) NOT NULL,
  request_id uuid NOT NULL,
  action_key varchar(128) NOT NULL,
  unit varchar(32) NOT NULL CHECK (unit IN ('media_minute','image_item','embedding_item','llm_call')),
  reserved_amount numeric(14,4) NOT NULL CHECK (reserved_amount > 0),
  settled_amount numeric(14,4),
  state varchar(16) NOT NULL CHECK (state IN ('reserved','settled','retained_uncertain','released')),
  evidence_digest char(64) NOT NULL,
  reserved_at timestamptz NOT NULL,
  settled_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, request_id),
  FOREIGN KEY (request_id) REFERENCES artifact_coverage_requests(public_id) ON DELETE RESTRICT
);
CREATE INDEX IF NOT EXISTS idx_artifact_budget_tenant_window
  ON artifact_coverage_budget_reservations(tenant_id, action_key, reserved_at DESC);

-- WP-16: qualification is tenant-bound and execution authority is explicit.
ALTER TABLE media_supply_qualification_reports
  ADD COLUMN IF NOT EXISTS tenant_id varchar(64);
UPDATE media_supply_qualification_reports reports
SET tenant_id = source.tenant_id
FROM (
  SELECT rc.report_id, MIN(c.tenant_id) AS tenant_id
  FROM media_supply_qualification_report_cases rc
  JOIN media_supply_qualification_cases c ON c.id = rc.case_id
  GROUP BY rc.report_id
) source
WHERE reports.id = source.report_id AND reports.tenant_id IS NULL;
-- Historical empty reports cannot be trusted for any tenant. Preserve them for
-- audit, but keep them outside every authenticated tenant scope.
UPDATE media_supply_qualification_reports
SET tenant_id = 'legacy_unknown'
WHERE tenant_id IS NULL;
ALTER TABLE media_supply_qualification_reports
  ALTER COLUMN tenant_id SET NOT NULL;
CREATE INDEX IF NOT EXISTS idx_media_supply_qualification_reports_tenant
  ON media_supply_qualification_reports(tenant_id, action_key, created_at DESC);

ALTER TABLE media_supply_action_previews
  ADD COLUMN IF NOT EXISTS execution_mode varchar(24) NOT NULL DEFAULT 'approval_required'
    CHECK (execution_mode IN ('approval_required','safe_auto')),
  ADD COLUMN IF NOT EXISTS promotion_id uuid;

ALTER TABLE media_supply_action_requests
  ADD COLUMN IF NOT EXISTS execution_mode varchar(24) NOT NULL DEFAULT 'approval_required'
    CHECK (execution_mode IN ('approval_required','safe_auto')),
  ADD COLUMN IF NOT EXISTS promotion_id uuid,
  ADD COLUMN IF NOT EXISTS qualification_report_id bigint;

ALTER TABLE media_supply_action_requests
  ADD CONSTRAINT media_supply_action_safe_auto_authority_check
  CHECK (
    (execution_mode = 'approval_required' AND promotion_id IS NULL AND qualification_report_id IS NULL)
    OR
    (execution_mode = 'safe_auto' AND promotion_id IS NOT NULL AND qualification_report_id IS NOT NULL)
  ) NOT VALID;
ALTER TABLE media_supply_action_requests
  VALIDATE CONSTRAINT media_supply_action_safe_auto_authority_check;

ALTER TABLE media_supply_action_previews
  ADD CONSTRAINT media_supply_action_previews_promotion_fk
  FOREIGN KEY (promotion_id) REFERENCES media_supply_action_promotions(public_id) ON DELETE RESTRICT NOT VALID;
ALTER TABLE media_supply_action_requests
  ADD CONSTRAINT media_supply_action_requests_promotion_fk
  FOREIGN KEY (promotion_id) REFERENCES media_supply_action_promotions(public_id) ON DELETE RESTRICT NOT VALID,
  ADD CONSTRAINT media_supply_action_requests_report_fk
  FOREIGN KEY (qualification_report_id) REFERENCES media_supply_qualification_reports(id) ON DELETE RESTRICT NOT VALID;
ALTER TABLE media_supply_action_previews VALIDATE CONSTRAINT media_supply_action_previews_promotion_fk;
ALTER TABLE media_supply_action_requests VALIDATE CONSTRAINT media_supply_action_requests_promotion_fk;
ALTER TABLE media_supply_action_requests VALIDATE CONSTRAINT media_supply_action_requests_report_fk;

CREATE INDEX IF NOT EXISTS idx_media_supply_action_execution_mode
  ON media_supply_action_requests(tenant_id, execution_mode, state, created_at);

ALTER TABLE media_supply_action_promotions
  ADD COLUMN IF NOT EXISTS environment_identity varchar(64),
  ADD COLUMN IF NOT EXISTS build_identity varchar(128);
UPDATE media_supply_action_promotions promotions
SET environment_identity = reports.environment_identity,
    build_identity = reports.build_identity
FROM media_supply_qualification_reports reports
WHERE reports.id = promotions.report_id
  AND (promotions.environment_identity IS NULL OR promotions.build_identity IS NULL);
UPDATE media_supply_action_promotions
SET environment_identity = COALESCE(environment_identity, 'legacy_unknown'),
    build_identity = COALESCE(build_identity, 'legacy_unknown');
ALTER TABLE media_supply_action_promotions
  ALTER COLUMN environment_identity SET NOT NULL,
    ALTER COLUMN build_identity SET NOT NULL;

-- Exact consumer-boundary observations. These rows are operational evidence,
-- not ranking/interaction state: the synthetic feed probe may append them
-- without creating or changing a consumer session.
CREATE TABLE IF NOT EXISTS pods_boundary_observations (
    public_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id VARCHAR(64) NOT NULL,
    content_item_id UUID NOT NULL,
    generation_id UUID NULL,
    boundary VARCHAR(24) NOT NULL CHECK (boundary IN ('feed_return','page_render','exact_view')),
    probe_kind VARCHAR(24) NOT NULL CHECK (probe_kind IN ('direct_probe','frozen_session','authenticated','anonymous')),
    probe_id VARCHAR(160) NOT NULL,
    source_run_request_row_id BIGINT NULL,
    verdict VARCHAR(16) NOT NULL CHECK (verdict IN ('present','absent','unknown')),
    provenance_digest CHAR(64) NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pods_boundary_observations_exact_identity
        UNIQUE (tenant_id, boundary, probe_kind, probe_id, content_item_id)
);

CREATE INDEX IF NOT EXISTS idx_pods_boundary_observations_lookup
    ON pods_boundary_observations (tenant_id, content_item_id, generation_id, boundary, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_pods_boundary_observations_retention
    ON pods_boundary_observations (created_at);

CREATE OR REPLACE FUNCTION pods_boundary_observations_immutable_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'Pods boundary observations are immutable';
END $$;
DROP TRIGGER IF EXISTS trg_pods_boundary_observations_immutable ON pods_boundary_observations;
CREATE TRIGGER trg_pods_boundary_observations_immutable
BEFORE UPDATE ON pods_boundary_observations
FOR EACH ROW EXECUTE FUNCTION pods_boundary_observations_immutable_guard();
