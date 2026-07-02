-- Media Circulation Engine — stage 2 (advisory verdict/recommendation layer).
--
-- Foundation only (Slice 1): the tenant policy (circulation-specific tuning knobs;
-- storage cost/protection knobs stay in storage_policies) and the recommendation
-- ledger the layer emits. The recommendation table is created now but populated in
-- later slices; its shape is autopilot-ready (stable public_id, outcome, reason
-- snapshot) so the stage-5 Autopilot can consume this history as a track record.
-- See docs/media-circulation-engine.md.

CREATE TABLE IF NOT EXISTS media_circulation_policies (
    id bigserial PRIMARY KEY,
    tenant_id varchar(64) NOT NULL,
    enabled boolean NOT NULL DEFAULT false,
    preset varchar(32) NOT NULL DEFAULT 'balanced',
    value_floor double precision NOT NULL DEFAULT 0.15,
    marginal_margin double precision NOT NULL DEFAULT 0.10,
    max_intake_per_source_per_cycle integer NOT NULL DEFAULT 5,
    max_intake_per_cycle integer NOT NULL DEFAULT 25,
    source_min_interval_minutes integer NOT NULL DEFAULT 60,
    source_max_interval_minutes integer NOT NULL DEFAULT 10080,
    freshness_demand_weight double precision NOT NULL DEFAULT 0.20,
    last_evaluated_at timestamp,
    created_at timestamp NOT NULL DEFAULT now(),
    updated_at timestamp NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_media_circulation_policy_tenant
    ON media_circulation_policies (tenant_id);

CREATE TABLE IF NOT EXISTS media_circulation_recommendations (
    id bigserial PRIMARY KEY,
    public_id uuid NOT NULL DEFAULT gen_random_uuid(),
    tenant_id varchar(64) NOT NULL,
    unit_type varchar(24) NOT NULL,
    subject_id uuid NOT NULL,
    subject_kind varchar(24),
    verdict varchar(32) NOT NULL,
    action varchar(32) NOT NULL,
    score double precision NOT NULL DEFAULT 0,
    reasons jsonb,
    metrics jsonb,
    status varchar(24) NOT NULL DEFAULT 'pending',
    outcome varchar(32),
    applied boolean NOT NULL DEFAULT false,
    applied_at timestamp,
    applied_by varchar(255),
    created_at timestamp NOT NULL DEFAULT now(),
    updated_at timestamp NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_media_circ_recs_public_id
    ON media_circulation_recommendations (public_id);
CREATE INDEX IF NOT EXISTS idx_media_circ_recs_tenant_unit_status
    ON media_circulation_recommendations (tenant_id, unit_type, status);
CREATE INDEX IF NOT EXISTS idx_media_circ_recs_tenant_subject
    ON media_circulation_recommendations (tenant_id, subject_id);
