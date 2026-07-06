-- Media Studio Clearance Autopilot — stage 6, Slice 1 (persistence foundation).
--
-- The editorial-clearance helper downstream of the Media Circulation Autopilot
-- (see docs/media-studio-autopilot-plan.md, grill S1–S20 / H1–H9). Three parts:
--   1. Normalize chapter review reasons to codes (needs_review_code[s]) so the
--      trust gate keys on a fixed taxonomy, not free text (S4/S5).
--   2. media_studio_autopilot_policies — per-tenant automation knobs (S17, H8).
--   3. media_studio_runs / media_studio_actions — the ledger pair, modeled on
--      media_circulation_runs / _actions.

-- ---------------------------------------------------------------
-- 1. Chapter review-reason codes (S4/S5)
--    needs_review_code   = primary (most-editorial) code
--    needs_review_codes  = full set (multi-flag chapters are never auto-published)
-- ---------------------------------------------------------------
ALTER TABLE chapters
    ADD COLUMN IF NOT EXISTS needs_review_code varchar(32),
    ADD COLUMN IF NOT EXISTS needs_review_codes text[];

-- Backfill existing rows from the free-text reason + structural fields, matching
-- the exact constants Aggregation emits (atomization.helpers.ts). Rows whose text
-- matches no constant stay NULL / unclassified and earn no trust (S4). The
-- low_confidence default threshold (0.82) mirrors the code default
-- HighConfidenceThreshold; live ingest uses the effective policy value.
UPDATE chapters SET needs_review_code = CASE
        WHEN contains_sponsor_intro THEN 'sponsor_intro'
        WHEN needs_review_reason = 'Fallback single chapter; planner returned no usable chapters.' THEN 'planner_fallback'
        WHEN confidence IS NOT NULL AND confidence < 0.82
             AND needs_review_reason IS NULL
             AND (boundary_reason IS NULL OR boundary_reason NOT LIKE '%merged_short_chapter%') THEN 'low_confidence'
        WHEN boundary_reason LIKE '%merged_short_chapter%'
             AND needs_review_reason IS NULL
             AND (confidence IS NULL OR confidence >= 0.82) THEN 'merged_short'
        WHEN needs_review_reason = 'Chapter below 4:30 and cannot merge without exceeding hard max.' THEN 'short_unmergeable'
        WHEN needs_review_reason = 'Chapter is below the 4:30 minimum feed duration.' THEN 'below_min'
        WHEN needs_review_reason = 'Chapter exceeds hard maximum duration.' THEN 'above_hard_max'
        ELSE NULL
    END
    WHERE status = 'needs_review' OR needs_review_reason IS NOT NULL;

-- Full code set: union of every condition that holds, so single-code detection
-- (S5) is exact. Order is irrelevant here; primary precedence is applied in code.
UPDATE chapters SET needs_review_codes = (
        SELECT array_remove(ARRAY[
            CASE WHEN contains_sponsor_intro THEN 'sponsor_intro' END,
            CASE WHEN needs_review_reason = 'Fallback single chapter; planner returned no usable chapters.' THEN 'planner_fallback' END,
            CASE WHEN confidence IS NOT NULL AND confidence < 0.82
                      AND (boundary_reason IS NULL OR boundary_reason NOT LIKE '%merged_short_chapter%') THEN 'low_confidence' END,
            CASE WHEN boundary_reason LIKE '%merged_short_chapter%' THEN 'merged_short' END,
            CASE WHEN needs_review_reason = 'Chapter below 4:30 and cannot merge without exceeding hard max.' THEN 'short_unmergeable' END,
            CASE WHEN needs_review_reason = 'Chapter is below the 4:30 minimum feed duration.' THEN 'below_min' END,
            CASE WHEN needs_review_reason = 'Chapter exceeds hard maximum duration.' THEN 'above_hard_max' END
        ], NULL)
    )
    WHERE status = 'needs_review' OR needs_review_reason IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_chapters_needs_review_code
    ON chapters (needs_review_code);

-- ---------------------------------------------------------------
-- 2. Per-tenant autopilot policy (S17 — its own table, not on the
--    atomization policy: automation cadence has no override hierarchy)
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS media_studio_autopilot_policies (
    id bigserial PRIMARY KEY,
    tenant_id varchar(64) NOT NULL UNIQUE,

    autopilot_enabled boolean NOT NULL DEFAULT false,
    autopilot_mode varchar(24) NOT NULL DEFAULT 'observe',
    observe_proposals boolean NOT NULL DEFAULT false,

    interval_minutes integer NOT NULL DEFAULT 360,       -- sweep-up; chain does real-time work (H8)
    chain_debounce_minutes integer NOT NULL DEFAULT 15,  -- S8
    max_clears_per_run integer NOT NULL DEFAULT 10,      -- H8
    max_publishes_per_run integer NOT NULL DEFAULT 5,    -- H8 sub-cap
    max_rejects_per_run integer NOT NULL DEFAULT 10,     -- H8 sub-cap
    max_stt_per_run integer NOT NULL DEFAULT 3,          -- + monthly budget guard (S11)
    max_proposals_per_run integer NOT NULL DEFAULT 15,   -- H2
    aged_threshold_days integer NOT NULL DEFAULT 7,      -- H6
    dirty_workbench_minutes integer NOT NULL DEFAULT 30, -- S6 courtesy layer

    trust_min_decisions integer NOT NULL DEFAULT 20,     -- H5
    trust_min_approve_pct integer NOT NULL DEFAULT 90,   -- H5
    trust_max_reversal_pct integer NOT NULL DEFAULT 5,   -- H5

    paused_until timestamp,
    last_run_at timestamp,

    created_at timestamp NOT NULL DEFAULT now(),
    updated_at timestamp NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------
-- 3. Runs + actions ledger (mirrors media_circulation_runs / _actions)
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS media_studio_runs (
    id bigserial PRIMARY KEY,
    public_id uuid NOT NULL DEFAULT gen_random_uuid(),
    tenant_id varchar(64) NOT NULL,
    trigger varchar(24) NOT NULL,         -- chained | interval | manual
    mode varchar(24) NOT NULL,            -- observe | safe_auto
    status varchar(24) NOT NULL,          -- running | completed | partial | failed
    started_at timestamp NOT NULL,
    finished_at timestamp,
    summary text,
    health_before jsonb,
    health_after jsonb,
    created_by varchar(255),
    error text,
    created_at timestamp NOT NULL DEFAULT now(),
    updated_at timestamp NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_media_studio_runs_public_id
    ON media_studio_runs (public_id);
CREATE INDEX IF NOT EXISTS idx_media_studio_runs_tenant
    ON media_studio_runs (tenant_id);
CREATE INDEX IF NOT EXISTS idx_media_studio_runs_status
    ON media_studio_runs (status);
CREATE INDEX IF NOT EXISTS idx_media_studio_runs_started_at
    ON media_studio_runs (started_at);

CREATE TABLE IF NOT EXISTS media_studio_actions (
    id bigserial PRIMARY KEY,
    public_id uuid NOT NULL DEFAULT gen_random_uuid(),
    run_id bigint NOT NULL,
    tenant_id varchar(64) NOT NULL,

    unit_type varchar(24) NOT NULL,       -- chapter_review | transcript_case
    chapter_id uuid,
    content_item_id uuid,
    recommendation_id uuid,               -- emit_reatomize_recommendation link (H1)

    verdict varchar(40) NOT NULL,
    tool_name varchar(80) NOT NULL,
    status varchar(24) NOT NULL,          -- success | error | approval_required | skipped | would_apply | would_skip | would_propose
    reason text,
    guardrail varchar(64),                -- skip/would-skip reason taxonomy (§9)

    -- LLM proposal payload + second-phase human outcome (S19)
    proposal jsonb,
    proposal_model varchar(80),
    proposal_confidence double precision,
    human_outcome varchar(24),            -- accepted | overridden
    human_outcome_by varchar(255),
    human_outcome_at timestamp,

    input jsonb,
    output jsonb,
    error text,
    feed_impact integer NOT NULL DEFAULT 0,
    stt_impact integer NOT NULL DEFAULT 0,

    started_at timestamp NOT NULL,
    finished_at timestamp,
    created_at timestamp NOT NULL DEFAULT now(),
    updated_at timestamp NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_media_studio_actions_public_id
    ON media_studio_actions (public_id);
CREATE INDEX IF NOT EXISTS idx_media_studio_actions_run_id
    ON media_studio_actions (run_id);
CREATE INDEX IF NOT EXISTS idx_media_studio_actions_tenant
    ON media_studio_actions (tenant_id);
CREATE INDEX IF NOT EXISTS idx_media_studio_actions_status
    ON media_studio_actions (status);
CREATE INDEX IF NOT EXISTS idx_media_studio_actions_chapter
    ON media_studio_actions (chapter_id);
CREATE INDEX IF NOT EXISTS idx_media_studio_actions_content_item
    ON media_studio_actions (content_item_id);
