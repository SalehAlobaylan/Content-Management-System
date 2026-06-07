-- Caption-first transcription + chapters + on-demand STT.
--
-- Adds:
--  1. transcripts: segments/chapters (jsonb) + source/provider provenance.
--  2. content_items: lightweight caption_state + transcript_source (decoupled
--     from the heavy transcript row, for fast feed filtering + console badges).
--  3. transcription_configs: single-row-per-tenant admin config (auto-STT toggle,
--     provider, monthly budget cap + spend tracking) — mirrors ranking_configs.
--
-- AutoMigrate handles the dev DB; this is the production-explicit version.

-- 1. Transcript caption-first columns ────────────────────────────────────────
ALTER TABLE transcripts
    ADD COLUMN IF NOT EXISTS segments jsonb,
    ADD COLUMN IF NOT EXISTS chapters jsonb,
    ADD COLUMN IF NOT EXISTS source   varchar(32),
    ADD COLUMN IF NOT EXISTS provider varchar(64);

-- 2. ContentItem lightweight provenance state ─────────────────────────────────
--    caption_state: none | youtube_auto | youtube_human | stt_done
--    (never-downgrade state machine; drives the idempotency/budget guard).
ALTER TABLE content_items
    ADD COLUMN IF NOT EXISTS caption_state     varchar(20),
    ADD COLUMN IF NOT EXISTS transcript_source varchar(32);

CREATE INDEX IF NOT EXISTS idx_content_items_caption_state
    ON content_items (caption_state);

-- 3. Transcription config (admin-tunable, per-tenant) ─────────────────────────
CREATE TABLE IF NOT EXISTS transcription_configs (
    id                     bigserial PRIMARY KEY,
    tenant_id              varchar(64)      NOT NULL DEFAULT 'default',
    auto_stt_enabled       boolean          NOT NULL DEFAULT false,
    provider               varchar(32)      NOT NULL DEFAULT 'deepgram',
    monthly_budget_cap_usd double precision NOT NULL DEFAULT 0,
    monthly_spend_usd      double precision NOT NULL DEFAULT 0,
    monthly_window_start   timestamp        NOT NULL DEFAULT now(),
    created_at             timestamp        NOT NULL DEFAULT now(),
    updated_at             timestamp        NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_transcription_config_tenant
    ON transcription_configs (tenant_id);
