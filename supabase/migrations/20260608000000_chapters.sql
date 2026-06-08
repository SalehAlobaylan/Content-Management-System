-- Media Studio: first-class chapters table.
--
-- Chapters become editable, generatable (LLM) markers per transcript. Stored as
-- rows (not jsonb on the transcript) to support manual-vs-derived provenance and
-- clean editing. END time is DERIVED at read time (next chapter's start / media
-- duration) so the set is always contiguous + gapless — only start_ms is stored.
--
-- AutoMigrate handles the dev DB; this is the production-explicit version.

CREATE TABLE IF NOT EXISTS chapters (
    id            bigserial   PRIMARY KEY,
    public_id     uuid        NOT NULL DEFAULT gen_random_uuid(),
    transcript_id uuid        NOT NULL,
    tenant_id     varchar(64) NOT NULL DEFAULT 'default',
    title         text        NOT NULL,
    summary       text,
    start_ms      integer     NOT NULL,
    source        varchar(16) NOT NULL DEFAULT 'manual',  -- youtube | derived | manual
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_chapters_public_id ON chapters (public_id);
CREATE INDEX IF NOT EXISTS idx_chapters_transcript ON chapters (transcript_id);
CREATE INDEX IF NOT EXISTS idx_chapters_tenant ON chapters (tenant_id);

-- Chapters die with their transcript.
ALTER TABLE chapters
    ADD CONSTRAINT chapters_transcript_id_fkey
    FOREIGN KEY (transcript_id) REFERENCES transcripts (public_id) ON DELETE CASCADE;
