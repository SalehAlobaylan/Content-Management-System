-- Deterministic merge provenance for Studio review taxonomy. Existing prose is
-- untrusted/ambiguous, so legacy rows deliberately default to false.
ALTER TABLE chapters
    ADD COLUMN IF NOT EXISTS merged_short_provenance BOOLEAN NOT NULL DEFAULT FALSE;
