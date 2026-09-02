-- Canonical ingest idempotency key. Older databases received this column from
-- the former startup compatibility patch; fresh replays need it before the
-- durable content-stage migration replaces the legacy global index with a
-- tenant-scoped unique index.

ALTER TABLE content_items
    ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(512);
