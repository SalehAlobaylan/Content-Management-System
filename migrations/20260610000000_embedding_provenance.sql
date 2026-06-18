-- P0 harness: embedding model provenance.
--
-- Records WHICH embedder produced each content item's text embedding. NULL =
-- no provenance (pre-provenance writer / wrong deployment) — the reconcile
-- sweep treats such vectors as missing and re-embeds them, so the corpus
-- converges on the current model instead of silently mixing embedding spaces
-- (the BGE-M3 → Qwen migration made this failure mode invisible without it).

ALTER TABLE content_items ADD COLUMN IF NOT EXISTS embedding_model varchar(80);

-- Backfill note: existing vectors should be stamped only after auditing them
-- against the live embedder (re-embed sample + cosine compare). The 2026-06-10
-- audit verified the full NEWS corpus as Qwen-written:
--   UPDATE content_items SET embedding_model = 'Qwen/Qwen3-Embedding-0.6B'
--   WHERE embedding IS NOT NULL;
