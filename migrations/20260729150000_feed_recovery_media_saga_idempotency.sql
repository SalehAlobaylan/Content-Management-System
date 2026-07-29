-- Backfill the durable per-item provider key introduced by the media purge
-- saga contract. Existing prepared/ambiguous rows must resume with the same
-- provider idempotency identity instead of receiving a new batch key.
UPDATE feed_recovery_media_purge_items
SET provider_idempotency_key = 'purge-media-item:' || manifest_hash || ':' || content_item_id::text,
    updated_at = NOW()
WHERE provider_idempotency_key IS NULL;

