-- Permit canonical NEWS intake without rewriting retained legacy identities.
-- Historical migration bytes stay unchanged; existing legacy kinds remain valid.
ALTER TABLE content_items
    DROP CONSTRAINT IF EXISTS content_items_type_check;
ALTER TABLE content_items
    ADD CONSTRAINT content_items_type_check
    CHECK (type IN ('NEWS', 'VIDEO', 'PODCAST', 'ARTICLE', 'TWEET', 'COMMENT')) NOT VALID;
ALTER TABLE content_items VALIDATE CONSTRAINT content_items_type_check;
