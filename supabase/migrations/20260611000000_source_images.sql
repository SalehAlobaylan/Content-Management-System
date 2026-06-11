-- Optional source image/logo used by News feed cards when an item has no hero image.
ALTER TABLE content_sources ADD COLUMN IF NOT EXISTS image_url TEXT;
