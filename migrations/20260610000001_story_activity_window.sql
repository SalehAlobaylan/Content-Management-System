-- P1 hardening: story activity window.
--
-- last_member_at = publish time of a story's most recent member. The
-- classifier only matches items against stories active within
-- ±storyActivityWindowDays (7) of the item's own publish time, so stories stay
-- bounded to their event instead of absorbing semantically-similar items
-- forever and decaying into evergreen topics.
-- (Dev gets this via GORM AutoMigrate; this migration is for prod.)

ALTER TABLE topics ADD COLUMN IF NOT EXISTS last_member_at timestamptz;
CREATE INDEX IF NOT EXISTS idx_topics_last_member_at ON topics (last_member_at);
