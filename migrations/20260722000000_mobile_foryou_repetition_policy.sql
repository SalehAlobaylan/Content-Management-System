ALTER TABLE ranking_configs
    ADD COLUMN IF NOT EXISTS foryou_completed_repeat_days integer NOT NULL DEFAULT 90,
    ADD COLUMN IF NOT EXISTS foryou_meaningful_repeat_days integer NOT NULL DEFAULT 30,
    ADD COLUMN IF NOT EXISTS foryou_sample_repeat_days integer NOT NULL DEFAULT 7;

ALTER TABLE ranking_configs
    ALTER COLUMN show_watched_when_unseen_exhausted SET DEFAULT false;

UPDATE ranking_configs
   SET show_watched_when_unseen_exhausted = false
 WHERE show_watched_when_unseen_exhausted = true;

CREATE INDEX IF NOT EXISTS idx_user_interactions_repetition_session
    ON user_interactions (session_id, type, created_at DESC, content_item_id);
CREATE INDEX IF NOT EXISTS idx_user_interactions_repetition_user
    ON user_interactions (user_id, type, created_at DESC, content_item_id);
