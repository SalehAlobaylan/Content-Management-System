ALTER TABLE user_interactions
    ADD COLUMN IF NOT EXISTS comment_moderation_status VARCHAR(16),
    ADD COLUMN IF NOT EXISTS comment_moderation_reason VARCHAR(64);

CREATE INDEX IF NOT EXISTS idx_comment_moderation_queue
    ON user_interactions(comment_moderation_status, created_at DESC)
    WHERE type = 'comment';
