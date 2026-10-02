-- Feed generations can host an isolated Content Reset candidate. Existing
-- generations retain the Feed Recovery behavior by default.
ALTER TABLE feed_generations
    ADD COLUMN IF NOT EXISTS purpose VARCHAR(24) NOT NULL DEFAULT 'feed_recovery';

ALTER TABLE feed_generations
    ADD COLUMN IF NOT EXISTS content_reset_campaign_id BIGINT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'feed_generations'::regclass
          AND conname = 'feed_generations_reset_campaign_fk'
    ) THEN
        ALTER TABLE feed_generations
            ADD CONSTRAINT feed_generations_reset_campaign_fk
            FOREIGN KEY (tenant_id, content_reset_campaign_id)
            REFERENCES content_reset_campaigns(tenant_id, id)
            ON DELETE RESTRICT;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'feed_generations'::regclass
          AND conname = 'feed_generations_purpose_check'
    ) THEN
        ALTER TABLE feed_generations
            ADD CONSTRAINT feed_generations_purpose_check
            CHECK (
                purpose IN ('feed_recovery','content_reset')
                AND ((purpose = 'content_reset') = (content_reset_campaign_id IS NOT NULL))
            );
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_feed_generations_content_reset_campaign
    ON feed_generations(tenant_id, content_reset_campaign_id, state)
    WHERE purpose = 'content_reset';
