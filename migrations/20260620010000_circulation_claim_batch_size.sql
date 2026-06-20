-- News Circulation: make the source-claim batch size a tunable policy knob
-- instead of a hardcoded ceiling in the Aggregation worker. CMS now applies
-- source_claim_batch_size when the claim request omits an explicit limit.
ALTER TABLE news_circulation_policies
    ADD COLUMN IF NOT EXISTS source_claim_batch_size INTEGER NOT NULL DEFAULT 20;
