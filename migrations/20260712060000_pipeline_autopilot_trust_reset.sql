-- Pipeline trust confidence can be reset by an operator after an outage.
ALTER TABLE pipeline_autopilot_policies
    ADD COLUMN IF NOT EXISTS trust_reset_at timestamp;
