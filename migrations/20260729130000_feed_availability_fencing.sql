ALTER TABLE feed_availability_states
    ADD COLUMN IF NOT EXISTS fencing_token UUID;
