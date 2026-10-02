-- Distinguish known admission waits from uncertain effects. No reset is enabled.
ALTER TABLE content_reset_steps DROP CONSTRAINT content_reset_steps_state_check;
ALTER TABLE content_reset_steps ADD CONSTRAINT content_reset_steps_state_check
    CHECK (state IN ('pending','deferred','claimed','waiting','outcome_unknown',
                     'succeeded','blocked','failed','cancelled'));

CREATE OR REPLACE FUNCTION protect_content_reset_step_settlement()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.state IN ('succeeded','cancelled') AND
       ROW(NEW.state, NEW.receipt, NEW.attempt_count, NEW.lease_token, NEW.lease_until, NEW.last_error)
       IS DISTINCT FROM ROW(OLD.state, OLD.receipt, OLD.attempt_count, OLD.lease_token, OLD.lease_until, OLD.last_error) THEN
        RAISE EXCEPTION 'Content Reset terminal owner receipt is immutable';
    END IF;
    IF NEW.state IS DISTINCT FROM OLD.state AND NOT (
        (OLD.state IN ('pending','deferred') AND NEW.state IN ('claimed','blocked','cancelled')) OR
        (OLD.state = 'claimed' AND NEW.state IN ('succeeded','waiting','deferred','blocked','failed','outcome_unknown')) OR
        (OLD.state IN ('waiting','outcome_unknown') AND NEW.state IN ('succeeded','waiting','blocked','failed','outcome_unknown'))
    ) THEN
        RAISE EXCEPTION 'Content Reset step transition is not permitted';
    END IF;
    IF NEW.state = 'claimed' AND (NEW.lease_token IS NULL OR NEW.lease_until IS NULL) THEN
        RAISE EXCEPTION 'Content Reset claimed step requires a lease';
    END IF;
    IF NEW.state IN ('succeeded','waiting','deferred') AND
       (NEW.receipt IS NULL OR NEW.receipt->>'state' IS DISTINCT FROM NEW.state
        OR NEW.receipt->>'command_id' IS DISTINCT FROM NEW.public_id::text
        OR NEW.receipt->>'command_hash' IS NULL OR NEW.receipt->>'command_hash' !~ '^[0-9a-f]{64}$') THEN
        RAISE EXCEPTION 'Content Reset progress requires a bound owner receipt';
    END IF;
    IF NEW.attempt_count < OLD.attempt_count OR NEW.attempt_count > OLD.attempt_count + 1 THEN
        RAISE EXCEPTION 'Content Reset owner attempt counter is invalid';
    END IF;
    RETURN NEW;
END;
$$;
