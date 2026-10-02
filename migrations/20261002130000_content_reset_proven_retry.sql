ALTER TABLE content_reset_decisions ADD COLUMN public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE;
ALTER TABLE content_reset_decisions ADD COLUMN command_id UUID;
ALTER TABLE content_reset_decisions ADD CONSTRAINT content_reset_retry_decision_scope
    CHECK ((action='retry') = (command_id IS NOT NULL));
ALTER TABLE content_reset_steps ADD COLUMN retry_decision_id UUID;
ALTER TABLE content_reset_steps ADD CONSTRAINT content_reset_step_retry_authority
    FOREIGN KEY (retry_decision_id) REFERENCES content_reset_decisions(public_id)
    DEFERRABLE INITIALLY DEFERRED;

CREATE OR REPLACE FUNCTION protect_content_reset_step_settlement()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE retry BOOLEAN;
BEGIN
    retry := COALESCE(OLD.state='failed' AND NEW.state='pending'
       AND NEW.retry_decision_id IS NOT NULL AND NEW.retry_decision_id IS DISTINCT FROM OLD.retry_decision_id
       AND OLD.receipt->>'state'='failed'
       AND OLD.receipt->>'command_id'=OLD.public_id::text
       AND OLD.receipt->>'command_hash' ~ '^[0-9a-f]{64}$'
       AND jsonb_typeof(OLD.receipt->'no_effect_proven')='boolean'
       AND OLD.receipt->>'no_effect_proven'='true'
       AND NEW.receipt IS NOT DISTINCT FROM OLD.receipt
       AND OLD.lease_token IS NULL AND NEW.lease_token IS NULL AND NEW.lease_until IS NULL
       AND NEW.attempt_count=OLD.attempt_count, FALSE);
    IF OLD.state IN ('succeeded','cancelled','failed','blocked') AND NOT retry AND
       ROW(NEW.state, NEW.receipt, NEW.attempt_count, NEW.lease_token, NEW.lease_until, NEW.last_error)
       IS DISTINCT FROM ROW(OLD.state, OLD.receipt, OLD.attempt_count, OLD.lease_token, OLD.lease_until, OLD.last_error) THEN
        RAISE EXCEPTION 'Content Reset terminal owner receipt is immutable';
    END IF;
    IF NEW.retry_decision_id IS DISTINCT FROM OLD.retry_decision_id AND NOT retry THEN
        RAISE EXCEPTION 'Content Reset retry authority cannot be replaced outside a proven retry';
    END IF;
    IF NEW.state IS DISTINCT FROM OLD.state AND NOT (
        retry OR
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
    IF NEW.state='deferred' AND (jsonb_typeof(NEW.receipt->'no_effect_proven') IS DISTINCT FROM 'boolean'
       OR NEW.receipt->>'no_effect_proven' IS DISTINCT FROM 'true') THEN
        RAISE EXCEPTION 'Content Reset deferral requires proof of no admitted effect';
    END IF;
    IF NEW.attempt_count < OLD.attempt_count OR NEW.attempt_count > OLD.attempt_count + 1 THEN
        RAISE EXCEPTION 'Content Reset owner attempt counter is invalid';
    END IF;
    RETURN NEW;
END;
$$;

-- The controller persists its exact response after preparing the retry in the
-- same transaction. Commit requires the new globally one-use reauth decision.
CREATE FUNCTION assert_content_reset_retry_decision()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.state='failed' AND NEW.state='pending' AND NOT EXISTS (
        SELECT 1 FROM content_reset_decisions d
        JOIN content_reset_executions e ON e.tenant_id=d.tenant_id AND e.campaign_id=d.campaign_id
        WHERE d.public_id=NEW.retry_decision_id AND d.tenant_id=NEW.tenant_id AND d.campaign_id=NEW.campaign_id
          AND d.command_id=NEW.public_id AND d.action='retry' AND d.proof_hash IS NOT NULL
          AND e.revision_id=NEW.revision_id AND e.pause_requested
          AND d.response->'execution'->>'manifest_hash'=e.manifest_hash
          AND d.response->'execution'->>'version'=e.version::text
    ) THEN
        RAISE EXCEPTION 'Content Reset retry requires its current reauthenticated campaign decision';
    END IF;
    RETURN NULL;
END;
$$;
CREATE CONSTRAINT TRIGGER content_reset_retry_decision_guard
AFTER UPDATE ON content_reset_steps DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION assert_content_reset_retry_decision();
