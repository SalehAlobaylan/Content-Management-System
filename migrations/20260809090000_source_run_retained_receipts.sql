-- WP-08: immutable Aggregation receipt retention. Retaining a receipt does
-- not project or mutate source-run state; only the existing receipt ledger
-- admission path may do that.
CREATE TABLE IF NOT EXISTS source_run_retained_receipts (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
  tenant_id VARCHAR(64) NOT NULL,
  producer_event_key VARCHAR(255) NOT NULL,
  source_run_request_id UUID NOT NULL,
  source_run_attempt_id UUID NOT NULL,
  execution_unit_id UUID NOT NULL,
  payload_digest VARCHAR(128) NOT NULL,
  receipt JSONB NOT NULL,
  state VARCHAR(24) NOT NULL DEFAULT 'retained',
  delivered_receipt_id UUID NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT source_run_retained_receipts_state_check CHECK (state IN ('retained', 'delivered')),
  CONSTRAINT source_run_retained_receipts_key_unique UNIQUE (tenant_id, producer_event_key),
  CONSTRAINT source_run_retained_receipts_payload_digest_check CHECK (payload_digest ~ '^[a-f0-9]{64}$')
);

CREATE INDEX IF NOT EXISTS idx_source_run_retained_receipts_pending
  ON source_run_retained_receipts (tenant_id, source_run_request_id, state, created_at);

CREATE OR REPLACE FUNCTION source_run_retained_receipts_immutable_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.tenant_id <> NEW.tenant_id OR OLD.producer_event_key <> NEW.producer_event_key
     OR OLD.source_run_request_id <> NEW.source_run_request_id OR OLD.source_run_attempt_id <> NEW.source_run_attempt_id
     OR OLD.execution_unit_id <> NEW.execution_unit_id OR OLD.payload_digest <> NEW.payload_digest OR OLD.receipt <> NEW.receipt THEN
    RAISE EXCEPTION 'retained source-run receipt is immutable';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS source_run_retained_receipts_immutable ON source_run_retained_receipts;
CREATE TRIGGER source_run_retained_receipts_immutable
BEFORE UPDATE ON source_run_retained_receipts
FOR EACH ROW EXECUTE FUNCTION source_run_retained_receipts_immutable_guard();
