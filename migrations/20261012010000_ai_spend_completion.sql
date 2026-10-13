ALTER TABLE ai_spend_events ADD COLUMN event_count bigint NOT NULL DEFAULT 1 CHECK (event_count > 0);
ALTER TABLE ai_spend_events ADD COLUMN folded_at timestamptz;
ALTER TABLE ai_spend_events ADD COLUMN usage_unavailable boolean NOT NULL DEFAULT false;
ALTER TABLE ai_spend_rollups ADD COLUMN tenant_id varchar(64) NOT NULL DEFAULT 'legacy_unallocated';
ALTER TABLE ai_spend_rollups ADD COLUMN estimated_events bigint NOT NULL DEFAULT 0;
ALTER TABLE ai_spend_rollups ADD COLUMN unpriced_events bigint NOT NULL DEFAULT 0;
ALTER TABLE ai_spend_rollups ADD COLUMN unknown_events bigint NOT NULL DEFAULT 0;
ALTER TABLE ai_spend_rollups ADD COLUMN usage_unavailable_events bigint NOT NULL DEFAULT 0;
ALTER TABLE ai_spend_rollups ADD COLUMN avoided_estimated_usd double precision NOT NULL DEFAULT 0;
-- Historical rollups did not retain tenant dimensions. Preserve them honestly;
-- only raw rows already represented by those rollups are marked folded.
UPDATE ai_spend_events e SET folded_at=now() WHERE EXISTS (
 SELECT 1 FROM ai_spend_rollups r WHERE r.day=(e.occurred_at AT TIME ZONE 'UTC')::date
 AND r.spend_class=e.spend_class AND r.operation=e.operation
 AND r.provider=COALESCE(e.provider,'') AND r.model=COALESCE(e.model,'')
 AND r.trigger_source=e.trigger_source AND r.system_run_id=COALESCE(e.system_run_id,''));
DO $$ DECLARE c record; BEGIN
 FOR c IN SELECT conname FROM pg_constraint WHERE conrelid='ai_spend_rollups'::regclass AND contype='u' LOOP
 EXECUTE format('ALTER TABLE ai_spend_rollups DROP CONSTRAINT %I',c.conname); END LOOP;
END $$;
CREATE UNIQUE INDEX ai_spend_rollups_dimensions ON ai_spend_rollups(tenant_id,day,spend_class,operation,provider,model,trigger_source,system_run_id);
CREATE INDEX ai_spend_events_unfolded ON ai_spend_events(tenant_id,id) WHERE folded_at IS NULL;
CREATE INDEX ai_spend_events_window ON ai_spend_events(tenant_id,occurred_at);
CREATE TABLE ai_spend_receipts(event_id uuid PRIMARY KEY, created_at timestamptz NOT NULL DEFAULT now());
INSERT INTO ai_spend_receipts(event_id) SELECT event_id FROM ai_spend_events ON CONFLICT DO NOTHING;
CREATE TABLE ai_spend_emitters(process_id varchar(96) NOT NULL,tenant_id varchar(64) NOT NULL,source_service varchar(64) NOT NULL,
 sequence bigint NOT NULL DEFAULT 0,emitted bigint NOT NULL DEFAULT 0,accepted bigint NOT NULL DEFAULT 0,dropped bigint NOT NULL DEFAULT 0,
 unobservable bigint NOT NULL DEFAULT 0,started_at timestamptz NOT NULL,updated_at timestamptz NOT NULL,PRIMARY KEY(process_id,tenant_id));
CREATE TABLE ai_spend_overrides(id uuid PRIMARY KEY,tenant_id varchar(64) NOT NULL,trigger_source varchar(64) NOT NULL,
 created_by varchar(120) NOT NULL,expires_at timestamptz NOT NULL,created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE ai_spend_invoices(id bigserial PRIMARY KEY,tenant_id varchar(64) NOT NULL,provider varchar(64) NOT NULL,month date NOT NULL,
 amount_usd double precision NOT NULL CHECK(amount_usd>=0),note text NOT NULL DEFAULT '',created_by varchar(120) NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(),updated_at timestamptz NOT NULL DEFAULT now(),UNIQUE(tenant_id,provider,month));
ALTER TABLE ai_spend_episodes ADD COLUMN confirmations integer NOT NULL DEFAULT 0;
ALTER TABLE ai_spend_episodes ADD COLUMN healthy_runs integer NOT NULL DEFAULT 0;
ALTER TABLE ai_spend_episodes ADD COLUMN reopens integer NOT NULL DEFAULT 0;
CREATE TABLE ai_spend_stt_settlements(job_id uuid PRIMARY KEY, tenant_id varchar(64) NOT NULL,
 cost_usd double precision NOT NULL, owner_updated_at timestamptz NOT NULL,updated_at timestamptz NOT NULL DEFAULT now());
