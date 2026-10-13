-- Preserve authenticated human grant attribution after raw event retention.
ALTER TABLE ai_spend_events ADD COLUMN override_id uuid, ADD COLUMN human_actor_id text NOT NULL DEFAULT '';
ALTER TABLE ai_spend_rollups ADD COLUMN over_cap_human_events bigint NOT NULL DEFAULT 0;
CREATE TABLE ai_spend_override_uses (
 event_id uuid PRIMARY KEY REFERENCES ai_spend_receipts(event_id),
 override_id uuid NOT NULL REFERENCES ai_spend_overrides(id),
 tenant_id varchar(64) NOT NULL,
 human_actor_id text NOT NULL,
 trigger_source varchar(64) NOT NULL,
 system_run_id varchar(96) NOT NULL DEFAULT '',
 occurred_at timestamptz NOT NULL
);
CREATE INDEX ai_spend_override_uses_tenant_run ON ai_spend_override_uses(tenant_id,system_run_id);
ALTER TABLE ai_spend_emitters ADD COLUMN guard_unavailable bigint NOT NULL DEFAULT 0,
 ADD COLUMN last_loss_at timestamptz, ADD COLUMN last_guard_unavailable_at timestamptz;
UPDATE ai_spend_emitters SET last_loss_at=updated_at WHERE dropped>0 OR unobservable>0;
