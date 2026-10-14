-- Additive Observe-only telemetry contract; preserves all processing history.
ALTER TABLE experience_events ADD COLUMN phase varchar(16);
ALTER TABLE experience_events ADD COLUMN outcome varchar(24);
ALTER TABLE experience_events ADD COLUMN terminal_status varchar(16);
ALTER TABLE experience_events ADD COLUMN observed_playback_ms integer;
ALTER TABLE experience_events ADD COLUMN stall_count integer;
ALTER TABLE experience_events ADD COLUMN position_drift_ms integer;
ALTER TABLE experience_events ADD COLUMN fallback_used boolean;
ALTER TABLE experience_events ADD COLUMN boundary_starved boolean;
ALTER TABLE experience_metric_rollups ADD COLUMN histogram_version integer NOT NULL DEFAULT 1;
ALTER TABLE experience_metric_rollups ADD COLUMN active_sessions bigint NOT NULL DEFAULT 0;
ALTER TABLE experience_metric_rollups ADD COLUMN affected_sessions bigint NOT NULL DEFAULT 0;
ALTER TABLE experience_policies ADD COLUMN latency_sample_floor integer NOT NULL DEFAULT 50 CHECK (latency_sample_floor > 0);
ALTER TABLE experience_policies ADD COLUMN last_minute_bucket timestamptz;
ALTER TABLE experience_incidents ADD COLUMN last_evaluated_bucket timestamptz;
ALTER TABLE experience_incidents ADD COLUMN confirmed_at timestamptz;
ALTER TABLE experience_incidents ADD COLUMN investigation_path varchar(200);
CREATE TABLE experience_terminal_receipts (
 identity char(64) PRIMARY KEY, digest char(64) NOT NULL, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE experience_rollup_progress (
 tenant_id varchar(64) NOT NULL, resolution varchar(8) NOT NULL,
 bucket_start timestamptz NOT NULL, cursor bigint NOT NULL DEFAULT 0,
 events_processed bigint NOT NULL DEFAULT 0, updated_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY (tenant_id, resolution)
);
CREATE INDEX experience_events_window_cursor ON experience_events(tenant_id, received_at, id);
CREATE INDEX experience_events_terminal_population ON experience_events(tenant_id, surface, received_at, session_id)
 WHERE schema_version=2 AND (terminal_status='accepted' OR event_type='client_failure');
-- No activation, content mutation, legacy metric reinterpretation or startup patch.
CREATE UNIQUE INDEX experience_incidents_active_fingerprint ON experience_incidents(tenant_id, fingerprint)
 WHERE status IN ('confirming','open','recovering');
ALTER TABLE experience_metric_rollups ADD COLUMN completed boolean NOT NULL DEFAULT false;
ALTER TABLE experience_metric_rollups ADD COLUMN outcome_count bigint NOT NULL DEFAULT 0;
