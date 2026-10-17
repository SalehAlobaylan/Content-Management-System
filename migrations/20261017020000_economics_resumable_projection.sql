-- Private, disposable build state. Published reporting remains in its existing owner tables.
CREATE TABLE economics_projection_jobs (
 tenant_id varchar(64) PRIMARY KEY,
 id text NOT NULL UNIQUE,
 started_at timestamptz NOT NULL,
 policy text NOT NULL,
 phase text NOT NULL DEFAULT 'invoices',
 cursor bigint NOT NULL DEFAULT 0,
 rows bigint NOT NULL DEFAULT 0
);
CREATE TABLE economics_projection_inputs (
 ordinal bigserial PRIMARY KEY,
 job_id text NOT NULL REFERENCES economics_projection_jobs(id) ON DELETE CASCADE,
 kind varchar(16) NOT NULL,
 scope text NOT NULL DEFAULT '',
 payload jsonb NOT NULL
);
CREATE INDEX economics_projection_page ON economics_projection_inputs(job_id,kind,ordinal);
CREATE INDEX economics_projection_scope ON economics_projection_inputs(job_id,kind,scope);
CREATE TABLE economics_projection_summaries (LIKE economics_cost_summaries INCLUDING DEFAULTS);
ALTER TABLE economics_projection_summaries ADD COLUMN job_id text NOT NULL REFERENCES economics_projection_jobs(id) ON DELETE CASCADE;
CREATE INDEX economics_projection_summary_job ON economics_projection_summaries(job_id);
CREATE INDEX economics_projection_summary_expense ON economics_projection_summaries(job_id,expense_key);
