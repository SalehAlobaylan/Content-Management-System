-- Additive extension. No changes to paid-admission or owner settlement semantics.
CREATE TABLE economics_policies (tenant_id varchar(64) PRIMARY KEY, collection_enabled boolean NOT NULL DEFAULT false, reports_enabled boolean NOT NULL DEFAULT false, imports_enabled boolean NOT NULL DEFAULT false, allocation_enabled boolean NOT NULL DEFAULT false, updated_by text NOT NULL DEFAULT '', updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE economics_sources (id text PRIMARY KEY, tenant_id varchar(64) NOT NULL, provider varchar(64) NOT NULL, resource varchar(32) NOT NULL, scope text NOT NULL, currency varchar(3) NOT NULL DEFAULT 'USD', capability text NOT NULL DEFAULT 'manual', status text NOT NULL DEFAULT 'unavailable', note text NOT NULL DEFAULT '', config jsonb NOT NULL DEFAULT '{}', updated_at timestamptz NOT NULL DEFAULT now(), UNIQUE(tenant_id,provider,resource,scope));
CREATE INDEX economics_sources_tenant ON economics_sources(tenant_id);
CREATE TABLE economics_attributions (id text PRIMARY KEY, tenant_id varchar(64) NOT NULL, source varchar(32) NOT NULL, subject text NOT NULL, owner_subject text NOT NULL DEFAULT '', feature varchar(32) NOT NULL, benefit varchar(32) NOT NULL, content_id text NOT NULL DEFAULT '', created_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX economics_attributions_subject ON economics_attributions(tenant_id,subject);
CREATE TABLE economics_cost_revisions (id text PRIMARY KEY, tenant_id varchar(64) NOT NULL, source_id text NOT NULL, logical_key text NOT NULL, revision integer NOT NULL, provider varchar(64) NOT NULL, resource varchar(32) NOT NULL, feature varchar(32) NOT NULL, period_start timestamptz NOT NULL, period_end timestamptz NOT NULL, net numeric(30,12) NOT NULL, before_benefits numeric(30,12), currency varchar(3) NOT NULL, destination varchar(32) NOT NULL, subject text NOT NULL DEFAULT '', attribution_ref text NOT NULL DEFAULT '', content_id text NOT NULL DEFAULT '', method text NOT NULL DEFAULT '', metric text NOT NULL DEFAULT '', allocable_bps integer NOT NULL DEFAULT 0 CHECK(allocable_bps BETWEEN 0 AND 10000), reconciled boolean NOT NULL DEFAULT false, reason text NOT NULL, actor text NOT NULL, checksum text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(tenant_id,source_id,logical_key,revision), CHECK(period_end>period_start));
CREATE INDEX economics_cost_period ON economics_cost_revisions(tenant_id,period_start,period_end);
CREATE TABLE economics_usage_daily (tenant_id varchar(64) NOT NULL, day date NOT NULL, subject text NOT NULL, operation varchar(32) NOT NULL, unit varchar(32) NOT NULL, variant varchar(32) NOT NULL DEFAULT '', value bigint NOT NULL CHECK(value>=0), PRIMARY KEY(tenant_id,day,subject,operation,unit,variant));
CREATE INDEX economics_usage_subject ON economics_usage_daily(tenant_id,subject,day);
CREATE TABLE economics_usage_monthly (LIKE economics_usage_daily INCLUDING ALL);
CREATE TABLE economics_collection_state (tenant_id varchar(64) NOT NULL, source varchar(32) NOT NULL, epoch text NOT NULL, sequence bigint NOT NULL DEFAULT 0, last_hash text NOT NULL DEFAULT '', started_at timestamptz NOT NULL, last_seen_at timestamptz NOT NULL DEFAULT now(), lost bigint NOT NULL DEFAULT 0, closed boolean NOT NULL DEFAULT false, PRIMARY KEY(tenant_id,source,epoch));
CREATE TABLE economics_content_weights (tenant_id varchar(64) NOT NULL, month date NOT NULL, content_id text NOT NULL, subject text NOT NULL, segment text NOT NULL, kind varchar(24) NOT NULL, client varchar(32) NOT NULL DEFAULT 'unknown', value bigint NOT NULL CHECK(value>=0), observed_at timestamptz NOT NULL, PRIMARY KEY(tenant_id,month,content_id,subject,segment,kind,client));
CREATE INDEX economics_content_subject ON economics_content_weights(tenant_id,subject,month);
CREATE TABLE economics_profiles (tenant_id varchar(64) NOT NULL, resource varchar(32) NOT NULL, operation varchar(32) NOT NULL, version integer NOT NULL, effective_from date NOT NULL, weight bigint NOT NULL CHECK(weight>=0), evidence text NOT NULL, actor text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(tenant_id,resource,operation,version));
CREATE TABLE economics_cost_summaries (tenant_id varchar(64) NOT NULL, revision text NOT NULL, grain varchar(8) NOT NULL DEFAULT 'day', day date NOT NULL, subject text NOT NULL, resource varchar(32) NOT NULL, feature varchar(32) NOT NULL, direct numeric(30,12) NOT NULL, shared numeric(30,12) NOT NULL, before_benefits numeric(30,12), reconciled boolean NOT NULL, method text NOT NULL, source_id text NOT NULL, rule_version text NOT NULL, published_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX economics_summary_lookup ON economics_cost_summaries(tenant_id,grain,day,subject);
CREATE TABLE economics_publications (tenant_id varchar(64) PRIMARY KEY, revision text NOT NULL, published_at timestamptz NOT NULL);
CREATE TABLE economics_runs (id text PRIMARY KEY, tenant_id varchar(64) NOT NULL, status text NOT NULL, started_at timestamptz NOT NULL DEFAULT now(), completed_at timestamptz, rows integer NOT NULL DEFAULT 0, error text NOT NULL DEFAULT '');
CREATE INDEX economics_runs_tenant ON economics_runs(tenant_id,started_at DESC);
CREATE TABLE economics_issues (id text PRIMARY KEY, tenant_id varchar(64) NOT NULL, kind text NOT NULL, scope text NOT NULL, status text NOT NULL, headline text NOT NULL, evidence jsonb NOT NULL DEFAULT '{}', first_seen_at timestamptz NOT NULL DEFAULT now(), last_seen_at timestamptz NOT NULL DEFAULT now(), confirmations integer NOT NULL DEFAULT 1, close_reason text NOT NULL DEFAULT '');
CREATE INDEX economics_issues_tenant ON economics_issues(tenant_id,status,last_seen_at);
CREATE TABLE economics_lifecycle_barriers (subject_hash text PRIMARY KEY, erased boolean NOT NULL DEFAULT false, cleared_at timestamptz, updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE economics_receipt_links (event_id uuid PRIMARY KEY, tenant_id varchar(64) NOT NULL, attribution_ref text NOT NULL DEFAULT '', projected_at timestamptz);
CREATE TABLE economics_diagnostic_examples (id text PRIMARY KEY, tenant_id varchar(64) NOT NULL, subject text NOT NULL DEFAULT '', evidence jsonb NOT NULL DEFAULT '{}', created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE economics_import_windows (tenant_id varchar(64) NOT NULL, source text NOT NULL, window_key text NOT NULL, checksum text NOT NULL, payload jsonb NOT NULL, observed_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(tenant_id,source,window_key));
ALTER TABLE transcription_jobs ADD COLUMN IF NOT EXISTS economics_attribution_ref text NOT NULL DEFAULT '';
INSERT INTO economics_policies(tenant_id) VALUES('default') ON CONFLICT DO NOTHING;
CREATE TABLE economics_interaction_receipts (interaction_id uuid PRIMARY KEY, tenant_id varchar(64) NOT NULL, observed_at timestamptz NOT NULL, status text NOT NULL);
CREATE INDEX economics_interaction_receipts_retention ON economics_interaction_receipts(observed_at);

ALTER TABLE economics_cost_summaries ADD COLUMN expense_key text NOT NULL DEFAULT '';
CREATE INDEX economics_summary_expense ON economics_cost_summaries(tenant_id,expense_key);

CREATE TABLE economics_lifecycle_keys (singleton boolean PRIMARY KEY CHECK(singleton), key bytea NOT NULL CHECK(octet_length(key)=32));
INSERT INTO economics_lifecycle_keys VALUES(true,decode(replace(gen_random_uuid()::text,'-','')||replace(gen_random_uuid()::text,'-',''),'hex'));
CREATE TABLE economics_artifact_snapshots (tenant_id varchar(64) NOT NULL, artifact_id uuid NOT NULL, owner_subject text NOT NULL DEFAULT '', size_bytes bigint NOT NULL, observed_at timestamptz NOT NULL, owner_updated_at timestamptz NOT NULL, coverage_gap boolean NOT NULL DEFAULT false, PRIMARY KEY(tenant_id,artifact_id));
CREATE INDEX economics_artifact_snapshot_due ON economics_artifact_snapshots(tenant_id,observed_at);
CREATE TABLE economics_lifecycle_sync (singleton boolean PRIMARY KEY CHECK(singleton), cursor_time timestamptz NOT NULL, cursor_user uuid NOT NULL, synced_at timestamptz);
INSERT INTO economics_lifecycle_sync VALUES(true,'1970-01-01','00000000-0000-0000-0000-000000000000',NULL);

CREATE TABLE economics_content_rules(tenant_id varchar(64) NOT NULL,kind varchar(16) NOT NULL,version integer NOT NULL,effective_from date NOT NULL,allocable_bps integer NOT NULL CHECK(allocable_bps BETWEEN 0 AND 10000),observation varchar(32) NOT NULL,evidence text NOT NULL,actor text NOT NULL,created_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(tenant_id,kind,version));
ALTER TABLE economics_cost_summaries ADD COLUMN expense_revision integer NOT NULL DEFAULT 1;
ALTER TABLE economics_cost_revisions ADD COLUMN occurred_at timestamptz, ADD COLUMN posted_at timestamptz;
CREATE TABLE economics_pool_weights(tenant_id varchar(64) NOT NULL,expense_key text NOT NULL,rule_version text NOT NULL,weight bigint NOT NULL CHECK(weight>=0),PRIMARY KEY(tenant_id,expense_key,rule_version));
ALTER TABLE economics_policies ADD COLUMN collection_since timestamptz;

CREATE INDEX economics_pending_receipt_usage ON economics_receipt_links(tenant_id,event_id) WHERE projected_at IS NULL;
CREATE INDEX economics_cost_attribution ON economics_cost_revisions(tenant_id,attribution_ref) WHERE attribution_ref<>'';
CREATE INDEX economics_attribution_retention ON economics_attributions(tenant_id,created_at);
CREATE TABLE economics_owner_usage_receipts(tenant_id varchar(64) NOT NULL,owner varchar(32) NOT NULL,receipt_key text NOT NULL,observed_at timestamptz NOT NULL,PRIMARY KEY(tenant_id,owner,receipt_key));
CREATE INDEX economics_owner_usage_retention ON economics_owner_usage_receipts(observed_at);
