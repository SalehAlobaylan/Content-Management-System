-- Capacity admission is opt-in. Startup never creates or applies this schema.
-- Only CMS owns these tables; native execution/finance/health retain authority.
CREATE TABLE workload_governor_policies (
 scope varchar(32) PRIMARY KEY CHECK (scope='platform'),
 mode varchar(16) NOT NULL DEFAULT 'off' CHECK(mode IN ('off','observe','enforce')),
 revision bigint NOT NULL DEFAULT 1 CHECK(revision>0),
 enforced_classes jsonb NOT NULL DEFAULT '[]'::jsonb,
 resource_limits jsonb NOT NULL DEFAULT '{}'::jsonb,
 snapshot_max_age_seconds integer NOT NULL DEFAULT 90 CHECK(snapshot_max_age_seconds BETWEEN 10 AND 300),
 recovery_seconds integer NOT NULL DEFAULT 120 CHECK(recovery_seconds BETWEEN 10 AND 3600),
 lease_seconds integer NOT NULL DEFAULT 600 CHECK(lease_seconds BETWEEN 30 AND 3600),
 starvation_minutes integer NOT NULL DEFAULT 120 CHECK(starvation_minutes BETWEEN 5 AND 1440),
 observation_hours integer NOT NULL DEFAULT 24 CHECK(observation_hours>=24),
 min_class_requests integer NOT NULL DEFAULT 20 CHECK(min_class_requests>=20),
 updated_by varchar(120) NOT NULL DEFAULT '', paused_until timestamptz,
 updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE workload_permits (
 id uuid PRIMARY KEY,
 tenant_id varchar(64) NOT NULL CHECK(length(tenant_id)>0),
 request_key varchar(128) NOT NULL CHECK(length(request_key)>0),
 system varchar(80) NOT NULL,
 class varchar(48) NOT NULL,
 run_id varchar(128) NOT NULL,
 origin varchar(16) NOT NULL CHECK(origin IN ('automation','campaign','human')),
 owner_kind varchar(64) NOT NULL, owner_id varchar(128) NOT NULL,
 requested_units integer NOT NULL CHECK(requested_units BETWEEN 1 AND 10000),
 granted_units integer NOT NULL CHECK(granted_units BETWEEN 1 AND 10000),
 actual_units integer CHECK(actual_units BETWEEN 0 AND 10000),
 state varchar(24) NOT NULL CHECK(state IN ('acquired','running','handed_off','uncertain','released','expired','revoked')),
 mode varchar(16) NOT NULL CHECK(mode IN ('off','observe','enforce')),
 verdict varchar(24) NOT NULL, would_verdict varchar(24) NOT NULL,
 reasons jsonb NOT NULL DEFAULT '[]'::jsonb,
 reservations jsonb NOT NULL DEFAULT '{}'::jsonb,
 bypass boolean NOT NULL DEFAULT false, policy_revision bigint NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
 expires_at timestamptz NOT NULL, began_at timestamptz, released_at timestamptz,
 settlement_proof varchar(512) NOT NULL DEFAULT '',
 UNIQUE(tenant_id,request_key)
);
CREATE INDEX workload_permits_owner ON workload_permits(tenant_id,owner_kind,owner_id,created_at DESC);
CREATE INDEX workload_permits_active ON workload_permits(state,expires_at) WHERE state IN ('acquired','running','handed_off','uncertain');
CREATE INDEX workload_permits_history ON workload_permits(created_at DESC,id DESC);
CREATE INDEX workload_permits_class ON workload_permits(class,state);
CREATE TABLE workload_decisions (
 id bigserial PRIMARY KEY, permit_id varchar(36) NOT NULL DEFAULT '',
 tenant_id varchar(64) NOT NULL, system varchar(80) NOT NULL, class varchar(48) NOT NULL,
 event varchar(32) NOT NULL, verdict varchar(24) NOT NULL, would_verdict varchar(24) NOT NULL,
 requested_units integer NOT NULL DEFAULT 0, granted_units integer NOT NULL DEFAULT 0, actual_units integer,
 reasons jsonb NOT NULL DEFAULT '[]'::jsonb, evidence jsonb NOT NULL DEFAULT '{}'::jsonb,
 mode varchar(16) NOT NULL, policy_revision bigint NOT NULL,
 actor varchar(120) NOT NULL DEFAULT '', created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX workload_decisions_permit ON workload_decisions(permit_id,id DESC);
CREATE INDEX workload_decisions_window ON workload_decisions(created_at,class,event);
CREATE INDEX workload_decisions_tenant_system ON workload_decisions(tenant_id,system,class,created_at DESC);
CREATE TABLE workload_capacity_snapshots (
 id bigserial PRIMARY KEY, resource varchar(64) NOT NULL,
 state varchar(24) NOT NULL CHECK(state IN ('normal','constrained','saturated','recovering','unknown')),
 available boolean NOT NULL, healthy boolean NOT NULL, pressure double precision NOT NULL DEFAULT 0 CHECK(pressure>=0 AND pressure<1e12),
 reason varchar(256) NOT NULL DEFAULT '', observed_at timestamptz NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(), recovery_since timestamptz,
 evidence jsonb NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX workload_snapshots_latest ON workload_capacity_snapshots(resource,id DESC);
CREATE INDEX workload_snapshots_retention ON workload_capacity_snapshots(created_at,id);
