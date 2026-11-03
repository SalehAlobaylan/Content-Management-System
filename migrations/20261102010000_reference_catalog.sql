-- Additive bookkeeping only; catalog definitions are applied explicitly, never at startup.
CREATE TABLE reference_catalog_bindings (
 id BIGSERIAL PRIMARY KEY,
 catalog_name TEXT NOT NULL CHECK(catalog_name='wahb'),
 scope_key TEXT NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN ('news-sources','media-sources','discovery-profiles','topics','topic-categories','quality-profiles')),
 catalog_key TEXT NOT NULL,
 target_id TEXT NOT NULL CHECK(target_id ~ '^[0-9]+$'),
 projection JSONB NOT NULL CHECK(jsonb_typeof(projection)='object'),
 definition_hash TEXT NOT NULL,
 release_version TEXT NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 CHECK(scope_key<>'@global' OR kind='quality-profiles'),
 UNIQUE(catalog_name,scope_key,kind,catalog_key),
 UNIQUE(catalog_name,scope_key,kind,target_id)
);
CREATE TABLE reference_catalog_applications (
 operation_id UUID PRIMARY KEY,
 plan_digest TEXT NOT NULL UNIQUE,
 catalog_hash TEXT NOT NULL,
 release_version TEXT NOT NULL,
 target TEXT NOT NULL,
 tenant_id TEXT NOT NULL,
 actor TEXT NOT NULL,
 evidence JSONB NOT NULL,
 committed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
