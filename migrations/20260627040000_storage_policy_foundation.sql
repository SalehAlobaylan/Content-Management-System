-- Canonical replacement for the retired GORM-created storage_policies table.
CREATE TABLE IF NOT EXISTS storage_policies (
    id bigserial PRIMARY KEY,
    tenant_id varchar(64),
    enabled boolean NOT NULL DEFAULT false,
    max_storage_bytes bigint NOT NULL DEFAULT 5368709120,
    target_utilization_pct integer NOT NULL DEFAULT 80,
    min_age_days integer NOT NULL DEFAULT 14,
    min_view_count_for_keep integer NOT NULL DEFAULT 5,
    sweep_interval_minutes integer NOT NULL DEFAULT 60,
    delete_failed_immediately boolean NOT NULL DEFAULT true,
    preserve_thumbnails boolean NOT NULL DEFAULT true,
    protect_top_n_by_views integer NOT NULL DEFAULT 50,
    protect_top_n_window_days integer NOT NULL DEFAULT 30,
    archive_action varchar(20) NOT NULL DEFAULT 're_encode',
    re_encode_target_profile_id bigint,
    class_a_free_budget bigint NOT NULL DEFAULT 1000000,
    class_b_free_budget bigint NOT NULL DEFAULT 10000000,
    class_a_warn_pct integer NOT NULL DEFAULT 80,
    class_a_cap_pct integer NOT NULL DEFAULT 95,
    class_b_warn_pct integer NOT NULL DEFAULT 80,
    class_b_cap_pct integer NOT NULL DEFAULT 95,
    last_sweep_at timestamp,
    created_at timestamp NOT NULL DEFAULT now(),
    updated_at timestamp NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_storage_policy_tenant ON storage_policies (tenant_id);
