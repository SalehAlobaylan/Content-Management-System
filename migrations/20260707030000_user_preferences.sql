-- Authenticated-user preference state and derived affinity caches.

CREATE TABLE IF NOT EXISTS user_topic_prefs (
    user_id    UUID NOT NULL,
    topic_id   UUID NOT NULL,
    state      TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, topic_id)
);

CREATE INDEX IF NOT EXISTS idx_user_topic_prefs_topic ON user_topic_prefs (topic_id);

CREATE TABLE IF NOT EXISTS user_topic_affinity (
    user_id    UUID NOT NULL,
    topic_id   UUID NOT NULL,
    score      DOUBLE PRECISION NOT NULL,
    declared   BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, topic_id)
);

CREATE INDEX IF NOT EXISTS idx_user_topic_affinity_topic ON user_topic_affinity (topic_id);

CREATE TABLE IF NOT EXISTS user_category_affinity (
    user_id       UUID NOT NULL,
    category_slug TEXT NOT NULL,
    score         DOUBLE PRECISION NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, category_slug)
);

CREATE TABLE IF NOT EXISTS preference_settings (
    id                    BIGSERIAL PRIMARY KEY,
    tenant_id              VARCHAR(64) NOT NULL UNIQUE,
    foryou_enabled         BOOLEAN NOT NULL DEFAULT FALSE,
    news_enabled           BOOLEAN NOT NULL DEFAULT FALSE,
    w_foryou               DOUBLE PRECISION NOT NULL DEFAULT 0.30,
    w_news                 DOUBLE PRECISION NOT NULL DEFAULT 0.15,
    weight_complete        DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    weight_bookmark        DOUBLE PRECISION NOT NULL DEFAULT 0.9,
    weight_share           DOUBLE PRECISION NOT NULL DEFAULT 0.9,
    weight_like            DOUBLE PRECISION NOT NULL DEFAULT 0.7,
    weight_comment         DOUBLE PRECISION NOT NULL DEFAULT 0.5,
    weight_view            DOUBLE PRECISION NOT NULL DEFAULT 0.2,
    decay_half_life_days   DOUBLE PRECISION NOT NULL DEFAULT 30,
    declared_prior         DOUBLE PRECISION NOT NULL DEFAULT 3.0,
    category_discount      DOUBLE PRECISION NOT NULL DEFAULT 0.5,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS preference_stats (
    id                   BIGSERIAL PRIMARY KEY,
    tenant_id             VARCHAR(64) NOT NULL,
    day                   DATE NOT NULL,
    users_with_prefs      BIGINT NOT NULL DEFAULT 0,
    boosted_serves        BIGINT NOT NULL DEFAULT 0,
    total_serves          BIGINT NOT NULL DEFAULT 0,
    recompute_runs        BIGINT NOT NULL DEFAULT 0,
    recompute_ms_total    BIGINT NOT NULL DEFAULT 0,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, day)
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'user_topic_prefs_topic_fkey'
    ) THEN
        ALTER TABLE user_topic_prefs
            ADD CONSTRAINT user_topic_prefs_topic_fkey
            FOREIGN KEY (topic_id) REFERENCES topics(public_id) ON DELETE CASCADE;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'user_topic_affinity_topic_fkey'
    ) THEN
        ALTER TABLE user_topic_affinity
            ADD CONSTRAINT user_topic_affinity_topic_fkey
            FOREIGN KEY (topic_id) REFERENCES topics(public_id) ON DELETE CASCADE;
    END IF;
END $$;
