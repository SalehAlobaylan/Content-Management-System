-- Story-product fixes: coverage-aware ranking.
--
-- story_coverage_weight lifts story momentum by ln(1 + recent members) so the
-- most-covered story of the day outranks fresher one-off posts — ranking now
-- expresses the aggregation model instead of pure per-item freshness.
-- (Dev gets the column via GORM AutoMigrate; this migration is for prod.)
ALTER TABLE ranking_configs
    ADD COLUMN IF NOT EXISTS story_coverage_weight double precision DEFAULT 0.30;

-- Related lists computed before the similarity floor (cosine ≥ 0.55) are
-- semantic noise — clear them so the backfill recomputes honest ones. Reads
-- fall back to the floored live kNN until then.
UPDATE topics SET related_ids = NULL;
