-- Preference-setting domains are code policy shared by the CMS API and Console.
-- Reconcile legacy values first; this migration is canonical and is not applied
-- automatically to any shared or production database.
UPDATE preference_settings
SET
    w_foryou = CASE WHEN w_foryou BETWEEN 0 AND 1 THEN w_foryou ELSE 0.30 END,
    w_news = CASE WHEN w_news BETWEEN 0 AND 1 THEN w_news ELSE 0.15 END,
    weight_complete = CASE WHEN weight_complete BETWEEN 0 AND 5 THEN weight_complete ELSE 1.0 END,
    weight_bookmark = CASE WHEN weight_bookmark BETWEEN 0 AND 5 THEN weight_bookmark ELSE 0.9 END,
    weight_share = CASE WHEN weight_share BETWEEN 0 AND 5 THEN weight_share ELSE 0.9 END,
    weight_like = CASE WHEN weight_like BETWEEN 0 AND 5 THEN weight_like ELSE 0.7 END,
    weight_comment = CASE WHEN weight_comment BETWEEN 0 AND 5 THEN weight_comment ELSE 0.5 END,
    weight_view = CASE WHEN weight_view BETWEEN 0 AND 5 THEN weight_view ELSE 0.2 END,
    decay_half_life_days = CASE WHEN decay_half_life_days BETWEEN 0.25 AND 365 THEN decay_half_life_days ELSE 30 END,
    declared_prior = CASE WHEN declared_prior BETWEEN 0 AND 5 THEN declared_prior ELSE 3 END,
    category_discount = CASE WHEN category_discount BETWEEN 0 AND 1 THEN category_discount ELSE 0.5 END,
    updated_at = NOW();

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_preference_settings_weights') THEN
        ALTER TABLE preference_settings ADD CONSTRAINT chk_preference_settings_weights CHECK (
            w_foryou BETWEEN 0 AND 1 AND w_news BETWEEN 0 AND 1 AND
            weight_complete BETWEEN 0 AND 5 AND weight_bookmark BETWEEN 0 AND 5 AND
            weight_share BETWEEN 0 AND 5 AND weight_like BETWEEN 0 AND 5 AND
            weight_comment BETWEEN 0 AND 5 AND weight_view BETWEEN 0 AND 5 AND
            declared_prior BETWEEN 0 AND 5 AND category_discount BETWEEN 0 AND 1
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_preference_settings_half_life') THEN
        ALTER TABLE preference_settings ADD CONSTRAINT chk_preference_settings_half_life
            CHECK (decay_half_life_days BETWEEN 0.25 AND 365);
    END IF;
END $$;
