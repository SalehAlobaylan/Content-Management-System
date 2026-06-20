-- News Circulation: automation heartbeat + safety controls. CMS runs a periodic
-- recommendation pass for tenants that opt in (automation_enabled). The cadence
-- mode decides what it does; these knobs bound how aggressively the auto loop may
-- act so it can never run away (velocity cap, confidence floor, cost asymmetry).
-- All default OFF / conservative — turning automation on is a deliberate choice.
ALTER TABLE news_circulation_policies
    ADD COLUMN IF NOT EXISTS automation_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS automation_interval_minutes INTEGER NOT NULL DEFAULT 60,
    ADD COLUMN IF NOT EXISTS auto_apply_speedups BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS max_auto_applies_per_run INTEGER NOT NULL DEFAULT 5,
    ADD COLUMN IF NOT EXISTS min_runs_for_auto INTEGER NOT NULL DEFAULT 4,
    ADD COLUMN IF NOT EXISTS last_automation_run_at TIMESTAMP;
