package controllers

import (
	"content-management-system/src/models"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Preferences Autopilot — scheduler (plan §16 file #7, §0.1.9). This REPLACES the
// bare StartTopicsHeartbeat. Exactly one maintenance path runs per tenant per due
// interval:
//   - disabled → the incumbent baseline primitive (runPreferenceBaseline), i.e.
//     today's behavior, now observable via last_run bookkeeping.
//   - observe  → the autopilot run, which itself runs the baseline once + shadow.
//   - safe_auto→ the autopilot run's bounded actions (baseline suppressed).
// There is never a second heartbeat racing the runner.

// StartPreferenceAutopilotHeartbeat launches the one-minute scheduler loop. It
// ensures the default policy row exists, runs an immediate due-pass (matching the
// old heartbeat's eager first tick), then ticks.
func StartPreferenceAutopilotHeartbeat(db *gorm.DB) {
	go func() {
		ensureDefaultPreferencePolicy(db)
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		runPreferenceAutopilotDue(db)
		for range ticker.C {
			runPreferenceAutopilotDue(db)
		}
	}()
}

func ensureDefaultPreferencePolicy(db *gorm.DB) {
	policy := models.DefaultPreferenceAutopilotPolicy(defaultCirculationTenant)
	_ = db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "tenant_id"}},
		DoNothing: true,
	}).Create(&policy).Error
}

func runPreferenceAutopilotDue(db *gorm.DB) {
	var policies []models.PreferenceAutopilotPolicy
	if err := db.Find(&policies).Error; err != nil {
		return
	}
	now := time.Now()
	for _, raw := range policies {
		policy := sanitizePreferenceAutopilotPolicy(raw)
		if raw.LastRunAt != nil && now.Sub(*raw.LastRunAt) < time.Duration(policy.IntervalMinutes)*time.Minute {
			continue
		}
		if policy.Enabled {
			if policy.PausedUntil != nil && policy.PausedUntil.After(now) {
				continue // paused: no scheduled run (policy untouched)
			}
			run, err := runPreferenceAutopilot(db, policy.TenantID, preferenceAutopilotRunOptions{
				Trigger: "scheduled", CreatedBy: "automation",
			})
			if errors.Is(err, errPreferenceAutopilotAlreadyRunning) {
				continue
			}
			payload := map[string]interface{}{"status": run.Status, "summary": run.Summary, "headline": run.Headline}
			if err != nil {
				payload["error"] = err.Error()
			}
			writeCirculationAuditSystem(db, policy.TenantID, "preferences.autopilot.scheduled", policy.TenantID, payload)
			continue
		}
		// Disabled tenant: keep the incumbent maintenance alive (today's behavior),
		// serialized against a manual run via the same in-flight mutex.
		if !tryStartPreferenceAutopilotRun(policy.TenantID) {
			continue
		}
		_ = runPreferenceBaseline(db, policy.TenantID, policy)
		touchPreferenceAutopilotLastRun(db, policy.TenantID, time.Now())
		finishPreferenceAutopilotRun(policy.TenantID)
	}
}
