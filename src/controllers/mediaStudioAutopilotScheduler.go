package controllers

import (
	"errors"
	"time"

	"content-management-system/src/models"

	"gorm.io/gorm"
)

// Media Studio Clearance Autopilot — stage 6, Slice 3 (scheduler + chain).
//
// The helper's primary trigger is the CHAIN: a one-directional lead→helper poll
// (H1) that fires a run after the lead executed atomize_now, so review residue
// is triaged in the same cycle it was created. The interval is a slower sweep-up
// for manual-atomization residue. Pause gates every trigger (S7); a 15-minute
// debounce prevents chain thrash (S8). No in-process coupling to the lead runner
// (S9) — this side only reads the lead's already-written ledger.

func StartMediaStudioAutopilotHeartbeat(db *gorm.DB) {
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			runStudioAutopilotDue(db)
		}
	}()
}

func runStudioAutopilotDue(db *gorm.DB) {
	var policies []models.MediaStudioAutopilotPolicy
	if err := db.Where("autopilot_enabled = ?", true).Find(&policies).Error; err != nil {
		return
	}
	now := time.Now().UTC()
	for _, raw := range policies {
		policy := sanitizeMediaStudioAutopilotPolicy(raw)
		chainAvailable := studioChainAvailable(db, policy.TenantID, raw.LastRunAt)
		trigger, run := decideStudioTrigger(now, raw.LastRunAt, policy, chainAvailable)
		if !run {
			continue
		}
		result, _, err := runMediaStudioAutopilot(db, policy.TenantID, studioAutopilotRunOptions{
			Trigger:   trigger,
			CreatedBy: "automation",
		})
		if errors.Is(err, errStudioAutopilotAlreadyRunning) || errors.Is(err, errStudioAutopilotPaused) {
			continue
		}
		payload := map[string]interface{}{"trigger": trigger, "status": result.Status, "summary": result.Summary}
		if err != nil {
			payload["error"] = err.Error()
		}
		writeCirculationAuditSystem(db, policy.TenantID, "media_studio.autopilot.scheduled", policy.TenantID, payload)
	}
}

// decideStudioTrigger is the pure trigger decision (S7/S8): pause blocks
// everything; a chain fires when the lead has qualifying work and the debounce
// has elapsed; otherwise the interval sweep-up fires when due. Returns the
// trigger name and whether to run.
func decideStudioTrigger(now time.Time, lastRun *time.Time, policy models.MediaStudioAutopilotPolicy, chainAvailable bool) (string, bool) {
	if policy.PausedUntil != nil && policy.PausedUntil.After(now) {
		return "", false
	}
	debounce := time.Duration(policy.ChainDebounceMinutes) * time.Minute
	debounceElapsed := lastRun == nil || now.Sub(*lastRun) >= debounce
	if chainAvailable && debounceElapsed {
		return models.StudioRunTriggerChained, true
	}
	interval := time.Duration(policy.IntervalMinutes) * time.Minute
	intervalElapsed := lastRun == nil || now.Sub(*lastRun) >= interval
	if intervalElapsed {
		return models.StudioRunTriggerInterval, true
	}
	return "", false
}

// studioChainAvailable reports whether the lead (Circulation Autopilot) executed
// a successful atomize_now since this helper last ran (S8/S9). Success-only, so
// the lead's Observe (would_apply) runs never chain. Reads only the lead's
// persisted ledger — no coupling.
func studioChainAvailable(db *gorm.DB, tenantID string, lastRun *time.Time) bool {
	since := time.Time{}
	if lastRun != nil {
		since = *lastRun
	}
	var count int64
	err := db.Table("media_circulation_runs r").
		Joins("JOIN media_circulation_actions a ON a.run_id = r.id").
		Where("r.tenant_id = ? AND a.tool_name = ? AND a.status = ? AND r.finished_at IS NOT NULL AND r.finished_at > ?",
			tenantID, "recommendation."+mediaCircVerdictAtomizeNow, models.MediaAutopilotActionStatusSuccess, since).
		Count(&count).Error
	if err != nil {
		return false
	}
	return count > 0
}

// emitReatomizeRecommendation writes a pending atomize_now recommendation into
// the LEAD's ledger for a parent whose transcript just improved (H1: Studio
// never atomizes; it asks the lead, whose gates decide). No-op if the parent is
// ineligible or already has a pending atomize_now recommendation.
func emitReatomizeRecommendation(db *gorm.DB, tenantID string, parent *models.ContentItem) *models.MediaCirculationRecommendation {
	if parent == nil || parent.DurationSec == nil || *parent.DurationSec <= forYouHardMaxDurationSec {
		return nil // only >40m parents are atomization-eligible
	}
	var existing int64
	_ = db.Model(&models.MediaCirculationRecommendation{}).
		Where("tenant_id = ? AND subject_id = ? AND verdict = ? AND status = ?",
			tenantID, parent.PublicID, mediaCircVerdictAtomizeNow, models.MediaCirculationRecStatusPending).
		Count(&existing).Error
	if existing > 0 {
		return nil
	}
	rec := models.MediaCirculationRecommendation{
		TenantID:    tenantID,
		UnitType:    models.MediaCirculationUnitItemFamily,
		SubjectID:   parent.PublicID,
		SubjectKind: "content_item",
		Verdict:     mediaCircVerdictAtomizeNow,
		Action:      "atomize",
		Score:       float64(*parent.DurationSec) / 3600.0,
		Reasons:     marshalAutopilotJSON([]string{"Transcript improved by Studio Autopilot; parent may re-atomize better."}),
		Status:      models.MediaCirculationRecStatusPending,
	}
	if err := db.Create(&rec).Error; err != nil {
		return nil
	}
	return &rec
}
