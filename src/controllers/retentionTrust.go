package controllers

// Slice 10: Retention trust promotion. Trust is derived from the immutable
// RetentionAction ledger and reset through an auditable human action; no new
// mutable "trust score" can silently promote a destructive capability.

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"content-management-system/src/models"

	"github.com/gin-gonic/gin"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type retentionTrustStat struct {
	ActionClass       string  `json:"action_class"`
	State             string  `json:"state"`
	ShadowRuns        int64   `json:"shadow_runs"`
	AssistDecisions   int64   `json:"assist_decisions"`
	Agreed            int64   `json:"agreed"`
	Disagreed         int64   `json:"disagreed"`
	AgreementPct      float64 `json:"agreement_pct"`
	Failures          int64   `json:"failures"`
	BreakerOpen       bool    `json:"breaker_open"`
	AutoEligible      bool    `json:"auto_eligible"`
	PromotionReady    bool    `json:"promotion_ready"`
	FailureWindowEnds string  `json:"failure_window_ends"`
}

type retentionPromotionStatus struct {
	Mode            string               `json:"mode"`
	ManualFlipOnly  bool                 `json:"manual_flip_only"`
	SafeAutoAllowed bool                 `json:"safe_auto_allowed"`
	BlockedReason   string               `json:"blocked_reason,omitempty"`
	Trust           []retentionTrustStat `json:"trust"`
}

type retentionSatelliteStatus struct {
	State           string `json:"state"`
	CompletedCycles int64  `json:"completed_cycles"`
	RequiredCycles  int    `json:"required_cycles"`
	Recommendation  string `json:"recommendation"`
	Reason          string `json:"reason"`
}

// The satellite is a structural escape hatch, not a migration performed by
// Retention. After two completed monthly cycles, expose a deterministic
// recommendation from measured allocation/trend so an operator can decide.
func retentionSatelliteEvaluation(db *gorm.DB, tenant string, policy models.RetentionPolicy, sample *models.RetentionDBSample, forecast retentionForecast) retentionSatelliteStatus {
	result := retentionSatelliteStatus{State: "not_ready", RequiredCycles: 2, Recommendation: "wait_for_two_monthly_cycles", Reason: "Two completed monthly cycles are required before structural evaluation"}
	// A completed cycle is a finalized immutable Month in Review revision. Do
	// not count the mutable retention-month bookkeeping row as an archive.
	db.Model(&models.NewsMonthArchive{}).
		Where("tenant_id=? AND state=?", tenant, "finalized").
		Distinct("month_start").
		Count(&result.CompletedCycles)
	if result.CompletedCycles < int64(result.RequiredCycles) {
		return result
	}
	result.State = "evaluated"
	if sample == nil {
		result.Recommendation = "collect_measurements"
		result.Reason = "Two cycles exist, but no current allocation measurement is available"
		return result
	}
	if sample.DatabaseBytes > policy.DatabaseTargetBytes || forecast.GrowthBytesPerDay > 0 {
		result.Recommendation = "evaluate_heavy_news_satellite"
		result.Reason = "Measured allocation remains above target or continues to grow after two cycles"
		return result
	}
	result.Recommendation = "retain_v1_anchor_design"
	result.Reason = "Allocation is at or below target with no positive measured growth"
	return result
}

func retentionTrustClasses() []string {
	return []string{models.RetentionActionRefreshNewsSnapshots}
}

func retentionModeRank(mode string) int {
	switch mode {
	case models.RetentionModeSafeAuto:
		return 3
	case models.RetentionModeAssist:
		return 2
	default:
		return 1
	}
}

func retentionActionAutoEligible(actionClass string) bool {
	// Canonical News rows, historical rows, physical rewrites, Storage, Media,
	// and Purge & Reseed remain human-owned regardless of trust history.
	return actionClass == models.RetentionActionRefreshNewsSnapshots
}

func retentionActionMode(policy models.RetentionPolicy, actionClass string) string {
	mode := policy.Mode
	var overrides map[string]string
	if len(policy.ActionModes) > 0 && jsonUnmarshalRetentionModes(policy.ActionModes, &overrides) == nil {
		if candidate := overrides[actionClass]; candidate == models.RetentionModeObserve || candidate == models.RetentionModeAssist || candidate == models.RetentionModeSafeAuto {
			if retentionModeRank(candidate) < retentionModeRank(mode) {
				mode = candidate
			}
		}
	}
	return mode
}

// Kept as a small seam so policy JSON parsing stays fail-closed and testable.
func jsonUnmarshalRetentionModes(raw datatypes.JSON, target *map[string]string) error {
	return json.Unmarshal(raw, target)
}

func retentionTrust(db *gorm.DB, tenant, actionClass string, policy models.RetentionPolicy) retentionTrustStat {
	stat := retentionTrustStat{ActionClass: actionClass, State: "probation", AutoEligible: retentionActionAutoEligible(actionClass)}
	failureSince := time.Now().UTC().Add(-24 * time.Hour)
	var reset models.AuditLog
	if db.Where("tenant_id=? AND action=? AND target_resource=? AND status='success'", tenant, "retention.autopilot.breaker.reset", actionClass).Order("created_at DESC").First(&reset).Error == nil && reset.CreatedAt.After(failureSince) {
		failureSince = reset.CreatedAt
	}
	stat.FailureWindowEnds = failureSince.Add(24 * time.Hour).UTC().Format(time.RFC3339)
	db.Model(&models.RetentionAction{}).Where("tenant_id=? AND action_class=? AND mode=? AND outcome=?", tenant, actionClass, models.RetentionModeObserve, models.RetentionActionWouldExecute).Count(&stat.ShadowRuns)
	// Decisions are immutable evidence. Execution outcomes intentionally do not
	// participate here: an approved action may later become running,
	// verification_failed, or verification_passed without erasing the human
	// agreement that earned (or failed to earn) trust.
	db.Model(&models.RetentionActionDecision{}).Where("tenant_id=? AND action_class=? AND mode=? AND decision IN ?", tenant, actionClass, models.RetentionModeAssist, []string{"approved", "rejected"}).Count(&stat.AssistDecisions)
	db.Model(&models.RetentionActionDecision{}).Where("tenant_id=? AND action_class=? AND mode=? AND decision=?", tenant, actionClass, models.RetentionModeAssist, "approved").Count(&stat.Agreed)
	db.Model(&models.RetentionActionDecision{}).Where("tenant_id=? AND action_class=? AND mode=? AND decision=?", tenant, actionClass, models.RetentionModeAssist, "rejected").Count(&stat.Disagreed)
	db.Model(&models.RetentionAction{}).Where("tenant_id=? AND action_class=? AND outcome IN ? AND updated_at > ?", tenant, actionClass, []string{models.RetentionActionToolFailed, models.RetentionActionVerifyFailed}, failureSince).Count(&stat.Failures)
	if stat.AssistDecisions > 0 {
		stat.AgreementPct = float64(stat.Agreed) * 100 / float64(stat.AssistDecisions)
	}
	stat.BreakerOpen = stat.Failures >= 2
	if stat.BreakerOpen {
		stat.State = "demoted"
	} else if stat.AssistDecisions >= int64(policy.TrustMinDecisions) && stat.AgreementPct >= float64(policy.TrustMinAgreementPct) {
		stat.State = "trusted"
	}
	stat.PromotionReady = stat.AutoEligible && stat.State == "trusted" && !stat.BreakerOpen
	return stat
}

func retentionPromotionFor(db *gorm.DB, tenant string, policy models.RetentionPolicy) retentionPromotionStatus {
	result := retentionPromotionStatus{Mode: policy.Mode, ManualFlipOnly: true, SafeAutoAllowed: true, Trust: make([]retentionTrustStat, 0, len(retentionTrustClasses()))}
	for _, class := range retentionTrustClasses() {
		stat := retentionTrust(db, tenant, class, policy)
		result.Trust = append(result.Trust, stat)
		if !stat.PromotionReady {
			result.SafeAutoAllowed = false
			if stat.BreakerOpen {
				result.BlockedReason = "action-class breaker is open"
			} else if stat.State != "trusted" {
				result.BlockedReason = "Assist agreement mileage has not earned trust"
			}
		}
	}
	return result
}

func retentionSafeAutoPromotionReady(db *gorm.DB, tenant string, policy models.RetentionPolicy) bool {
	promotion := retentionPromotionFor(db, tenant, policy)
	return promotion.SafeAutoAllowed
}

func retentionSnapshotRefreshWindows(db *gorm.DB, tenant string) []string {
	windows := []string{models.NewsWindowToday, models.NewsWindowWeek, models.NewsWindowMonth}
	var snaps []models.NewsSnapshot
	db.Select("window", "built_at", "dirty").Where("tenant_id=? AND \"window\" IN ?", tenant, windows).Find(&snaps)
	byWindow := make(map[string]models.NewsSnapshot, len(snaps))
	for _, snap := range snaps {
		byWindow[snap.Window] = snap
	}
	needed := make([]string, 0, len(windows))
	for _, window := range windows {
		snap, ok := byWindow[window]
		if !ok || snap.Dirty || time.Since(snap.BuiltAt) > newsSnapshotTTL {
			needed = append(needed, window)
		}
	}
	return needed
}

func buildRetentionSnapshotRefreshAction(run models.RetentionRun, policy models.RetentionPolicy, preview retentionPreview, windows []string, trusted bool) models.RetentionAction {
	actionMode := retentionActionMode(policy, models.RetentionActionRefreshNewsSnapshots)
	decision := "shadow_preview"
	outcome := models.RetentionActionWouldExecute
	guardrail := "shadow_no_side_effect"
	reason := "Observe shadow agrees that stale News snapshots should be refreshed"
	if actionMode == models.RetentionModeAssist {
		decision, outcome, guardrail, reason = "approval_required", models.RetentionActionApprovalRequired, "noncanonical_snapshot_refresh_human_assist", "Bounded derived-state refresh requires an Assist decision"
	} else if actionMode == models.RetentionModeSafeAuto {
		if trusted {
			decision, outcome, guardrail, reason = "ready", models.RetentionActionReady, "trusted_noncanonical_snapshot_refresh", "Trusted bounded derived-state refresh is ready for Safe Auto"
		} else {
			// A failed promotion is fail-closed to Assist, so an operator can
			// still make the same bounded decision deliberately and add mileage.
			actionMode = models.RetentionModeAssist
			decision, outcome, guardrail, reason = "approval_required", models.RetentionActionApprovalRequired, "trust_gate", "Safe Auto remains held until this action class earns Assist agreement trust"
		}
	}
	raw := retentionActionEvidence(map[string]interface{}{"windows": windows, "preview": preview, "shadow_decision": "would_execute", "canonical_rows_deleted": false, "decision_reason": reason})
	fingerprint := retentionSHA256(models.RetentionActionRefreshNewsSnapshots, strings.Join(windows, ","), fmt.Sprintf("%d", preview.CandidateRows), fmt.Sprintf("%d", preview.EstimatedBytes))
	return models.RetentionAction{RunID: run.ID, TenantID: run.TenantID, ActionClass: models.RetentionActionRefreshNewsSnapshots, OwnerSystem: "news_database", TargetScope: "windows:" + strings.Join(windows, ","), Mode: actionMode, Decision: decision, Outcome: outcome, IdempotencyKey: run.CorrelationID.String() + ":snapshot-refresh", EvidenceFingerprint: fingerprint, TargetCount: len(windows), EstimatedBytes: 0, Guardrail: guardrail, Evidence: raw, BeforeBytes: nil, ForecastAfterBytes: nil, CreatedAt: time.Now().UTC()}
}

func executeRetentionSnapshotRefresh(db *gorm.DB, action models.RetentionAction, actor string) (models.RetentionAction, error) {
	if !retentionActionAutoEligible(action.ActionClass) {
		return action, errors.New("action class is not Safe Auto eligible")
	}
	now := time.Now().UTC()
	if action.Mode == models.RetentionModeSafeAuto {
		policy := loadRetentionPolicy(db, action.TenantID)
		trust := retentionTrust(db, action.TenantID, action.ActionClass, policy)
		if !trust.PromotionReady {
			return action, errors.New("action trust gate is not earned")
		}
	}
	result := db.Model(&models.RetentionAction{}).
		Where("id=? AND tenant_id=? AND outcome IN ?", action.ID, action.TenantID, []string{models.RetentionActionReady, models.RetentionActionApproved}).
		Updates(map[string]interface{}{"outcome": models.RetentionActionRunning, "started_at": now, "updated_at": now})
	if result.Error != nil || result.RowsAffected != 1 {
		return action, errors.New("retention action is no longer executable")
	}
	windows := retentionSnapshotRefreshWindows(db, action.TenantID)
	if len(windows) == 0 {
		_ = db.Model(&action).Updates(map[string]interface{}{"outcome": models.RetentionActionSkipped, "decision": "blocked", "guardrail": "no_stale_snapshots", "finished_at": now, "updated_at": now}).Error
		return action, nil
	}
	counts := map[string]int{}
	for _, window := range windows {
		count, err := buildNewsSnapshot(db, action.TenantID, window)
		if err != nil {
			finished := time.Now().UTC()
			failure := retentionActionEvidence(map[string]interface{}{"window": window, "error": err.Error()})
			_ = db.Model(&action).Updates(map[string]interface{}{"outcome": models.RetentionActionToolFailed, "decision": "action_failed", "verification": failure, "finished_at": finished, "updated_at": finished}).Error
			return action, err
		}
		counts[window] = count
	}
	failed := retentionSnapshotRefreshWindows(db, action.TenantID)
	finished := time.Now().UTC()
	if len(failed) > 0 {
		verification := retentionActionEvidence(map[string]interface{}{"windows": windows, "remaining_stale": failed, "actor": actor})
		_ = db.Model(&action).Updates(map[string]interface{}{"outcome": models.RetentionActionVerifyFailed, "decision": "action_failed", "verification": verification, "finished_at": finished, "updated_at": finished}).Error
		return action, fmt.Errorf("snapshot refresh verification failed for %s", strings.Join(failed, ","))
	}
	verification := retentionActionEvidence(map[string]interface{}{"windows": windows, "slide_counts": counts, "actor": actor, "canonical_rows_deleted": false})
	_ = db.Model(&action).Updates(map[string]interface{}{"outcome": models.RetentionActionVerified, "decision": "executed", "verification": verification, "finished_at": finished, "updated_at": finished}).Error
	return action, nil
}

func ResetRetentionAutopilotBreaker(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	class := strings.TrimSpace(c.Param("class"))
	if !retentionActionAutoEligible(class) {
		c.JSON(400, gin.H{"error": "unknown or human-only retention action class"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	policy := loadRetentionPolicy(db, principal.TenantID)
	retentionAudit(db, principal, "retention.autopilot.breaker.reset", class, "success", map[string]interface{}{"action_class": class})
	c.JSON(200, gin.H{"data": retentionTrust(db, principal.TenantID, class, policy)})
}
