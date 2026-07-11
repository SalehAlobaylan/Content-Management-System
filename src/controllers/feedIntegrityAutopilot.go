package controllers

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"content-management-system/src/models"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	feedIntegrityRegistryVersion     = "fi-registry-v1"
	feedIntegrityVerificationV1      = "fi-verify-v1"
	feedIntegritySnapshotToolVersion = "news-snapshot-v1"
	feedIntegrityClaimLease          = 2 * time.Minute
)

var feedIntegrityRetention = struct{ Last time.Time }{}

type feedIntegrityActionSpec struct {
	Owner        string
	ActionClass  string
	Scope        string
	DeepLink     string
	AutoEligible bool
}

func feedIntegrityActionSpecFor(f models.FeedIntegrityFinding) feedIntegrityActionSpec {
	spec := feedIntegrityActionSpec{Owner: "cms-feed", Scope: feedIntegrityScope(f), DeepLink: "/platform/feed-integrity"}
	switch f.CheckKey {
	case "edge_news_cache_stale":
		spec.Owner, spec.ActionClass, spec.DeepLink, spec.AutoEligible = "news-circulation", models.FeedIntegrityActionRefreshWindow, "/platform/news/circulation", true
	case "inv_news_cache_rebuild_debt":
		spec.Owner, spec.ActionClass, spec.DeepLink = "news-circulation", "news_snapshot.inspect", "/platform/news/circulation"
	case "inv_news_unlabeled_stale":
		spec.Owner, spec.ActionClass, spec.DeepLink = "enrichment", "enrichment.review", "/platform/enrichment"
	case "inv_fy_bounds", "inv_fy_parent_leak", "inv_fy_bucket_mismatch":
		spec.Owner, spec.ActionClass, spec.DeepLink = "media-studio", "media_studio.inspect", "/platform/media/atomization?tab=studio"
	case "inv_fy_playback_missing", "inv_fy_renditions_malformed", "edge_fy_playback_fields", "probe_url_dead", "probe_hls_manifest":
		spec.Owner, spec.ActionClass, spec.DeepLink = "storage", "storage.inspect", "/platform/storage"
	case "edge_fy_empty":
		spec.Owner, spec.ActionClass, spec.DeepLink = "media-circulation", "media_circulation.inspect", "/platform/media/circulation"
	case "edge_news_empty", "edge_news_shape", "edge_news_dup":
		spec.Owner, spec.ActionClass, spec.DeepLink = "news-circulation", "news_circulation.inspect", "/platform/news/circulation"
	case "edge_fy_http", "edge_news_http":
		spec.Owner, spec.ActionClass, spec.DeepLink = "system-health", "system_health.inspect", "/platform/system-health"
	default:
		spec.ActionClass = models.FeedIntegrityActionConfirm
	}
	return spec
}

// feedIntegrityActionExecutable reports whether an action class has a registered
// executable adapter. V1 exposes exactly one repair tool (the one-window News
// snapshot refresh); every other class is recommendation/attention only until
// its owner registers a tool (Slice 5). This is the single source of truth the
// decision loop uses to avoid offering a human approval that can only dead-end.
func feedIntegrityActionExecutable(actionClass string) bool {
	return actionClass == models.FeedIntegrityActionRefreshWindow
}

func feedIntegrityScope(f models.FeedIntegrityFinding) string {
	if f.CheckKey == "inv_news_cache_rebuild_debt" || f.CheckKey == "edge_news_cache_stale" {
		return strings.TrimPrefix(f.Variant, "window:")
	}
	switch f.TargetType {
	case "content_item", "story", "snapshot":
		if strings.TrimSpace(f.TargetRef) != "" {
			return f.TargetType + ":" + strings.TrimSpace(f.TargetRef)
		}
	}
	return "feed:" + f.Feed + ":" + f.Variant
}

func feedIntegrityFingerprint(f models.FeedIntegrityFinding) string {
	scope := feedIntegrityScope(f)
	sum := sha256.Sum256([]byte(f.CheckKey + "|" + f.Feed + "|" + f.Variant + "|" + scope))
	return hex.EncodeToString(sum[:])
}

func feedIntegrityAutopilotSchemaReady(db *gorm.DB) bool {
	return db.Migrator().HasTable(&models.FeedIntegrityAction{}) && db.Migrator().HasColumn(&models.FeedIntegrityRun{}, "AutopilotEvaluatedAt")
}

func sanitizeFeedIntegrityAutopilotPolicy(p *models.FeedIntegrityPolicy) {
	if p.AutopilotMode != models.FeedIntegrityAutopilotModeAssist && p.AutopilotMode != models.FeedIntegrityAutopilotModeSafeAuto {
		p.AutopilotMode = models.FeedIntegrityAutopilotModeObserve
	}
	if p.AutopilotActionHourlyCap < 1 || p.AutopilotActionHourlyCap > 20 {
		p.AutopilotActionHourlyCap = 2
	}
	if p.AutopilotDiagnosticHourlyCap < 1 || p.AutopilotDiagnosticHourlyCap > 40 {
		p.AutopilotDiagnosticHourlyCap = 4
	}
	if p.AutopilotCooldownMinutes < 15 || p.AutopilotCooldownMinutes > 1440 {
		p.AutopilotCooldownMinutes = 60
	}
	if p.AutopilotEvidenceMaxAgeMinutes < 2 || p.AutopilotEvidenceMaxAgeMinutes > 60 {
		p.AutopilotEvidenceMaxAgeMinutes = 10
	}
	if p.AutopilotRetryLimit < 0 || p.AutopilotRetryLimit > 3 {
		p.AutopilotRetryLimit = 1
	}
	if p.AutopilotTrustMinDecisions < 1 || p.AutopilotTrustMinDecisions > 1000 {
		p.AutopilotTrustMinDecisions = 20
	}
	if p.AutopilotTrustMinAgreementPct < 50 || p.AutopilotTrustMinAgreementPct > 100 {
		p.AutopilotTrustMinAgreementPct = 95
	}
}

type feedIntegrityTrustStat struct {
	ActionClass  string  `json:"action_class"`
	State        string  `json:"state"`
	Decisions    int64   `json:"decisions"`
	Agreed       int64   `json:"agreed"`
	AgreementPct float64 `json:"agreement_pct"`
	Failures     int64   `json:"failures"`
	BreakerOpen  bool    `json:"breaker_open"`
}

func feedIntegrityTrust(db *gorm.DB, tenant, actionClass string, policy models.FeedIntegrityPolicy) feedIntegrityTrustStat {
	stat := feedIntegrityTrustStat{ActionClass: actionClass, State: "probation"}
	failureSince := time.Now().UTC().Add(-24 * time.Hour)
	var reset models.AuditLog
	if db.Where("tenant_id=? AND action=? AND target_resource=? AND status='success'", tenant, "feed_integrity.autopilot.breaker.reset", actionClass).Order("created_at DESC").First(&reset).Error == nil && reset.CreatedAt.After(failureSince) {
		failureSince = reset.CreatedAt
	}
	db.Model(&models.FeedIntegrityAction{}).Where("tenant_id=? AND action_class=? AND outcome IN ?", tenant, actionClass, []string{models.FeedIntegrityActionApproved, models.FeedIntegrityActionRejected, models.FeedIntegrityActionVerificationPassed, models.FeedIntegrityActionVerificationFailed}).Distinct("evidence_fingerprint").Count(&stat.Decisions)
	db.Model(&models.FeedIntegrityAction{}).Where("tenant_id=? AND action_class=? AND outcome IN ?", tenant, actionClass, []string{models.FeedIntegrityActionApproved, models.FeedIntegrityActionVerificationPassed}).Distinct("evidence_fingerprint").Count(&stat.Agreed)
	db.Model(&models.FeedIntegrityAction{}).Where("tenant_id=? AND action_class=? AND outcome IN ? AND created_at > ?", tenant, actionClass, []string{models.FeedIntegrityActionToolFailed, models.FeedIntegrityActionVerificationFailed}, failureSince).Count(&stat.Failures)
	if stat.Decisions > 0 {
		stat.AgreementPct = float64(stat.Agreed) * 100 / float64(stat.Decisions)
	}
	stat.BreakerOpen = stat.Failures >= 2
	if stat.BreakerOpen {
		stat.State = "demoted"
	} else if stat.Decisions >= int64(policy.AutopilotTrustMinDecisions) && stat.AgreementPct >= float64(policy.AutopilotTrustMinAgreementPct) {
		stat.State = "trusted"
	}
	return stat
}

func feedIntegrityActionMode(policy models.FeedIntegrityPolicy, actionClass string) string {
	mode := policy.AutopilotMode
	var overrides map[string]string
	if len(policy.AutopilotActionModes) > 0 && json.Unmarshal(policy.AutopilotActionModes, &overrides) == nil {
		if v := overrides[actionClass]; v == models.FeedIntegrityAutopilotModeObserve || v == models.FeedIntegrityAutopilotModeAssist || v == models.FeedIntegrityAutopilotModeSafeAuto {
			if feedIntegrityModeRank(v) < feedIntegrityModeRank(mode) {
				mode = v
			}
		}
	}
	return mode
}

func feedIntegrityModeRank(mode string) int {
	switch mode {
	case models.FeedIntegrityAutopilotModeSafeAuto:
		return 3
	case models.FeedIntegrityAutopilotModeAssist:
		return 2
	default:
		return 1
	}
}

func feedIntegritySystemIncidentOpen(db *gorm.DB) bool {
	if !db.Migrator().HasTable(&models.SystemIncidentEpisode{}) {
		return false
	}
	var n int64
	db.Model(&models.SystemIncidentEpisode{}).Where("status IN ?", []string{models.SystemIncidentStatusOpen, models.SystemIncidentStatusRecovering}).Count(&n)
	return n > 0
}

func evaluateFeedIntegrityAutopilot(db *gorm.DB, runID uint) error {
	if !feedIntegrityAutopilotSchemaReady(db) {
		return nil
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var run models.FeedIntegrityRun
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&run, runID).Error; err != nil {
			return err
		}
		if run.AutopilotEvaluatedAt != nil || (run.Status != models.FeedIntegrityRunCompleted && run.Status != models.FeedIntegrityRunPartial) {
			return nil
		}
		policy := loadFeedIntegrityPolicy(tx, run.TenantID)
		sanitizeFeedIntegrityAutopilotPolicy(&policy)
		now := time.Now().UTC()
		decision := models.FeedIntegrityDecisionNoAction
		counts := map[string]int{"considered": 0, "proposed": 0, "blocked": 0}

		var findings []models.FeedIntegrityFinding
		tx.Where("run_id=? AND status='violation'", run.ID).Order("created_at ASC").Find(&findings)
		sort.SliceStable(findings, func(i, j int) bool {
			left, right := feedIntegrityPriority(findings[i]), feedIntegrityPriority(findings[j])
			if left != right {
				return left > right
			}
			return findings[i].PublicID.String() < findings[j].PublicID.String()
		})
		seenEpisodes := map[uint]bool{}
		readyCreated := false
		for _, finding := range findings {
			scope := feedIntegrityScope(finding)
			var ep models.FeedIntegrityEpisode
			if tx.Where("tenant_id=? AND check_key=? AND feed=? AND variant=? AND scope=? AND status IN ?", run.TenantID, finding.CheckKey, finding.Feed, finding.Variant, scope, []string{models.FeedIntegrityEpisodeOpen, models.FeedIntegrityEpisodeRecovering}).First(&ep).Error != nil || seenEpisodes[ep.ID] {
				continue
			}
			seenEpisodes[ep.ID] = true
			counts["considered"]++
			spec := feedIntegrityActionSpecFor(finding)
			fingerprint := feedIntegrityFingerprint(finding)
			idempotency := fmt.Sprintf("fi:%d:%d:%s:%s", run.ID, ep.ID, spec.ActionClass, fingerprint[:16])
			action := models.FeedIntegrityAction{TenantID: run.TenantID, RunID: run.ID, EpisodeID: ep.ID, ActionClass: spec.ActionClass, OwnerSystem: spec.Owner, TargetScope: scope, Mode: models.FeedIntegrityAutopilotModeObserve, Decision: models.FeedIntegrityDecisionAttention, Outcome: models.FeedIntegrityActionWouldExecute, Reason: "Observe mode: recommendation recorded without execution", IdempotencyKey: idempotency, EvidenceFingerprint: fingerprint, RegistryVersion: feedIntegrityRegistryVersion, VerificationContractVersion: feedIntegrityVerificationV1, Actor: "automation"}
			input, _ := json.Marshal(gin.H{"check_key": finding.CheckKey, "feed": finding.Feed, "variant": finding.Variant, "scope": scope, "deep_link": spec.DeepLink, "evidence": json.RawMessage(finding.Evidence)})
			action.Input = datatypes.JSON(input)

			blocked := ""
			mode := feedIntegrityActionMode(policy, spec.ActionClass)
			action.Mode = mode
			if !policy.AutopilotEnabled {
				blocked = "autopilot_disabled"
			} else if policy.AutopilotPausedUntil != nil && policy.AutopilotPausedUntil.After(now) {
				blocked = "autopilot_paused"
			} else if !policy.ScheduledEnabled {
				blocked = "detection_disabled"
			} else if run.FinishedAt == nil || now.Sub(*run.FinishedAt) > time.Duration(policy.AutopilotEvidenceMaxAgeMinutes)*time.Minute {
				blocked = "stale_evidence"
			} else if feedIntegritySystemIncidentOpen(tx) {
				blocked = "system_incident_open"
			}
			if blocked != "" {
				action.Outcome, action.Decision, action.Guardrail, action.Reason = models.FeedIntegrityActionSkipped, models.FeedIntegrityDecisionBlocked, blocked, "Action blocked by "+blocked
				counts["blocked"]++
				decision = promoteFeedIntegrityDecision(decision, models.FeedIntegrityDecisionBlocked)
			} else if mode == models.FeedIntegrityAutopilotModeObserve {
				decision = promoteFeedIntegrityDecision(decision, models.FeedIntegrityDecisionAttention)
			} else if !feedIntegrityActionExecutable(spec.ActionClass) {
				// V1 registers an executable adapter only for the one-window News
				// refresh. Owner "inspect" classes and confirm-only cases have no
				// autopilot tool, so approving them would dead-end in
				// `owner_tool_unregistered`. Surface them as attention with a deep
				// link instead; owner intervention protocol is Slice 5.
				action.Reason, action.Decision = "Owner-owned issue: inspect and repair via the linked cockpit", models.FeedIntegrityDecisionAttention
				decision = promoteFeedIntegrityDecision(decision, models.FeedIntegrityDecisionAttention)
			} else if mode == models.FeedIntegrityAutopilotModeAssist {
				action.Outcome, action.Decision, action.Reason = models.FeedIntegrityActionApprovalRequired, models.FeedIntegrityDecisionApprovalRequired, "Registered repair requires human approval"
				decision = promoteFeedIntegrityDecision(decision, models.FeedIntegrityDecisionApprovalRequired)
				counts["proposed"]++
			} else {
				trust := feedIntegrityTrust(tx, run.TenantID, spec.ActionClass, policy)
				if spec.ActionClass == models.FeedIntegrityActionRefreshWindow && !feedIntegritySnapshotDualEvidence(tx, run.ID, finding.Variant) {
					action.Outcome, action.Decision, action.Guardrail, action.Reason = models.FeedIntegrityActionApprovalRequired, models.FeedIntegrityDecisionApprovalRequired, "dual_evidence_missing", "Safe Auto requires stale inventory and stale-served-cache evidence in the same run"
					decision = promoteFeedIntegrityDecision(decision, models.FeedIntegrityDecisionApprovalRequired)
				} else if trust.State != "trusted" || trust.BreakerOpen {
					action.Outcome, action.Decision, action.Guardrail, action.Reason = models.FeedIntegrityActionApprovalRequired, models.FeedIntegrityDecisionApprovalRequired, "trust_gate", "Safe Auto held in Assist until this action class earns trust"
					decision = promoteFeedIntegrityDecision(decision, models.FeedIntegrityDecisionApprovalRequired)
				} else if readyCreated {
					action.Outcome, action.Decision, action.Guardrail, action.Reason = models.FeedIntegrityActionSkipped, models.FeedIntegrityDecisionBlocked, "run_action_cap", "A higher-priority repair already owns this run's action slot"
					decision = promoteFeedIntegrityDecision(decision, models.FeedIntegrityDecisionBlocked)
				} else {
					action.Outcome, action.Decision, action.Reason = models.FeedIntegrityActionReady, models.FeedIntegrityDecisionReady, "Trusted registered action is ready for execution"
					decision = promoteFeedIntegrityDecision(decision, models.FeedIntegrityDecisionReady)
					readyCreated = true
				}
				counts["proposed"]++
			}
			created := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&action)
			if created.Error != nil {
				return created.Error
			}
			if created.RowsAffected == 0 {
				continue
			}
			attr, _ := json.Marshal(gin.H{"owner": spec.Owner, "action_class": spec.ActionClass, "deep_link": spec.DeepLink, "registry_version": feedIntegrityRegistryVersion})
			if err := tx.Model(&models.FeedIntegrityEpisode{}).Where("id=? AND tenant_id=?", ep.ID, run.TenantID).Updates(map[string]interface{}{"attribution": datatypes.JSON(attr), "attribution_version": feedIntegrityRegistryVersion, "recommended_action": spec.ActionClass, "latest_action_id": action.ID, "updated_at": now}).Error; err != nil {
				return err
			}
		}
		countsJSON, _ := json.Marshal(counts)
		return tx.Model(&models.FeedIntegrityRun{}).Where("id=? AND autopilot_evaluated_at IS NULL", run.ID).Updates(map[string]interface{}{"autopilot_evaluated_at": now, "autopilot_decision": decision, "autopilot_counts": datatypes.JSON(countsJSON), "autopilot_error_class": "none", "updated_at": now}).Error
	})
}

func feedIntegrityPriority(f models.FeedIntegrityFinding) int {
	priority := 0
	switch f.Severity {
	case "critical":
		priority = 400
	case "major":
		priority = 300
	case "minor":
		priority = 200
	default:
		priority = 100
	}
	if f.Axis == models.FeedIntegrityAxisConsumer {
		priority += 50
	}
	return priority
}

func promoteFeedIntegrityDecision(current, candidate string) string {
	rank := map[string]int{models.FeedIntegrityDecisionNoAction: 0, models.FeedIntegrityDecisionBlocked: 1, models.FeedIntegrityDecisionAttention: 2, models.FeedIntegrityDecisionConfirming: 3, models.FeedIntegrityDecisionApprovalRequired: 4, models.FeedIntegrityDecisionReady: 5, models.FeedIntegrityDecisionExecuted: 6, models.FeedIntegrityDecisionRecovering: 7, models.FeedIntegrityDecisionActionFailed: 8}
	if rank[candidate] > rank[current] {
		return candidate
	}
	return current
}

func feedIntegritySnapshotDualEvidence(db *gorm.DB, runID uint, variant string) bool {
	window := strings.TrimPrefix(variant, "window:")
	var inventory, edge int64
	db.Model(&models.FeedIntegrityFinding{}).Where("run_id=? AND check_key=? AND status='violation' AND (variant=? OR target_ref=?)", runID, "inv_news_cache_rebuild_debt", variant, window).Count(&inventory)
	db.Model(&models.FeedIntegrityFinding{}).Where("run_id=? AND check_key=? AND status='violation' AND variant=?", runID, "edge_news_cache_stale", "window:"+window).Count(&edge)
	return inventory > 0 && edge > 0
}

func evaluatePendingFeedIntegrityRuns(db *gorm.DB) {
	if !feedIntegrityAutopilotSchemaReady(db) {
		return
	}
	var runs []models.FeedIntegrityRun
	db.Where("status IN ? AND autopilot_evaluated_at IS NULL", []string{models.FeedIntegrityRunCompleted, models.FeedIntegrityRunPartial}).Order("started_at ASC").Limit(20).Find(&runs)
	for _, run := range runs {
		if err := evaluateFeedIntegrityAutopilot(db, run.ID); err != nil {
			_ = db.Model(&models.FeedIntegrityRun{}).Where("id=?", run.ID).Updates(map[string]interface{}{"autopilot_error_class": "evaluation_failed", "updated_at": time.Now().UTC()}).Error
		}
	}
}

func claimFeedIntegrityAction(db *gorm.DB, tenant string, actionID uint) (models.FeedIntegrityAction, error) {
	now := time.Now().UTC()
	var pending models.FeedIntegrityAction
	if err := db.Where("id=? AND tenant_id=?", actionID, tenant).First(&pending).Error; err != nil {
		return pending, err
	}
	policy := loadFeedIntegrityPolicy(db, tenant)
	sanitizeFeedIntegrityAutopilotPolicy(&policy)
	if !policy.AutopilotEnabled || !policy.ScheduledEnabled || policy.AutopilotPausedUntil != nil && policy.AutopilotPausedUntil.After(now) {
		return pending, fmt.Errorf("autopilot is disabled or paused")
	}
	if feedIntegritySystemIncidentOpen(db) {
		return pending, fmt.Errorf("system incident is open")
	}
	var run models.FeedIntegrityRun
	if db.Where("id=? AND tenant_id=?", pending.RunID, tenant).First(&run).Error != nil || run.FinishedAt == nil || now.Sub(*run.FinishedAt) > time.Duration(policy.AutopilotEvidenceMaxAgeMinutes)*time.Minute {
		finished := now
		_ = db.Model(&pending).Where("outcome IN ?", []string{models.FeedIntegrityActionReady, models.FeedIntegrityActionApproved, models.FeedIntegrityActionApprovalRequired}).Updates(map[string]interface{}{"outcome": models.FeedIntegrityActionExpired, "guardrail": "stale_evidence", "reason": "Action evidence expired before execution", "finished_at": finished, "updated_at": finished}).Error
		return pending, fmt.Errorf("action evidence is stale")
	}
	var runActions int64
	db.Model(&models.FeedIntegrityAction{}).Where("tenant_id=? AND run_id=? AND executed_at IS NOT NULL", tenant, pending.RunID).Count(&runActions)
	if runActions > 0 {
		return pending, fmt.Errorf("run action cap reached")
	}
	var hourly int64
	db.Model(&models.FeedIntegrityAction{}).Where("tenant_id=? AND executed_at > ?", tenant, now.Add(-time.Hour)).Count(&hourly)
	if hourly >= int64(policy.AutopilotActionHourlyCap) {
		return pending, fmt.Errorf("hourly action cap reached")
	}
	var cooldown int64
	db.Model(&models.FeedIntegrityAction{}).Where("tenant_id=? AND episode_id=? AND action_class=? AND executed_at > ?", tenant, pending.EpisodeID, pending.ActionClass, now.Add(-time.Duration(policy.AutopilotCooldownMinutes)*time.Minute)).Count(&cooldown)
	if cooldown > 0 {
		return pending, fmt.Errorf("action cooldown is active")
	}
	if pending.Mode == models.FeedIntegrityAutopilotModeSafeAuto {
		trust := feedIntegrityTrust(db, tenant, pending.ActionClass, policy)
		if trust.State != "trusted" || trust.BreakerOpen {
			return pending, fmt.Errorf("action trust gate is not earned")
		}
	}
	token := uuid.NewString()
	expires := now.Add(feedIntegrityClaimLease)
	result := db.Model(&models.FeedIntegrityAction{}).Where("id=? AND tenant_id=? AND outcome IN ?", actionID, tenant, []string{models.FeedIntegrityActionReady, models.FeedIntegrityActionApproved}).Updates(map[string]interface{}{"outcome": models.FeedIntegrityActionClaimed, "claim_token": token, "claimed_at": now, "claim_expires_at": expires, "updated_at": now})
	if result.Error != nil || result.RowsAffected != 1 {
		return models.FeedIntegrityAction{}, fmt.Errorf("action is no longer executable")
	}
	var action models.FeedIntegrityAction
	if err := db.Where("id=? AND claim_token=?", actionID, token).First(&action).Error; err != nil {
		return action, err
	}
	return action, nil
}

func executeFeedIntegrityAction(db *gorm.DB, action models.FeedIntegrityAction) error {
	started := time.Now().UTC()
	_ = db.Model(&action).Updates(map[string]interface{}{"outcome": models.FeedIntegrityActionRunning, "updated_at": started}).Error
	if !feedIntegrityActionExecutable(action.ActionClass) {
		finished := time.Now().UTC()
		return db.Model(&action).Updates(map[string]interface{}{"outcome": models.FeedIntegrityActionSkipped, "guardrail": "owner_tool_unregistered", "reason": "No executable owner adapter is registered", "finished_at": finished, "updated_at": finished}).Error
	}
	window := normalizeNewsWindow(action.TargetScope)
	var snap models.NewsSnapshot
	if err := db.Where("tenant_id=? AND \"window\"=?", action.TenantID, window).First(&snap).Error; err != nil {
		return finishFeedIntegrityActionFailure(db, action, "precondition_changed", err)
	}
	staleFloor := newsSnapshotTTL
	if action.Mode == models.FeedIntegrityAutopilotModeSafeAuto {
		staleFloor = newsSnapshotMaxStale
	}
	if time.Since(snap.BuiltAt) <= staleFloor && !snap.Dirty {
		finished := time.Now().UTC()
		return db.Model(&action).Updates(map[string]interface{}{"outcome": models.FeedIntegrityActionSkipped, "guardrail": "precondition_changed", "reason": "Snapshot recovered before execution", "finished_at": finished, "updated_at": finished}).Error
	}
	count, err := buildNewsSnapshot(db, action.TenantID, window)
	if err != nil {
		return finishFeedIntegrityActionFailure(db, action, "tool_failed", err)
	}
	executed := time.Now().UTC()
	output, _ := json.Marshal(gin.H{"window": window, "slide_count": count, "built_at": executed, "tool_version": feedIntegritySnapshotToolVersion})
	due := executed.Add(5 * time.Second)
	if err := db.Model(&action).Updates(map[string]interface{}{"outcome": models.FeedIntegrityActionVerifying, "output": datatypes.JSON(output), "tool_version": feedIntegritySnapshotToolVersion, "executed_at": executed, "verification_due_at": due, "duration_ms": executed.Sub(started).Milliseconds(), "updated_at": executed}).Error; err != nil {
		return err
	}
	return nil
}

func finishFeedIntegrityActionFailure(db *gorm.DB, action models.FeedIntegrityAction, class string, err error) error {
	now := time.Now().UTC()
	_ = db.Model(&action).Updates(map[string]interface{}{"outcome": models.FeedIntegrityActionToolFailed, "error_class": class, "reason": safeIntegrityError(err), "finished_at": now, "duration_ms": now.Sub(action.UpdatedAt).Milliseconds(), "updated_at": now}).Error
	return err
}

func verifyFeedIntegrityAction(db *gorm.DB, action models.FeedIntegrityAction) {
	now := time.Now().UTC()
	window := normalizeNewsWindow(action.TargetScope)
	var snap models.NewsSnapshot
	err := db.Where("tenant_id=? AND \"window\"=?", action.TenantID, window).First(&snap).Error
	edgePassed, edgeEvidence := verifyFeedIntegrityNewsEdge(window)
	passed := err == nil && !snap.Dirty && time.Since(snap.BuiltAt) <= newsSnapshotTTL && snap.SlideCount > 0 && edgePassed
	outcome := models.FeedIntegrityActionVerificationFailed
	reason := "Snapshot verification did not meet freshness and fill contract"
	decision := models.FeedIntegrityDecisionActionFailed
	if passed {
		outcome, reason, decision = models.FeedIntegrityActionVerificationPassed, "Fresh snapshot is populated and clean", models.FeedIntegrityDecisionRecovering
	}
	verification, _ := json.Marshal(gin.H{"window": window, "passed": passed, "dirty": snap.Dirty, "slide_count": snap.SlideCount, "built_at": snap.BuiltAt, "error": safeIntegrityError(err), "edge": edgeEvidence})
	_ = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&models.FeedIntegrityAction{}).Where("id=? AND tenant_id=? AND outcome=?", action.ID, action.TenantID, models.FeedIntegrityActionVerifying).Updates(map[string]interface{}{"outcome": outcome, "decision": decision, "verification": datatypes.JSON(verification), "reason": reason, "finished_at": now, "updated_at": now}).Error; err != nil {
			return err
		}
		if passed {
			return tx.Model(&models.FeedIntegrityEpisode{}).Where("id=? AND tenant_id=? AND status=?", action.EpisodeID, action.TenantID, models.FeedIntegrityEpisodeOpen).Updates(map[string]interface{}{"status": models.FeedIntegrityEpisodeRecovering, "recovering_since": now, "clean_streak": 1, "updated_at": now}).Error
		}
		return nil
	})
}

func verifyFeedIntegrityNewsEdge(window string) (bool, gin.H) {
	client := &http.Client{Timeout: 5 * time.Second}
	req, _ := http.NewRequest(http.MethodGet, feedIntegritySelfURL()+"/api/v1/feed/news?limit=10&window="+url.QueryEscape(window), nil)
	req.Header.Set(feedIntegritySyntheticHdr, feedIntegrityCapability)
	resp, err := client.Do(req)
	if err != nil {
		return false, gin.H{"error": safeIntegrityError(err)}
	}
	defer resp.Body.Close()
	var payload struct {
		Slides []struct {
			Featured struct {
				StoryID string            `json:"story_id"`
				Members []json.RawMessage `json:"members"`
			} `json:"featured"`
			Related []json.RawMessage `json:"related"`
		} `json:"slides"`
	}
	decodeErr := json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(&payload)
	valid := resp.StatusCode == http.StatusOK && decodeErr == nil && len(payload.Slides) > 0
	for _, slide := range payload.Slides {
		if slide.Featured.StoryID == "" || len(slide.Featured.Members) == 0 || len(slide.Related) > 3 {
			valid = false
			break
		}
	}
	age, _ := strconv.ParseInt(resp.Header.Get("X-Wahb-Snapshot-Age-Ms"), 10, 64)
	if resp.Header.Get("X-Wahb-Feed-Source") == "cache" && age > newsSnapshotTTL.Milliseconds() {
		valid = false
	}
	return valid, gin.H{"status": resp.StatusCode, "slides": len(payload.Slides), "source": resp.Header.Get("X-Wahb-Feed-Source"), "snapshot_age_ms": age, "decode_error": safeIntegrityError(decodeErr)}
}

func processFeedIntegrityActions(db *gorm.DB) {
	if !feedIntegrityAutopilotSchemaReady(db) {
		return
	}
	now := time.Now().UTC()
	recoverExpiredFeedIntegrityClaims(db, now)
	var ready []models.FeedIntegrityAction
	db.Where("outcome IN ?", []string{models.FeedIntegrityActionReady, models.FeedIntegrityActionApproved}).Order("created_at ASC").Limit(10).Find(&ready)
	for _, row := range ready {
		action, err := claimFeedIntegrityAction(db, row.TenantID, row.ID)
		if err == nil {
			_ = executeFeedIntegrityAction(db, action)
		}
	}
	var verifying []models.FeedIntegrityAction
	db.Where("outcome=? AND verification_due_at <= ?", models.FeedIntegrityActionVerifying, now).Order("verification_due_at ASC").Limit(20).Find(&verifying)
	for _, action := range verifying {
		verifyFeedIntegrityAction(db, action)
	}
}

// recoverExpiredFeedIntegrityClaims makes a crashed worker's lease available to
// one later claimant. Only the claimed state can be recovered, so terminal or
// verification states cannot be resurrected by the scheduler.
func recoverExpiredFeedIntegrityClaims(db *gorm.DB, now time.Time) {
	var stale []models.FeedIntegrityAction
	db.Where("outcome=? AND claim_expires_at < ?", models.FeedIntegrityActionClaimed, now).Limit(20).Find(&stale)
	for _, action := range stale {
		_ = db.Model(&action).Where("id=? AND outcome=? AND claim_expires_at < ?", action.ID, models.FeedIntegrityActionClaimed, now).Updates(map[string]interface{}{"outcome": models.FeedIntegrityActionReady, "claim_token": "", "claimed_at": nil, "claim_expires_at": nil, "guardrail": "stale_claim_recovered", "updated_at": now}).Error
	}
}

func sweepFeedIntegrityRetention(db *gorm.DB) {
	now := time.Now().UTC()
	if !feedIntegrityRetention.Last.IsZero() && now.Sub(feedIntegrityRetention.Last) < 24*time.Hour {
		return
	}
	feedIntegrityRetention.Last = now
	_ = db.Where("created_at < ? AND status NOT IN ?", now.Add(-30*24*time.Hour), []string{"violation", "check_error"}).Delete(&models.FeedIntegrityFinding{}).Error
	_ = db.Where("created_at < ? AND status IN ?", now.Add(-90*24*time.Hour), []string{"violation", "check_error"}).Delete(&models.FeedIntegrityFinding{}).Error
	terminal := []string{models.FeedIntegrityActionWouldExecute, models.FeedIntegrityActionRejected, models.FeedIntegrityActionToolFailed, models.FeedIntegrityActionVerificationPassed, models.FeedIntegrityActionVerificationFailed, models.FeedIntegrityActionSkipped, models.FeedIntegrityActionExpired}
	_ = db.Where("created_at < ? AND outcome IN ?", now.Add(-365*24*time.Hour), terminal).Delete(&models.FeedIntegrityAction{}).Error
	_ = db.Where("created_at < ? AND NOT EXISTS (SELECT 1 FROM feed_integrity_actions a WHERE a.run_id=feed_integrity_runs.id)", now.Add(-365*24*time.Hour)).Delete(&models.FeedIntegrityRun{}).Error
}

func feedIntegrityAutopilotStatus(db *gorm.DB, tenant string) gin.H {
	policy := loadFeedIntegrityPolicy(db, tenant)
	sanitizeFeedIntegrityAutopilotPolicy(&policy)
	var latest models.FeedIntegrityRun
	_ = db.Where("tenant_id=?", tenant).Order("started_at DESC").First(&latest).Error
	var pending, stuck int64
	db.Model(&models.FeedIntegrityRun{}).Where("tenant_id=? AND status IN ? AND autopilot_evaluated_at IS NULL", tenant, []string{models.FeedIntegrityRunCompleted, models.FeedIntegrityRunPartial}).Count(&pending)
	db.Model(&models.FeedIntegrityAction{}).Where("tenant_id=? AND outcome IN ? AND updated_at < ?", tenant, []string{models.FeedIntegrityActionClaimed, models.FeedIntegrityActionRunning, models.FeedIntegrityActionVerifying}, time.Now().UTC().Add(-5*time.Minute)).Count(&stuck)
	var actions []models.FeedIntegrityAction
	db.Where("tenant_id=?", tenant).Order("created_at DESC").Limit(20).Find(&actions)
	classes := []string{models.FeedIntegrityActionRefreshWindow, models.FeedIntegrityActionConfirm}
	trust := make([]feedIntegrityTrustStat, 0, len(classes))
	for _, class := range classes {
		trust = append(trust, feedIntegrityTrust(db, tenant, class, policy))
	}
	self := "healthy"
	if pending > 0 || stuck > 0 || latest.AutopilotErrorClass != "" && latest.AutopilotErrorClass != "none" {
		self = "degraded"
	}
	actionState := "disabled"
	if policy.AutopilotEnabled {
		actionState = policy.AutopilotMode
		if policy.AutopilotPausedUntil != nil && policy.AutopilotPausedUntil.After(time.Now().UTC()) {
			actionState = "paused"
		}
	}
	return gin.H{"policy": policy, "latest_run": latest, "decision": latest.AutopilotDecision, "self_health": self, "action_state": actionState, "pending_evaluations": pending, "stuck_actions": stuck, "recent_actions": actions, "trust": trust, "registry_version": feedIntegrityRegistryVersion}
}

func GetFeedIntegrityAutopilotStatus(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	if !feedIntegrityAutopilotSchemaReady(db) {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "feed integrity autopilot migration is not applied"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": feedIntegrityAutopilotStatus(db, principal.TenantID)})
}

func UpdateFeedIntegrityAutopilotPolicy(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	var req struct {
		Enabled               *bool             `json:"autopilot_enabled"`
		Mode                  *string           `json:"autopilot_mode"`
		ActionModes           map[string]string `json:"autopilot_action_modes"`
		ActionHourlyCap       *int              `json:"autopilot_action_hourly_cap"`
		DiagnosticHourlyCap   *int              `json:"autopilot_diagnostic_hourly_cap"`
		CooldownMinutes       *int              `json:"autopilot_cooldown_minutes"`
		EvidenceMaxAgeMinutes *int              `json:"autopilot_evidence_max_age_minutes"`
		TrustMinDecisions     *int              `json:"autopilot_trust_min_decisions"`
		TrustMinAgreementPct  *int              `json:"autopilot_trust_min_agreement_pct"`
	}
	if c.ShouldBindJSON(&req) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid policy"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	policy := loadFeedIntegrityPolicy(db, principal.TenantID)
	if req.Enabled != nil {
		policy.AutopilotEnabled = *req.Enabled
	}
	if req.Mode != nil {
		policy.AutopilotMode = *req.Mode
	}
	if req.ActionModes != nil {
		raw, _ := json.Marshal(req.ActionModes)
		policy.AutopilotActionModes = datatypes.JSON(raw)
	}
	if req.ActionHourlyCap != nil {
		policy.AutopilotActionHourlyCap = *req.ActionHourlyCap
	}
	if req.DiagnosticHourlyCap != nil {
		policy.AutopilotDiagnosticHourlyCap = *req.DiagnosticHourlyCap
	}
	if req.CooldownMinutes != nil {
		policy.AutopilotCooldownMinutes = *req.CooldownMinutes
	}
	if req.EvidenceMaxAgeMinutes != nil {
		policy.AutopilotEvidenceMaxAgeMinutes = *req.EvidenceMaxAgeMinutes
	}
	if req.TrustMinDecisions != nil {
		policy.AutopilotTrustMinDecisions = *req.TrustMinDecisions
	}
	if req.TrustMinAgreementPct != nil {
		policy.AutopilotTrustMinAgreementPct = *req.TrustMinAgreementPct
	}
	sanitizeFeedIntegrityAutopilotPolicy(&policy)
	if err := db.Save(&policy).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not update policy"})
		return
	}
	feedIntegrityAudit(db, principal, "feed_integrity.autopilot.policy.update", principal.TenantID, "success", map[string]interface{}{"mode": policy.AutopilotMode, "enabled": policy.AutopilotEnabled})
	c.JSON(http.StatusOK, gin.H{"data": policy})
}

func RunFeedIntegrityAutopilotNow(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var run models.FeedIntegrityRun
	if db.Where("tenant_id=? AND status IN ?", principal.TenantID, []string{models.FeedIntegrityRunCompleted, models.FeedIntegrityRunPartial}).Order("started_at DESC").First(&run).Error != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "no eligible feed integrity run"})
		return
	}
	if err := evaluateFeedIntegrityAutopilot(db, run.ID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	_ = db.Where("id=?", run.ID).First(&run).Error
	feedIntegrityAudit(db, principal, "feed_integrity.autopilot.run", run.PublicID.String(), "success", nil)
	c.JSON(http.StatusOK, gin.H{"data": run})
}

func PauseFeedIntegrityAutopilot(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	var req struct {
		Minutes int `json:"minutes"`
	}
	_ = c.ShouldBindJSON(&req)
	var until *time.Time
	if req.Minutes > 0 {
		if req.Minutes > 10080 {
			req.Minutes = 10080
		}
		t := time.Now().UTC().Add(time.Duration(req.Minutes) * time.Minute)
		until = &t
	}
	db := c.MustGet("db").(*gorm.DB)
	policy := loadFeedIntegrityPolicy(db, principal.TenantID)
	_ = db.Model(&policy).Updates(map[string]interface{}{"autopilot_paused_until": until, "updated_at": time.Now().UTC()}).Error
	feedIntegrityAudit(db, principal, "feed_integrity.autopilot.pause", principal.TenantID, "success", map[string]interface{}{"minutes": req.Minutes})
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"autopilot_paused_until": until}})
}

func ListFeedIntegrityAutopilotActions(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	if limit < 1 || limit > 100 {
		limit = 50
	}
	q := db.Where("tenant_id=?", principal.TenantID)
	for param, column := range map[string]string{"action_class": "action_class", "outcome": "outcome", "owner": "owner_system", "guardrail": "guardrail"} {
		if v := strings.TrimSpace(c.Query(param)); v != "" {
			q = q.Where(column+"=?", v)
		}
	}
	var rows []models.FeedIntegrityAction
	q.Order("created_at DESC").Limit(limit).Find(&rows)
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"items": rows}})
}

func GetFeedIntegrityAutopilotAction(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid action id"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var row models.FeedIntegrityAction
	if db.Where("tenant_id=? AND public_id=?", principal.TenantID, id).First(&row).Error != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "action not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": row})
}

func ApproveFeedIntegrityAutopilotAction(c *gin.Context) { decideFeedIntegrityAction(c, true) }
func RejectFeedIntegrityAutopilotAction(c *gin.Context)  { decideFeedIntegrityAction(c, false) }
func decideFeedIntegrityAction(c *gin.Context, approve bool) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid action id"})
		return
	}
	var req struct {
		Reason string `json:"reason"`
	}
	_ = c.ShouldBindJSON(&req)
	if !approve && strings.TrimSpace(req.Reason) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "reason is required"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var row models.FeedIntegrityAction
	if db.Where("tenant_id=? AND public_id=?", principal.TenantID, id).First(&row).Error != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "action not found"})
		return
	}
	if row.Outcome != models.FeedIntegrityActionApprovalRequired {
		c.JSON(http.StatusConflict, gin.H{"error": "action is no longer awaiting approval"})
		return
	}
	now := time.Now().UTC()
	outcome := models.FeedIntegrityActionRejected
	decision := models.FeedIntegrityDecisionAttention
	updates := map[string]interface{}{"outcome": outcome, "decision": decision, "reason": req.Reason, "actor": principal.Email, "finished_at": now, "updated_at": now}
	if approve {
		updates["outcome"] = models.FeedIntegrityActionApproved
		updates["decision"] = models.FeedIntegrityDecisionReady
		updates["approved_at"] = now
		updates["finished_at"] = nil
	}
	if db.Model(&models.FeedIntegrityAction{}).Where("id=? AND outcome=?", row.ID, models.FeedIntegrityActionApprovalRequired).Updates(updates).RowsAffected != 1 {
		c.JSON(http.StatusConflict, gin.H{"error": "action changed concurrently"})
		return
	}
	feedIntegrityAudit(db, principal, "feed_integrity.autopilot.action."+map[bool]string{true: "approve", false: "reject"}[approve], row.PublicID.String(), "success", map[string]interface{}{"reason": req.Reason})
	_ = db.Where("id=?", row.ID).First(&row).Error
	if approve {
		if claimed, e := claimFeedIntegrityAction(db, row.TenantID, row.ID); e == nil {
			_ = executeFeedIntegrityAction(db, claimed)
			_ = db.Where("id=?", row.ID).First(&row).Error
		}
	}
	c.JSON(http.StatusOK, gin.H{"data": row})
}

func ResetFeedIntegrityAutopilotBreaker(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	class := strings.TrimSpace(c.Param("class"))
	if class != models.FeedIntegrityActionRefreshWindow && class != models.FeedIntegrityActionConfirm {
		c.JSON(http.StatusNotFound, gin.H{"error": "action class not found"})
		return
	}
	var req struct {
		Reason string `json:"reason"`
	}
	_ = c.ShouldBindJSON(&req)
	if strings.TrimSpace(req.Reason) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "reason is required"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	feedIntegrityAudit(db, principal, "feed_integrity.autopilot.breaker.reset", class, "success", map[string]interface{}{"reason": req.Reason})
	c.JSON(http.StatusOK, gin.H{"data": feedIntegrityTrust(db, principal.TenantID, class, loadFeedIntegrityPolicy(db, principal.TenantID))})
}
