package controllers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"content-management-system/src/models"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	feedIntegrityRegistryVersion     = "fi-registry-v2"
	feedIntegrityVerificationV1      = "fi-verify-v1"
	feedIntegritySnapshotToolVersion = "news-snapshot-v1"
	feedIntegrityClaimLease          = 2 * time.Minute
)

var feedIntegrityRetention = struct {
	sync.Mutex
	Last time.Time
}{}

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
		spec.Owner, spec.ActionClass, spec.DeepLink, spec.AutoEligible = "news-circulation", models.FeedIntegrityActionRefreshWindow, "/platform/news/circulation", true
	case "inv_news_unlabeled_stale":
		spec.Owner, spec.ActionClass, spec.DeepLink = "enrichment", "enrichment.review", "/platform/enrichment"
	case "inv_pods_bounds", "inv_pods_parent_leak", "inv_pods_bucket_mismatch":
		spec.Owner, spec.ActionClass, spec.DeepLink = "media-studio", "media_studio.inspect", "/platform/media/atomization?tab=studio"
	case "inv_pods_playback_missing", "inv_pods_renditions_malformed", "edge_pods_playback_fields", "probe_url_dead", "probe_hls_manifest":
		spec.Owner, spec.ActionClass, spec.DeepLink = "storage", "storage.inspect", "/platform/storage"
	case "edge_pods_empty":
		spec.Owner, spec.ActionClass, spec.DeepLink = "media-circulation", "media_circulation.inspect", "/platform/media/circulation"
	case "edge_news_empty", "edge_news_shape", "edge_news_dup":
		spec.Owner, spec.ActionClass, spec.DeepLink = "news-circulation", "news_circulation.inspect", "/platform/news/circulation"
	case "edge_pods_http", "edge_news_http":
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
	if f.TargetType == "probe" || f.TargetType == "checker" {
		return f.TargetRef
	}
	switch f.TargetType {
	case "content_item", "story", "snapshot", "page", "predicate", "generation":
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
	// Legacy Safe Auto settings are read as Observe. Report the same reduced
	// capability that admission enforces, without rewriting stored policy.
	if p.AutopilotMode != models.FeedIntegrityAutopilotModeAssist {
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
	var n int64
	if err := db.Model(&models.SystemIncidentEpisode{}).Where("status IN ?", []string{"open", "recovering"}).Count(&n).Error; err != nil || n > 0 {
		return true
	}
	var health models.SystemAutopilotRun
	if err := db.Order("started_at DESC").First(&health).Error; err != nil {
		return true
	}
	return health.FinishedAt == nil || health.FinishedAt.After(time.Now().UTC().Add(time.Second)) || time.Since(*health.FinishedAt) > 10*time.Minute || !systemHealthRunHealthy(health)
}

func evaluateFeedIntegrityAutopilot(db *gorm.DB, runID uint) error {
	if !feedIntegrityAutopilotSchemaReady(db) {
		return nil
	}
	var hint models.FeedIntegrityRun
	if err := db.First(&hint, runID).Error; err != nil {
		return err
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var locked models.FeedIntegrityPolicy
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=?", hint.TenantID).First(&locked).Error; err != nil {
			return err
		}
		var run models.FeedIntegrityRun
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&run, runID).Error; err != nil {
			return err
		}
		if run.AutopilotEvaluatedAt != nil || (run.Status != models.FeedIntegrityRunCompleted) {
			return nil
		}
		policy, policyErr := readFeedIntegrityPolicy(tx, run.TenantID)
		if policyErr != nil {
			return policyErr
		}
		if len(integrityRunObservations(run)) == 0 {
			return fmt.Errorf("versioned completed observations required")
		}
		sanitizeFeedIntegrityAutopilotPolicy(&policy)
		now := time.Now().UTC()
		decision := models.FeedIntegrityDecisionNoAction
		counts := map[string]int{"considered": 0, "proposed": 0, "blocked": 0}

		var findings []models.FeedIntegrityFinding
		if err := tx.Where("run_id=? AND status='violation'", run.ID).Order("created_at ASC").Find(&findings).Error; err != nil {
			return err
		}
		sort.SliceStable(findings, func(i, j int) bool {
			left, right := feedIntegrityPriority(findings[i]), feedIntegrityPriority(findings[j])
			if left != right {
				return left > right
			}
			return findings[i].PublicID.String() < findings[j].PublicID.String()
		})
		seenEpisodes := map[uint]bool{}
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
			evidence := finding.Evidence
			if spec.ActionClass == models.FeedIntegrityActionRefreshWindow {
				var precondition models.FeedIntegrityFinding
				window := strings.TrimPrefix(finding.Variant, "window:")
				if err := tx.Where("run_id=? AND check_key='inv_news_cache_rebuild_debt' AND variant=? AND status='violation'", run.ID, window).First(&precondition).Error; err == nil {
					evidence = precondition.Evidence
				}
			}
			input, _ := json.Marshal(gin.H{"check_key": finding.CheckKey, "feed": finding.Feed, "variant": finding.Variant, "scope": scope, "axis": finding.Axis, "deep_link": spec.DeepLink, "evidence": json.RawMessage(evidence)})
			action.Input = datatypes.JSON(input)

			blocked := ""
			mode := feedIntegrityActionMode(policy, spec.ActionClass)
			action.Mode = mode
			if !policy.AutopilotEnabled {
				blocked = "autopilot_disabled"
			} else if policy.AutopilotPausedUntil != nil && policy.AutopilotPausedUntil.After(now) {
				blocked = "autopilot_paused"
			} else if policy.AutopilotMode == "safe_auto" || mode == "safe_auto" {
				blocked = "safe_auto_deferred"
			} else if run.FinishedAt == nil || now.Sub(*run.FinishedAt) > time.Duration(policy.AutopilotEvidenceMaxAgeMinutes)*time.Minute {
				blocked = "stale_evidence"
			} else if isFeedIntegritySuppressed(tx, run.TenantID, finding, now) {
				blocked = "suppressed"
			} else if feedIntegrityActionExecutable(spec.ActionClass) && !feedIntegritySnapshotDualEvidence(tx, run.ID, finding.Variant) {
				blocked = "repair_evidence_missing"
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
				if finding.Axis == "readiness" {
					action.Reason = "Readiness maintenance: healthy delivery still has snapshot rebuild debt; human approval required"
				}
				decision = promoteFeedIntegrityDecision(decision, models.FeedIntegrityDecisionApprovalRequired)
				counts["proposed"]++
			} else {
				action.Outcome, action.Decision, action.Guardrail = "skipped", "blocked", "safe_auto_deferred"
			}

			created := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&action)
			if created.Error != nil {
				return created.Error
			}
			if created.RowsAffected == 0 {
				continue
			}
			if err := tx.Model(&models.FeedIntegrityEpisode{}).Where("id=? AND tenant_id=?", ep.ID, run.TenantID).Updates(map[string]interface{}{"attribution_version": feedIntegrityRegistryVersion, "recommended_action": spec.ActionClass, "latest_action_id": action.ID, "updated_at": now}).Error; err != nil {
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
	var run models.FeedIntegrityRun
	if db.First(&run, runID).Error != nil || run.Status != "completed" {
		return false
	}
	window := strings.TrimPrefix(variant, "window:")
	inventory, ok := integrityObservationFor(run, "inv_news_cache_rebuild_debt", "news", window)
	edge, edgeOK := integrityObservationFor(run, "edge_news_cache_stale", "news", "window:"+window)
	httpProof, httpOK := integrityObservationFor(run, "edge_news_http", "news", "window:"+window)
	return ok && edgeOK && httpOK && inventory.Outcome == "violation" && (edge.Outcome == "pass" || edge.Outcome == "violation") && httpProof.Outcome == "pass"
}

func evaluatePendingFeedIntegrityRuns(db *gorm.DB) {
	if !feedIntegrityAutopilotSchemaReady(db) {
		return
	}
	var runs []models.FeedIntegrityRun
	db.Where("status = ? AND autopilot_evaluated_at IS NULL", models.FeedIntegrityRunCompleted).Order("started_at ASC").Limit(20).Find(&runs)
	for _, run := range runs {
		if err := evaluateFeedIntegrityAutopilot(db, run.ID); err != nil {
			_ = db.Model(&models.FeedIntegrityRun{}).Where("id=?", run.ID).Updates(map[string]interface{}{"autopilot_error_class": "evaluation_failed", "updated_at": time.Now().UTC()}).Error
		}
	}
}

func claimFeedIntegrityAction(db *gorm.DB, tenant string, actionID uint) (models.FeedIntegrityAction, error) {
	return admitIntegrityAction(db, tenant, actionID, "", false)
}
func executeFeedIntegrityAction(db *gorm.DB, action models.FeedIntegrityAction) error {
	admitted, err := admitIntegrityAction(db, action.TenantID, action.ID, action.ClaimToken, true)
	if err != nil {
		return err
	}
	started := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	snapshot, err := buildNewsSnapshotResult(db.WithContext(ctx), admitted.TenantID, admitted.TargetScope)
	if err != nil {
		return markIntegrityEffectUnknown(db, admitted, err)
	}
	count := snapshot.SlideCount
	now := time.Now().UTC()
	due := now.Add(5 * time.Second)
	output := integrityJSON(gin.H{"window": admitted.TargetScope, "slide_count": count, "built_at": snapshot.BuiltAt, "generation": snapshot.Generation, "tool_version": feedIntegritySnapshotToolVersion, "effect_known": true})
	update := db.Model(&models.FeedIntegrityAction{}).Where("id=? AND tenant_id=? AND outcome='running' AND claim_token=?", admitted.ID, admitted.TenantID, admitted.ClaimToken).Updates(map[string]interface{}{"outcome": "verifying", "output": output, "tool_version": feedIntegritySnapshotToolVersion, "executed_at": now, "verification_due_at": due, "duration_ms": now.Sub(started).Milliseconds(), "updated_at": now})
	if update.Error != nil || update.RowsAffected != 1 {
		return markIntegrityEffectUnknown(db, admitted, fmt.Errorf("effect output was not durably recorded"))
	}
	return nil
}

func finishFeedIntegrityActionFailure(db *gorm.DB, action models.FeedIntegrityAction, class string, err error) error {
	now := time.Now().UTC()
	_ = db.Model(&action).Updates(map[string]interface{}{"outcome": models.FeedIntegrityActionToolFailed, "error_class": class, "reason": safeIntegrityError(err), "finished_at": now, "duration_ms": now.Sub(action.UpdatedAt).Milliseconds(), "updated_at": now}).Error
	return err
}

func verifyFeedIntegrityAction(db *gorm.DB, action models.FeedIntegrityAction) {
	var receipt struct {
		Known      bool      `json:"effect_known"`
		BuiltAt    time.Time `json:"built_at"`
		Generation int64     `json:"generation"`
	}
	if json.Unmarshal(action.Output, &receipt) != nil || !receipt.Known || receipt.BuiltAt.IsZero() {
		_ = markIntegrityEffectUnknown(db, action, fmt.Errorf("missing effect receipt"))
		return
	}
	edgePassed, edgeEvidence := verifyFeedIntegrityNewsEdge(action.TargetScope, receipt.BuiltAt)
	// Recheck serving authority after the HTTP probe: ingestion or another
	// builder may invalidate the recorded effect while delivery is inspected.
	var snap struct {
		BuiltAt           time.Time
		Dirty             bool
		SlideCount        int
		Generation        int64
		CurrentGeneration int64
	}
	err := db.Table("news_snapshots snapshot").
		Select("snapshot.built_at, snapshot.dirty, snapshot.slide_count, snapshot.generation, generation.generation AS current_generation").
		Joins("JOIN news_snapshot_generations generation ON generation.tenant_id=snapshot.tenant_id AND generation.\"window\"=snapshot.\"window\"").
		Where("snapshot.tenant_id=? AND snapshot.\"window\"=?", action.TenantID, action.TargetScope).Take(&snap).Error
	now := time.Now().UTC()
	attributable := err == nil && snap.BuiltAt.Equal(receipt.BuiltAt) && snap.Generation == receipt.Generation && snap.CurrentGeneration == receipt.Generation
	passed := attributable && newsSnapshotFreshForLive(snap.BuiltAt, snap.Dirty, now) && snap.SlideCount > 0 && edgePassed
	outcome, reason := models.FeedIntegrityActionVerificationFailed, "Snapshot effect is known; fresh populated delivery was not verified"
	if passed {
		outcome, reason = models.FeedIntegrityActionVerificationPassed, "Recorded snapshot refresh verified at CMS edge"
	}
	verification := integrityJSON(gin.H{"window": action.TargetScope, "passed": passed, "effect_known": true, "recorded_version_current": attributable, "generation": snap.Generation, "current_generation": snap.CurrentGeneration, "dirty": snap.Dirty, "slide_count": snap.SlideCount, "built_at": snap.BuiltAt, "edge": edgeEvidence, "error": safeIntegrityError(err)})
	_ = db.Model(&models.FeedIntegrityAction{}).Where("id=? AND tenant_id=? AND outcome='verifying' AND claim_token=?", action.ID, action.TenantID, action.ClaimToken).Updates(map[string]interface{}{"outcome": outcome, "verification": verification, "reason": reason, "finished_at": now, "updated_at": now}).Error
	// Episode recovery remains owned by consecutive checker observations.
}
func verifyFeedIntegrityNewsEdge(window string, expectedBuiltAt time.Time) (bool, gin.H) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var page integrityNewsPage
	headers, status, _, err := integrityReadPage(ctx, &http.Client{Timeout: 5 * time.Second}, "/api/v1/feed/news?limit=10&window="+url.QueryEscape(window), &page)
	diagnostics, diagErr := integrityNewsDiagnostics(headers, window)
	valid := err == nil && diagErr == nil && len(page.Slides) > 0
	seen := map[string]bool{}
	for _, slide := range page.Slides {
		_, idErr := uuid.Parse(slide.Featured.StoryID)
		if idErr != nil || len(slide.Featured.Members) == 0 || len(slide.Related) > 3 || seen[slide.Featured.StoryID] {
			valid = false
		}
		seen[slide.Featured.StoryID] = true
		related := map[string]bool{}
		for _, story := range slide.Related {
			_, e := uuid.Parse(story.StoryID)
			if e != nil || story.StoryID == slide.Featured.StoryID || related[story.StoryID] {
				valid = false
			}
			related[story.StoryID] = true
		}
	}
	served := diagErr == nil && diagnostics.Source == "cache" && diagnostics.SnapshotBuiltAt.Equal(expectedBuiltAt) && !diagnostics.SnapshotDirty && diagnostics.SnapshotAge <= newsSnapshotTTL
	return valid && served, gin.H{"status": status, "slides": len(page.Slides), "delivery_valid": valid, "recorded_version_served": served, "source": diagnostics.Source, "snapshot_built_at": diagnostics.SnapshotBuiltAt, "snapshot_age_ms": diagnostics.SnapshotAge.Milliseconds(), "error": safeIntegrityError(err), "diagnostics_error": safeIntegrityError(diagErr)}
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
	db.Where("outcome IN ? AND claim_expires_at < ?", []string{"claimed", "running", "verifying"}, now).Limit(20).Find(&stale)
	for _, action := range stale {
		if action.Outcome != "claimed" {
			_ = markIntegrityEffectUnknown(db, action, fmt.Errorf("interrupted effect"))
			continue
		}
		_ = db.Model(&action).Where("id=? AND outcome=? AND claim_expires_at < ?", action.ID, models.FeedIntegrityActionClaimed, now).Updates(map[string]interface{}{"outcome": models.FeedIntegrityActionApproved, "claim_token": "", "claimed_at": nil, "claim_expires_at": nil, "guardrail": "stale_claim_recovered", "updated_at": now}).Error
	}
}

func sweepFeedIntegrityRetention(db *gorm.DB) {
	feedIntegrityRetention.Lock()
	defer feedIntegrityRetention.Unlock()
	now := time.Now().UTC()
	if !feedIntegrityRetention.Last.IsZero() && now.Sub(feedIntegrityRetention.Last) < 24*time.Hour {
		return
	}
	feedIntegrityRetention.Last = now
	_ = db.Where("created_at < ? AND status <> ?", now.Add(-30*24*time.Hour), "violation").Delete(&models.FeedIntegrityFinding{}).Error
	_ = db.Where("created_at < ? AND status='violation' AND NOT EXISTS (SELECT 1 FROM feed_integrity_episodes ep WHERE ep.tenant_id=feed_integrity_findings.tenant_id AND ep.check_key=feed_integrity_findings.check_key AND ep.feed=feed_integrity_findings.feed AND ep.variant=feed_integrity_findings.variant AND ep.status IN ('open','recovering'))", now.Add(-90*24*time.Hour)).Delete(&models.FeedIntegrityFinding{}).Error
	terminal := []string{models.FeedIntegrityActionWouldExecute, models.FeedIntegrityActionRejected, models.FeedIntegrityActionToolFailed, models.FeedIntegrityActionVerificationPassed, models.FeedIntegrityActionVerificationFailed, models.FeedIntegrityActionSkipped, models.FeedIntegrityActionExpired}
	_ = db.Where("created_at < ? AND outcome IN ? AND NOT (error_class='effect_unknown' AND COALESCE(output->'uncertainty_review'->>'disposition','')<>'allow_fresh_approval')", now.Add(-365*24*time.Hour), terminal).Delete(&models.FeedIntegrityAction{}).Error
	_ = db.Where("created_at < ? AND NOT EXISTS (SELECT 1 FROM feed_integrity_actions a WHERE a.run_id=feed_integrity_runs.id)", now.Add(-365*24*time.Hour)).Delete(&models.FeedIntegrityRun{}).Error
}

func feedIntegrityAutopilotStatus(db *gorm.DB, tenant string) gin.H {
	policy := loadFeedIntegrityPolicy(db, tenant)
	sanitizeFeedIntegrityAutopilotPolicy(&policy)
	var latest models.FeedIntegrityRun
	_ = db.Where("tenant_id=?", tenant).Order("started_at DESC").First(&latest).Error
	var pending, stuck int64
	db.Model(&models.FeedIntegrityRun{}).Where("tenant_id=? AND status = ? AND autopilot_evaluated_at IS NULL", tenant, models.FeedIntegrityRunCompleted).Count(&pending)
	db.Model(&models.FeedIntegrityAction{}).Where("tenant_id=? AND outcome IN ? AND updated_at < ?", tenant, []string{models.FeedIntegrityActionClaimed, models.FeedIntegrityActionRunning, models.FeedIntegrityActionVerifying}, time.Now().UTC().Add(-5*time.Minute)).Count(&stuck)
	var actions []models.FeedIntegrityAction
	db.Where("tenant_id=?", tenant).Order("created_at DESC").Limit(20).Find(&actions)
	classes := []string{models.FeedIntegrityActionRefreshWindow, models.FeedIntegrityActionConfirm}
	trust := make([]feedIntegrityTrustStat, 0, len(classes))
	for _, class := range classes {
		trust = append(trust, feedIntegrityTrust(db, tenant, class, policy))
	}
	self := "unknown"
	if latest.ID != 0 {
		self = "healthy"
	}
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
	if _, err := readFeedIntegrityPolicy(db, principal.TenantID); err != nil {
		integrityReadError(c, err)
		return
	}
	var count int64
	for _, table := range []string{"feed_integrity_runs", "feed_integrity_actions"} {
		if err := db.Table(table).Where("tenant_id=?", principal.TenantID).Count(&count).Error; err != nil {
			integrityReadError(c, err)
			return
		}
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
	if req.Mode != nil && *req.Mode != "observe" && *req.Mode != "assist" {
		c.JSON(400, gin.H{"error": "only Observe and Assist are supported; Safe Auto is deferred"})
		return
	}
	for _, mode := range req.ActionModes {
		if mode != "observe" && mode != "assist" {
			c.JSON(400, gin.H{"error": "unsupported per-action mode"})
			return
		}
	}
	if req.TrustMinDecisions != nil || req.TrustMinAgreementPct != nil || req.DiagnosticHourlyCap != nil {
		c.JSON(400, gin.H{"error": "trust and diagnostic promotion controls are read-only compatibility"})
		return
	}
	if (req.ActionHourlyCap != nil && (*req.ActionHourlyCap < 1 || *req.ActionHourlyCap > 20)) || (req.CooldownMinutes != nil && (*req.CooldownMinutes < 15 || *req.CooldownMinutes > 1440)) || (req.EvidenceMaxAgeMinutes != nil && (*req.EvidenceMaxAgeMinutes < 2 || *req.EvidenceMaxAgeMinutes > 60)) {
		c.JSON(400, gin.H{"error": "repair limits outside supported bounds"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	policy, err := readFeedIntegrityPolicy(db, principal.TenantID)
	if err != nil {
		integrityReadError(c, err)
		return
	}
	updates := map[string]interface{}{"updated_at": time.Now().UTC()}
	if req.Enabled != nil {
		updates["autopilot_enabled"] = *req.Enabled
	}
	if req.Mode != nil {
		updates["autopilot_mode"] = *req.Mode
	}
	if req.ActionModes != nil {
		updates["autopilot_action_modes"] = integrityJSON(req.ActionModes)
	}
	if req.ActionHourlyCap != nil {
		updates["autopilot_action_hourly_cap"] = *req.ActionHourlyCap
	}
	if req.CooldownMinutes != nil {
		updates["autopilot_cooldown_minutes"] = *req.CooldownMinutes
	}
	if req.EvidenceMaxAgeMinutes != nil {
		updates["autopilot_evidence_max_age_minutes"] = *req.EvidenceMaxAgeMinutes
	}
	if err := updateIntegrityPolicyAudited(db, principal, "feed_integrity.autopilot.policy.update", updates); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not update policy"})
		return
	}
	policy, err = readFeedIntegrityPolicy(db, principal.TenantID)
	if err != nil {
		integrityReadError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": policy})
}

func RunFeedIntegrityAutopilotNow(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var run models.FeedIntegrityRun
	if db.Where("tenant_id=? AND status = ?", principal.TenantID, models.FeedIntegrityRunCompleted).Order("started_at DESC").First(&run).Error != nil {
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
	if c.ShouldBindJSON(&req) != nil || req.Minutes < 0 || req.Minutes > 10080 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "minutes must be between 0 and 10080"})
		return
	}
	var until *time.Time
	if req.Minutes > 0 {
		if req.Minutes > 10080 {
			req.Minutes = 10080
		}
		t := time.Now().UTC().Add(time.Duration(req.Minutes) * time.Minute)
		until = &t
	}
	db := c.MustGet("db").(*gorm.DB)
	_, policyErr := readFeedIntegrityPolicy(db, principal.TenantID)
	if policyErr != nil {
		integrityReadError(c, policyErr)
		return
	}
	if err := updateIntegrityPolicyAudited(db, principal, "feed_integrity.autopilot.pause", map[string]interface{}{"autopilot_paused_until": until, "updated_at": time.Now().UTC()}); err != nil {
		integrityReadError(c, err)
		return
	}
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
	if err := q.Order("created_at DESC").Limit(limit).Find(&rows).Error; err != nil {
		integrityReadError(c, err)
		return
	}
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
		c.JSON(400, gin.H{"error": "invalid action id"})
		return
	}
	var req struct {
		Reason string `json:"reason"`
	}
	if c.ShouldBindJSON(&req) != nil || strings.TrimSpace(req.Reason) == "" || len(req.Reason) > 2000 {
		c.JSON(400, gin.H{"error": "review note required (up to 2000 characters)"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var row models.FeedIntegrityAction
	err = db.Transaction(func(tx *gorm.DB) error {
		var hint models.FeedIntegrityAction
		if err := tx.Where("tenant_id=? AND public_id=?", principal.TenantID, id).First(&hint).Error; err != nil {
			return err
		}
		if err := lockIntegrityPolicy(tx, principal.TenantID); err != nil {
			return err
		}
		var ep models.FeedIntegrityEpisode
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND id=?", principal.TenantID, hint.EpisodeID).First(&ep).Error; err != nil {
			return err
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND id=?", principal.TenantID, hint.ID).First(&row).Error; err != nil {
			return err
		}
		if row.Outcome != "approval_required" {
			return fmt.Errorf("action no longer awaits approval")
		}
		now := time.Now().UTC()
		updates := map[string]interface{}{"reason": req.Reason, "actor": principal.Email, "updated_at": now, "outcome": "rejected", "finished_at": now}
		if approve {
			updates["outcome"] = "approved"
			updates["approved_at"] = now
			updates["finished_at"] = nil
		}
		if err := tx.Model(&row).Updates(updates).Error; err != nil {
			return err
		}
		if approve {
			claimed, e := admitIntegrityAction(tx, principal.TenantID, row.ID, "", false)
			if e != nil {
				return e
			}
			row = claimed
		}
		return tx.Create(&models.AuditLog{TenantID: principal.TenantID, UserID: principal.UserID, UserEmail: principal.Email, Action: map[bool]string{true: "feed_integrity.autopilot.action.approve", false: "feed_integrity.autopilot.action.reject"}[approve], TargetService: "cms", TargetResource: id.String(), Status: "success", Payload: integrityJSON(gin.H{"reason": req.Reason})}).Error
	})
	if err != nil {
		c.JSON(409, gin.H{"error": safeIntegrityError(err)})
		return
	}
	if approve {
		if err := executeFeedIntegrityAction(db, row); err != nil {
			c.JSON(409, gin.H{"error": safeIntegrityError(err)})
			return
		}
	}
	if err := db.First(&row, row.ID).Error; err != nil {
		integrityReadError(c, err)
		return
	}
	c.JSON(200, gin.H{"data": row})
}

func ResetFeedIntegrityAutopilotBreaker(c *gin.Context) {
	if _, ok := requireAdminPrincipal(c); !ok {
		return
	}
	c.JSON(http.StatusGone, gin.H{"error": "Safe Auto promotion and trust controls are deferred"})
}
