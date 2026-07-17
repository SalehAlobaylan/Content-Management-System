package controllers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/utils"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Media Studio Clearance Autopilot (stage 6) — policy surface + ledger helpers.
// The deterministic runner lives in mediaStudioAutopilotRunner.go (Slice 2);
// the scheduler + trust gate in mediaStudioAutopilotScheduler.go (Slice 3).
// See docs/media-studio-autopilot-plan.md.

// ---------------------------------------------------------------
// Policy load / sanitize
// ---------------------------------------------------------------

func loadEffectiveMediaStudioAutopilotPolicy(db *gorm.DB, tenantID string) models.MediaStudioAutopilotPolicy {
	var policy models.MediaStudioAutopilotPolicy
	if err := db.Where("tenant_id = ?", tenantID).First(&policy).Error; err != nil {
		return models.DefaultMediaStudioAutopilotPolicy(tenantID)
	}
	return sanitizeMediaStudioAutopilotPolicy(policy)
}

func sanitizeMediaStudioAutopilotPolicy(p models.MediaStudioAutopilotPolicy) models.MediaStudioAutopilotPolicy {
	if strings.TrimSpace(p.TenantID) == "" {
		p.TenantID = defaultCirculationTenant
	}
	switch p.AutopilotMode {
	case models.StudioAutopilotModeObserve, models.StudioAutopilotModeSafeAuto:
		// valid
	default:
		p.AutopilotMode = models.StudioAutopilotModeObserve
	}
	p.IntervalMinutes = clampIntRange(p.IntervalMinutes, 15, 1440, 360)
	p.ChainDebounceMinutes = clampIntRange(p.ChainDebounceMinutes, 0, 240, 15)
	p.MaxClearsPerRun = clampIntRange(p.MaxClearsPerRun, 1, 50, 10)
	p.MaxPublishesPerRun = clampIntRange(p.MaxPublishesPerRun, 0, 50, 5)
	p.MaxRejectsPerRun = clampIntRange(p.MaxRejectsPerRun, 0, 50, 10)
	p.MaxSTTPerRun = clampIntRange(p.MaxSTTPerRun, 0, 20, 3)
	p.MaxProposalsPerRun = clampIntRange(p.MaxProposalsPerRun, 0, 15, 15)
	p.AgedThresholdDays = clampIntRange(p.AgedThresholdDays, 1, 90, 7)
	p.DirtyWorkbenchMinutes = clampIntRange(p.DirtyWorkbenchMinutes, 0, 240, 30)
	p.TrustMinDecisions = clampIntRange(p.TrustMinDecisions, 1, 1000, 20)
	p.TrustMinApprovePct = clampIntRange(p.TrustMinApprovePct, 0, 100, 90)
	p.TrustMaxReversalPct = clampIntRange(p.TrustMaxReversalPct, 0, 100, 5)
	return p
}

// ---------------------------------------------------------------
// Admin API — policy get/update
// ---------------------------------------------------------------

func GetMediaStudioAutopilotPolicy(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	c.JSON(http.StatusOK, gin.H{"data": loadEffectiveMediaStudioAutopilotPolicy(db, principal.TenantID)})
}

type mediaStudioAutopilotPolicyPatch struct {
	AutopilotEnabled      *bool            `json:"autopilot_enabled"`
	AutopilotMode         *string          `json:"autopilot_mode"`
	ObserveProposals      *bool            `json:"observe_proposals"`
	IntervalMinutes       *int             `json:"interval_minutes"`
	ChainDebounceMinutes  *int             `json:"chain_debounce_minutes"`
	MaxClearsPerRun       *int             `json:"max_clears_per_run"`
	MaxPublishesPerRun    *int             `json:"max_publishes_per_run"`
	MaxRejectsPerRun      *int             `json:"max_rejects_per_run"`
	MaxSTTPerRun          *int             `json:"max_stt_per_run"`
	MaxProposalsPerRun    *int             `json:"max_proposals_per_run"`
	AgedThresholdDays     *int             `json:"aged_threshold_days"`
	DirtyWorkbenchMinutes *int             `json:"dirty_workbench_minutes"`
	TrustMinDecisions     *int             `json:"trust_min_decisions"`
	TrustMinApprovePct    *int             `json:"trust_min_approve_pct"`
	TrustMaxReversalPct   *int             `json:"trust_max_reversal_pct"`
	PausedUntil           *json.RawMessage `json:"paused_until"`
}

func (p mediaStudioAutopilotPolicyPatch) applyTo(policy *models.MediaStudioAutopilotPolicy) error {
	if p.AutopilotEnabled != nil {
		policy.AutopilotEnabled = *p.AutopilotEnabled
	}
	if p.AutopilotMode != nil {
		policy.AutopilotMode = *p.AutopilotMode
	}
	if p.ObserveProposals != nil {
		policy.ObserveProposals = *p.ObserveProposals
	}
	if p.IntervalMinutes != nil {
		policy.IntervalMinutes = *p.IntervalMinutes
	}
	if p.ChainDebounceMinutes != nil {
		policy.ChainDebounceMinutes = *p.ChainDebounceMinutes
	}
	if p.MaxClearsPerRun != nil {
		policy.MaxClearsPerRun = *p.MaxClearsPerRun
	}
	if p.MaxPublishesPerRun != nil {
		policy.MaxPublishesPerRun = *p.MaxPublishesPerRun
	}
	if p.MaxRejectsPerRun != nil {
		policy.MaxRejectsPerRun = *p.MaxRejectsPerRun
	}
	if p.MaxSTTPerRun != nil {
		policy.MaxSTTPerRun = *p.MaxSTTPerRun
	}
	if p.MaxProposalsPerRun != nil {
		policy.MaxProposalsPerRun = *p.MaxProposalsPerRun
	}
	if p.AgedThresholdDays != nil {
		policy.AgedThresholdDays = *p.AgedThresholdDays
	}
	if p.DirtyWorkbenchMinutes != nil {
		policy.DirtyWorkbenchMinutes = *p.DirtyWorkbenchMinutes
	}
	if p.TrustMinDecisions != nil {
		policy.TrustMinDecisions = *p.TrustMinDecisions
	}
	if p.TrustMinApprovePct != nil {
		policy.TrustMinApprovePct = *p.TrustMinApprovePct
	}
	if p.TrustMaxReversalPct != nil {
		policy.TrustMaxReversalPct = *p.TrustMaxReversalPct
	}
	if p.PausedUntil != nil {
		if string(*p.PausedUntil) == "null" {
			policy.PausedUntil = nil
		} else {
			var raw string
			if err := json.Unmarshal(*p.PausedUntil, &raw); err != nil {
				return fmt.Errorf("paused_until must be RFC3339 or null")
			}
			until, err := time.Parse(time.RFC3339, raw)
			if err != nil {
				return fmt.Errorf("paused_until must be RFC3339 or null")
			}
			policy.PausedUntil = &until
		}
	}
	return nil
}

func UpdateMediaStudioAutopilotPolicy(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var req mediaStudioAutopilotPolicyPatch
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request: " + err.Error(), Code: "INVALID_REQUEST"})
		return
	}
	policy := loadEffectiveMediaStudioAutopilotPolicy(db, principal.TenantID)
	if err := req.applyTo(&policy); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: err.Error(), Code: "INVALID_REQUEST"})
		return
	}
	policy = sanitizeMediaStudioAutopilotPolicy(policy)
	if err := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "tenant_id"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"autopilot_enabled", "autopilot_mode", "observe_proposals",
			"interval_minutes", "chain_debounce_minutes", "max_clears_per_run",
			"max_publishes_per_run", "max_rejects_per_run", "max_stt_per_run",
			"max_proposals_per_run", "aged_threshold_days", "dirty_workbench_minutes",
			"trust_min_decisions", "trust_min_approve_pct", "trust_max_reversal_pct",
			"paused_until", "updated_at",
		}),
	}).Create(&policy).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to save policy", Code: "SAVE_FAILED"})
		return
	}
	writeStudioAutopilotAudit(db, principal, "media_studio.autopilot.policy.update", principal.TenantID, map[string]interface{}{
		"enabled": policy.AutopilotEnabled,
		"mode":    policy.AutopilotMode,
	})
	c.JSON(http.StatusOK, gin.H{"data": policy})
}

// GetMediaStudioAutopilotStatus is the cockpit read-model (§10/§13): policy,
// queue headline, last/next run, per-code trust table, pending-proposal count,
// and the lead-relationship (H4 — chain idle when the lead is off).
func GetMediaStudioAutopilotStatus(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	tenantID := principal.TenantID
	policy := loadEffectiveMediaStudioAutopilotPolicy(db, tenantID)
	health, healthErr := collectStudioHealth(db, tenantID, policy.AgedThresholdDays)
	if healthErr != nil {
		c.JSON(http.StatusServiceUnavailable, authErrorResponse{Message: "Studio health is temporarily unavailable", Code: "HEALTH_UNAVAILABLE"})
		return
	}

	var lastRun models.MediaStudioRun
	hasLast := db.Where("tenant_id = ?", tenantID).Order("started_at DESC").First(&lastRun).Error == nil

	var nextRunAt *time.Time
	if policy.LastRunAt != nil {
		n := policy.LastRunAt.Add(time.Duration(policy.IntervalMinutes) * time.Minute)
		nextRunAt = &n
	}

	// Trust table over the auto-publish candidate codes (V1: merged_short).
	trust := []studioReasonCodeTrust{
		computeStudioReasonCodeTrust(db, tenantID, models.StudioReviewCodeMergedShort, policy),
	}

	// Pending proposals are durable across runs. A newer proposal for the same
	// open chapter supersedes the older draft in the inbox, but neither row is
	// deleted from the audit-grade ledger.
	var pendingProposals int64
	_ = db.Model(&models.MediaStudioAction{}).
		Where("tenant_id = ? AND unit_type = ? AND status = ? AND proposal IS NOT NULL AND proposal <> 'null'::jsonb AND COALESCE(human_outcome, '') = ''",
			tenantID, models.StudioUnitChapterReview, models.StudioActionStatusApprovalRequired).
		Count(&pendingProposals).Error

	// Lead relationship (H4): chain is idle when the lead autopilot is off.
	var leadPolicy models.MediaCirculationPolicy
	leadEnabled := false
	if db.Where("tenant_id = ?", tenantID).First(&leadPolicy).Error == nil {
		leadEnabled = leadPolicy.Enabled && leadPolicy.AutopilotEnabled
	}

	resp := gin.H{
		"policy":            policy,
		"health":            health,
		"next_run_at":       nextRunAt,
		"trust":             trust,
		"pending_proposals": pendingProposals,
		"lead": gin.H{
			"circulation_autopilot_enabled": leadEnabled,
			"chain_idle":                    !leadEnabled,
		},
	}
	if hasLast {
		resp["last_run"] = lastRun
	}
	c.JSON(http.StatusOK, gin.H{"data": resp})
}

// ListMediaStudioAutopilotProposals returns one latest unresolved draft for
// each currently-open chapter. Ranking is confidence descending then oldest
// case, so an older run remains actionable until a human resolves it.
func ListMediaStudioAutopilotProposals(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	policy := loadEffectiveMediaStudioAutopilotPolicy(db, principal.TenantID)
	limit := boundedLimit(c.Query("limit"), 50, 200)
	var actions []models.MediaStudioAction
	if err := db.Where("tenant_id = ? AND unit_type = ? AND status = ? AND chapter_id IS NOT NULL AND proposal IS NOT NULL AND proposal <> 'null'::jsonb AND COALESCE(human_outcome, '') = ''",
		principal.TenantID, models.StudioUnitChapterReview, models.StudioActionStatusApprovalRequired).
		Order("chapter_id ASC, created_at DESC, id DESC").Find(&actions).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to load Studio proposals", Code: "QUERY_FAILED"})
		return
	}
	now := time.Now().UTC()
	seen := map[uuid.UUID]bool{}
	items := make([]gin.H, 0, limit)
	for _, action := range actions {
		if action.ChapterID == nil || seen[*action.ChapterID] {
			continue
		}
		var chapter models.Chapter
		if err := db.Where("public_id = ? AND tenant_id = ? AND status = ?", *action.ChapterID, principal.TenantID, chapterStatusReview).First(&chapter).Error; err != nil {
			continue // stale cases do not belong in the unresolved human inbox
		}
		seen[*action.ChapterID] = true
		proposal := studioProposal{}
		if err := json.Unmarshal(action.Proposal, &proposal); err != nil || (proposal.Proposal != "publish" && proposal.Proposal != "reject") {
			continue
		}
		var child models.ContentItem
		if chapter.ChildContentItemID != nil {
			_ = db.Where("public_id = ? AND tenant_id = ?", *chapter.ChildContentItemID, principal.TenantID).First(&child).Error
		}
		age := now.Sub(chapter.CreatedAt)
		items = append(items, gin.H{
			"action_id": action.PublicID.String(), "chapter_id": chapter.PublicID.String(), "content_item_id": uuidPtrString(chapter.ChildContentItemID), "parent_id": uuidPtrString(child.ParentContentItemID),
			"title": chapter.Title, "summary": chapter.Summary, "review_code": chapter.NeedsReviewCode, "review_reason": chapter.NeedsReviewReason,
			"proposal": proposal.Proposal, "confidence": proposal.Confidence, "rationale": proposal.Rationale, "checked": proposal.Checked,
			"age_hours": age.Hours(), "aged": age >= time.Duration(policy.AgedThresholdDays)*24*time.Hour,
			"created_at": chapter.CreatedAt, "duration_sec": child.DurationSec,
		})
	}
	sort.SliceStable(items, func(i, j int) bool {
		left, lok := items[i]["confidence"].(float64)
		right, rok := items[j]["confidence"].(float64)
		if lok && rok && left != right {
			return left > right
		}
		return items[i]["created_at"].(time.Time).Before(items[j]["created_at"].(time.Time))
	})
	if len(items) > limit {
		items = items[:limit]
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"items": items}})
}

// GetMediaStudioAutopilotInsights returns visualization-ready rollups: per-run
// outcome buckets (for the activity timeline), guardrail/outcome totals (why work
// is held), and the latest run's flow (for the hero decision-flow). Pure read;
// cheap grouped SQL over the ledger + run snapshots.
func GetMediaStudioAutopilotInsights(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	tenantID := principal.TenantID

	var runs []models.MediaStudioRun
	_ = db.Where("tenant_id = ?", tenantID).Order("started_at DESC").Limit(20).Find(&runs).Error

	// One pass over the actions of these runs; bucket by run.
	runIDs := make([]uint, 0, len(runs))
	for _, r := range runs {
		runIDs = append(runIDs, r.ID)
	}
	type actionRow struct {
		RunID     uint
		Verdict   string
		Status    string
		Guardrail string
		N         int
	}
	var rows []actionRow
	if len(runIDs) > 0 {
		_ = db.Model(&models.MediaStudioAction{}).
			Select("run_id, verdict, status, guardrail, COUNT(*) AS n").
			Where("run_id IN ?", runIDs).
			Group("run_id, verdict, status, guardrail").
			Scan(&rows).Error
	}

	// Aggregate per run + global totals.
	perRun := map[uint]*studioRunBuckets{}
	guardrailTotals := map[string]int{}
	outcomeTotals := map[string]int{}
	for _, row := range rows {
		b := perRun[row.RunID]
		if b == nil {
			b = &studioRunBuckets{}
			perRun[row.RunID] = b
		}
		bucketStudioAction(b, row.Verdict, row.Status, row.Guardrail, row.N)
		outcomeTotals[row.Status] += row.N
		if row.Guardrail != "" && isStudioHeldStatus(row.Status) {
			guardrailTotals[row.Guardrail] += row.N
		}
	}

	history := make([]gin.H, 0, len(runs))
	for _, r := range runs {
		b := perRun[r.ID]
		if b == nil {
			b = &studioRunBuckets{}
		}
		before, after := studioRunCaseCounts(r)
		history = append(history, gin.H{
			"id":           r.PublicID.String(),
			"started_at":   r.StartedAt,
			"trigger":      r.Trigger,
			"mode":         r.Mode,
			"status":       r.Status,
			"cases_before": before,
			"cases_after":  after,
			"buckets": gin.H{
				"rejected":      b.Rejected,
				"published":     b.Published,
				"held_approval": b.HeldApproval,
				"skipped":       b.Skipped,
				"errored":       b.Errored,
				"proposals":     b.Proposals,
				"stt":           b.STT,
			},
		})
	}

	// latest_flow = buckets of the most recent completed/partial run.
	var latestFlow gin.H
	for _, r := range runs {
		if r.Status == models.StudioRunStatusCompleted || r.Status == models.StudioRunStatusPartial {
			b := perRun[r.ID]
			if b == nil {
				b = &studioRunBuckets{}
			}
			before, after := studioRunCaseCounts(r)
			latestFlow = gin.H{
				"run_id": r.PublicID.String(), "mode": r.Mode, "trigger": r.Trigger,
				"cases_before": before, "cases_after": after,
				"rejected": b.Rejected, "published": b.Published, "held_approval": b.HeldApproval,
				"held_trust": b.HeldTrust, "held_editorial": b.HeldEditorial, "held_multicode": b.HeldMultiCode,
				"held_upstream": b.HeldUpstream, "skipped": b.Skipped, "errored": b.Errored,
				"proposals": b.Proposals, "stt": b.STT, "observe": r.Mode == models.StudioAutopilotModeObserve,
			}
			break
		}
	}

	c.JSON(http.StatusOK, gin.H{"data": gin.H{
		"run_history":      history,
		"guardrail_totals": guardrailTotals,
		"outcome_totals":   outcomeTotals,
		"latest_flow":      latestFlow,
	}})
}

type studioRunBuckets struct {
	Rejected, Published, HeldApproval, Skipped, Errored, Proposals, STT int
	HeldTrust, HeldEditorial, HeldMultiCode, HeldUpstream               int
}

func isStudioHeldStatus(status string) bool {
	return status == models.StudioActionStatusApprovalRequired ||
		status == models.StudioActionStatusSkipped ||
		status == models.StudioActionStatusWouldSkip
}

// bucketStudioAction maps a (verdict, status, guardrail, count) group into the
// outcome buckets the visuals consume. Observe would_apply/would_skip fold into
// the same buckets as their Safe-Auto equivalents so the flow shape is identical.
func bucketStudioAction(b *studioRunBuckets, verdict, status, guardrail string, n int) {
	switch status {
	case models.StudioActionStatusSuccess, models.StudioActionStatusWouldApply:
		switch verdict {
		case models.StudioVerdictAutoRejectImpossible:
			b.Rejected += n
		case models.StudioVerdictAutoPublishMechanical:
			b.Published += n
		case models.StudioVerdictRerunSTT:
			b.STT += n
		}
	case models.StudioActionStatusApprovalRequired:
		b.HeldApproval += n
		switch guardrail {
		case models.StudioGuardTrustGate:
			b.HeldTrust += n
		case models.StudioGuardEditorialReason:
			b.HeldEditorial += n
		case models.StudioGuardMultiCode:
			b.HeldMultiCode += n
		case models.StudioGuardUpstreamDisabled:
			b.HeldUpstream += n
		}
	case models.StudioActionStatusSkipped, models.StudioActionStatusWouldSkip:
		b.Skipped += n
	case models.StudioActionStatusError:
		b.Errored += n
	}
	// Proposal phase / drafted proposals: verdict-based, mode-agnostic.
	if verdict == models.StudioVerdictProposePublish || verdict == models.StudioVerdictProposeReject {
		b.Proposals += n
	}
}

// studioRunCaseCounts pulls review_queue_depth from the run's health snapshots.
func studioRunCaseCounts(r models.MediaStudioRun) (before, after int) {
	before = studioSnapshotDepth(r.HealthBefore)
	after = studioSnapshotDepth(r.HealthAfter)
	return
}

func studioSnapshotDepth(raw datatypes.JSON) int {
	if len(raw) == 0 {
		return 0
	}
	var snap struct {
		ReviewQueueDepth int `json:"review_queue_depth"`
	}
	if err := json.Unmarshal(raw, &snap); err != nil {
		return 0
	}
	return snap.ReviewQueueDepth
}

// PauseMediaStudioAutopilot sets paused_until; the scheduler skips all triggers
// while paused (S7). Body: {"minutes": <int>} (default 720 = 12h).
func PauseMediaStudioAutopilot(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var body struct {
		Minutes int `json:"minutes"`
	}
	_ = c.ShouldBindJSON(&body)
	minutes := body.Minutes
	if minutes <= 0 {
		minutes = 720
	}
	until := time.Now().UTC().Add(time.Duration(minutes) * time.Minute)
	policy := loadEffectiveMediaStudioAutopilotPolicy(db, principal.TenantID)
	policy.PausedUntil = &until
	if err := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "tenant_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"paused_until", "updated_at"}),
	}).Create(&policy).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to pause", Code: "SAVE_FAILED"})
		return
	}
	writeStudioAutopilotAudit(db, principal, "media_studio.autopilot.pause", principal.TenantID, map[string]interface{}{"paused_until": until})
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"paused_until": until}})
}

// ---------------------------------------------------------------
// Ledger helpers (modeled on the circulation runner's persistence)
// ---------------------------------------------------------------

func startStudioRun(db *gorm.DB, tenantID, trigger, mode, createdBy string, healthBefore any) (*models.MediaStudioRun, error) {
	run := models.MediaStudioRun{
		TenantID:  tenantID,
		Trigger:   trigger,
		Mode:      mode,
		Status:    models.StudioRunStatusRunning,
		StartedAt: time.Now().UTC(),
		CreatedBy: createdBy,
	}
	if healthBefore != nil {
		if raw, err := json.Marshal(healthBefore); err == nil {
			run.HealthBefore = datatypes.JSON(raw)
		}
	}
	if err := db.Create(&run).Error; err != nil {
		return nil, err
	}
	return &run, nil
}

func finishStudioRun(db *gorm.DB, run *models.MediaStudioRun, status, summary string, healthAfter any, runErr string) error {
	now := time.Now().UTC()
	run.Status = status
	run.Summary = summary
	run.FinishedAt = &now
	run.Error = runErr
	if healthAfter != nil {
		if raw, err := json.Marshal(healthAfter); err == nil {
			run.HealthAfter = datatypes.JSON(raw)
		}
	}
	return db.Save(run).Error
}

// studioActionInput is the ledger row payload for one considered case.
type studioActionInput struct {
	UnitType         string
	ChapterID        *uuid.UUID
	ContentItemID    *uuid.UUID
	RecommendationID *uuid.UUID
	Verdict          string
	ToolName         string
	Status           string
	Reason           string
	Guardrail        string
	FeedImpact       int
	STTImpact        int
	Input            any
	Output           any
	Err              string
}

func recordStudioAction(db *gorm.DB, run *models.MediaStudioRun, in studioActionInput) (*models.MediaStudioAction, error) {
	now := time.Now().UTC()
	action := models.MediaStudioAction{
		RunID:            run.ID,
		TenantID:         run.TenantID,
		UnitType:         in.UnitType,
		ChapterID:        in.ChapterID,
		ContentItemID:    in.ContentItemID,
		RecommendationID: in.RecommendationID,
		Verdict:          in.Verdict,
		ToolName:         in.ToolName,
		Status:           in.Status,
		Reason:           in.Reason,
		Guardrail:        in.Guardrail,
		FeedImpact:       in.FeedImpact,
		STTImpact:        in.STTImpact,
		Error:            in.Err,
		StartedAt:        now,
		FinishedAt:       &now,
	}
	if in.Input != nil {
		if raw, err := json.Marshal(in.Input); err == nil {
			action.Input = datatypes.JSON(raw)
		}
	}
	if in.Output != nil {
		if raw, err := json.Marshal(in.Output); err == nil {
			action.Output = datatypes.JSON(raw)
		}
	}
	if err := db.Create(&action).Error; err != nil {
		return nil, err
	}
	return &action, nil
}

// writeStudioAutopilotAudit records human policy/pause changes in the audit log.
func writeStudioAutopilotAudit(db *gorm.DB, principal utils.AdminPrincipal, action, resource string, payload map[string]interface{}) {
	entry := models.AuditLog{
		TenantID:       principal.TenantID,
		UserID:         principal.UserID,
		UserEmail:      principal.Email,
		Action:         action,
		TargetService:  "media_studio",
		TargetResource: resource,
		Status:         "success",
	}
	if payload != nil {
		if raw, err := json.Marshal(payload); err == nil {
			entry.Payload = datatypes.JSON(raw)
		}
	}
	_ = db.Create(&entry).Error
}
