package controllers

import (
	"content-management-system/src/models"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// Preferences Autopilot — admin endpoints + the human-only catalog-topic merge.
// The five autopilot routes follow the family convention; the merge endpoint is a
// human catalog operation (content:write) and is NEVER registered as an autopilot
// tool (plan §10, §14).

// ----------------------------------------------------------------
// Policy load + sanitize
// ----------------------------------------------------------------

func loadPreferenceAutopilotPolicy(db *gorm.DB, tenantID string) models.PreferenceAutopilotPolicy {
	if strings.TrimSpace(tenantID) == "" {
		tenantID = defaultCirculationTenant
	}
	var policy models.PreferenceAutopilotPolicy
	if err := db.Where("tenant_id = ?", tenantID).First(&policy).Error; err != nil {
		policy = models.DefaultPreferenceAutopilotPolicy(tenantID)
	}
	return sanitizePreferenceAutopilotPolicy(policy)
}

func sanitizePreferenceAutopilotPolicy(p models.PreferenceAutopilotPolicy) models.PreferenceAutopilotPolicy {
	if p.Mode != models.PreferenceAutopilotModeSafeAuto {
		p.Mode = models.PreferenceAutopilotModeObserve
	}
	p.IntervalMinutes = clampIntOrDefault(p.IntervalMinutes, 5, 1440, 15)
	p.MaxItemCandidates = clampIntOrDefault(p.MaxItemCandidates, 10, 5000, 250)
	// The following caps use a plain range clamp (NOT clampIntOrDefault) because 0 is
	// a MEANINGFUL value — it disables that action class. clampIntOrDefault treats 0
	// as "unset" and would silently revert an operator's deliberate 0 to the default.
	p.MaxStoryCandidates = clampInt(p.MaxStoryCandidates, 0, 5000)
	p.MaxDirtyTopics = clampInt(p.MaxDirtyTopics, 0, 100)
	p.MaxUsersRecompute = clampInt(p.MaxUsersRecompute, 0, 5000)
	p.MaxProposalsEnriched = clampInt(p.MaxProposalsEnriched, 0, 500)
	p.MaxEmbeddingCalls = clampInt(p.MaxEmbeddingCalls, 0, 500)
	p.MaxTranslationCalls = clampInt(p.MaxTranslationCalls, 0, 200)
	p.MaxMinedProposals = clampInt(p.MaxMinedProposals, 0, 500)
	p.MaxCentroidRefresh = clampInt(p.MaxCentroidRefresh, 0, 200)
	p.MaxPendingProposals = clampIntOrDefault(p.MaxPendingProposals, 1, 100000, 100)
	p.CoverageFloorForyouPct = clampIntOrDefault(p.CoverageFloorForyouPct, 1, 100, 70)
	p.CoverageFloorNewsPct = clampIntOrDefault(p.CoverageFloorNewsPct, 1, 100, 60)
	p.CoverageFloorStoryPct = clampIntOrDefault(p.CoverageFloorStoryPct, 1, 100, 50)
	p.FailureBreakerPct = clampIntOrDefault(p.FailureBreakerPct, 1, 100, 25)
	p.DeadTopicDays = clampIntOrDefault(p.DeadTopicDays, 1, 365, 14)
	p.TrustMinDecisions = clampIntOrDefault(p.TrustMinDecisions, 1, 100000, 30)
	p.TrustMinAgreementPct = clampIntOrDefault(p.TrustMinAgreementPct, 1, 100, 90)
	p.HighConfidence = clampFloatOrDefault(p.HighConfidence, 0.5, 1.0, 0.80)
	p.AdvisoryRejectFloor = clampFloatOrDefault(p.AdvisoryRejectFloor, 0.0, 0.7, 0.35)
	p.DuplicateCosine = clampFloatOrDefault(p.DuplicateCosine, 0.5, 1.0, 0.90)
	// Auto-approve tier: 0.85 floor means the tier can NEVER be tuned down to the
	// 0.80 high-confidence default; cap uses clampInt because 0 meaningfully
	// disables auto-approvals while leaving the switch/trust machinery visible.
	p.AutoApproveMinConfidence = clampFloatOrDefault(p.AutoApproveMinConfidence, 0.85, 1.0, 0.92)
	p.MaxAutoApprovals = clampInt(p.MaxAutoApprovals, 0, 20)
	return p
}

func clampFloatOrDefault(v, lo, hi, def float64) float64 {
	if v == 0 {
		return def
	}
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// ----------------------------------------------------------------
// Trust ladder evidence (§15) — read-only banner in V1
// ----------------------------------------------------------------

type preferenceTrustBanner struct {
	Decisions     int64   `json:"decisions"`
	Agreements    int64   `json:"agreements"`
	AgreementPct  float64 `json:"agreement_pct"`
	MinDecisions  int     `json:"min_decisions"`
	MinAgreement  int     `json:"min_agreement_pct"`
	Eligible      bool    `json:"eligible"`
	MuteViolation bool    `json:"mute_violation"`
}

// computePreferenceTrust compares each enriched proposal's FROZEN decisive
// prediction to the HUMAN outcome. review predictions are not decisive and are
// excluded from the denominator. Resolutions made by the autopilot itself
// (resolved_by = autopilot:preferences) are EXCLUDED — without this, the earned
// auto-approve tier's own approvals would count as "agreements" and inflate its
// own eligibility. Trust is earned from human decisions only.
func computePreferenceTrust(db *gorm.DB, tenantID string, policy models.PreferenceAutopilotPolicy, muteViolations int64) preferenceTrustBanner {
	var row struct {
		Decisions  int64
		Agreements int64
	}
	db.Model(&models.TopicProposal{}).
		Select(`
			COUNT(*) FILTER (WHERE predicted_verdict IN ('high_confidence','suggest_reject') AND status IN ('approved','rejected')) AS decisions,
			COUNT(*) FILTER (WHERE (predicted_verdict = 'high_confidence' AND status = 'approved') OR (predicted_verdict = 'suggest_reject' AND status = 'rejected')) AS agreements`).
		Where("tenant_id = ? AND prediction_version <> '' AND COALESCE(resolved_by, '') <> ?", tenantID, models.PreferenceAutopilotResolver).Scan(&row)
	b := preferenceTrustBanner{
		Decisions: row.Decisions, Agreements: row.Agreements,
		MinDecisions: policy.TrustMinDecisions, MinAgreement: policy.TrustMinAgreementPct,
		MuteViolation: muteViolations > 0,
	}
	if row.Decisions > 0 {
		b.AgreementPct = 100 * float64(row.Agreements) / float64(row.Decisions)
	}
	b.Eligible = row.Decisions >= int64(policy.TrustMinDecisions) &&
		b.AgreementPct >= float64(policy.TrustMinAgreementPct) && !b.MuteViolation
	return b
}

// ----------------------------------------------------------------
// Status block (cockpit read model) — lightweight (§11)
// ----------------------------------------------------------------

type preferenceAttentionTopic struct {
	ID          string `json:"id"`
	Slug        string `json:"slug"`
	LabelEN     string `json:"label_en"`
	MemberCount int    `json:"member_count"`
}

type preferenceAutopilotStatusBlock struct {
	Enabled           bool                             `json:"enabled"`
	Mode              string                           `json:"mode"`
	State             string                           `json:"state"` // off | observe | safe_auto | paused
	IntervalMinutes   int                              `json:"interval_minutes"`
	PausedUntil       *time.Time                       `json:"paused_until,omitempty"`
	LastRunAt         *time.Time                       `json:"last_run_at,omitempty"`
	NextRunAt         *time.Time                       `json:"next_run_at,omitempty"`
	SnapshotAgeSec    *int64                           `json:"snapshot_age_sec,omitempty"`
	Headline          string                           `json:"headline"`
	RecommendedAction string                           `json:"recommended_action,omitempty"`
	LastRun           *models.PreferenceAutopilotRun   `json:"last_run,omitempty"`
	Snapshot          *preferenceSnapshot              `json:"snapshot,omitempty"`
	FlipGates         map[string]preferenceFlipGate    `json:"flip_gates,omitempty"`
	Trust             preferenceTrustBanner            `json:"trust"`
	NullCentroids     []preferenceAttentionTopic       `json:"null_centroid_topics"`
	DeadTopics        []preferenceAttentionTopic       `json:"dead_topics"`
	PendingProposals  int64                            `json:"pending_proposals"`
	Policy            models.PreferenceAutopilotPolicy `json:"policy"`
}

func buildPreferenceAutopilotStatus(db *gorm.DB, tenantID string, policy models.PreferenceAutopilotPolicy) preferenceAutopilotStatusBlock {
	now := time.Now()
	block := preferenceAutopilotStatusBlock{
		Enabled: policy.Enabled, Mode: policy.Mode, IntervalMinutes: policy.IntervalMinutes,
		PausedUntil: policy.PausedUntil, LastRunAt: policy.LastRunAt, Policy: policy,
		Headline:      models.PreferenceAutopilotHeadlineNotObserved,
		NullCentroids: []preferenceAttentionTopic{}, DeadTopics: []preferenceAttentionTopic{},
	}
	switch {
	case !policy.Enabled:
		block.State = "off"
	case policy.PausedUntil != nil && policy.PausedUntil.After(now):
		block.State = "paused"
	default:
		block.State = policy.Mode
	}
	if policy.Enabled && block.State != "paused" {
		next := now
		if policy.LastRunAt != nil {
			next = policy.LastRunAt.Add(time.Duration(policy.IntervalMinutes) * time.Minute)
		}
		block.NextRunAt = &next
	}

	// Latest completed run carries the persisted snapshot + verdict (§11). status
	// never launches a heavy scan from a GET.
	var lastRun models.PreferenceAutopilotRun
	if err := db.Where("tenant_id = ? AND status IN ?", tenantID,
		[]string{models.PreferenceAutopilotRunStatusCompleted, models.PreferenceAutopilotRunStatusPartial, models.PreferenceAutopilotRunStatusFailed}).
		Order("started_at DESC").First(&lastRun).Error; err == nil {
		block.LastRun = &lastRun
		block.Headline = lastRun.Headline
		block.RecommendedAction = lastRun.RecommendedAction
		if lastRun.FinishedAt != nil {
			age := int64(now.Sub(*lastRun.FinishedAt).Seconds())
			block.SnapshotAgeSec = &age
		}
		if len(lastRun.StatsAfter) > 0 {
			var snap preferenceSnapshot
			if json.Unmarshal(lastRun.StatsAfter, &snap) == nil {
				block.Snapshot = &snap
				block.FlipGates = snap.FlipGates
			}
		}
	}
	if block.LastRun == nil {
		block.RecommendedAction = "Run now — no completed observation yet. Enable Observe and run to produce a verdict."
	}

	// Lightweight live checks (§11): pending count + a mute-integrity probe for the
	// trust banner, plus small indexed attention lists.
	db.Model(&models.TopicProposal{}).Where("tenant_id = ? AND status = ?", tenantID, "pending").Count(&block.PendingProposals)
	var muteViolations int64
	db.Raw(`
		SELECT COUNT(*) FROM user_topic_prefs p
		JOIN user_topic_affinity a ON a.tenant_id = p.tenant_id AND a.user_id = p.user_id AND a.topic_id = p.topic_id
		WHERE p.tenant_id = ? AND p.state = 'muted'`, tenantID).Scan(&muteViolations)
	block.Trust = computePreferenceTrust(db, tenantID, policy, muteViolations)

	block.NullCentroids = loadAttentionTopics(db, tenantID, "centroid IS NULL", 20)
	deadBefore := now.AddDate(0, 0, -policy.DeadTopicDays)
	block.DeadTopics = loadAttentionTopicsArgs(db, tenantID, "member_count = 0 AND created_at < ?", []interface{}{deadBefore}, 20)
	return block
}

func loadAttentionTopics(db *gorm.DB, tenantID, cond string, limit int) []preferenceAttentionTopic {
	return loadAttentionTopicsArgs(db, tenantID, cond, nil, limit)
}

func loadAttentionTopicsArgs(db *gorm.DB, tenantID, cond string, args []interface{}, limit int) []preferenceAttentionTopic {
	q := db.Model(&models.Topic{}).Where("tenant_id = ? AND active = ?", tenantID, true)
	if len(args) > 0 {
		q = q.Where(cond, args...)
	} else {
		q = q.Where(cond)
	}
	var topics []models.Topic
	q.Order("updated_at ASC").Limit(limit).Find(&topics)
	out := make([]preferenceAttentionTopic, 0, len(topics))
	for _, t := range topics {
		out = append(out, preferenceAttentionTopic{ID: t.PublicID.String(), Slug: t.Slug, LabelEN: t.LabelEN, MemberCount: t.MemberCount})
	}
	return out
}

// ----------------------------------------------------------------
// Endpoints (§10)
// ----------------------------------------------------------------

// GET /admin/preferences/autopilot/status
func GetPreferenceAutopilotStatus(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	policy := loadPreferenceAutopilotPolicy(db, principal.TenantID)
	c.JSON(http.StatusOK, gin.H{"data": buildPreferenceAutopilotStatus(db, principal.TenantID, policy)})
}

type updatePreferenceAutopilotRequest struct {
	Enabled              *bool    `json:"enabled"`
	Mode                 *string  `json:"mode"`
	IntervalMinutes      *int     `json:"interval_minutes"`
	MaxItemCandidates    *int     `json:"max_item_candidates"`
	MaxStoryCandidates   *int     `json:"max_story_candidates"`
	MaxDirtyTopics       *int     `json:"max_dirty_topics"`
	MaxUsersRecompute    *int     `json:"max_users_recompute"`
	MaxProposalsEnriched *int     `json:"max_proposals_enriched"`
	MaxEmbeddingCalls    *int     `json:"max_embedding_calls"`
	MaxTranslationCalls  *int     `json:"max_translation_calls"`
	MaxMinedProposals    *int     `json:"max_mined_proposals"`
	MaxCentroidRefresh   *int     `json:"max_centroid_refresh"`
	MaxPendingProposals  *int     `json:"max_pending_proposals"`
	CoverageFloorForyou  *int     `json:"coverage_floor_foryou_pct"`
	CoverageFloorNews    *int     `json:"coverage_floor_news_pct"`
	CoverageFloorStory   *int     `json:"coverage_floor_story_pct"`
	FailureBreakerPct    *int     `json:"failure_breaker_pct"`
	DeadTopicDays        *int     `json:"dead_topic_days"`
	HighConfidence       *float64 `json:"high_confidence"`
	AdvisoryRejectFloor  *float64 `json:"advisory_reject_floor"`
	DuplicateCosine      *float64 `json:"duplicate_cosine"`
	PausedMinutes        *int     `json:"paused_minutes"` // 0 = resume
	AutoApproveEnabled   *bool    `json:"auto_approve_enabled"`
	AutoApproveMinConf   *float64 `json:"auto_approve_min_confidence"`
	MaxAutoApprovals     *int     `json:"max_auto_approvals"`
}

// PUT /admin/preferences/autopilot/policy
func UpdatePreferenceAutopilotPolicy(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var req updatePreferenceAutopilotRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request: " + err.Error(), Code: "INVALID_REQUEST"})
		return
	}
	var policy models.PreferenceAutopilotPolicy
	if err := db.Where("tenant_id = ?", principal.TenantID).First(&policy).Error; err != nil {
		policy = models.DefaultPreferenceAutopilotPolicy(principal.TenantID)
	}
	if req.Enabled != nil {
		policy.Enabled = *req.Enabled
	}
	if req.Mode != nil {
		policy.Mode = *req.Mode
	}
	setIntPtr(&policy.IntervalMinutes, req.IntervalMinutes)
	setIntPtr(&policy.MaxItemCandidates, req.MaxItemCandidates)
	setIntPtr(&policy.MaxStoryCandidates, req.MaxStoryCandidates)
	setIntPtr(&policy.MaxDirtyTopics, req.MaxDirtyTopics)
	setIntPtr(&policy.MaxUsersRecompute, req.MaxUsersRecompute)
	setIntPtr(&policy.MaxProposalsEnriched, req.MaxProposalsEnriched)
	setIntPtr(&policy.MaxEmbeddingCalls, req.MaxEmbeddingCalls)
	setIntPtr(&policy.MaxTranslationCalls, req.MaxTranslationCalls)
	setIntPtr(&policy.MaxMinedProposals, req.MaxMinedProposals)
	setIntPtr(&policy.MaxCentroidRefresh, req.MaxCentroidRefresh)
	setIntPtr(&policy.MaxPendingProposals, req.MaxPendingProposals)
	setIntPtr(&policy.CoverageFloorForyouPct, req.CoverageFloorForyou)
	setIntPtr(&policy.CoverageFloorNewsPct, req.CoverageFloorNews)
	setIntPtr(&policy.CoverageFloorStoryPct, req.CoverageFloorStory)
	setIntPtr(&policy.FailureBreakerPct, req.FailureBreakerPct)
	setIntPtr(&policy.DeadTopicDays, req.DeadTopicDays)
	setFloatPtr(&policy.HighConfidence, req.HighConfidence)
	setFloatPtr(&policy.AdvisoryRejectFloor, req.AdvisoryRejectFloor)
	setFloatPtr(&policy.DuplicateCosine, req.DuplicateCosine)
	setFloatPtr(&policy.AutoApproveMinConfidence, req.AutoApproveMinConf)
	setIntPtr(&policy.MaxAutoApprovals, req.MaxAutoApprovals)
	// Earned-tier enable is SERVER-gated on trust eligibility (off→on transition
	// only). The Console disables the switch too, but UI gating is cosmetic —
	// this check is the real boundary.
	if req.AutoApproveEnabled != nil {
		if *req.AutoApproveEnabled && !policy.AutoApproveEnabled {
			var muteViolations int64
			db.Raw(`
				SELECT COUNT(*) FROM user_topic_prefs p
				JOIN user_topic_affinity a ON a.tenant_id = p.tenant_id AND a.user_id = p.user_id AND a.topic_id = p.topic_id
				WHERE p.tenant_id = ? AND p.state = 'muted'`, principal.TenantID).Scan(&muteViolations)
			trust := computePreferenceTrust(db, principal.TenantID, sanitizePreferenceAutopilotPolicy(policy), muteViolations)
			if !trust.Eligible {
				c.JSON(http.StatusConflict, authErrorResponse{
					Message: fmt.Sprintf("Auto-approve requires earned trust: %d decisions at %.0f%% agreement (needs ≥%d at ≥%d%%, no mute violations).",
						trust.Decisions, trust.AgreementPct, trust.MinDecisions, trust.MinAgreement),
					Code: "TRUST_NOT_ELIGIBLE",
				})
				return
			}
		}
		policy.AutoApproveEnabled = *req.AutoApproveEnabled
	}
	if req.PausedMinutes != nil {
		if *req.PausedMinutes <= 0 {
			policy.PausedUntil = nil
		} else {
			minutes := *req.PausedMinutes
			if minutes > 10080 {
				minutes = 10080
			}
			t := time.Now().Add(time.Duration(minutes) * time.Minute)
			policy.PausedUntil = &t
		}
	}
	policy.TenantID = principal.TenantID
	policy = sanitizePreferenceAutopilotPolicy(policy)

	if err := db.Save(&policy).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to save policy", Code: "SAVE_FAILED"})
		return
	}
	writeCirculationAudit(db, principal, "preferences.autopilot.policy", principal.TenantID, map[string]interface{}{
		"enabled": policy.Enabled, "mode": policy.Mode, "auto_approve_enabled": policy.AutoApproveEnabled,
	})
	c.JSON(http.StatusOK, gin.H{"data": buildPreferenceAutopilotStatus(db, principal.TenantID, policy)})
}

func setIntPtr(dst *int, src *int) {
	if src != nil {
		*dst = *src
	}
}

func setFloatPtr(dst *float64, src *float64) {
	if src != nil {
		*dst = *src
	}
}

// POST /admin/preferences/autopilot/run
func RunPreferenceAutopilotNow(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	run, err := runPreferenceAutopilot(db, principal.TenantID, preferenceAutopilotRunOptions{
		Trigger: "manual", CreatedBy: principal.Email,
	})
	if err != nil {
		switch {
		case errors.Is(err, errPreferenceAutopilotDisabled):
			c.JSON(http.StatusConflict, authErrorResponse{Message: err.Error(), Code: "AUTOPILOT_DISABLED"})
		case errors.Is(err, errPreferenceAutopilotAlreadyRunning):
			c.JSON(http.StatusConflict, authErrorResponse{Message: err.Error(), Code: "AUTOPILOT_ALREADY_RUNNING"})
		default:
			c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Autopilot run failed: " + err.Error(), Code: "RUN_FAILED"})
		}
		return
	}
	writeCirculationAudit(db, principal, "preferences.autopilot.run", principal.TenantID, map[string]interface{}{
		"run_id": run.PublicID.String(), "status": run.Status, "headline": run.Headline, "summary": run.Summary,
	})
	var actions []models.PreferenceAutopilotAction
	_ = db.Where("tenant_id = ? AND run_id = ?", principal.TenantID, run.ID).Order("started_at ASC, id ASC").Find(&actions).Error
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"run": run, "actions": actions}})
}

// GET /admin/preferences/autopilot/runs
func ListPreferenceAutopilotRuns(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var runs []models.PreferenceAutopilotRun
	if err := db.Where("tenant_id = ?", principal.TenantID).
		Order("started_at DESC").Limit(boundedLimit(c.Query("limit"), 20, 100)).Find(&runs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to list runs", Code: "QUERY_FAILED"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"items": runs}})
}

// GET /admin/preferences/autopilot/runs/:id/actions
func GetPreferenceAutopilotRunActions(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	runID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid run ID", Code: "INVALID_ID"})
		return
	}
	var run models.PreferenceAutopilotRun
	if err := db.Where("tenant_id = ? AND public_id = ?", principal.TenantID, runID).First(&run).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Run not found", Code: "NOT_FOUND"})
		return
	}
	var actions []models.PreferenceAutopilotAction
	_ = db.Where("tenant_id = ? AND run_id = ?", principal.TenantID, run.ID).Order("started_at ASC, id ASC").Find(&actions).Error
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"run": run, "actions": actions}})
}

// ----------------------------------------------------------------
// Human-only catalog-topic merge (§6.7, §14) — NOT an autopilot tool
// ----------------------------------------------------------------

type mergeCatalogTopicRequest struct {
	Into string `json:"into"`
}

// AdminMergeCatalogTopic handles POST /admin/topics/catalog/:id/merge. One
// transaction rehomes content/story mappings and user pref/affinity rows from the
// source (loser) into the target (survivor), resolves unique-key collisions
// (muted wins over declared on prefs; best-continuity score on affinity/mappings),
// deactivates the source, marks the survivor dirty, and queues every affected user
// for authoritative recompute. No autopilot path calls this.
func AdminMergeCatalogTopic(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	tenant := principal.TenantID
	sourceID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid source topic id", Code: "INVALID_ID"})
		return
	}
	var req mergeCatalogTopicRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request", Code: "INVALID_REQUEST"})
		return
	}
	targetID, err := uuid.Parse(strings.TrimSpace(req.Into))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid target topic id", Code: "INVALID_TARGET"})
		return
	}
	if sourceID == targetID {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Cannot merge a topic into itself", Code: "INVALID_TARGET"})
		return
	}
	var source, target models.Topic
	if err := db.Where("tenant_id = ? AND public_id = ?", tenant, sourceID).First(&source).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Source topic not found", Code: "NOT_FOUND"})
		return
	}
	if err := db.Where("tenant_id = ? AND public_id = ? AND active = ?", tenant, targetID, true).First(&target).Error; err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Target topic is not an active topic in this tenant", Code: "INVALID_TARGET"})
		return
	}

	var affectedUsers []uuid.UUID
	err = db.Transaction(func(tx *gorm.DB) error {
		// Affected users BEFORE the moves, for authoritative recompute afterward.
		tx.Raw(`
			SELECT DISTINCT user_id FROM (
				SELECT user_id FROM user_topic_prefs WHERE tenant_id = ? AND topic_id = ?
				UNION
				SELECT user_id FROM user_topic_affinity WHERE tenant_id = ? AND topic_id = ?
			) u`, tenant, sourceID, tenant, sourceID).Scan(&affectedUsers)

		// content_item_topics: keep best score on collision, then move the rest.
		if err := tx.Exec(`
			UPDATE content_item_topics t SET score = GREATEST(t.score, s.score)
			FROM content_item_topics s
			WHERE s.topic_id = ? AND t.topic_id = ? AND t.content_item_id = s.content_item_id`, sourceID, targetID).Error; err != nil {
			return err
		}
		if err := tx.Exec(`
			DELETE FROM content_item_topics s
			WHERE s.topic_id = ? AND EXISTS (SELECT 1 FROM content_item_topics t WHERE t.topic_id = ? AND t.content_item_id = s.content_item_id)`, sourceID, targetID).Error; err != nil {
			return err
		}
		if err := tx.Exec(`UPDATE content_item_topics SET topic_id = ? WHERE topic_id = ?`, targetID, sourceID).Error; err != nil {
			return err
		}

		// story_topics: same collision handling.
		if err := tx.Exec(`
			UPDATE story_topics t SET score = GREATEST(t.score, s.score)
			FROM story_topics s
			WHERE s.topic_id = ? AND t.topic_id = ? AND t.story_id = s.story_id`, sourceID, targetID).Error; err != nil {
			return err
		}
		if err := tx.Exec(`
			DELETE FROM story_topics s
			WHERE s.topic_id = ? AND EXISTS (SELECT 1 FROM story_topics t WHERE t.topic_id = ? AND t.story_id = s.story_id)`, sourceID, targetID).Error; err != nil {
			return err
		}
		if err := tx.Exec(`UPDATE story_topics SET topic_id = ? WHERE topic_id = ?`, targetID, sourceID).Error; err != nil {
			return err
		}

		// user_topic_prefs: muted wins over declared on collision.
		if err := tx.Exec(`
			UPDATE user_topic_prefs t SET state = 'muted', updated_at = now()
			FROM user_topic_prefs s
			WHERE s.tenant_id = ? AND s.topic_id = ? AND t.tenant_id = ? AND t.topic_id = ?
			  AND t.user_id = s.user_id AND (s.state = 'muted' OR t.state = 'muted')`, tenant, sourceID, tenant, targetID).Error; err != nil {
			return err
		}
		if err := tx.Exec(`
			DELETE FROM user_topic_prefs s
			WHERE s.tenant_id = ? AND s.topic_id = ?
			  AND EXISTS (SELECT 1 FROM user_topic_prefs t WHERE t.tenant_id = ? AND t.topic_id = ? AND t.user_id = s.user_id)`, tenant, sourceID, tenant, targetID).Error; err != nil {
			return err
		}
		if err := tx.Exec(`UPDATE user_topic_prefs SET topic_id = ?, updated_at = now() WHERE tenant_id = ? AND topic_id = ?`, targetID, tenant, sourceID).Error; err != nil {
			return err
		}

		// user_topic_affinity: best-continuity score, OR declared.
		if err := tx.Exec(`
			UPDATE user_topic_affinity t SET score = GREATEST(t.score, s.score), declared = (t.declared OR s.declared), updated_at = now()
			FROM user_topic_affinity s
			WHERE s.tenant_id = ? AND s.topic_id = ? AND t.tenant_id = ? AND t.topic_id = ? AND t.user_id = s.user_id`, tenant, sourceID, tenant, targetID).Error; err != nil {
			return err
		}
		if err := tx.Exec(`
			DELETE FROM user_topic_affinity s
			WHERE s.tenant_id = ? AND s.topic_id = ?
			  AND EXISTS (SELECT 1 FROM user_topic_affinity t WHERE t.tenant_id = ? AND t.topic_id = ? AND t.user_id = s.user_id)`, tenant, sourceID, tenant, targetID).Error; err != nil {
			return err
		}
		if err := tx.Exec(`UPDATE user_topic_affinity SET topic_id = ?, updated_at = now() WHERE tenant_id = ? AND topic_id = ?`, targetID, tenant, sourceID).Error; err != nil {
			return err
		}

		// Deactivate the source; mark both dirty so mappings settle authoritatively.
		if err := tx.Model(&models.Topic{}).Where("tenant_id = ? AND public_id = ?", tenant, sourceID).
			Updates(map[string]interface{}{"active": false, "needs_remap": true}).Error; err != nil {
			return err
		}
		if err := tx.Model(&models.Topic{}).Where("tenant_id = ? AND public_id = ?", tenant, targetID).
			UpdateColumn("needs_remap", true).Error; err != nil {
			return err
		}
		// Queue every affected user for authoritative recompute (§10).
		for _, uid := range affectedUsers {
			if err := enqueueAffinityRecompute(tx, tenant, uid, models.PreferenceRecomputeReasonMerge); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Merge failed: " + err.Error(), Code: "MERGE_FAILED"})
		return
	}
	_ = refreshTopicMemberCounts(db, tenant)
	writeCirculationAudit(db, principal, "topics.catalog.merge", tenant, map[string]interface{}{
		"source": sourceID.String(), "target": targetID.String(), "affected_users": len(affectedUsers),
	})
	c.JSON(http.StatusOK, gin.H{"message": "Topics merged", "data": gin.H{
		"source": source.Slug, "target": target.Slug, "affected_users": len(affectedUsers),
	}})
}

// ----------------------------------------------------------------
// One-click revert of an autopilot-approved topic (human-only)
// ----------------------------------------------------------------

// AdminRevertAutopilotTopic handles POST /admin/topics/catalog/:id/revert-autopilot.
// Only valid for created_from='autopilot' topics (the quarantined tier). Revert
// means "the machine's call is withdrawn; a human should decide": the topic is
// deactivated + marked dirty (the bounded sweep unwinds its mappings in every
// mode), and the matching autopilot-resolved proposal returns to PENDING with the
// resolver cleared — it re-enters the ranked queue rather than being buried as a
// fabricated human rejection. Trust evidence is untouched (autopilot resolutions
// are already excluded from trust).
func AdminRevertAutopilotTopic(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	tenant := principal.TenantID
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid topic id", Code: "INVALID_ID"})
		return
	}
	var topic models.Topic
	if err := db.Where("tenant_id = ? AND public_id = ?", tenant, id).First(&topic).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Topic not found", Code: "NOT_FOUND"})
		return
	}
	if topic.CreatedFrom != models.PreferenceCreatedFromAutopilot {
		c.JSON(http.StatusConflict, authErrorResponse{Message: "Only autopilot-created topics can be reverted", Code: "NOT_AUTOPILOT_TOPIC"})
		return
	}

	// Users who accrued affinity on the quarantined topic get an authoritative
	// recompute; a featured=false topic rarely has many, so this stays cheap.
	var affectedUsers []uuid.UUID
	if topic.MemberCount > 0 {
		db.Model(&models.UserTopicAffinity{}).
			Where("tenant_id = ? AND topic_id = ?", tenant, topic.PublicID).
			Distinct("user_id").Pluck("user_id", &affectedUsers)
	}

	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&models.Topic{}).Where("tenant_id = ? AND public_id = ?", tenant, topic.PublicID).
			Updates(map[string]interface{}{"active": false, "needs_remap": true}).Error; err != nil {
			return err
		}
		// Return the matching machine-approved proposal to the human queue. Slug
		// matching goes through the same canonicalization the approval used.
		var proposals []models.TopicProposal
		if err := tx.Where("tenant_id = ? AND status = ? AND resolved_by = ?", tenant, "approved", models.PreferenceAutopilotResolver).
			Find(&proposals).Error; err != nil {
			return err
		}
		for _, p := range proposals {
			slug, _, _ := canonicalTopicLabels(p.SuggestedSlug, p.SuggestedLabelAR, p.SuggestedLabelEN)
			if slug != topic.Slug {
				continue
			}
			if err := tx.Model(&models.TopicProposal{}).Where("id = ?", p.ID).Updates(map[string]interface{}{
				"status": "pending", "resolved_by": "", "resolved_at": nil,
			}).Error; err != nil {
				return err
			}
			break
		}
		for _, uid := range affectedUsers {
			if err := enqueueAffinityRecompute(tx, tenant, uid, models.PreferenceRecomputeReasonRevert); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Revert failed: " + err.Error(), Code: "REVERT_FAILED"})
		return
	}
	writeCirculationAudit(db, principal, "topics.catalog.revert_autopilot", tenant, map[string]interface{}{
		"topic": topic.PublicID.String(), "slug": topic.Slug, "affected_users": len(affectedUsers),
	})
	c.JSON(http.StatusOK, gin.H{"message": "Autopilot topic reverted", "data": gin.H{
		"slug": topic.Slug, "affected_users": len(affectedUsers),
	}})
}
