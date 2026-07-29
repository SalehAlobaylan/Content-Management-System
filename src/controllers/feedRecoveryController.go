package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const feedRecoveryPlanTTL = 30 * time.Minute

type feedRecoveryPlanRequest struct {
	Lane           string `json:"lane"`
	Level          string `json:"level"`
	CapacityMode   string `json:"capacity_mode"`
	NoFullRollback bool   `json:"no_full_rollback"`
}

func normalizedRecoveryField(value string, allowed ...string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	for _, candidate := range allowed {
		if value == candidate {
			return value
		}
	}
	return ""
}
func recoveryHash(value interface{}) string {
	raw, _ := json.Marshal(value)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func recoverySourceProof(db *gorm.DB, tenant, lane string) (string, int, error) {
	q := db.Model(&models.ContentSource{}).Where("tenant_id=? AND is_active=?", tenant, true)
	if lane == "news" {
		q = q.Where("category=?", "news")
	} else if lane == "media" {
		q = q.Where("category=?", "media")
	}
	var sources []models.ContentSource
	if err := q.Select("public_id, updated_at, category, type").Find(&sources).Error; err != nil {
		return "", 0, err
	}
	parts := make([]string, 0, len(sources))
	for _, source := range sources {
		parts = append(parts, source.PublicID.String()+"|"+source.Category+"|"+string(source.Type)+"|"+source.UpdatedAt.UTC().Format(time.RFC3339Nano))
	}
	sort.Strings(parts)
	return recoveryHash(parts), len(parts), nil
}

func buildRecoveryPlan(db *gorm.DB, tenant string, req feedRecoveryPlanRequest, actor string) (models.FeedRecoveryPlan, error) {
	lane := normalizedRecoveryField(req.Lane, "news", "media", "both")
	level := normalizedRecoveryField(req.Level, "repair", "rotate", "purge_reseed")
	mode := normalizedRecoveryField(req.CapacityMode, "safe_cutover", "low_space_reset")
	if lane == "" || level == "" || mode == "" {
		return models.FeedRecoveryPlan{}, gorm.ErrInvalidData
	}
	if level != "purge_reseed" && req.NoFullRollback {
		return models.FeedRecoveryPlan{}, gorm.ErrInvalidData
	}
	checksum, count, err := recoverySourceProof(db, tenant, lane)
	if err != nil {
		return models.FeedRecoveryPlan{}, err
	}
	evidence := map[string]interface{}{"source_checksum": checksum, "source_count": count, "execution_installed": true, "enabled_levels": []string{"repair", "rotate"}, "disabled_levels": []string{"purge_reseed"}, "safe_note": "Repair and single-lane Safe Cutover change only derived feed state; sources and canonical content are never mutated."}
	manifest := map[string]interface{}{"tenant": tenant, "lane": lane, "level": level, "capacity_mode": mode, "source_checksum": checksum, "source_count": count}
	manifestHash := recoveryHash(manifest)
	planHash := recoveryHash(map[string]interface{}{"manifest": manifest, "no_full_rollback": req.NoFullRollback})
	evidenceJSON, _ := json.Marshal(evidence)
	policyJSON, _ := json.Marshal(map[string]interface{}{"execution": "repair_and_single_lane_rotate", "purge_reseed": "disabled_until_slice_9", "both_rotate": "disabled_until_slice_9"})
	return models.FeedRecoveryPlan{TenantID: tenant, Lane: lane, Level: level, CapacityMode: mode, State: "awaiting_approval", PlanHash: planHash, ManifestHash: manifestHash, SourceChecksum: checksum, SourceCount: count, Evidence: datatypes.JSON(evidenceJSON), PolicySnapshot: datatypes.JSON(policyJSON), NoFullRollback: req.NoFullRollback, ExpiresAt: time.Now().UTC().Add(feedRecoveryPlanTTL), CreatedBy: actor}, nil
}

func ensureFeedGenerationFoundation(db *gorm.DB, tenant, requestedLane string) error {
	lanes := []string{requestedLane}
	if requestedLane == "both" {
		lanes = []string{"news", "media"}
	}
	for _, lane := range lanes {
		if err := db.Transaction(func(tx *gorm.DB) error {
			var head models.FeedGenerationHead
			if err := tx.Where("tenant_id=? AND lane=?", tenant, lane).First(&head).Error; err == nil {
				return nil
			} else if err != gorm.ErrRecordNotFound {
				return err
			}
			generation := models.FeedGeneration{TenantID: tenant, Lane: lane, State: "active", BuildWatermark: time.Now().UTC(), Verification: datatypes.JSON([]byte(`{"foundation":true}`))}
			if err := tx.Create(&generation).Error; err != nil {
				return err
			}
			head = models.FeedGenerationHead{TenantID: tenant, Lane: lane, ActiveGenerationID: &generation.PublicID, Generation: 1}
			if err := tx.Create(&head).Error; err != nil {
				return err
			}
			if lane == "news" {
				return tx.Exec("INSERT INTO feed_generation_memberships (generation_id, member_type, member_id) SELECT ?, 'story', story_id FROM content_items WHERE tenant_id=? AND type='NEWS' AND status='READY' AND story_id IS NOT NULL GROUP BY story_id ON CONFLICT DO NOTHING", generation.PublicID, tenant).Error
			}
			return tx.Exec("INSERT INTO feed_generation_memberships (generation_id, member_type, member_id) SELECT ?, 'feed_unit', public_id FROM content_items WHERE tenant_id=? AND type IN ('VIDEO','PODCAST') AND status='READY' AND is_feed_unit=TRUE AND feed_visibility='visible' ON CONFLICT DO NOTHING", generation.PublicID, tenant).Error
		}); err != nil {
			return err
		}
	}
	return nil
}

func CreateFeedRecoveryPlan(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	var req feedRecoveryPlanRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid recovery plan"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	plan, err := buildRecoveryPlan(db, principal.TenantID, req, principal.Email)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "lane, level, capacity_mode, and rollback acknowledgement are invalid"})
		return
	}
	if err := ensureFeedGenerationFoundation(db, principal.TenantID, plan.Lane); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to initialize feed generation boundary"})
		return
	}
	if err := db.Create(&plan).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create recovery plan"})
		return
	}
	c.JSON(http.StatusCreated, plan)
}

func GetFeedRecoveryPlan(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "plan not found"})
		return
	}
	var plan models.FeedRecoveryPlan
	if err := c.MustGet("db").(*gorm.DB).Where("tenant_id=? AND public_id=?", principal.TenantID, id).First(&plan).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "plan not found"})
		return
	}
	c.JSON(http.StatusOK, plan)
}

func RefreshFeedRecoveryPlan(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "plan not found"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var old models.FeedRecoveryPlan
	if err := db.Where("tenant_id=? AND public_id=?", principal.TenantID, id).First(&old).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "plan not found"})
		return
	}
	plan, err := buildRecoveryPlan(db, principal.TenantID, feedRecoveryPlanRequest{Lane: old.Lane, Level: old.Level, CapacityMode: old.CapacityMode, NoFullRollback: old.NoFullRollback}, principal.Email)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "unable to refresh preflight"})
		return
	}
	if err := db.Create(&plan).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to refresh preflight"})
		return
	}
	c.JSON(http.StatusCreated, plan)
}

func ApproveFeedRecoveryPlan(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok || !principal.HasRole("admin") {
		c.JSON(http.StatusForbidden, gin.H{"error": "admin role required"})
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "plan not found"})
		return
	}
	var req struct {
		Phrase      string `json:"phrase"`
		ReauthProof string `json:"reauth_proof"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "approval details required"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var plan models.FeedRecoveryPlan
	if err := db.Where("tenant_id=? AND public_id=?", principal.TenantID, id).First(&plan).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "plan not found"})
		return
	}
	if !plan.ExpiresAt.After(time.Now().UTC()) || plan.State != "awaiting_approval" {
		c.JSON(http.StatusConflict, gin.H{"error": "plan is no longer approvable"})
		return
	}
	secret, err := utils.GetJWTSecret()
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "re-auth unavailable"})
		return
	}
	proof, err := utils.ParseFeedRecoveryReauthProof(strings.TrimSpace(req.ReauthProof), secret)
	if err != nil || proof.UserID != principal.UserID || proof.TenantID != principal.TenantID || proof.PlanID != plan.PublicID.String() || proof.ManifestHash != plan.ManifestHash {
		c.JSON(http.StatusForbidden, gin.H{"error": "fresh re-auth proof does not match this plan"})
		return
	}
	expectedPhrase := "APPROVE FEED RECOVERY " + strings.ToUpper(plan.ManifestHash[:12])
	if strings.TrimSpace(req.Phrase) != expectedPhrase {
		c.JSON(http.StatusBadRequest, gin.H{"error": "confirmation phrase does not match plan"})
		return
	}
	now := time.Now().UTC()
	notBefore := now.Add(time.Minute)
	phraseHash := recoveryHash(req.Phrase)
	approval := models.FeedRecoveryApproval{PlanID: plan.ID, TenantID: plan.TenantID, Actor: principal.Email, PlanHash: plan.PlanHash, ManifestHash: plan.ManifestHash, TargetCount: plan.TargetCount, PhraseProofHash: phraseHash, ReauthJTI: proof.ID, NoFullRollback: plan.NoFullRollback, ApprovedAt: now, ConsumedAt: &now}
	run := models.FeedRecoveryRun{PlanID: plan.ID, TenantID: plan.TenantID, Lane: plan.Lane, CorrelationID: uuid.New(), Phase: "cancel_window", NotBefore: &notBefore, CancelDeadline: &notBefore}
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&approval).Error; err != nil {
			return err
		}
		if err := tx.Create(&run).Error; err != nil {
			return err
		}
		if err := tx.Create(&models.FeedRecoveryAction{RunID: run.ID, ActionType: "approval", State: "succeeded", IdempotencyKey: "approval:" + plan.PlanHash, Evidence: datatypes.JSON([]byte(`{"fresh_reauth":true,"cancel_window_seconds":60}`))}).Error; err != nil {
			return err
		}
		return tx.Model(&plan).Update("state", "approved").Error
	})
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "approval proof was already consumed or approval could not be saved"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"plan": plan.PublicID, "run": run, "cancel_deadline": notBefore})
}

func ListFeedRecoveryRuns(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	var runs []models.FeedRecoveryRun
	if err := c.MustGet("db").(*gorm.DB).Where("tenant_id=?", principal.TenantID).Order("created_at DESC").Limit(100).Find(&runs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list runs"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": runs})
}

func GetFeedRecoveryRun(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "run not found"})
		return
	}
	var run models.FeedRecoveryRun
	if err := c.MustGet("db").(*gorm.DB).Where("tenant_id=? AND public_id=?", principal.TenantID, id).First(&run).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "run not found"})
		return
	}
	c.JSON(http.StatusOK, run)
}

func ListFeedRecoveryActions(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "run not found"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var run models.FeedRecoveryRun
	if err := db.Where("tenant_id=? AND public_id=?", principal.TenantID, id).First(&run).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "run not found"})
		return
	}
	var actions []models.FeedRecoveryAction
	if err := db.Where("run_id=?", run.ID).Order("created_at ASC").Find(&actions).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list actions"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": actions})
}

const recoveryClaimTTL = 2 * time.Minute
const recoveryRollbackTTL = 24 * time.Hour

// Feed Recovery only invokes tools registered here. Each tool is bounded to a
// derived feed/cache artifact; none is allowed to delete canonical content or
// mutate source checkpoints.
type feedRecoveryRepairTool struct {
	Name        string
	Description string
}

var registeredFeedRecoveryRepairTools = []feedRecoveryRepairTool{
	{Name: "feed_generation_membership_reconcile", Description: "attach current READY feed members to the active generation"},
	{Name: "news_snapshot_rebuild", Description: "invalidate and rebuild today/week/month News snapshots"},
}

func recoveryLanes(lane string) []string {
	if lane == "both" {
		return []string{"news", "media"}
	}
	return []string{lane}
}

func setRecoveryAvailabilityForRun(db *gorm.DB, run models.FeedRecoveryRun, state string) {
	for _, lane := range recoveryLanes(run.Lane) {
		var runID *uint
		if state != "normal" {
			runID = &run.ID
		}
		setRecoveryAvailability(db, run.TenantID, lane, state, runID)
	}
}

func reconcileRecoveryGenerationMemberships(db *gorm.DB, tenant, lane string) error {
	if err := ensureFeedGenerationFoundation(db, tenant, lane); err != nil {
		return err
	}
	var head models.FeedGenerationHead
	if err := db.Where("tenant_id=? AND lane=?", tenant, lane).First(&head).Error; err != nil {
		return err
	}
	if head.ActiveGenerationID == nil {
		return fmt.Errorf("active generation is missing")
	}
	if lane == "news" {
		return db.Exec("INSERT INTO feed_generation_memberships (generation_id, member_type, member_id) SELECT ?, 'story', story_id FROM content_items WHERE tenant_id=? AND type='NEWS' AND status='READY' AND story_id IS NOT NULL GROUP BY story_id ON CONFLICT DO NOTHING", *head.ActiveGenerationID, tenant).Error
	}
	return db.Exec("INSERT INTO feed_generation_memberships (generation_id, member_type, member_id) SELECT ?, 'feed_unit', public_id FROM content_items WHERE tenant_id=? AND type IN ('VIDEO','PODCAST') AND status='READY' AND is_feed_unit=TRUE AND feed_visibility='visible' ON CONFLICT DO NOTHING", *head.ActiveGenerationID, tenant).Error
}

func runRegisteredRecoveryRepairTools(db *gorm.DB, run models.FeedRecoveryRun) error {
	if len(registeredFeedRecoveryRepairTools) == 0 {
		return fmt.Errorf("no recovery repair tools are registered")
	}
	for _, lane := range recoveryLanes(run.Lane) {
		if err := reconcileRecoveryGenerationMemberships(db, run.TenantID, lane); err != nil {
			return fmt.Errorf("%s: %w", registeredFeedRecoveryRepairTools[0].Name, err)
		}
		_ = db.Create(&models.FeedRecoveryAction{RunID: run.ID, ActionType: registeredFeedRecoveryRepairTools[0].Name, State: "succeeded", IdempotencyKey: "repair-memberships:" + lane, Evidence: datatypes.JSON([]byte(fmt.Sprintf(`{"lane":"%s","non_destructive":true}`, lane)))})
	}
	if run.Lane == "news" || run.Lane == "both" {
		markNewsSnapshotDirty(db, run.TenantID)
		for _, window := range []string{"today", "week", "month"} {
			if _, err := buildNewsSnapshot(db, run.TenantID, window); err != nil {
				return fmt.Errorf("%s (%s): %w", registeredFeedRecoveryRepairTools[1].Name, window, err)
			}
		}
		_ = db.Create(&models.FeedRecoveryAction{RunID: run.ID, ActionType: registeredFeedRecoveryRepairTools[1].Name, State: "succeeded", IdempotencyKey: "repair-snapshots:" + run.PublicID.String(), Evidence: datatypes.JSON([]byte(`{"non_destructive":true,"windows":["today","week","month"]}`))})
	}
	return nil
}

func setRecoveryAvailability(db *gorm.DB, tenant, lane, state string, runID *uint) {
	message := "feed_ready"
	var retry *int
	if state == "refreshing" {
		message = "feed_refreshing"
		value := 30
		retry = &value
	}
	if state == "partial" {
		message = "feed_partial"
		value := 60
		retry = &value
	}
	_ = db.Where("tenant_id=? AND lane=?", tenant, lane).Assign(models.FeedAvailabilityState{State: state, RecoveryRunID: runID, MessageKey: message, RetryAfterSeconds: retry, UpdatedAt: time.Now().UTC()}).FirstOrCreate(&models.FeedAvailabilityState{TenantID: tenant, Lane: lane})
	_ = db.Model(&models.FeedAvailabilityState{}).Where("tenant_id=? AND lane=?", tenant, lane).Updates(map[string]interface{}{"state": state, "recovery_run_id": runID, "message_key": message, "retry_after_seconds": retry, "updated_at": time.Now().UTC()}).Error
}

func claimRecoveryRun(db *gorm.DB, tenant string, publicID uuid.UUID) (models.FeedRecoveryRun, error) {
	var run models.FeedRecoveryRun
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", tenant, publicID).First(&run).Error; err != nil {
			return err
		}
		now := time.Now().UTC()
		if run.Phase == "cancel_window" {
			if run.NotBefore != nil && run.NotBefore.After(now) {
				return fmt.Errorf("cancel window remains open until %s", run.NotBefore.Format(time.RFC3339))
			}
		} else if run.Phase == "verification_wait" {
			if run.VerificationDueAt != nil && run.VerificationDueAt.After(now) {
				return fmt.Errorf("second verification is not due until %s", run.VerificationDueAt.Format(time.RFC3339))
			}
		} else if run.Phase == "partial" || run.Phase == "failed" {
			// A failed/partial run is resumable only through the same persisted
			// phase machine; it never reopens approval or widens its plan.
			run.Phase = "executing"
		} else if run.Phase != "executing" && run.Phase != "reseeding" {
			return fmt.Errorf("run is not resumable from phase %s", run.Phase)
		}
		if run.ClaimExpiresAt != nil && run.ClaimExpiresAt.After(now) {
			return fmt.Errorf("recovery run is already claimed")
		}
		token := uuid.New()
		expiry := now.Add(recoveryClaimTTL)
		updates := map[string]interface{}{"claim_token": token, "claim_expires_at": expiry, "heartbeat_at": now, "lane_lease": run.Lane, "updated_at": now}
		if run.Phase == "cancel_window" {
			updates["phase"] = "executing"
			run.Phase = "executing"
		}
		if err := tx.Model(&run).Updates(updates).Error; err != nil {
			return err
		}
		run.ClaimToken, run.ClaimExpiresAt, run.HeartbeatAt, run.LaneLease = &token, &expiry, &now, run.Lane
		return nil
	})
	return run, err
}

func buildRecoveryCandidate(db *gorm.DB, run models.FeedRecoveryRun) error {
	return db.Transaction(func(tx *gorm.DB) error {
		var head models.FeedGenerationHead
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND lane=?", run.TenantID, run.Lane).First(&head).Error; err != nil {
			return err
		}
		if head.ActiveGenerationID == nil {
			return fmt.Errorf("active generation is missing")
		}
		if head.CandidateGenerationID != nil {
			run.CandidateGenerationID = head.CandidateGenerationID
			run.ActiveGenerationID = head.ActiveGenerationID
			return nil
		}
		generation := models.FeedGeneration{TenantID: run.TenantID, Lane: run.Lane, State: "candidate", PreviousGenerationID: head.ActiveGenerationID, BuildWatermark: time.Now().UTC(), Verification: datatypes.JSON([]byte(`{"source":"feed_recovery","complete":false}`))}
		if err := tx.Create(&generation).Error; err != nil {
			return err
		}
		if err := tx.Exec("INSERT INTO feed_generation_memberships (generation_id, member_type, member_id) SELECT ?, member_type, member_id FROM feed_generation_memberships WHERE generation_id=? ON CONFLICT DO NOTHING", generation.PublicID, *head.ActiveGenerationID).Error; err != nil {
			return err
		}
		if err := tx.Model(&head).Updates(map[string]interface{}{"candidate_generation_id": generation.PublicID, "updated_at": time.Now().UTC()}).Error; err != nil {
			return err
		}
		run.ActiveGenerationID, run.CandidateGenerationID = head.ActiveGenerationID, &generation.PublicID
		return tx.Model(&models.FeedRecoveryRun{}).Where("id=?", run.ID).Updates(map[string]interface{}{"active_generation_id": head.ActiveGenerationID, "candidate_generation_id": generation.PublicID, "phase": "reseeding", "heartbeat_at": time.Now().UTC(), "updated_at": time.Now().UTC()}).Error
	})
}

func markRecoveryCandidateCaughtUp(db *gorm.DB, run models.FeedRecoveryRun) error {
	if run.CandidateGenerationID == nil {
		return fmt.Errorf("candidate generation is missing")
	}
	now := time.Now().UTC()
	var generation models.FeedGeneration
	if err := db.Where("public_id=? AND tenant_id=? AND lane=?", *run.CandidateGenerationID, run.TenantID, run.Lane).First(&generation).Error; err != nil {
		return err
	}
	// Reconcile anything that became READY after the candidate was seeded. The
	// normal ingest hook dual-writes this path, but the bounded catch-up makes a
	// crashed worker/retry safe and records the later watermark explicitly.
	if run.Lane == "news" {
		if err := db.Exec("INSERT INTO feed_generation_memberships (generation_id, member_type, member_id) SELECT ?, 'story', story_id FROM content_items WHERE tenant_id=? AND type='NEWS' AND status='READY' AND story_id IS NOT NULL AND updated_at >= ? GROUP BY story_id ON CONFLICT DO NOTHING", *run.CandidateGenerationID, run.TenantID, generation.BuildWatermark).Error; err != nil {
			return err
		}
	} else {
		if err := db.Exec("INSERT INTO feed_generation_memberships (generation_id, member_type, member_id) SELECT ?, 'feed_unit', public_id FROM content_items WHERE tenant_id=? AND type IN ('VIDEO','PODCAST') AND status='READY' AND is_feed_unit=TRUE AND feed_visibility='visible' AND updated_at >= ? ON CONFLICT DO NOTHING", *run.CandidateGenerationID, run.TenantID, generation.BuildWatermark).Error; err != nil {
			return err
		}
	}
	return db.Model(&generation).Updates(map[string]interface{}{"state": "candidate", "caught_up_at": now, "build_watermark": now, "verification": datatypes.JSON([]byte(`{"caught_up":true,"dual_write_reconciled":true}`)), "updated_at": now}).Error
}

func runRecoveryVerification(db *gorm.DB, run models.FeedRecoveryRun, pass int) bool {
	fi, fiErr := runFeedIntegrity(db, run.TenantID, feedIntegrityRunOptions{Trigger: "feed_recovery", CreatedBy: "feed_recovery", Tier: models.FeedIntegrityTierDeep, CorrelationID: &run.CorrelationID, TriggerRef: run.PublicID.String()})
	sh, shActions, shErr := runSystemHealthAutopilot(db, systemAutopilotRunOptions{Trigger: "feed_recovery", CreatedBy: "feed_recovery", CorrelationID: &run.CorrelationID, TriggerRef: run.PublicID.String()})
	clean := fiErr == nil && shErr == nil && fi.Status == models.FeedIntegrityRunCompleted && fi.Headline == "all_clear" && sh.Status == models.SystemAutopilotRunStatusCompleted
	state := "failed"
	if clean {
		state = "succeeded"
	}
	var attempts int64
	_ = db.Model(&models.FeedRecoveryAction{}).Where("run_id=? AND action_type=?", run.ID, fmt.Sprintf("verification_probe_%d", pass)).Count(&attempts)
	_ = db.Create(&models.FeedRecoveryAction{RunID: run.ID, ActionType: fmt.Sprintf("verification_probe_%d", pass), State: state, IdempotencyKey: fmt.Sprintf("verify:%d:%d", pass, attempts+1), Evidence: datatypes.JSON([]byte(fmt.Sprintf(`{"feed_integrity_run":"%s","feed_integrity_headline":"%s","system_health_run":"%s","system_health_actions":%d,"feed_integrity_error":%t,"system_health_error":%t}`, fi.PublicID.String(), fi.Headline, sh.PublicID.String(), len(shActions), fiErr != nil, shErr != nil)))})
	return clean
}

func ExecuteFeedRecoveryRun(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "run not found"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	run, err := claimRecoveryRun(db, principal.TenantID, id)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	var plan models.FeedRecoveryPlan
	if err := db.First(&plan, run.PlanID).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "recovery plan not found"})
		return
	}
	if plan.Level == "purge_reseed" {
		_ = db.Model(&run).Updates(map[string]interface{}{"phase": "blocked", "outcome": "purge_reseed_not_installed", "claim_token": nil, "claim_expires_at": nil, "updated_at": time.Now().UTC()})
		c.JSON(http.StatusConflict, gin.H{"error": "Purge & Reseed is not installed in Slice 8"})
		return
	}
	// Slice 8 owns single-lane Safe Cutover. Low-Space/typed Both sequencing
	// is deliberately reserved for Slice 9; reject it explicitly instead of
	// ever writing an invalid `both` availability or generation head.
	if plan.Level == "rotate" && run.Lane == "both" {
		_ = db.Model(&run).Updates(map[string]interface{}{"phase": "blocked", "outcome": "both_requires_sequential_lane_execution", "claim_token": nil, "claim_expires_at": nil, "updated_at": time.Now().UTC()})
		c.JSON(http.StatusConflict, gin.H{"error": "Both-lane rotate is blocked until sequential lane execution is enabled"})
		return
	}
	setRecoveryAvailabilityForRun(db, run, "refreshing")
	if plan.Level == "repair" {
		if repairErr := runRegisteredRecoveryRepairTools(db, run); repairErr != nil {
			_ = db.Model(&run).Updates(map[string]interface{}{"phase": "partial", "outcome": "repair_failed", "error": repairErr.Error(), "claim_token": nil, "claim_expires_at": nil, "lane_lease": ""})
			setRecoveryAvailabilityForRun(db, run, "partial")
			c.JSON(http.StatusConflict, gin.H{"error": "registered recovery repair failed"})
			return
		}
	}
	if plan.Level == "rotate" {
		if run.Phase == "executing" && run.CandidateGenerationID == nil {
			if err := buildRecoveryCandidate(db, run); err != nil {
				_ = db.Model(&run).Updates(map[string]interface{}{"phase": "failed", "outcome": "failed", "error": err.Error()})
				setRecoveryAvailabilityForRun(db, run, "partial")
				c.JSON(http.StatusConflict, gin.H{"error": "candidate build failed"})
				return
			}
			// buildRecoveryCandidate persists the generation pointers in the
			// transaction. Reload before catch-up so retries and concurrent
			// workers use the durable state, not the pre-claim copy.
			if err := db.First(&run, run.ID).Error; err != nil {
				_ = db.Model(&run).Updates(map[string]interface{}{"phase": "failed", "outcome": "failed", "error": err.Error()})
				setRecoveryAvailabilityForRun(db, run, "partial")
				c.JSON(http.StatusConflict, gin.H{"error": "candidate state could not be reloaded"})
				return
			}
		}
		if err := markRecoveryCandidateCaughtUp(db, run); err != nil {
			_ = db.Model(&run).Updates(map[string]interface{}{"phase": "failed", "outcome": "failed", "error": err.Error(), "claim_token": nil, "claim_expires_at": nil})
			setRecoveryAvailabilityForRun(db, run, "partial")
			c.JSON(http.StatusConflict, gin.H{"error": "candidate catch-up failed"})
			return
		}
	}
	pass := 1
	if run.Phase == "verification_wait" {
		pass = 2
	}
	if run.Phase != "verification_wait" {
		_ = db.Model(&run).Updates(map[string]interface{}{"phase": "verifying_probe_1", "heartbeat_at": time.Now().UTC()})
	} else {
		_ = db.Model(&run).Updates(map[string]interface{}{"phase": "verifying_probe_2", "heartbeat_at": time.Now().UTC()})
	}
	clean := runRecoveryVerification(db, run, pass)
	if !clean {
		_ = db.Model(&run).Updates(map[string]interface{}{"phase": "partial", "outcome": "verification_failed", "error": "verification gate did not pass", "claim_token": nil, "claim_expires_at": nil, "lane_lease": "", "updated_at": time.Now().UTC()})
		setRecoveryAvailabilityForRun(db, run, "partial")
		c.JSON(http.StatusConflict, gin.H{"error": "verification gate did not pass; no cutover occurred"})
		return
	}
	if pass == 1 {
		due := time.Now().UTC().Add(5 * time.Minute)
		_ = db.Model(&run).Updates(map[string]interface{}{"phase": "verification_wait", "verification_due_at": due, "claim_token": nil, "claim_expires_at": nil, "lane_lease": "", "heartbeat_at": time.Now().UTC(), "updated_at": time.Now().UTC()})
		c.JSON(http.StatusAccepted, gin.H{"data": gin.H{"run": run.PublicID, "phase": "verification_wait", "verification_due_at": due}})
		return
	}
	if plan.Level == "rotate" {
		if err := cutoverRecoveryCandidate(db, run); err != nil {
			_ = db.Model(&run).Updates(map[string]interface{}{"phase": "partial", "outcome": "cutover_failed", "error": err.Error()})
			setRecoveryAvailabilityForRun(db, run, "partial")
			c.JSON(http.StatusConflict, gin.H{"error": "safe cutover failed"})
			return
		}
	}
	terminalPhase := "succeeded"
	if plan.Level == "rotate" {
		terminalPhase = "rollback_ready"
	}
	_ = db.Model(&run).Updates(map[string]interface{}{"phase": terminalPhase, "outcome": "succeeded", "error": nil, "claim_token": nil, "claim_expires_at": nil, "lane_lease": "", "verification_due_at": nil, "updated_at": time.Now().UTC()})
	setRecoveryAvailabilityForRun(db, run, "normal")
	data := gin.H{"run": run.PublicID, "phase": terminalPhase}
	if plan.Level == "rotate" {
		data["rollback_deadline"] = time.Now().UTC().Add(recoveryRollbackTTL)
	}
	c.JSON(http.StatusOK, gin.H{"data": data})
}

func CancelFeedRecoveryRun(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "run not found"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var run models.FeedRecoveryRun
	if err := db.Where("tenant_id=? AND public_id=?", principal.TenantID, id).First(&run).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "run not found"})
		return
	}
	if run.Phase != "cancel_window" && run.Phase != "verification_wait" {
		c.JSON(http.StatusConflict, gin.H{"error": "run is no longer cancellable"})
		return
	}
	now := time.Now().UTC()
	_ = db.Model(&run).Updates(map[string]interface{}{"phase": "cancelled", "outcome": "cancelled", "claim_token": nil, "claim_expires_at": nil, "lane_lease": "", "updated_at": now}).Error
	setRecoveryAvailabilityForRun(db, run, "normal")
	_ = db.Create(&models.FeedRecoveryAction{RunID: run.ID, ActionType: "cancel", State: "succeeded", IdempotencyKey: "cancel:" + run.PublicID.String(), Evidence: datatypes.JSON([]byte(`{"cancelled":true}`))})
	c.JSON(http.StatusOK, gin.H{"data": run.PublicID})
}

func RollbackFeedRecoveryRun(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "run not found"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var run models.FeedRecoveryRun
	if err := db.Where("tenant_id=? AND public_id=?", principal.TenantID, id).First(&run).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "run not found"})
		return
	}
	if run.Outcome != "succeeded" || run.ActiveGenerationID == nil || run.CandidateGenerationID == nil || run.RollbackDeadline == nil || !run.RollbackDeadline.After(time.Now().UTC()) {
		c.JSON(http.StatusConflict, gin.H{"error": "rollback window is closed or no cutover exists"})
		return
	}
	now := time.Now().UTC()
	err = db.Transaction(func(tx *gorm.DB) error {
		var head models.FeedGenerationHead
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND lane=?", run.TenantID, run.Lane).First(&head).Error; err != nil {
			return err
		}
		if head.ActiveGenerationID == nil || *head.ActiveGenerationID != *run.CandidateGenerationID {
			return fmt.Errorf("active generation no longer matches run")
		}
		if err := tx.Model(&models.FeedGeneration{}).Where("public_id=?", *run.CandidateGenerationID).Updates(map[string]interface{}{"state": "retired", "updated_at": now}).Error; err != nil {
			return err
		}
		if err := tx.Model(&models.FeedGeneration{}).Where("public_id=?", *run.ActiveGenerationID).Updates(map[string]interface{}{"state": "active", "rollback_deadline": nil, "updated_at": now}).Error; err != nil {
			return err
		}
		if err := tx.Model(&head).Updates(map[string]interface{}{"active_generation_id": *run.ActiveGenerationID, "generation": gorm.Expr("generation + 1"), "updated_at": now}).Error; err != nil {
			return err
		}
		return tx.Model(&run).Updates(map[string]interface{}{"phase": "rolled_back", "outcome": "rolled_back", "updated_at": now}).Error
	})
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	setRecoveryAvailabilityForRun(db, run, "normal")
	_ = db.Create(&models.FeedRecoveryAction{RunID: run.ID, ActionType: "rollback", State: "succeeded", IdempotencyKey: "rollback:" + run.PublicID.String(), Evidence: datatypes.JSON([]byte(`{"within_24h":true}`))})
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"run": run.PublicID, "phase": "rolled_back"}})
}

func cutoverRecoveryCandidate(db *gorm.DB, run models.FeedRecoveryRun) error {
	if run.ActiveGenerationID == nil || run.CandidateGenerationID == nil {
		return fmt.Errorf("generation pointers are incomplete")
	}
	now := time.Now().UTC()
	deadline := now.Add(recoveryRollbackTTL)
	return db.Transaction(func(tx *gorm.DB) error {
		var head models.FeedGenerationHead
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND lane=?", run.TenantID, run.Lane).First(&head).Error; err != nil {
			return err
		}
		if head.CandidateGenerationID == nil || *head.CandidateGenerationID != *run.CandidateGenerationID {
			return fmt.Errorf("candidate no longer matches this run")
		}
		if err := tx.Model(&models.FeedGeneration{}).Where("public_id=?", *run.ActiveGenerationID).Updates(map[string]interface{}{"state": "rollback", "rollback_deadline": deadline, "updated_at": now}).Error; err != nil {
			return err
		}
		if err := tx.Model(&models.FeedGeneration{}).Where("public_id=?", *run.CandidateGenerationID).Updates(map[string]interface{}{"state": "active", "cutover_at": now, "updated_at": now}).Error; err != nil {
			return err
		}
		if err := tx.Model(&head).Updates(map[string]interface{}{"active_generation_id": *run.CandidateGenerationID, "candidate_generation_id": nil, "generation": gorm.Expr("generation + 1"), "updated_at": now}).Error; err != nil {
			return err
		}
		return tx.Model(&models.FeedRecoveryRun{}).Where("id=?", run.ID).Updates(map[string]interface{}{"rollback_deadline": deadline, "phase": "rollback_ready", "outcome": "succeeded", "verification_due_at": nil, "updated_at": now}).Error
	})
}

// feedAvailabilityMeta is intentionally safe for public clients: it contains
// neither quota nor deletion details.
type feedAvailabilityMeta struct {
	Availability      string `json:"availability"`
	MessageKey        string `json:"message_key,omitempty"`
	RetryAfterSeconds *int   `json:"retry_after_seconds,omitempty"`
}

func currentFeedAvailability(db *gorm.DB, tenant, lane string) *feedAvailabilityMeta {
	var state models.FeedAvailabilityState
	if err := db.Where("tenant_id=? AND lane=?", tenant, lane).First(&state).Error; err != nil || state.State == "normal" {
		return nil
	}
	return &feedAvailabilityMeta{Availability: state.State, MessageKey: state.MessageKey, RetryAfterSeconds: state.RetryAfterSeconds}
}

// applyActiveGenerationMembership makes a generation head a real serving
// boundary. During the migration window (before the recovery tables exist) it
// is a no-op so older installations keep their existing live query behavior.
func applyActiveGenerationMembership(db *gorm.DB, query *gorm.DB, tenant, lane, memberType, memberColumn string) *gorm.DB {
	if !db.Migrator().HasTable(&models.FeedGenerationHead{}) || !db.Migrator().HasTable(&models.FeedGenerationMembership{}) {
		return query
	}
	var head models.FeedGenerationHead
	if err := db.Where("tenant_id=? AND lane=?", tenant, lane).First(&head).Error; err != nil || head.ActiveGenerationID == nil {
		return query
	}
	return query.Where(`EXISTS (
        SELECT 1
        FROM feed_generation_heads generation_head
        JOIN feed_generation_memberships generation_membership
          ON generation_membership.generation_id = generation_head.active_generation_id
        WHERE generation_head.tenant_id = ?
          AND generation_head.lane = ?
          AND generation_membership.member_type = ?
          AND generation_membership.member_id = `+memberColumn+`
    )`, tenant, lane, memberType)
}

// attachReadyContentToFeedGenerations is the single dual-write primitive for
// future Safe Cutover. Normal serving remains live; this only records which
// active/candidate namespaces have seen a newly eligible item.
func attachReadyContentToFeedGenerations(db *gorm.DB, item models.ContentItem) {
	if item.Status != models.ContentStatusReady {
		return
	}
	lane, memberType, memberID := "media", "feed_unit", item.PublicID
	if item.Type == models.ContentTypeNews {
		if item.StoryID == nil {
			return
		}
		lane, memberType, memberID = "news", "story", *item.StoryID
	} else if !item.IsFeedUnit || item.FeedVisibility != "visible" {
		return
	}
	var head models.FeedGenerationHead
	if err := db.Where("tenant_id=? AND lane=?", item.TenantID, lane).First(&head).Error; err != nil {
		return
	}
	for _, generationID := range []*uuid.UUID{head.ActiveGenerationID, head.CandidateGenerationID} {
		if generationID == nil {
			continue
		}
		_ = db.Exec("INSERT INTO feed_generation_memberships (generation_id, member_type, member_id) VALUES (?, ?, ?) ON CONFLICT DO NOTHING", *generationID, memberType, memberID).Error
	}
}
