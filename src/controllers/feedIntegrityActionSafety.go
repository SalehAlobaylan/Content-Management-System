package controllers

import (
	"content-management-system/src/models"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"strings"
	"time"
)

func admitIntegrityAction(db *gorm.DB, tenant string, id uint, token string, preEffect bool) (models.FeedIntegrityAction, error) {
	var result models.FeedIntegrityAction
	err := db.Transaction(func(tx *gorm.DB) error {
		var hint models.FeedIntegrityAction
		if err := tx.Where("tenant_id=? AND id=?", tenant, id).First(&hint).Error; err != nil {
			return err
		}
		var policy models.FeedIntegrityPolicy
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=?", tenant).First(&policy).Error; err != nil {
			return err
		}
		var episode models.FeedIntegrityEpisode
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND id=?", tenant, hint.EpisodeID).First(&episode).Error; err != nil {
			return err
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND id=?", tenant, id).First(&result).Error; err != nil {
			return err
		}
		now := time.Now().UTC()
		sanitizeFeedIntegrityAutopilotPolicy(&policy)
		if !policy.AutopilotEnabled || (policy.AutopilotPausedUntil != nil && policy.AutopilotPausedUntil.After(now)) {
			return fmt.Errorf("actions disabled or paused")
		}
		if policy.AutopilotMode == "safe_auto" || result.Mode == "safe_auto" {
			return fmt.Errorf("safe_auto_deferred")
		}
		if result.Mode != "assist" || feedIntegrityActionMode(policy, result.ActionClass) != "assist" || result.ApprovedAt == nil {
			return fmt.Errorf("fresh human Assist approval required")
		}
		if result.TargetScope != "today" && result.TargetScope != "week" && result.TargetScope != "month" {
			return fmt.Errorf("invalid repair window")
		}
		if !feedIntegrityActionExecutable(result.ActionClass) || result.RegistryVersion != feedIntegrityRegistryVersion {
			return fmt.Errorf("unsupported repair capability")
		}
		boundary, boundaryErr := integrityConfirmationBoundary(tx, tenant, models.FeedIntegrityFinding{CheckKey: episode.CheckKey, Feed: episode.Feed, Variant: episode.Variant, TargetType: "probe", TargetRef: episode.Scope})
		if boundaryErr != nil {
			return boundaryErr
		}
		if result.ApprovedAt.Before(boundary) {
			return fmt.Errorf("human decision invalidated old approval")
		}
		if episode.Status != "open" || integrityEpisodeSuppressed(tx, tenant, episode, now) {
			return fmt.Errorf("episode closed, recovering or suppressed")
		}
		if preEffect {
			if result.Outcome != "claimed" || result.ClaimToken != token || result.ClaimExpiresAt == nil || !result.ClaimExpiresAt.After(now) {
				return fmt.Errorf("claim changed or expired")
			}
		} else if result.Outcome != "approved" && result.Outcome != "ready" {
			return fmt.Errorf("action is no longer executable")
		}
		var run models.FeedIntegrityRun
		if err := tx.Where("id=? AND tenant_id=?", result.RunID, tenant).First(&run).Error; err != nil {
			return err
		}
		if run.Status != "completed" || run.FinishedAt == nil || now.Sub(*run.FinishedAt) > time.Duration(policy.AutopilotEvidenceMaxAgeMinutes)*time.Minute || !feedIntegritySnapshotDualEvidence(tx, run.ID, result.TargetScope) {
			return fmt.Errorf("fresh completed repair evidence required")
		}
		if feedIntegritySystemIncidentOpen(tx) {
			return fmt.Errorf("System Health unavailable or incident open")
		}
		var input struct {
			Evidence struct {
				BuiltAt    time.Time `json:"built_at"`
				Generation int64     `json:"generation"`
			} `json:"evidence"`
		}
		if json.Unmarshal(result.Input, &input) != nil || input.Evidence.BuiltAt.IsZero() {
			return fmt.Errorf("snapshot precondition version missing")
		}
		var snapshot models.NewsSnapshot
		if err := tx.Where("tenant_id=? AND \"window\"=?", tenant, result.TargetScope).First(&snapshot).Error; err != nil {
			return err
		}
		if !snapshot.BuiltAt.Equal(input.Evidence.BuiltAt) || snapshot.Generation != input.Evidence.Generation || (!snapshot.Dirty && time.Since(snapshot.BuiltAt) <= newsSnapshotTTL) {
			return fmt.Errorf("snapshot precondition changed")
		}
		var unknown int64
		if err := tx.Model(&models.FeedIntegrityAction{}).Where("tenant_id=? AND target_scope=? AND error_class='effect_unknown' AND COALESCE(output->'uncertainty_review'->>'disposition','') <> 'allow_fresh_approval'", tenant, result.TargetScope).Count(&unknown).Error; err != nil {
			return err
		}
		if unknown > 0 {
			return fmt.Errorf("unknown previous effect requires audited review")
		}
		if !preEffect {
			var runCount, hourCount, cooldown int64
			if err := tx.Model(&models.FeedIntegrityAction{}).Where("tenant_id=? AND run_id=? AND claimed_at IS NOT NULL", tenant, result.RunID).Count(&runCount).Error; err != nil {
				return err
			}
			if err := tx.Model(&models.FeedIntegrityAction{}).Where("tenant_id=? AND claimed_at>?", tenant, now.Add(-time.Hour)).Count(&hourCount).Error; err != nil {
				return err
			}
			if err := tx.Model(&models.FeedIntegrityAction{}).Where("tenant_id=? AND action_class=? AND target_scope=? AND claimed_at>?", tenant, result.ActionClass, result.TargetScope, now.Add(-time.Duration(policy.AutopilotCooldownMinutes)*time.Minute)).Count(&cooldown).Error; err != nil {
				return err
			}
			if runCount > 0 || hourCount >= int64(policy.AutopilotActionHourlyCap) || cooldown > 0 {
				return fmt.Errorf("action run/hour/cooldown cap reached")
			}
		}
		updates := map[string]interface{}{"updated_at": now}
		if preEffect {
			updates["outcome"] = "running"
		} else {
			expires := now.Add(feedIntegrityClaimLease)
			updates["outcome"] = "claimed"
			updates["claim_token"] = uuid.NewString()
			updates["claimed_at"] = now
			updates["claim_expires_at"] = expires
		}
		prior := result.Outcome
		changed := tx.Model(&models.FeedIntegrityAction{}).Where("id=? AND tenant_id=? AND outcome=? AND COALESCE(claim_token,'')=?", id, tenant, prior, result.ClaimToken).Updates(updates)
		if changed.Error != nil {
			return changed.Error
		}
		if changed.RowsAffected != 1 {
			return fmt.Errorf("action changed concurrently")
		}
		return tx.First(&result, id).Error
	})
	return result, err
}
func markIntegrityEffectUnknown(db *gorm.DB, action models.FeedIntegrityAction, cause error) error {
	now := time.Now().UTC()
	update := db.Model(&models.FeedIntegrityAction{}).Where("id=? AND tenant_id=? AND claim_token=? AND outcome IN ?", action.ID, action.TenantID, action.ClaimToken, []string{"running", "verifying"}).Updates(map[string]interface{}{"outcome": "tool_failed", "error_class": "effect_unknown", "output": integrityJSON(gin.H{"effect_known": false}), "reason": "Effect could not be durably established; human review required", "finished_at": now, "updated_at": now})
	if update.Error != nil {
		return update.Error
	}
	return cause
}
func ReviewFeedIntegrityUnknownAction(c *gin.Context) {
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
		Reason      string `json:"reason"`
		Disposition string `json:"disposition"`
	}
	if c.ShouldBindJSON(&req) != nil || strings.TrimSpace(req.Reason) == "" || len(req.Reason) > 2000 || (req.Disposition != "keep_blocked" && req.Disposition != "allow_fresh_approval") {
		c.JSON(400, gin.H{"error": "reason and valid review disposition required"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var result models.FeedIntegrityAction
	err = db.Transaction(func(tx *gorm.DB) error {
		var hint models.FeedIntegrityAction
		if err := tx.Where("tenant_id=? AND public_id=?", principal.TenantID, id).First(&hint).Error; err != nil {
			return err
		}
		var policy models.FeedIntegrityPolicy
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=?", principal.TenantID).First(&policy).Error; err != nil {
			return err
		}
		var ep models.FeedIntegrityEpisode
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND id=?", principal.TenantID, hint.EpisodeID).First(&ep).Error; err != nil {
			return err
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=? AND tenant_id=?", hint.ID, principal.TenantID).First(&result).Error; err != nil {
			return err
		}
		if result.TargetScope != "today" && result.TargetScope != "week" && result.TargetScope != "month" {
			return fmt.Errorf("invalid repair window")
		}
		if result.Outcome != "tool_failed" || result.ErrorClass != "effect_unknown" {
			return fmt.Errorf("action has no terminal unknown effect")
		}
		var output map[string]interface{}
		_ = json.Unmarshal(result.Output, &output)
		if output == nil {
			output = map[string]interface{}{}
		}
		review := gin.H{"action_id": result.PublicID, "reviewer": principal.Email, "at": time.Now().UTC(), "reason": req.Reason, "disposition": req.Disposition, "window": result.TargetScope}
		output["uncertainty_review"] = review
		if err := tx.Model(&result).Updates(map[string]interface{}{"output": integrityJSON(output), "updated_at": time.Now().UTC()}).Error; err != nil {
			return err
		}
		if err := tx.Create(&models.AuditLog{TenantID: principal.TenantID, UserID: principal.UserID, UserEmail: principal.Email, Action: "feed_integrity.autopilot.action.review_unknown", TargetService: "cms", TargetResource: result.PublicID.String(), Status: "success", Payload: integrityJSON(review)}).Error; err != nil {
			return err
		}
		result.Output = integrityJSON(output)
		return nil
	})
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			c.JSON(404, gin.H{"error": "action not found"})
		} else {
			c.JSON(409, gin.H{"error": safeIntegrityError(err)})
		}
		return
	}
	c.JSON(200, gin.H{"data": result})
}
