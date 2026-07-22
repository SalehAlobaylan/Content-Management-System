package controllers

import (
	"net/http"
	"strings"

	"content-management-system/src/models"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type deleteUserProductDataRequest struct {
	TenantID string `json:"tenant_id" binding:"required"`
}

// InternalDeleteUserProductData is IAM's idempotent erasure command. CMS only
// removes product data; IAM remains responsible for identity and confirmation.
func InternalDeleteUserProductData(c *gin.Context) {
	userID, err := uuid.Parse(c.Param("user_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid user id"})
		return
	}
	var req deleteUserProductDataRequest
	if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.TenantID) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "tenant_id is required"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	identityScope := "user:" + userID.String()
	err = db.Transaction(func(tx *gorm.DB) error {
		// Remove the account's interactions, but first preserve the rows needed
		// to keep the denormalized engagement counts and comment-report graph
		// consistent after the erasure.
		var interactions []models.UserInteraction
		if err := tx.Where("user_id = ?", userID).Find(&interactions).Error; err != nil {
			return err
		}
		commentIDs := make([]uuid.UUID, 0)
		for _, interaction := range interactions {
			if interaction.Type == models.InteractionTypeComment {
				commentIDs = append(commentIDs, interaction.PublicID)
			}
		}
		statements := []struct {
			query string
			args  []any
		}{
			{"DELETE FROM consumer_moderation_idempotency WHERE reporter_id = ?", []any{userID}},
			{"DELETE FROM moderation_reports WHERE reporter_id = ? AND tenant_id = ?", []any{userID, req.TenantID}},
			{"DELETE FROM user_blocks WHERE tenant_id = ? AND (user_id = ? OR blocked_user_id = ?)", []any{req.TenantID, userID, userID}},
			{"DELETE FROM consumer_request_idempotency WHERE identity_scope = ?", []any{identityScope}},
			{"DELETE FROM consumer_feed_sessions WHERE identity_scope = ?", []any{identityScope}},
			{"DELETE FROM user_topic_prefs WHERE tenant_id = ? AND user_id = ?", []any{req.TenantID, userID}},
			{"DELETE FROM user_source_prefs WHERE tenant_id = ? AND user_id = ?", []any{req.TenantID, userID}},
			{"DELETE FROM user_topic_affinity WHERE tenant_id = ? AND user_id = ?", []any{req.TenantID, userID}},
			{"DELETE FROM user_category_affinity WHERE tenant_id = ? AND user_id = ?", []any{req.TenantID, userID}},
			{"DELETE FROM preference_affinity_recompute_queue WHERE tenant_id = ? AND user_id = ?", []any{req.TenantID, userID}},
			{"DELETE FROM user_interactions WHERE user_id = ?", []any{userID}},
			{"DELETE FROM auth_suspensions WHERE user_id = ? AND tenant_id = ?", []any{userID, req.TenantID}},
		}
		if len(commentIDs) > 0 {
			// Other users' report idempotency records reference the comment report
			// with ON DELETE RESTRICT, so remove those retry receipts before the
			// now-invalid reports for an erased comment.
			if err := tx.Exec(
				"DELETE FROM consumer_moderation_idempotency WHERE report_id IN (SELECT public_id FROM moderation_reports WHERE target_type = ? AND target_id IN ?)",
				models.ModerationTargetComment,
				commentIDs,
			).Error; err != nil {
				return err
			}
			if err := tx.Where("target_type = ? AND target_id IN ?", models.ModerationTargetComment, commentIDs).Delete(&models.ModerationReport{}).Error; err != nil {
				return err
			}
		}
		for _, statement := range statements {
			if err := tx.Exec(statement.query, statement.args...).Error; err != nil {
				return err
			}
		}
		for _, interaction := range interactions {
			if err := updateEngagementCount(tx, interaction.ContentItemID, interaction.Type, -1); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to delete product data"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"user_id": userID, "deleted": true})
}
