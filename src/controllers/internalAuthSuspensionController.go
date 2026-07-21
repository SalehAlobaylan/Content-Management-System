package controllers

import (
	"content-management-system/src/models"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type syncAuthSuspensionRequest struct {
	TenantID  string `json:"tenant_id" binding:"required"`
	Suspended bool   `json:"suspended"`
}

// InternalSyncAuthSuspension mirrors an IAM-owned account suspension. It is
// intentionally idempotent: the IAM control-plane retries safely on timeouts.
func InternalSyncAuthSuspension(c *gin.Context) {
	userID, err := uuid.Parse(c.Param("user_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid user id"})
		return
	}

	var req syncAuthSuspensionRequest
	if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.TenantID) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "tenant_id is required"})
		return
	}

	db := c.MustGet("db").(*gorm.DB)
	if req.Suspended {
		record := models.AuthSuspension{UserID: userID, TenantID: strings.TrimSpace(req.TenantID), SuspendedAt: time.Now().UTC()}
		if err := db.Where("user_id = ?", userID).Assign(record).FirstOrCreate(&record).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to mirror account suspension"})
			return
		}
	} else if err := db.Where("user_id = ? AND tenant_id = ?", userID, strings.TrimSpace(req.TenantID)).Delete(&models.AuthSuspension{}).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to clear account suspension"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"user_id": userID, "suspended": req.Suspended})
}
