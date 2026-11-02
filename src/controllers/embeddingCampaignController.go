package controllers

import (
	"content-management-system/src/utils"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"content-management-system/src/models"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// Embedding & Model Lifecycle System (stage 10) — campaign HTTP surface + tick
// (Slice 3). Privileged mutations (create/start/abort) are gated by admin role
// at the route layer; a `started_by` principal is recorded for the audit trail.

// PreviewEmbeddingCampaign — dry-run impact for a space.
func PreviewEmbeddingCampaign(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	space := c.Query("space")
	if space == "" {
		var body struct {
			Space string `json:"space"`
		}
		_ = c.ShouldBindJSON(&body)
		space = body.Space
	}
	if space != EmbeddingSpaceText && space != EmbeddingSpaceImage {
		c.JSON(http.StatusBadRequest, gin.H{"error": "space must be 'text' or 'image'"})
		return
	}
	c.JSON(http.StatusOK, computeCampaignPreview(db, space))
}

// CreateEmbeddingCampaign — create a frozen draft.
func CreateEmbeddingCampaign(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var body struct {
		Space         string `json:"space"`
		ItemsPerBatch int    `json:"items_per_batch"`
		BatchesPerRun int    `json:"batches_per_run"`
		DailyItemCap  int    `json:"daily_item_cap"`
		RetryCeiling  int    `json:"retry_ceiling"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	policy, err := getOrCreateEmbeddingPolicy(db)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	caps := campaignCaps{
		ItemsPerBatch: clampPositive(body.ItemsPerBatch, policy.ItemsPerBatch, 1, 500),
		BatchesPerRun: clampPositive(body.BatchesPerRun, policy.BatchesPerRun, 1, 5),
		DailyItemCap:  clampPositive(body.DailyItemCap, policy.DailyItemCap, 1, 50_000),
		RetryCeiling:  clampPositive(body.RetryCeiling, policy.RetryCeiling, 1, 10),
	}
	var camp *models.EmbeddingCampaign
	err = db.Transaction(func(tx *gorm.DB) error {
		var e error
		camp, e = createCampaignDraft(tx, body.Space, caps)
		if e != nil {
			return e
		}
		return writeEmbeddingAdminAudit(c, tx, "campaign_draft", fmt.Sprint(camp.ID), map[string]any{"target_space_id": camp.TargetSpaceID, "space": camp.Space})
	})
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, camp)
}

// StartEmbeddingCampaign — human start (admin role). Requires a reason.
func StartEmbeddingCampaign(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	var body struct {
		Reason        string `json:"reason"`
		TargetSpaceID string `json:"target_space_id"`
	}
	_ = c.ShouldBindJSON(&body)
	body.Reason = strings.TrimSpace(body.Reason)
	if body.Reason == "" || strings.TrimSpace(body.TargetSpaceID) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "a reason and confirmed target_space_id are required to start a campaign"})
		return
	}
	principal := adminPrincipalLabel(c)
	var camp *models.EmbeddingCampaign
	err = db.Transaction(func(tx *gorm.DB) error {
		var e error
		camp, e = startCampaign(tx, uint(id), principal, body.Reason, body.TargetSpaceID)
		if e != nil {
			return e
		}
		return writeEmbeddingAdminAudit(c, tx, "campaign_start", fmt.Sprint(id), map[string]any{"reason": body.Reason, "target_space_id": camp.TargetSpaceID, "caps": map[string]int{"items_per_batch": camp.ItemsPerBatch, "daily_item_cap": camp.DailyItemCap}})
	})
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, camp)
}

// PauseEmbeddingCampaign / ResumeEmbeddingCampaign / AbortEmbeddingCampaign.
func PauseEmbeddingCampaign(c *gin.Context)  { transitionCampaignHandler(c, "pause") }
func ResumeEmbeddingCampaign(c *gin.Context) { transitionCampaignHandler(c, "resume") }
func AbortEmbeddingCampaign(c *gin.Context)  { transitionCampaignHandler(c, "abort") }

func transitionCampaignHandler(c *gin.Context, action string) {
	db := c.MustGet("db").(*gorm.DB)
	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	var camp *models.EmbeddingCampaign
	err = db.Transaction(func(tx *gorm.DB) error {
		var e error
		camp, e = transitionCampaign(tx, uint(id), action)
		if e != nil {
			return e
		}
		return writeEmbeddingAdminAudit(c, tx, "campaign_"+action, fmt.Sprint(id), map[string]any{"state": camp.State})
	})
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, camp)
}

// ListEmbeddingCampaigns / GetEmbeddingCampaign.
func ListEmbeddingCampaigns(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var camps []models.EmbeddingCampaign
	if err := db.Where("tenant_id = ?", embeddingLifecycleTenant).Order("created_at DESC").Limit(100).Find(&camps).Error; err != nil {
		c.JSON(500, gin.H{"error": "campaign history unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"campaigns": camps})
}

func GetEmbeddingCampaign(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	var camp models.EmbeddingCampaign
	if err := db.First(&camp, id).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "campaign not found"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "campaign history unavailable"})
		}
		return
	}
	var actions []models.EmbeddingCampaignAction
	if err := db.Where("campaign_id = ?", id).Order("id DESC").Limit(200).Find(&actions).Error; err != nil {
		c.JSON(500, gin.H{"error": "campaign actions unavailable"})
		return
	}
	var exceptions []models.EmbeddingCampaignException
	if err := db.Where("campaign_id = ? AND status != ?", id, models.EmbeddingExceptionResolved).Find(&exceptions).Error; err != nil {
		c.JSON(500, gin.H{"error": "campaign exceptions unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"campaign": camp, "actions": actions, "exceptions": exceptions})
}

// ListEmbeddingCampaignActions — filterable action ledger.
func ListEmbeddingCampaignActions(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	q := db.Model(&models.EmbeddingCampaignAction{}).Where("campaign_id IN (SELECT id FROM embedding_campaigns WHERE tenant_id=?)", embeddingLifecycleTenant)
	if v := c.Query("campaign_id"); v != "" {
		q = q.Where("campaign_id = ?", v)
	}
	if v := c.Query("status"); v != "" {
		q = q.Where("status = ?", v)
	}
	limit := 200
	if l := c.Query("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 1000 {
			limit = n
		}
	}
	var actions []models.EmbeddingCampaignAction
	if err := q.Order("id DESC").Limit(limit).Find(&actions).Error; err != nil {
		c.JSON(500, gin.H{"error": "campaign actions unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"actions": actions})
}

// ListEmbeddingCampaignExceptions.
func ListEmbeddingCampaignExceptions(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	var exceptions []models.EmbeddingCampaignException
	if err := db.Where("campaign_id = ?", id).Order("id DESC").Find(&exceptions).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "campaign exceptions unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"exceptions": exceptions})
}

// RetryEmbeddingException / WaiveEmbeddingException — human exception path.
func RetryEmbeddingException(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	var ex models.EmbeddingCampaignException
	if err := db.First(&ex, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "exception not found"})
		return
	}
	ex.Status = models.EmbeddingExceptionRetrying
	if err := db.Transaction(func(tx *gorm.DB) error {
		if e := tx.Save(&ex).Error; e != nil {
			return e
		}
		return writeEmbeddingAdminAudit(c, tx, "exception_retry", fmt.Sprint(ex.ID), map[string]any{"campaign_id": ex.CampaignID})
	}); err != nil {
		c.JSON(500, gin.H{"error": "could not record retry"})
		return
	}
	c.JSON(http.StatusOK, ex)
}

func WaiveEmbeddingException(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	var body struct {
		Reason      string `json:"reason"`
		ExpiryHours int    `json:"expiry_hours"`
	}
	_ = c.ShouldBindJSON(&body)
	body.Reason = strings.TrimSpace(body.Reason)
	if body.Reason == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "a reason is required to waive"})
		return
	}
	if body.ExpiryHours < 1 || body.ExpiryHours > 720 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "expiry_hours must be in [1, 720]"})
		return
	}
	var ex models.EmbeddingCampaignException
	if err := db.First(&ex, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "exception not found"})
		return
	}
	// A waiver removes a target from campaign scope ONLY — it never marks the
	// surface coherent or the vector comparable (comparability guards still
	// exclude it). Recorded in the CMS audit log by the route middleware.
	ex.Status = models.EmbeddingExceptionWaived
	ex.WaivedBy = adminPrincipalLabel(c)
	ex.WaiverReason = body.Reason
	expires := time.Now().Add(time.Duration(body.ExpiryHours) * time.Hour)
	ex.WaiverExpires = &expires
	if err := db.Transaction(func(tx *gorm.DB) error {
		if e := tx.Save(&ex).Error; e != nil {
			return e
		}
		return writeEmbeddingAdminAudit(c, tx, "exception_waive", fmt.Sprint(ex.ID), map[string]any{"campaign_id": ex.CampaignID, "reason": ex.WaiverReason, "expires_at": expires})
	}); err != nil {
		c.JSON(500, gin.H{"error": "could not record waiver"})
		return
	}
	c.JSON(http.StatusOK, ex)
}

// adminPrincipalLabel best-effort extracts the acting admin for the audit trail.
func adminPrincipalLabel(c *gin.Context) string {
	p, ok := utils.GetAdminPrincipal(c)
	if !ok {
		return "unknown"
	}
	if p.Email != "" {
		return p.Email
	}
	return p.UserID
}

func writeEmbeddingAdminAudit(c *gin.Context, tx *gorm.DB, action, target string, payload map[string]any) error {
	principal, ok := utils.GetAdminPrincipal(c)
	if !ok {
		return fmt.Errorf("authenticated admin identity missing")
	}
	return tx.Create(&models.AuditLog{TenantID: embeddingLifecycleTenant, UserID: principal.UserID, UserEmail: principal.Email, Action: "embedding_" + action, TargetService: "cms", TargetResource: target, Status: "success", Payload: jsonEvidence(payload)}).Error
}

func firstPositive(v, def int) int {
	if v > 0 {
		return v
	}
	return def
}
func firstNonNeg(v, def int) int {
	if v >= 0 && v != 0 {
		return v
	}
	return def
}

func clampPositive(v, def, minV, maxV int) int {
	if v <= 0 {
		v = def
	}
	if v < minV {
		return minV
	}
	if v > maxV {
		return maxV
	}
	return v
}

func clampNonNegative(v, def, maxV int) int {
	if v < 0 {
		v = def
	}
	if v > maxV {
		return maxV
	}
	return v
}
