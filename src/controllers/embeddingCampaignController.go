package controllers

import (
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
	camp, err := createCampaignDraft(db, body.Space, caps)
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
		Reason string `json:"reason"`
	}
	_ = c.ShouldBindJSON(&body)
	body.Reason = strings.TrimSpace(body.Reason)
	if body.Reason == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "a reason is required to start a campaign"})
		return
	}
	principal := adminPrincipalLabel(c)
	camp, err := startCampaign(db, uint(id), principal, body.Reason)
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
	camp, err := transitionCampaign(db, uint(id), action)
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
	db.Where("tenant_id = ?", embeddingLifecycleTenant).Order("created_at DESC").Limit(100).Find(&camps)
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
		c.JSON(http.StatusNotFound, gin.H{"error": "campaign not found"})
		return
	}
	var actions []models.EmbeddingCampaignAction
	db.Where("campaign_id = ?", id).Order("id DESC").Limit(200).Find(&actions)
	var exceptions []models.EmbeddingCampaignException
	db.Where("campaign_id = ? AND status != ?", id, models.EmbeddingExceptionResolved).Find(&exceptions)
	c.JSON(http.StatusOK, gin.H{"campaign": camp, "actions": actions, "exceptions": exceptions})
}

// ListEmbeddingCampaignActions — filterable action ledger.
func ListEmbeddingCampaignActions(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	q := db.Model(&models.EmbeddingCampaignAction{}).Where("tenant_id = ?", embeddingLifecycleTenant)
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
	q.Order("id DESC").Limit(limit).Find(&actions)
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
	db.Where("campaign_id = ?", id).Order("id DESC").Find(&exceptions)
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
	db.Save(&ex)
	db.Model(&models.EmbeddingCampaign{}).Where("id = ? AND state = ?", ex.CampaignID, models.EmbeddingCampaignBlocked).
		Updates(map[string]interface{}{"state": models.EmbeddingCampaignRunning, "blocked_reason": ""})
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
	db.Save(&ex)
	db.Model(&models.EmbeddingCampaign{}).Where("id = ? AND state = ?", ex.CampaignID, models.EmbeddingCampaignBlocked).
		Updates(map[string]interface{}{"state": models.EmbeddingCampaignRunning, "blocked_reason": ""})
	c.JSON(http.StatusOK, ex)
}

// adminPrincipalLabel best-effort extracts the acting admin for the audit trail.
func adminPrincipalLabel(c *gin.Context) string {
	if v, ok := c.Get("admin_email"); ok {
		if s, ok := v.(string); ok && s != "" {
			return s
		}
	}
	if v, ok := c.Get("principal"); ok {
		if s, ok := v.(string); ok && s != "" {
			return s
		}
	}
	return "admin"
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
