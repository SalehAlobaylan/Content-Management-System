package controllers

import (
	"content-management-system/src/models"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// sttEstimatedCostPerHourUsd is the code-default per-audio-hour cost estimate
// used by the budget guard. An algorithm constant → code default (per Config
// Discipline), not an env var. Tracks the wired default engine (Deepgram Nova-3,
// ~$0.26/hr); swapping engines can revisit this.
const sttEstimatedCostPerHourUsd = 0.26

// sttSkippedError signals the guard declined to run STT for a NON-failure reason
// (already upgraded, human caption present, auto-STT disabled, over budget).
// Callers surface these as "skipped" results, not errors.
type sttSkippedError struct{ reason string }

func (e *sttSkippedError) Error() string { return e.reason }

// isSTTSkipped reports whether err is a guard skip (vs a real failure).
func isSTTSkipped(err error) bool {
	var s *sttSkippedError
	return errors.As(err, &s)
}

// getOrCreateTranscriptionConfig loads the tenant's STT config, creating a
// default row if missing, and rolls the 30-day spend window when elapsed.
func getOrCreateTranscriptionConfig(db *gorm.DB, tenantID string) *models.TranscriptionConfig {
	var cfg models.TranscriptionConfig
	if err := db.Where("tenant_id = ?", tenantID).First(&cfg).Error; err != nil {
		cfg = models.DefaultTranscriptionConfig(tenantID)
		db.Create(&cfg)
	}
	if time.Since(cfg.MonthlyWindowStart) > 30*24*time.Hour {
		cfg.MonthlySpendUsd = 0
		cfg.MonthlyReservedUsd = 0
		cfg.MonthlyWindowStart = time.Now()
		db.Save(&cfg)
	}
	return &cfg
}

// estimateSTTCostUSD estimates transcription cost from an item's duration.
func estimateSTTCostUSD(durationSec *int) float64 {
	if durationSec == nil || *durationSec <= 0 {
		return 0
	}
	return (float64(*durationSec) / 3600.0) * sttEstimatedCostPerHourUsd
}

// ── GET /admin/transcription-config ─────────────────────────

func GetTranscriptionConfig(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	cfg := getOrCreateTranscriptionConfig(db, principal.TenantID)
	c.JSON(http.StatusOK, cfg)
}

// ── PATCH /admin/transcription-config ───────────────────────

func UpdateTranscriptionConfig(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)

	var req struct {
		AutoSttEnabled             *bool    `json:"auto_stt_enabled"`
		Provider                   *string  `json:"provider"`
		MonthlyBudgetCapUsd        *float64 `json:"monthly_budget_cap_usd"`
		AutoRepairEnabled          *bool    `json:"auto_repair_enabled"`
		QualityReviewThreshold     *float64 `json:"quality_review_threshold"`
		QualityAutoRepairThreshold *float64 `json:"quality_auto_repair_threshold"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request: " + err.Error(), Code: "INVALID_REQUEST"})
		return
	}

	cfg := getOrCreateTranscriptionConfig(db, principal.TenantID)
	if req.AutoSttEnabled != nil {
		cfg.AutoSttEnabled = *req.AutoSttEnabled
	}
	if req.Provider != nil {
		cfg.Provider = *req.Provider
	}
	if req.MonthlyBudgetCapUsd != nil {
		if *req.MonthlyBudgetCapUsd < 0 {
			c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Budget cap must be >= 0", Code: "INVALID_BUDGET"})
			return
		}
		cfg.MonthlyBudgetCapUsd = *req.MonthlyBudgetCapUsd
	}
	if req.AutoRepairEnabled != nil {
		cfg.AutoRepairEnabled = *req.AutoRepairEnabled
	}
	if req.QualityReviewThreshold != nil {
		if *req.QualityReviewThreshold < 0 || *req.QualityReviewThreshold > 1 {
			c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Review threshold must be between 0 and 1", Code: "INVALID_THRESHOLD"})
			return
		}
		cfg.QualityReviewThreshold = *req.QualityReviewThreshold
	}
	if req.QualityAutoRepairThreshold != nil {
		if *req.QualityAutoRepairThreshold < 0 || *req.QualityAutoRepairThreshold > 1 {
			c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Auto-repair threshold must be between 0 and 1", Code: "INVALID_THRESHOLD"})
			return
		}
		cfg.QualityAutoRepairThreshold = *req.QualityAutoRepairThreshold
	}
	if cfg.QualityAutoRepairThreshold > cfg.QualityReviewThreshold {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Auto-repair threshold cannot be higher than review threshold", Code: "INVALID_THRESHOLD"})
		return
	}

	if err := db.Save(cfg).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to update config", Code: "UPDATE_FAILED"})
		return
	}
	c.JSON(http.StatusOK, cfg)
}

// ── POST /internal/content-items/:id/request-stt ────────────
//
// DECISION endpoint for Aggregation's AI worker. It applies the guard (auto-STT
// toggle + caption-state machine + budget cap) and, when allowed, RESERVES the
// estimated spend and returns triggered=true — Aggregation then runs STT via
// Media using its own sync/async routing (so long podcasts keep the async path
// instead of blocking a CMS→Media call). The guard stays centralized here so
// the toggle + budget are enforced in one place. `triggered=false` carries the
// skip reason (disabled / already upgraded / over budget).
func InternalRequestSTT(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)

	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid content ID"})
		return
	}

	var item models.ContentItem
	if err := db.Where("public_id = ?", id).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Content not found"})
		return
	}

	var req struct {
		Force bool `json:"force"`
	}
	_ = c.ShouldBindJSON(&req) // body optional

	job, triggered, reason, err := createTranscriptionJobForItem(db, &item, models.TranscriptionTriggerIngestAuto, req.Force)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"triggered": false, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"triggered": triggered,
		"reason":    reason,
		"job_id":    job.PublicID.String(),
		"status":    job.Status,
	})
}
