package controllers

import (
	"content-management-system/src/contentstage"
	"content-management-system/src/models"
	"content-management-system/src/podscontrol"
	"content-management-system/src/supply"
	"encoding/json"
	"errors"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"io"
	"net/http"
	"os"
	"time"
)

func GetPodsAutomation(c *gin.Context) {
	p, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB).WithContext(c.Request.Context())
	control, err := podscontrol.Read(db, p.TenantID)
	if err != nil {
		c.JSON(503, gin.H{"message": "Pods pipeline controls are unavailable"})
		return
	}
	acquisition := models.MediaAcquisitionConfig{DefaultMode: models.MediaAcquisitionAutomatic}
	if err := db.Where("tenant_id=?", p.TenantID).First(&acquisition).Error; err != nil && err != gorm.ErrRecordNotFound {
		c.JSON(503, gin.H{"message": "Processing settings are unavailable"})
		return
	}
	policy := models.DefaultMediaAtomizationPolicy(p.TenantID)
	if err := db.Where("tenant_id=?", p.TenantID).First(&policy).Error; err != nil && err != gorm.ErrRecordNotFound {
		c.JSON(503, gin.H{"message": "Publishing settings are unavailable"})
		return
	}
	var activeSources, automaticOverrides, sourceOverrides int64
	queries := []struct {
		query *gorm.DB
		count *int64
	}{
		{db.Model(&models.ContentSource{}).Where("tenant_id=? AND category='media' AND is_active=TRUE", p.TenantID), &activeSources},
		{db.Model(&models.ContentSource{}).Where("tenant_id=? AND category='media' AND is_active=TRUE AND media_acquisition_mode='automatic'", p.TenantID), &automaticOverrides},
		{db.Model(&models.ContentSource{}).Where("tenant_id=? AND category='media' AND is_active=TRUE AND media_acquisition_mode IS NOT NULL", p.TenantID), &sourceOverrides},
	}
	activity, err := readPodsPipelineActivityCounts(db, p.TenantID)
	if err != nil {
		c.JSON(503, gin.H{"message": "Pods activity could not be checked"})
		return
	}
	for _, q := range queries {
		if err := q.query.Count(q.count).Error; err != nil {
			c.JSON(503, gin.H{"message": "Pods activity could not be checked"})
			return
		}
	}
	mode, err := contentstage.CutoverMode(db, p.TenantID, models.ContentStageLanePods)
	if err != nil {
		c.JSON(503, gin.H{"message": "Pods processing readiness is unavailable"})
		return
	}
	ready := mode == models.ContentStageCutoverDurableRequired && supply.RequireDurableAdmission(db, p.TenantID, models.SourceCategoryMedia) == nil
	c.Header("Cache-Control", "no-store")
	c.JSON(200, gin.H{
		"paused":  !control.SchedulingEnabled || !control.ExecutionEnabled,
		"version": podscontrol.Version(control), "environment": os.Getenv("WAHB_ENVIRONMENT_ID"), "tenant_id": p.TenantID,
		"can_manage": p.HasPermission("aggregation:manage"), "ready": ready,
		"active_sources": activeSources, "active_steps": activity.ClaimedSteps + activity.RunningSteps,
		"queued_steps": activity.QueuedSteps, "active_source_runs": activity.AdmittedSourceRuns + activity.RunningSourceRuns,
		"activity": activity, "observed_at": time.Now().UTC(), "pause_reason": control.Reason,
		"processing_mode": acquisition.DefaultMode, "automatic_source_overrides": automaticOverrides, "source_override_count": sourceOverrides,
		"auto_publish": policy.AutoPublishHighConfidence, "chaptering_enabled": policy.ChapteringEnabled,
		"updated_at": control.UpdatedAt, "updated_by": control.UpdatedBy,
	})
}

func UpdatePodsAutomation(c *gin.Context) {
	p, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	var req struct {
		Paused          *bool  `json:"paused"`
		ExpectedVersion string `json:"expected_version"`
	}
	decoder := json.NewDecoder(io.LimitReader(c.Request.Body, 4096))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&req) != nil || req.Paused == nil || len(req.ExpectedVersion) != 64 {
		c.JSON(400, gin.H{"message": "Paused state and current control version are required"})
		return
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		c.JSON(400, gin.H{"message": "Invalid pipeline change"})
		return
	}
	db := c.MustGet("db").(*gorm.DB).WithContext(c.Request.Context())
	if !*req.Paused {
		mode, err := contentstage.CutoverMode(db, p.TenantID, models.ContentStageLanePods)
		if err != nil || mode != models.ContentStageCutoverDurableRequired || supply.RequireDurableAdmission(db, p.TenantID, models.SourceCategoryMedia) != nil {
			c.JSON(409, gin.H{"message": "Pods processing must be provisioned before the pipeline can resume"})
			return
		}
	}
	enabled := !*req.Paused
	reason := "resumed_from_pods_automation"
	if *req.Paused {
		reason = "paused_from_pods_automation"
	}
	_, err := contentstage.UpdateControl(db, p.TenantID, models.ContentStageLanePods, p.Email, &enabled, &enabled, nil, nil, reason, req.ExpectedVersion)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, podscontrol.ErrChanged) {
			status = http.StatusConflict
		}
		c.JSON(status, gin.H{"message": err.Error()})
		return
	}
	GetPodsAutomation(c)
}
