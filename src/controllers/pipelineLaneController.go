package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type pipelineLaneAdmissionCounts struct {
	Accepted          int64 `json:"accepted"`
	Rejected          int64 `json:"rejected"`
	InFlight          int64 `json:"in_flight"`
	RetryAfterSeconds int64 `json:"retry_after_seconds"`
}

type pipelineLaneSnapshotRequest struct {
	TenantID                 string                                 `json:"tenant_id"`
	Lane                     string                                 `json:"lane"`
	RequiredQueueDepth       int                                    `json:"required_queue_depth"`
	OptionalQueueDepth       int                                    `json:"optional_queue_depth"`
	RequiredOldestAgeSeconds float64                                `json:"required_oldest_age_seconds"`
	OptionalOldestAgeSeconds float64                                `json:"optional_oldest_age_seconds"`
	DLQDelta                 int                                    `json:"dlq_delta"`
	FailureClasses           map[string]int                         `json:"failure_classes"`
	StageCounts              map[string]int                         `json:"stage_counts"`
	EnrichmentCounts         map[string]pipelineLaneAdmissionCounts `json:"enrichment_counts"`
	ProcessMetrics           map[string]interface{}                 `json:"process_metrics"`
	ResourceMetrics          map[string]interface{}                 `json:"resource_metrics"`
	CapturedAt               *time.Time                             `json:"captured_at"`
}

func InternalPutPipelineLaneSnapshot(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	principal, ok := utils.GetMachinePrincipal(c)
	if !ok || (principal != utils.MachinePrincipalAggregation && principal != utils.MachinePrincipalEnrichment) {
		c.JSON(http.StatusForbidden, gin.H{"error": "pipeline snapshot principal is not allowed"})
		return
	}
	if !db.Migrator().HasTable(&models.PipelineLaneHealthSnapshot{}) {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "feed reliability migration is pending", "state": "schema_pending"})
		return
	}
	var req pipelineLaneSnapshotRequest
	if err := c.ShouldBindJSON(&req); err != nil || (req.Lane != models.ContentStageLaneNews && req.Lane != models.ContentStageLanePods) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "valid lane snapshot is required"})
		return
	}
	if req.Lane != strings.TrimSpace(c.Param("lane")) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "lane snapshot path and payload must match"})
		return
	}
	if len(req.FailureClasses) > 32 || len(req.StageCounts) > 64 || len(req.EnrichmentCounts) > 32 || len(req.ProcessMetrics) > 32 || len(req.ResourceMetrics) > 32 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "snapshot dimensions exceed bounded limits"})
		return
	}
	for _, counts := range req.EnrichmentCounts {
		if counts.Accepted < 0 || counts.Rejected < 0 || counts.InFlight < 0 || counts.RetryAfterSeconds < 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "enrichment counts cannot be negative"})
			return
		}
	}
	tenantIDs, err := snapshotTenantIDs(db, req.TenantID, req.Lane)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	captured := time.Now().UTC()
	if req.CapturedAt != nil {
		captured = req.CapturedAt.UTC()
	}
	if captured.Before(time.Now().UTC().Add(-5*time.Minute)) || captured.After(time.Now().UTC().Add(time.Minute)) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "captured_at is outside the current snapshot window"})
		return
	}
	jsonOrEmpty := func(value any) datatypes.JSON { return datatypes.JSON(mustJSON(value)) }
	rows := make([]models.PipelineLaneHealthSnapshot, 0, len(tenantIDs))
	for _, tenantID := range tenantIDs {
		rows = append(rows, models.PipelineLaneHealthSnapshot{
			TenantID: tenantID, Lane: req.Lane, OwnerPrincipal: string(principal),
			RequiredQueueDepth: maxInt(req.RequiredQueueDepth, 0), OptionalQueueDepth: maxInt(req.OptionalQueueDepth, 0),
			RequiredOldestAgeSeconds: maxFloat(req.RequiredOldestAgeSeconds, 0), OptionalOldestAgeSeconds: maxFloat(req.OptionalOldestAgeSeconds, 0),
			DLQDelta: req.DLQDelta, FailureClasses: jsonOrEmpty(req.FailureClasses), StageCounts: jsonOrEmpty(req.StageCounts),
			EnrichmentCounts: jsonOrEmpty(req.EnrichmentCounts), ProcessMetrics: jsonOrEmpty(req.ProcessMetrics), ResourceMetrics: jsonOrEmpty(req.ResourceMetrics), CapturedAt: captured,
		})
	}
	if err := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "tenant_id"}, {Name: "lane"}, {Name: "owner_principal"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"required_queue_depth", "optional_queue_depth", "required_oldest_age_seconds",
			"optional_oldest_age_seconds", "dlq_delta", "failure_classes", "stage_counts",
			"enrichment_counts", "process_metrics", "resource_metrics", "captured_at",
		}),
	}).Create(&rows).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "pipeline snapshot could not be persisted"})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"tenants": rows})
}

func snapshotTenantIDs(db *gorm.DB, hinted, lane string) ([]string, error) {
	if value := strings.TrimSpace(hinted); value != "" {
		if len(value) > 64 {
			return nil, fmt.Errorf("tenant_id exceeds the bounded scope")
		}
		return []string{value}, nil
	}
	category := models.SourceCategoryNews
	if lane == models.ContentStageLanePods {
		category = models.SourceCategoryMedia
	}
	var tenants []string
	if err := db.Model(&models.ContentSource{}).Where("category=? AND is_active=TRUE", category).Distinct("tenant_id").Order("tenant_id ASC").Pluck("tenant_id", &tenants).Error; err != nil {
		return nil, err
	}
	if len(tenants) == 0 {
		var kinds []models.ContentType
		if lane == models.ContentStageLaneNews {
			kinds = []models.ContentType{models.ContentTypeNews}
		} else {
			kinds = []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}
		}
		if err := db.Model(&models.ContentItem{}).Where("type IN ?", kinds).Distinct("tenant_id").Order("tenant_id ASC").Pluck("tenant_id", &tenants).Error; err != nil {
			return nil, err
		}
	}
	if len(tenants) == 0 {
		// A newly provisioned tenant can have no source or content rows yet,
		// while the lane-health publisher is already running.  Rejecting that
		// telemetry with 400 makes the console report a false "live data
		// unavailable" state and leaves the first snapshot impossible to write.
		// The CMS default tenant is the only valid scope in this situation; once
		// a tenant has data the queries above return its exact id instead.
		tenants = []string{"default"}
	}
	return tenants, nil
}

func mustJSON(value any) []byte {
	data, _ := json.Marshal(value)
	return data
}
