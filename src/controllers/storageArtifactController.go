package controllers

import (
	"content-management-system/src/models"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type storageArtifactEventInput struct {
	TenantID              string
	ContentItemID         uuid.UUID
	ParentContentItemID   *uuid.UUID
	EventType             string
	Status                string
	Reason                string
	Trigger               string
	Source                string
	StorageTier           string
	OldStorageTier        string
	OldMediaURL           string
	NewMediaURL           string
	OldSizeBytes          int64
	NewSizeBytes          int64
	FreedBytes            int64
	DeletedBytes          int64
	QualityProfileID      *uint
	ArtifactKeys          interface{}
	RecoveryPayload       interface{}
	Error                 string
	CreatedBy             string
	StorageState          string
	StorageStateReason    string
	StorageRecoveryStatus string
}

type storageArtifactEventRequest struct {
	TenantID              string      `json:"tenant_id"`
	ContentItemID         string      `json:"content_item_id"`
	ParentContentItemID   *string     `json:"parent_content_item_id"`
	EventType             string      `json:"event_type"`
	Status                string      `json:"status"`
	Reason                string      `json:"reason"`
	Trigger               string      `json:"trigger"`
	Source                string      `json:"source"`
	StorageTier           string      `json:"storage_tier"`
	OldStorageTier        string      `json:"old_storage_tier"`
	OldMediaURL           string      `json:"old_media_url"`
	NewMediaURL           string      `json:"new_media_url"`
	OldSizeBytes          int64       `json:"old_size_bytes"`
	NewSizeBytes          int64       `json:"new_size_bytes"`
	FreedBytes            int64       `json:"freed_bytes"`
	DeletedBytes          int64       `json:"deleted_bytes"`
	QualityProfileID      *uint       `json:"quality_profile_id"`
	ArtifactKeys          interface{} `json:"artifact_keys"`
	RecoveryPayload       interface{} `json:"recovery_payload"`
	Error                 string      `json:"error"`
	CreatedBy             string      `json:"created_by"`
	StorageState          string      `json:"storage_state"`
	StorageStateReason    string      `json:"storage_state_reason"`
	StorageRecoveryStatus string      `json:"storage_recovery_status"`
}

func storageJSON(v interface{}) datatypes.JSON {
	if v == nil {
		return nil
	}
	switch t := v.(type) {
	case datatypes.JSON:
		return t
	case []byte:
		if len(t) == 0 {
			return nil
		}
		return datatypes.JSON(t)
	case string:
		if strings.TrimSpace(t) == "" {
			return nil
		}
		b, err := json.Marshal(t)
		if err != nil {
			return nil
		}
		return datatypes.JSON(b)
	default:
		b, err := json.Marshal(v)
		if err != nil || string(b) == "null" {
			return nil
		}
		return datatypes.JSON(b)
	}
}

func tierFromItem(item models.ContentItem) string {
	if item.StorageTier != nil && strings.TrimSpace(*item.StorageTier) != "" {
		return *item.StorageTier
	}
	return "primary"
}

func stringValue(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func int64Value(v *int64, fallback int64) int64 {
	if v == nil {
		return fallback
	}
	return *v
}

func defaultStorageStateForItem(item models.ContentItem) string {
	if item.StorageTier != nil && *item.StorageTier == "cold" {
		return models.StorageStateCold
	}
	if item.MediaURL == nil || strings.TrimSpace(*item.MediaURL) == "" || item.FileSizeBytes <= 0 {
		return models.StorageStateMissing
	}
	return models.StorageStateHot
}

func createStorageArtifactEvent(db *gorm.DB, in storageArtifactEventInput) (*models.MediaStorageArtifactEvent, error) {
	eventType := strings.TrimSpace(in.EventType)
	if eventType == "" {
		eventType = "unknown"
	}
	status := strings.TrimSpace(in.Status)
	if status == "" {
		status = models.StorageArtifactEventStatusSuccess
	}
	tenantID := strings.TrimSpace(in.TenantID)
	if tenantID == "" {
		tenantID = "default"
	}
	recoveryPayload := storageJSON(in.RecoveryPayload)
	if eventType == models.StorageArtifactEventRecoverableDeleted &&
		status == models.StorageArtifactEventStatusSuccess &&
		len(recoveryPayload) == 0 {
		return nil, errors.New("recoverable deletion requires recovery metadata")
	}
	event := models.MediaStorageArtifactEvent{
		TenantID:            tenantID,
		ContentItemID:       in.ContentItemID,
		ParentContentItemID: in.ParentContentItemID,
		EventType:           eventType,
		Status:              status,
		Reason:              in.Reason,
		Trigger:             in.Trigger,
		Source:              in.Source,
		StorageTier:         in.StorageTier,
		OldStorageTier:      in.OldStorageTier,
		OldMediaURL:         in.OldMediaURL,
		NewMediaURL:         in.NewMediaURL,
		OldSizeBytes:        in.OldSizeBytes,
		NewSizeBytes:        in.NewSizeBytes,
		FreedBytes:          in.FreedBytes,
		DeletedBytes:        in.DeletedBytes,
		QualityProfileID:    in.QualityProfileID,
		ArtifactKeys:        storageJSON(in.ArtifactKeys),
		RecoveryPayload:     recoveryPayload,
		Error:               in.Error,
		CreatedBy:           in.CreatedBy,
	}
	if err := db.Create(&event).Error; err != nil {
		return nil, err
	}

	updates := map[string]interface{}{}
	if strings.TrimSpace(in.StorageState) != "" {
		updates["storage_state"] = strings.TrimSpace(in.StorageState)
	}
	if strings.TrimSpace(in.StorageStateReason) != "" {
		reason := strings.TrimSpace(in.StorageStateReason)
		updates["storage_state_reason"] = &reason
	}
	if strings.TrimSpace(in.StorageRecoveryStatus) != "" {
		updates["storage_recovery_status"] = strings.TrimSpace(in.StorageRecoveryStatus)
	}
	switch eventType {
	case models.StorageArtifactEventDeleted, models.StorageArtifactEventRecoverableDeleted:
		now := time.Now().UTC()
		updates["storage_deleted_at"] = &now
	case models.StorageArtifactEventMovedCold, models.StorageArtifactEventReencoded, models.StorageArtifactEventRecovered:
		now := time.Now().UTC()
		updates["storage_last_verified_at"] = &now
	}
	if len(updates) > 0 {
		_ = db.Model(&models.ContentItem{}).Where("public_id = ?", in.ContentItemID).Updates(updates).Error
	}
	return &event, nil
}

func storageRecoveryPayloadForItem(item models.ContentItem) gin.H {
	payload := gin.H{
		"content_item_id":              item.PublicID.String(),
		"tenant_id":                    item.TenantID,
		"type":                         item.Type,
		"status":                       item.Status,
		"source":                       item.Source,
		"title":                        item.Title,
		"source_name":                  item.SourceName,
		"original_url":                 item.OriginalURL,
		"source_feed_url":              item.SourceFeedURL,
		"source_episode_id":            item.SourceEpisodeID,
		"idempotency_key":              item.IdempotencyKey,
		"duration_sec":                 item.DurationSec,
		"published_at":                 item.PublishedAt,
		"transcript_id":                item.TranscriptID,
		"chapter_index":                item.ChapterIndex,
		"chapter_start_ms":             item.ChapterStartMs,
		"chapter_end_ms":               item.ChapterEndMs,
		"duration_bucket":              item.DurationBucket,
		"quality_profile_id":           item.CurrentQualityProfileID,
		"media_version":                item.MediaVersion,
		"feed_visibility":              item.FeedVisibility,
		"is_feed_unit":                 item.IsFeedUnit,
		"media_suitability":            item.MediaSuitability,
		"media_suitability_confidence": item.MediaSuitabilityConfidence,
		"media_suitability_reasons":    item.MediaSuitabilityReasons,
		"storage_state":                item.StorageState,
		"recovery_status":              item.StorageRecoveryStatus,
	}
	if item.ParentContentItemID != nil {
		payload["parent_content_item_id"] = item.ParentContentItemID.String()
	}
	return payload
}

// InternalRecordStorageArtifactEvent handles POST /internal/storage/artifact-events.
func InternalRecordStorageArtifactEvent(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	var req storageArtifactEventRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}
	id, err := uuid.Parse(strings.TrimSpace(req.ContentItemID))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "content_item_id must be a uuid"})
		return
	}
	var parent *uuid.UUID
	if req.ParentContentItemID != nil && strings.TrimSpace(*req.ParentContentItemID) != "" {
		if parsed, err := uuid.Parse(strings.TrimSpace(*req.ParentContentItemID)); err == nil {
			parent = &parsed
		}
	}
	var item models.ContentItem
	tenantID := req.TenantID
	if err := db.Where("public_id = ?", id).First(&item).Error; err == nil {
		if tenantID == "" {
			tenantID = item.TenantID
		}
		if parent == nil {
			parent = item.ParentContentItemID
		}
	}
	event, err := createStorageArtifactEvent(db, storageArtifactEventInput{
		TenantID:              tenantID,
		ContentItemID:         id,
		ParentContentItemID:   parent,
		EventType:             req.EventType,
		Status:                req.Status,
		Reason:                req.Reason,
		Trigger:               req.Trigger,
		Source:                req.Source,
		StorageTier:           req.StorageTier,
		OldStorageTier:        req.OldStorageTier,
		OldMediaURL:           req.OldMediaURL,
		NewMediaURL:           req.NewMediaURL,
		OldSizeBytes:          req.OldSizeBytes,
		NewSizeBytes:          req.NewSizeBytes,
		FreedBytes:            req.FreedBytes,
		DeletedBytes:          req.DeletedBytes,
		QualityProfileID:      req.QualityProfileID,
		ArtifactKeys:          req.ArtifactKeys,
		RecoveryPayload:       req.RecoveryPayload,
		Error:                 req.Error,
		CreatedBy:             req.CreatedBy,
		StorageState:          req.StorageState,
		StorageStateReason:    req.StorageStateReason,
		StorageRecoveryStatus: req.StorageRecoveryStatus,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to record event"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "event_id": event.PublicID.String()})
}

type storageArtifactEventsResponse struct {
	Data  []models.MediaStorageArtifactEvent `json:"data"`
	Total int64                              `json:"total"`
	Limit int                                `json:"limit"`
}

// ListStorageArtifactEvents handles GET /admin/storage/artifact-events.
func ListStorageArtifactEvents(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	limit := atoiDefault(c.Query("limit"), 50)
	if limit > 200 {
		limit = 200
	}
	query := db.Model(&models.MediaStorageArtifactEvent{}).Where("tenant_id = ?", principal.TenantID)
	if raw := strings.TrimSpace(c.Query("content_id")); raw != "" {
		if id, err := uuid.Parse(raw); err == nil {
			query = query.Where("content_item_id = ? OR parent_content_item_id = ?", id, id)
		}
	}
	if eventType := strings.TrimSpace(c.Query("event_type")); eventType != "" {
		query = query.Where("event_type = ?", eventType)
	}
	if status := strings.TrimSpace(c.Query("status")); status != "" {
		query = query.Where("status = ?", status)
	}
	var total int64
	query.Count(&total)
	var rows []models.MediaStorageArtifactEvent
	if err := query.Order("created_at DESC, id DESC").Limit(limit).Find(&rows).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to list artifact events", Code: "LIST_FAILED"})
		return
	}
	c.JSON(http.StatusOK, storageArtifactEventsResponse{Data: rows, Total: total, Limit: limit})
}
