package controllers

import (
	"content-management-system/src/models"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// =============================================================================
// Internal endpoints — Aggregation → CMS
//
// Phase 7 surface: the only things Aggregation needs from CMS for the Quality
// system are
//   - resolve a profile for (tenant, source_type) on every ingest job
//   - fetch a profile by id (used by the re-encode worker invoked from Storage)
//   - patch per-item quality fields after a re-encode
// All gated by InternalAuthMiddleware (CMS_SERVICE_TOKEN).
// =============================================================================

// InternalGetQualityProfile handles GET /internal/quality/profiles/:id
func InternalGetQualityProfile(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid id"})
		return
	}
	var p models.QualityProfile
	if err := db.First(&p, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Profile not found"})
		return
	}
	c.JSON(http.StatusOK, p)
}

// InternalResolveQualityProfile handles GET /internal/quality/profiles/resolve?tenant_id=X&source_type=Y&preset_key=storage-saver
//
// Returns the most-specific matching profile or an explicit safe-default
// resolution when no rung matches. Aggregation's media worker calls this on every
// fresh job; a 60-second per-process cache lives on the Aggregation side.
func InternalResolveQualityProfile(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	tenantID := strings.TrimSpace(c.Query("tenant_id"))
	sourceType := strings.ToUpper(strings.TrimSpace(c.Query("source_type")))
	presetKey := strings.TrimSpace(c.Query("preset_key"))

	profile, matched := resolveProfileWithPreset(db, tenantID, sourceType, presetKey)
	if profile == nil {
		c.JSON(http.StatusOK, gin.H{"profile": nil, "matched_on": "none", "used_default": true})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"profile":      profile,
		"matched_on":   matched,
		"used_default": false,
	})
}

// =============================================================================
// Per-item quality update (bumps media_version, swaps URL & bitrate)
// Called by the re-encode worker after a successful encode.
// =============================================================================

type internalUpdateItemQualityRequest struct {
	MediaURL                *string `json:"media_url"`
	FileSizeBytes           *int64  `json:"file_size_bytes"`
	CurrentBitrateKbps      *int    `json:"current_bitrate_kbps"`
	CurrentQualityProfileID *uint   `json:"current_quality_profile_id"`
	BumpVersion             bool    `json:"bump_version"`
	OldMediaURL             *string `json:"old_media_url"`
	OldSizeBytes            *int64  `json:"old_size_bytes"`
	OldStorageKey           *string `json:"old_storage_key"`
	NewStorageKey           *string `json:"new_storage_key"`
	NewManifestID           string  `json:"new_manifest_id"`
	NewProducerEventID      string  `json:"new_producer_event_id"`
	NewFenceToken           string  `json:"new_fence_token"`
	EventReason             *string `json:"event_reason"`
}

func qualityVersionedObjectKey(contentItemID uuid.UUID, version int) string {
	if version <= 1 {
		return fmt.Sprintf("content/%s/processed.mp4", contentItemID)
	}
	return fmt.Sprintf("content/%s/processed.v%d.mp4", contentItemID, version)
}

// InternalUpdateContentItemQuality handles PATCH /internal/content-items/:id/quality
func InternalUpdateContentItemQuality(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid id"})
		return
	}
	var req internalUpdateItemQualityRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}
	var item models.ContentItem
	var newManifestID, producerEventID, fenceToken uuid.UUID
	alreadyApplied := false
	if err := db.Where("public_id = ?", id).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "content item not found"})
		return
	}
	expectedUpdatedAt := item.UpdatedAt
	if req.BumpVersion {
		var idErr, producerErr, fenceErr error
		newManifestID, idErr = uuid.Parse(strings.TrimSpace(req.NewManifestID))
		producerEventID, producerErr = uuid.Parse(strings.TrimSpace(req.NewProducerEventID))
		fenceToken, fenceErr = uuid.Parse(strings.TrimSpace(req.NewFenceToken))
		if idErr != nil || producerErr != nil || fenceErr != nil || req.NewStorageKey == nil || strings.TrimSpace(*req.NewStorageKey) == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "quality re-encode requires its exact verified artifact identity"})
			return
		}
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := checkContentLifecycleMutation(tx, item); err != nil {
			return err
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ?", id).First(&item).Error; err != nil {
			return err
		}
		if !item.UpdatedAt.Equal(expectedUpdatedAt) {
			return fmt.Errorf("content changed while its quality update was being prepared")
		}
		if item.RetiredPayloadAt != nil || item.Status == models.ContentStatusArchived {
			return fmt.Errorf("content identity is permanently retired")
		}
		if req.BumpVersion {
			var manifest models.MediaArtifactManifest
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=? AND content_item_id=? AND object_key=? AND producer_event_id=? AND fence_token=?", item.TenantID, newManifestID, item.PublicID, strings.TrimSpace(*req.NewStorageKey), producerEventID, fenceToken).First(&manifest).Error; err != nil {
				return fmt.Errorf("verified quality artifact is unavailable")
			}
			itemTier := "primary"
			if item.StorageTier != nil && strings.TrimSpace(*item.StorageTier) != "" {
				itemTier = strings.TrimSpace(*item.StorageTier)
			}
			if manifest.ArtifactRole != "playback_mp4" || manifest.CreatorRole != "aggregation_quality_worker" || manifest.ContentType != "video/mp4" || len(manifest.SHA256) != 64 || manifest.SizeBytes <= 0 || req.FileSizeBytes == nil || manifest.SizeBytes != *req.FileSizeBytes || manifest.StorageTier != itemTier || (manifest.State != "verified" && manifest.State != "active") || req.MediaURL == nil || manifest.PublicURL != *req.MediaURL {
				return fmt.Errorf("quality artifact does not match the requested media pointer")
			}
			nextKey := qualityVersionedObjectKey(item.PublicID, item.MediaVersion+1)
			currentKey := qualityVersionedObjectKey(item.PublicID, item.MediaVersion)
			if strings.TrimSpace(*req.NewStorageKey) == nextKey && manifest.State == "verified" {
				// A fresh output must target exactly the next version. The row lock
				// makes two workers encoding from the same version serialize; only
				// the first may advance the pointer.
			} else if strings.TrimSpace(*req.NewStorageKey) == currentKey && manifest.State == "active" && item.MediaURL != nil && *item.MediaURL == *req.MediaURL {
				// The first PATCH may have committed while its response was lost.
				// Replay is safe only when both the versioned key and current pointer
				// already identify this exact active manifest.
				alreadyApplied = true
			} else {
				return fmt.Errorf("quality artifact version is stale or not the exact next version")
			}
		}
		if req.MediaURL != nil {
			item.MediaURL = req.MediaURL
		}
		if req.FileSizeBytes != nil {
			item.FileSizeBytes = *req.FileSizeBytes
		}
		if req.CurrentBitrateKbps != nil {
			v := *req.CurrentBitrateKbps
			item.CurrentBitrateKbps = &v
		}
		if req.CurrentQualityProfileID != nil {
			v := *req.CurrentQualityProfileID
			item.CurrentQualityProfileID = &v
		}
		if req.BumpVersion && !alreadyApplied {
			item.MediaVersion++
		}
		now := time.Now().UTC()
		stateReason := "reencoded"
		item.StorageState = models.StorageStateReencoded
		item.StorageStateReason = &stateReason
		item.StorageRecoveryStatus = models.StorageRecoveryRecoverable
		item.StorageLastVerifiedAt = &now
		if err := tx.Save(&item).Error; err != nil {
			return err
		}
		if req.BumpVersion && !alreadyApplied {
			changed := tx.Model(&models.MediaArtifactManifest{}).Where("tenant_id=? AND public_id=? AND content_item_id=? AND object_key=? AND producer_event_id=? AND fence_token=? AND state='verified'", item.TenantID, newManifestID, item.PublicID, strings.TrimSpace(*req.NewStorageKey), producerEventID, fenceToken).
				Updates(map[string]interface{}{"state": "active", "updated_at": now})
			if changed.Error != nil {
				return changed.Error
			}
			if changed.RowsAffected == 0 {
				var stillActive int64
				if err := tx.Model(&models.MediaArtifactManifest{}).Where("tenant_id=? AND public_id=? AND content_item_id=? AND producer_event_id=? AND fence_token=? AND state='active'", item.TenantID, newManifestID, item.PublicID, producerEventID, fenceToken).Count(&stillActive).Error; err != nil || stillActive != 1 {
					return fmt.Errorf("quality artifact activation did not persist")
				}
			}
		}
		return nil
	}); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "quality update was rejected by the content or artifact ownership fence"})
		return
	}
	if alreadyApplied {
		c.JSON(http.StatusOK, gin.H{"success": true, "media_version": item.MediaVersion})
		return
	}
	oldSize := int64(0)
	if req.OldSizeBytes != nil {
		oldSize = *req.OldSizeBytes
	}
	newSize := item.FileSizeBytes
	freed := oldSize - newSize
	if freed < 0 {
		freed = 0
	}
	var keys []string
	if req.OldStorageKey != nil && strings.TrimSpace(*req.OldStorageKey) != "" {
		keys = append(keys, *req.OldStorageKey)
	}
	if req.NewStorageKey != nil && strings.TrimSpace(*req.NewStorageKey) != "" {
		keys = append(keys, *req.NewStorageKey)
	}
	eventReason := "Quality re-encode completed"
	if req.EventReason != nil && strings.TrimSpace(*req.EventReason) != "" {
		eventReason = *req.EventReason
	}
	_, _ = createStorageArtifactEvent(db, storageArtifactEventInput{
		TenantID:              item.TenantID,
		ContentItemID:         item.PublicID,
		ParentContentItemID:   item.ParentContentItemID,
		EventType:             models.StorageArtifactEventReencoded,
		Status:                models.StorageArtifactEventStatusSuccess,
		Reason:                eventReason,
		Trigger:               "quality_reencode",
		Source:                "aggregation",
		StorageTier:           tierFromItem(item),
		OldMediaURL:           stringValue(req.OldMediaURL),
		NewMediaURL:           stringValue(item.MediaURL),
		OldSizeBytes:          oldSize,
		NewSizeBytes:          newSize,
		FreedBytes:            freed,
		QualityProfileID:      item.CurrentQualityProfileID,
		ArtifactKeys:          keys,
		RecoveryPayload:       storageRecoveryPayloadForItem(item),
		StorageState:          models.StorageStateReencoded,
		StorageStateReason:    "reencoded",
		StorageRecoveryStatus: models.StorageRecoveryRecoverable,
	})
	c.JSON(http.StatusOK, gin.H{
		"success":       true,
		"media_version": item.MediaVersion,
	})
}
