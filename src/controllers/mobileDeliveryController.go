package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// GetContentPlayback is deliberately narrower than GET /content/:id. It is a
// generation read, not a storage probe: mobile frozen sessions can refresh
// just their active playback contract after a repair without changing ranking.
func GetContentPlayback(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Invalid content ID"})
		return
	}
	var item models.ContentItem
	if err := publicContentQuery(db).Where("content_items.public_id=?", id).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, utils.HTTPError{Code: http.StatusNotFound, Message: "Content not found"})
		return
	}
	if item.ActiveMediaRenditionGenerationID == nil || item.RenditionDigest == "" {
		c.JSON(http.StatusConflict, utils.HTTPError{Code: http.StatusConflict, Message: "No active rendition generation"})
		return
	}
	etag := `W/"` + item.ActiveMediaRenditionGenerationID.String() + `:` + item.RenditionDigest + `"`
	if c.GetHeader("If-None-Match") == etag {
		c.Status(http.StatusNotModified)
		return
	}
	var renditions []map[string]any
	if json.Unmarshal(item.MediaRenditions, &renditions) != nil || len(renditions) == 0 {
		c.JSON(http.StatusConflict, utils.HTTPError{Code: http.StatusConflict, Message: "Active rendition contract is invalid"})
		return
	}
	c.Header("ETag", etag)
	c.JSON(http.StatusOK, gin.H{
		"content_item_id":                item.PublicID,
		"active_rendition_generation_id": item.ActiveMediaRenditionGenerationID,
		"rendition_set_version":          item.RenditionSetVersion,
		"rendition_digest":               item.RenditionDigest,
		"delivery_class":                 item.DeliveryClass,
		"playback_url":                   item.PlaybackURL,
		"playback_type":                  item.PlaybackType,
		"fallback_playback_url":          item.FallbackPlaybackURL,
		"has_video":                      item.HasVideo,
		"media_renditions":               renditions,
	})
}

type playbackHealthRequest struct {
	RenditionGenerationID string `json:"rendition_generation_id"`
	RenditionID           string `json:"rendition_id"`
	FailureClass          string `json:"failure_class"`
	Platform              string `json:"platform"`
	AppBuild              string `json:"app_build"`
	NetworkClass          string `json:"network_class"`
}

func playbackHealthIdentity(c *gin.Context) string {
	userID, sessionID := readIdentity(c)
	identity := strings.TrimSpace(userID)
	if identity == "" {
		identity = strings.TrimSpace(sessionID)
	}
	if identity == "" {
		identity = c.ClientIP()
	}
	sum := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(sum[:])
}

// RecordPlaybackHealth accepts only bounded identifiers and enums. It is a
// signal for inventory and repair prioritisation, never a mobile queue entry.
func RecordPlaybackHealth(c *gin.Context) {
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Invalid content ID"})
		return
	}
	var req playbackHealthRequest
	if c.ShouldBindJSON(&req) != nil || strings.TrimSpace(req.RenditionID) == "" || len(req.RenditionID) > 128 || len(req.AppBuild) > 64 {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Invalid playback health receipt"})
		return
	}
	allowedFailure := map[string]bool{"load": true, "decode": true, "stall": true, "seek": true, "manifest": true, "fallback_exhausted": true}
	allowedPlatform := map[string]bool{"ios": true, "android": true}
	allowedNetwork := map[string]bool{"wifi": true, "cellular": true, "offline": true, "unknown": true, "expensive": true}
	if !allowedFailure[req.FailureClass] || !allowedPlatform[req.Platform] || !allowedNetwork[req.NetworkClass] {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Invalid playback health classification"})
		return
	}
	var item models.ContentItem
	if err := publicContentQuery(db).Where("content_items.public_id=?", id).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, utils.HTTPError{Code: http.StatusNotFound, Message: "Content not found"})
		return
	}
	rawGenerationID := strings.TrimSpace(req.RenditionGenerationID)
	parsedGenerationID, parseErr := uuid.Parse(rawGenerationID)
	if parseErr != nil || item.ActiveMediaRenditionGenerationID == nil || parsedGenerationID != *item.ActiveMediaRenditionGenerationID {
		c.JSON(http.StatusConflict, utils.HTTPError{Code: http.StatusConflict, Message: "Playback generation is no longer active"})
		return
	}
	var renditions []map[string]any
	if json.Unmarshal(item.MediaRenditions, &renditions) != nil {
		c.JSON(http.StatusConflict, utils.HTTPError{Code: http.StatusConflict, Message: "Active rendition contract is invalid"})
		return
	}
	renderedRendition := false
	for _, rendition := range renditions {
		if renditionStringValue(rendition["id"]) == req.RenditionID {
			renderedRendition = true
			break
		}
	}
	if !renderedRendition {
		c.JSON(http.StatusConflict, utils.HTTPError{Code: http.StatusConflict, Message: "Playback rendition is no longer active"})
		return
	}
	identityKey := playbackHealthIdentity(c)
	idempotency := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	if idempotency == "" {
		idempotency = req.FailureClass + ":" + req.RenditionID
	}
	if len(idempotency) > 128 {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Invalid idempotency key"})
		return
	}
	var recent models.MediaPlaybackHealthReceipt
	if db.Where("tenant_id=? AND content_item_id=? AND identity_key=? AND created_at>?", item.TenantID, item.PublicID, identityKey, time.Now().UTC().Add(-time.Minute)).First(&recent).Error == nil {
		c.JSON(http.StatusAccepted, gin.H{"status": "rate_limited"})
		return
	}
	receipt := models.MediaPlaybackHealthReceipt{PublicID: uuid.New(), TenantID: item.TenantID, ContentItemID: item.PublicID, RenditionGenerationID: &parsedGenerationID, RenditionID: req.RenditionID, FailureClass: req.FailureClass, Platform: req.Platform, AppBuild: req.AppBuild, NetworkClass: req.NetworkClass, IdentityKey: identityKey, IdempotencyKey: idempotency}
	if err := db.Create(&receipt).Error; err != nil && err != gorm.ErrDuplicatedKey {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Unable to record playback health"})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"status": "accepted"})
}
