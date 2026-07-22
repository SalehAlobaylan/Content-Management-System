package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"net/http"
	"net/url"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func canonicalContentSourceKey(item models.ContentItem) string {
	if item.SourceFeedURL != nil {
		rawURL := strings.TrimSpace(*item.SourceFeedURL)
		if parsed, err := url.Parse(rawURL); err == nil {
			if host := strings.ToLower(strings.TrimPrefix(parsed.Hostname(), "www.")); host != "" {
				return "feed:" + host
			}
		}
		if parsed, err := url.Parse("//" + strings.TrimPrefix(rawURL, "//")); err == nil {
			if host := strings.ToLower(strings.TrimPrefix(parsed.Hostname(), "www.")); host != "" {
				return "feed:" + host
			}
		}
	}
	if item.SourceName != nil {
		name := strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(*item.SourceName))), " ")
		if name != "" {
			return "name:" + name
		}
	}
	return ""
}

// MutePreferenceSource derives the source key from an actual content item so
// clients cannot create opaque or cross-tenant preference rows.
func MutePreferenceSource(c *gin.Context) {
	setSourcePreferenceMute(c, true)
}

// UnmutePreferenceSource removes the same server-derived exclusion. The
// content ID is intentionally used again instead of accepting a raw key.
func UnmutePreferenceSource(c *gin.Context) {
	setSourcePreferenceMute(c, false)
}

// UnmutePreferenceSourceByKey is the recovery route for a source that is no
// longer eligible to appear in the user's feed. Source keys are only returned
// from that same user's preferences response and the delete remains scoped to
// their authenticated identity.
func UnmutePreferenceSourceByKey(c *gin.Context) {
	uid, ok := authedUserID(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, utils.HTTPError{Code: http.StatusUnauthorized, Message: "Authentication required"})
		return
	}
	key := strings.TrimSpace(c.Query("source_key"))
	if len(key) == 0 || len(key) > 320 || (!strings.HasPrefix(key, "feed:") && !strings.HasPrefix(key, "name:")) {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Invalid source key"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	if err := db.Where("tenant_id = ? AND user_id = ? AND source_key = ?", "default", uid, key).Delete(&models.UserSourcePref{}).Error; err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Failed to remove source preference"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"source_key": key, "state": "active"})
}

func setSourcePreferenceMute(c *gin.Context, muted bool) {
	uid, ok := authedUserID(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, utils.HTTPError{Code: http.StatusUnauthorized, Message: "Authentication required"})
		return
	}
	contentID, err := uuid.Parse(c.Param("content_id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, utils.HTTPError{Code: http.StatusBadRequest, Message: "Invalid content id"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var item models.ContentItem
	if err := publicContentQuery(db).Where("public_id = ?", contentID).First(&item).Error; err != nil {
		c.JSON(http.StatusNotFound, utils.HTTPError{Code: http.StatusNotFound, Message: "Content item not found"})
		return
	}
	key := canonicalContentSourceKey(item)
	if key == "" {
		c.JSON(http.StatusUnprocessableEntity, utils.HTTPError{Code: http.StatusUnprocessableEntity, Message: "Content has no stable source identity"})
		return
	}
	if muted {
		pref := models.UserSourcePref{TenantID: "default", UserID: uid, SourceKey: key, State: "muted"}
		if err := db.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}, {Name: "user_id"}, {Name: "source_key"}}, DoUpdates: clause.AssignmentColumns([]string{"state", "updated_at"})}).Create(&pref).Error; err != nil {
			c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Failed to save source preference"})
			return
		}
	} else if err := db.Where("tenant_id = ? AND user_id = ? AND source_key = ?", "default", uid, key).Delete(&models.UserSourcePref{}).Error; err != nil {
		c.JSON(http.StatusInternalServerError, utils.HTTPError{Code: http.StatusInternalServerError, Message: "Failed to remove source preference"})
		return
	}
	state := "active"
	if muted {
		state = "muted"
	}
	c.JSON(http.StatusOK, gin.H{"source_key": key, "state": state})
}
