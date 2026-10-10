package controllers

import (
	"errors"
	"net/http"

	"content-management-system/src/models"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

func validateHistoricalManifest(manifest models.RetentionHistoricalManifest) error {
	ids, err := historicalIDs(manifest.ContentIDs)
	if err != nil {
		return err
	}
	stories, err := historicalIDs(manifest.StoryIDs)
	if err != nil || len(ids) == 0 || len(uniqueUUIDs(ids)) != len(ids) || len(uniqueUUIDs(stories)) != len(stories) || len(ids) != manifest.ContentCount || len(stories) != manifest.StoryCount || historicalManifestHash(manifest.TenantID, manifest.Timezone, ids, stories) != manifest.ManifestHash {
		return errors.New("historical manifest evidence is inconsistent")
	}
	return nil
}

// Exact selection is exposed only to native, tenant-authorized review.
// Return frozen evidence without provider keys or model-facing projections.
func GetRetentionActionReview(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid action id"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var action models.RetentionAction
	if db.Where("tenant_id=? AND public_id=?", principal.TenantID, id).First(&action).Error != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "retention action not found"})
		return
	}
	var run models.RetentionRun
	if db.Where("tenant_id=? AND id=?", principal.TenantID, action.RunID).First(&run).Error != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "action run evidence is unavailable"})
		return
	}
	var manifest interface{}
	var hash string
	switch action.ActionClass {
	case retentionActionPrepareNewsCompaction, retentionActionExecuteNewsCompaction:
		var value models.RetentionCompactionManifest
		err = db.Where("tenant_id=? AND action_id=?", principal.TenantID, action.ID).First(&value).Error
		if value.RunID != action.RunID {
			err = errors.New("manifest run mismatch")
		}
		if err == nil {
			var payload retentionManifestPayload
			payload, err = decodeRetentionManifest(value)
			if err == nil {
				stories, anchors, protected, retire := compactManifestIDs(payload)
				value.StoryIDs, value.AnchorContentIDs, value.ProtectedContentIDs, value.RetireContentIDs = idsJSON(stories), idsJSON(anchors), idsJSON(protected), idsJSON(retire)
				value.StoryCount, value.AnchorCount, value.ProtectedCount, value.RetireCount = len(stories), len(anchors), len(protected), len(retire)
			}
		}
		manifest, hash = value, value.ManifestHash
	case models.RetentionActionPrepareHistoricalPurge, models.RetentionActionExecuteHistoricalPurge:
		var value models.RetentionHistoricalManifest
		err = db.Where("tenant_id=? AND action_id=?", principal.TenantID, action.ID).First(&value).Error
		if value.RunID != action.RunID {
			err = errors.New("manifest run mismatch")
		}
		if err == nil {
			err = validateHistoricalManifest(value)
		}
		manifest, hash = value, value.ManifestHash
	default:
		RetentionCapabilityRetired(c)
		return
	}
	if err != nil || action.ManifestHash == nil || *action.ManifestHash != hash {
		c.JSON(http.StatusConflict, gin.H{"error": "frozen manifest evidence is unavailable or inconsistent"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{
		"action_id": action.PublicID, "run_id": run.PublicID, "action_class": action.ActionClass,
		"outcome": action.Outcome, "approved_at": action.ApprovedAt, "approved_by": action.ApprovedBy,
		"manifest": manifest,
	}})
}
