package controllers

import (
	"content-management-system/src/contentstage"
	"content-management-system/src/models"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func getMediaAcquisitionConfig(db *gorm.DB, tenantID string) (models.MediaAcquisitionConfig, error) {
	value := models.MediaAcquisitionConfig{TenantID: tenantID, DefaultMode: models.MediaAcquisitionAutomatic, PodsSourceRunItemLimit: 10, UpdatedBy: "system"}
	err := db.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}}, DoNothing: true}).Create(&value).Error
	if err != nil {
		return value, err
	}
	err = db.Where("tenant_id=?", tenantID).First(&value).Error
	return value, err
}

func GetMediaAcquisitionConfig(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	value, err := getMediaAcquisitionConfig(c.MustGet("db").(*gorm.DB), principal.TenantID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to load media acquisition config", Code: "FETCH_FAILED"})
		return
	}
	c.JSON(http.StatusOK, value)
}

func UpdateMediaAcquisitionConfig(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	var req struct {
		DefaultMode            *string `json:"default_mode"`
		PodsSourceRunItemLimit *int    `json:"pods_source_run_item_limit"`
	}
	if c.ShouldBindJSON(&req) != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request", Code: "INVALID_REQUEST"})
		return
	}
	if req.DefaultMode == nil && req.PodsSourceRunItemLimit == nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "At least one media acquisition setting is required", Code: "INVALID_REQUEST"})
		return
	}
	mode := ""
	if req.DefaultMode != nil {
		mode = strings.ToLower(strings.TrimSpace(*req.DefaultMode))
		if mode != models.MediaAcquisitionAutomatic && mode != models.MediaAcquisitionManual {
			c.JSON(http.StatusBadRequest, authErrorResponse{Message: "default_mode must be automatic or manual", Code: "INVALID_MEDIA_ACQUISITION_MODE"})
			return
		}
	}
	if req.PodsSourceRunItemLimit != nil && (*req.PodsSourceRunItemLimit < 1 || *req.PodsSourceRunItemLimit > 50) {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "pods_source_run_item_limit must be between 1 and 50", Code: "INVALID_PODS_SOURCE_RUN_ITEM_LIMIT"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var value models.MediaAcquisitionConfig
	err := db.Transaction(func(tx *gorm.DB) error {
		var err error
		value, err = getMediaAcquisitionConfig(tx, principal.TenantID)
		if err != nil {
			return err
		}
		if req.DefaultMode != nil {
			value.DefaultMode = mode
		}
		if req.PodsSourceRunItemLimit != nil {
			value.PodsSourceRunItemLimit = *req.PodsSourceRunItemLimit
		}
		value.UpdatedBy = principal.Email
		if err := tx.Save(&value).Error; err != nil {
			return err
		}
		if req.DefaultMode != nil {
			var inherited []models.ContentSource
			if err := tx.Where("tenant_id=? AND category=? AND media_acquisition_mode IS NULL", principal.TenantID, models.SourceCategoryMedia).Find(&inherited).Error; err != nil {
				return err
			}
			for _, source := range inherited {
				if err := contentstage.ReconcileSourceAcquisitionPolicy(tx, source, mode); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to update media acquisition config", Code: "UPDATE_FAILED"})
		return
	}
	c.JSON(http.StatusOK, value)
}

func RequestMediaAcquisition(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid content ID", Code: "INVALID_ID"})
		return
	}
	result, err := contentstage.RequestMediaAcquisition(c.MustGet("db").(*gorm.DB), principal.TenantID, id, principal.Email)
	if err != nil {
		status := http.StatusConflict
		if err == gorm.ErrRecordNotFound {
			status = http.StatusNotFound
		}
		c.JSON(status, authErrorResponse{Message: err.Error(), Code: "MEDIA_ACQUISITION_REJECTED"})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"data": result, "message": "Media acquisition request accepted"})
}

func BulkRequestMediaAcquisition(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	var req struct {
		ContentIDs []string `json:"content_ids"`
	}
	if c.ShouldBindJSON(&req) != nil || len(req.ContentIDs) == 0 || len(req.ContentIDs) > 100 {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "content_ids must contain 1 to 100 items", Code: "INVALID_REQUEST"})
		return
	}
	type bulkResult struct {
		ContentItemID string `json:"content_item_id"`
		State         string `json:"state,omitempty"`
		Disposition   string `json:"disposition,omitempty"`
		Error         string `json:"error,omitempty"`
	}
	results := make([]bulkResult, 0, len(req.ContentIDs))
	seen := map[uuid.UUID]bool{}
	db := c.MustGet("db").(*gorm.DB)
	for _, raw := range req.ContentIDs {
		id, err := uuid.Parse(strings.TrimSpace(raw))
		if err != nil || seen[id] {
			results = append(results, bulkResult{ContentItemID: raw, Error: "invalid or duplicate content ID"})
			continue
		}
		seen[id] = true
		result, err := contentstage.RequestMediaAcquisition(db, principal.TenantID, id, principal.Email)
		if err != nil {
			results = append(results, bulkResult{ContentItemID: raw, Error: err.Error()})
			continue
		}
		results = append(results, bulkResult{ContentItemID: raw, State: result.State, Disposition: result.Disposition})
	}
	c.JSON(http.StatusAccepted, gin.H{"data": results, "message": "Bulk media acquisition evaluated"})
}
