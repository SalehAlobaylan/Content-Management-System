package controllers

// Preserve tenant-scoped historical owner envelopes after bridge retirement.
import (
	"content-management-system/src/models"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"net/http"
)

func ListRetentionOwnerRequests(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var rows []models.RetentionOwnerRequest
	if err := db.Where("tenant_id=?", principal.TenantID).Order("created_at DESC").Limit(100).Find(&rows).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if len(rows) > 0 {
		actionIDs := make([]uint, 0, len(rows))
		for _, row := range rows {
			actionIDs = append(actionIDs, row.ActionID)
		}
		var actions []models.RetentionAction
		if err := db.Where("id IN ?", actionIDs).Find(&actions).Error; err == nil {
			public := map[uint]uuid.UUID{}
			for _, action := range actions {
				public[action.ID] = action.PublicID
			}
			for index := range rows {
				if id, ok := public[rows[index].ActionID]; ok {
					rows[index].ActionPublicID = &id
				}
			}
		}
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"items": rows}})
}
