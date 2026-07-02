package controllers

import (
	"content-management-system/src/models"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type mediaCirculationOverrideRequest struct {
	SubjectKind  string                 `json:"subject_kind"`
	SubjectID    string                 `json:"subject_id"`
	OverrideType string                 `json:"override_type"`
	Params       map[string]interface{} `json:"params"`
	ExpiresAt    *string                `json:"expires_at"`
	Notes        string                 `json:"notes"`
}

type mediaCircOverrideIndex map[string][]models.MediaCirculationOverride

func ListMediaCirculationOverrides(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var rows []models.MediaCirculationOverride
	q := db.Where("tenant_id = ?", principal.TenantID)
	if subjectKind := strings.TrimSpace(c.Query("subject_kind")); subjectKind != "" {
		q = q.Where("subject_kind = ?", normalizeMediaCircOverrideSubjectKind(subjectKind))
	}
	if overrideType := strings.TrimSpace(c.Query("override_type")); overrideType != "" {
		q = q.Where("override_type = ?", normalizeMediaCircOverrideType(overrideType))
	}
	if !strings.EqualFold(strings.TrimSpace(c.Query("include_expired")), "true") {
		q = q.Where("expires_at IS NULL OR expires_at > ?", time.Now().UTC())
	}
	q.Order("updated_at DESC").Limit(300).Find(&rows)
	c.JSON(http.StatusOK, gin.H{"data": rows})
}

func CreateMediaCirculationOverride(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var req mediaCirculationOverrideRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request: " + err.Error(), Code: "INVALID_REQUEST"})
		return
	}
	subjectKind := normalizeMediaCircOverrideSubjectKind(req.SubjectKind)
	overrideType := normalizeMediaCircOverrideType(req.OverrideType)
	subjectID, err := uuid.Parse(strings.TrimSpace(req.SubjectID))
	if err != nil || subjectKind == "" || overrideType == "" {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid override subject or type", Code: "INVALID_OVERRIDE"})
		return
	}
	if !mediaCircOverrideSubjectExists(db, principal.TenantID, subjectKind, subjectID) {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Override subject not found", Code: "SUBJECT_NOT_FOUND"})
		return
	}
	var expiresAt *time.Time
	if req.ExpiresAt != nil && strings.TrimSpace(*req.ExpiresAt) != "" {
		parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(*req.ExpiresAt))
		if err != nil {
			c.JSON(http.StatusBadRequest, authErrorResponse{Message: "expires_at must be RFC3339", Code: "INVALID_EXPIRES_AT"})
			return
		}
		expiresAt = &parsed
	}
	params := req.Params
	if params == nil {
		params = map[string]interface{}{}
	}
	rawParams, _ := json.Marshal(params)
	row := models.MediaCirculationOverride{
		TenantID:     principal.TenantID,
		SubjectKind:  subjectKind,
		SubjectID:    subjectID,
		OverrideType: overrideType,
		Params:       datatypes.JSON(rawParams),
		ExpiresAt:    expiresAt,
		SetBy:        principal.Email,
		Notes:        strings.TrimSpace(req.Notes),
	}
	if err := db.Create(&row).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to save override", Code: "SAVE_FAILED"})
		return
	}
	invalidateMediaCircHealth(principal.TenantID)
	writeCirculationAudit(db, principal, "media_circulation.override.create", row.PublicID.String(), map[string]interface{}{
		"subject_kind": subjectKind, "subject_id": subjectID.String(), "override_type": overrideType,
	})
	c.JSON(http.StatusOK, gin.H{"data": row})
}

func DeleteMediaCirculationOverride(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	overrideID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid override ID", Code: "INVALID_ID"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	result := db.Where("tenant_id = ? AND public_id = ?", principal.TenantID, overrideID).Delete(&models.MediaCirculationOverride{})
	if result.Error != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to delete override", Code: "DELETE_FAILED"})
		return
	}
	if result.RowsAffected == 0 {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Override not found", Code: "NOT_FOUND"})
		return
	}
	invalidateMediaCircHealth(principal.TenantID)
	writeCirculationAudit(db, principal, "media_circulation.override.delete", overrideID.String(), nil)
	c.JSON(http.StatusOK, gin.H{"success": true})
}

func normalizeMediaCircOverrideSubjectKind(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "source", "item", "family":
		return strings.ToLower(strings.TrimSpace(raw))
	default:
		return ""
	}
}

func normalizeMediaCircOverrideType(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case models.MediaCirculationOverrideNeverArchive,
		models.MediaCirculationOverrideKeepLatestNHot,
		models.MediaCirculationOverridePremiumSource,
		models.MediaCirculationOverrideNoAtomize,
		models.MediaCirculationOverrideEditorialHold:
		return strings.ToLower(strings.TrimSpace(raw))
	default:
		return ""
	}
}

func mediaCircOverrideSubjectExists(db *gorm.DB, tenantID, subjectKind string, subjectID uuid.UUID) bool {
	var count int64
	switch subjectKind {
	case "source":
		db.Model(&models.ContentSource{}).Where("tenant_id = ? AND public_id = ?", tenantID, subjectID).Count(&count)
	case "item", "family":
		db.Model(&models.ContentItem{}).Where("tenant_id = ? AND public_id = ?", tenantID, subjectID).Count(&count)
	default:
		return false
	}
	return count > 0
}

func loadActiveMediaCircOverrides(db *gorm.DB, tenantID string) mediaCircOverrideIndex {
	var rows []models.MediaCirculationOverride
	now := time.Now().UTC()
	db.Where("tenant_id = ?", tenantID).
		Where("expires_at IS NULL OR expires_at > ?", now).
		Find(&rows)
	idx := mediaCircOverrideIndex{}
	for _, row := range rows {
		key := mediaCircOverrideKey(row.SubjectKind, row.SubjectID)
		idx[key] = append(idx[key], row)
	}
	return idx
}

func mediaCircOverrideKey(kind string, id uuid.UUID) string {
	return strings.ToLower(strings.TrimSpace(kind)) + ":" + id.String()
}

func mediaCircOverridesFor(idx mediaCircOverrideIndex, kind string, id uuid.UUID) []models.MediaCirculationOverride {
	if idx == nil {
		return nil
	}
	return idx[mediaCircOverrideKey(kind, id)]
}

func mediaCircHasOverride(idx mediaCircOverrideIndex, kind string, id uuid.UUID, overrideTypes ...string) (models.MediaCirculationOverride, bool) {
	rows := mediaCircOverridesFor(idx, kind, id)
	for _, row := range rows {
		for _, typ := range overrideTypes {
			if row.OverrideType == typ {
				return row, true
			}
		}
	}
	return models.MediaCirculationOverride{}, false
}

func mediaCircOverrideReason(row models.MediaCirculationOverride) string {
	reason := "Human override: " + strings.ReplaceAll(row.OverrideType, "_", " ") + "."
	if strings.TrimSpace(row.Notes) != "" {
		reason += " " + strings.TrimSpace(row.Notes)
	}
	return reason
}
