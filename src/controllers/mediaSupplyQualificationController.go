package controllers

import (
	"net/http"
	"strconv"
	"strings"

	"content-management-system/src/models"
	operatorpkg "content-management-system/src/operator"
	"content-management-system/src/supply"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// Cases are immutable owner/Observe evidence. Administrators can compare a
// prediction with its terminal proof; this endpoint grants no execution power.
func ListMediaSupplyQualificationCases(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	key := strings.TrimSpace(c.Query("action_key"))
	if _, ok := supply.QualificationVersions(key); !ok {
		c.JSON(400, gin.H{"message": "Registered action_key is required"})
		return
	}
	limit := 25
	if raw := c.Query("limit"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 100 {
			c.JSON(400, gin.H{"message": "Invalid limit"})
			return
		}
		limit = value
	}
	db := c.MustGet("db").(*gorm.DB)
	query := db.Where("tenant_id=? AND action_key=?", principal.TenantID, key)
	if raw := c.Query("after"); raw != "" {
		after, err := uuid.Parse(raw)
		if err != nil {
			c.JSON(400, gin.H{"message": "Invalid cursor"})
			return
		}
		var anchor models.MediaSupplyQualificationCase
		if db.Where("tenant_id=? AND action_key=? AND public_id=?", principal.TenantID, key, after).First(&anchor).Error != nil {
			c.JSON(400, gin.H{"message": "Invalid cursor"})
			return
		}
		query = query.Where("(created_at,public_id)<(?,?)", anchor.CreatedAt, anchor.PublicID)
	}
	var cases []models.MediaSupplyQualificationCase
	if query.Order("created_at DESC,public_id DESC").Limit(limit+1).Find(&cases).Error != nil {
		c.JSON(503, gin.H{"message": "Qualification cases are unavailable"})
		return
	}
	var cursor *string
	if len(cases) > limit {
		cases = cases[:limit]
		value := cases[len(cases)-1].PublicID.String()
		cursor = &value
	}
	c.JSON(200, gin.H{"schema_version": supply.MediaSupplyQualificationRubricVersion, "items": cases, "next_cursor": cursor})
}

func freshSupplyQualificationAdmin(c *gin.Context) (string, string, bool) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return "", "", false
	}
	client, err := operatorpkg.NewIAMAccessClientFromEnv()
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Current admin access cannot be verified"})
		return "", "", false
	}
	access, err := client.Snapshot(c.Request.Context(), principal.UserID, principal.TenantID)
	if err != nil || !access.IsAdmin || !access.HasPermission("aggregation:manage") {
		c.JSON(http.StatusForbidden, gin.H{"message": "Current qualification authority is unavailable"})
		return "", "", false
	}
	return access.UserID, access.AccessVersion, true
}

// DecideMediaSupplyQualificationCase records only an independently refreshed
// human comparison. It cannot alter verifier outcomes or case provenance.
func DecideMediaSupplyQualificationCase(c *gin.Context) {
	actor, accessVersion, ok := freshSupplyQualificationAdmin(c)
	if !ok {
		return
	}
	principal, _ := requireAdminPrincipal(c)
	var body struct {
		Decision string `json:"decision"`
	}
	if decodeStrictJSON(c, &body) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Invalid qualification decision"})
		return
	}
	decision, err := supply.RecordQualificationHumanDecision(c.Request.Context(), c.MustGet("db").(*gorm.DB), principal.TenantID, c.Param("id"), strings.TrimSpace(body.Decision), actor, accessVersion)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"message": "Qualification decision preconditions changed"})
		return
	}
	c.JSON(http.StatusCreated, decision)
}

func CreateMediaSupplyQualificationReport(c *gin.Context) {
	_, _, ok := freshSupplyQualificationAdmin(c)
	if !ok {
		return
	}
	key := strings.TrimSpace(c.Param("key"))
	versions, found := supply.QualificationVersions(key)
	if !found {
		c.JSON(http.StatusNotFound, gin.H{"message": "Qualification action is not registered"})
		return
	}
	principal, _ := requireAdminPrincipal(c)
	report, err := supply.CreateQualificationReport(c.Request.Context(), c.MustGet("db").(*gorm.DB), principal.TenantID, key, versions.ActionVersion, versions.AdapterVersion, versions.VerifierVersion, versions.SchemaVersion, versions.PolicyVersion)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"message": "Qualification report could not be created"})
		return
	}
	c.JSON(http.StatusCreated, report)
}

func SignoffMediaSupplyQualificationReport(c *gin.Context) {
	actor, accessVersion, ok := freshSupplyQualificationAdmin(c)
	if !ok {
		return
	}
	principal, _ := requireAdminPrincipal(c)
	signoff, err := supply.AddQualificationSignoff(c.Request.Context(), c.MustGet("db").(*gorm.DB), principal.TenantID, c.Param("id"), strings.ToLower(strings.TrimSpace(c.Param("role"))), actor, accessVersion)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"message": "Qualification signoff preconditions changed"})
		return
	}
	c.JSON(http.StatusCreated, signoff)
}

func SealMediaSupplyQualificationReport(c *gin.Context) {
	_, _, ok := freshSupplyQualificationAdmin(c)
	if !ok {
		return
	}
	key, err := supply.QualificationSigningKeyFromEnv()
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Qualification signing is unavailable"})
		return
	}
	principal, _ := requireAdminPrincipal(c)
	report, err := supply.SealQualificationReport(c.Request.Context(), c.MustGet("db").(*gorm.DB), principal.TenantID, c.Param("id"), key)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"message": "Qualification report gates did not pass"})
		return
	}
	c.JSON(http.StatusOK, report)
}

func PromoteMediaSupplyQualificationReport(c *gin.Context) {
	actor, accessVersion, ok := freshSupplyQualificationAdmin(c)
	if !ok {
		return
	}
	principal, _ := requireAdminPrincipal(c)
	key, err := supply.QualificationSigningKeyFromEnv()
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Qualification signing is unavailable"})
		return
	}
	promotion, err := supply.PromoteQualifiedSupplyAction(c.Request.Context(), c.MustGet("db").(*gorm.DB), principal.TenantID, c.Param("id"), actor, accessVersion, key)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"message": "Safe Auto promotion gates did not pass"})
		return
	}
	c.JSON(http.StatusCreated, promotion)
}

func DemoteMediaSupplyActionPromotion(c *gin.Context) {
	actor, _, ok := freshSupplyQualificationAdmin(c)
	if !ok {
		return
	}
	principal, _ := requireAdminPrincipal(c)
	if err := supply.DemoteSupplyActionPromotion(c.Request.Context(), c.MustGet("db").(*gorm.DB), principal.TenantID, strings.TrimSpace(c.Param("key")), actor, "admin_disable_only_demotion", false); err != nil {
		c.JSON(http.StatusConflict, gin.H{"message": "Active promotion was not found"})
		return
	}
	c.Status(http.StatusNoContent)
}

func ListMediaSupplyQualificationState(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var reports []models.MediaSupplyQualificationReport
	var promotions []models.MediaSupplyActionPromotion
	if err := db.Where("tenant_id=?", principal.TenantID).Order("created_at DESC").Limit(100).Find(&reports).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Qualification reports are unavailable"})
		return
	}
	if err := db.Where("tenant_id=?", principal.TenantID).Order("created_at DESC").Limit(100).Find(&promotions).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Qualification promotions are unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"schema_version": supply.MediaSupplyQualificationRubricVersion, "safe_auto_default": "disabled", "reports": reports, "promotions": promotions})
}
