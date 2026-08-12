package controllers

import (
	"net/http"
	"strings"

	"content-management-system/src/models"
	operatorpkg "content-management-system/src/operator"
	"content-management-system/src/supply"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

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
