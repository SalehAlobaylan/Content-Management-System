package controllers

import (
	"content-management-system/src/utils"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

func requireAdminPrincipal(c *gin.Context) (utils.AdminPrincipal, bool) {
	principal, ok := utils.GetAdminPrincipal(c)
	if !ok {
		c.JSON(http.StatusUnauthorized, authErrorResponse{
			Message: "Unauthorized",
			Code:    "UNAUTHORIZED",
		})
		return utils.AdminPrincipal{}, false
	}

	tenantID := strings.TrimSpace(principal.TenantID)
	if tenantID == "" {
		c.JSON(http.StatusForbidden, authErrorResponse{
			Message: "Missing tenant scope",
			Code:    "TENANT_REQUIRED",
		})
		return utils.AdminPrincipal{}, false
	}

	return principal, true
}
