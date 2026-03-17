package controllers

import (
	"content-management-system/src/utils"
	"net/http"

	"github.com/gin-gonic/gin"
)

type meResponse struct {
	ID          string   `json:"id"`
	Email       string   `json:"email"`
	Role        string   `json:"role"`
	Permissions []string `json:"permissions"`
}

type authErrorResponse struct {
	Message string `json:"message"`
	Code    string `json:"code,omitempty"`
	Details any    `json:"details,omitempty"`
}

func respondAuthError(c *gin.Context, status int, message string, code string) {
	c.JSON(status, authErrorResponse{
		Message: message,
		Code:    code,
	})
}

// AdminMe handles GET /admin/me
// Reads claims from the IAM-issued JWT (set by AdminAuthMiddleware).
func AdminMe(c *gin.Context) {
	principal, ok := utils.GetAdminPrincipal(c)
	if !ok {
		respondAuthError(c, http.StatusUnauthorized, "Unauthorized", "UNAUTHORIZED")
		return
	}

	c.JSON(http.StatusOK, meResponse{
		ID:          principal.UserID,
		Email:       principal.Email,
		Role:        principal.Role,
		Permissions: principal.Permissions,
	})
}
