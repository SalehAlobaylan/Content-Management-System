package utils

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// RequireAdminPermission gates an admin route on a specific resource:action
// permission. It must run AFTER AdminAuthMiddleware, which authenticates the
// IAM-issued JWT and stores the AdminPrincipal in the context.
//
// Authorization is evaluated purely from the token claims — the IAM access
// token already carries the user's flattened role-derived + direct permissions,
// so no database lookup is needed. AdminPrincipal.HasPermission short-circuits
// for the "admin" role and honors "resource:*" / "*:*" wildcards.
func RequireAdminPermission(resource, action string) gin.HandlerFunc {
	required := resource + ":" + action
	return func(c *gin.Context) {
		principal, ok := GetAdminPrincipal(c)
		if !ok {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"message": "Authentication required",
				"code":    "AUTH_REQUIRED",
			})
			return
		}
		if !principal.HasPermission(required) {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"message": "Insufficient permission",
				"code":    "FORBIDDEN",
			})
			return
		}
		c.Next()
	}
}

// RequireAdminRole gates a route on a specific role (e.g. "admin"). Used for the
// few super-admin-only operations that should not be reachable via a granular
// permission grant. Must run AFTER AdminAuthMiddleware.
func RequireAdminRole(role string) gin.HandlerFunc {
	return func(c *gin.Context) {
		principal, ok := GetAdminPrincipal(c)
		if !ok {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"message": "Authentication required",
				"code":    "AUTH_REQUIRED",
			})
			return
		}
		if !principal.HasRole(role) {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"message": "Insufficient role",
				"code":    "FORBIDDEN",
			})
			return
		}
		c.Next()
	}
}
