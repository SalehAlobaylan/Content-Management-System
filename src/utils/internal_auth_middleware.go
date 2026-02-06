package utils

import (
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
)

// InternalAuthMiddleware validates service-to-service token for /internal/* routes
func InternalAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		expected := os.Getenv("CMS_SERVICE_TOKEN")
		if expected == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "CMS_SERVICE_TOKEN not configured",
			})
			return
		}

		authHeader := c.GetHeader("Authorization")
		if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "Missing service token",
			})
			return
		}

		token := strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer "))
		if token != expected {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"error": "Invalid service token",
			})
			return
		}

		c.Next()
	}
}
