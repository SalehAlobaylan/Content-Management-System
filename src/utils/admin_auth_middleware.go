package utils

import (
	"content-management-system/src/models"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// AdminAuthMiddleware validates JWT tokens for /admin routes
func AdminAuthMiddleware(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"message": "Authentication required", "code": "AUTH_REQUIRED"})
			c.Abort()
			return
		}

		if !strings.HasPrefix(authHeader, "Bearer ") {
			c.JSON(http.StatusUnauthorized, gin.H{"message": "Invalid authentication token", "code": "INVALID_TOKEN"})
			c.Abort()
			return
		}

		tokenString := strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer "))
		if tokenString == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"message": "Invalid authentication token", "code": "INVALID_TOKEN"})
			c.Abort()
			return
		}

		secret, err := GetJWTSecret()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"message": "Internal server error", "code": "INTERNAL_SERVER_ERROR"})
			c.Abort()
			return
		}

		claims, err := ParseJWT(tokenString, secret)
		if err != nil {
			if err == ErrTokenExpired {
				c.JSON(http.StatusUnauthorized, gin.H{"message": "Token has expired", "code": "TOKEN_EXPIRED"})
			} else if err == ErrTokenSignatureInvalid {
				c.JSON(http.StatusUnauthorized, gin.H{"message": "Invalid token signature", "code": "INVALID_SIGNATURE"})
			} else {
				c.JSON(http.StatusUnauthorized, gin.H{"message": "Invalid authentication token", "code": "INVALID_TOKEN"})
			}
			c.Abort()
			return
		}

		var user models.AdminUser
		if err := db.Where("public_id = ?", claims.RegisteredClaims.Subject).First(&user).Error; err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"message": "Unauthorized", "code": "UNAUTHORIZED"})
			c.Abort()
			return
		}
		if !user.IsActive {
			c.JSON(http.StatusForbidden, gin.H{"message": "Forbidden", "code": "FORBIDDEN"})
			c.Abort()
			return
		}

		c.Set("admin_user", user)
		c.Next()
	}
}
