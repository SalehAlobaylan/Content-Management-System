package utils

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// AdminAuthMiddleware validates JWT tokens for /admin routes and injects a
// claim-based principal. Authentication validity is claim-driven and does not
// require admin_users table lookup.
func AdminAuthMiddleware(_ *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"message": "Authentication required", "code": "AUTH_REQUIRED"})
			c.Abort()
			return
		}

		if !strings.HasPrefix(strings.ToLower(authHeader), "bearer ") {
			c.JSON(http.StatusUnauthorized, gin.H{"message": "Invalid authentication token", "code": "INVALID_TOKEN"})
			c.Abort()
			return
		}

		parts := strings.SplitN(authHeader, " ", 2)
		tokenString := ""
		if len(parts) == 2 {
			tokenString = strings.TrimSpace(parts[1])
		}
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
			switch err {
			case ErrTokenExpired:
				c.JSON(http.StatusUnauthorized, gin.H{"message": "Token has expired", "code": "TOKEN_EXPIRED"})
			case ErrTokenSignatureInvalid:
				c.JSON(http.StatusUnauthorized, gin.H{"message": "Invalid token signature", "code": "INVALID_SIGNATURE"})
			default:
				c.JSON(http.StatusUnauthorized, gin.H{"message": "Invalid authentication token", "code": "INVALID_TOKEN"})
			}
			c.Abort()
			return
		}

		principal := BuildAdminPrincipal(claims)
		c.Set(AdminPrincipalContextKey, principal)
		c.Next()
	}
}
