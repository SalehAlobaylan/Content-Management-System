package controllers

import (
	"net/http"

	"content-management-system/src/utils"

	"github.com/gin-gonic/gin"
)

// Only the authenticated inspection route sets this execution context.
const feedInspectionContextKey = "cms.feedInspection"

func suppressFeedAccounting(c *gin.Context) bool {
	return c.GetBool(feedInspectionContextKey) || isFeedIntegritySynthetic(c)
}

func FeedInspectionIdentity() gin.HandlerFunc {
	authenticateConsumer := UserAuthMiddleware()
	return func(c *gin.Context) {
		operator, ok := utils.GetAdminPrincipal(c)
		tenant, err := utils.GetConfiguredPublicTenantID()
		if err != nil {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"message": "Public feed tenant is unavailable"})
			return
		}
		if !ok || !operator.TenantClaimed || operator.TenantID != tenant || suspendedUser(c, operator.UserID) {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"message": "Feed inspection requires an active operator in the public tenant"})
			return
		}
		const consumerHeader = "X-Wahb-Consumer-Authorization"
		consumer, supplied := c.Request.Header[consumerHeader]
		c.Request.Header.Del("Authorization")
		c.Request.Header.Del(consumerHeader)
		c.Set("user_id", "")
		c.Set("tenant_id", "")
		c.Set(feedInspectionContextKey, true)
		if !supplied {
			c.Next()
			return
		}
		if len(consumer) != 1 {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"message": "Invalid consumer credential"})
			return
		}
		c.Request.Header.Set("Authorization", consumer[0])
		authenticateConsumer(c)
	}
}

// FeedInspectionConsumerScope removes credentials after validation and keeps
// the canonical handler's consumer identity separate from the operator.
func FeedInspectionConsumerScope(c *gin.Context) {
	c.Request.Header.Del("Authorization")
	c.Set(utils.AdminPrincipalContextKey, nil)
	if _, err := trustedPublicFeedTenant(c); err != nil {
		c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"message": "Consumer must belong to the public tenant"})
		return
	}
	c.Next()
}
