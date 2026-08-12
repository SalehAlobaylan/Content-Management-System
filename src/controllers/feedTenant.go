package controllers

import (
	"fmt"
	"strings"

	"content-management-system/src/utils"

	"github.com/gin-gonic/gin"
)

// trustedPublicFeedTenant derives feed scope from boot configuration and, for
// authenticated callers, verifies the IAM claim agrees. Query parameters and
// ordinary browser headers are intentionally ignored.
func trustedPublicFeedTenant(c *gin.Context) (string, error) {
	configured, err := utils.GetConfiguredPublicTenantID()
	if err != nil {
		return "", err
	}
	if rawUser, authenticated := c.Get("user_id"); authenticated && strings.TrimSpace(fmt.Sprint(rawUser)) != "" {
		rawTenant, ok := c.Get("tenant_id")
		claimed := strings.TrimSpace(fmt.Sprint(rawTenant))
		if !ok || claimed == "" || claimed != configured {
			return "", fmt.Errorf("authenticated feed tenant does not match the public tenant")
		}
	}
	return configured, nil
}
