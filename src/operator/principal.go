package operator

import (
	"fmt"
	"strings"

	"content-management-system/src/utils"
)

// RequireExplicitTenant is stricter than legacy CMS admin routes. Wahb
// Operator has durable background work and therefore never inherits a default
// tenant when an old token omitted the tenant claim.
func RequireExplicitTenant(principal utils.AdminPrincipal) (string, error) {
	if !principal.TenantClaimed || strings.TrimSpace(principal.TenantID) == "" {
		return "", fmt.Errorf("%w: an explicit tenant claim is required", ErrInvalidContract)
	}
	return principal.TenantID, nil
}
