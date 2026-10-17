package economics

import (
	"content-management-system/src/utils"
	"os"
	"strings"
)

func PublicTenant() string {
	s, e := utils.GetConfiguredPublicTenantID()
	if e != nil {
		return ""
	}
	return s
}

func EnvironmentName() string {
	if name := strings.TrimSpace(os.Getenv("WAHB_ENVIRONMENT_ID")); name != "" {
		return name
	}
	return "unmapped"
}
