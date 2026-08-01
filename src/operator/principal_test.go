package operator

import (
	"testing"

	"content-management-system/src/utils"
)

func TestRequireExplicitTenantRejectsLegacyDefault(t *testing.T) {
	if _, err := RequireExplicitTenant(utils.AdminPrincipal{TenantID: "default"}); err == nil {
		t.Fatal("expected missing tenant claim to fail")
	}
	if tenant, err := RequireExplicitTenant(utils.AdminPrincipal{TenantID: "tenant-a", TenantClaimed: true}); err != nil || tenant != "tenant-a" {
		t.Fatalf("expected explicit tenant to pass, tenant=%q err=%v", tenant, err)
	}
}
