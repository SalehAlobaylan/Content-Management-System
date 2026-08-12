package supply

import "testing"

func TestAdmissionCutoverRequiresExplicitDatabaseTenantAndLane(t *testing.T) {
	if err := RequireDurableAdmission(nil, "tenant-a", "media"); err == nil {
		t.Fatal("durable admission without a database must fail closed")
	}
	if err := RequireDurableAdmission(nil, "", "media"); err == nil {
		t.Fatal("durable admission must reject implicit tenants")
	}
	if err := RequireLegacyAdmission(nil, "tenant-a", "media"); err == nil {
		t.Fatal("legacy admission without a database must fail closed")
	}
}
