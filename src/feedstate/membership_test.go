package feedstate

import "testing"

func TestAdvisoryKeyIsTenantScopedAndStable(t *testing.T) {
	if advisoryKey("wahb:feed-membership:tenant-a") != advisoryKey("wahb:feed-membership:tenant-a") {
		t.Fatal("advisory key must be stable")
	}
	if advisoryKey("wahb:feed-membership:tenant-a") == advisoryKey("wahb:feed-membership:tenant-b") {
		t.Fatal("tenants must not share a reconciliation lock")
	}
}
