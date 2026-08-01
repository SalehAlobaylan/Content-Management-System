package operator

import "testing"

func TestShadowInventoryIsReadOnlyAndBilingualReady(t *testing.T) {
	if len(ShadowDomains) != 26 {
		t.Fatalf("expected every Console domain, got %d", len(ShadowDomains))
	}
	seen := map[string]struct{}{}
	for _, domain := range ShadowDomains {
		if _, exists := seen[domain]; exists {
			t.Fatalf("duplicate shadow domain %q", domain)
		}
		seen[domain] = struct{}{}
		context := ShadowVisibleContext(domain)
		if err := context.Validate(); err != nil {
			t.Fatalf("invalid shadow context for %q: %v", domain, err)
		}
		if len(context.AvailableIntents) != 1 || context.AvailableIntents[0] != IntentExplain {
			t.Fatalf("shadow %q admitted mutation intent", domain)
		}
	}
}

func TestShadowRejectsLegacyDefaultTenant(t *testing.T) {
	access := AccessSnapshot{UserID: "admin", TenantID: "default", IsAdmin: true, AccessVersion: "version", Permissions: []string{"feed:read"}}
	if err := validateShadowAccess(access); err == nil {
		t.Fatal("shadow qualification must not fall back to the default tenant")
	}
}
