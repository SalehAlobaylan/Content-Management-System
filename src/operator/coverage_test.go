package operator

import (
	"strings"
	"testing"
)

func TestWholeConsoleCoverageAdaptersAreRegisteredAndBounded(t *testing.T) {
	registry := DefaultAdapterRegistry()
	if err := registry.Validate(); err != nil {
		t.Fatal(err)
	}
	for domain, adapter := range defaultCoverageAdapters() {
		descriptor, ok := registry.Descriptor(adapter.Descriptor.Key)
		if !ok || descriptor != adapter.Descriptor {
			t.Fatalf("coverage adapter %q is not registered", domain)
		}
		if domain != "auth_center" && len(adapter.Reads) == 0 {
			t.Fatalf("coverage adapter %q has no authoritative read model", domain)
		}
		for _, read := range adapter.Reads {
			if read.Key == "" || read.Table == "" || strings.ContainsAny(read.Table, ";'\" `") || strings.Contains(read.Where, ";") || strings.Contains(read.Order, ";") || read.Order == "" || read.Scope == "" {
				t.Fatalf("coverage adapter %q has an unsafe read: %#v", domain, read)
			}
		}
	}
}

func TestWholeConsoleCanonicalDomainsHaveExactlyOneContextAdapter(t *testing.T) {
	coveredByGolden := map[string]struct{}{"feed_integrity": {}, "feed_recovery": {}, "retention": {}, "media_circulation": {}}
	canonicalDomains := []string{"global_ops", "system_health", "feed_integrity", "feed_recovery", "retention", "real_experience", "ai_economics", "sources", "content", "news", "news_finding", "news_circulation", "media_sources", "atomization", "media_circulation", "redundancy", "media_library", "storage_quality", "pipeline", "enrichment", "intelligence", "embeddings", "topics_preferences", "moderation", "auth_center", "operator"}
	coverage := defaultCoverageAdapters()
	for _, domain := range canonicalDomains {
		_, generic := coverage[domain]
		_, golden := coveredByGolden[domain]
		if generic == golden {
			t.Fatalf("domain %q must be admitted by exactly one adapter class", domain)
		}
	}
}
