package controllers

import (
	"strings"
	"testing"

	"content-management-system/src/models"
)

func sourceStringPtr(value string) *string { return &value }

func TestCanonicalContentSourceKeyPrefersNormalizedFeedHost(t *testing.T) {
	item := models.ContentItem{
		SourceFeedURL: sourceStringPtr("https://www.Example.com/podcast/feed.xml"),
		SourceName:    sourceStringPtr("A fallback source"),
	}
	if got := canonicalContentSourceKey(item); got != "feed:example.com" {
		t.Fatalf("expected canonical feed host, got %q", got)
	}
}

func TestCanonicalContentSourceKeyAcceptsSchemelessFeedAndFallsBackToName(t *testing.T) {
	item := models.ContentItem{SourceFeedURL: sourceStringPtr("feeds.example.org/news")}
	if got := canonicalContentSourceKey(item); got != "feed:feeds.example.org" {
		t.Fatalf("expected schemeless feed host, got %q", got)
	}

	item = models.ContentItem{SourceName: sourceStringPtr("  Saudi   News  ")}
	if got := canonicalContentSourceKey(item); got != "name:saudi news" {
		t.Fatalf("expected normalized source-name fallback, got %q", got)
	}
}

func TestSourcePreferenceKeyShapeRejectsUnownedArbitraryValues(t *testing.T) {
	for _, key := range []string{"feed:example.com", "name:saudi news"} {
		if len(key) == 0 || len(key) > 320 || (!strings.HasPrefix(key, "feed:") && !strings.HasPrefix(key, "name:")) {
			t.Fatalf("expected valid source preference key %q", key)
		}
	}
	for _, key := range []string{"", "other:example.com"} {
		if len(key) > 0 && len(key) <= 320 && (strings.HasPrefix(key, "feed:") || strings.HasPrefix(key, "name:")) {
			t.Fatalf("expected invalid source preference key %q", key)
		}
	}
}
