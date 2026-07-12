package controllers

import (
	"testing"
	"time"
)

func TestOpsHeadlinePrioritizesIncidentsAndStalledMembers(t *testing.T) {
	now := time.Now().UTC()
	incident, _ := opsHeadline([]opsAttentionItem{{System: "system_health", Kind: "episode", Severity: "major", FirstSeen: now}}, nil)
	if incident != "incident" {
		t.Fatalf("system health episode headline = %q, want incident", incident)
	}
	stalled, _ := opsHeadline([]opsAttentionItem{
		{System: "pipeline", Kind: "stalled", Severity: "major", FirstSeen: now},
		{System: "enrichment", Kind: "stalled", Severity: "major", FirstSeen: now},
		{System: "preferences", Kind: "stalled", Severity: "major", FirstSeen: now},
	}, nil)
	if stalled != "incident" {
		t.Fatalf("three independent stalled members headline = %q, want incident", stalled)
	}
	watching, _ := opsHeadline([]opsAttentionItem{{System: "storage", Kind: "attention", Severity: "minor", FirstSeen: now}}, nil)
	if watching != "watching" {
		t.Fatalf("minor attention headline = %q, want watching", watching)
	}
}

func TestOpsTimeFingerprintIsNilSafeAndStable(t *testing.T) {
	if got := opsTimeFingerprint(nil); got != "" {
		t.Fatalf("nil fingerprint = %q, want empty", got)
	}
	value := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
	if opsTimeFingerprint(&value) != opsTimeFingerprint(&value) {
		t.Fatal("same timestamp must have a stable fingerprint")
	}
}

func TestOpsHeadlineSentenceCountsOnlyVisibleItems(t *testing.T) {
	now := time.Now().UTC()
	headline, summary := opsHeadline([]opsAttentionItem{
		{System: "storage", Kind: "budget", Severity: "major", FirstSeen: now},
		{System: "ai_spend", Kind: "budget", Severity: "major", FirstSeen: now, Snoozed: true},
	}, nil)
	if headline != "attention" {
		t.Fatalf("headline = %q, want attention", headline)
	}
	if summary != "1 fleet items need attention." {
		t.Fatalf("summary counts snoozed items: %q", summary)
	}
}

func TestOpsPauseValuesMatchTolerance(t *testing.T) {
	base := time.Date(2026, 7, 12, 12, 0, 0, 500, time.UTC) // sub-microsecond precision
	truncated := base.Truncate(time.Microsecond)
	if !opsPauseValuesMatch(&truncated, &base) {
		t.Fatal("microsecond-truncated round-trip must match")
	}
	foreign := base.Add(2 * time.Minute)
	if opsPauseValuesMatch(&foreign, &base) {
		t.Fatal("a pause minutes away must not match")
	}
	if opsPauseValuesMatch(nil, &base) || opsPauseValuesMatch(&base, nil) {
		t.Fatal("nil values must never match")
	}
}
