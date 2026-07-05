package controllers

import (
	"encoding/json"
	"testing"
	"time"

	"content-management-system/src/models"
)

// Trust gate: a class self-seeds through probation, is promoted to trusted once
// it clears the attempt+failure bar, and is demoted only after enough attempts
// prove it unreliable.
func TestEvaluateEnrichmentTrust(t *testing.T) {
	policy := models.DefaultEnrichmentAutopilotPolicy("default") // min 50 attempts, max 15% fail
	cases := []struct {
		name     string
		attempts int64
		failures int64
		want     string
	}{
		{"fresh class → probation", 0, 0, models.EnrichmentTrustStateProbation},
		{"few clean attempts → probation", 5, 0, models.EnrichmentTrustStateProbation},
		{"enough clean attempts → trusted", 60, 3, models.EnrichmentTrustStateTrusted}, // 5% < 15%
		{"enough attempts but too many fails → demoted", 60, 30, models.EnrichmentTrustStateDemoted},
		{"high fail rate but below demotion floor → probation", 8, 8, models.EnrichmentTrustStateProbation},
		{"at demotion floor with high fail → demoted", 10, 5, models.EnrichmentTrustStateDemoted}, // 50%
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stat := enrichmentTrustStat{Attempts: tc.attempts, Failures: tc.failures}
			if tc.attempts > 0 {
				stat.FailurePct = float64(tc.failures) * 100 / float64(tc.attempts)
			}
			if got := evaluateEnrichmentTrust(stat, policy); got != tc.want {
				t.Fatalf("state = %q, want %q (%.0f%% over %d)", got, tc.want, stat.FailurePct, tc.attempts)
			}
		})
	}
}

// Only the backfill_catchup preset, while unexpired, raises the item caps.
func TestEnrichmentAutopilotElevatedCaps(t *testing.T) {
	base := models.DefaultEnrichmentAutopilotPolicy("default")

	// No elevation → unchanged.
	if got := enrichmentAutopilotElevatedCaps(base); got.MaxItemsPerRun != base.MaxItemsPerRun {
		t.Fatalf("no elevation should not change caps, got %d", got.MaxItemsPerRun)
	}

	// Expired elevation → cleared, unchanged.
	past := time.Now().Add(-time.Minute)
	expired := base
	expired.ElevatedMode = models.EnrichmentAutopilotElevatedBackfillCatchup
	expired.ElevatedUntil = &past
	if got := enrichmentAutopilotElevatedCaps(expired); got.ElevatedMode != "" || got.MaxItemsPerRun != base.MaxItemsPerRun {
		t.Fatalf("expired elevation should clear + not scale, got mode=%q items=%d", got.ElevatedMode, got.MaxItemsPerRun)
	}

	// Active elevation → caps scaled by the multiplier.
	future := time.Now().Add(time.Hour)
	active := base
	active.ElevatedMode = models.EnrichmentAutopilotElevatedBackfillCatchup
	active.ElevatedUntil = &future
	got := enrichmentAutopilotElevatedCaps(active)
	if got.MaxItemsPerRun != base.MaxItemsPerRun*enrichmentBackfillCatchupMultiplier {
		t.Fatalf("active elevation MaxItemsPerRun = %d, want %d", got.MaxItemsPerRun, base.MaxItemsPerRun*enrichmentBackfillCatchupMultiplier)
	}
	if got.MaxItemsPerClass != base.MaxItemsPerClass*enrichmentBackfillCatchupMultiplier {
		t.Fatalf("active elevation MaxItemsPerClass = %d, want scaled", got.MaxItemsPerClass)
	}
}

// Headline reflects breaker → degraded, zero gaps → fully_enriched, budget skip →
// budget_capped, else backlog.
func TestEnrichmentHeadline(t *testing.T) {
	policy := models.DefaultEnrichmentAutopilotPolicy("default")
	full := enrichmentStatsResponse{}
	backlog := enrichmentStatsResponse{MissingEmbedding: 12}

	if r := (&enrichmentAutopilotRunner{policy: policy}).computeHeadline(full); r != models.EnrichmentAutopilotHeadlineFullyEnriched {
		t.Fatalf("no gaps → %q, want fully_enriched", r)
	}
	if r := (&enrichmentAutopilotRunner{policy: policy}).computeHeadline(backlog); r != models.EnrichmentAutopilotHeadlineBacklog {
		t.Fatalf("gaps → %q, want backlog", r)
	}
	if r := (&enrichmentAutopilotRunner{policy: policy, budgetCapped: true}).computeHeadline(enrichmentStatsResponse{MissingTranscript: 4}); r != models.EnrichmentAutopilotHeadlineBudgetCapped {
		t.Fatalf("budget skip → %q, want budget_capped", r)
	}
	if r := (&enrichmentAutopilotRunner{policy: policy, breakerFired: true}).computeHeadline(backlog); r != models.EnrichmentAutopilotHeadlineDegraded {
		t.Fatalf("breaker → %q, want degraded", r)
	}
}

func TestEnrichmentAIQueueDepth(t *testing.T) {
	stats := []autopilotQueueStat{
		{Queue: "media-queue", Waiting: 99, Active: 99},
		{Queue: "ai-queue", Waiting: 10, Active: 3, Delayed: 2},
	}
	if got := enrichmentAIQueueDepth(stats); got != 15 {
		t.Fatalf("ai-queue depth = %d, want 15 (must ignore media-queue)", got)
	}
	if got := enrichmentAIQueueDepth(nil); got != 0 {
		t.Fatalf("no stats → %d, want 0", got)
	}
}

// Sanitize clamps out-of-range knobs and forces Observe for any non-safe_auto mode.
func TestSanitizeEnrichmentAutopilotPolicy(t *testing.T) {
	p := models.EnrichmentAutopilotPolicy{
		Mode:            "banana",
		IntervalMinutes: 5,     // below floor 15
		MaxItemsPerRun:  99999, // above ceil 2000
		MaxQueueDepth:   0,     // zero → default 100
		ElevatedMode:    "nope",
	}
	got := sanitizeEnrichmentAutopilotPolicy(p)
	if got.Mode != models.EnrichmentAutopilotModeObserve {
		t.Fatalf("mode = %q, want observe", got.Mode)
	}
	if got.IntervalMinutes != 15 {
		t.Fatalf("interval = %d, want clamped to 15", got.IntervalMinutes)
	}
	if got.MaxItemsPerRun != 2000 {
		t.Fatalf("max items = %d, want clamped to 2000", got.MaxItemsPerRun)
	}
	if got.MaxQueueDepth != 100 {
		t.Fatalf("queue depth = %d, want default 100", got.MaxQueueDepth)
	}
	if got.ElevatedMode != "" {
		t.Fatalf("unknown elevated mode should be cleared, got %q", got.ElevatedMode)
	}
}

func TestMissingEmbeddingFromSnapshot(t *testing.T) {
	raw, _ := json.Marshal(enrichmentStatsResponse{MissingEmbedding: 42})
	got, ok := missingEmbeddingFromSnapshot(raw)
	if !ok || got != 42 {
		t.Fatalf("parsed = %d ok=%v, want 42 true", got, ok)
	}
	if _, ok := missingEmbeddingFromSnapshot(nil); ok {
		t.Fatalf("nil snapshot should report not-ok")
	}
}

// The managed class list must never include the dead sparse lane.
func TestSparseNeverManaged(t *testing.T) {
	for _, a := range enrichmentManagedArtifacts {
		if a == "sparse" {
			t.Fatal("sparse must never be an autopilot-managed artifact class")
		}
	}
}
