package controllers

import (
	"encoding/json"
	"errors"
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
	if r := (&enrichmentAutopilotRunner{policy: policy, budgetCapped: true}).computeHeadline(enrichmentStatsResponse{MissingTranscript: 4, MissingTranscriptActionable: 4}); r != models.EnrichmentAutopilotHeadlineBudgetCapped {
		t.Fatalf("budget skip → %q, want budget_capped", r)
	}
	if r := (&enrichmentAutopilotRunner{policy: policy, breakerFired: true}).computeHeadline(backlog); r != models.EnrichmentAutopilotHeadlineDegraded {
		t.Fatalf("breaker → %q, want degraded", r)
	}
	if r := (&enrichmentAutopilotRunner{policy: policy, serviceGated: true}).computeHeadline(backlog); r != models.EnrichmentAutopilotHeadlineDegraded {
		t.Fatalf("service gate → %q, want degraded", r)
	}
	if r := (&enrichmentAutopilotRunner{policy: policy}).computeHeadline(enrichmentStatsResponse{MissingTranscript: 40}); r != models.EnrichmentAutopilotHeadlineFullyEnriched {
		t.Fatalf("long parents only → %q, want fully_enriched", r)
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
	zero := sanitizeEnrichmentAutopilotPolicy(models.DefaultEnrichmentAutopilotPolicy("default"))
	zero.MaxTranscriptsPerRun, zero.AgeFloorMinutes = 0, 0
	zero = sanitizeEnrichmentAutopilotPolicy(zero)
	if zero.MaxTranscriptsPerRun != 0 || zero.AgeFloorMinutes != 0 {
		t.Fatalf("legal zero knobs must remain zero: %+v", zero)
	}
	zero.MaxTranscriptsPerRun = 501
	if got := sanitizeEnrichmentAutopilotPolicy(zero).MaxTranscriptsPerRun; got != 500 {
		t.Fatalf("transcript cap = %d, want 500", got)
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

func TestClassMissingFromSnapshot(t *testing.T) {
	raw, _ := json.Marshal(enrichmentStatsResponse{MissingTranscriptActionable: 2, MissingEmbedding: 3, MissingImageEmbedding: 4})
	for artifact, want := range map[string]int64{models.EnrichmentArtifactTranscript: 2, models.EnrichmentArtifactEmbedding: 3, models.EnrichmentArtifactImage: 4} {
		if got, ok := classMissingFromSnapshot(raw, artifact); !ok || got != want {
			t.Fatalf("%s = %d ok=%v, want %d", artifact, got, ok, want)
		}
	}
	if _, ok := classMissingFromSnapshot(raw, "sparse"); ok {
		t.Fatal("sparse must not map to a class snapshot field")
	}
}

func TestAttentionTarget(t *testing.T) {
	if got := attentionTargetFor(models.EnrichmentArtifactTranscript, ""); got != "media_studio" {
		t.Fatalf("transcript target = %q", got)
	}
	if got := attentionTargetFor(models.EnrichmentArtifactEmbedding, models.EnrichmentAutopilotGuardQueueDepth); got != "pipeline" {
		t.Fatalf("queue target = %q", got)
	}
	if got := attentionTargetFor(models.EnrichmentArtifactImage, ""); got != "missing_panel" {
		t.Fatalf("image target = %q", got)
	}
}

func TestInFlightSTTGuardrailConstant(t *testing.T) {
	if models.EnrichmentAutopilotGuardAlreadyPresent != "already_present" {
		t.Fatalf("guardrail = %q", models.EnrichmentAutopilotGuardAlreadyPresent)
	}
}

func TestBulkLane(t *testing.T) {
	if !tryStartEnrichmentAutopilotRun("bulk-lane-test") {
		t.Fatal("fresh tenant should acquire")
	}
	if !enrichmentAutopilotAnyRunInFlight() {
		t.Fatal("autopilot must mark lane busy")
	}
	finishEnrichmentAutopilotRun("bulk-lane-test")
	bulkMu.Lock()
	bulkState.Running = true
	bulkMu.Unlock()
	if !bulkLaneBusy() {
		t.Fatal("manual run must mark lane busy")
	}
	bulkMu.Lock()
	bulkState.Running = false
	bulkMu.Unlock()
}

func TestManagedArtifact(t *testing.T) {
	for _, artifact := range []string{models.EnrichmentArtifactTranscript, models.EnrichmentArtifactEmbedding, models.EnrichmentArtifactImage} {
		if !isManagedEnrichmentArtifact(artifact) {
			t.Fatalf("%s should be managed", artifact)
		}
	}
	for _, artifact := range []string{"sparse", "", "banana"} {
		if isManagedEnrichmentArtifact(artifact) {
			t.Fatalf("%s should not be managed", artifact)
		}
	}
}

func TestEnsuredPolicyRowIsInert(t *testing.T) {
	p := models.DefaultEnrichmentAutopilotPolicy("t")
	if p.Enabled || p.Mode != models.EnrichmentAutopilotModeObserve {
		t.Fatalf("default must be disabled observe: %+v", p)
	}
}

func TestHeartbeatPanicContainment(t *testing.T) {
	defer func() {
		if rec := recover(); rec != nil {
			t.Fatalf("panic escaped: %v", rec)
		}
	}()
	withEnrichmentAutopilotRecovery("tenant-x", func() { panic("boom") })
}

func TestSTTSkipKindClassification(t *testing.T) {
	if got := sttSkipKindOf(&sttSkippedError{reason: "monthly STT budget cap reached", kind: sttSkipBudget}); got != sttSkipBudget {
		t.Fatalf("budget = %q", got)
	}
	if got := sttSkipKindOf(&sttSkippedError{reason: "guard", kind: sttSkipGuard}); got != sttSkipGuard {
		t.Fatalf("guard = %q", got)
	}
	if got := sttSkipKindOf(errors.New("boom")); got != sttSkipNone {
		t.Fatalf("non-skip = %q", got)
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
