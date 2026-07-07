package controllers

import (
	"testing"
	"time"

	"content-management-system/src/models"
)

// Sanitize clamps every knob into range and fills zero-values with the grilled
// defaults (interval 180, stuck 4h, cooldown 60, source ceiling 100, cap 3).
func TestSanitizePipelineAutopilotPolicy(t *testing.T) {
	// Zero-value policy → grilled defaults.
	def := sanitizePipelineAutopilotPolicy(models.PipelineAutopilotPolicy{})
	if def.TenantID != defaultCirculationTenant {
		t.Fatalf("empty tenant should default to %q, got %q", defaultCirculationTenant, def.TenantID)
	}
	if def.Mode != models.PipelineAutopilotModeObserve {
		t.Fatalf("blank mode should sanitize to observe, got %q", def.Mode)
	}
	checks := map[string]struct{ got, want int }{
		"interval":        {def.IntervalMinutes, 180},
		"processingStuck": {def.ProcessingStuckHours, 4},
		"cooldown":        {def.RecoveryCooldownMinutes, 60},
		"sourceCeiling":   {def.PerSourceDailyRetries, 100},
		"maxAttempts":     {def.MaxAttempts, 3},
		"trustOutcomes":   {def.TrustMinOutcomes, 20},
		"trustSuccess":    {def.TrustMinSuccessPct, 40},
	}
	for name, c := range checks {
		if c.got != c.want {
			t.Errorf("default %s = %d, want %d", name, c.got, c.want)
		}
	}

	// Out-of-range values clamp to bounds; an unknown mode falls back to observe.
	wild := sanitizePipelineAutopilotPolicy(models.PipelineAutopilotPolicy{
		Mode: "safe_auto", IntervalMinutes: 5, MaxItemsPerRun: 9000,
		ProcessingStuckHours: 999, RecoveryCooldownMinutes: -3, TrustMinSuccessPct: 250,
		ElevatedMode: "bogus",
	})
	if wild.Mode != models.PipelineAutopilotModeSafeAuto {
		t.Errorf("safe_auto mode should survive, got %q", wild.Mode)
	}
	if wild.IntervalMinutes != 15 {
		t.Errorf("interval 5 should clamp up to 15, got %d", wild.IntervalMinutes)
	}
	if wild.MaxItemsPerRun != 500 {
		t.Errorf("max items 9000 should clamp to 500, got %d", wild.MaxItemsPerRun)
	}
	if wild.ProcessingStuckHours != 24 {
		t.Errorf("processing stuck 999 should clamp to 24, got %d", wild.ProcessingStuckHours)
	}
	if wild.RecoveryCooldownMinutes != 0 {
		t.Errorf("negative cooldown should clamp to the 0 floor (disabled), got %d", wild.RecoveryCooldownMinutes)
	}
	if wild.TrustMinSuccessPct != 100 {
		t.Errorf("trust success 250 should clamp to 100, got %d", wild.TrustMinSuccessPct)
	}
	if wild.ElevatedMode != "" {
		t.Errorf("unknown elevated mode should clear, got %q", wild.ElevatedMode)
	}
}

// Trust lanes self-seed in probation, earn trusted, and demote only past the
// outcome floor (G5/§8).
func TestEvaluatePipelineTrust(t *testing.T) {
	policy := models.DefaultPipelineAutopilotPolicy("default") // min 20 outcomes, 40% success bar
	cases := []struct {
		name     string
		outcomes int64
		success  float64
		want     string
	}{
		{"fresh lane → probation", 0, 0, models.PipelineTrustStateProbation},
		{"few outcomes → probation", 5, 20, models.PipelineTrustStateProbation},
		{"enough outcomes, good rate → trusted", 25, 70, models.PipelineTrustStateTrusted},
		{"exactly at bar → trusted", 20, 40, models.PipelineTrustStateTrusted},
		{"enough outcomes, churny → demoted", 25, 20, models.PipelineTrustStateDemoted},
		{"churny but below floor → probation", 10, 0, models.PipelineTrustStateProbation},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := evaluatePipelineTrust(tc.outcomes, tc.success, policy); got != tc.want {
				t.Fatalf("state = %q, want %q (%.0f%% over %d)", got, tc.want, tc.success, tc.outcomes)
			}
		})
	}
}

// The cooldown window (G8) holds for its full duration after the recovery stamp,
// independent of run cadence, and is inert when unset or expired.
func TestPipelineCooldownActive(t *testing.T) {
	now := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)
	recent := now.Add(-30 * time.Minute) // 30m into a 60m window
	old := now.Add(-2 * time.Hour)       // window long expired

	if pipelineCooldownActive(nil, 60, now) {
		t.Error("nil stamp (never recovered) must not hold retries")
	}
	if pipelineCooldownActive(&recent, 0, now) {
		t.Error("zero-minute cooldown must be inert")
	}
	if !pipelineCooldownActive(&recent, 60, now) {
		t.Error("30m into a 60m window should still hold — regardless of interval")
	}
	if pipelineCooldownActive(&old, 60, now) {
		t.Error("2h after recovery with a 60m window should have released")
	}
}

// backlog_drain (the only preset) scales item + batch caps ×3 while unexpired and
// leaves the queue-depth cap untouched; expired/absent elevation changes nothing.
func TestPipelineAutopilotElevatedCaps(t *testing.T) {
	base := models.DefaultPipelineAutopilotPolicy("default")

	if got := pipelineAutopilotElevatedCaps(base); got.MaxItemsPerRun != base.MaxItemsPerRun {
		t.Fatalf("no elevation should not change caps, got %d", got.MaxItemsPerRun)
	}

	past := time.Now().Add(-time.Minute)
	expired := base
	expired.ElevatedMode = models.PipelineAutopilotElevatedBacklogDrain
	expired.ElevatedUntil = &past
	if got := pipelineAutopilotElevatedCaps(expired); got.ElevatedMode != "" || got.MaxItemsPerRun != base.MaxItemsPerRun {
		t.Fatalf("expired elevation should clear + not scale, got mode=%q items=%d", got.ElevatedMode, got.MaxItemsPerRun)
	}

	future := time.Now().Add(time.Hour)
	active := base
	active.ElevatedMode = models.PipelineAutopilotElevatedBacklogDrain
	active.ElevatedUntil = &future
	got := pipelineAutopilotElevatedCaps(active)
	if got.MaxItemsPerRun != base.MaxItemsPerRun*pipelineBacklogDrainMultiplier {
		t.Errorf("active drain MaxItemsPerRun = %d, want %d", got.MaxItemsPerRun, base.MaxItemsPerRun*pipelineBacklogDrainMultiplier)
	}
	if got.MaxBatchesPerRun != base.MaxBatchesPerRun*pipelineBacklogDrainMultiplier {
		t.Errorf("active drain MaxBatchesPerRun = %d, want scaled", got.MaxBatchesPerRun)
	}
	if got.MaxQueueDepth != base.MaxQueueDepth {
		t.Errorf("queue-depth cap must be exempt from the multiplier, got %d want %d", got.MaxQueueDepth, base.MaxQueueDepth)
	}
}

// Retry routing mirrors Aggregation's enqueueRetryJob guard: only genuine A/V
// (VIDEO/PODCAST/Telegram-photo) needs the media queue; everything else embeds.
func TestPipelineTargetQueue(t *testing.T) {
	video := models.ContentItem{Type: models.ContentTypeVideo}
	if q := pipelineTargetQueue(video); q != "media-queue" {
		t.Errorf("VIDEO should route to media-queue, got %q", q)
	}
	podcast := models.ContentItem{Type: models.ContentTypePodcast}
	if q := pipelineTargetQueue(podcast); q != "media-queue" {
		t.Errorf("PODCAST should route to media-queue, got %q", q)
	}
	news := models.ContentItem{Type: models.ContentTypeNews}
	if q := pipelineTargetQueue(news); q != "ai-queue" {
		t.Errorf("NEWS text should route to ai-queue, got %q", q)
	}
	tgText := models.ContentItem{Type: models.ContentTypeNews, Source: models.SourceTypeTelegram}
	if q := pipelineTargetQueue(tgText); q != "ai-queue" {
		t.Errorf("Telegram text should embed on ai-queue, got %q", q)
	}
}

// The run headline reflects the strongest signal: degraded > backlogged > repairing
// > clogged (attention only) > flowing (nothing to do).
func TestPipelineHeadline(t *testing.T) {
	cases := []struct {
		name string
		r    pipelineAutopilotRunner
		want string
	}{
		{"all failed, nothing enqueued → degraded", pipelineAutopilotRunner{errors: 3}, models.PipelineAutopilotHeadlineDegraded},
		{"queue blocked → backlogged", pipelineAutopilotRunner{queueBlocked: true}, models.PipelineAutopilotHeadlineBacklogged},
		{"repairs made → repairing", pipelineAutopilotRunner{enqueued: 5}, models.PipelineAutopilotHeadlineRepairing},
		{"only attention → clogged", pipelineAutopilotRunner{attention: 2}, models.PipelineAutopilotHeadlineClogged},
		{"idle → flowing", pipelineAutopilotRunner{}, models.PipelineAutopilotHeadlineFlowing},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.r.headline(); got != tc.want {
				t.Fatalf("headline = %q, want %q", got, tc.want)
			}
		})
	}
}
