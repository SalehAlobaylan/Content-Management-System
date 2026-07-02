package controllers

import (
	"content-management-system/src/models"
	"strings"
	"testing"
)

func TestComposeMediaCirculationHeadline(t *testing.T) {
	tests := []struct {
		name         string
		storageState string
		thinBuckets  []string
		want         string
	}{
		{name: "healthy baseline", storageState: "healthy", thinBuckets: nil, want: "healthy"},
		{name: "watch from storage", storageState: "watch", thinBuckets: nil, want: "watch"},
		{name: "feed_thin overlays healthy", storageState: "healthy", thinBuckets: []string{"10"}, want: "feed_thin"},
		{name: "feed_thin overlays watch", storageState: "watch", thinBuckets: []string{"5", "40"}, want: "feed_thin"},
		{name: "over_budget from pressure", storageState: "pressure", thinBuckets: nil, want: "over_budget"},
		{name: "over_budget from critical", storageState: "critical", thinBuckets: nil, want: "over_budget"},
		{name: "over_budget outranks feed_thin", storageState: "pressure", thinBuckets: []string{"10"}, want: "over_budget"},
		{name: "degraded from no-cold", storageState: "degraded_no_cold", thinBuckets: nil, want: "degraded"},
		{name: "degraded outranks feed_thin", storageState: "degraded", thinBuckets: []string{"20"}, want: "degraded"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, reasons := composeMediaCirculationHeadline(tt.storageState, tt.thinBuckets)
			if got != tt.want {
				t.Errorf("headline = %q, want %q", got, tt.want)
			}
			// Thin buckets must always be surfaced as a reason, even when the
			// headline itself is a more urgent storage state.
			if len(tt.thinBuckets) > 0 {
				joined := strings.Join(reasons, " | ")
				if !strings.Contains(joined, "thin in duration buckets") {
					t.Errorf("thin buckets not surfaced in reasons: %v", reasons)
				}
			}
		})
	}
}

func TestClassifyBucketHealth(t *testing.T) {
	tests := []struct {
		count int64
		want  string
	}{
		{count: 0, want: "thin"},
		{count: mediaCirculationBucketThinFloor - 1, want: "thin"},
		{count: mediaCirculationBucketThinFloor, want: "ok"},
		{count: mediaCirculationBucketSaturatedCeil, want: "ok"},
		{count: mediaCirculationBucketSaturatedCeil + 1, want: "saturated"},
	}
	for _, tt := range tests {
		if got := classifyBucketHealth(tt.count); got != tt.want {
			t.Errorf("classifyBucketHealth(%d) = %q, want %q", tt.count, got, tt.want)
		}
	}
}

func TestSanitizeMediaCirculationPolicy(t *testing.T) {
	// Out-of-range values are clamped; an unknown preset falls back to balanced;
	// an inverted interval range is normalized.
	in := models.MediaCirculationPolicy{
		TenantID:                 "tenant-1",
		Preset:                   "nonsense",
		ValueFloor:               2.0,
		MarginalMargin:           -0.5,
		FreshnessDemandWeight:    9.0,
		SourceMinIntervalMinutes: 120,
		SourceMaxIntervalMinutes: 30,
	}
	out := sanitizeMediaCirculationPolicy(in)

	if out.Preset != models.MediaCirculationPresetBalanced {
		t.Errorf("preset = %q, want balanced", out.Preset)
	}
	if out.ValueFloor != 1.0 {
		t.Errorf("value_floor = %v, want 1.0 (clamped)", out.ValueFloor)
	}
	if out.MarginalMargin != 0.0 {
		t.Errorf("marginal_margin = %v, want 0.0 (clamped)", out.MarginalMargin)
	}
	if out.FreshnessDemandWeight != 1.0 {
		t.Errorf("freshness_demand_weight = %v, want 1.0 (clamped)", out.FreshnessDemandWeight)
	}
	if out.SourceMaxIntervalMinutes < out.SourceMinIntervalMinutes {
		t.Errorf("max interval %d < min interval %d after sanitize",
			out.SourceMaxIntervalMinutes, out.SourceMinIntervalMinutes)
	}
}

func TestDefaultMediaCirculationPolicyDisabled(t *testing.T) {
	p := models.DefaultMediaCirculationPolicy("tenant-x")
	if p.Enabled {
		t.Error("default policy must be disabled (deliberate opt-in)")
	}
	if p.Preset != models.MediaCirculationPresetBalanced {
		t.Errorf("default preset = %q, want balanced", p.Preset)
	}
}
