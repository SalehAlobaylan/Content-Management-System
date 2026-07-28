package controllers

import (
	"testing"
	"time"

	"content-management-system/src/models"
)

func TestRetentionVerdictBandsAndForecast(t *testing.T) {
	policy := models.DefaultRetentionPolicy("default")
	cases := []struct {
		name     string
		bytes    int64
		forecast retentionForecast
		want     string
	}{
		{name: "healthy", bytes: 399 * 1024 * 1024, want: models.RetentionVerdictHealthy},
		{name: "warning bytes", bytes: 401 * 1024 * 1024, want: models.RetentionVerdictWarning},
		{name: "action bytes", bytes: 441 * 1024 * 1024, want: models.RetentionVerdictActionRequired},
		{name: "critical bytes", bytes: 481 * 1024 * 1024, want: models.RetentionVerdictCritical},
		{name: "warning runway", bytes: 390 * 1024 * 1024, forecast: retentionForecast{RunwayToTargetDays: retentionFloatPtr(10)}, want: models.RetentionVerdictWarning},
		{name: "action runway", bytes: 390 * 1024 * 1024, forecast: retentionForecast{RunwayToActionDays: retentionFloatPtr(6)}, want: models.RetentionVerdictActionRequired},
		{name: "critical runway", bytes: 390 * 1024 * 1024, forecast: retentionForecast{RunwayToCriticalDays: retentionFloatPtr(1.5)}, want: models.RetentionVerdictCritical},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			sample := &models.RetentionDBSample{DatabaseBytes: test.bytes}
			if got := retentionVerdict(policy, sample, test.forecast); got != test.want {
				t.Fatalf("retentionVerdict() = %q, want %q", got, test.want)
			}
		})
	}
	if got := retentionVerdict(policy, nil, retentionForecast{}); got != models.RetentionVerdictInconclusive {
		t.Fatalf("nil sample verdict = %q", got)
	}
}

func TestCalculateRetentionForecast(t *testing.T) {
	policy := models.DefaultRetentionPolicy("default")
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	samples := []models.RetentionDBSample{
		{DatabaseBytes: 380 * 1024 * 1024, MeasuredAt: start},
		{DatabaseBytes: 390 * 1024 * 1024, MeasuredAt: start.Add(48 * time.Hour)},
	}
	forecast := calculateRetentionForecast(samples, policy)
	if forecast.GrowthBytesPerDay != 5*1024*1024 {
		t.Fatalf("growth/day = %d, want %d", forecast.GrowthBytesPerDay, 5*1024*1024)
	}
	if forecast.RunwayToActionDays == nil || *forecast.RunwayToActionDays != 10 {
		t.Fatalf("action runway = %#v, want 10 days", forecast.RunwayToActionDays)
	}
}

func TestRetentionPolicyRejectsMisorderedThresholds(t *testing.T) {
	policy := models.DefaultRetentionPolicy("default")
	policy.DatabaseActionBytes = policy.DatabaseWarningBytes
	if err := retentionPolicyValid(policy); err == nil {
		t.Fatal("expected equal warning/action bands to be rejected")
	}
}

func retentionFloatPtr(value float64) *float64 { return &value }
