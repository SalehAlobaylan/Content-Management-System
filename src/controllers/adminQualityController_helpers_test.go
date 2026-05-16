package controllers

import (
	"content-management-system/src/models"
	"testing"
)

// These tests cover the pure projection helpers in adminQualityController.go.
// They are the basis for every "projected savings" number shown in the Console
// — a silent regression here misleads operators about how much they'd save.

func TestBitrateForHeight_KnownPoints(t *testing.T) {
	cases := []struct {
		height int
		crf    int
		want   int
		// tolerancePct is the allowed ±deviation from `want`. The function
		// uses an exponential CRF scaling factor so non-integer power values
		// produce minor floating drift; we accept a small band.
		tolerancePct float64
	}{
		// CRF 23 = base table value, exact match.
		{2160, 23, 12000, 0},
		{1440, 23, 8000, 0},
		{1080, 23, 4500, 0},
		{720, 23, 2000, 0},
		{480, 23, 900, 0},
		{360, 23, 500, 0},
		{0, 23, 3500, 0}, // 0 = no cap, falls back to assumed-1080p

		// CRF higher than 23 → smaller bitrate (×0.75 per step).
		// 720p @ CRF 26: 2000 * 0.75^3 = 843.75 → 843
		{720, 26, 843, 5},
		// 1080p @ CRF 28: 4500 * 0.75^5 ≈ 1068
		{1080, 28, 1068, 5},

		// CRF lower than 23 → larger bitrate.
		// 720p @ CRF 20: 2000 * 0.75^-3 = 4740.74... → 4740
		{720, 20, 4740, 5},
	}
	for _, c := range cases {
		got := bitrateForHeight(c.height, c.crf)
		var diff float64
		if c.want > 0 {
			diff = float64(abs(got-c.want)) / float64(c.want) * 100
		}
		if c.tolerancePct == 0 && got != c.want {
			t.Errorf("bitrateForHeight(%d,%d) = %d, want %d", c.height, c.crf, got, c.want)
			continue
		}
		if c.tolerancePct > 0 && diff > c.tolerancePct {
			t.Errorf("bitrateForHeight(%d,%d) = %d, want ~%d (Δ%.1f%% > %.1f%%)",
				c.height, c.crf, got, c.want, diff, c.tolerancePct)
		}
	}
}

func TestBitrateForHeight_NearestHeightFallback(t *testing.T) {
	// Heights not in the explicit table fall through the switch ladder.
	// Cutoffs (from the source): >=2000 → 2160, >=1300 → 1440, >=900 → 1080,
	// >=600 → 720, >=420 → 480, >0 → 360, else → no-cap default.
	if got := bitrateForHeight(1200, 23); got != 4500 {
		t.Errorf("1200p (between 1080's >=900 and 1440's >=1300) → expected 1080 row (4500), got %d", got)
	}
	if got := bitrateForHeight(800, 23); got != 2000 {
		t.Errorf("800p (>=600, <900) → expected 720 row (2000), got %d", got)
	}
	if got := bitrateForHeight(240, 23); got != 500 {
		t.Errorf("240p → expected 360 row (500), got %d", got)
	}
	if got := bitrateForHeight(1500, 23); got != 8000 {
		t.Errorf("1500p (>=1300) → expected 1440 row (8000), got %d", got)
	}
}

func TestProjectSizeBytes_NoDurationFallsBackToCurrentSize(t *testing.T) {
	item := models.ContentItem{
		FileSizeBytes: 12345,
		// DurationSec nil
	}
	profile := models.QualityProfile{
		MaxHeight: 720, CRF: 23, AudioBitrateKbps: 96,
	}
	if got := projectSizeBytes(item, profile); got != 12345 {
		t.Errorf("expected fallback to file_size_bytes (12345), got %d", got)
	}
}

func TestProjectSizeBytes_BitrateMode(t *testing.T) {
	dur := 60
	item := models.ContentItem{DurationSec: &dur}
	profile := models.QualityProfile{
		TargetBitrateKbps: 2000, // explicit kbps
		AudioBitrateKbps:  128,
		MaxHeight:         720, // ignored in bitrate mode
	}
	// (2000 + 128) * 60 * 1000 / 8 = 15_960_000 bytes
	want := int64((2000 + 128) * 60 * 1000 / 8)
	if got := projectSizeBytes(item, profile); got != want {
		t.Errorf("projectSizeBytes bitrate mode: got %d, want %d", got, want)
	}
}

func TestProjectSizeBytes_CRFMode(t *testing.T) {
	dur := 60
	item := models.ContentItem{DurationSec: &dur}
	profile := models.QualityProfile{
		TargetBitrateKbps: 0, // CRF mode
		CRF:               23,
		MaxHeight:         720, // → 2000 kbps from the table
		AudioBitrateKbps:  128,
	}
	// (2000 + 128) * 60 * 1000 / 8
	want := int64((2000 + 128) * 60 * 1000 / 8)
	if got := projectSizeBytes(item, profile); got != want {
		t.Errorf("projectSizeBytes CRF mode: got %d, want %d", got, want)
	}
}

func TestPow_PositiveAndNegativeExponents(t *testing.T) {
	cases := []struct {
		base float64
		exp  int
		want float64
	}{
		{2, 0, 1},
		{2, 3, 8},
		{0.5, 2, 0.25},
		{2, -2, 0.25}, // 1 / (2*2) = 0.25
	}
	for _, c := range cases {
		got := pow(c.base, c.exp)
		// floating tolerance
		if abs64(got-c.want) > 1e-9 {
			t.Errorf("pow(%v,%d) = %v, want %v", c.base, c.exp, got, c.want)
		}
	}
}

// Tiny helpers — we avoid pulling in math just for Abs.
func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
func abs64(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
