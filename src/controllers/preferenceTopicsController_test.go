package controllers

import (
	"math"
	"testing"
)

func TestPreferenceSettingDomains(t *testing.T) {
	cases := []struct {
		name  string
		key   string
		value float64
		valid bool
	}{
		{"zero disables boost", "w_foryou", 0, true},
		{"max boost", "w_foryou", 1, true},
		{"unsafe boost", "w_foryou", 1.01, false},
		{"half life default", "decay_half_life_days", 30, true},
		{"zero half life", "decay_half_life_days", 0, false},
		{"negative weight", "weight_like", -0.1, false},
		{"max interaction", "weight_like", 5, true},
		{"infinite", "declared_prior", math.Inf(1), false},
		{"nan", "category_discount", math.NaN(), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := validPreferenceSettingValue(tc.key, tc.value); got != tc.valid {
				t.Fatalf("validPreferenceSettingValue(%q, %v) = %v, want %v", tc.key, tc.value, got, tc.valid)
			}
		})
	}
}

func TestNormalizedTopicSlugPreservesArabicLetters(t *testing.T) {
	got := normalizedTopicSlug("  الذكاء الاصطناعي / AI_2026  ")
	want := "الذكاء-الاصطناعي-ai-2026"
	if got != want {
		t.Fatalf("normalizedTopicSlug() = %q, want %q", got, want)
	}
}

func TestNormalizedTopicSlugCollapsesSeparators(t *testing.T) {
	got := normalizedTopicSlug("Saudi___Vision !! 2030")
	if got != "saudi-vision-2030" {
		t.Fatalf("normalizedTopicSlug() = %q", got)
	}
}
