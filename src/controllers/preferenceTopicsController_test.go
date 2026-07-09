package controllers

import "testing"

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
