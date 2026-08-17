package models

import (
	"testing"
	"time"
)

func TestContentSourceEnsureInitialSchedule(t *testing.T) {
	now := time.Date(2026, 8, 14, 11, 0, 0, 0, time.FixedZone("test", 3*60*60))
	media := ContentSource{Category: SourceCategoryMedia, IsActive: true}
	media.EnsureInitialSchedule(now)
	if media.NextDueAt == nil || !media.NextDueAt.Equal(now.UTC()) {
		t.Fatalf("active Media source due = %v, want %v", media.NextDueAt, now.UTC())
	}

	future := now.Add(time.Hour).UTC()
	preserved := ContentSource{Category: SourceCategoryMedia, IsActive: true, NextDueAt: &future}
	preserved.EnsureInitialSchedule(now)
	if preserved.NextDueAt == nil || !preserved.NextDueAt.Equal(future) {
		t.Fatalf("existing schedule was changed: %v", preserved.NextDueAt)
	}

	for name, source := range map[string]ContentSource{
		"inactive media": {Category: SourceCategoryMedia, IsActive: false},
		"news":           {Category: SourceCategoryNews, IsActive: true},
	} {
		source.EnsureInitialSchedule(now)
		if source.NextDueAt != nil {
			t.Fatalf("%s unexpectedly scheduled at %v", name, source.NextDueAt)
		}
	}
}
