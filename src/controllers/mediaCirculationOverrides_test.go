package controllers

import (
	"content-management-system/src/models"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestMediaCircHasOverrideBySubjectAndType(t *testing.T) {
	id := uuid.New()
	row := models.MediaCirculationOverride{
		PublicID:     uuid.New(),
		SubjectKind:  "item",
		SubjectID:    id,
		OverrideType: models.MediaCirculationOverrideNeverArchive,
	}
	idx := mediaCircOverrideIndex{
		mediaCircOverrideKey("item", id): []models.MediaCirculationOverride{row},
	}
	if _, ok := mediaCircHasOverride(idx, "item", id, models.MediaCirculationOverrideNeverArchive); !ok {
		t.Fatal("expected never_archive override to match")
	}
	if _, ok := mediaCircHasOverride(idx, "source", id, models.MediaCirculationOverrideNeverArchive); ok {
		t.Fatal("override matched the wrong subject kind")
	}
}

func TestMediaCircOverrideReasonIncludesNotes(t *testing.T) {
	expires := time.Now().Add(time.Hour)
	row := models.MediaCirculationOverride{
		OverrideType: models.MediaCirculationOverrideNoAtomize,
		ExpiresAt:    &expires,
		Notes:        "editorial exception",
	}
	got := mediaCircOverrideReason(row)
	if got == "" || got == "Human override: no atomize." {
		t.Fatalf("override reason did not include notes: %q", got)
	}
}
