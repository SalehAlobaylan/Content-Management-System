package contentstage

import (
	"content-management-system/src/models"
	"github.com/google/uuid"
	"testing"
	"time"
)

func TestGeneratedChapterDoesNotRequireRootDownloadApproval(t *testing.T) {
	root := uuid.New()
	item := models.ContentItem{ParentContentItemID: &root}
	if got := ResolveMediaAcquisitionMode(nil, item); got != models.MediaAcquisitionAutomatic {
		t.Fatalf("chapter acquisition=%s", got)
	}
}

func TestAtomizationLeaseAllowsLongEffects(t *testing.T) {
	if got := leaseDurationForStage(models.ContentStagePodsAtomization); got != 5*time.Minute {
		t.Fatalf("lease=%s", got)
	}
}
