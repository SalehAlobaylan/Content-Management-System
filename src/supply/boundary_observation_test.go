package supply

import (
	"testing"
	"time"

	"content-management-system/src/models"
	"github.com/google/uuid"
)

func TestPodsBoundaryObservationRejectsBrowserLikeAuthority(t *testing.T) {
	row := models.PodsBoundaryObservation{TenantID: "tenant-a", ContentItemID: uuid.New(), Boundary: "page_render", ProbeKind: "frozen_session", ProbeID: uuid.NewString(), Verdict: string(VerdictPresent), ObservedAt: time.Now().UTC()}
	if err := RecordPodsBoundaryObservation(nil, row); err == nil {
		t.Fatal("a caller without the CMS evidence store was accepted")
	}
	row.Boundary = "ranking_success"
	if err := RecordPodsBoundaryObservation(nil, row); err == nil {
		t.Fatal("an unregistered consumer boundary was accepted")
	}
}
