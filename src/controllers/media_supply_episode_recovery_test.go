package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/supply"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

func TestMediaSupplyEpisodeSourceIDsRejectsMalformedSubjectsAndStaysBounded(t *testing.T) {
	first, second := uuid.New(), uuid.New()
	episode := models.MediaSupplyEpisode{AffectedSubjects: datatypes.JSON([]byte(`[
        {"type":"content_source","id":"` + first.String() + `"},
        {"type":"source_run_request","id":"` + uuid.NewString() + `"},
        {"type":"content_source","id":"` + second.String() + `"},
        {"type":"content_source","id":"` + first.String() + `"}
    ]`))}
	ids, err := mediaSupplyEpisodeSourceIDs(episode)
	if err != nil || len(ids) != 2 || ids[0].String() > ids[1].String() {
		t.Fatalf("source subjects = %#v, %v", ids, err)
	}
	episode.AffectedSubjects = datatypes.JSON([]byte(`[{"type":"content_source","id":"not-a-uuid"}]`))
	if _, err := mediaSupplyEpisodeSourceIDs(episode); err == nil {
		t.Fatal("malformed source subject must not become a recovery target")
	}
}

func TestMediaSupplyEvidenceBoundaryRecoveryPreservesUnknown(t *testing.T) {
	if got := mediaSupplyEvidenceBoundaryRecovery(supply.SupplyEvaluation{EvidenceCompleteness: "complete", Verdict: supply.SupplyVerdictNoCurrentBreak}); got != supply.ResolutionObservationPresent {
		t.Fatalf("complete fresh evidence = %q", got)
	}
	for _, evaluation := range []supply.SupplyEvaluation{
		{EvidenceCompleteness: "partial", Verdict: supply.SupplyVerdictNoCurrentBreak},
		{EvidenceCompleteness: "unavailable", Verdict: supply.SupplyVerdictEvidenceUnavailable},
	} {
		if got := mediaSupplyEvidenceBoundaryRecovery(evaluation); got != supply.ResolutionObservationUnknown {
			t.Fatalf("incomplete evidence must remain unknown, got %q", got)
		}
	}
}

func TestResolutionProofUsesFreshClosureTime(t *testing.T) {
	before := time.Now().UTC().Add(-time.Second)
	proof := mediaSupplyEpisodeResolutionProof{ResolvedAt: time.Now().UTC()}
	if proof.ResolvedAt.Before(before) {
		t.Fatal("resolution proof must carry its own fresh closure observation time")
	}
}
