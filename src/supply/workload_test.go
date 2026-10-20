package supply

import (
	"testing"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/workload"
	"github.com/google/uuid"
	"gorm.io/datatypes"
)

func TestSourceRunGovernorOriginRequiresNativeHumanProof(t *testing.T) {
	base := models.SourceRunRequest{PublicID: uuid.New(), TenantID: "tenant-a", Purpose: "baseline", RequestedBy: "schedule", Metadata: datatypes.JSON(`{"origin":"human","requested_by":"manual"}`)}
	if got := sourceRunWorkloadRequest(base); got.Origin != workload.OriginAutomation || got.TenantID != base.TenantID || got.OwnerID != base.PublicID.String() {
		t.Fatalf("untrusted metadata changed native admission: %#v", got)
	}
	base.Purpose, base.RequestedByActorID = "manual", ""
	if got := sourceRunWorkloadRequest(base); got.Origin != workload.OriginAutomation {
		t.Fatalf("missing actor proof bypassed governor: %#v", got)
	}
	base.RequestedByActorID = uuid.NewString()
	if got := sourceRunWorkloadRequest(base); got.Origin != workload.OriginHuman {
		t.Fatalf("exact native manual operation should bypass soft governor: %#v", got)
	}
	plan := uuid.New()
	base.OperatorPlanID = &plan
	if got := sourceRunWorkloadRequest(base); got.Origin != workload.OriginCampaign {
		t.Fatalf("operator campaign inherited a human bypass: %#v", got)
	}
}

func TestSourceRunCapacitySettlementRequiresKnownTerminalEffects(t *testing.T) {
	now := time.Now().UTC()
	for _, tc := range []struct {
		name    string
		units   []models.SourceRunExecutionUnit
		settled bool
		actual  int
	}{
		{"known provider failure", []models.SourceRunExecutionUnit{{State: string(UnitFailed), TerminalOutcome: string(OutcomeProviderFailed), EffectStartedAt: &now}}, true, 1},
		{"unknown provider outcome", []models.SourceRunExecutionUnit{{State: string(UnitExpired), TerminalOutcome: string(OutcomeUnknown), EffectStartedAt: &now}}, false, 0},
		{"supervisory unit still running", []models.SourceRunExecutionUnit{{State: string(UnitRunning)}}, false, 0},
		{"cancelled before effect", []models.SourceRunExecutionUnit{{State: string(UnitCancelled), TerminalOutcome: string(OutcomeCancelled)}}, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			settled, actual := sourceRunEffectsSettled(tc.units)
			if settled != tc.settled || actual != tc.actual {
				t.Fatalf("capacity settlement=%v/%d want=%v/%d", settled, actual, tc.settled, tc.actual)
			}
		})
	}
}
