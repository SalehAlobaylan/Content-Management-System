package supply

import (
	"strings"

	"content-management-system/src/models"
	"content-management-system/src/workload"
)

const workloadOwner = "source_run"

func sourceRunWorkloadRequest(request models.SourceRunRequest) workload.Request {
	origin := workload.OriginAutomation
	// Only CMS-persisted manual purpose and actor proof permit this bypass.
	// Request metadata and provider envelopes cannot select an execution origin.
	if request.Purpose == "manual" && strings.TrimSpace(request.RequestedByActorID) != "" && request.OperatorPlanID == nil {
		origin = workload.OriginHuman
	}
	if request.OperatorPlanID != nil {
		origin = workload.OriginCampaign
	}
	return workload.Request{
		TenantID: request.TenantID, System: "source_supply", Class: "source_pull", Units: 1,
		RunID: request.PublicID.String(), Origin: origin, OwnerKind: workloadOwner, OwnerID: request.PublicID.String(),
	}
}

func sourceRunEffectsSettled(units []models.SourceRunExecutionUnit) (bool, int) {
	started := false
	for _, unit := range units {
		if !IsTerminalUnit(ExecutionUnitState(unit.State)) {
			return false, 0
		}
		if unit.EffectStartedAt == nil {
			continue
		}
		started = true
		if !IsKnownOutcome(SourceRunOutcome(unit.TerminalOutcome)) || unit.TerminalOutcome == string(OutcomeUnknown) {
			return false, 0
		}
	}
	if started {
		return true, 1
	}
	return true, 0
}
