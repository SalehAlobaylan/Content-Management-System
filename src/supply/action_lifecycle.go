package supply

import (
	"fmt"
	"strings"

	"content-management-system/src/models"
)

// Supply action state is code-owned. The database constraint is defense in
// depth; this transition table keeps controllers, workers, and recovery from
// treating an arbitrary persisted string as executable authority.
type SupplyActionRequestState string

const (
	SupplyActionAwaitingApproval SupplyActionRequestState = models.MediaSupplyActionRequestAwaitingApproval
	SupplyActionQueued           SupplyActionRequestState = models.MediaSupplyActionRequestQueued
	SupplyActionClaimed          SupplyActionRequestState = models.MediaSupplyActionRequestClaimed
	SupplyActionRunning          SupplyActionRequestState = models.MediaSupplyActionRequestRunning
	SupplyActionVerifying        SupplyActionRequestState = models.MediaSupplyActionRequestVerifying
	SupplyActionSucceeded        SupplyActionRequestState = models.MediaSupplyActionRequestSucceeded
	SupplyActionFailed           SupplyActionRequestState = models.MediaSupplyActionRequestFailed
	SupplyActionCancelled        SupplyActionRequestState = models.MediaSupplyActionRequestCancelled
	SupplyActionUncertain        SupplyActionRequestState = models.MediaSupplyActionRequestUncertain
)

var supplyActionTransitions = map[SupplyActionRequestState]map[SupplyActionRequestState]bool{
	SupplyActionAwaitingApproval: {SupplyActionQueued: true, SupplyActionCancelled: true},
	SupplyActionQueued:           {SupplyActionClaimed: true, SupplyActionCancelled: true},
	SupplyActionClaimed:          {SupplyActionRunning: true, SupplyActionQueued: true, SupplyActionCancelled: true, SupplyActionVerifying: true},
	SupplyActionRunning:          {SupplyActionVerifying: true, SupplyActionFailed: true, SupplyActionCancelled: true, SupplyActionUncertain: true},
	SupplyActionVerifying:        {SupplyActionSucceeded: true, SupplyActionFailed: true, SupplyActionCancelled: true, SupplyActionUncertain: true},
	SupplyActionUncertain:        {SupplyActionVerifying: true},
}

func CanTransitionSupplyAction(from, to SupplyActionRequestState) bool {
	return supplyActionTransitions[from][to]
}

func ValidateSupplyActionTransition(from, to SupplyActionRequestState) error {
	if !CanTransitionSupplyAction(from, to) {
		return fmt.Errorf("media supply action transition %q -> %q is not permitted", from, to)
	}
	return nil
}

func IsTerminalSupplyAction(state SupplyActionRequestState) bool {
	return state == SupplyActionSucceeded || state == SupplyActionFailed || state == SupplyActionCancelled
}

// RequireSupplyActionDescriptor is the common anti-dispatcher boundary. A
// caller has to bind its exact key and target type to a static descriptor
// before it may create a preview, approval, worker claim, or owner handoff.
func RequireSupplyActionDescriptor(key, targetType string) (SupplyActionDescriptor, error) {
	descriptor, ok := SupplyAction(strings.TrimSpace(key))
	if !ok || descriptor.TargetType != strings.TrimSpace(targetType) {
		return SupplyActionDescriptor{}, fmt.Errorf("media supply action is not registered for its target")
	}
	return descriptor, nil
}
