package supply

import (
	"fmt"
	"strings"

	"content-management-system/src/lifecycle"
	"content-management-system/src/models"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// CheckSupplyActionLifecycle serializes a native Supply action with the
// lifecycle boundary for its CMS-derived target. It is called both when the
// action becomes executable and when a worker claims it, so queued approvals
// cannot outlive a reset boundary.
func CheckSupplyActionLifecycle(tx *gorm.DB, request models.MediaSupplyActionRequest) error {
	if tx == nil || request.TenantID == "" || request.TargetID == uuid.Nil {
		return fmt.Errorf("media Supply lifecycle target is invalid")
	}
	var scope lifecycle.Scope
	switch request.TargetType {
	case "content_item":
		var item models.ContentItem
		if err := tx.Where("tenant_id=? AND public_id=?", request.TenantID, request.TargetID).First(&item).Error; err != nil {
			return fmt.Errorf("media Supply content target is unavailable: %w", err)
		}
		scope = lifecycleScopeForContentItem(item)
	case "content_source":
		var source models.ContentSource
		if err := tx.Where("tenant_id=? AND public_id=?", request.TenantID, request.TargetID).First(&source).Error; err != nil {
			return fmt.Errorf("media Supply source target is unavailable: %w", err)
		}
		scope = lifecycleScopeForContentSource(source)
	case "source_run_attempt":
		var attempt models.SourceRunAttempt
		if err := tx.Where("tenant_id=? AND public_id=?", request.TenantID, request.TargetID).First(&attempt).Error; err != nil {
			return fmt.Errorf("media Supply source-run attempt is unavailable: %w", err)
		}
		resolvedScope, err := sourceScopeForLifecycle(tx, request.TenantID, attempt.ContentSourceID)
		if err != nil {
			return err
		}
		scope = resolvedScope
	case "source_run_execution_unit":
		var unit models.SourceRunExecutionUnit
		if err := tx.Where("tenant_id=? AND public_id=?", request.TenantID, request.TargetID).First(&unit).Error; err != nil {
			return fmt.Errorf("media Supply execution unit is unavailable: %w", err)
		}
		resolvedScope, err := sourceScopeForLifecycle(tx, request.TenantID, unit.ContentSourceID)
		if err != nil {
			return err
		}
		scope = resolvedScope
	case "source_run_retained_receipt":
		var receipt models.SourceRunRetainedReceipt
		if err := tx.Where("tenant_id=? AND public_id=?", request.TenantID, request.TargetID).First(&receipt).Error; err != nil {
			return fmt.Errorf("media Supply retained receipt is unavailable: %w", err)
		}
		var unit models.SourceRunExecutionUnit
		if err := tx.Where("tenant_id=? AND public_id=?", request.TenantID, receipt.ExecutionUnitID).First(&unit).Error; err != nil {
			return fmt.Errorf("media Supply receipt source identity is unavailable: %w", err)
		}
		resolvedScope, err := sourceScopeForLifecycle(tx, request.TenantID, unit.ContentSourceID)
		if err != nil {
			return err
		}
		scope = resolvedScope
	case "atomization_work_request":
		var work models.AtomizationWorkRequest
		if err := tx.Where("tenant_id=? AND public_id=?", request.TenantID, request.TargetID).First(&work).Error; err != nil {
			return fmt.Errorf("media Supply atomization target is unavailable: %w", err)
		}
		var parent models.ContentItem
		if err := tx.Where("tenant_id=? AND public_id=?", request.TenantID, work.ParentContentItemID).First(&parent).Error; err != nil {
			return fmt.Errorf("media Supply atomization parent is unavailable: %w", err)
		}
		scope = lifecycleScopeForContentItem(parent)
	default:
		return fmt.Errorf("media Supply target type %q has no lifecycle contract", request.TargetType)
	}
	if err := lifecycle.Check(tx, scope, lifecycle.PhaseSourceDispatch); err != nil {
		return err
	}
	return lifecycle.Check(tx, scope, lifecycle.PhaseContentWrite)
}

func sourceScopeForLifecycle(tx *gorm.DB, tenant string, sourceID uuid.UUID) (lifecycle.Scope, error) {
	var source models.ContentSource
	if err := tx.Where("tenant_id=? AND public_id=?", tenant, sourceID).First(&source).Error; err != nil {
		return lifecycle.Scope{}, fmt.Errorf("media Supply source identity is unavailable: %w", err)
	}
	return lifecycleScopeForContentSource(source), nil
}

func lifecycleScopeForContentSource(source models.ContentSource) lifecycle.Scope {
	lane := "news"
	if strings.EqualFold(strings.TrimSpace(source.Category), models.SourceCategoryMedia) {
		lane = "pods"
	}
	return lifecycle.Scope{TenantID: source.TenantID, Lane: lane, SourceID: source.PublicID.String()}
}

func lifecycleScopeForContentItem(item models.ContentItem) lifecycle.Scope {
	lane := "news"
	if item.Type == models.ContentTypeVideo || item.Type == models.ContentTypePodcast {
		lane = "pods"
	}
	scope := lifecycle.Scope{TenantID: item.TenantID, Lane: lane, ItemID: item.PublicID.String()}
	if item.ContentSourceID != nil {
		scope.SourceID = item.ContentSourceID.String()
	}
	return scope
}

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
