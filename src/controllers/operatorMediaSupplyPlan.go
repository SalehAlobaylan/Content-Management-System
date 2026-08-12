package controllers

import (
	"context"
	"time"

	"content-management-system/src/models"
	operatorpkg "content-management-system/src/operator"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// executeOperatorMediaSupplyEvaluatorDisablePlan is a closed, signed-plan
// bridge to the Supply evaluator's durable subtractive control. It never
// starts a source run, reaches Aggregation, changes a Media Circulation policy,
// or creates a resume path. Existing evidence reads and safe episode
// terminalization deliberately remain outside this stop.
func executeOperatorMediaSupplyEvaluatorDisablePlan(ctx context.Context, db *gorm.DB, tenantID string, plan models.OperatorActionPlan, canonical operatorpkg.CanonicalPlan) (bool, map[string]any, map[string]any, map[string]any) {
	if !mediaSupplyEvaluatorDisableArgumentsValid(canonical) {
		return false, nil, map[string]any{"error_class": "invalid_registered_arguments"}, nil
	}

	now := time.Now().UTC()
	before := map[string]any{"recording_enabled": true, "control_present": false}
	after := map[string]any{}
	verified := map[string]any{}
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current models.MediaSupplyControl
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where(
			"tenant_id = ? AND control_key = ? AND scope_type = ? AND scope_id = ?", tenantID,
			models.MediaSupplyControlReadEvaluation, models.MediaSupplyControlScopeTenant, models.MediaSupplyControlScopeAll,
		).First(&current).Error
		if err != nil && err != gorm.ErrRecordNotFound {
			return err
		}
		if err == nil {
			before["recording_enabled"] = false
			before["control_present"] = true
			before["disabled_at"] = current.DisabledAt
			before["disabled_by"] = current.DisabledBy
		} else {
			candidate := models.MediaSupplyControl{
				TenantID: tenantID, ControlKey: models.MediaSupplyControlReadEvaluation,
				ScopeType: models.MediaSupplyControlScopeTenant, ScopeID: models.MediaSupplyControlScopeAll,
				DisabledAt: now, DisabledBy: plan.ActorID, Reason: "operator_signed_plan",
			}
			// The unique durable scope makes concurrent signed plans converge on
			// one disable row. Re-read below is the verification source of truth.
			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&candidate).Error; err != nil {
				return err
			}
		}
		if err := tx.Where(
			"tenant_id = ? AND control_key = ? AND scope_type = ? AND scope_id = ?", tenantID,
			models.MediaSupplyControlReadEvaluation, models.MediaSupplyControlScopeTenant, models.MediaSupplyControlScopeAll,
		).First(&current).Error; err != nil {
			return err
		}
		after["recording_enabled"] = false
		after["control_present"] = true
		after["control_key"] = current.ControlKey
		after["scope_type"] = current.ScopeType
		after["scope_id"] = current.ScopeID
		after["disabled_at"] = current.DisabledAt
		after["disabled_by"] = current.DisabledBy
		verified["recording_disabled"] = current.ControlKey == models.MediaSupplyControlReadEvaluation && current.ScopeType == models.MediaSupplyControlScopeTenant && current.ScopeID == models.MediaSupplyControlScopeAll
		verified["disable_only"] = true
		verified["existing_evidence_remains_available"] = true
		return nil
	})
	if err != nil {
		return false, before, map[string]any{"error_class": "media_supply_evaluator_control_write_failed"}, map[string]any{"recording_disabled": false}
	}
	succeeded, _ := verified["recording_disabled"].(bool)
	return succeeded, before, after, verified
}

func mediaSupplyEvaluatorDisableArgumentsValid(canonical operatorpkg.CanonicalPlan) bool {
	if canonical.ToolKey != "media_circulation.supply.disable_evaluator" || len(canonical.TargetIDs) != 1 || canonical.TargetIDs[0] != "current" {
		return false
	}
	return canonical.NormalizedArguments["control_key"] == models.MediaSupplyControlReadEvaluation &&
		canonical.NormalizedArguments["scope_type"] == models.MediaSupplyControlScopeTenant &&
		canonical.NormalizedArguments["scope_id"] == models.MediaSupplyControlScopeAll
}
