package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"content-management-system/src/models"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// The native request and this waiting transition commit together. A crash can
// never leave an accepted effect without an exact signed-plan owner reference.
func waitOperatorSupplyHandoff(tx *gorm.DB, plan models.OperatorActionPlan, step models.OperatorActionStep, action models.MediaSupplyActionRequest) error {
	after, _ := json.Marshal(map[string]any{"schema_version": "operator-supply-handoff/v2", "native_action_id": action.PublicID, "native_action_state": action.State, "waiting_for_native_action": true})
	if err := tx.Model(&plan).Updates(map[string]any{"state": "verifying", "claim_token": nil, "claim_expires_at": nil}).Error; err != nil {
		return err
	}
	if err := tx.Model(&step).Updates(map[string]any{"state": "verifying", "after_state": datatypes.JSON(after), "claim_token": nil, "claim_expires_at": nil}).Error; err != nil {
		return err
	}
	if err := tx.Model(&models.OperatorActionJob{}).Where("plan_id=? AND tenant_id=?", plan.ID, plan.TenantID).Updates(map[string]any{"state": "verifying", "claim_token": nil, "claim_expires_at": nil}).Error; err != nil {
		return err
	}
	return appendOperatorSupplyEvent(tx, plan, step, "native_action_waiting", map[string]any{"native_action_id": action.PublicID, "native_action_state": action.State})
}

func operatorSupplyTerminalState(action models.MediaSupplyActionRequest) (string, bool) {
	switch action.State {
	case "succeeded":
		var proof struct {
			SchemaVersion string          `json:"schema_version"`
			Proof         json.RawMessage `json:"proof"`
		}
		if json.Unmarshal(action.VerifiedEffects, &proof) != nil || proof.SchemaVersion != "media-supply-verified-effects/v1" || len(proof.Proof) == 0 || string(proof.Proof) == "null" || string(proof.Proof) == "{}" {
			return "verifying", false
		}
		return "succeeded", true
	case "failed", "cancelled":
		return action.State, true
	default:
		return "verifying", false
	}
}

// This observer authorizes no new effect and intentionally does not depend on
// current IAM, disabled execution controls, or a still-live approval deadline.
func reconcileOperatorSupplyHandoffs(ctx context.Context, db *gorm.DB) error {
	var actions []models.MediaSupplyActionRequest
	if err := db.WithContext(ctx).Table("media_supply_action_requests AS action").Select("action.*").Joins("JOIN operator_action_plans AS plan ON plan.public_id=action.operator_plan_id AND plan.tenant_id=action.tenant_id").Where("action.operator_step_id IS NOT NULL AND action.state IN ? AND plan.state=?", []string{"succeeded", "failed", "cancelled"}, "verifying").Order("plan.updated_at ASC, action.public_id ASC").Limit(32).Scan(&actions).Error; err != nil {
		return err
	}
	for _, candidate := range actions {
		if err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var plan models.OperatorActionPlan
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id=? AND tenant_id=? AND state=?", candidate.OperatorPlanID, candidate.TenantID, "verifying").First(&plan).Error; err == gorm.ErrRecordNotFound {
				return nil
			} else if err != nil {
				return err
			}
			var step models.OperatorActionStep
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id=? AND plan_id=? AND tenant_id=? AND state=?", candidate.OperatorStepID, plan.ID, plan.TenantID, "verifying").First(&step).Error; err != nil {
				return err
			}
			var action models.MediaSupplyActionRequest
			if err := tx.Where("public_id=? AND tenant_id=? AND operator_plan_id=? AND operator_step_id=? AND action_key=?", candidate.PublicID, plan.TenantID, plan.PublicID, step.PublicID, step.ToolKey).First(&action).Error; err != nil {
				return err
			}
			state, terminal := operatorSupplyTerminalState(action)
			now := time.Now().UTC()
			if !terminal {
				return tx.Model(&plan).UpdateColumn("updated_at", now).Error
			}
			after, _ := json.Marshal(map[string]any{"schema_version": "operator-supply-handoff/v2", "native_action_id": action.PublicID, "native_action_state": action.State, "waiting_for_native_action": false, "failure_class": action.FailureClass})
			updates := map[string]any{"state": state, "after_state": datatypes.JSON(after), "verified_state": action.VerifiedEffects, "finished_at": now, "claim_token": nil, "claim_expires_at": nil}
			if err := tx.Model(&step).Updates(updates).Error; err != nil {
				return err
			}
			if err := tx.Model(&plan).Updates(map[string]any{"state": state, "completed_at": now}).Error; err != nil {
				return err
			}
			if err := tx.Model(&models.OperatorActionJob{}).Where("plan_id=? AND tenant_id=?", plan.ID, plan.TenantID).Updates(map[string]any{"state": state, "completed_at": now, "claim_token": nil, "claim_expires_at": nil}).Error; err != nil {
				return err
			}
			if err := tx.Model(&models.OperatorPlanApproval{}).Where("plan_id=? AND tenant_id=? AND plan_digest=? AND consumed_at IS NULL", plan.ID, plan.TenantID, plan.Digest).Update("consumed_at", now).Error; err != nil {
				return err
			}
			return appendOperatorSupplyEvent(tx, plan, step, "native_action_"+state, map[string]any{"native_action_id": action.PublicID, "verified_effects": json.RawMessage(action.VerifiedEffects)})
		}); err != nil {
			return err
		}
	}
	return nil
}

func appendOperatorSupplyEvent(tx *gorm.DB, plan models.OperatorActionPlan, step models.OperatorActionStep, kind string, value map[string]any) error {
	if step.PlanID != plan.ID || step.TenantID != plan.TenantID {
		return fmt.Errorf("signed native handoff tenant or step mismatch")
	}
	var sequence int64
	if err := tx.Model(&models.OperatorPlanEvent{}).Where("plan_id=? AND tenant_id=?", plan.ID, plan.TenantID).Select("COALESCE(MAX(sequence),0)").Scan(&sequence).Error; err != nil {
		return err
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return tx.Create(&models.OperatorPlanEvent{PublicID: uuid.New(), PlanID: plan.ID, StepID: &step.ID, TenantID: plan.TenantID, Sequence: sequence + 1, EventType: kind, ActorType: "owner_verifier", Payload: datatypes.JSON(payload), CreatedAt: time.Now().UTC()}).Error
}
