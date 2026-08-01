package operator

import (
	"context"
	"encoding/json"
	"fmt"

	"content-management-system/src/models"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ClaimPlan binds a freshly authorized identity to one approved, signed plan.
// It also fences the only executable step. The caller must still do its own
// tool-specific preflight before BeginClaimedPlan.
func (store *PlanStore) ClaimPlan(ctx context.Context, planID uint, snapshot AccessSnapshot) (models.OperatorActionPlan, CanonicalPlan, ToolDescriptor, error) {
	if err := snapshot.ValidateFor(snapshot.UserID, snapshot.TenantID); err != nil {
		return models.OperatorActionPlan{}, CanonicalPlan{}, ToolDescriptor{}, err
	}
	now := store.now()
	claimToken := uuid.New()
	claimExpiry := now.Add(defaultPlanClaimLease)
	var stored models.OperatorActionPlan
	var canonical CanonicalPlan
	var descriptor ToolDescriptor
	err := store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&stored, planID).Error; err != nil {
			return err
		}
		var err error
		canonical, descriptor, err = store.DecodeStoredPlan(stored)
		if err != nil {
			return err
		}
		if stored.State != "queued" || !now.Before(stored.ExpiresAt) || stored.TenantID != snapshot.TenantID || stored.ActorID != snapshot.UserID || stored.AccessVersion != snapshot.AccessVersion || !snapshot.HasPermission(descriptor.RequiredPermission) {
			return fmt.Errorf("%w: plan claim preconditions changed", ErrAccessUnavailable)
		}
		if err := EnsureToolCapabilityEnabled(tx, stored.TenantID, descriptor.Key, now); err != nil {
			return err
		}
		var approval models.OperatorPlanApproval
		if err := tx.Where("plan_id=? AND tenant_id=? AND actor_id=? AND plan_digest=? AND access_version=? AND consumed_at IS NULL AND expires_at>?", stored.ID, stored.TenantID, stored.ActorID, stored.Digest, stored.AccessVersion, now).First(&approval).Error; err != nil {
			return fmt.Errorf("%w: live plan approval is required", ErrInvalidContract)
		}
		var step models.OperatorActionStep
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("plan_id=? AND ordinal=? AND state=?", stored.ID, 1, "pending").First(&step).Error; err != nil {
			return fmt.Errorf("%w: plan has no runnable step", ErrInvalidContract)
		}
		if err := tx.Model(&stored).Updates(map[string]any{"state": "claimed", "claim_token": claimToken, "claim_expires_at": claimExpiry}).Error; err != nil {
			return err
		}
		if err := tx.Model(&step).Updates(map[string]any{"state": "claimed", "claim_token": claimToken, "claim_expires_at": claimExpiry}).Error; err != nil {
			return err
		}
		stored.State, stored.ClaimToken, stored.ClaimExpiresAt = "claimed", &claimToken, &claimExpiry
		return appendNextPlanEvent(tx, stored.ID, &step.ID, stored.TenantID, "plan_claimed", "worker", "", map[string]any{"claim_expires_at": claimExpiry, "tool_key": descriptor.Key})
	})
	return stored, canonical, descriptor, err
}

// CancelPlan is deliberately limited to the signed plan's declared
// before-start cancellation window. It cannot interrupt an already-running
// mutation or invent a rollback; those need their own registered lifecycle.
func (store *PlanStore) CancelPlan(ctx context.Context, planID uint, snapshot AccessSnapshot) (models.OperatorActionPlan, error) {
	if err := snapshot.ValidateFor(snapshot.UserID, snapshot.TenantID); err != nil {
		return models.OperatorActionPlan{}, err
	}
	now := store.now()
	var plan models.OperatorActionPlan
	err := store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=? AND tenant_id=? AND actor_id=?", planID, snapshot.TenantID, snapshot.UserID).First(&plan).Error; err != nil {
			return err
		}
		canonical, _, err := store.DecodeStoredPlan(plan)
		if err != nil {
			return err
		}
		if canonical.Cancellation != "before_start_only" || (plan.State != "awaiting_approval" && plan.State != "queued" && plan.State != "claimed") {
			return fmt.Errorf("%w: plan is outside its cancellation window", ErrInvalidContract)
		}
		var step models.OperatorActionStep
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("plan_id=? AND ordinal=?", plan.ID, 1).First(&step).Error; err != nil {
			return err
		}
		if step.State != "pending" && step.State != "claimed" {
			return fmt.Errorf("%w: step is outside its cancellation window", ErrInvalidContract)
		}
		if err := tx.Model(&step).Updates(map[string]any{"state": "cancelled", "finished_at": now, "claim_token": nil, "claim_expires_at": nil}).Error; err != nil {
			return err
		}
		if err := tx.Model(&plan).Updates(map[string]any{"state": "cancelled", "completed_at": now, "claim_token": nil, "claim_expires_at": nil}).Error; err != nil {
			return err
		}
		if err := tx.Model(&models.OperatorPlanApproval{}).Where("plan_id=? AND consumed_at IS NULL", plan.ID).Update("consumed_at", now).Error; err != nil {
			return err
		}
		if err := tx.Model(&models.OperatorActionJob{}).Where("plan_id=? AND state IN ?", plan.ID, []string{"queued", "authorizing", "claimed"}).Updates(map[string]any{"state": "cancelled", "completed_at": now, "claim_token": nil, "claim_expires_at": nil}).Error; err != nil {
			return err
		}
		plan.State, plan.CompletedAt, plan.ClaimToken, plan.ClaimExpiresAt = "cancelled", &now, nil, nil
		return appendNextPlanEvent(tx, plan.ID, &step.ID, plan.TenantID, "plan_cancelled", "admin", snapshot.UserID, map[string]any{"cancellation": canonical.Cancellation})
	})
	return plan, err
}

// BeginClaimedPlan moves only the current lease into execution. It writes an
// immutable intent event before a controller invokes the registered CMS tool.
func (store *PlanStore) BeginClaimedPlan(ctx context.Context, planID uint, tenantID string, claimToken uuid.UUID) (models.OperatorActionPlan, models.OperatorActionStep, error) {
	now := store.now()
	var plan models.OperatorActionPlan
	var step models.OperatorActionStep
	err := store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=? AND tenant_id=?", planID, tenantID).First(&plan).Error; err != nil {
			return err
		}
		if plan.State != "claimed" || plan.ClaimToken == nil || *plan.ClaimToken != claimToken || plan.ClaimExpiresAt == nil || !now.Before(*plan.ClaimExpiresAt) {
			return fmt.Errorf("%w: plan claim is not current", ErrInvalidContract)
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("plan_id=? AND ordinal=?", plan.ID, 1).First(&step).Error; err != nil {
			return err
		}
		if step.State != "claimed" || step.ClaimToken == nil || *step.ClaimToken != claimToken || step.ClaimExpiresAt == nil || !now.Before(*step.ClaimExpiresAt) {
			return fmt.Errorf("%w: action step claim is not current", ErrInvalidContract)
		}
		if err := tx.Model(&plan).Update("state", "running").Error; err != nil {
			return err
		}
		if err := tx.Model(&step).Updates(map[string]any{"state": "running", "started_at": now}).Error; err != nil {
			return err
		}
		plan.State, step.State, step.StartedAt = "running", "running", &now
		return appendNextPlanEvent(tx, plan.ID, &step.ID, tenantID, "step_started", "worker", "", map[string]any{"tool_key": step.ToolKey})
	})
	return plan, step, err
}

// FinishClaimedPlan records a bounded before/after/verification proof and
// consumes the exact approval only when a claimed execution reaches a terminal
// state. Stale workers cannot write a successor's result.
func (store *PlanStore) FinishClaimedPlan(ctx context.Context, planID uint, tenantID string, claimToken uuid.UUID, succeeded bool, before map[string]any, after map[string]any, verified map[string]any) error {
	now := store.now()
	return store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var plan models.OperatorActionPlan
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=? AND tenant_id=?", planID, tenantID).First(&plan).Error; err != nil {
			return err
		}
		if plan.State != "running" || plan.ClaimToken == nil || *plan.ClaimToken != claimToken || plan.ClaimExpiresAt == nil || !now.Before(*plan.ClaimExpiresAt) {
			return fmt.Errorf("%w: plan completion claim is not current", ErrInvalidContract)
		}
		var step models.OperatorActionStep
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("plan_id=? AND ordinal=?", plan.ID, 1).First(&step).Error; err != nil {
			return err
		}
		if step.State != "running" || step.ClaimToken == nil || *step.ClaimToken != claimToken {
			return fmt.Errorf("%w: action step completion claim is not current", ErrInvalidContract)
		}
		beforeRaw, err := json.Marshal(before)
		if err != nil {
			return err
		}
		afterRaw, err := json.Marshal(after)
		if err != nil {
			return err
		}
		verifiedRaw, err := json.Marshal(verified)
		if err != nil {
			return err
		}
		stepState, planState := "failed", "failed"
		if succeeded {
			stepState, planState = "succeeded", "succeeded"
		}
		if err := tx.Model(&step).Updates(map[string]any{"state": stepState, "before_state": datatypes.JSON(beforeRaw), "after_state": datatypes.JSON(afterRaw), "verified_state": datatypes.JSON(verifiedRaw), "finished_at": now, "claim_token": nil, "claim_expires_at": nil}).Error; err != nil {
			return err
		}
		if err := tx.Model(&plan).Updates(map[string]any{"state": planState, "completed_at": now, "claim_token": nil, "claim_expires_at": nil}).Error; err != nil {
			return err
		}
		if err := tx.Model(&models.OperatorPlanApproval{}).Where("plan_id=? AND tenant_id=? AND actor_id=? AND plan_digest=? AND consumed_at IS NULL", plan.ID, tenantID, plan.ActorID, plan.Digest).Update("consumed_at", now).Error; err != nil {
			return err
		}
		return appendNextPlanEvent(tx, plan.ID, &step.ID, tenantID, "step_"+stepState, "worker", "", map[string]any{"before": before, "after": after, "verified": verified})
	})
}
