package operator

import (
	"context"
	"fmt"
	"time"

	"content-management-system/src/models"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const defaultPlanClaimLease = 2 * time.Minute

type AccessSnapshotProvider interface {
	Snapshot(ctx context.Context, userID, tenantID string) (AccessSnapshot, error)
}

// ClaimNextPlan makes a queued plan exclusive to one worker after a fresh
// IAM lookup. The external lookup occurs before the row lock; the transaction
// then rechecks the plan state, expiry, actor, tenant, and access version.
func (store *PlanStore) ClaimNextPlan(ctx context.Context, access AccessSnapshotProvider, requiredPermission string) (*models.OperatorActionPlan, error) {
	if access == nil {
		return nil, fmt.Errorf("%w: access snapshot provider is required", ErrAccessUnavailable)
	}
	var candidates []models.OperatorActionPlan
	now := store.now()
	if err := store.db.WithContext(ctx).Where("state=? AND expires_at>?", "queued", now).Order("created_at ASC").Limit(20).Find(&candidates).Error; err != nil {
		return nil, err
	}
	for _, candidate := range candidates {
		snapshot, err := access.Snapshot(ctx, candidate.ActorID, candidate.TenantID)
		if err != nil || snapshot.ValidateFor(candidate.ActorID, candidate.TenantID) != nil || !snapshot.HasPermission(requiredPermission) {
			continue // fail closed: another eligible plan may still be claimed.
		}
		claimed, err := store.claimPlan(ctx, candidate.ID, snapshot)
		if err == nil {
			return &claimed, nil
		}
	}
	return nil, gorm.ErrRecordNotFound
}

func (store *PlanStore) claimPlan(ctx context.Context, planID uint, snapshot AccessSnapshot) (models.OperatorActionPlan, error) {
	claimed, _, _, err := store.ClaimPlan(ctx, planID, snapshot)
	return claimed, err
}

// RecoverExpiredPlanClaims treats an interrupted claim differently depending
// on whether the tool began. A never-started claim may return to queued;
// work that may already have reached the tool moves to verification and is
// never blindly re-executed after a crash.
func (store *PlanStore) RecoverExpiredPlanClaims(ctx context.Context) (int, error) {
	now := store.now()
	var expired []models.OperatorActionPlan
	if err := store.db.WithContext(ctx).Where("state IN ? AND claim_expires_at<?", []string{"claimed", "running"}, now).Find(&expired).Error; err != nil {
		return 0, err
	}
	recovered := 0
	for _, candidate := range expired {
		err := store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var plan models.OperatorActionPlan
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&plan, candidate.ID).Error; err != nil {
				return err
			}
			if (plan.State != "claimed" && plan.State != "running") || plan.ClaimExpiresAt == nil || !plan.ClaimExpiresAt.Before(now) {
				return nil
			}
			var step models.OperatorActionStep
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("plan_id=? AND ordinal=?", plan.ID, 1).First(&step).Error; err != nil {
				return err
			}
			state, stepState, eventType := "queued", "pending", "claim_recovered"
			if plan.State == "running" {
				state, stepState, eventType = "verifying", "verifying", "execution_interrupted"
			}
			if err := tx.Model(&plan).Updates(map[string]any{"state": state, "claim_token": nil, "claim_expires_at": nil}).Error; err != nil {
				return err
			}
			if err := tx.Model(&step).Updates(map[string]any{"state": stepState, "claim_token": nil, "claim_expires_at": nil}).Error; err != nil {
				return err
			}
			if err := appendNextPlanEvent(tx, plan.ID, &step.ID, plan.TenantID, eventType, "system", "", map[string]any{"reason": "lease_expired"}); err != nil {
				return err
			}
			recovered++
			return nil
		})
		if err != nil {
			return recovered, err
		}
	}
	return recovered, nil
}
