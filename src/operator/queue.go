package operator

import (
	"context"
	"fmt"
	"time"

	"content-management-system/src/models"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const defaultPlanJobLease = 2 * time.Minute

// ClaimNextQueuedJob claims only the durable CMS job. IAM and runtime policy
// are deliberately resolved after this short lease and before PlanStore claims
// the signed plan, so no browser token can become worker authority.
func (store *PlanStore) ClaimNextQueuedJob(ctx context.Context) (models.OperatorActionJob, models.OperatorActionPlan, error) {
	now := store.now()
	leaseUntil := now.Add(defaultPlanJobLease)
	claimToken := uuid.New()
	var job models.OperatorActionJob
	var plan models.OperatorActionPlan
	err := store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("state=? AND available_at<=?", "queued", now).Order("available_at ASC").First(&job).Error; err != nil {
			return err
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&plan, job.PlanID).Error; err != nil {
			return err
		}
		if plan.State != "queued" || !now.Before(plan.ExpiresAt) {
			return fmt.Errorf("%w: queued job no longer has an executable plan", ErrInvalidContract)
		}
		if err := tx.Model(&job).Updates(map[string]any{"state": "authorizing", "attempt_count": gorm.Expr("attempt_count + 1"), "claim_token": claimToken, "claim_expires_at": leaseUntil}).Error; err != nil {
			return err
		}
		job.State, job.ClaimToken, job.ClaimExpiresAt, job.AttemptCount = "authorizing", &claimToken, &leaseUntil, job.AttemptCount+1
		return appendNextPlanEvent(tx, plan.ID, nil, plan.TenantID, "plan_authorizing", "worker", "", map[string]any{"job_id": job.PublicID.String(), "attempt": job.AttemptCount})
	})
	return job, plan, err
}

func (store *PlanStore) MarkQueuedJobClaimed(ctx context.Context, jobID uint, tenantID string, token uuid.UUID) error {
	return store.db.WithContext(ctx).Model(&models.OperatorActionJob{}).
		Where("id=? AND tenant_id=? AND state=? AND claim_token=?", jobID, tenantID, "authorizing", token).
		Updates(map[string]any{"state": "claimed"}).Error
}

// BlockQueuedJob stops unstarted work when current IAM authority or a control
// is unavailable. It never executes under stale permission and leaves an
// immutable reason on the signed plan ledger.
func (store *PlanStore) BlockQueuedJob(ctx context.Context, job models.OperatorActionJob, plan models.OperatorActionPlan, reason string) error {
	if reason == "" {
		reason = "execution_unavailable"
	}
	now := store.now()
	return store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&models.OperatorActionJob{}).Where("id=? AND state IN ?", job.ID, []string{"queued", "authorizing"}).Updates(map[string]any{"state": "blocked", "last_error_class": reason, "completed_at": now, "claim_token": nil, "claim_expires_at": nil}).Error; err != nil {
			return err
		}
		if err := tx.Model(&models.OperatorActionPlan{}).Where("id=? AND state=?", plan.ID, "queued").Updates(map[string]any{"state": "blocked", "completed_at": now, "claim_token": nil, "claim_expires_at": nil}).Error; err != nil {
			return err
		}
		return appendNextPlanEvent(tx, plan.ID, nil, plan.TenantID, "plan_blocked", "worker", "", map[string]any{"reason": reason})
	})
}

func (store *PlanStore) FinishQueuedJob(ctx context.Context, jobID uint, tenantID string, succeeded bool) error {
	now := store.now()
	state := "failed"
	if succeeded {
		state = "succeeded"
	}
	return store.db.WithContext(ctx).Model(&models.OperatorActionJob{}).Where("id=? AND tenant_id=?", jobID, tenantID).Updates(map[string]any{"state": state, "completed_at": now, "claim_token": nil, "claim_expires_at": nil}).Error
}

// RecoverExpiredJobs returns unstarted authorization work to the durable queue
// and reconciles jobs whose plan already reached a terminal state.
func (store *PlanStore) RecoverExpiredJobs(ctx context.Context) (int, error) {
	now := store.now()
	var jobs []models.OperatorActionJob
	if err := store.db.WithContext(ctx).Where("state IN ? AND claim_expires_at<?", []string{"authorizing", "claimed", "running", "verifying"}, now).Find(&jobs).Error; err != nil {
		return 0, err
	}
	recovered := 0
	for _, candidate := range jobs {
		err := store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var job models.OperatorActionJob
			var plan models.OperatorActionPlan
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&job, candidate.ID).Error; err != nil {
				return err
			}
			if job.ClaimExpiresAt == nil || !job.ClaimExpiresAt.Before(now) {
				return nil
			}
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&plan, job.PlanID).Error; err != nil {
				return err
			}
			updates := map[string]any{"claim_token": nil, "claim_expires_at": nil}
			switch plan.State {
			case "queued":
				updates["state"], updates["available_at"] = "queued", now
				if err := tx.Model(&job).Updates(updates).Error; err != nil {
					return err
				}
				if err := appendNextPlanEvent(tx, plan.ID, nil, plan.TenantID, "job_requeued", "system", "", map[string]any{"reason": "authorization_lease_expired"}); err != nil {
					return err
				}
			case "succeeded", "failed", "cancelled", "blocked":
				updates["state"], updates["completed_at"] = plan.State, now
				if err := tx.Model(&job).Updates(updates).Error; err != nil {
					return err
				}
			case "verifying", "running", "claimed":
				updates["state"] = "verifying"
				if err := tx.Model(&job).Updates(updates).Error; err != nil {
					return err
				}
			default:
				updates["state"], updates["last_error_class"], updates["completed_at"] = "blocked", "invalid_plan_state", now
				if err := tx.Model(&job).Updates(updates).Error; err != nil {
					return err
				}
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
