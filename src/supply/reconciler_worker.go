package supply

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"content-management-system/src/models"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	reconcilerWorkerInterval = 20 * time.Second
	reconcilerWorkerLease    = 45 * time.Second
	reconcilerBatchLimit     = 64
)

var reconcilerWorkerLastHeartbeat atomic.Int64

// StartReconcilerWorker repairs durable source-run convergence gaps. It owns
// no provider, queue, or executor effect: it only reaps expired leases and
// ensures that every effect-ambiguous unit has an idempotent verification
// task. Aggregation remains the independent observer that completes a task.
func StartReconcilerWorker(db *gorm.DB) {
	runReconcilerWorkerOnce(db)
	go func() {
		ticker := time.NewTicker(reconcilerWorkerInterval)
		defer ticker.Stop()
		for range ticker.C {
			runReconcilerWorkerOnce(db)
		}
	}()
}

// ReconcilerWorkerHealthy is deliberately separate from recovery health:
// recovery notices an expired lease, while reconciliation closes the durable
// verification-task gap left by an interrupted state transition.
func ReconcilerWorkerHealthy(now time.Time) bool {
	last := reconcilerWorkerLastHeartbeat.Load()
	if last <= 0 {
		return false
	}
	lastAt := time.Unix(0, last).UTC()
	return !now.UTC().Before(lastAt) && now.UTC().Sub(lastAt) <= reconcilerWorkerLease*2
}

func runReconcilerWorkerOnce(db *gorm.DB) {
	if _, err := ReconcileSourceRunWork(db, reconcilerBatchLimit); err != nil {
		log.Printf("source-run reconciliation failed: %v", err)
		return
	}
	reconcilerWorkerLastHeartbeat.Store(time.Now().UTC().UnixNano())
}

// ReconcileSourceRunWork is bounded and repeatable. It must be safe to run
// concurrently with recovery because both paths use row locks and the task
// key is tenant-scoped unique. It never re-authorizes an expired effect.
func ReconcileSourceRunWork(db *gorm.DB, limit int) (int, error) {
	if db == nil || limit < 1 || limit > 1000 {
		return 0, fmt.Errorf("source-run reconciliation requires a bounded database batch")
	}

	recovered, err := reapExpiredSourceRunWork(db, limit)
	if err != nil {
		return 0, err
	}
	remaining := limit - recovered
	if remaining < 1 {
		return recovered, nil
	}

	ensured, err := ensureMissingVerificationTasks(db, remaining)
	if err != nil {
		return recovered, err
	}
	return recovered + ensured, nil
}

func reapExpiredSourceRunWork(db *gorm.DB, limit int) (int, error) {
	expired, err := expireOverdueSourceRunRequests(db, limit)
	if err != nil {
		return 0, err
	}
	if expired >= limit {
		return expired, nil
	}
	if err := reapExpiredAttemptClaims(db, limit-expired); err != nil {
		return 0, err
	}
	var tenants []string
	if err := db.Model(&models.SourceRunExecutionUnit{}).
		Distinct("tenant_id").
		Where("unit_type <> ? AND state IN ? AND execution_lease_expires_at <= ?", "coordinator", []string{string(UnitAccepted), string(UnitRunning)}, time.Now().UTC()).
		Limit(limit).
		Pluck("tenant_id", &tenants).Error; err != nil {
		return 0, err
	}

	recovered := expired
	for _, tenantID := range tenants {
		if recovered >= limit {
			break
		}
		units, err := ReapExpiredUnitLeases(db, tenantID, limit-recovered)
		if err != nil {
			return recovered, err
		}
		recovered += len(units)
	}
	return recovered, nil
}

// expireOverdueSourceRunRequests releases reservations for work that never
// crossed an effect boundary. If any effect began, expiry becomes verification
// instead; it never converts uncertainty into a retry or a false release.
func expireOverdueSourceRunRequests(db *gorm.DB, limit int) (int, error) {
	now := time.Now().UTC()
	var candidates []models.SourceRunRequest
	if err := db.Where("state IN ? AND ((expires_at IS NOT NULL AND expires_at<=?) OR (deadline_at IS NOT NULL AND deadline_at<=?))", []string{string(RequestRequested), string(RequestAccepted), string(RequestRunning)}, now, now).Order("COALESCE(deadline_at, expires_at) ASC").Limit(limit).Find(&candidates).Error; err != nil {
		return 0, err
	}
	count := 0
	for _, selected := range candidates {
		err := db.Transaction(func(tx *gorm.DB) error {
			var request models.SourceRunRequest
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=? AND state IN ?", selected.TenantID, selected.PublicID, []string{string(RequestRequested), string(RequestAccepted), string(RequestRunning)}).First(&request).Error; err != nil {
				if err == gorm.ErrRecordNotFound {
					return nil
				}
				return err
			}
			var units []models.SourceRunExecutionUnit
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND source_run_request_id=?", request.TenantID, request.PublicID).Find(&units).Error; err != nil {
				return err
			}
			started := false
			for _, unit := range units {
				if unit.EffectStartedAt != nil {
					started = true
					break
				}
			}
			if started {
				for _, unit := range units {
					if IsTerminalUnit(ExecutionUnitState(unit.State)) {
						continue
					}
					if err := tx.Model(&unit).Updates(map[string]any{"state": string(UnitVerificationRequired), "verification_required": true}).Error; err != nil {
						return err
					}
					if unit.EffectStartedAt != nil {
						if _, err := ensureVerificationTask(tx, unit, "request_deadline_elapsed"); err != nil {
							return err
						}
					}
				}
				if err := tx.Model(&request).Updates(map[string]any{"state": string(RequestVerificationRequired), "evidence_state": "unknown"}).Error; err != nil {
					return err
				}
			} else {
				if err := tx.Model(&models.SourceRunExecutionUnit{}).Where("tenant_id=? AND source_run_request_id=? AND state NOT IN ?", request.TenantID, request.PublicID, []string{string(UnitSucceeded), string(UnitFailed), string(UnitCancelled), string(UnitExpired)}).Updates(map[string]any{"state": string(UnitExpired), "terminal_outcome": string(OutcomeUnknown), "finished_at": now}).Error; err != nil {
					return err
				}
				if err := tx.Model(&models.SourceRunAttempt{}).Where("tenant_id=? AND source_run_request_id=? AND state NOT IN ?", request.TenantID, request.PublicID, []string{string(AttemptSucceeded), string(AttemptPartial), string(AttemptBlocked), string(AttemptFailed), string(AttemptCancelled), string(AttemptExpired)}).Updates(map[string]any{"state": string(AttemptExpired), "finished_at": now}).Error; err != nil {
					return err
				}
				if err := settleSourceRunBudget(tx, request, now); err != nil {
					return err
				}
				if err := tx.Model(&request).Updates(map[string]any{"state": string(RequestExpired), "finished_at": now, "finalized_at": now, "evidence_state": "unknown"}).Error; err != nil {
					return err
				}
			}
			count++
			return nil
		})
		if err != nil {
			return count, err
		}
	}
	return count, nil
}

// ensureMissingVerificationTasks covers crash windows after a unit or attempt
// was marked verification_required but before its task insert committed. Only
// a unit with an already-started effect is selected; pre-effect expiry stays a
// terminal unknown outcome and is never converted into a retry here.
func ensureMissingVerificationTasks(db *gorm.DB, limit int) (int, error) {
	ensured := 0
	err := db.Transaction(func(tx *gorm.DB) error {
		var units []models.SourceRunExecutionUnit
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("state = ? AND effect_started_at IS NOT NULL", string(UnitVerificationRequired)).
			Order("updated_at ASC, public_id ASC").
			Limit(limit).
			Find(&units).Error; err != nil {
			return err
		}
		for _, unit := range units {
			if _, err := ensureVerificationTask(tx, unit, "reconciler"); err != nil {
				return err
			}
			if err := reconcileAttemptAndRequest(tx, unit.TenantID, unit.SourceRunAttemptID, unit.SourceRunRequestID); err != nil {
				return err
			}
			ensured++
		}
		return nil
	})
	return ensured, err
}
