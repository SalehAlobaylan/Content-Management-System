package supply

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"content-management-system/src/models"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	recoveryWorkerInterval = 15 * time.Second
	recoveryWorkerLease    = 45 * time.Second
	recoveryBatchLimit     = 64
)

var recoveryWorkerLastHeartbeat atomic.Int64

// StartRecoveryWorker converges expired source-run ownership without provider
// I/O. It can release a pre-effect dispatcher claim for redelivery, but every
// unit that crossed BeginUnitEffect is sent to verification rather than rerun.
func StartRecoveryWorker(db *gorm.DB) {
	owner := "cms-source-run-recovery-" + uuid.NewString()
	runRecoveryWorkerOnce(db, owner)
	go func() {
		ticker := time.NewTicker(recoveryWorkerInterval)
		defer ticker.Stop()
		for range ticker.C {
			runRecoveryWorkerOnce(db, owner)
		}
	}()
}

func RecoveryWorkerHealthy(now time.Time) bool {
	last := recoveryWorkerLastHeartbeat.Load()
	return last > 0 && now.UTC().Sub(time.Unix(0, last)) <= recoveryWorkerLease*2
}

func runRecoveryWorkerOnce(db *gorm.DB, _ string) {
	if err := ReapExpiredSourceRunWork(db, recoveryBatchLimit); err != nil {
		log.Printf("source-run recovery failed: %v", err)
		return
	}
	recoveryWorkerLastHeartbeat.Store(time.Now().UTC().UnixNano())
}

// ReapExpiredSourceRunWork is bounded global convergence. Tenant identity is
// always read from the durable row; callers cannot choose a default tenant.
func ReapExpiredSourceRunWork(db *gorm.DB, limit int) error {
	if db == nil || limit < 1 || limit > 1000 {
		return fmt.Errorf("source-run recovery requires a bounded limit")
	}
	_, err := reapExpiredSourceRunWork(db, limit)
	return err
}

// ReclaimExpiredDispatchClaim performs the same pre-effect convergence as the
// bounded reconciler, but for one exact attempt selected by a signed recovery
// action. It cannot allocate a new attempt or touch a provider: the original
// fence returns to authorized only when no unit crossed its effect boundary.
func ReclaimExpiredDispatchClaim(db *gorm.DB, tenantID, attemptID string) (models.SourceRunAttempt, error) {
	if db == nil || tenantID == "" {
		return models.SourceRunAttempt{}, fmt.Errorf("explicit tenant and database are required")
	}
	publicID, err := uuid.Parse(attemptID)
	if err != nil {
		return models.SourceRunAttempt{}, fmt.Errorf("source-run attempt ID is invalid")
	}
	var reclaimed models.SourceRunAttempt
	now := time.Now().UTC()
	err = db.Transaction(func(tx *gorm.DB) error {
		var attempt models.SourceRunAttempt
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ? AND state = ? AND dispatcher_lease_expires_at <= ?", publicID, tenantID, string(AttemptClaimed), now).First(&attempt).Error; err != nil {
			return err
		}
		var started int64
		if err := tx.Model(&models.SourceRunExecutionUnit{}).Where("tenant_id = ? AND source_run_attempt_id = ? AND effect_started_at IS NOT NULL", tenantID, attempt.PublicID).Count(&started).Error; err != nil {
			return err
		}
		if started != 0 {
			return fmt.Errorf("expired dispatcher claim crossed an effect boundary")
		}
		result := tx.Model(&attempt).Where("public_id = ? AND tenant_id = ? AND state = ? AND dispatcher_lease_expires_at <= ?", attempt.PublicID, tenantID, string(AttemptClaimed), now).Updates(map[string]any{"state": string(AttemptAuthorized), "dispatcher_owner": "", "dispatcher_token": nil, "dispatcher_lease_expires_at": nil, "heartbeat_at": now})
		if result.Error != nil || result.RowsAffected != 1 {
			if result.Error != nil {
				return result.Error
			}
			return fmt.Errorf("expired dispatcher claim changed before reclaim")
		}
		if err := tx.Model(&models.SourceRunRequest{}).Where("public_id = ? AND tenant_id = ? AND state = ?", attempt.SourceRunRequestID, tenantID, string(RequestAccepted)).Update("next_dispatch_at", now).Error; err != nil {
			return err
		}
		attempt.State, attempt.DispatcherOwner, attempt.DispatcherToken, attempt.DispatcherLeaseExpiresAt, attempt.HeartbeatAt = string(AttemptAuthorized), "", nil, nil, &now
		reclaimed = attempt
		return nil
	})
	return reclaimed, err
}

func reapExpiredAttemptClaims(db *gorm.DB, limit int) error {
	now := time.Now().UTC()
	return db.Transaction(func(tx *gorm.DB) error {
		var attempts []models.SourceRunAttempt
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("state IN ? AND dispatcher_lease_expires_at <= ?", []string{string(AttemptClaimed), string(AttemptRunning)}, now).
			Order("dispatcher_lease_expires_at ASC").Limit(limit).Find(&attempts).Error; err != nil {
			return err
		}
		for _, attempt := range attempts {
			var started int64
			if err := tx.Model(&models.SourceRunExecutionUnit{}).Where("tenant_id = ? AND source_run_attempt_id = ? AND effect_started_at IS NOT NULL", attempt.TenantID, attempt.PublicID).Count(&started).Error; err != nil {
				return err
			}
			if started == 0 {
				// No side effect crossed the boundary. Return this exact attempt to
				// CMS dispatch rather than allocating a new fence/attempt.
				if err := tx.Model(&models.SourceRunAttempt{}).Where("public_id = ? AND tenant_id = ? AND dispatcher_lease_expires_at <= ?", attempt.PublicID, attempt.TenantID, now).Updates(map[string]any{"state": string(AttemptAuthorized), "dispatcher_owner": "", "dispatcher_token": nil, "dispatcher_lease_expires_at": nil, "heartbeat_at": now}).Error; err != nil {
					return err
				}
				if err := tx.Model(&models.SourceRunRequest{}).Where("public_id = ? AND tenant_id = ? AND state = ?", attempt.SourceRunRequestID, attempt.TenantID, string(RequestAccepted)).Update("next_dispatch_at", now).Error; err != nil {
					return err
				}
				continue
			}
			var liveEffectLease int64
			if err := tx.Model(&models.SourceRunExecutionUnit{}).Where("tenant_id = ? AND source_run_attempt_id = ? AND effect_started_at IS NOT NULL AND state IN ? AND execution_lease_expires_at > ?", attempt.TenantID, attempt.PublicID, []string{string(UnitAccepted), string(UnitRunning)}, now).Count(&liveEffectLease).Error; err != nil {
				return err
			}
			// The dispatcher only expands the manifest; a page/batch executor owns
			// its own lease after that handoff. Do not manufacture uncertainty just
			// because the short dispatcher lease elapsed while a live executor is
			// correctly renewing its current unit lease.
			if liveEffectLease > 0 {
				continue
			}
			if err := tx.Model(&models.SourceRunAttempt{}).Where("public_id = ? AND tenant_id = ?", attempt.PublicID, attempt.TenantID).Updates(map[string]any{"state": string(AttemptVerificationRequired), "verification_required_at": now, "failure_class": "expired_dispatcher_lease", "failure_summary": "dispatcher ownership expired after an effect began"}).Error; err != nil {
				return err
			}
			if err := requireVerificationForAttempt(tx, attempt.TenantID, attempt.PublicID, "expired_dispatcher_lease"); err != nil {
				return err
			}
		}
		return nil
	})
}
