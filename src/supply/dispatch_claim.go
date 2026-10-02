package supply

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"content-management-system/src/lifecycle"
	"content-management-system/src/models"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// DispatchClaim is the only CMS-issued envelope from which Aggregation may
// enqueue a durable source-run unit. It contains no queue name or caller
// supplied provider arguments; a later WP-04 dispatcher derives those from
// the tenant-scoped source configuration.
type DispatchClaim struct {
	Request         models.SourceRunRequest
	Source          models.ContentSource
	Attempt         models.SourceRunAttempt
	RootUnit        models.SourceRunExecutionUnit
	DispatcherToken uuid.UUID
	ExecutionToken  uuid.UUID
}

const maxDispatchCandidateScan = 16

// ClaimNextDispatchableRequest selects due work from CMS state, creates or
// adopts its immutable attempt/root unit, and issues bounded dispatcher and
// unit leases. Aggregation never selects a tenant, source, or queue payload.
func ClaimNextDispatchableRequest(db *gorm.DB, owner string, dispatcherLease, executionLease time.Duration) (DispatchClaim, bool, error) {
	if db == nil || strings.TrimSpace(owner) == "" || dispatcherLease <= 0 || executionLease <= 0 {
		return DispatchClaim{}, false, fmt.Errorf("dispatcher owner, database, and positive leases are required")
	}
	now := time.Now().UTC()
	var candidates []models.SourceRunRequest
	if err := db.Table("source_run_requests AS request").Select("request.*").Joins("JOIN content_sources AS source ON source.public_id = request.content_source_id AND source.tenant_id = request.tenant_id").Where("request.tenant_id <> '' AND request.budget_state = 'reserved' AND request.state IN ? AND (request.not_before_at IS NULL OR request.not_before_at <= ?) AND (request.next_dispatch_at IS NULL OR request.next_dispatch_at <= ?) AND (request.expires_at IS NULL OR request.expires_at > ?) AND (request.deadline_at IS NULL OR request.deadline_at > ?) AND source.is_active = TRUE AND (source.intake_circuit_until IS NULL OR source.intake_circuit_until <= ?)", []string{string(RequestRequested), string(RequestAccepted)}, now, now, now, now, now).Order("request.next_dispatch_at ASC NULLS FIRST, request.requested_at ASC, request.tenant_id ASC").Limit(maxDispatchCandidateScan).Find(&candidates).Error; err != nil {
		return DispatchClaim{}, false, err
	}
	for _, candidate := range candidates {
		if err := RequireDurableAdmission(db, candidate.TenantID, candidate.Lane); err != nil {
			// Old requests remain readable/auditable, but they cannot be
			// dispatched through the durable worker until their tenant/lane is
			// explicitly provisioned and the global protocol is activated.
			continue
		}
		var result DispatchClaim
		var skipCandidate bool
		err := db.Transaction(func(tx *gorm.DB) error {
			var source models.ContentSource
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ? AND is_active = TRUE", candidate.ContentSourceID, candidate.TenantID).First(&source).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					skipCandidate = true
					return nil
				}
				return err
			}
			lane := source.Category
			if lane == models.SourceCategoryMedia {
				lane = "pods"
			}
			if err := lifecycle.Check(tx, lifecycle.Scope{
				TenantID: candidate.TenantID,
				Lane:     lane,
				SourceID: source.PublicID.String(),
			}, lifecycle.PhaseSourceDispatch); err != nil {
				if lifecycle.IsConflict(err) || lifecycle.IsIntakePaused(err) {
					skipCandidate = true
					return nil
				}
				return err
			}

			var lease AttemptLease
			if candidate.Purpose == "content_reset_replay" {
				if _, err := ValidateContentResetReplayRequest(tx, candidate); err != nil {
					skipCandidate = true
					return nil
				}
			}
			if candidate.State == string(RequestRequested) {
				var err error
				lease, err = CreateAttemptAndRootUnit(tx, candidate.TenantID, candidate.PublicID.String())
				if err != nil {
					// A competing dispatcher may have won this candidate. Re-read
					// bounded candidates rather than treating it as provider failure.
					skipCandidate = true
					return nil
				}
			} else {
				var attempt models.SourceRunAttempt
				if err := tx.Where("tenant_id = ? AND source_run_request_id = ? AND state IN ?", candidate.TenantID, candidate.PublicID, []string{string(AttemptAuthorized), string(AttemptClaimed), string(AttemptRunning)}).Order("attempt_number DESC").First(&attempt).Error; err != nil {
					skipCandidate = true
					return nil
				}
				if attempt.RootExecutionUnitID == nil {
					skipCandidate = true
					return nil
				}
				var root models.SourceRunExecutionUnit
				if err := tx.Where("public_id = ? AND tenant_id = ?", *attempt.RootExecutionUnitID, candidate.TenantID).First(&root).Error; err != nil {
					skipCandidate = true
					return nil
				}
				lease = AttemptLease{Attempt: attempt, RootExecutionUnit: root}
			}
			claimed, err := ClaimAttempt(tx, candidate.TenantID, lease.Attempt.PublicID.String(), owner, dispatcherLease)
			if err != nil {
				return err
			}
			if claimed.DispatcherToken == nil {
				return fmt.Errorf("CMS did not issue a dispatcher claim token")
			}
			unitLease, err := AcquireUnitExecution(tx, candidate.TenantID, lease.RootExecutionUnit.PublicID.String(), owner, executionLease)
			if err != nil {
				return err
			}
			if err := tx.Model(&models.SourceRunRequest{}).Where("public_id = ? AND tenant_id = ? AND state = ?", candidate.PublicID, candidate.TenantID, string(RequestRequested)).Updates(map[string]any{"state": string(RequestAccepted), "accepted_at": now}).Error; err != nil {
				return err
			}
			candidate.State, candidate.AcceptedAt = string(RequestAccepted), &now
			result = DispatchClaim{Request: candidate, Source: source, Attempt: claimed, RootUnit: unitLease.Unit, DispatcherToken: *claimed.DispatcherToken, ExecutionToken: unitLease.LeaseToken}
			return nil
		})
		if err != nil {
			return DispatchClaim{}, false, err
		}
		if skipCandidate {
			continue
		}
		if result.Request.PublicID != uuid.Nil {
			return result, true, nil
		}
	}
	return DispatchClaim{}, false, nil
}

// VerifyExecutionEnvelope binds a route body to one exact CMS-issued unit
// before a begin or heartbeat call. It rejects sibling and stale job/fence
// combinations before the lease operation can mutate any state.
func VerifyExecutionEnvelope(db *gorm.DB, tenantID, requestID, attemptID, unitID, jobID, fence string) (models.SourceRunExecutionUnit, error) {
	if db == nil || strings.TrimSpace(tenantID) == "" || strings.TrimSpace(jobID) == "" {
		return models.SourceRunExecutionUnit{}, fmt.Errorf("execution envelope is incomplete")
	}
	requestUUID, err := uuid.Parse(strings.TrimSpace(requestID))
	if err != nil {
		return models.SourceRunExecutionUnit{}, fmt.Errorf("source-run request ID is invalid")
	}
	attemptUUID, err := uuid.Parse(strings.TrimSpace(attemptID))
	if err != nil {
		return models.SourceRunExecutionUnit{}, fmt.Errorf("source-run attempt ID is invalid")
	}
	unitUUID, err := uuid.Parse(strings.TrimSpace(unitID))
	if err != nil {
		return models.SourceRunExecutionUnit{}, fmt.Errorf("source-run execution-unit ID is invalid")
	}
	fenceUUID, err := uuid.Parse(strings.TrimSpace(fence))
	if err != nil {
		return models.SourceRunExecutionUnit{}, fmt.Errorf("source-run attempt fence is invalid")
	}
	var unit models.SourceRunExecutionUnit
	if err := db.Where("public_id = ? AND tenant_id = ? AND source_run_request_id = ? AND source_run_attempt_id = ?", unitUUID, tenantID, requestUUID, attemptUUID).First(&unit).Error; err != nil {
		return models.SourceRunExecutionUnit{}, err
	}
	if unit.JobID != strings.TrimSpace(jobID) || unit.AttemptFenceToken != fenceUUID {
		return models.SourceRunExecutionUnit{}, fmt.Errorf("execution envelope does not match the CMS-issued unit")
	}
	return unit, nil
}
