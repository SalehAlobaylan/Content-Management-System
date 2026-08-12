package supply

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"content-management-system/src/models"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	supplyActionWorkerInterval = 10 * time.Second
	supplyActionWorkerLease    = 45 * time.Second
	supplyActionWorkerBatch    = 1
)

var supplyActionWorkerLastHeartbeat atomic.Int64

// StartSupplyActionWorker is CMS-only. It intentionally claims only static
// adapters whose deterministic execution owner is CMS; Aggregation-owned
// adoption/redelivery remain unclaimable until their own typed handshakes are
// installed.
func StartSupplyActionWorker(db *gorm.DB) {
	runSupplyActionWorkerOnce(db)
	go func() {
		ticker := time.NewTicker(supplyActionWorkerInterval)
		defer ticker.Stop()
		for range ticker.C {
			runSupplyActionWorkerOnce(db)
		}
	}()
}

func SupplyActionWorkerHealthy(now time.Time) bool {
	last := supplyActionWorkerLastHeartbeat.Load()
	if last <= 0 {
		return false
	}
	lastAt := time.Unix(0, last).UTC()
	return !now.UTC().Before(lastAt) && now.UTC().Sub(lastAt) <= supplyActionWorkerLease*2
}

func runSupplyActionWorkerOnce(db *gorm.DB) {
	if _, err := RecoverExpiredSupplyActionClaims(db, supplyActionWorkerBatch); err != nil {
		log.Printf("media supply action recovery failed: %v", err)
		return
	}
	if verified, err := verifyOneCMSOwnedSupplyAction(db); err != nil {
		log.Printf("media supply action verification failed: %v", err)
		return
	} else if verified {
		supplyActionWorkerLastHeartbeat.Store(time.Now().UTC().UnixNano())
		return
	}
	if verified, err := verifyOneAggregationOwnedSupplyAction(db); err != nil {
		log.Printf("media supply aggregation action verification failed: %v", err)
		return
	} else if verified {
		supplyActionWorkerLastHeartbeat.Store(time.Now().UTC().UnixNano())
		return
	}
	lease, found, err := claimNextCMSOwnedSupplyAction(db, "cms-media-supply-action-worker")
	if err != nil {
		log.Printf("media supply action claim failed: %v", err)
		return
	}
	supplyActionWorkerLastHeartbeat.Store(time.Now().UTC().UnixNano())
	if !found {
		return
	}
	if accessErr := RecheckSupplyActionAccess(context.Background(), lease.Request); accessErr != nil {
		if err := TransitionSupplyActionForAccessFailure(db, lease.Request.TenantID, lease.Request.PublicID.String(), lease.ClaimToken.String(), accessErr); err != nil {
			log.Printf("media supply action %s authorization transition failed: %v", lease.Request.PublicID, err)
		}
		return
	}
	allowed, _, controlErr := MayExecuteSupplyAction(db, lease.Request.TenantID, lease.Request.ActionKey)
	if controlErr != nil {
		log.Printf("media supply action %s control recheck failed: %v", lease.Request.PublicID, controlErr)
		return
	}
	if !allowed {
		if err := releaseSupplyActionClaimForControl(db, lease); err != nil {
			log.Printf("media supply action %s could not be released after control change: %v", lease.Request.PublicID, err)
		}
		return
	}
	if err := executeCMSOwnedSupplyAction(db, lease); err != nil {
		log.Printf("media supply action %s failed: %v", lease.Request.PublicID, err)
		_ = failSupplyActionBeforeEffect(db, lease, "cms_adapter_failed")
	}
}

// releaseSupplyActionClaimForControl leaves a pre-effect request eligible for
// a later, explicitly re-authorized claim. A subtractive control is not an
// operational failure and must never erase the cancellation/verification path.
func releaseSupplyActionClaimForControl(db *gorm.DB, lease SupplyActionLease) error {
	return db.Transaction(func(tx *gorm.DB) error {
		var request models.MediaSupplyActionRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", lease.Request.PublicID, lease.Request.TenantID).First(&request).Error; err != nil {
			return err
		}
		var attempt models.MediaSupplyActionAttempt
		if err := tx.Where("public_id = ? AND tenant_id = ?", lease.Attempt.PublicID, lease.Request.TenantID).First(&attempt).Error; err != nil {
			return err
		}
		if request.State != string(SupplyActionClaimed) || request.ClaimToken == nil || *request.ClaimToken != lease.ClaimToken || attempt.EffectStartedAt != nil {
			return fmt.Errorf("media supply action claim already crossed its effect boundary")
		}
		now := time.Now().UTC()
		if err := tx.Model(&request).Updates(map[string]any{"state": string(SupplyActionQueued), "claim_owner": "", "claim_token": nil, "claim_expires_at": nil}).Error; err != nil {
			return err
		}
		if err := tx.Model(&attempt).Updates(map[string]any{"state": string(SupplyActionCancelled), "finished_at": now}).Error; err != nil {
			return err
		}
		request.State, request.ClaimOwner, request.ClaimToken, request.ClaimExpiresAt = string(SupplyActionQueued), "", nil, nil
		attempt.State, attempt.FinishedAt = string(SupplyActionCancelled), &now
		return appendSupplyActionEvent(tx, request, &attempt, "claim_released_by_control", map[string]any{})
	})
}

func claimNextCMSOwnedSupplyAction(db *gorm.DB, owner string) (SupplyActionLease, bool, error) {
	if db == nil {
		return SupplyActionLease{}, false, fmt.Errorf("media supply action worker requires a database")
	}
	now := time.Now().UTC()
	var lease SupplyActionLease
	err := db.Transaction(func(tx *gorm.DB) error {
		// A disabled queued action must not starve other independently-enabled
		// actions. Lock a bounded ordered window and claim the first one whose
		// current subtractive controls still permit recovery.
		var queued []models.MediaSupplyActionRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Where("state = ? AND execution_owner = ?", string(SupplyActionQueued), "cms").Order("created_at ASC").Limit(16).Find(&queued).Error; err != nil {
			return err
		}
		if len(queued) == 0 {
			return nil
		}
		var request models.MediaSupplyActionRequest
		for _, candidate := range queued {
			candidateDescriptor, descriptorErr := RequireSupplyActionDescriptor(candidate.ActionKey, candidate.TargetType)
			if descriptorErr != nil || candidateDescriptor.ExecutionOwner != "cms" {
				return fmt.Errorf("CMS supply action is not statically admitted")
			}
			allowed, _, controlErr := MayExecuteSupplyAction(tx, candidate.TenantID, candidateDescriptor.Key)
			if controlErr != nil {
				return fmt.Errorf("media supply action control could not be checked: %w", controlErr)
			}
			if !allowed {
				continue
			}
			if authorityErr := RecheckSupplyActionExecutionAuthority(tx, candidate); authorityErr != nil {
				continue
			}
			request = candidate
			break
		}
		if request.PublicID == uuid.Nil {
			return nil
		}
		var attempts int64
		if err := tx.Model(&models.MediaSupplyActionAttempt{}).Where("tenant_id = ? AND action_request_id = ?", request.TenantID, request.PublicID).Count(&attempts).Error; err != nil {
			return err
		}
		if attempts >= 2 {
			proof := datatypes.JSON([]byte(`{"schema_version":"media-supply-action-proof/v1","verified":"absent","reason":"attempt_budget_exhausted"}`))
			if err := tx.Model(&request).Updates(map[string]any{"state": string(SupplyActionFailed), "failure_class": "attempt_budget_exhausted", "verified_effects": proof, "finished_at": now, "claim_expires_at": nil}).Error; err != nil {
				return err
			}
			if err := appendSupplyActionEvent(tx, request, nil, "failed", map[string]any{"failure_class": "attempt_budget_exhausted"}); err != nil {
				return err
			}
			return nil
		}
		token, expires := uuid.New(), now.Add(supplyActionWorkerLease)
		attempt := models.MediaSupplyActionAttempt{PublicID: uuid.New(), TenantID: request.TenantID, ActionRequestID: request.PublicID, AttemptNumber: int(attempts) + 1, State: string(SupplyActionClaimed), FenceToken: uuid.New(), OwnerProtocol: "cms"}
		if err := tx.Create(&attempt).Error; err != nil {
			return err
		}
		if err := tx.Model(&request).Updates(map[string]any{"state": string(SupplyActionClaimed), "claim_owner": owner, "claim_token": token, "claim_epoch": gorm.Expr("claim_epoch + 1"), "claim_expires_at": expires}).Error; err != nil {
			return err
		}
		request.State, request.ClaimOwner, request.ClaimToken, request.ClaimEpoch, request.ClaimExpiresAt = string(SupplyActionClaimed), owner, &token, request.ClaimEpoch+1, &expires
		lease = SupplyActionLease{Request: request, Attempt: attempt, ClaimToken: token}
		return appendSupplyActionEvent(tx, request, &attempt, "claimed", map[string]any{"owner": owner, "attempt": attempt.AttemptNumber})
	})
	if err != nil || lease.Request.PublicID == uuid.Nil {
		return lease, false, err
	}
	return lease, true, nil
}

func executeCMSOwnedSupplyAction(db *gorm.DB, lease SupplyActionLease) error {
	switch lease.Request.ActionKey {
	case SupplyActionRepairMissedAdmission:
		return executeRepairMissedAdmissionSupplyAction(db, lease)
	case SupplyActionReclaimDispatchClaim:
		return executeReclaimDispatchClaimSupplyAction(db, lease)
	case SupplyActionVerifyEffect:
		return executeVerifyEffectSupplyAction(db, lease)
	case SupplyActionTransferUnitLease:
		return executeTransferUnitLeaseSupplyAction(db, lease)
	case SupplyActionFinalizeVerifiedNoChange:
		return executeFinalizeVerifiedNoChangeSupplyAction(db, lease)
	case SupplyActionCancelUnstarted:
		return executeCancelUnstartedSupplyAction(db, lease)
	case SupplyActionFeedGenerationAttachVerifiedMember:
		return executeFeedMembershipRepair(db, lease)
	default:
		// A registered descriptor without its complete native adapter remains
		// non-executable. It stays visible as unavailable rather than falling
		// through a generic worker dispatch.
		return fmt.Errorf("CMS supply action adapter is not installed")
	}
}

// executeTransferUnitLeaseSupplyAction releases only an expired pre-effect
// lease. It never assigns a successor: the normal CMS-issued dispatcher
// claim is the only component allowed to acquire the next lease. This avoids
// turning an operator action into arbitrary queue or worker control.
func executeTransferUnitLeaseSupplyAction(db *gorm.DB, lease SupplyActionLease) error {
	var before models.SourceRunExecutionUnit
	now := time.Now().UTC()
	if err := db.Where("public_id = ? AND tenant_id = ? AND state = ? AND effect_started_at IS NULL AND cancellation_requested_at IS NULL AND execution_lease_expires_at <= ?", lease.Request.TargetID, lease.Request.TenantID, string(UnitAccepted), now).First(&before).Error; err != nil {
		return fmt.Errorf("source-run execution unit is no longer an expired pre-effect lease: %w", err)
	}
	var receipts int64
	if err := db.Model(&models.SourceRunReceipt{}).Where("tenant_id = ? AND execution_unit_id = ?", lease.Request.TenantID, before.PublicID).Count(&receipts).Error; err != nil || receipts != 0 {
		if err != nil {
			return err
		}
		return fmt.Errorf("source-run execution unit has retained receipt evidence")
	}
	if err := recordSupplyActionEffects(db, lease, "before_effects", map[string]any{"schema_version": "media-supply-action-effects/v1", "execution_unit_id": before.PublicID.String(), "unit_state": before.State, "effect_started": false, "receipt_count": receipts}); err != nil {
		return err
	}
	if _, err := BeginSupplyActionEffect(db, lease.Request.TenantID, lease.Request.PublicID.String(), lease.Request.ClaimOwner, lease.ClaimToken.String()); err != nil {
		return err
	}
	unit, err := releaseVerifiedNoEffectUnitLease(db, lease.Request.TenantID, lease.Request.TargetID)
	if err != nil {
		return err
	}
	if err := recordSupplyActionEffects(db, lease, "after_effects", map[string]any{"schema_version": "media-supply-action-effects/v1", "execution_unit_id": unit.PublicID.String(), "unit_state": string(UnitAuthorized), "next_owner": "cms_dispatcher_claim"}); err != nil {
		return err
	}
	return terminalizeSupplyAction(db, lease, true, datatypes.JSON([]byte(`{"schema_version":"media-supply-action-proof/v1","verified":"expired_pre_effect_lease_released","execution_unit_id":"`+unit.PublicID.String()+`"}`)))
}

// releaseVerifiedNoEffectUnitLease makes the evidence boundary explicit. A
// missing receipt is not enough: the lease must be expired, the unit must
// still be accepted, and CMS must have no recorded effect start.  Any other
// state is potentially observable work and must go through verification.
func releaseVerifiedNoEffectUnitLease(db *gorm.DB, tenantID string, unitID uuid.UUID) (models.SourceRunExecutionUnit, error) {
	if db == nil || tenantID == "" || unitID == uuid.Nil {
		return models.SourceRunExecutionUnit{}, fmt.Errorf("verified no-effect lease release is invalid")
	}
	now := time.Now().UTC()
	var unit models.SourceRunExecutionUnit
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", unitID, tenantID).First(&unit).Error; err != nil {
			return err
		}
		if unit.State != string(UnitAccepted) || unit.EffectStartedAt != nil || unit.ExecutionLeaseExpiresAt == nil || unit.ExecutionLeaseExpiresAt.After(now) || unit.CancellationRequestedAt != nil {
			return fmt.Errorf("source-run execution unit is not an expired pre-effect lease")
		}
		var receipts int64
		if err := tx.Model(&models.SourceRunReceipt{}).Where("tenant_id = ? AND execution_unit_id = ?", tenantID, unit.PublicID).Count(&receipts).Error; err != nil {
			return err
		}
		if receipts != 0 {
			return fmt.Errorf("source-run execution unit has retained receipt evidence")
		}
		if err := tx.Model(&unit).Updates(map[string]any{"state": string(UnitAuthorized), "execution_owner": "", "execution_lease_token": nil, "execution_lease_expires_at": nil, "heartbeat_at": now}).Error; err != nil {
			return err
		}
		unit.State, unit.ExecutionOwner, unit.ExecutionLeaseToken, unit.ExecutionLeaseExpiresAt, unit.HeartbeatAt = string(UnitAuthorized), "", nil, nil, &now
		return nil
	})
	return unit, err
}

// executeFinalizeVerifiedNoChangeSupplyAction is deliberately narrow. The
// verifier remains the authority for its observation; this action only
// reconciles a request after an already-terminal, present/no-change verifier
// event. It cannot turn unknown evidence into completion.
func executeFinalizeVerifiedNoChangeSupplyAction(db *gorm.DB, lease SupplyActionLease) error {
	var unit models.SourceRunExecutionUnit
	if err := db.Where("public_id = ? AND tenant_id = ? AND state = ? AND terminal_outcome = ?", lease.Request.TargetID, lease.Request.TenantID, string(UnitSucceeded), string(OutcomeNoChange)).First(&unit).Error; err != nil {
		return fmt.Errorf("execution unit has no verified no-change terminal proof: %w", err)
	}
	var task models.SourceRunVerificationTask
	if err := db.Where("tenant_id = ? AND execution_unit_id = ? AND state = ? AND terminal_verdict = ?", lease.Request.TenantID, unit.PublicID, models.SourceRunVerificationTaskTerminal, string(VerdictPresent)).Order("updated_at DESC").First(&task).Error; err != nil {
		return fmt.Errorf("execution unit has no present verifier proof: %w", err)
	}
	if err := recordSupplyActionEffects(db, lease, "before_effects", map[string]any{"schema_version": "media-supply-action-effects/v1", "execution_unit_id": unit.PublicID.String(), "unit_state": unit.State, "terminal_outcome": unit.TerminalOutcome, "verification_task_id": task.PublicID.String(), "verification_verdict": task.TerminalVerdict}); err != nil {
		return err
	}
	if _, err := BeginSupplyActionEffect(db, lease.Request.TenantID, lease.Request.PublicID.String(), lease.Request.ClaimOwner, lease.ClaimToken.String()); err != nil {
		return err
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		return reconcileAttemptAndRequest(tx, lease.Request.TenantID, unit.SourceRunAttemptID, unit.SourceRunRequestID)
	}); err != nil {
		return err
	}
	var request models.SourceRunRequest
	if err := db.Where("public_id = ? AND tenant_id = ?", unit.SourceRunRequestID, lease.Request.TenantID).First(&request).Error; err != nil {
		return err
	}
	if err := recordSupplyActionEffects(db, lease, "after_effects", map[string]any{"schema_version": "media-supply-action-effects/v1", "source_run_request_id": request.PublicID.String(), "request_state": request.State, "execution_unit_id": unit.PublicID.String(), "terminal_outcome": unit.TerminalOutcome}); err != nil {
		return err
	}
	return terminalizeSupplyAction(db, lease, request.State == string(RequestSucceeded), datatypes.JSON([]byte(`{"schema_version":"media-supply-action-proof/v1","verified":"present_no_change_reconciled","source_run_request_id":"`+request.PublicID.String()+`","verification_task_id":"`+task.PublicID.String()+`"}`)))
}

func executeVerifyEffectSupplyAction(db *gorm.DB, lease SupplyActionLease) error {
	var unit models.SourceRunExecutionUnit
	if err := db.Where("public_id = ? AND tenant_id = ? AND state = ? AND effect_started_at IS NOT NULL", lease.Request.TargetID, lease.Request.TenantID, string(UnitVerificationRequired)).First(&unit).Error; err != nil {
		return fmt.Errorf("execution unit no longer needs verification: %w", err)
	}
	if err := recordSupplyActionEffects(db, lease, "before_effects", map[string]any{"schema_version": "media-supply-action-effects/v1", "unit_id": unit.PublicID.String(), "unit_state": unit.State, "verification_required": unit.VerificationRequired}); err != nil {
		return err
	}
	if _, err := BeginSupplyActionEffect(db, lease.Request.TenantID, lease.Request.PublicID.String(), lease.Request.ClaimOwner, lease.ClaimToken.String()); err != nil {
		return err
	}
	task, err := EnsureUnitVerificationTask(db, lease.Request.TenantID, unit.PublicID.String(), "signed_supply_action")
	if err != nil {
		return err
	}
	if err := recordSupplyActionEffects(db, lease, "after_effects", map[string]any{"schema_version": "media-supply-action-effects/v1", "verification_task_id": task.PublicID.String(), "verification_task_state": task.State}); err != nil {
		return err
	}
	return terminalizeSupplyAction(db, lease, task.TenantID == lease.Request.TenantID && task.ExecutionUnitID != nil && *task.ExecutionUnitID == unit.PublicID, datatypes.JSON([]byte(`{"schema_version":"media-supply-action-proof/v1","verified":"verification_task_ensured","verification_task_id":"`+task.PublicID.String()+`"}`)))
}

func executeReclaimDispatchClaimSupplyAction(db *gorm.DB, lease SupplyActionLease) error {
	var attempt models.SourceRunAttempt
	now := time.Now().UTC()
	if err := db.Where("public_id = ? AND tenant_id = ? AND state = ? AND dispatcher_lease_expires_at <= ?", lease.Request.TargetID, lease.Request.TenantID, string(AttemptClaimed), now).First(&attempt).Error; err != nil {
		return fmt.Errorf("dispatcher claim is no longer reclaimable: %w", err)
	}
	var started int64
	if err := db.Model(&models.SourceRunExecutionUnit{}).Where("tenant_id = ? AND source_run_attempt_id = ? AND effect_started_at IS NOT NULL", lease.Request.TenantID, attempt.PublicID).Count(&started).Error; err != nil {
		return err
	}
	if started != 0 {
		return fmt.Errorf("dispatcher claim crossed an effect boundary")
	}
	if err := recordSupplyActionEffects(db, lease, "before_effects", map[string]any{"schema_version": "media-supply-action-effects/v1", "source_run_attempt_id": attempt.PublicID.String(), "attempt_state": attempt.State, "dispatcher_lease_expires_at": attempt.DispatcherLeaseExpiresAt}); err != nil {
		return err
	}
	if _, err := BeginSupplyActionEffect(db, lease.Request.TenantID, lease.Request.PublicID.String(), lease.Request.ClaimOwner, lease.ClaimToken.String()); err != nil {
		return err
	}
	reclaimed, err := ReclaimExpiredDispatchClaim(db, lease.Request.TenantID, attempt.PublicID.String())
	if err != nil {
		return err
	}
	if err := recordSupplyActionEffects(db, lease, "after_effects", map[string]any{"schema_version": "media-supply-action-effects/v1", "source_run_attempt_id": reclaimed.PublicID.String(), "attempt_state": reclaimed.State, "next_dispatch_admitted": true}); err != nil {
		return err
	}
	return terminalizeSupplyAction(db, lease, reclaimed.State == string(AttemptAuthorized), datatypes.JSON([]byte(`{"schema_version":"media-supply-action-proof/v1","verified":"expired_dispatch_claim_reclaimed","source_run_attempt_id":"`+reclaimed.PublicID.String()+`"}`)))
}

// executeRepairMissedAdmissionSupplyAction creates only the CMS-owned durable
// request. It never dispatches or selects provider work. The effect identity
// is action-specific, so verification after a lost acknowledgement can prove
// the request instead of blindly admitting another one.
func executeRepairMissedAdmissionSupplyAction(db *gorm.DB, lease SupplyActionLease) error {
	var source models.ContentSource
	now := time.Now().UTC()
	if err := db.Where("public_id = ? AND tenant_id = ? AND is_active = TRUE AND next_due_at IS NOT NULL AND next_due_at <= ? AND (intake_circuit_until IS NULL OR intake_circuit_until <= ?)", lease.Request.TargetID, lease.Request.TenantID, now, now).First(&source).Error; err != nil {
		return fmt.Errorf("source is no longer due for admission: %w", err)
	}
	if err := RequireDurableAdmission(db, source.TenantID, source.Category); err != nil {
		return fmt.Errorf("durable admission is not ready: %w", err)
	}
	var active int64
	if err := db.Model(&models.SourceRunRequest{}).Where("tenant_id = ? AND content_source_id = ? AND state IN ?", source.TenantID, source.PublicID, []string{string(RequestRequested), string(RequestAccepted), string(RequestRunning), string(RequestVerificationRequired)}).Count(&active).Error; err != nil {
		return err
	}
	if active != 0 {
		return fmt.Errorf("source already has an active durable request")
	}
	if err := recordSupplyActionEffects(db, lease, "before_effects", map[string]any{"schema_version": "media-supply-action-effects/v1", "content_source_id": source.PublicID.String(), "next_due_at": source.NextDueAt, "active_request_count": active}); err != nil {
		return err
	}
	if _, err := BeginSupplyActionEffect(db, lease.Request.TenantID, lease.Request.PublicID.String(), lease.Request.ClaimOwner, lease.ClaimToken.String()); err != nil {
		return err
	}
	identity, err := scheduledRequestIdentity(source)
	if err != nil {
		return err
	}
	identity.Purpose = "missed_admission_repair"
	evidence := "supply-action-admission:" + lease.Request.PublicID.String()
	metadata := datatypes.JSON([]byte(`{"schema_version":"media-supply-action/v1","action_request_id":"` + lease.Request.PublicID.String() + `"}`))
	request, _, err := CreateRequest(db, CreateRequestInput{Source: source, Identity: identity, RequestedBy: "system", RequestedByActorID: "media-supply-action-worker", EvidenceFingerprint: evidence, Metadata: metadata})
	if err != nil {
		return err
	}
	if err := recordSupplyActionEffects(db, lease, "after_effects", map[string]any{"schema_version": "media-supply-action-effects/v1", "source_run_request_id": request.PublicID.String(), "request_state": request.State, "request_evidence_fingerprint": request.EvidenceFingerprint}); err != nil {
		return err
	}
	return terminalizeSupplyAction(db, lease, request.TenantID == lease.Request.TenantID && request.ContentSourceID == source.PublicID && request.EvidenceFingerprint == evidence, datatypes.JSON([]byte(`{"schema_version":"media-supply-action-proof/v1","verified":"durable_request_admitted","source_run_request_id":"`+request.PublicID.String()+`"}`)))
}

// verifyOneCMSOwnedSupplyAction converges only requests that crossed their
// effect boundary and were moved to verifying by cancellation or lease
// recovery. There is no retry here: a missing proof remains uncertain.
func verifyOneCMSOwnedSupplyAction(db *gorm.DB) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("media supply action verifier requires a database")
	}
	var request models.MediaSupplyActionRequest
	if err := db.Where("state = ? AND execution_owner = ?", string(SupplyActionVerifying), "cms").Order("updated_at ASC").First(&request).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return false, nil
		}
		return false, err
	}
	switch request.ActionKey {
	case SupplyActionRepairMissedAdmission:
		evidence := "supply-action-admission:" + request.PublicID.String()
		var sourceRun models.SourceRunRequest
		err := db.Where("tenant_id = ? AND content_source_id = ? AND evidence_fingerprint = ?", request.TenantID, request.TargetID, evidence).First(&sourceRun).Error
		if err == gorm.ErrRecordNotFound {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		return true, terminalizeVerifiedSupplyAction(db, request, true, datatypes.JSON([]byte(`{"schema_version":"media-supply-action-proof/v1","verified":"durable_request_admitted_after_recovery","source_run_request_id":"`+sourceRun.PublicID.String()+`"}`)))
	case SupplyActionReclaimDispatchClaim:
		var attempt models.SourceRunAttempt
		err := db.Where("public_id = ? AND tenant_id = ? AND state = ? AND dispatcher_token IS NULL", request.TargetID, request.TenantID, string(AttemptAuthorized)).First(&attempt).Error
		if err == gorm.ErrRecordNotFound {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		return true, terminalizeVerifiedSupplyAction(db, request, true, datatypes.JSON([]byte(`{"schema_version":"media-supply-action-proof/v1","verified":"expired_dispatch_claim_reclaimed_after_recovery","source_run_attempt_id":"`+attempt.PublicID.String()+`"}`)))
	case SupplyActionVerifyEffect:
		var unit models.SourceRunExecutionUnit
		if err := db.Where("public_id = ? AND tenant_id = ?", request.TargetID, request.TenantID).First(&unit).Error; err != nil {
			return false, err
		}
		key := "source-run-verify:" + unit.PublicID.String() + ":" + unit.AttemptFenceToken.String()
		var task models.SourceRunVerificationTask
		err := db.Where("tenant_id = ? AND task_key = ?", request.TenantID, key).First(&task).Error
		if err == gorm.ErrRecordNotFound {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		return true, terminalizeVerifiedSupplyAction(db, request, true, datatypes.JSON([]byte(`{"schema_version":"media-supply-action-proof/v1","verified":"verification_task_ensured_after_recovery","verification_task_id":"`+task.PublicID.String()+`"}`)))
	case SupplyActionFeedGenerationAttachVerifiedMember:
		var repair models.FeedGenerationMembershipRepair
		if err := db.Where("tenant_id=? AND action_request_id=?", request.TenantID, request.PublicID).First(&repair).Error; err != nil {
			if err == gorm.ErrRecordNotFound {
				return true, nil
			}
			return false, err
		}
		var head models.FeedGenerationHead
		var count int64
		err := db.Where("tenant_id=? AND lane=? AND active_generation_id=? AND generation=?", repair.TenantID, "media", repair.ExpectedGenerationID, repair.ExpectedHeadVersion).First(&head).Error
		if err == nil {
			err = db.Model(&models.FeedGenerationMembership{}).Where("generation_id=? AND member_type=? AND member_id=?", repair.ExpectedGenerationID, "feed_unit", repair.ContentItemID).Count(&count).Error
		}
		if err != nil || count != 1 {
			return true, nil
		}
		proof := datatypes.JSON([]byte(`{"schema_version":"feed-membership-proof/v1","verified":"active_generation_membership_recovered","content_item_id":"` + repair.ContentItemID.String() + `","generation_id":"` + repair.ExpectedGenerationID.String() + `"}`))
		if err := terminalizeVerifiedSupplyAction(db, request, true, proof); err != nil {
			return false, err
		}
		now := time.Now().UTC()
		if err := db.Model(&repair).Updates(map[string]any{"state": "succeeded", "verified_effects": proof, "finished_at": now}).Error; err != nil {
			return false, err
		}
		return true, nil
	default:
		return false, nil
	}
}

// verifyOneAggregationOwnedSupplyAction establishes the action outcome from
// retained CMS evidence rather than a queue acknowledgement. An absent receipt
// is intentionally non-terminal: it leaves the action verifying/uncertain so
// reconciliation can observe the fixed source-run unit without replaying it.
func verifyOneAggregationOwnedSupplyAction(db *gorm.DB) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("media supply action verifier requires a database")
	}
	var request models.MediaSupplyActionRequest
	if err := db.Where("state = ? AND execution_owner = ? AND action_key = ?", string(SupplyActionVerifying), SupplyActionOwnerAggregationDispatcher, SupplyActionAdoptUnitJob).Order("updated_at ASC").First(&request).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return false, nil
		}
		return false, err
	}
	var receipt models.SourceRunReceipt
	err := db.Where("tenant_id = ? AND execution_unit_id = ? AND stage = ? AND event_type = ?", request.TenantID, request.TargetID, string(ReceiptStageDispatch), string(ReceiptEventAccepted)).Order("observed_at ASC").First(&receipt).Error
	if err == gorm.ErrRecordNotFound {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return true, terminalizeVerifiedSupplyAction(db, request, true, datatypes.JSON([]byte(`{"schema_version":"media-supply-action-proof/v1","verified":"retained_dispatch_receipt","receipt_id":"`+receipt.PublicID.String()+`","execution_unit_id":"`+request.TargetID.String()+`"}`)))
}

func executeCancelUnstartedSupplyAction(db *gorm.DB, lease SupplyActionLease) error {
	var unit models.SourceRunExecutionUnit
	if err := db.Where("public_id = ? AND tenant_id = ?", lease.Request.TargetID, lease.Request.TenantID).First(&unit).Error; err != nil {
		return err
	}
	if unit.EffectStartedAt != nil || unit.State != string(UnitAuthorized) {
		return fmt.Errorf("source-run unit is no longer safely cancellable")
	}
	if err := recordSupplyActionEffects(db, lease, "before_effects", map[string]any{"schema_version": "media-supply-action-effects/v1", "execution_unit_id": unit.PublicID.String(), "unit_state": unit.State, "effect_started": false}); err != nil {
		return err
	}
	if _, err := BeginSupplyActionEffect(db, lease.Request.TenantID, lease.Request.PublicID.String(), lease.Request.ClaimOwner, lease.ClaimToken.String()); err != nil {
		return err
	}
	cancelled, err := RequestUnitCancellation(db, lease.Request.TenantID, unit.PublicID.String())
	if err != nil {
		return err
	}
	if err := recordSupplyActionEffects(db, lease, "after_effects", map[string]any{"schema_version": "media-supply-action-effects/v1", "execution_unit_id": cancelled.PublicID.String(), "unit_state": cancelled.State, "effect_started": false}); err != nil {
		return err
	}
	verified := cancelled.State == string(UnitCancelled) && cancelled.EffectStartedAt == nil
	return terminalizeSupplyAction(db, lease, verified, datatypes.JSON([]byte(`{"schema_version":"media-supply-action-proof/v1","verified":"cancelled_before_effect"}`)))
}

// recordSupplyActionEffects persists only worker-derived target snapshots.
// It refuses stale/cancelled claims so a lost worker cannot rewrite the audit
// ledger after another owner has recovered the same durable action.
func recordSupplyActionEffects(db *gorm.DB, lease SupplyActionLease, field string, value map[string]any) error {
	if field != "before_effects" && field != "after_effects" {
		return fmt.Errorf("media supply action effect field is not admitted")
	}
	bytes, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var request models.MediaSupplyActionRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", lease.Request.PublicID, lease.Request.TenantID).First(&request).Error; err != nil {
			return err
		}
		if request.ClaimToken == nil || *request.ClaimToken != lease.ClaimToken || request.ClaimOwner != lease.Request.ClaimOwner || (request.State != string(SupplyActionClaimed) && request.State != string(SupplyActionRunning)) {
			return fmt.Errorf("media supply action effect ledger claim is stale")
		}
		return tx.Model(&request).Update(field, datatypes.JSON(bytes)).Error
	})
}

func terminalizeSupplyAction(db *gorm.DB, lease SupplyActionLease, succeeded bool, verified datatypes.JSON) error {
	return db.Transaction(func(tx *gorm.DB) error {
		var request models.MediaSupplyActionRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", lease.Request.PublicID, lease.Request.TenantID).First(&request).Error; err != nil {
			return err
		}
		if request.ClaimToken == nil || *request.ClaimToken != lease.ClaimToken || request.ClaimOwner != lease.Request.ClaimOwner || request.State != string(SupplyActionRunning) {
			return fmt.Errorf("media supply action terminal claim is stale")
		}
		var attempt models.MediaSupplyActionAttempt
		if err := tx.Where("public_id = ? AND tenant_id = ?", lease.Attempt.PublicID, lease.Request.TenantID).First(&attempt).Error; err != nil {
			return err
		}
		now := time.Now().UTC()
		state := SupplyActionFailed
		event := "verification_failed"
		if succeeded {
			state, event = SupplyActionSucceeded, "verified_succeeded"
		}
		if err := tx.Model(&request).Updates(map[string]any{"state": string(state), "verified_effects": VerifiedSupplyActionEffects(request, actionJSON(verified, `{}`)), "finished_at": now, "claim_expires_at": nil}).Error; err != nil {
			return err
		}
		if err := tx.Model(&attempt).Updates(map[string]any{"state": string(state), "finished_at": now}).Error; err != nil {
			return err
		}
		if !succeeded {
			if err := DemoteBoundSupplyPromotion(tx, request, "independent_verifier_failed"); err != nil {
				return err
			}
		}
		verdict := VerdictAbsent
		if succeeded {
			verdict = VerdictPresent
		}
		RecordSupplyVerifierQualificationCaseBestEffort(tx, request, succeeded, verdict, false)
		request.State, request.FinishedAt = string(state), &now
		attempt.State, attempt.FinishedAt = string(state), &now
		return appendSupplyActionEvent(tx, request, &attempt, event, map[string]any{"verified": succeeded})
	})
}

// terminalizeVerifiedSupplyAction is reserved for reconciliation after a
// lease/cancellation moved an already-started effect into verifying. It has no
// execution token because the caller is establishing proof, not repeating an
// effect. This is the only terminal path that may finish a recovered action.
func terminalizeVerifiedSupplyAction(db *gorm.DB, requested models.MediaSupplyActionRequest, succeeded bool, verified datatypes.JSON) error {
	return db.Transaction(func(tx *gorm.DB) error {
		var request models.MediaSupplyActionRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", requested.PublicID, requested.TenantID).First(&request).Error; err != nil {
			return err
		}
		if request.State != string(SupplyActionVerifying) {
			return fmt.Errorf("media supply action is no longer awaiting verification")
		}
		var attempt models.MediaSupplyActionAttempt
		if err := tx.Where("tenant_id = ? AND action_request_id = ?", request.TenantID, request.PublicID).Order("attempt_number DESC").First(&attempt).Error; err != nil {
			return err
		}
		now := time.Now().UTC()
		state, event := SupplyActionFailed, "verification_failed"
		if succeeded {
			state, event = SupplyActionSucceeded, "verified_succeeded_after_recovery"
		}
		if err := tx.Model(&request).Updates(map[string]any{"state": string(state), "verified_effects": VerifiedSupplyActionEffects(request, actionJSON(verified, `{}`)), "finished_at": now, "claim_expires_at": nil}).Error; err != nil {
			return err
		}
		if err := tx.Model(&attempt).Updates(map[string]any{"state": string(state), "finished_at": now}).Error; err != nil {
			return err
		}
		if !succeeded {
			if err := DemoteBoundSupplyPromotion(tx, request, "recovered_verifier_failed"); err != nil {
				return err
			}
		}
		verdict := VerdictAbsent
		if succeeded {
			verdict = VerdictPresent
		}
		RecordSupplyVerifierQualificationCaseBestEffort(tx, request, succeeded, verdict, false)
		request.State, request.FinishedAt = string(state), &now
		attempt.State, attempt.FinishedAt = string(state), &now
		return appendSupplyActionEvent(tx, request, &attempt, event, map[string]any{"verified": succeeded, "recovered": true})
	})
}

func failSupplyActionBeforeEffect(db *gorm.DB, lease SupplyActionLease, failureClass string) error {
	return db.Transaction(func(tx *gorm.DB) error {
		var request models.MediaSupplyActionRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", lease.Request.PublicID, lease.Request.TenantID).First(&request).Error; err != nil {
			return err
		}
		var attempt models.MediaSupplyActionAttempt
		if err := tx.Where("public_id = ? AND tenant_id = ?", lease.Attempt.PublicID, lease.Request.TenantID).First(&attempt).Error; err != nil {
			return err
		}
		if attempt.EffectStartedAt != nil {
			return nil
		}
		now := time.Now().UTC()
		if err := tx.Model(&request).Updates(map[string]any{"state": string(SupplyActionFailed), "failure_class": failureClass, "finished_at": now}).Error; err != nil {
			return err
		}
		if err := tx.Model(&attempt).Updates(map[string]any{"state": string(SupplyActionFailed), "finished_at": now}).Error; err != nil {
			return err
		}
		if strings.Contains(strings.ToLower(failureClass), "budget") {
			if err := DemoteBoundSupplyPromotion(tx, request, "budget_breach"); err != nil {
				return err
			}
		}
		request.State, request.FinishedAt = string(SupplyActionFailed), &now
		attempt.State, attempt.FinishedAt = string(SupplyActionFailed), &now
		return appendSupplyActionEvent(tx, request, &attempt, "failed_before_effect", map[string]any{"failure_class": failureClass})
	})
}
