package supply

import (
	"context"
	"fmt"
	"strings"
	"time"

	"content-management-system/src/models"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	SupplyActionOwnerAggregationDispatcher = "aggregation_dispatcher"
	SupplyActionOwnerAggregationReceipt    = "aggregation_receipt"
)

// ClaimNextSupplyActionForOwner is the only cross-service action claim. The
// protocol and machine identity are both static inputs from a CMS route; a
// caller cannot substitute a tool key, target, queue name, or tenant.
func ClaimNextSupplyActionForOwner(db *gorm.DB, protocol, owner string, leaseFor time.Duration) (SupplyActionLease, bool, error) {
	if db == nil || (protocol != SupplyActionOwnerAggregationDispatcher && protocol != SupplyActionOwnerAggregationReceipt) || strings.TrimSpace(owner) == "" || leaseFor <= 0 || leaseFor > 5*time.Minute {
		return SupplyActionLease{}, false, fmt.Errorf("media supply action owner claim is invalid")
	}
	now := time.Now().UTC()
	if !SupplyActionOwnerReady(protocol, now) {
		return SupplyActionLease{}, false, fmt.Errorf("media supply action owner readiness is unavailable")
	}
	var lease SupplyActionLease
	err := db.Transaction(func(tx *gorm.DB) error {
		var queued []models.MediaSupplyActionRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Where("state = ? AND execution_owner = ?", string(SupplyActionQueued), protocol).Order("created_at ASC").Limit(16).Find(&queued).Error; err != nil {
			return err
		}
		for _, request := range queued {
			descriptor, descriptorErr := RequireSupplyActionDescriptor(request.ActionKey, request.TargetType)
			if descriptorErr != nil || descriptor.ExecutionOwner != protocol {
				return fmt.Errorf("media supply owner request is not statically admitted")
			}
			allowed, _, controlErr := MayExecuteSupplyAction(tx, request.TenantID, descriptor.Key)
			if controlErr != nil {
				return controlErr
			}
			if !allowed {
				continue
			}
			if authorityErr := RecheckSupplyActionExecutionAuthority(tx, request); authorityErr != nil {
				continue
			}
			var count int64
			if err := tx.Model(&models.MediaSupplyActionAttempt{}).Where("tenant_id = ? AND action_request_id = ?", request.TenantID, request.PublicID).Count(&count).Error; err != nil {
				return err
			}
			if count >= 2 {
				continue
			}
			token, expires := uuid.New(), now.Add(leaseFor)
			attempt := models.MediaSupplyActionAttempt{PublicID: uuid.New(), TenantID: request.TenantID, ActionRequestID: request.PublicID, AttemptNumber: int(count) + 1, State: string(SupplyActionClaimed), FenceToken: uuid.New(), OwnerProtocol: protocol}
			if err := tx.Create(&attempt).Error; err != nil {
				return err
			}
			if err := tx.Model(&request).Updates(map[string]any{"state": string(SupplyActionClaimed), "claim_owner": owner, "claim_token": token, "claim_epoch": gorm.Expr("claim_epoch + 1"), "claim_expires_at": expires}).Error; err != nil {
				return err
			}
			request.State, request.ClaimOwner, request.ClaimToken, request.ClaimEpoch, request.ClaimExpiresAt = string(SupplyActionClaimed), owner, &token, request.ClaimEpoch+1, &expires
			lease = SupplyActionLease{Request: request, Attempt: attempt, ClaimToken: token}
			return appendSupplyActionEvent(tx, request, &attempt, "owner_claimed", map[string]any{"protocol": protocol, "owner": owner, "attempt": attempt.AttemptNumber})
		}
		return nil
	})
	if err != nil || lease.Request.PublicID == uuid.Nil {
		return lease, false, err
	}
	if accessErr := RecheckSupplyActionAccess(context.Background(), lease.Request); accessErr != nil {
		if transitionErr := TransitionSupplyActionForAccessFailure(db, lease.Request.TenantID, lease.Request.PublicID.String(), lease.ClaimToken.String(), accessErr); transitionErr != nil {
			return SupplyActionLease{}, false, transitionErr
		}
		return SupplyActionLease{}, false, accessErr
	}
	return lease, true, nil
}

// BeginSupplyActionOwnerStep rechecks the static protocol and records the
// one-way owner effect boundary. The caller still needs a separate exact CMS
// unit/receipt authorization before any queue-facing work.
func BeginSupplyActionOwnerStep(db *gorm.DB, tenantID, requestID, owner, token, protocol string) (models.MediaSupplyActionRequest, error) {
	if protocol != SupplyActionOwnerAggregationDispatcher && protocol != SupplyActionOwnerAggregationReceipt {
		return models.MediaSupplyActionRequest{}, fmt.Errorf("media supply owner protocol is not admitted")
	}
	if !SupplyActionOwnerReady(protocol, time.Now().UTC()) {
		return models.MediaSupplyActionRequest{}, fmt.Errorf("media supply action owner readiness is unavailable")
	}
	if _, err := BeginSupplyActionEffect(db, tenantID, requestID, owner, token); err != nil {
		return models.MediaSupplyActionRequest{}, err
	}
	var request models.MediaSupplyActionRequest
	if err := db.Where("public_id = ? AND tenant_id = ? AND execution_owner = ?", requestID, tenantID, protocol).First(&request).Error; err != nil {
		return models.MediaSupplyActionRequest{}, err
	}
	return request, nil
}

// PrepareUnitAdoption is the only bridge from an approved adoption action to
// Aggregation's existing, CMS-selected coordinator.  It accepts neither a
// source, queue, provider argument, nor job identity: all of those values are
// reconstructed from the immutable action target and tenant-scoped CMS rows.
func PrepareUnitAdoption(db *gorm.DB, requestID, owner, token string, dispatcherLease, executionLease time.Duration) (DispatchClaim, SupplyActionLease, error) {
	if db == nil || dispatcherLease <= 0 || executionLease <= 0 {
		return DispatchClaim{}, SupplyActionLease{}, fmt.Errorf("unit adoption preparation is invalid")
	}
	var action models.MediaSupplyActionRequest
	if err := db.Where("public_id = ? AND execution_owner = ? AND action_key = ?", strings.TrimSpace(requestID), SupplyActionOwnerAggregationDispatcher, SupplyActionAdoptUnitJob).First(&action).Error; err != nil {
		return DispatchClaim{}, SupplyActionLease{}, err
	}
	if _, err := BeginSupplyActionOwnerStep(db, action.TenantID, action.PublicID.String(), owner, token, SupplyActionOwnerAggregationDispatcher); err != nil {
		return DispatchClaim{}, SupplyActionLease{}, err
	}
	var claim DispatchClaim
	err := db.Transaction(func(tx *gorm.DB) error {
		var current models.MediaSupplyActionRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", action.PublicID, action.TenantID).First(&current).Error; err != nil {
			return err
		}
		if current.State != string(SupplyActionRunning) || current.ClaimOwner != owner || current.ClaimToken == nil || current.ClaimToken.String() != strings.TrimSpace(token) {
			return fmt.Errorf("unit adoption action claim is stale")
		}
		var unit models.SourceRunExecutionUnit
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ? AND unit_type = ? AND state = ? AND effect_started_at IS NULL", current.TargetID, current.TenantID, "coordinator", string(UnitAuthorized)).First(&unit).Error; err != nil {
			return fmt.Errorf("unit adoption target is no longer an unstarted coordinator: %w", err)
		}
		var attempt models.SourceRunAttempt
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ? AND state = ? AND root_execution_unit_id = ?", unit.SourceRunAttemptID, current.TenantID, string(AttemptAuthorized), unit.PublicID).First(&attempt).Error; err != nil {
			return fmt.Errorf("unit adoption attempt is no longer dispatchable: %w", err)
		}
		var request models.SourceRunRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ? AND state IN ?", unit.SourceRunRequestID, current.TenantID, []string{string(RequestRequested), string(RequestAccepted)}).First(&request).Error; err != nil {
			return fmt.Errorf("unit adoption request is no longer dispatchable: %w", err)
		}
		if err := RequireDurableAdmission(tx, current.TenantID, request.Lane); err != nil {
			return err
		}
		var source models.ContentSource
		if err := tx.Where("public_id = ? AND tenant_id = ? AND is_active = TRUE", request.ContentSourceID, current.TenantID).First(&source).Error; err != nil {
			return fmt.Errorf("unit adoption source is no longer active: %w", err)
		}
		now := time.Now().UTC()
		dispatchToken, executionToken := uuid.New(), uuid.New()
		dispatchExpires, executionExpires := now.Add(dispatcherLease), now.Add(executionLease)
		if err := tx.Model(&attempt).Where("id = ? AND state = ?", attempt.ID, string(AttemptAuthorized)).Updates(map[string]any{"state": string(AttemptClaimed), "dispatcher_owner": owner, "dispatcher_token": dispatchToken, "dispatcher_epoch": gorm.Expr("dispatcher_epoch + 1"), "dispatcher_lease_expires_at": dispatchExpires, "heartbeat_at": now}).Error; err != nil {
			return err
		}
		if err := tx.Model(&unit).Where("id = ? AND state = ? AND effect_started_at IS NULL", unit.ID, string(UnitAuthorized)).Updates(map[string]any{"state": string(UnitAccepted), "execution_owner": owner, "execution_lease_token": executionToken, "execution_lease_epoch": gorm.Expr("execution_lease_epoch + 1"), "execution_lease_expires_at": executionExpires, "heartbeat_at": now}).Error; err != nil {
			return err
		}
		if request.State == string(RequestRequested) {
			if err := tx.Model(&request).Update("state", string(RequestAccepted)).Update("accepted_at", now).Error; err != nil {
				return err
			}
			request.State, request.AcceptedAt = string(RequestAccepted), &now
		}
		attempt.State, attempt.DispatcherOwner, attempt.DispatcherToken, attempt.DispatcherEpoch, attempt.DispatcherLeaseExpiresAt, attempt.HeartbeatAt = string(AttemptClaimed), owner, &dispatchToken, attempt.DispatcherEpoch+1, &dispatchExpires, &now
		unit.State, unit.ExecutionOwner, unit.ExecutionLeaseToken, unit.ExecutionLeaseEpoch, unit.ExecutionLeaseExpiresAt, unit.HeartbeatAt = string(UnitAccepted), owner, &executionToken, unit.ExecutionLeaseEpoch+1, &executionExpires, &now
		claim = DispatchClaim{Request: request, Source: source, Attempt: attempt, RootUnit: unit, DispatcherToken: dispatchToken, ExecutionToken: executionToken}
		var actionAttempt models.MediaSupplyActionAttempt
		if err := tx.Where("tenant_id = ? AND action_request_id = ?", current.TenantID, current.PublicID).Order("attempt_number DESC").First(&actionAttempt).Error; err != nil {
			return err
		}
		lease := SupplyActionLease{Request: current, Attempt: actionAttempt, ClaimToken: *current.ClaimToken}
		return recordSupplyActionEffects(tx, lease, "before_effects", map[string]any{"schema_version": "media-supply-action-effects/v1", "execution_unit_id": unit.PublicID.String(), "source_run_request_id": request.PublicID.String(), "source_run_attempt_id": attempt.PublicID.String(), "unit_state_before_adoption": string(UnitAuthorized)})
	})
	if err != nil {
		return DispatchClaim{}, SupplyActionLease{}, err
	}
	var latestAttempt models.MediaSupplyActionAttempt
	if err := db.Where("tenant_id = ? AND action_request_id = ?", action.TenantID, action.PublicID).Order("attempt_number DESC").First(&latestAttempt).Error; err != nil {
		return DispatchClaim{}, SupplyActionLease{}, err
	}
	var latest models.MediaSupplyActionRequest
	if err := db.Where("public_id = ? AND tenant_id = ?", action.PublicID, action.TenantID).First(&latest).Error; err != nil {
		return DispatchClaim{}, SupplyActionLease{}, err
	}
	return claim, SupplyActionLease{Request: latest, Attempt: latestAttempt, ClaimToken: *latest.ClaimToken}, nil
}

// MarkUnitAdoptionQueued moves an action to verification only after the
// static coordinator job was durably accepted by Aggregation. Queue acceptance
// is not success: the CMS worker later requires a retained dispatch receipt.
func MarkUnitAdoptionQueued(db *gorm.DB, requestID, owner, token string) error {
	if db == nil {
		return fmt.Errorf("unit adoption acknowledgement requires a database")
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var request models.MediaSupplyActionRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND execution_owner = ? AND action_key = ?", strings.TrimSpace(requestID), SupplyActionOwnerAggregationDispatcher, SupplyActionAdoptUnitJob).First(&request).Error; err != nil {
			return err
		}
		if request.State != string(SupplyActionRunning) || request.ClaimOwner != owner || request.ClaimToken == nil || request.ClaimToken.String() != strings.TrimSpace(token) {
			return fmt.Errorf("unit adoption acknowledgement is stale")
		}
		var attempt models.MediaSupplyActionAttempt
		if err := tx.Where("tenant_id = ? AND action_request_id = ?", request.TenantID, request.PublicID).Order("attempt_number DESC").First(&attempt).Error; err != nil {
			return err
		}
		lease := SupplyActionLease{Request: request, Attempt: attempt, ClaimToken: *request.ClaimToken}
		if err := recordSupplyActionEffects(tx, lease, "after_effects", map[string]any{"schema_version": "media-supply-action-effects/v1", "execution_unit_id": request.TargetID.String(), "handoff": "aggregation_coordinator_enqueued", "verification": "retained_dispatch_receipt"}); err != nil {
			return err
		}
		if err := tx.Model(&request).Update("state", string(SupplyActionVerifying)).Error; err != nil {
			return err
		}
		if err := tx.Model(&attempt).Update("state", string(SupplyActionVerifying)).Error; err != nil {
			return err
		}
		request.State, attempt.State = string(SupplyActionVerifying), string(SupplyActionVerifying)
		return appendSupplyActionEvent(tx, request, &attempt, "owner_handoff_requires_verification", map[string]any{"protocol": SupplyActionOwnerAggregationDispatcher})
	})
}

// PrepareReceiptRedelivery returns the exact immutable envelope retained by
// CMS. It does not accept, reconstruct, or modify receipt payload data.
func PrepareReceiptRedelivery(db *gorm.DB, requestID, owner, token string) (datatypes.JSON, error) {
	if db == nil {
		return nil, fmt.Errorf("receipt redelivery preparation requires a database")
	}
	var action models.MediaSupplyActionRequest
	if err := db.Where("public_id = ? AND execution_owner = ? AND action_key = ?", strings.TrimSpace(requestID), SupplyActionOwnerAggregationReceipt, SupplyActionRedeliverReceipt).First(&action).Error; err != nil {
		return nil, err
	}
	var retained models.SourceRunRetainedReceipt
	if err := db.Where("public_id = ? AND tenant_id = ? AND state = ?", action.TargetID, action.TenantID, "retained").First(&retained).Error; err != nil {
		return nil, fmt.Errorf("retained receipt is no longer redeliverable: %w", err)
	}
	if _, err := BeginSupplyActionOwnerStep(db, action.TenantID, action.PublicID.String(), owner, token, SupplyActionOwnerAggregationReceipt); err != nil {
		return nil, err
	}
	var attempt models.MediaSupplyActionAttempt
	if err := db.Where("tenant_id = ? AND action_request_id = ?", action.TenantID, action.PublicID).Order("attempt_number DESC").First(&attempt).Error; err != nil {
		return nil, err
	}
	lease := SupplyActionLease{Request: action, Attempt: attempt, ClaimToken: mustActionClaimToken(action)}
	if err := recordSupplyActionEffects(db, lease, "before_effects", map[string]any{"schema_version": "media-supply-action-effects/v1", "retained_receipt_id": retained.PublicID.String(), "producer_event_key": retained.ProducerEventKey, "payload_digest": retained.PayloadDigest, "retention_state": retained.State}); err != nil {
		return nil, err
	}
	return append(datatypes.JSON(nil), retained.Receipt...), nil
}

// CompleteReceiptRedelivery establishes success from the CMS receipt ledger,
// not a worker acknowledgement. A duplicate delivery is valid only when its
// producer key and immutable digest match the retained target.
func CompleteReceiptRedelivery(db *gorm.DB, requestID, owner, token string) error {
	if db == nil {
		return fmt.Errorf("receipt redelivery completion requires a database")
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var request models.MediaSupplyActionRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND execution_owner = ? AND action_key = ?", strings.TrimSpace(requestID), SupplyActionOwnerAggregationReceipt, SupplyActionRedeliverReceipt).First(&request).Error; err != nil {
			return err
		}
		if request.State != string(SupplyActionRunning) || request.ClaimOwner != owner || request.ClaimToken == nil || request.ClaimToken.String() != strings.TrimSpace(token) {
			return fmt.Errorf("receipt redelivery completion is stale")
		}
		var retained models.SourceRunRetainedReceipt
		if err := tx.Where("public_id = ? AND tenant_id = ?", request.TargetID, request.TenantID).First(&retained).Error; err != nil {
			return err
		}
		var receipt models.SourceRunReceipt
		err := tx.Where("tenant_id = ? AND producer_event_key = ?", request.TenantID, retained.ProducerEventKey).First(&receipt).Error
		if err == gorm.ErrRecordNotFound {
			var created bool
			receipt, created, err = RecordRetainedReceipt(tx, retained)
			if err != nil {
				return err
			}
			_ = created
		} else if err != nil {
			return err
		}
		if receipt.ExecutionUnitID != retained.ExecutionUnitID || receipt.PayloadDigest != retained.PayloadDigest {
			return fmt.Errorf("receipt ledger proof does not match retained receipt")
		}
		if retained.State != "delivered" {
			if _, err := MarkRetainedReceiptDelivered(tx, request.TenantID, retained.ProducerEventKey); err != nil {
				return err
			}
		}
		var attempt models.MediaSupplyActionAttempt
		if err := tx.Where("tenant_id = ? AND action_request_id = ?", request.TenantID, request.PublicID).Order("attempt_number DESC").First(&attempt).Error; err != nil {
			return err
		}
		lease := SupplyActionLease{Request: request, Attempt: attempt, ClaimToken: *request.ClaimToken}
		if err := recordSupplyActionEffects(tx, lease, "after_effects", map[string]any{"schema_version": "media-supply-action-effects/v1", "retained_receipt_id": retained.PublicID.String(), "receipt_id": receipt.PublicID.String(), "receipt_ledger_proved": true}); err != nil {
			return err
		}
		return terminalizeSupplyAction(tx, lease, true, datatypes.JSON([]byte(`{"schema_version":"media-supply-action-proof/v1","verified":"retained_receipt_delivered","receipt_id":"`+receipt.PublicID.String()+`"}`)))
	})
}

func mustActionClaimToken(request models.MediaSupplyActionRequest) uuid.UUID {
	if request.ClaimToken == nil {
		return uuid.Nil
	}
	return *request.ClaimToken
}
