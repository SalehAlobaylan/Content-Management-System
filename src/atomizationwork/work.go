package atomizationwork

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/supply"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	lease                = 60 * time.Second
	verificationDeadline = 30 * time.Minute
	maxAttempts          = 2
)

type Claim struct {
	Request models.AtomizationWorkRequest `json:"request"`
	Attempt models.AtomizationWorkAttempt `json:"attempt"`
	Parent  models.ContentItem            `json:"-"`
}

func Candidate(db *gorm.DB, tenant string, parentID uuid.UUID) (models.ContentItem, models.Transcript, models.MediaAtomizationPolicy, string, error) {
	var parent models.ContentItem
	if err := db.Where("tenant_id = ? AND public_id = ? AND parent_content_item_id IS NULL AND type IN ?", tenant, parentID, []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}).First(&parent).Error; err != nil {
		return parent, models.Transcript{}, models.MediaAtomizationPolicy{}, "", err
	}
	if parent.DurationSec == nil || *parent.DurationSec <= 2400 || parent.TranscriptID == nil || parent.Status == models.ContentStatusArchived {
		return parent, models.Transcript{}, models.MediaAtomizationPolicy{}, "", fmt.Errorf("parent is not atomization eligible")
	}
	var transcript models.Transcript
	if err := db.Where("public_id = ? AND content_item_id = ?", *parent.TranscriptID, parent.PublicID).First(&transcript).Error; err != nil {
		return parent, transcript, models.MediaAtomizationPolicy{}, "", err
	}
	var policy models.MediaAtomizationPolicy
	if err := db.Where("tenant_id = ?", tenant).First(&policy).Error; err != nil {
		return parent, transcript, policy, "", fmt.Errorf("atomization policy is unavailable")
	}
	if !policy.ChapteringEnabled {
		return parent, transcript, policy, "", fmt.Errorf("atomization is disabled by tenant policy")
	}
	policyBytes, _ := json.Marshal(policy)
	transcriptHash := digest("transcript/v1", transcript.PublicID.String(), transcript.FullText, transcript.CreatedAt.UTC().Format(time.RFC3339Nano))
	fingerprint := digest("atomization-input/v1", tenant, parent.PublicID.String(), parent.UpdatedAt.UTC().Format(time.RFC3339Nano), transcriptHash, string(policyBytes))
	return parent, transcript, policy, fingerprint, nil
}

func CreateApproved(db *gorm.DB, action models.MediaSupplyActionRequest) (models.AtomizationWorkRequest, error) {
	if action.ActionKey != supply.SupplyActionAtomizationExecuteExactParent || action.State != models.MediaSupplyActionRequestQueued || action.TargetType != "content_item" {
		return models.AtomizationWorkRequest{}, fmt.Errorf("atomization action is invalid")
	}
	parent, transcript, policy, fingerprint, err := Candidate(db, action.TenantID, action.TargetID)
	if err != nil {
		return models.AtomizationWorkRequest{}, err
	}
	policyBytes, _ := json.Marshal(policy)
	now := time.Now().UTC()
	request := models.AtomizationWorkRequest{PublicID: uuid.New(), TenantID: action.TenantID, ParentContentItemID: parent.PublicID, ParentUpdatedAt: parent.UpdatedAt, TranscriptID: transcript.PublicID, TranscriptFingerprint: digest(transcript.FullText), PolicyHash: digest(string(policyBytes)), InputFingerprint: fingerprint, ActionRequestID: &action.PublicID, State: "queued", Checkpoints: datatypes.JSON([]byte(`{}`)), TerminalProof: datatypes.JSON([]byte(`{}`)), ApprovedBy: action.ApprovedBy, ApprovedAt: now}
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&request).Error; err != nil {
			return err
		}
		return event(tx, request, "queued", map[string]any{"input_fingerprint": fingerprint})
	})
	return request, err
}

func ClaimNext(db *gorm.DB, owner string) (Claim, bool, error) {
	if strings.TrimSpace(owner) == "" {
		return Claim{}, false, fmt.Errorf("atomization owner is required")
	}
	now := time.Now().UTC()
	if !supply.SupplyActionOwnerReady("aggregation_atomization", now) {
		return Claim{}, false, fmt.Errorf("atomization owner readiness is unavailable")
	}
	var claim Claim
	err := db.Transaction(func(tx *gorm.DB) error {
		var candidates []models.AtomizationWorkRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Where("state = ?", "queued").Order("created_at").Limit(16).Find(&candidates).Error; err != nil {
			return err
		}
		for _, request := range candidates {
			allowed, _, err := supply.MayExecuteSupplyAction(tx, request.TenantID, supply.SupplyActionAtomizationExecuteExactParent)
			if err != nil {
				return err
			}
			if !allowed {
				continue
			}
			parent, transcript, _, fingerprint, err := Candidate(tx, request.TenantID, request.ParentContentItemID)
			if err != nil || fingerprint != request.InputFingerprint || transcript.PublicID != request.TranscriptID || !parent.UpdatedAt.Equal(request.ParentUpdatedAt) {
				continue
			}
			var attempts int64
			if err := tx.Model(&models.AtomizationWorkAttempt{}).Where("tenant_id=? AND request_id=?", request.TenantID, request.PublicID).Count(&attempts).Error; err != nil {
				return err
			}
			var latest models.AtomizationWorkAttempt
			if attempts > 0 {
				_ = tx.Where("tenant_id=? AND request_id=?", request.TenantID, request.PublicID).Order("attempt_number DESC").First(&latest).Error
			}
			adopting := latest.PublicID != uuid.Nil && latest.State == "claimed" && latest.EffectStartedAt == nil && !latest.LeaseExpiresAt.After(now)
			if !adopting && attempts >= maxAttempts {
				now := time.Now().UTC()
				proof := jsonValue(map[string]any{"verified": "absent", "reason": "attempt_budget_exhausted"})
				if err := tx.Model(&request).Updates(map[string]any{"state": "failed", "terminal_proof": proof, "finished_at": now}).Error; err != nil {
					return err
				}
				if err := failAction(tx, request, proof, "attempt_budget_exhausted", now); err != nil {
					return err
				}
				if err := event(tx, request, "failed", map[string]any{"failure_class": "attempt_budget_exhausted"}); err != nil {
					return err
				}
				continue
			}
			token, fence, expires := uuid.New(), uuid.New(), now.Add(lease)
			attempt := latest
			if adopting {
				fence = latest.FenceToken
				if err := tx.Model(&attempt).Updates(map[string]any{"claim_token": token, "lease_expires_at": expires, "heartbeat_at": now}).Error; err != nil {
					return err
				}
				attempt.ClaimToken, attempt.LeaseExpiresAt, attempt.HeartbeatAt = token, expires, now
				if err := adoptAction(tx, request, attempt, token, expires); err != nil {
					return err
				}
			} else {
				attempt = models.AtomizationWorkAttempt{PublicID: uuid.New(), TenantID: request.TenantID, RequestID: request.PublicID, AttemptNumber: int(attempts) + 1, State: "claimed", ClaimToken: token, FenceToken: fence, DeterministicJobID: "atomize:" + request.InputFingerprint, LeaseExpiresAt: expires, HeartbeatAt: now}
				if err := tx.Create(&attempt).Error; err != nil {
					return err
				}
				if err := claimAction(tx, request, attempt, token, expires); err != nil {
					return err
				}
			}
			if err := tx.Model(&request).Updates(map[string]any{"state": "claimed", "claim_owner": owner, "claim_token": token, "fence_token": fence, "claim_epoch": gorm.Expr("claim_epoch+1"), "claim_expires_at": expires}).Error; err != nil {
				return err
			}
			request.State = "claimed"
			request.ClaimOwner = owner
			request.ClaimToken = &token
			request.FenceToken = &fence
			request.ClaimExpiresAt = &expires
			claim = Claim{Request: request, Attempt: attempt, Parent: parent}
			eventType := "claimed"
			if adopting {
				eventType = "claim_adopted"
			}
			return event(tx, request, eventType, map[string]any{"attempt_id": attempt.PublicID, "owner": owner, "deterministic_job_id": attempt.DeterministicJobID})
		}
		return nil
	})
	if err != nil || claim.Request.PublicID == uuid.Nil {
		return claim, false, err
	}
	if accessErr := recheckAtomizationActionAccess(db, claim.Request.PublicID.String()); accessErr != nil {
		return Claim{}, false, accessErr
	}
	return claim, true, nil
}

func Begin(db *gorm.DB, requestID, owner string, token uuid.UUID) error {
	if !supply.SupplyActionOwnerReady("aggregation_atomization", time.Now().UTC()) {
		return fmt.Errorf("atomization owner readiness is unavailable")
	}
	if err := recheckAtomizationActionAccess(db, requestID); err != nil {
		return err
	}
	return leaseStep(db, requestID, owner, token, true)
}
func Heartbeat(db *gorm.DB, requestID, owner string, token uuid.UUID) error {
	if !supply.SupplyActionOwnerReady("aggregation_atomization", time.Now().UTC()) {
		return fmt.Errorf("atomization owner readiness is unavailable")
	}
	if err := recheckAtomizationActionAccess(db, requestID); err != nil {
		return err
	}
	return leaseStep(db, requestID, owner, token, false)
}

func recheckAtomizationActionAccess(db *gorm.DB, requestID string) error {
	var request models.AtomizationWorkRequest
	if err := db.Select("tenant_id", "action_request_id").Where("public_id = ?", requestID).First(&request).Error; err != nil {
		return err
	}
	if request.ActionRequestID == nil {
		return fmt.Errorf("atomization work has no signed supply action")
	}
	var action models.MediaSupplyActionRequest
	if err := db.Where("tenant_id = ? AND public_id = ?", request.TenantID, *request.ActionRequestID).First(&action).Error; err != nil {
		return err
	}
	if err := supply.RecheckSupplyActionAccess(context.Background(), action); err != nil {
		return err
	}
	return supply.RecheckSupplyActionExecutionAuthority(db, action)
}
func leaseStep(db *gorm.DB, requestID, owner string, token uuid.UUID, begin bool) error {
	now := time.Now().UTC()
	return db.Transaction(func(tx *gorm.DB) error {
		var request models.AtomizationWorkRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id=? AND claim_owner=? AND claim_token=?", requestID, owner, token).First(&request).Error; err != nil {
			return err
		}
		if request.ClaimExpiresAt == nil || !request.ClaimExpiresAt.After(now) || request.CancellationRequestedAt != nil {
			return fmt.Errorf("atomization claim is stale")
		}
		allowed, _, err := supply.MayExecuteSupplyAction(tx, request.TenantID, supply.SupplyActionAtomizationExecuteExactParent)
		if err != nil || !allowed {
			return fmt.Errorf("atomization action is disabled")
		}
		expires := now.Add(lease)
		updates := map[string]any{"claim_expires_at": expires}
		attemptUpdates := map[string]any{"lease_expires_at": expires, "heartbeat_at": now}
		if begin {
			if request.State != "claimed" {
				return fmt.Errorf("atomization claim cannot begin")
			}
			parent, transcript, policy, fingerprint, candidateErr := Candidate(tx, request.TenantID, request.ParentContentItemID)
			policyBytes, _ := json.Marshal(policy)
			if candidateErr != nil || !parent.UpdatedAt.Equal(request.ParentUpdatedAt) || transcript.PublicID != request.TranscriptID || digest(transcript.FullText) != request.TranscriptFingerprint || fingerprint != request.InputFingerprint || digest(string(policyBytes)) != request.PolicyHash {
				return fmt.Errorf("atomization target evidence changed before effect")
			}
			updates["state"] = "running"
			updates["effect_started_at"] = now
			attemptUpdates["state"] = "running"
			attemptUpdates["effect_started_at"] = now
		}
		if err := tx.Model(&request).Updates(updates).Error; err != nil {
			return err
		}
		if err := tx.Model(&models.AtomizationWorkAttempt{}).Where("tenant_id=? AND request_id=? AND claim_token=?", request.TenantID, request.PublicID, token).Updates(attemptUpdates).Error; err != nil {
			return err
		}
		if err := stepAction(tx, request, token, expires, begin, now); err != nil {
			return err
		}
		if begin {
			return event(tx, request, "effect_started", map[string]any{})
		}
		return nil
	})
}

func Checkpoint(db *gorm.DB, requestID string, token uuid.UUID, phase string, proof map[string]any) error {
	allowed := map[string]bool{"plan_persisted": true, "first_cut": true, "uploads_complete": true, "children_persisted": true, "embedding_handoff": true, "owner_complete": true}
	if !allowed[phase] {
		return fmt.Errorf("atomization checkpoint is not registered")
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var request models.AtomizationWorkRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id=? AND claim_token=? AND state=?", requestID, token, "running").First(&request).Error; err != nil {
			return err
		}
		var checkpoints map[string]any
		_ = json.Unmarshal(request.Checkpoints, &checkpoints)
		if checkpoints == nil {
			checkpoints = map[string]any{}
		}
		checkpoints[phase] = proof
		bytes, _ := json.Marshal(checkpoints)
		updates := map[string]any{"checkpoints": datatypes.JSON(bytes)}
		if phase == "owner_complete" {
			updates["state"] = "verifying"
			updates["claim_expires_at"] = nil
		}
		if err := tx.Model(&request).Updates(updates).Error; err != nil {
			return err
		}
		if phase == "owner_complete" {
			if err := verifyActionPending(tx, request, proof); err != nil {
				return err
			}
		}
		return event(tx, request, "checkpoint_"+phase, proof)
	})
}

func VerifyOne(db *gorm.DB) (bool, error) {
	var request models.AtomizationWorkRequest
	err := db.Where("state IN ?", []string{"verifying", "uncertain"}).Order("updated_at").First(&request).Error
	if err == gorm.ErrRecordNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var checkpoints map[string]any
	if json.Unmarshal(request.Checkpoints, &checkpoints) != nil {
		return true, fmt.Errorf("atomization checkpoints are malformed")
	}
	ownerComplete, _ := checkpoints["owner_complete"].(map[string]any)
	rawIDs, _ := ownerComplete["child_ids"].([]any)
	declared := make([]uuid.UUID, 0, len(rawIDs))
	seen := map[uuid.UUID]bool{}
	for _, raw := range rawIDs {
		id, parseErr := uuid.Parse(fmt.Sprint(raw))
		if parseErr != nil || seen[id] {
			return true, fmt.Errorf("atomization child declaration is invalid")
		}
		seen[id] = true
		declared = append(declared, id)
	}
	if len(declared) == 0 || len(declared) > 100 {
		return failVerificationIfExpired(db, request, "owner_child_set_not_declared")
	}
	var children []models.ContentItem
	if err = db.Where("tenant_id=? AND parent_content_item_id=? AND public_id IN ? AND status<>?", request.TenantID, request.ParentContentItemID, declared, models.ContentStatusArchived).Find(&children).Error; err != nil {
		return true, err
	}
	if len(children) != len(declared) {
		return failVerificationIfExpired(db, request, "declared_child_set_absent")
	}
	var liveChildCount int64
	if err = db.Model(&models.ContentItem{}).Where("tenant_id=? AND parent_content_item_id=? AND status<>?", request.TenantID, request.ParentContentItemID, models.ContentStatusArchived).Count(&liveChildCount).Error; err != nil {
		return true, err
	}
	if liveChildCount != int64(len(declared)) {
		return failVerificationIfExpired(db, request, "conflicting_live_child_set")
	}
	ids := make([]string, 0, len(children))
	for _, child := range children {
		if child.DurationSec == nil || *child.DurationSec < 270 || *child.DurationSec > 2400 {
			return failVerificationIfExpired(db, request, "declared_child_duration_invalid")
		}
		ids = append(ids, child.PublicID.String())
	}
	sort.Strings(ids)
	now := time.Now().UTC()
	proof := jsonValue(map[string]any{"verified": "child_set_present", "child_ids": ids, "input_fingerprint": request.InputFingerprint})
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&request).Updates(map[string]any{"state": "succeeded", "terminal_proof": proof, "finished_at": now}).Error; err != nil {
			return err
		}
		if err := tx.Model(&models.AtomizationWorkAttempt{}).Where("tenant_id=? AND request_id=?", request.TenantID, request.PublicID).Order("attempt_number DESC").Updates(map[string]any{"state": "succeeded", "finished_at": now}).Error; err != nil {
			return err
		}
		if err := completeAction(tx, request, proof, now); err != nil {
			return err
		}
		clearance := models.StudioClearanceRequest{PublicID: uuid.New(), TenantID: request.TenantID, AtomizationRequestID: request.PublicID, ChildIDs: jsonValue(ids), ChildSetDigest: digest(strings.Join(ids, "\n")), State: "queued", TerminalProof: datatypes.JSON([]byte(`{}`))}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&clearance).Error; err != nil {
			return err
		}
		return event(tx, request, "verified", map[string]any{"child_count": len(ids)})
	})
	return true, err
}

func failVerificationIfExpired(db *gorm.DB, request models.AtomizationWorkRequest, failure string) (bool, error) {
	started := request.EffectStartedAt
	if started == nil || time.Since(started.UTC()) < verificationDeadline {
		return true, nil
	}
	now := time.Now().UTC()
	proof := jsonValue(map[string]any{"verified": "absent", "failure_class": failure, "input_fingerprint": request.InputFingerprint})
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&request).Where("state IN ?", []string{"verifying", "uncertain"}).Updates(map[string]any{"state": "failed", "terminal_proof": proof, "finished_at": now}).Error; err != nil {
			return err
		}
		if err := tx.Model(&models.AtomizationWorkAttempt{}).Where("tenant_id=? AND request_id=?", request.TenantID, request.PublicID).Order("attempt_number DESC").Updates(map[string]any{"state": "failed", "finished_at": now}).Error; err != nil {
			return err
		}
		if err := failAction(tx, request, proof, failure, now); err != nil {
			return err
		}
		return event(tx, request, "verification_absent", map[string]any{"failure_class": failure})
	})
	return true, err
}

// CancelByAction propagates cancellation to the exact atomization ledger
// linked to a signed Supply action. Work that crossed its effect boundary is
// retained for verification and is never treated as safely undone.
func CancelByAction(db *gorm.DB, tenant string, actionID uuid.UUID, actor string) error {
	now := time.Now().UTC()
	return db.Transaction(func(tx *gorm.DB) error {
		var request models.AtomizationWorkRequest
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND action_request_id=?", tenant, actionID).First(&request).Error
		if err == gorm.ErrRecordNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		switch request.State {
		case "queued", "claimed":
			if request.EffectStartedAt == nil {
				if err := tx.Model(&request).Updates(map[string]any{"state": "cancelled", "cancellation_requested_at": now, "claim_owner": "", "claim_token": nil, "fence_token": nil, "claim_expires_at": nil, "finished_at": now}).Error; err != nil {
					return err
				}
				if err := tx.Model(&models.AtomizationWorkAttempt{}).Where("tenant_id=? AND request_id=? AND finished_at IS NULL", tenant, request.PublicID).Updates(map[string]any{"state": "cancelled", "finished_at": now}).Error; err != nil {
					return err
				}
				return event(tx, request, "cancelled_before_effect", map[string]any{"actor": actor})
			}
			fallthrough
		case "running", "verifying", "uncertain":
			if err := tx.Model(&request).Update("cancellation_requested_at", now).Error; err != nil {
				return err
			}
			return event(tx, request, "cancellation_requires_verification", map[string]any{"actor": actor})
		case "succeeded", "failed", "cancelled":
			return nil
		default:
			return fmt.Errorf("atomization work cannot be cancelled from state %s", request.State)
		}
	})
}

func RecoverExpired(db *gorm.DB) error {
	now := time.Now().UTC()
	return db.Transaction(func(tx *gorm.DB) error {
		var rows []models.AtomizationWorkRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Where("state IN ? AND claim_expires_at<?", []string{"claimed", "running"}, now).Find(&rows).Error; err != nil {
			return err
		}
		for _, request := range rows {
			next := "queued"
			if request.EffectStartedAt != nil {
				next = "uncertain"
			}
			if err := tx.Model(&request).Updates(map[string]any{"state": next, "claim_owner": "", "claim_token": nil, "fence_token": nil, "claim_expires_at": nil}).Error; err != nil {
				return err
			}
			if err := recoverAction(tx, request, next, now); err != nil {
				return err
			}
			disposition, _ := AtomizationRecoveryDisposition(request.Checkpoints, request.EffectStartedAt != nil)
			if err := event(tx, request, "claim_expired", map[string]any{"next": next, "recovery_disposition": disposition}); err != nil {
				return err
			}
		}
		return nil
	})
}

func AtomizationRecoveryDisposition(checkpoints datatypes.JSON, effectStarted bool) (string, error) {
	var inventory map[string]any
	if len(checkpoints) > 0 && json.Unmarshal(checkpoints, &inventory) != nil {
		return "manual_attention", fmt.Errorf("atomization checkpoint inventory is malformed")
	}
	if !effectStarted {
		return "adopt_same_job", nil
	}
	for _, phase := range []string{"owner_complete", "embedding_handoff", "children_persisted", "uploads_complete", "first_cut", "plan_persisted"} {
		if _, present := inventory[phase]; present {
			return "verify_from_" + phase, nil
		}
	}
	return "verify_effect_unknown", nil
}

func event(tx *gorm.DB, request models.AtomizationWorkRequest, kind string, payload map[string]any) error {
	var n int64
	if err := tx.Model(&models.AtomizationWorkEvent{}).Where("tenant_id=? AND request_id=?", request.TenantID, request.PublicID).Count(&n).Error; err != nil {
		return err
	}
	return tx.Create(&models.AtomizationWorkEvent{PublicID: uuid.New(), TenantID: request.TenantID, RequestID: request.PublicID, Sequence: n + 1, EventType: kind, Payload: jsonValue(payload), OccurredAt: time.Now().UTC()}).Error
}

func claimAction(tx *gorm.DB, request models.AtomizationWorkRequest, attempt models.AtomizationWorkAttempt, token uuid.UUID, expires time.Time) error {
	if request.ActionRequestID == nil {
		return nil
	}
	var action models.MediaSupplyActionRequest
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=? AND state=?", request.TenantID, *request.ActionRequestID, models.MediaSupplyActionRequestQueued).First(&action).Error; err != nil {
		return err
	}
	linked := models.MediaSupplyActionAttempt{PublicID: uuid.New(), TenantID: request.TenantID, ActionRequestID: action.PublicID, AttemptNumber: attempt.AttemptNumber, State: models.MediaSupplyActionRequestClaimed, FenceToken: attempt.FenceToken, OwnerProtocol: "atomization-work/v1:" + attempt.PublicID.String()}
	if err := tx.Create(&linked).Error; err != nil {
		return err
	}
	return tx.Model(&action).Updates(map[string]any{"state": models.MediaSupplyActionRequestClaimed, "claim_owner": "aggregation-atomization", "claim_token": token, "claim_epoch": gorm.Expr("claim_epoch+1"), "claim_expires_at": expires}).Error
}

func adoptAction(tx *gorm.DB, request models.AtomizationWorkRequest, attempt models.AtomizationWorkAttempt, token uuid.UUID, expires time.Time) error {
	if request.ActionRequestID == nil {
		return nil
	}
	var action models.MediaSupplyActionRequest
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=? AND state=?", request.TenantID, *request.ActionRequestID, models.MediaSupplyActionRequestQueued).First(&action).Error; err != nil {
		return err
	}
	var linked models.MediaSupplyActionAttempt
	if err := tx.Where("tenant_id=? AND action_request_id=? AND owner_protocol=?", request.TenantID, action.PublicID, "atomization-work/v1:"+attempt.PublicID.String()).First(&linked).Error; err != nil {
		return err
	}
	if linked.State != models.MediaSupplyActionRequestClaimed || linked.EffectStartedAt != nil {
		return fmt.Errorf("linked atomization action is not adoptable")
	}
	return tx.Model(&action).Updates(map[string]any{"state": models.MediaSupplyActionRequestClaimed, "claim_owner": "aggregation-atomization", "claim_token": token, "claim_epoch": gorm.Expr("claim_epoch+1"), "claim_expires_at": expires, "failure_class": ""}).Error
}
func stepAction(tx *gorm.DB, request models.AtomizationWorkRequest, token uuid.UUID, expires time.Time, begin bool, now time.Time) error {
	if request.ActionRequestID == nil {
		return nil
	}
	updates := map[string]any{"claim_expires_at": expires}
	if begin {
		updates["state"] = models.MediaSupplyActionRequestRunning
	}
	result := tx.Model(&models.MediaSupplyActionRequest{}).Where("tenant_id=? AND public_id=? AND claim_token=?", request.TenantID, *request.ActionRequestID, token).Updates(updates)
	if result.Error != nil || result.RowsAffected != 1 {
		return fmt.Errorf("linked atomization action is stale")
	}
	if begin {
		return tx.Model(&models.MediaSupplyActionAttempt{}).Where("tenant_id=? AND action_request_id=?", request.TenantID, *request.ActionRequestID).Order("attempt_number DESC").Updates(map[string]any{"state": models.MediaSupplyActionRequestRunning, "started_at": now, "effect_started_at": now}).Error
	}
	return nil
}
func verifyActionPending(tx *gorm.DB, request models.AtomizationWorkRequest, proof map[string]any) error {
	if request.ActionRequestID == nil {
		return nil
	}
	return tx.Model(&models.MediaSupplyActionRequest{}).Where("tenant_id=? AND public_id=?", request.TenantID, *request.ActionRequestID).Updates(map[string]any{"state": models.MediaSupplyActionRequestVerifying, "after_effects": jsonValue(map[string]any{"owner_checkpoint": proof, "verification": "pending"}), "claim_expires_at": nil}).Error
}
func completeAction(tx *gorm.DB, request models.AtomizationWorkRequest, proof datatypes.JSON, now time.Time) error {
	if request.ActionRequestID == nil {
		return nil
	}
	var action models.MediaSupplyActionRequest
	if err := tx.Where("tenant_id=? AND public_id=?", request.TenantID, *request.ActionRequestID).First(&action).Error; err != nil {
		return err
	}
	if err := tx.Model(&action).Updates(map[string]any{"state": models.MediaSupplyActionRequestSucceeded, "verified_effects": supply.VerifiedSupplyActionEffects(action, proof), "finished_at": now, "claim_expires_at": nil}).Error; err != nil {
		return err
	}
	supply.RecordSupplyVerifierQualificationCaseBestEffort(tx, action, true, supply.VerdictPresent, false)
	return tx.Model(&models.MediaSupplyActionAttempt{}).Where("tenant_id=? AND action_request_id=?", request.TenantID, *request.ActionRequestID).Order("attempt_number DESC").Updates(map[string]any{"state": models.MediaSupplyActionRequestSucceeded, "finished_at": now}).Error
}
func failAction(tx *gorm.DB, request models.AtomizationWorkRequest, proof datatypes.JSON, failure string, now time.Time) error {
	if request.ActionRequestID == nil {
		return nil
	}
	var action models.MediaSupplyActionRequest
	if err := tx.Where("tenant_id=? AND public_id=?", request.TenantID, *request.ActionRequestID).First(&action).Error; err != nil {
		return err
	}
	if err := tx.Model(&action).Updates(map[string]any{"state": models.MediaSupplyActionRequestFailed, "verified_effects": supply.VerifiedSupplyActionEffects(action, proof), "failure_class": failure, "finished_at": now, "claim_expires_at": nil}).Error; err != nil {
		return err
	}
	if err := supply.DemoteBoundSupplyPromotion(tx, action, "atomization_verifier_failed"); err != nil {
		return err
	}
	supply.RecordSupplyVerifierQualificationCaseBestEffort(tx, action, false, supply.VerdictAbsent, false)
	return tx.Model(&models.MediaSupplyActionAttempt{}).Where("tenant_id=? AND action_request_id=?", request.TenantID, *request.ActionRequestID).Order("attempt_number DESC").Updates(map[string]any{"state": models.MediaSupplyActionRequestFailed, "finished_at": now}).Error
}
func recoverAction(tx *gorm.DB, request models.AtomizationWorkRequest, next string, now time.Time) error {
	if request.ActionRequestID == nil {
		return nil
	}
	state := models.MediaSupplyActionRequestQueued
	failure := ""
	if next == "uncertain" {
		state = models.MediaSupplyActionRequestUncertain
		failure = "atomization_claim_expired_after_effect"
	}
	if err := tx.Model(&models.MediaSupplyActionRequest{}).Where("tenant_id=? AND public_id=?", request.TenantID, *request.ActionRequestID).Updates(map[string]any{"state": state, "claim_owner": "", "claim_token": nil, "claim_expires_at": nil, "failure_class": failure}).Error; err != nil {
		return err
	}
	if next == "uncertain" {
		return tx.Model(&models.MediaSupplyActionAttempt{}).Where("tenant_id=? AND action_request_id=?", request.TenantID, *request.ActionRequestID).Order("attempt_number DESC").Updates(map[string]any{"state": models.MediaSupplyActionRequestUncertain, "finished_at": now}).Error
	}
	return nil
}
func digest(values ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(values, "\n")))
	return hex.EncodeToString(sum[:])
}
func jsonValue(v any) datatypes.JSON { bytes, _ := json.Marshal(v); return datatypes.JSON(bytes) }
