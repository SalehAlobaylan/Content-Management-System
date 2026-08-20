package contentstage

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"content-management-system/src/feedstate"
	"content-management-system/src/models"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func ClaimNext(db *gorm.DB, tenantID, lane, claimOwner string) (ClaimEnvelope, bool, error) {
	expectedOwner := models.ContentStageOwnerAggregationNews
	allowedStages := []string{models.ContentStageNewsTextEmbedding, models.ContentStageNewsLLMMetadata}
	if lane == models.ContentStageLanePods {
		expectedOwner = models.ContentStageOwnerAggregationPods
		allowedStages = []string{models.ContentStagePodsMediaArtifacts, models.ContentStagePodsTextEmbedding, models.ContentStagePodsAtomization, models.ContentStagePodsCaptionReembedding, models.ContentStagePodsLLMMetadata}
	}
	return claimNextForOwner(db, tenantID, lane, expectedOwner, claimOwner, true, allowedStages)
}

func ClaimMediaNext(db *gorm.DB, tenantID, claimOwner string) (ClaimEnvelope, bool, error) {
	return claimNextForOwner(db, tenantID, models.ContentStageLanePods, models.ContentStageOwnerMedia, claimOwner, true, []string{models.ContentStagePodsTranscript, models.ContentStagePodsImageEmbedding})
}

func claimNextForOwner(db *gorm.DB, tenantID, lane, expectedOwner, claimOwner string, allowOptional bool, allowedStages []string) (ClaimEnvelope, bool, error) {
	if db == nil || strings.TrimSpace(tenantID) == "" || (lane != models.ContentStageLaneNews && lane != models.ContentStageLanePods) {
		return ClaimEnvelope{}, false, fmt.Errorf("tenant and registered lane are required")
	}
	if strings.TrimSpace(claimOwner) == "" {
		return ClaimEnvelope{}, false, fmt.Errorf("claim owner is required")
	}
	now := time.Now().UTC()
	var envelope ClaimEnvelope
	err := db.Transaction(func(tx *gorm.DB) error {
		scheduling, err := schedulingAllowed(tx, tenantID, lane)
		if err != nil || !scheduling {
			return err
		}
		var requests []models.ContentStageRequest
		query := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("tenant_id=? AND lane=? AND owner=? AND state IN ? AND (not_before_at IS NULL OR not_before_at<=?) AND cancellation_requested_at IS NULL", tenantID, lane, expectedOwner, []string{models.ContentStageQueued, models.ContentStageDeferred}, now)
		if !allowOptional {
			query = query.Where("blocking_scope<>?", models.ContentStageBlockingOptional)
		}
		if len(allowedStages) > 0 {
			query = query.Where("stage IN ?", allowedStages)
		}
		if err := query.Order("CASE WHEN blocking_scope='optional' THEN 1 ELSE 0 END, created_at ASC").Limit(24).Find(&requests).Error; err != nil {
			return err
		}
		var optionalOutstanding int64
		if err := tx.Model(&models.ContentStageRequest{}).Where("tenant_id=? AND lane=? AND blocking_scope=? AND state IN ?", tenantID, lane, models.ContentStageBlockingOptional, []string{models.ContentStageClaimed, models.ContentStageRunning, models.ContentStageVerifying, models.ContentStageUncertain, models.ContentStageReconciling}).Count(&optionalOutstanding).Error; err != nil {
			return err
		}
		for _, request := range requests {
			if request.BlockingScope == models.ContentStageBlockingOptional && optionalOutstanding >= 4 {
				continue
			}
			allowed, err := executionAllowed(tx, request.TenantID, request.Lane, request.Stage)
			if err != nil {
				return err
			}
			if !allowed {
				continue
			}
			ready, err := dependenciesVerified(tx, request)
			if err != nil {
				return err
			}
			if !ready {
				continue
			}
			var item models.ContentItem
			if err := tx.Where("tenant_id=? AND public_id=? AND processing_generation=? AND status<>?", request.TenantID, request.ContentItemID, request.ProcessingGeneration, models.ContentStatusArchived).First(&item).Error; err != nil {
				continue
			}
			if stageFingerprint(item, descriptors[request.Stage]) != request.InputFingerprint {
				if err := supersede(tx, request, "input_fingerprint_changed"); err != nil {
					return err
				}
				continue
			}
			attempt, reclaimed, err := reclaimUnstartedAttempt(tx, request, claimOwner, now)
			if err != nil {
				return err
			}
			if !reclaimed {
				var count int64
				if err := tx.Model(&models.ContentStageAttempt{}).Where("tenant_id=? AND request_id=? AND effect_started_at IS NOT NULL", request.TenantID, request.PublicID).Count(&count).Error; err != nil {
					return err
				}
				if count >= maxEffectAttempts || (request.DeadlineAt != nil && !request.DeadlineAt.After(now)) {
					if err := failRequest(tx, request, "execution_budget_exhausted", "verified effect remains absent"); err != nil {
						return err
					}
					continue
				}
				var total int64
				if err := tx.Model(&models.ContentStageAttempt{}).Where("tenant_id=? AND request_id=?", request.TenantID, request.PublicID).Count(&total).Error; err != nil {
					return err
				}
				claimToken, fence := uuid.New(), uuid.New()
				expires := now.Add(leaseDuration)
				attempt = models.ContentStageAttempt{
					PublicID: uuid.New(), TenantID: request.TenantID, RequestID: request.PublicID,
					AttemptNumber: int(total) + 1, Lane: request.Lane, Stage: request.Stage,
					Owner: request.Owner, InputFingerprint: request.InputFingerprint,
					State: models.ContentStageClaimed, ClaimToken: claimToken, FenceToken: fence,
					LeaseEpoch: 1, DeterministicJobID: fmt.Sprintf("stage:%s:%d", request.PublicID, int(total)+1),
					LeaseExpiresAt: expires, HeartbeatAt: now,
				}
				if err := tx.Create(&attempt).Error; err != nil {
					return err
				}
				if err := tx.Model(&request).Updates(map[string]any{
					"state": models.ContentStageClaimed, "claim_owner": claimOwner,
					"claim_token": claimToken, "claim_epoch": gorm.Expr("claim_epoch + 1"),
					"claim_expires_at": expires, "updated_at": now,
				}).Error; err != nil {
					return err
				}
				request.State, request.ClaimOwner, request.ClaimToken, request.ClaimExpiresAt = models.ContentStageClaimed, claimOwner, &claimToken, &expires
				request.ClaimEpoch++
				if err := appendEvent(tx, request, &attempt, "claimed", map[string]any{"claim_owner": claimOwner, "fence_token": fence}); err != nil {
					return err
				}
			}
			if err := reduceReadiness(tx, request.TenantID, request.ContentItemID, request.ProcessingGeneration); err != nil {
				return err
			}
			envelope, err = makeEnvelope(tx, request, attempt, item)
			if err != nil {
				return err
			}
			if request.BlockingScope == models.ContentStageBlockingOptional {
				optionalOutstanding++
			}
			return nil
		}
		return nil
	})
	if err != nil || envelope.RequestID == uuid.Nil {
		return envelope, false, err
	}
	return envelope, true, nil
}

func reclaimUnstartedAttempt(tx *gorm.DB, request models.ContentStageRequest, owner string, now time.Time) (models.ContentStageAttempt, bool, error) {
	var attempt models.ContentStageAttempt
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND request_id=? AND state=? AND effect_started_at IS NULL AND lease_expires_at<=?", request.TenantID, request.PublicID, models.ContentStageClaimed, now).Order("attempt_number DESC").First(&attempt).Error
	if err == gorm.ErrRecordNotFound {
		return models.ContentStageAttempt{}, false, nil
	}
	if err != nil {
		return models.ContentStageAttempt{}, false, err
	}
	token := uuid.New()
	expires := now.Add(leaseDuration)
	if err := tx.Model(&attempt).Updates(map[string]any{"claim_token": token, "lease_epoch": gorm.Expr("lease_epoch + 1"), "lease_expires_at": expires, "heartbeat_at": now, "updated_at": now}).Error; err != nil {
		return models.ContentStageAttempt{}, false, err
	}
	if err := tx.Model(&request).Updates(map[string]any{"state": models.ContentStageClaimed, "claim_owner": owner, "claim_token": token, "claim_epoch": gorm.Expr("claim_epoch + 1"), "claim_expires_at": expires, "updated_at": now}).Error; err != nil {
		return models.ContentStageAttempt{}, false, err
	}
	attempt.ClaimToken, attempt.LeaseEpoch, attempt.LeaseExpiresAt, attempt.HeartbeatAt = token, attempt.LeaseEpoch+1, expires, now
	request.State, request.ClaimOwner, request.ClaimToken, request.ClaimExpiresAt = models.ContentStageClaimed, owner, &token, &expires
	request.ClaimEpoch++
	if err := appendEvent(tx, request, &attempt, "reclaimed_unstarted", map[string]any{"lease_epoch": attempt.LeaseEpoch, "fence_token": attempt.FenceToken}); err != nil {
		return models.ContentStageAttempt{}, false, err
	}
	return attempt, true, nil
}

func makeEnvelope(tx *gorm.DB, request models.ContentStageRequest, attempt models.ContentStageAttempt, item models.ContentItem) (ClaimEnvelope, error) {
	input := boundedInput(item, request.Stage)
	if request.Stage == models.ContentStagePodsCaptionReembedding && item.TranscriptID != nil {
		var transcript models.Transcript
		if err := tx.Where("public_id=? AND content_item_id=?", *item.TranscriptID, item.PublicID).First(&transcript).Error; err != nil {
			return ClaimEnvelope{}, err
		}
		runes := []rune(strings.TrimSpace(transcript.FullText))
		if len(runes) > 12_000 {
			runes = runes[:12_000]
		}
		input["caption_text"] = string(runes)
	}
	return ClaimEnvelope{
		SchemaVersion: ProtocolVersion, RequestID: request.PublicID, AttemptID: attempt.PublicID,
		TenantID: request.TenantID, ContentItemID: request.ContentItemID,
		ProcessingGeneration: request.ProcessingGeneration, Lane: request.Lane, Stage: request.Stage,
		InputFingerprint: request.InputFingerprint, ClaimToken: attempt.ClaimToken,
		FenceToken: attempt.FenceToken, LeaseEpoch: attempt.LeaseEpoch,
		LeaseExpiresAt: attempt.LeaseExpiresAt, DeterministicJobID: attempt.DeterministicJobID,
		BoundedInput: input, Request: request, Attempt: attempt,
	}, nil
}

type Correlation struct {
	RequestID        string `json:"request_id"`
	AttemptID        string `json:"attempt_id"`
	ClaimToken       string `json:"claim_token"`
	FenceToken       string `json:"fence_token"`
	InputFingerprint string `json:"input_fingerprint"`
	ProducerEventID  string `json:"producer_event_id"`
}

func parseCorrelation(input Correlation) (uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID, error) {
	requestID, err := uuid.Parse(strings.TrimSpace(input.RequestID))
	if err != nil {
		return uuid.Nil, uuid.Nil, uuid.Nil, uuid.Nil, uuid.Nil, fmt.Errorf("invalid request id")
	}
	attemptID, err := uuid.Parse(strings.TrimSpace(input.AttemptID))
	if err != nil {
		return uuid.Nil, uuid.Nil, uuid.Nil, uuid.Nil, uuid.Nil, fmt.Errorf("invalid attempt id")
	}
	claim, err := uuid.Parse(strings.TrimSpace(input.ClaimToken))
	if err != nil {
		return uuid.Nil, uuid.Nil, uuid.Nil, uuid.Nil, uuid.Nil, fmt.Errorf("invalid claim token")
	}
	fence, err := uuid.Parse(strings.TrimSpace(input.FenceToken))
	if err != nil {
		return uuid.Nil, uuid.Nil, uuid.Nil, uuid.Nil, uuid.Nil, fmt.Errorf("invalid fence token")
	}
	eventID := uuid.Nil
	if strings.TrimSpace(input.ProducerEventID) != "" {
		eventID, err = uuid.Parse(strings.TrimSpace(input.ProducerEventID))
		if err != nil {
			return uuid.Nil, uuid.Nil, uuid.Nil, uuid.Nil, uuid.Nil, fmt.Errorf("invalid producer event id")
		}
	}
	return requestID, attemptID, claim, fence, eventID, nil
}

func loadCorrelated(tx *gorm.DB, contentID uuid.UUID, input Correlation, allowedStates []string, lock bool) (models.ContentStageRequest, models.ContentStageAttempt, error) {
	requestID, attemptID, claim, fence, _, err := parseCorrelation(input)
	if err != nil || strings.TrimSpace(input.InputFingerprint) == "" {
		return models.ContentStageRequest{}, models.ContentStageAttempt{}, fmt.Errorf("invalid content-stage correlation")
	}
	query := tx
	if lock {
		query = query.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	var request models.ContentStageRequest
	if err := query.Where("public_id=? AND content_item_id=? AND input_fingerprint=? AND state IN ? AND claim_token=? AND claim_expires_at>? AND cancellation_requested_at IS NULL", requestID, contentID, input.InputFingerprint, allowedStates, claim, time.Now().UTC()).First(&request).Error; err != nil {
		return request, models.ContentStageAttempt{}, fmt.Errorf("content-stage request is stale: %w", err)
	}
	var attempt models.ContentStageAttempt
	if err := tx.Where("public_id=? AND tenant_id=? AND request_id=? AND fence_token=? AND claim_token=? AND state IN ?", attemptID, request.TenantID, request.PublicID, fence, claim, allowedStates).First(&attempt).Error; err != nil {
		return request, attempt, fmt.Errorf("content-stage attempt is stale: %w", err)
	}
	return request, attempt, nil
}

func Begin(db *gorm.DB, requestID uuid.UUID, input Correlation) error {
	return transitionClaim(db, requestID, input, true)
}

func Heartbeat(db *gorm.DB, requestID uuid.UUID, input Correlation) error {
	return transitionClaim(db, requestID, input, false)
}

func transitionClaim(db *gorm.DB, requestID uuid.UUID, input Correlation, begin bool) error {
	parsed, _, _, _, _, err := parseCorrelation(input)
	if err != nil || parsed != requestID {
		return fmt.Errorf("request correlation does not match route")
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var request models.ContentStageRequest
		if err := tx.Where("public_id=?", requestID).First(&request).Error; err != nil {
			return err
		}
		states := []string{models.ContentStageClaimed, models.ContentStageRunning}
		request, attempt, err := loadCorrelated(tx, request.ContentItemID, input, states, true)
		if err != nil {
			return err
		}
		now, expires := time.Now().UTC(), time.Now().UTC().Add(leaseDuration)
		requestState, attemptState, event := request.State, attempt.State, "heartbeat"
		updates := map[string]any{"claim_expires_at": expires, "updated_at": now}
		attemptUpdates := map[string]any{"lease_expires_at": expires, "heartbeat_at": now, "updated_at": now}
		if begin {
			if request.State != models.ContentStageClaimed || attempt.State != models.ContentStageClaimed {
				return fmt.Errorf("content stage cannot begin")
			}
			requestState, attemptState, event = models.ContentStageRunning, models.ContentStageRunning, "began"
			updates["state"] = requestState
			updates["not_before_at"] = nil
			attemptUpdates["state"] = attemptState
			attemptUpdates["effect_started_at"] = now
		}
		if err := tx.Model(&request).Updates(updates).Error; err != nil {
			return err
		}
		if err := tx.Model(&attempt).Updates(attemptUpdates).Error; err != nil {
			return err
		}
		request.State, request.ClaimExpiresAt = requestState, &expires
		attempt.State, attempt.LeaseExpiresAt, attempt.HeartbeatAt = attemptState, expires, now
		if begin {
			attempt.EffectStartedAt = &now
		}
		return appendEvent(tx, request, &attempt, event, map[string]any{"lease_expires_at": expires})
	})
}

func MarkAccepted(db *gorm.DB, requestID uuid.UUID, input Correlation) error {
	return db.Transaction(func(tx *gorm.DB) error {
		var base models.ContentStageRequest
		if err := tx.Where("public_id=?", requestID).First(&base).Error; err != nil {
			return err
		}
		request, attempt, err := loadCorrelated(tx, base.ContentItemID, input, []string{models.ContentStageClaimed, models.ContentStageRunning}, true)
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		if err := tx.Model(&request).Update("accepted_at", now).Error; err != nil {
			return err
		}
		if err := tx.Model(&attempt).Update("accepted_at", now).Error; err != nil {
			return err
		}
		return appendEvent(tx, request, &attempt, "delivery_accepted", map[string]any{"deterministic_job_id": attempt.DeterministicJobID})
	})
}

// MarkNotRequired closes an already-started conditional stage only after its
// CMS owner has revalidated the condition. It is intentionally not a generic
// worker-controlled success transition.
func MarkNotRequired(db *gorm.DB, requestID uuid.UUID, input Correlation, proof map[string]any) error {
	return db.Transaction(func(tx *gorm.DB) error {
		var base models.ContentStageRequest
		if err := tx.Where("public_id=?", requestID).First(&base).Error; err != nil {
			return err
		}
		request, attempt, err := loadCorrelated(tx, base.ContentItemID, input, []string{models.ContentStageRunning}, true)
		if err != nil {
			return err
		}
		if request.Stage != models.ContentStagePodsAtomization {
			return fmt.Errorf("only conditional atomization can be settled as not required")
		}
		now := time.Now().UTC()
		terminalProof := jsonValue(proof)
		if err := tx.Model(&request).Updates(map[string]any{
			"state": models.ContentStageVerified, "verified_at": now, "finished_at": now,
			"terminal_proof": terminalProof, "claim_token": nil, "claim_expires_at": nil, "updated_at": now,
		}).Error; err != nil {
			return err
		}
		if err := tx.Model(&attempt).Updates(map[string]any{"state": models.ContentStageVerified, "finished_at": now, "updated_at": now}).Error; err != nil {
			return err
		}
		request.State, request.VerifiedAt, request.FinishedAt, request.TerminalProof = models.ContentStageVerified, &now, &now, terminalProof
		attempt.State, attempt.FinishedAt = models.ContentStageVerified, &now
		if err := appendEvent(tx, request, &attempt, "verified_not_required", proof); err != nil {
			return err
		}
		return reduceReadiness(tx, request.TenantID, request.ContentItemID, request.ProcessingGeneration)
	})
}

func MarkDeferred(db *gorm.DB, requestID uuid.UUID, input Correlation, notBefore time.Time, reason string) error {
	return terminalTransition(db, requestID, input, models.ContentStageDeferred, "capacity_deferred", reason, &notBefore)
}

func MarkUncertain(db *gorm.DB, requestID uuid.UUID, input Correlation, reason string) error {
	return terminalTransition(db, requestID, input, models.ContentStageUncertain, "effect_unknown", reason, nil)
}

func MarkFailed(db *gorm.DB, requestID uuid.UUID, input Correlation, failureClass, summary string) error {
	return terminalTransition(db, requestID, input, models.ContentStageFailed, failureClass, summary, nil)
}

func terminalTransition(db *gorm.DB, requestID uuid.UUID, input Correlation, state, failureClass, summary string, notBefore *time.Time) error {
	return db.Transaction(func(tx *gorm.DB) error {
		var base models.ContentStageRequest
		if err := tx.Where("public_id=?", requestID).First(&base).Error; err != nil {
			return err
		}
		request, attempt, err := loadCorrelated(tx, base.ContentItemID, input, []string{models.ContentStageClaimed, models.ContentStageRunning}, true)
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		requestUpdates := map[string]any{"state": state, "failure_class": failureClass, "claim_token": nil, "claim_expires_at": nil, "updated_at": now}
		if notBefore != nil {
			requestUpdates["not_before_at"] = *notBefore
		} else {
			requestUpdates["not_before_at"] = now
		}
		attemptUpdates := map[string]any{"state": state, "failure_class": failureClass, "failure_summary": summary, "finished_at": now, "updated_at": now}
		// A capacity defer proves that the downstream effect never started. Begin
		// records the worker-side execution boundary before making the dependency
		// call, so clear that provisional marker when the dependency explicitly
		// rejects admission. This keeps capacity pressure out of the bounded
		// post-effect retry budget.
		if state == models.ContentStageDeferred && failureClass == "capacity_deferred" {
			attemptUpdates["effect_started_at"] = nil
		}
		if err := tx.Model(&request).Updates(requestUpdates).Error; err != nil {
			return err
		}
		if err := tx.Model(&attempt).Updates(attemptUpdates).Error; err != nil {
			return err
		}
		request.State, request.FailureClass, request.ClaimToken, request.ClaimExpiresAt = state, failureClass, nil, nil
		attempt.State, attempt.FailureClass, attempt.FailureSummary, attempt.FinishedAt = state, failureClass, summary, &now
		if state == models.ContentStageDeferred && failureClass == "capacity_deferred" {
			attempt.EffectStartedAt = nil
		}
		if err := appendEvent(tx, request, &attempt, state, map[string]any{"failure_class": failureClass, "summary": summary, "not_before_at": notBefore}); err != nil {
			return err
		}
		return reduceReadiness(tx, request.TenantID, request.ContentItemID, request.ProcessingGeneration)
	})
}

func AuthorizeWriteback(tx *gorm.DB, contentID uuid.UUID, input Correlation, expectedStage string) (models.ContentStageRequest, models.ContentStageAttempt, error) {
	request, attempt, err := loadCorrelated(tx, contentID, input, []string{models.ContentStageRunning}, true)
	if err != nil {
		return request, attempt, err
	}
	if request.Stage != expectedStage || request.ProcessingGeneration <= 0 {
		return request, attempt, fmt.Errorf("content-stage writeback stage mismatch")
	}
	var item models.ContentItem
	if err := tx.Where("tenant_id=? AND public_id=? AND processing_generation=?", request.TenantID, contentID, request.ProcessingGeneration).First(&item).Error; err != nil {
		return request, attempt, fmt.Errorf("content-stage target generation changed: %w", err)
	}
	return request, attempt, nil
}

func RecordPersistence(tx *gorm.DB, request models.ContentStageRequest, attempt models.ContentStageAttempt, input Correlation, owner, artifactDigest string, payload map[string]any) error {
	_, _, _, _, producerEventID, err := parseCorrelation(input)
	if err != nil || producerEventID == uuid.Nil {
		return fmt.Errorf("producer event id is required")
	}
	raw, _ := json.Marshal(payload)
	receipt := models.ContentStageReceipt{
		PublicID: uuid.New(), TenantID: request.TenantID, RequestID: request.PublicID,
		AttemptID: attempt.PublicID, ContentItemID: request.ContentItemID,
		ProcessingGeneration: request.ProcessingGeneration, Lane: request.Lane, Stage: request.Stage,
		Owner: owner, ProducerEventID: producerEventID, FenceToken: attempt.FenceToken,
		InputFingerprint: request.InputFingerprint, Outcome: "persisted", PayloadDigest: digest(string(raw)),
		ArtifactDigest: artifactDigest, ObservedAt: time.Now().UTC(), Payload: jsonValue(payload),
	}
	result := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}, {Name: "owner"}, {Name: "producer_event_id"}}, DoNothing: true}).Create(&receipt)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return nil
	}
	now := time.Now().UTC()
	if err := tx.Model(&request).Updates(map[string]any{"state": models.ContentStageVerifying, "claim_token": nil, "claim_expires_at": nil, "not_before_at": now, "updated_at": now}).Error; err != nil {
		return err
	}
	if err := tx.Model(&attempt).Updates(map[string]any{"state": models.ContentStageVerifying, "finished_at": now, "updated_at": now}).Error; err != nil {
		return err
	}
	request.State, request.ClaimToken, request.ClaimExpiresAt = models.ContentStageVerifying, nil, nil
	attempt.State, attempt.FinishedAt = models.ContentStageVerifying, &now
	return appendEvent(tx, request, &attempt, "effect_persisted", map[string]any{"producer_event_id": producerEventID, "artifact_digest": artifactDigest})
}

func VerifyOne(db *gorm.DB) (bool, error) {
	var request models.ContentStageRequest
	err := db.Where("state IN ? AND (not_before_at IS NULL OR not_before_at<=?)", []string{models.ContentStageVerifying, models.ContentStageUncertain, models.ContentStageReconciling}, time.Now().UTC()).Order("updated_at ASC").First(&request).Error
	if err == gorm.ErrRecordNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, VerifyRequest(db, request.PublicID)
}

// VerifyShadowOne closes manifest stages from artifacts produced by the
// compatibility pipeline. Shadow mode never issues claims; this observer is
// what makes parity measurable before a lane is promoted.
func VerifyShadowOne(db *gorm.DB) (bool, error) {
	var request models.ContentStageRequest
	err := db.Model(&models.ContentStageRequest{}).
		Joins("JOIN content_stage_cutovers c ON c.tenant_id=content_stage_requests.tenant_id AND c.lane=content_stage_requests.lane AND c.mode=?", models.ContentStageCutoverShadow).
		Where("content_stage_requests.state IN ? AND (content_stage_requests.not_before_at IS NULL OR content_stage_requests.not_before_at<=?)", []string{models.ContentStageQueued, models.ContentStageDeferred}, time.Now().UTC()).
		Order("content_stage_requests.created_at ASC").First(&request).Error
	if err == gorm.ErrRecordNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id=? AND state IN ?", request.PublicID, []string{models.ContentStageQueued, models.ContentStageDeferred}).First(&request).Error; err != nil {
			return err
		}
		ready, err := dependenciesVerified(tx, request)
		if err != nil {
			return err
		}
		if !ready {
			return tx.Model(&request).Update("not_before_at", time.Now().UTC().Add(2*time.Second)).Error
		}
		var item models.ContentItem
		if err := tx.Where("tenant_id=? AND public_id=? AND processing_generation=?", request.TenantID, request.ContentItemID, request.ProcessingGeneration).First(&item).Error; err != nil {
			return err
		}
		present, proof, err := artifactPresent(tx, item, request.Stage)
		if err != nil {
			return err
		}
		if !present {
			return tx.Model(&request).Update("not_before_at", time.Now().UTC().Add(5*time.Second)).Error
		}
		now := time.Now().UTC()
		proof["observed_in_shadow"] = true
		if err := tx.Model(&request).Updates(map[string]any{"state": models.ContentStageVerified, "verified_at": now, "finished_at": now, "terminal_proof": jsonValue(proof), "failure_class": "", "updated_at": now}).Error; err != nil {
			return err
		}
		request.State, request.VerifiedAt, request.FinishedAt, request.TerminalProof = models.ContentStageVerified, &now, &now, jsonValue(proof)
		if err := appendEvent(tx, request, nil, "shadow_artifact_verified", proof); err != nil {
			return err
		}
		if request.Stage == models.ContentStagePodsMediaArtifacts {
			return settleConditionalPodsStages(tx, item)
		}
		return nil
	})
}

func VerifyRequest(db *gorm.DB, requestID uuid.UUID) error {
	return db.Transaction(func(tx *gorm.DB) error {
		var request models.ContentStageRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id=?", requestID).First(&request).Error; err != nil {
			return err
		}
		var item models.ContentItem
		if err := tx.Where("tenant_id=? AND public_id=? AND processing_generation=?", request.TenantID, request.ContentItemID, request.ProcessingGeneration).First(&item).Error; err != nil {
			return supersede(tx, request, "target_generation_changed")
		}
		present, proof, err := artifactPresent(tx, item, request.Stage)
		if err != nil {
			return err
		}
		if present && request.Stage != models.ContentStageNewsStoryClassification {
			var receipts int64
			if err := tx.Model(&models.ContentStageReceipt{}).Where("tenant_id=? AND request_id=? AND input_fingerprint=? AND outcome=?", request.TenantID, request.PublicID, request.InputFingerprint, "persisted").Count(&receipts).Error; err != nil {
				return err
			}
			if receipts == 0 {
				present = false
				proof["matching_persistence_receipt"] = false
			}
		}
		if present {
			now := time.Now().UTC()
			if err := tx.Model(&request).Updates(map[string]any{"state": models.ContentStageVerified, "verified_at": now, "finished_at": now, "terminal_proof": jsonValue(proof), "failure_class": "", "updated_at": now}).Error; err != nil {
				return err
			}
			request.State, request.VerifiedAt, request.FinishedAt, request.TerminalProof = models.ContentStageVerified, &now, &now, jsonValue(proof)
			if err := appendEvent(tx, request, nil, "verified", proof); err != nil {
				return err
			}
			if request.Stage == models.ContentStagePodsMediaArtifacts {
				if err := settleConditionalPodsStages(tx, item); err != nil {
					return err
				}
			}
			return reduceReadiness(tx, request.TenantID, request.ContentItemID, request.ProcessingGeneration)
		}
		if (request.State == models.ContentStageUncertain || request.State == models.ContentStageVerifying) && time.Since(request.UpdatedAt) < verificationWindow {
			// Do not let one recently absent artifact monopolize VerifyOne. The
			// initial observation happens immediately; the next one is scheduled
			// at the uncertainty window without advancing UpdatedAt.
			return tx.Model(&request).UpdateColumn("not_before_at", request.UpdatedAt.Add(verificationWindow)).Error
		}
		var attempts int64
		if err := tx.Model(&models.ContentStageAttempt{}).Where("tenant_id=? AND request_id=? AND effect_started_at IS NOT NULL", request.TenantID, request.PublicID).Count(&attempts).Error; err != nil {
			return err
		}
		if attempts < maxEffectAttempts && (request.DeadlineAt == nil || request.DeadlineAt.After(time.Now().UTC())) {
			now := time.Now().UTC()
			if err := tx.Model(&request).Updates(map[string]any{"state": models.ContentStageQueued, "failure_class": "verified_absent", "not_before_at": now, "updated_at": now}).Error; err != nil {
				return err
			}
			request.State = models.ContentStageQueued
			return appendEvent(tx, request, nil, "verified_absent_requeued", map[string]any{"attempts": attempts})
		}
		return failRequest(tx, request, "verified_absent_budget_exhausted", "expected stage artifact is absent")
	})
}

func artifactPresent(tx *gorm.DB, item models.ContentItem, stage string) (bool, map[string]any, error) {
	switch stage {
	case models.ContentStageNewsTextEmbedding, models.ContentStagePodsTextEmbedding, models.ContentStagePodsCaptionReembedding:
		ok := item.Embedding != nil && item.EmbeddingModel != nil && item.EmbeddingSpaceID != nil && item.EmbeddingProducerID != nil
		return ok, map[string]any{"embedding_model": item.EmbeddingModel, "space_id": item.EmbeddingSpaceID, "producer_id": item.EmbeddingProducerID}, nil
	case models.ContentStageNewsStoryClassification:
		return item.StoryID != nil, map[string]any{"story_id": item.StoryID}, nil
	case models.ContentStagePodsMediaArtifacts:
		ok := item.PlaybackURL != nil && strings.TrimSpace(*item.PlaybackURL) != "" && item.DurationSec != nil && *item.DurationSec > 0
		return ok, map[string]any{"playback_url_present": ok, "duration_sec": item.DurationSec, "playback_type": item.PlaybackType}, nil
	case models.ContentStagePodsTranscript:
		return item.TranscriptID != nil, map[string]any{"transcript_id": item.TranscriptID, "transcript_source": item.TranscriptSource}, nil
	case models.ContentStagePodsAtomization:
		if item.DurationSec != nil && *item.DurationSec <= 2400 {
			return true, map[string]any{"not_required": true, "duration_sec": *item.DurationSec}, nil
		}
		var count int64
		err := tx.Model(&models.ContentItem{}).Where("tenant_id=? AND parent_content_item_id=? AND is_feed_unit=true AND status=?", item.TenantID, item.PublicID, models.ContentStatusReady).Count(&count).Error
		return count > 0, map[string]any{"ready_feed_unit_children": count}, err
	case models.ContentStagePodsImageEmbedding:
		return item.ImageEmbedding != nil, map[string]any{"image_embedding_model": item.ImageEmbeddingModel}, nil
	case models.ContentStageNewsLLMMetadata, models.ContentStagePodsLLMMetadata:
		var metadata map[string]any
		if len(item.Metadata) == 0 || json.Unmarshal(item.Metadata, &metadata) != nil {
			return false, map[string]any{}, nil
		}
		_, summary := metadata["summary"]
		_, points := metadata["key_points"]
		return summary || points, map[string]any{"summary": summary, "key_points": points}, nil
	default:
		return false, nil, fmt.Errorf("stage verifier is not registered")
	}
}

// AdoptPresentStages records artifacts created atomically by another
// CMS-governed stage (notably atomized child renditions/transcripts) before a
// child manifest is dispatched. It never invents success: every adopted stage
// passes the same artifact verifier used after worker writeback.
func AdoptPresentStages(tx *gorm.DB, item models.ContentItem, provenance string) error {
	var requests []models.ContentStageRequest
	if err := tx.Where("tenant_id=? AND content_item_id=? AND processing_generation=? AND state IN ?", item.TenantID, item.PublicID, item.ProcessingGeneration, []string{models.ContentStageQueued, models.ContentStageDeferred}).Order("created_at ASC").Find(&requests).Error; err != nil {
		return err
	}
	for _, request := range requests {
		ready, err := dependenciesVerified(tx, request)
		if err != nil || !ready {
			if err != nil {
				return err
			}
			continue
		}
		present, proof, err := artifactPresent(tx, item, request.Stage)
		if err != nil {
			return err
		}
		if !present {
			continue
		}
		now := time.Now().UTC()
		proof["adopted_from"] = provenance
		if err := tx.Model(&request).Updates(map[string]any{"state": models.ContentStageVerified, "verified_at": now, "finished_at": now, "terminal_proof": jsonValue(proof), "failure_class": "", "updated_at": now}).Error; err != nil {
			return err
		}
		request.State, request.VerifiedAt, request.FinishedAt, request.TerminalProof = models.ContentStageVerified, &now, &now, jsonValue(proof)
		if err := appendEvent(tx, request, nil, "existing_artifact_adopted", proof); err != nil {
			return err
		}
		if request.Stage == models.ContentStagePodsMediaArtifacts {
			if err := settleConditionalPodsStages(tx, item); err != nil {
				return err
			}
		}
	}
	return reduceReadiness(tx, item.TenantID, item.PublicID, item.ProcessingGeneration)
}

func settleConditionalPodsStages(tx *gorm.DB, item models.ContentItem) error {
	if item.DurationSec == nil || *item.DurationSec > 2400 {
		return nil
	}
	now := time.Now().UTC()
	var requests []models.ContentStageRequest
	if err := tx.Where("tenant_id=? AND content_item_id=? AND processing_generation=? AND stage IN ? AND state NOT IN ?", item.TenantID, item.PublicID, item.ProcessingGeneration, []string{models.ContentStagePodsTranscript, models.ContentStagePodsAtomization, models.ContentStagePodsCaptionReembedding}, []string{models.ContentStageVerified, models.ContentStageCancelled, models.ContentStageSuperseded}).Find(&requests).Error; err != nil {
		return err
	}
	hasCaptionArtifact := false
	if len(item.Metadata) > 0 {
		var metadata map[string]any
		if json.Unmarshal(item.Metadata, &metadata) == nil {
			_, hasCaptionArtifact = metadata["caption_artifact"].(map[string]any)
		}
	}
	for _, request := range requests {
		if hasCaptionArtifact && request.Stage == models.ContentStagePodsTranscript {
			if err := tx.Model(&request).Updates(map[string]any{"blocking_scope": models.ContentStageBlockingOptional, "not_before_at": now, "updated_at": now}).Error; err != nil {
				return err
			}
			request.BlockingScope = models.ContentStageBlockingOptional
			if err := appendEvent(tx, request, nil, "made_optional", map[string]any{"reason": "raw_parent_caption_enrichment"}); err != nil {
				return err
			}
			continue
		}
		if hasCaptionArtifact && request.Stage == models.ContentStagePodsCaptionReembedding {
			continue
		}
		proof := jsonValue(map[string]any{"not_required": true, "duration_sec": *item.DurationSec, "policy": "raw_parent_at_or_under_40m"})
		if err := tx.Model(&request).Updates(map[string]any{"state": models.ContentStageVerified, "verified_at": now, "finished_at": now, "terminal_proof": proof, "updated_at": now}).Error; err != nil {
			return err
		}
		request.State, request.VerifiedAt, request.FinishedAt, request.TerminalProof = models.ContentStageVerified, &now, &now, proof
		if err := appendEvent(tx, request, nil, "verified_not_required", map[string]any{"duration_sec": *item.DurationSec}); err != nil {
			return err
		}
	}
	return nil
}

func RecoverExpired(db *gorm.DB) error {
	now := time.Now().UTC()
	return db.Transaction(func(tx *gorm.DB) error {
		var requests []models.ContentStageRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Where("state IN ? AND claim_expires_at<=?", []string{models.ContentStageClaimed, models.ContentStageRunning}, now).Limit(50).Find(&requests).Error; err != nil {
			return err
		}
		for _, request := range requests {
			var attempt models.ContentStageAttempt
			if err := tx.Where("tenant_id=? AND request_id=?", request.TenantID, request.PublicID).Order("attempt_number DESC").First(&attempt).Error; err != nil {
				continue
			}
			if request.CancellationRequestedAt != nil {
				if err := cancelRequest(tx, &request, &attempt, request.CancellationReason); err != nil {
					return err
				}
				continue
			}
			if attempt.EffectStartedAt == nil {
				if err := tx.Model(&request).Updates(map[string]any{"state": models.ContentStageQueued, "claim_token": nil, "claim_expires_at": nil, "failure_class": "lease_expired_before_effect", "updated_at": now}).Error; err != nil {
					return err
				}
				request.State = models.ContentStageQueued
				if err := appendEvent(tx, request, &attempt, "lease_expired_before_effect", map[string]any{"reclaimable": true}); err != nil {
					return err
				}
				if err := reduceReadiness(tx, request.TenantID, request.ContentItemID, request.ProcessingGeneration); err != nil {
					return err
				}
				continue
			}
			if err := tx.Model(&request).Updates(map[string]any{"state": models.ContentStageUncertain, "claim_token": nil, "claim_expires_at": nil, "failure_class": "lease_expired_after_effect", "updated_at": now}).Error; err != nil {
				return err
			}
			if err := tx.Model(&attempt).Updates(map[string]any{"state": models.ContentStageUncertain, "failure_class": "lease_expired_after_effect", "finished_at": now, "updated_at": now}).Error; err != nil {
				return err
			}
			request.State = models.ContentStageUncertain
			if err := appendEvent(tx, request, &attempt, "lease_expired_after_effect", map[string]any{"verification_required": true}); err != nil {
				return err
			}
			if err := reduceReadiness(tx, request.TenantID, request.ContentItemID, request.ProcessingGeneration); err != nil {
				return err
			}
		}
		return nil
	})
}

func Cancel(db *gorm.DB, tenantID string, requestID uuid.UUID, reason string) error {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "operator_cancelled"
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var request models.ContentStageRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", tenantID, requestID).First(&request).Error; err != nil {
			return err
		}
		if request.State == models.ContentStageVerified || request.State == models.ContentStageFailed || request.State == models.ContentStageCancelled || request.State == models.ContentStageSuperseded {
			return nil
		}
		var attempt models.ContentStageAttempt
		result := tx.Where("tenant_id=? AND request_id=?", tenantID, request.PublicID).Order("attempt_number DESC").First(&attempt)
		if result.Error != nil && result.Error != gorm.ErrRecordNotFound {
			return result.Error
		}
		if result.Error == gorm.ErrRecordNotFound {
			return cancelRequest(tx, &request, nil, reason)
		}
		return cancelRequest(tx, &request, &attempt, reason)
	})
}

func cancelRequest(tx *gorm.DB, request *models.ContentStageRequest, attempt *models.ContentStageAttempt, reason string) error {
	now := time.Now().UTC()
	proof := jsonValue(map[string]any{"cancelled": true, "reason": reason})
	if err := tx.Model(request).Updates(map[string]any{
		"state": models.ContentStageCancelled, "cancellation_requested_at": now, "cancellation_reason": reason,
		"finished_at": now, "terminal_proof": proof, "claim_token": nil, "claim_expires_at": nil, "updated_at": now,
	}).Error; err != nil {
		return err
	}
	request.State, request.CancellationRequestedAt, request.CancellationReason, request.FinishedAt, request.TerminalProof = models.ContentStageCancelled, &now, reason, &now, proof
	if attempt != nil {
		if err := tx.Model(attempt).Updates(map[string]any{"state": models.ContentStageCancelled, "failure_class": "cancelled", "failure_summary": reason, "finished_at": now, "updated_at": now}).Error; err != nil {
			return err
		}
		attempt.State, attempt.FailureClass, attempt.FailureSummary, attempt.FinishedAt = models.ContentStageCancelled, "cancelled", reason, &now
	}
	if err := appendEvent(tx, *request, attempt, "cancelled", map[string]any{"reason": reason}); err != nil {
		return err
	}
	return reduceReadiness(tx, request.TenantID, request.ContentItemID, request.ProcessingGeneration)
}

func supersede(tx *gorm.DB, request models.ContentStageRequest, reason string) error {
	now := time.Now().UTC()
	if err := tx.Model(&request).Updates(map[string]any{"state": models.ContentStageSuperseded, "failure_class": reason, "finished_at": now, "claim_token": nil, "claim_expires_at": nil, "updated_at": now}).Error; err != nil {
		return err
	}
	request.State, request.FailureClass, request.FinishedAt = models.ContentStageSuperseded, reason, &now
	return appendEvent(tx, request, nil, "superseded", map[string]any{"reason": reason})
}

func failRequest(tx *gorm.DB, request models.ContentStageRequest, failureClass, summary string) error {
	now := time.Now().UTC()
	proof := jsonValue(map[string]any{"effect": "absent", "failure_class": failureClass, "summary": summary})
	if err := tx.Model(&request).Updates(map[string]any{"state": models.ContentStageFailed, "failure_class": failureClass, "finished_at": now, "terminal_proof": proof, "claim_token": nil, "claim_expires_at": nil, "updated_at": now}).Error; err != nil {
		return err
	}
	request.State, request.FailureClass, request.FinishedAt, request.TerminalProof = models.ContentStageFailed, failureClass, &now, proof
	if err := appendEvent(tx, request, nil, "failed", map[string]any{"failure_class": failureClass, "summary": summary}); err != nil {
		return err
	}
	return reduceReadiness(tx, request.TenantID, request.ContentItemID, request.ProcessingGeneration)
}

func ReduceReadiness(db *gorm.DB, tenantID string, contentID uuid.UUID, generation int64) error {
	return db.Transaction(func(tx *gorm.DB) error { return reduceReadiness(tx, tenantID, contentID, generation) })
}

func reduceReadiness(tx *gorm.DB, tenantID string, contentID uuid.UUID, generation int64) error {
	var item models.ContentItem
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=? AND processing_generation=?", tenantID, contentID, generation).First(&item).Error; err != nil {
		return err
	}
	mode, err := CutoverMode(tx, tenantID, laneForType(item.Type))
	if err != nil || mode != models.ContentStageCutoverDurableRequired {
		return err
	}
	var requests []models.ContentStageRequest
	if err := tx.Where("tenant_id=? AND content_item_id=? AND processing_generation=? AND blocking_scope=?", tenantID, contentID, generation, models.ContentStageBlockingContentReady).Find(&requests).Error; err != nil {
		return err
	}
	if len(requests) == 0 {
		return nil
	}
	allVerified, hasFailed, hasActive := true, false, false
	for _, request := range requests {
		if request.State != models.ContentStageVerified {
			allVerified = false
		}
		if request.State == models.ContentStageFailed {
			hasFailed = true
		}
		if request.State != models.ContentStageQueued && request.State != models.ContentStageVerified && request.State != models.ContentStageFailed && request.State != models.ContentStageCancelled && request.State != models.ContentStageSuperseded {
			hasActive = true
		}
	}
	next := models.ContentStatusPending
	if allVerified {
		next = models.ContentStatusReady
	} else if hasFailed {
		next = models.ContentStatusFailed
	} else if hasActive {
		next = models.ContentStatusProcessing
	}
	if item.Status == next {
		return nil
	}
	item.Status = next
	if err := tx.Save(&item).Error; err != nil {
		return err
	}
	if err := feedstate.AttachReadyNewsStory(tx, item); err != nil {
		return err
	}
	return feedstate.SyncMediaMembership(tx, item)
}

func laneForType(kind models.ContentType) string {
	if kind == models.ContentTypeNews {
		return models.ContentStageLaneNews
	}
	return models.ContentStageLanePods
}

func Trace(db *gorm.DB, tenantID string, contentID uuid.UUID) (map[string]any, error) {
	var item models.ContentItem
	if err := db.Where("tenant_id=? AND public_id=?", tenantID, contentID).First(&item).Error; err != nil {
		return nil, err
	}
	var requests []models.ContentStageRequest
	if err := db.Where("tenant_id=? AND content_item_id=?", tenantID, contentID).Order("processing_generation, created_at").Find(&requests).Error; err != nil {
		return nil, err
	}
	ids := make([]uuid.UUID, 0, len(requests))
	for _, request := range requests {
		ids = append(ids, request.PublicID)
	}
	var attempts []models.ContentStageAttempt
	var receipts []models.ContentStageReceipt
	var events []models.ContentStageEvent
	if len(ids) > 0 {
		if err := db.Where("tenant_id=? AND request_id IN ?", tenantID, ids).Order("created_at").Find(&attempts).Error; err != nil {
			return nil, err
		}
		if err := db.Where("tenant_id=? AND request_id IN ?", tenantID, ids).Order("created_at").Find(&receipts).Error; err != nil {
			return nil, err
		}
		if err := db.Where("tenant_id=? AND request_id IN ?", tenantID, ids).Order("occurred_at, sequence").Find(&events).Error; err != nil {
			return nil, err
		}
	}
	return map[string]any{"item": item, "requests": requests, "attempts": attempts, "receipts": receipts, "events": events}, nil
}
