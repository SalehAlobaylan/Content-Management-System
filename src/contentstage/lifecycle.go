package contentstage

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"content-management-system/src/feedcontract"
	"content-management-system/src/feedstate"
	"content-management-system/src/models"
	"content-management-system/src/pipeline"

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

// ClaimNextAnyTenant is the production claim path. CMS selects the tenant;
// callers provide only a lane and receive a fully tenant-scoped envelope.
func ClaimNextAnyTenant(db *gorm.DB, lane, claimOwner string) (ClaimEnvelope, bool, error) {
	expectedOwner := models.ContentStageOwnerAggregationNews
	allowedStages := []string{models.ContentStageNewsTextEmbedding, models.ContentStageNewsLLMMetadata}
	if lane == models.ContentStageLanePods {
		expectedOwner = models.ContentStageOwnerAggregationPods
		allowedStages = []string{models.ContentStagePodsMediaArtifacts, models.ContentStagePodsTextEmbedding, models.ContentStagePodsAtomization, models.ContentStagePodsCaptionReembedding, models.ContentStagePodsLLMMetadata}
	}
	return claimNextForOwner(db, "", lane, expectedOwner, claimOwner, true, allowedStages)
}

// ClaimNextAnyTenantForStages is used when a deployment role owns only a
// subset of a lane's queues. CMS still selects the tenant and applies the
// same fenced lifecycle rules; the stage filter only prevents a controller
// from claiming work it cannot deliver to its local queue set.
func ClaimNextAnyTenantForStages(db *gorm.DB, lane, claimOwner string, allowedStages []string) (ClaimEnvelope, bool, error) {
	expectedOwner := models.ContentStageOwnerAggregationNews
	if lane == models.ContentStageLanePods {
		expectedOwner = models.ContentStageOwnerAggregationPods
	}
	return claimNextForOwner(db, "", lane, expectedOwner, claimOwner, true, allowedStages)
}

func ClaimMediaNextAnyTenant(db *gorm.DB, claimOwner string) (ClaimEnvelope, bool, error) {
	return claimNextForOwner(db, "", models.ContentStageLanePods, models.ContentStageOwnerMedia, claimOwner, true, []string{models.ContentStagePodsTranscript, models.ContentStagePodsImageEmbedding})
}

func ClaimMediaNext(db *gorm.DB, tenantID, claimOwner string) (ClaimEnvelope, bool, error) {
	return claimNextForOwner(db, tenantID, models.ContentStageLanePods, models.ContentStageOwnerMedia, claimOwner, true, []string{models.ContentStagePodsTranscript, models.ContentStagePodsImageEmbedding})
}

type claimCandidateFilter struct {
	tenantID      string
	lane          string
	expectedOwner string
	allowedStages []string
	optional      bool
	now           time.Time
}

// eligibleClaimCandidateScope deliberately starts from the transaction handle
// on every call. A tenant-ranking statement uses GROUP BY and aggregate
// ordering, while a candidate statement uses FOR UPDATE SKIP LOCKED; sharing a
// chained GORM scope between those statements leaks the aggregate clauses into
// the row lock and PostgreSQL rejects the resulting query.
func eligibleClaimCandidateScope(tx *gorm.DB, filter claimCandidateFilter) *gorm.DB {
	scope := tx.Model(&models.ContentStageRequest{}).
		Where(
			"lane=? AND owner=? AND state IN ? AND (not_before_at IS NULL OR not_before_at<=?) AND cancellation_requested_at IS NULL",
			filter.lane,
			filter.expectedOwner,
			[]string{models.ContentStageQueued, models.ContentStageDeferred},
			filter.now,
		)
	if filter.lane == models.ContentStageLanePods {
		// Metadata discovery may create durable intent for many media items at
		// once, but execution is deliberately serial per source. A request is
		// claimable only when it belongs to the oldest admitted item from that
		// source whose required pipeline has not reached a terminal state. Items
		// still awaiting media approval are not admitted and therefore do not
		// block an operator-selected item.
		scope = scope.Where(`
			NOT EXISTS (
				SELECT 1
				FROM content_items claim_item
				JOIN content_items earlier_item
				  ON earlier_item.tenant_id = claim_item.tenant_id
				 AND earlier_item.content_source_id = claim_item.content_source_id
				 AND earlier_item.parent_content_item_id IS NULL
				 AND (
					earlier_item.created_at < claim_item.created_at
					OR (earlier_item.created_at = claim_item.created_at AND earlier_item.public_id::text < claim_item.public_id::text)
				 )
				WHERE claim_item.tenant_id = content_stage_requests.tenant_id
				  AND claim_item.public_id = content_stage_requests.content_item_id
				  AND claim_item.processing_generation = content_stage_requests.processing_generation
				  AND claim_item.content_source_id IS NOT NULL
				  AND EXISTS (
					SELECT 1 FROM content_stage_requests earlier_media
					WHERE earlier_media.tenant_id = earlier_item.tenant_id
					  AND earlier_media.content_item_id = earlier_item.public_id
					  AND earlier_media.processing_generation = earlier_item.processing_generation
					  AND earlier_media.stage = ?
					  AND earlier_media.state NOT IN ?
				  )
				  AND EXISTS (
					SELECT 1 FROM content_stage_requests earlier_required
					WHERE earlier_required.tenant_id = earlier_item.tenant_id
					  AND earlier_required.content_item_id = earlier_item.public_id
					  AND earlier_required.processing_generation = earlier_item.processing_generation
					  AND earlier_required.blocking_scope <> ?
					  AND earlier_required.state NOT IN ?
				  )
			)
		`,
			models.ContentStagePodsMediaArtifacts,
			[]string{models.ContentStageAwaitingApproval, models.ContentStageCancelled, models.ContentStageSuperseded},
			models.ContentStageBlockingOptional,
			[]string{models.ContentStageVerified, models.ContentStageCancelled, models.ContentStageSuperseded},
		)
		// A late approval of an older metadata item must not start beside a
		// newer item that already crossed the effect boundary. Creation order
		// chooses the next idle item, while this guard enforces one required
		// end-to-end pipeline per source at a time.
		scope = scope.Where(`
			NOT EXISTS (
				SELECT 1
				FROM content_items claim_item
				JOIN content_items active_item
				  ON active_item.tenant_id = claim_item.tenant_id
				 AND active_item.content_source_id = claim_item.content_source_id
				 AND active_item.public_id <> claim_item.public_id
				JOIN content_stage_requests active_request
				  ON active_request.tenant_id = active_item.tenant_id
				 AND active_request.content_item_id = active_item.public_id
				 AND active_request.processing_generation = active_item.processing_generation
				WHERE claim_item.tenant_id = content_stage_requests.tenant_id
				  AND claim_item.public_id = content_stage_requests.content_item_id
				  AND claim_item.content_source_id IS NOT NULL
				  AND active_request.blocking_scope <> ?
				  AND active_request.state IN ?
			)
		`,
			models.ContentStageBlockingOptional,
			[]string{
				models.ContentStageClaimed,
				models.ContentStageRunning,
				models.ContentStageVerifying,
				models.ContentStageUncertain,
				models.ContentStageReconciling,
			},
		)
		// No work for a metadata-only item may bypass manual media admission.
		// Once admitted, media must verify before any other stage for that item
		// becomes claimable; this keeps each source item end-to-end instead of
		// advancing a batch of metadata through one stage at a time.
		scope = scope.Where(`
			content_stage_requests.stage = ? OR EXISTS (
				SELECT 1 FROM content_stage_requests item_media
				WHERE item_media.tenant_id = content_stage_requests.tenant_id
				  AND item_media.content_item_id = content_stage_requests.content_item_id
				  AND item_media.processing_generation = content_stage_requests.processing_generation
				  AND item_media.stage = ?
				  AND item_media.state = ?
			)
		`, models.ContentStagePodsMediaArtifacts, models.ContentStagePodsMediaArtifacts, models.ContentStageVerified)
	}
	if strings.TrimSpace(filter.tenantID) != "" {
		scope = scope.Where("tenant_id=?", filter.tenantID)
	}
	if len(filter.allowedStages) > 0 {
		scope = scope.Where("stage IN ?", filter.allowedStages)
	}
	if filter.optional {
		return scope.Where("blocking_scope=?", models.ContentStageBlockingOptional)
	}
	return scope.Where("blocking_scope<>?", models.ContentStageBlockingOptional)
}

func rankedClaimTenantScope(tx *gorm.DB, filter claimCandidateFilter) *gorm.DB {
	return eligibleClaimCandidateScope(tx, filter).
		Select("content_stage_requests.tenant_id").
		Group("content_stage_requests.tenant_id").
		Clauses(clause.OrderBy{Expression: clause.Expr{
			SQL:  "(SELECT MAX(csa.created_at) FROM content_stage_attempts csa WHERE csa.tenant_id=content_stage_requests.tenant_id AND csa.lane=? AND csa.owner=?) ASC NULLS FIRST, MIN(content_stage_requests.created_at) ASC, content_stage_requests.tenant_id ASC",
			Vars: []any{filter.lane, filter.expectedOwner},
		}}).
		Limit(64)
}

func lockedClaimCandidateScope(tx *gorm.DB, filter claimCandidateFilter, limit int) *gorm.DB {
	return eligibleClaimCandidateScope(tx, filter).
		Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
		Order("priority DESC, created_at ASC, public_id ASC").
		Limit(limit)
}

// effectAttemptsInCurrentBudget preserves the full attempt audit trail while
// treating a fresh operator media admission as a new bounded execution window.
// Without this boundary, an item reconciled back to awaiting approval can fail
// immediately because historical effects consumed the old window.
func effectAttemptsInCurrentBudget(tx *gorm.DB, request models.ContentStageRequest) (int64, error) {
	query := tx.Model(&models.ContentStageAttempt{}).
		Where("tenant_id=? AND request_id=? AND effect_started_at IS NOT NULL", request.TenantID, request.PublicID)
	if request.Stage == models.ContentStagePodsMediaArtifacts {
		var admission models.ContentStageEvent
		err := tx.Where("tenant_id=? AND request_id=? AND event_type=?", request.TenantID, request.PublicID, "media_acquisition_approved").
			Order("occurred_at DESC, sequence DESC").First(&admission).Error
		if err != nil && err != gorm.ErrRecordNotFound {
			return 0, err
		}
		if err == nil {
			query = query.Where("created_at>=?", admission.OccurredAt)
		}
	}
	var count int64
	if err := query.Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}

// ExpediteManualTranscript raises only the durable prerequisite chain for an
// operator-requested transcript. It does not bypass dependencies or ownership;
// it merely makes the media artifact the next eligible claim and the transcript
// the next eligible Media claim after that artifact verifies.
func ExpediteManualTranscript(db *gorm.DB, tenantID string, contentID uuid.UUID, generation int64) error {
	if db == nil || strings.TrimSpace(tenantID) == "" || contentID == uuid.Nil || generation < 1 {
		return fmt.Errorf("manual transcript priority requires tenant, content item, and generation")
	}
	return db.Transaction(func(tx *gorm.DB) error {
		if MediaAcquisitionRequiresApproval(tx, tenantID, contentID, generation) {
			return fmt.Errorf("media acquisition requires approval before transcription")
		}
		var requests []models.ContentStageRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where(
			"tenant_id=? AND content_item_id=? AND processing_generation=? AND stage IN ? AND state IN ?",
			tenantID, contentID, generation,
			[]string{models.ContentStagePodsMediaArtifacts, models.ContentStagePodsTranscript},
			[]string{models.ContentStageBlocked, models.ContentStageAwaitingApproval, models.ContentStageQueued, models.ContentStageDeferred},
		).Find(&requests).Error; err != nil {
			return err
		}
		if len(requests) == 0 {
			return fmt.Errorf("durable media/transcript prerequisites are not queued")
		}
		now := time.Now().UTC()
		for _, request := range requests {
			updates := map[string]any{"priority": 100, "not_before_at": nil, "updated_at": now}
			if request.Stage == models.ContentStagePodsTranscript && request.State == models.ContentStageAwaitingApproval {
				updates["state"] = models.ContentStageQueued
				request.State = models.ContentStageQueued
			}
			if err := tx.Model(&request).Updates(updates).Error; err != nil {
				return err
			}
			request.Priority, request.NotBeforeAt, request.UpdatedAt = 100, nil, now
			if err := appendEvent(tx, request, nil, "manual_priority_raised", map[string]any{"priority": 100}); err != nil {
				return err
			}
		}
		return nil
	})
}

func claimNextForOwner(db *gorm.DB, tenantID, lane, expectedOwner, claimOwner string, allowOptional bool, allowedStages []string) (ClaimEnvelope, bool, error) {
	if db == nil || (strings.TrimSpace(tenantID) == "" && strings.TrimSpace(claimOwner) == "") || (lane != models.ContentStageLaneNews && lane != models.ContentStageLanePods) {
		return ClaimEnvelope{}, false, fmt.Errorf("tenant and registered lane are required")
	}
	if !SchemaAvailable(db) {
		return ClaimEnvelope{}, false, nil
	}
	if strings.TrimSpace(claimOwner) == "" {
		return ClaimEnvelope{}, false, fmt.Errorf("claim owner is required")
	}
	now := time.Now().UTC()
	var envelope ClaimEnvelope
	err := db.Transaction(func(tx *gorm.DB) error {
		// Required work is selected separately so optional backlog cannot consume
		// the admission/dispatch capacity needed by the feed. When CMS selects a
		// tenant, take the oldest eligible row from each tenant first, then let the
		// same loop apply scheduling/dependency/fence checks. This prevents a
		// flood in one tenant from starving every other tenant while preserving
		// oldest-first ordering within each tenant.
		loadCandidates := func(optional bool) ([]models.ContentStageRequest, error) {
			filter := claimCandidateFilter{
				tenantID: tenantID, lane: lane, expectedOwner: expectedOwner,
				allowedStages: allowedStages, optional: optional, now: now,
			}
			if strings.TrimSpace(tenantID) != "" {
				var rows []models.ContentStageRequest
				if err := lockedClaimCandidateScope(tx, filter, 64).Find(&rows).Error; err != nil {
					return nil, err
				}
				return rows, nil
			}
			// Fairness is based on the tenant's latest admitted attempt, not only
			// its oldest queued row. Otherwise one tenant with a deep historical
			// backlog remains globally oldest and wins every claim indefinitely.
			// Tenants with no prior attempt are served first, then the least
			// recently served tenant, while each tenant remains FIFO internally.
			var tenantIDs []string
			if err := rankedClaimTenantScope(tx, filter).Pluck("content_stage_requests.tenant_id", &tenantIDs).Error; err != nil {
				return nil, err
			}
			rows := make([]models.ContentStageRequest, 0, len(tenantIDs))
			for _, selectedTenant := range tenantIDs {
				var tenantRows []models.ContentStageRequest
				rowFilter := filter
				rowFilter.tenantID = selectedTenant
				if err := lockedClaimCandidateScope(tx, rowFilter, 1).Find(&tenantRows).Error; err != nil {
					return nil, err
				}
				rows = append(rows, tenantRows...)
			}
			return rows, nil
		}
		requests, err := loadCandidates(false)
		if err != nil {
			return err
		}
		if len(requests) == 0 && allowOptional {
			requests, err = loadCandidates(true)
			if err != nil {
				return err
			}
		}
		var optionalOutstanding int64
		optionalQuery := tx.Model(&models.ContentStageRequest{}).Where("lane=? AND blocking_scope=? AND state IN ?", lane, models.ContentStageBlockingOptional, []string{models.ContentStageClaimed, models.ContentStageRunning, models.ContentStageVerifying, models.ContentStageUncertain, models.ContentStageReconciling})
		if strings.TrimSpace(tenantID) != "" {
			optionalQuery = optionalQuery.Where("tenant_id=?", tenantID)
		}
		if err := optionalQuery.Count(&optionalOutstanding).Error; err != nil {
			return err
		}
		for _, request := range requests {
			scheduling, err := schedulingAllowed(tx, request.TenantID, lane)
			if err != nil {
				return err
			}
			if !scheduling {
				continue
			}
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
				count, err := effectAttemptsInCurrentBudget(tx, request)
				if err != nil {
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
				expires := now.Add(leaseDurationForStage(request.Stage))
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
	expires := now.Add(leaseDurationForStage(request.Stage))
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
	if request.Stage == models.ContentStagePodsTranscript {
		var job models.TranscriptionJob
		if err := tx.Where("tenant_id=? AND content_item_id=? AND status IN ?", request.TenantID, request.ContentItemID, []string{models.TranscriptionJobStatusQueued, models.TranscriptionJobStatusRunning}).Order("created_at DESC").First(&job).Error; err == nil {
			input["transcription_job_id"] = job.PublicID.String()
		} else if err != gorm.ErrRecordNotFound {
			return ClaimEnvelope{}, err
		}
	}
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

// Checkpoint records a bounded, fenced phase checkpoint for a running stage.
// Checkpoints are append-only lifecycle evidence: they never imply that the
// effect is complete and they cannot be written by a reclaimed or stale
// attempt. Refreshing the lease here also protects long phases whose normal
// heartbeat interval is longer than the work between two meaningful outputs.
func Checkpoint(db *gorm.DB, requestID uuid.UUID, input Correlation, phase string, proof map[string]any) error {
	phase = strings.TrimSpace(phase)
	if phase == "" || len(phase) > 64 || strings.ContainsAny(phase, "\r\n") {
		return fmt.Errorf("invalid content-stage checkpoint phase")
	}
	raw, err := json.Marshal(proof)
	if err != nil || len(raw) > 16<<10 {
		return fmt.Errorf("content-stage checkpoint proof exceeds limit")
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var base models.ContentStageRequest
		if err := tx.Where("public_id=?", requestID).First(&base).Error; err != nil {
			return err
		}
		request, attempt, err := loadCorrelated(tx, base.ContentItemID, input, []string{models.ContentStageRunning}, true)
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		expires := now.Add(leaseDurationForStage(request.Stage))
		if err := tx.Model(&request).Updates(map[string]any{
			"claim_expires_at": expires,
			"updated_at":       now,
		}).Error; err != nil {
			return err
		}
		if err := tx.Model(&attempt).Updates(map[string]any{
			"lease_expires_at": expires,
			"heartbeat_at":     now,
			"updated_at":       now,
		}).Error; err != nil {
			return err
		}
		request.ClaimExpiresAt = &expires
		attempt.LeaseExpiresAt, attempt.HeartbeatAt = expires, now
		return appendEvent(tx, request, &attempt, "checkpoint:"+phase, map[string]any{
			"phase":            phase,
			"proof":            proof,
			"lease_expires_at": expires,
		})
	})
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
		now := time.Now().UTC()
		expires := now.Add(leaseDurationForStage(request.Stage))
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
	if !SchemaAvailable(db) {
		return false, nil
	}
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
	if !SchemaAvailable(db) {
		return false, nil
	}
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
			if err := settleConditionalPodsStages(tx, item); err != nil {
				return err
			}
		}
		return promoteReadyDependents(tx, item)
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
				adopted := false
				if request.Stage == models.ContentStagePodsMediaArtifacts {
					var repair models.PipelineRepairRequest
					err := tx.Where(
						"tenant_id=? AND content_item_id=? AND stage=? AND repair_class=? AND state=?",
						request.TenantID,
						request.ContentItemID,
						models.PipelineRepairClassMediaDeliveryGeneration,
						models.PipelineStageMediaDeliveryGeneration,
						models.PipelineRepairSucceeded,
					).Order("finished_at DESC").First(&repair).Error
					if err != nil && err != gorm.ErrRecordNotFound {
						return err
					}
					if err == nil {
						adopted = true
						proof["adopted_from"] = "verified_media_delivery_repair"
						proof["pipeline_repair_id"] = repair.PublicID.String()
						proof["matching_persistence_receipt"] = false
					}
				}
				if !adopted {
					present = false
					proof["matching_persistence_receipt"] = false
				}
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
			if err := promoteReadyDependents(tx, item); err != nil {
				return err
			}
			return reduceReadiness(tx, request.TenantID, request.ContentItemID, request.ProcessingGeneration)
		}
		if (request.State == models.ContentStageUncertain || request.State == models.ContentStageVerifying) && time.Since(request.UpdatedAt) < verificationWindow {
			// Do not let one recently absent artifact monopolize VerifyOne. The
			// initial observation happens immediately; the next one is scheduled
			// at the uncertainty window without advancing UpdatedAt.
			return tx.Model(&request).UpdateColumn("not_before_at", request.UpdatedAt.Add(verificationWindow)).Error
		}
		if request.Stage == models.ContentStagePodsMediaArtifacts && (request.State == models.ContentStageUncertain || request.State == models.ContentStageReconciling) {
			var sourceCount int64
			if err := tx.Table("media_artifact_manifests AS mam").
				Joins("JOIN content_stage_attempts csa ON csa.tenant_id=mam.tenant_id AND csa.public_id=mam.attempt_id").
				Where("mam.tenant_id=? AND mam.content_item_id=? AND mam.artifact_role='source' AND mam.state IN ? AND csa.request_id=?",
					request.TenantID, request.ContentItemID, []string{"verified", "active"}, request.PublicID).
				Count(&sourceCount).Error; err != nil {
				return err
			}
			if sourceCount > 0 {
				repair, created, err := pipeline.EnsureMediaDeliveryRepair(tx, request.TenantID, request.ContentItemID)
				if err != nil {
					var failed int64
					if countErr := tx.Model(&models.PipelineRepairRequest{}).Where(
						"tenant_id=? AND content_item_id=? AND stage=? AND repair_class=? AND state=?",
						request.TenantID, request.ContentItemID, models.PipelineStageMediaDeliveryGeneration,
						models.PipelineRepairClassMediaDeliveryGeneration, models.PipelineRepairFailed,
					).Count(&failed).Error; countErr != nil {
						return countErr
					}
					if failed > 0 {
						return failRequest(tx, request, "media_delivery_reconciliation_failed", err.Error())
					}
					return err
				}
				nextCheck := time.Now().UTC().Add(5 * time.Second)
				if err := tx.Model(&request).Updates(map[string]any{"state": models.ContentStageReconciling, "not_before_at": nextCheck, "failure_class": "media_delivery_repair", "updated_at": time.Now().UTC()}).Error; err != nil {
					return err
				}
				if created {
					return appendEvent(tx, request, nil, "media_delivery_repair_queued", map[string]any{"pipeline_repair_id": repair.PublicID})
				}
				return nil
			}
		}
		attempts, err := effectAttemptsInCurrentBudget(tx, request)
		if err != nil {
			return err
		}
		if attempts < maxEffectAttempts && (request.DeadlineAt == nil || request.DeadlineAt.After(time.Now().UTC())) {
			now := time.Now().UTC()
			next := models.ContentStageQueued
			if request.Stage == models.ContentStagePodsMediaArtifacts && ResolveMediaAcquisitionMode(tx, item) == models.MediaAcquisitionManual {
				next = models.ContentStageAwaitingApproval
			}
			if err := tx.Model(&request).Updates(map[string]any{"state": next, "failure_class": "verified_absent", "not_before_at": nil, "updated_at": now}).Error; err != nil {
				return err
			}
			request.State = next
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
		ok := item.PlaybackURL != nil && strings.TrimSpace(*item.PlaybackURL) != "" && item.DurationSec != nil && *item.DurationSec >= feedcontract.PodsMinDurationSec
		return ok, map[string]any{"playback_url_present": ok, "duration_sec": item.DurationSec, "playback_type": item.PlaybackType}, nil
	case models.ContentStagePodsTranscript:
		return item.TranscriptID != nil, map[string]any{"transcript_id": item.TranscriptID, "transcript_source": item.TranscriptSource}, nil
	case models.ContentStagePodsAtomization:
		if item.DurationSec != nil && *item.DurationSec < feedcontract.PodsMinDurationSec {
			return false, map[string]any{"invalid_duration": true, "duration_sec": *item.DurationSec}, nil
		}
		if item.DurationSec != nil && *item.DurationSec >= feedcontract.PodsMinDurationSec && *item.DurationSec <= feedcontract.PodsHardMaxDuration {
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
	if err := tx.Where("tenant_id=? AND content_item_id=? AND processing_generation=? AND state IN ?", item.TenantID, item.PublicID, item.ProcessingGeneration, []string{models.ContentStageBlocked, models.ContentStageQueued, models.ContentStageDeferred}).Order("created_at ASC").Find(&requests).Error; err != nil {
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
	if err := promoteReadyDependents(tx, item); err != nil {
		return err
	}
	return reduceReadiness(tx, item.TenantID, item.PublicID, item.ProcessingGeneration)
}

func settleConditionalPodsStages(tx *gorm.DB, item models.ContentItem) error {
	if item.DurationSec == nil || *item.DurationSec < feedcontract.PodsMinDurationSec || *item.DurationSec > feedcontract.PodsHardMaxDuration {
		return nil
	}
	now := time.Now().UTC()
	var requests []models.ContentStageRequest
	if err := tx.Where("tenant_id=? AND content_item_id=? AND processing_generation=? AND stage IN ? AND state NOT IN ?", item.TenantID, item.PublicID, item.ProcessingGeneration, []string{models.ContentStagePodsTranscript, models.ContentStagePodsAtomization, models.ContentStagePodsCaptionReembedding}, []string{models.ContentStageVerified, models.ContentStageCancelled, models.ContentStageSuperseded}).Find(&requests).Error; err != nil {
		return err
	}
	// A metadata key alone is not a caption. The same bounded validity check is
	// used when the dependent stage is promoted, so an empty/stale discovery
	// hint can never suppress the manual-STT gate or leave a transcript stage
	// waiting on an artifact the Media worker cannot import.
	wantsTranscript := hasCaptionArtifact(item) || automaticSTTEnabled(tx, item.TenantID)
	for _, request := range requests {
		if request.Stage == models.ContentStagePodsTranscript && request.Priority >= 100 {
			wantsTranscript = true
			break
		}
	}
	for _, request := range requests {
		if wantsTranscript && request.Stage == models.ContentStagePodsTranscript {
			if err := tx.Model(&request).Updates(map[string]any{"blocking_scope": models.ContentStageBlockingOptional, "not_before_at": now, "updated_at": now}).Error; err != nil {
				return err
			}
			request.BlockingScope = models.ContentStageBlockingOptional
			if err := appendEvent(tx, request, nil, "made_optional", map[string]any{"reason": "raw_parent_caption_enrichment"}); err != nil {
				return err
			}
			continue
		}
		if wantsTranscript && request.Stage == models.ContentStagePodsCaptionReembedding {
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
	if !SchemaAvailable(db) {
		return nil
	}
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
	if !SchemaAvailable(tx) {
		return nil
	}
	var item models.ContentItem
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=? AND processing_generation=?", tenantID, contentID, generation).First(&item).Error; err != nil {
		return err
	}
	mode, err := CutoverMode(tx, tenantID, laneForType(item.Type))
	if err != nil || mode != models.ContentStageCutoverDurableRequired {
		return err
	}
	// Lifecycle is monotonic after publication. Optional failures, duplicate
	// observations, stale fences, and timeout recovery must not unpublish a
	// currently visible item; generation activation/rollback owns replacement.
	if item.Status == models.ContentStatusReady || item.Status == models.ContentStatusArchived {
		return nil
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

type TraceOptions struct {
	// SessionID is optional diagnostic context. It is never used to alter feed
	// serving; it only explains whether an otherwise eligible item is absent
	// from a frozen Pods session.
	SessionID string
	// PlaybackDigest is an optional client-observed rendition digest used to
	// distinguish stale playback metadata from a feed/ranking problem.
	PlaybackDigest string
}

func Trace(db *gorm.DB, tenantID string, contentID uuid.UUID) (map[string]any, error) {
	return TraceWithOptions(db, tenantID, contentID, TraceOptions{})
}

func TraceWithOptions(db *gorm.DB, tenantID string, contentID uuid.UUID, options TraceOptions) (map[string]any, error) {
	var item models.ContentItem
	if err := db.Where("tenant_id=? AND public_id=?", tenantID, contentID).First(&item).Error; err != nil {
		return nil, err
	}
	if !SchemaAvailable(db) {
		classification, reason := classifyTrace(item, nil, false, false, false, false, false, options.PlaybackDigest)
		return map[string]any{
			"item": item, "requests": []models.ContentStageRequest{}, "attempts": []models.ContentStageAttempt{},
			"receipts": []models.ContentStageReceipt{}, "events": []models.ContentStageEvent{},
			"source_lineage":     traceSourceLineage(db, item),
			"current_generation": item.ProcessingGeneration, "historical_generations": []int64{},
			"artifacts":           traceArtifacts(db, tenantID, item.PublicID),
			"lifecycle_decisions": []any{}, "feed_generation_membership": []any{},
			"eligibility": map[string]any{"status": item.Status, "feed_visibility": item.FeedVisibility},
			"ranking":     nil,
			"preference":  map[string]any{"scope": "anonymous", "applied": false, "reason": "trace has no user identity"},
			"diagnostic":  map[string]any{"classification": classification, "reason": reason, "schema_state": "legacy_schema_pending"},
		}, nil
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
	membership, generationSupported, generationActive, generationMember := traceFeedMembership(db, tenantID, item)
	frozen, frozenKnown := traceFrozenSessionMembership(db, tenantID, options.SessionID, contentID)
	boundaryObserved := traceBoundaryObserved(db, tenantID, contentID)
	classification, reason := classifyTrace(item, requests, generationSupported, generationActive, generationMember, frozen, boundaryObserved, options.PlaybackDigest)
	return map[string]any{
		"item": item, "requests": requests, "attempts": attempts, "receipts": receipts, "events": events,
		"source_lineage":             traceSourceLineage(db, item),
		"current_generation":         item.ProcessingGeneration,
		"historical_generations":     historicalGenerations(requests, item.ProcessingGeneration),
		"artifacts":                  traceArtifacts(db, tenantID, item.PublicID),
		"lifecycle_decisions":        lifecycleDecisions(events),
		"feed_generation_membership": membership,
		"eligibility":                map[string]any{"status": item.Status, "feed_visibility": item.FeedVisibility, "is_feed_unit": item.IsFeedUnit},
		"ranking":                    nil,
		"preference":                 map[string]any{"scope": "anonymous", "applied": false, "reason": "trace has no user identity"},
		"diagnostic":                 map[string]any{"classification": classification, "reason": reason, "schema_state": "available", "frozen_session_known": frozenKnown, "boundary_observed": boundaryObserved},
	}, nil
}

// traceSourceLineage returns bounded source/run identity and status fields. It
// deliberately does not return provider configuration or replay locators;
// those can contain credentials or signed URLs and are not needed to locate a
// feed failure.
func traceSourceLineage(db *gorm.DB, item models.ContentItem) map[string]any {
	lineage := map[string]any{
		"content_source_id":     item.ContentSourceID,
		"source_run_request_id": item.SourceRunRequestID,
	}
	if db == nil {
		return lineage
	}
	if item.ContentSourceID != nil && db.Migrator().HasTable(&models.ContentSource{}) {
		var source models.ContentSource
		if db.Select("public_id, tenant_id, name, type, category, is_active, next_due_at, last_claimed_at, last_attempted_at, last_provider_success_at, last_new_item_at, last_delivery_verified_at, failure_streak, intake_circuit_until").Where("public_id=? AND tenant_id=?", *item.ContentSourceID, item.TenantID).First(&source).Error == nil {
			lineage["source"] = map[string]any{
				"id": source.PublicID, "name": source.Name, "type": source.Type, "category": source.Category,
				"active": source.IsActive, "next_due_at": source.NextDueAt,
				"last_claimed_at": source.LastClaimedAt, "last_attempted_at": source.LastAttemptedAt,
				"last_provider_success_at": source.LastProviderSuccessAt, "last_new_item_at": source.LastNewItemAt,
				"last_delivery_verified_at": source.LastDeliveryVerifiedAt, "failure_streak": source.FailureStreak,
				"intake_circuit_until": source.IntakeCircuitUntil,
			}
		}
	}
	if item.SourceRunRequestID != nil && db.Migrator().HasTable(&models.SourceRunRequest{}) {
		var run models.SourceRunRequest
		if db.Select("public_id, tenant_id, content_source_id, state, lane, purpose, requested_by, aggregation_job_id, correlation_id, manifest_state, manifest_version, expected_unit_count, completed_unit_count, budget_state, evidence_state, requested_at, accepted_at, started_at, finished_at, failure_class, failure_summary").Where("id=? AND tenant_id=?", *item.SourceRunRequestID, item.TenantID).First(&run).Error == nil {
			lineage["source_run"] = map[string]any{
				"id": run.PublicID, "source_id": run.ContentSourceID, "state": run.State, "lane": run.Lane, "purpose": run.Purpose,
				"requested_by": run.RequestedBy, "aggregation_job_id": run.AggregationJobID, "correlation_id": run.CorrelationID,
				"manifest_state": run.ManifestState, "manifest_version": run.ManifestVersion,
				"expected_unit_count": run.ExpectedUnitCount, "completed_unit_count": run.CompletedUnitCount,
				"budget_state": run.BudgetState, "evidence_state": run.EvidenceState,
				"requested_at": run.RequestedAt, "accepted_at": run.AcceptedAt, "started_at": run.StartedAt,
				"finished_at": run.FinishedAt, "failure_class": run.FailureClass, "failure_summary": run.FailureSummary,
			}
		}
	}
	return lineage
}

func traceArtifacts(db *gorm.DB, tenantID string, contentID uuid.UUID) []any {
	if db == nil || !db.Migrator().HasTable(&models.MediaArtifactManifest{}) {
		return []any{}
	}
	var manifests []models.MediaArtifactManifest
	if err := db.Where("tenant_id=? AND (content_item_id=? OR parent_content_item_id=?)", tenantID, contentID, contentID).Order("created_at ASC").Limit(256).Find(&manifests).Error; err != nil {
		return []any{}
	}
	result := make([]any, 0, len(manifests))
	for _, manifest := range manifests {
		result = append(result, map[string]any{
			"id": manifest.PublicID, "content_item_id": manifest.ContentItemID, "parent_content_item_id": manifest.ParentContentItemID,
			"artifact_role": manifest.ArtifactRole, "package_manifest_id": manifest.PackageManifestID,
			"storage_tier": manifest.StorageTier, "bucket": manifest.Bucket, "object_key": manifest.ObjectKey,
			"public_url": manifest.PublicURL, "content_type": manifest.ContentType, "cache_control": manifest.CacheControl,
			"size_bytes": manifest.SizeBytes, "etag": manifest.ETag, "sha256": manifest.SHA256, "duration_ms": manifest.DurationMs,
			"creator_role": manifest.CreatorRole, "producer_event_id": manifest.ProducerEventID, "fence_token": manifest.FenceToken,
			"input_digest": manifest.InputDigest, "state": manifest.State, "recovery_class": manifest.RecoveryClass,
			"verification_evidence": manifest.VerificationEvidence, "terminal_proof": manifest.TerminalProof,
			"cleanup_eligible_at": manifest.CleanupEligibleAt, "verified_at": manifest.VerifiedAt, "deleted_at": manifest.DeletedAt,
		})
	}
	return result
}

func historicalGenerations(requests []models.ContentStageRequest, current int64) []int64 {
	seen := map[int64]bool{}
	for _, request := range requests {
		if request.ProcessingGeneration != current {
			seen[request.ProcessingGeneration] = true
		}
	}
	out := make([]int64, 0, len(seen))
	for generation := range seen {
		out = append(out, generation)
	}
	return out
}

func lifecycleDecisions(events []models.ContentStageEvent) []models.ContentStageEvent { return events }

func classifyTrace(item models.ContentItem, requests []models.ContentStageRequest, generationSupported, generationActive, generationMember, frozenSession, boundaryObserved bool, playbackDigest string) (string, string) {
	if strings.TrimSpace(playbackDigest) != "" && strings.TrimSpace(item.RenditionDigest) != "" && strings.TrimSpace(playbackDigest) != strings.TrimSpace(item.RenditionDigest) {
		return "ui_playback_metadata_stale", "the client supplied an older active rendition digest"
	}
	for _, request := range requests {
		if request.BlockingScope != models.ContentStageBlockingOptional && (request.State == models.ContentStageFailed || request.State == models.ContentStageUncertain) {
			return "stage_blocked", "a current stage is failed or effect outcome is uncertain"
		}
	}
	if item.Status == models.ContentStatusReady && (item.PlaybackURL == nil || item.Embedding == nil) {
		return "artifact_status_conflict", "published status is missing a required artifact"
	}
	if item.FeedVisibility != "visible" && (item.Type == models.ContentTypeVideo || item.Type == models.ContentTypePodcast) {
		return "not_eligible", "CMS feed visibility is not visible"
	}
	if generationSupported && generationActive && !generationMember && (item.Type == models.ContentTypeNews || item.IsFeedUnit) {
		return "not_generation_member", "the item is eligible but is absent from the active feed generation"
	}
	if frozenSession && item.Status == models.ContentStatusReady {
		return "frozen_session", "the item is not present in the requested frozen session snapshot"
	}
	if item.Status == models.ContentStatusReady && (item.Type == models.ContentTypeNews || item.IsFeedUnit || item.PlaybackURL != nil) {
		if generationMember && !boundaryObserved {
			return "ranked_outside_page", "the item is an active generation member but no feed/page boundary observed it"
		}
		return "served", "item is published or has a serving artifact"
	}
	return "not_produced", "required serving evidence is absent"
}

func traceFeedMembership(db *gorm.DB, tenantID string, item models.ContentItem) ([]any, bool, bool, bool) {
	lane, memberType := models.ContentStageLanePods, "feed_unit"
	memberID := item.PublicID
	if item.Type == models.ContentTypeNews {
		lane, memberType = models.ContentStageLaneNews, "story"
		if item.StoryID == nil {
			return []any{}, false, false, false
		}
		memberID = *item.StoryID
	}
	if !db.Migrator().HasTable(&models.FeedGenerationHead{}) || !db.Migrator().HasTable(&models.FeedGenerationMembership{}) {
		return []any{}, false, false, false
	}
	var head models.FeedGenerationHead
	if err := db.Where("tenant_id=? AND lane=?", tenantID, lane).First(&head).Error; err != nil || head.ActiveGenerationID == nil {
		return []any{}, true, false, false
	}
	var rows []models.FeedGenerationMembership
	if err := db.Where("generation_id=? AND member_type=? AND member_id=?", *head.ActiveGenerationID, memberType, memberID).Find(&rows).Error; err != nil {
		return []any{}, true, true, false
	}
	membership := make([]any, 0, len(rows))
	for _, row := range rows {
		membership = append(membership, map[string]any{"generation_id": row.GenerationID, "member_type": row.MemberType, "member_id": row.MemberID, "attached_at": row.AttachedAt})
	}
	return membership, true, true, len(rows) > 0
}

func traceFrozenSessionMembership(db *gorm.DB, tenantID, sessionID string, contentID uuid.UUID) (bool, bool) {
	if strings.TrimSpace(sessionID) == "" || !db.Migrator().HasTable(&models.ConsumerFeedSession{}) {
		return false, false
	}
	parsed, err := uuid.Parse(strings.TrimSpace(sessionID))
	if err != nil {
		return false, false
	}
	var session models.ConsumerFeedSession
	if err := db.Where("id=? AND tenant_id=? AND feed_type=?", parsed, tenantID, "pods").First(&session).Error; err != nil {
		return false, false
	}
	var snapshot []struct {
		ID uuid.UUID `json:"id"`
	}
	if err := json.Unmarshal(session.Snapshot, &snapshot); err != nil {
		return false, false
	}
	for _, entry := range snapshot {
		if entry.ID == contentID {
			return false, true
		}
	}
	return true, true
}

func traceBoundaryObserved(db *gorm.DB, tenantID string, contentID uuid.UUID) bool {
	if !db.Migrator().HasTable(&models.PodsBoundaryObservation{}) {
		return false
	}
	var count int64
	if err := db.Model(&models.PodsBoundaryObservation{}).Where("tenant_id=? AND content_item_id=? AND verdict=? AND boundary IN ?", tenantID, contentID, "present", []string{"feed_return", "page_render"}).Count(&count).Error; err != nil {
		return false
	}
	return count > 0
}
