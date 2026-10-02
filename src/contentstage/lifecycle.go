package contentstage

import (
	"content-management-system/src/podsflow"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"content-management-system/src/feedcontract"
	"content-management-system/src/feedstate"
	"content-management-system/src/lifecycle"
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
	tenantID            string
	lane                string
	expectedOwner       string
	allowedStages       []string
	optional            bool
	now                 time.Time
	admissionRestricted bool
	admissionRoots      []models.ContentItem
}

// eligibleClaimCandidateScope deliberately starts from the transaction handle
// on every call. A tenant-ranking statement uses GROUP BY and aggregate
// ordering, while a candidate statement uses FOR UPDATE SKIP LOCKED; sharing a
// chained GORM scope between those statements leaks the aggregate clauses into
// the row lock and PostgreSQL rejects the resulting query.
func eligibleClaimCandidateScope(tx *gorm.DB, filter claimCandidateFilter) *gorm.DB {
	if supported, _ := tx.Get("chapter_plan_v1"); supported != true {
		tx = tx.Where("COALESCE(content_stage_requests.workload_estimate->>'chapter_plan_id','')='' ")
	}
	scope := tx.Model(&models.ContentStageRequest{}).
		Where(
			"lane=? AND owner=? AND state IN ? AND (not_before_at IS NULL OR not_before_at<=?) AND cancellation_requested_at IS NULL",
			filter.lane,
			filter.expectedOwner,
			[]string{models.ContentStageQueued, models.ContentStageDeferred},
			filter.now,
		)
	if filter.lane == models.ContentStageLanePods {
		if filter.admissionRestricted {
			admission := tx.Where("1=0")
			for _, root := range filter.admissionRoots {
				admission = admission.Or(`EXISTS (SELECT 1 FROM content_items admitted_leaf WHERE admitted_leaf.tenant_id=content_stage_requests.tenant_id AND admitted_leaf.public_id=content_stage_requests.content_item_id AND admitted_leaf.tenant_id=? AND COALESCE(admitted_leaf.parent_content_item_id,admitted_leaf.public_id)=?)`, root.TenantID, root.PublicID)
			}
			scope = scope.Where(admission)
		}
		scope = scope.Where(`EXISTS (SELECT 1 FROM content_items leaf
			WHERE leaf.tenant_id=content_stage_requests.tenant_id AND leaf.public_id=content_stage_requests.content_item_id
			AND leaf.processing_generation=content_stage_requests.processing_generation AND leaf.status<>'ARCHIVED'
			AND (leaf.parent_content_item_id IS NULL OR EXISTS (
				SELECT 1 FROM atomization_generations g JOIN content_items root ON root.public_id=g.parent_content_item_id AND root.tenant_id=g.tenant_id
				WHERE g.tenant_id=leaf.tenant_id AND g.public_id::text=leaf.metadata->>'atomization_generation_id'
				AND g.processing_generation=root.processing_generation AND g.state<>'superseded')))`)
		scope = scope.Where(`NOT EXISTS (
			SELECT 1 FROM content_items leaf
			JOIN content_items root ON root.tenant_id=leaf.tenant_id AND root.public_id=COALESCE(leaf.parent_content_item_id,leaf.public_id)
			JOIN pods_episode_dispositions d ON d.tenant_id=root.tenant_id AND d.root_content_item_id=root.public_id AND d.processing_generation=root.processing_generation
			WHERE leaf.tenant_id=content_stage_requests.tenant_id AND leaf.public_id=content_stage_requests.content_item_id AND d.disposition<>'active'
		)`)
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
	activeTenantOrder := ""
	if filter.lane == models.ContentStageLanePods {
		activeTenantOrder = "EXISTS (SELECT 1 FROM pods_episode_execution_slot s WHERE s.singleton=true AND s.tenant_id=content_stage_requests.tenant_id) DESC, "
	}
	return eligibleClaimCandidateScope(tx, filter).
		Select("content_stage_requests.tenant_id").
		Group("content_stage_requests.tenant_id").
		Clauses(clause.OrderBy{Expression: clause.Expr{
			SQL:  activeTenantOrder + "(SELECT MAX(csa.created_at) FROM content_stage_attempts csa WHERE csa.tenant_id=content_stage_requests.tenant_id AND csa.lane=? AND csa.owner=?) ASC NULLS FIRST, MIN(content_stage_requests.created_at) ASC, content_stage_requests.tenant_id ASC",
			Vars: []any{filter.lane, filter.expectedOwner},
		}}).
		Limit(64)
}

func claimCandidateScope(tx *gorm.DB, filter claimCandidateFilter, limit int) *gorm.DB {
	scope := eligibleClaimCandidateScope(tx, filter)
	if filter.lane == models.ContentStageLanePods {
		// Rank the slot owner's family before limiting candidates. Otherwise an
		// older waiting root can hide the active family's newly created children.
		scope = scope.Order(`EXISTS (SELECT 1 FROM pods_episode_execution_slot s
			JOIN content_items leaf ON leaf.tenant_id=s.tenant_id
			AND COALESCE(leaf.parent_content_item_id,leaf.public_id)=s.root_content_item_id
			WHERE s.singleton=true AND leaf.tenant_id=content_stage_requests.tenant_id
			AND leaf.public_id=content_stage_requests.content_item_id) DESC`)
	}
	return scope.
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
	if request.Stage == models.ContentStagePodsMediaArtifacts || request.Stage == models.ContentStagePodsAtomization {
		eventTypes := []string{"media_acquisition_approved"}
		if request.Stage == models.ContentStagePodsAtomization {
			// A partial chapter effect is a new execution boundary after CMS has
			// retired its exact manifests. Count only attempts started after the
			// latest operator approval or reconciliation boundary; otherwise a
			// historical attempt can exhaust the fresh retry budget forever.
			eventTypes = []string{"manual_atomization_retry_approved", "chapter_effects_reconciled", "atomization_finalization_requeued"}
		}
		var admission models.ContentStageEvent
		err := tx.Where("tenant_id=? AND request_id=? AND event_type IN ?", request.TenantID, request.PublicID, eventTypes).
			Order("occurred_at DESC, sequence DESC").First(&admission).Error
		if err != nil && err != gorm.ErrRecordNotFound {
			return 0, err
		}
		if err == nil {
			query = query.Where("effect_started_at>=?", admission.OccurredAt)
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
		var item models.ContentItem
		if err := tx.Where("tenant_id=? AND public_id=? AND processing_generation=?", tenantID, contentID, generation).First(&item).Error; err != nil {
			return err
		}
		sourceID := ""
		if item.ContentSourceID != nil {
			sourceID = item.ContentSourceID.String()
		}
		scope := lifecycle.Scope{TenantID: tenantID, Lane: models.ContentStageLanePods, SourceID: sourceID, ItemID: contentID.String()}
		if err := lifecycle.Check(tx, scope, lifecycle.PhaseSourceDispatch); err != nil {
			return err
		}
		if err := lifecycle.Check(tx, scope, lifecycle.PhaseContentWrite); err != nil {
			return err
		}
		if MediaAcquisitionRequiresApproval(tx, tenantID, contentID, generation) {
			return fmt.Errorf("media acquisition requires approval before transcription")
		}
		var requests []models.ContentStageRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where(
			"tenant_id=? AND content_item_id=? AND processing_generation=? AND stage IN ? AND state IN ?",
			tenantID, contentID, generation,
			[]string{models.ContentStagePodsMediaArtifacts, models.ContentStagePodsTranscript},
			[]string{models.ContentStageBlocked, models.ContentStageAwaitingApproval, models.ContentStageQueued, models.ContentStageDeferred, models.ContentStageVerified},
		).Find(&requests).Error; err != nil {
			return err
		}
		if len(requests) == 0 {
			return fmt.Errorf("durable media/transcript prerequisites are not queued")
		}
		if err := CheckJourneyGeneration(tx, tenantID, contentID); err != nil {
			return err
		}
		now := time.Now().UTC()
		for _, request := range requests {
			// Media is an immutable prerequisite. A verified media request must
			// never be reopened by a transcript click; the transcript request may
			// legitimately be verified for a short raw item (where generated STT is
			// optional) and needs a fresh queued effect for an explicit operator
			// request.
			if request.Stage == models.ContentStagePodsMediaArtifacts && request.State == models.ContentStageVerified {
				continue
			}
			updates := map[string]any{"priority": 100, "not_before_at": nil, "updated_at": now}
			if request.Stage == models.ContentStagePodsTranscript && (request.State == models.ContentStageAwaitingApproval || request.State == models.ContentStageVerified) {
				updates["state"] = models.ContentStageQueued
				updates["verified_at"] = nil
				updates["finished_at"] = nil
				updates["failure_class"] = ""
				updates["terminal_proof"] = jsonValue(map[string]any{})
				updates["claim_owner"] = ""
				updates["claim_token"] = nil
				updates["claim_expires_at"] = nil
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
		return podsflow.Resume(tx, item)
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
			if lane == models.ContentStageLanePods {
				filter.admissionRestricted = true
				winner, err := podsflow.NextCandidate(tx)
				if err != nil {
					return nil, err
				}
				if winner != nil {
					filter.admissionRoots = append(filter.admissionRoots, *winner)
				}
				var slot podsflow.Slot
				if err := tx.Where("singleton=true").First(&slot).Error; err != nil {
					return nil, err
				}
				if slot.TenantID != nil && slot.RootContentItemID != nil {
					var active models.ContentItem
					if err := tx.Where("tenant_id=? AND public_id=?", *slot.TenantID, *slot.RootContentItemID).First(&active).Error; err != nil {
						return nil, err
					}
					state, _, err := podsflow.Observe(tx, active)
					if err != nil {
						return nil, err
					}
					if state == "active" || state == "reconciling" {
						filter.admissionRoots = []models.ContentItem{active}
					}
				}
			}
			if strings.TrimSpace(tenantID) != "" {
				var rows []models.ContentStageRequest
				if err := claimCandidateScope(tx, filter, 64).Find(&rows).Error; err != nil {
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
				if err := claimCandidateScope(tx, rowFilter, 1).Find(&tenantRows).Error; err != nil {
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
			var observedItem models.ContentItem
			if err := tx.Where("tenant_id=? AND public_id=? AND processing_generation=? AND status<>?", request.TenantID, request.ContentItemID, request.ProcessingGeneration, models.ContentStatusArchived).First(&observedItem).Error; err != nil {
				continue
			}
			lifecycleSourceID := ""
			if observedItem.ContentSourceID != nil {
				lifecycleSourceID = observedItem.ContentSourceID.String()
			}
			scope := lifecycle.Scope{
				TenantID: observedItem.TenantID,
				Lane:     request.Lane,
				SourceID: lifecycleSourceID,
				ItemID:   observedItem.PublicID.String(),
			}
			if err := lifecycle.Check(tx, scope, lifecycle.PhaseSourceDispatch); err != nil {
				if lifecycle.IsConflict(err) || lifecycle.IsIntakePaused(err) {
					continue
				}
				return err
			}
			if err := lifecycle.Check(tx, scope, lifecycle.PhaseContentWrite); err != nil {
				if lifecycle.IsConflict(err) {
					continue
				}
				return err
			}
			// Candidate discovery is deliberately unlocked so the owner first takes
			// the shared lifecycle boundary. Lock the content row and then the stage
			// request in one stable order, revalidating both observations before the
			// queue claim. This avoids holding a stage-request row lock while waiting
			// for a campaign's exclusive resource lock.
			var item models.ContentItem
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=? AND processing_generation=? AND status<>?", request.TenantID, request.ContentItemID, request.ProcessingGeneration, models.ContentStatusArchived).First(&item).Error; err != nil {
				continue
			}
			if !item.UpdatedAt.Equal(observedItem.UpdatedAt) || item.Type != observedItem.Type ||
				(item.ContentSourceID == nil) != (observedItem.ContentSourceID == nil) ||
				(item.ContentSourceID != nil && *item.ContentSourceID != *observedItem.ContentSourceID) {
				continue
			}
			var lockedRequest models.ContentStageRequest
			lockErr := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Where(
				"tenant_id=? AND public_id=? AND lane=? AND stage=? AND processing_generation=? AND input_fingerprint=? AND state IN ? AND (not_before_at IS NULL OR not_before_at<=?) AND cancellation_requested_at IS NULL",
				request.TenantID, request.PublicID, request.Lane, request.Stage, request.ProcessingGeneration,
				request.InputFingerprint, []string{models.ContentStageQueued, models.ContentStageDeferred}, now,
			).First(&lockedRequest).Error
			if lockErr == gorm.ErrRecordNotFound {
				continue
			}
			if lockErr != nil {
				return lockErr
			}
			request = lockedRequest
			if stageFingerprint(item, descriptors[request.Stage]) != request.InputFingerprint {
				if err := supersede(tx, request, "input_fingerprint_changed"); err != nil {
					return err
				}
				continue
			}
			ready, err = dependenciesVerified(tx, request)
			if err != nil {
				return err
			}
			if !ready {
				continue
			}
			if request.Lane == models.ContentStageLanePods {
				admitted, err := podsflow.Acquire(tx, item)
				if err != nil {
					return err
				}
				if !admitted {
					continue
				}
			}
			attempt, reclaimed, err := reclaimUnstartedAttempt(tx, request, claimOwner, now)
			if err != nil {
				return err
			}
			if !reclaimed {
				var total int64
				if err := tx.Model(&models.ContentStageAttempt{}).Where("tenant_id=? AND request_id=?", request.TenantID, request.PublicID).Count(&total).Error; err != nil {
					return err
				}
				count, err := effectAttemptsInCurrentBudget(tx, request)
				if err != nil {
					return err
				}
				if total == 0 || count == 0 {
					// Start only after slot admission, including a fresh explicitly
					// approved budget. Retries with effects retain their deadline.
					budget := textElapsedBudget
					if request.Stage == models.ContentStagePodsMediaArtifacts {
						budget = mediaElapsedBudget
					}
					if isTrustedLongForm(item) {
						budget = 24 * time.Hour
					}
					deadline := now.Add(budget)
					request.DeadlineAt = &deadline
					if err := tx.Model(&request).UpdateColumn("deadline_at", deadline).Error; err != nil {
						return err
					}
				}
				if count >= maxEffectAttempts || (request.DeadlineAt != nil && !request.DeadlineAt.After(now)) {
					if err := failRequest(tx, request, "execution_budget_exhausted", "verified effect remains absent"); err != nil {
						return err
					}
					continue
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
		requestUpdate := tx.Model(&models.ContentStageRequest{}).Where("tenant_id=? AND public_id=? AND state=? AND claim_token=? AND claim_expires_at>?", request.TenantID, request.PublicID, models.ContentStageRunning, request.ClaimToken, now).Updates(map[string]any{
			"claim_expires_at": expires,
			"updated_at":       now,
		})
		if requestUpdate.Error != nil {
			return requestUpdate.Error
		}
		if requestUpdate.RowsAffected != 1 {
			return fmt.Errorf("content-stage checkpoint authority changed")
		}
		attemptUpdate := tx.Model(&models.ContentStageAttempt{}).Where("tenant_id=? AND public_id=? AND request_id=? AND fence_token=? AND claim_token=? AND state=? AND lease_expires_at>?", attempt.TenantID, attempt.PublicID, attempt.RequestID, attempt.FenceToken, attempt.ClaimToken, models.ContentStageRunning, now).Updates(map[string]any{
			"lease_expires_at": expires,
			"heartbeat_at":     now,
			"updated_at":       now,
		})
		if attemptUpdate.Error != nil {
			return attemptUpdate.Error
		}
		if attemptUpdate.RowsAffected != 1 {
			return fmt.Errorf("content-stage attempt checkpoint authority changed")
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
		requestUpdate := tx.Model(&models.ContentStageRequest{}).Where("tenant_id=? AND public_id=? AND state=? AND claim_token=? AND claim_expires_at>?", request.TenantID, request.PublicID, request.State, request.ClaimToken, now).Updates(updates)
		if requestUpdate.Error != nil {
			return requestUpdate.Error
		}
		if requestUpdate.RowsAffected != 1 {
			return fmt.Errorf("content-stage claim authority changed")
		}
		attemptUpdate := tx.Model(&models.ContentStageAttempt{}).Where("tenant_id=? AND public_id=? AND request_id=? AND fence_token=? AND claim_token=? AND state=? AND lease_expires_at>?", attempt.TenantID, attempt.PublicID, attempt.RequestID, attempt.FenceToken, attempt.ClaimToken, attempt.State, now).Updates(attemptUpdates)
		if attemptUpdate.Error != nil {
			return attemptUpdate.Error
		}
		if attemptUpdate.RowsAffected != 1 {
			return fmt.Errorf("content-stage attempt authority changed")
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
		requestUpdate := tx.Model(&models.ContentStageRequest{}).Where("tenant_id=? AND public_id=? AND state=? AND claim_token=? AND claim_expires_at>?", request.TenantID, request.PublicID, models.ContentStageRunning, request.ClaimToken, now).Updates(map[string]any{
			"state": models.ContentStageVerified, "verified_at": now, "finished_at": now,
			"terminal_proof": terminalProof, "claim_token": nil, "claim_expires_at": nil, "updated_at": now,
		})
		if requestUpdate.Error != nil {
			return requestUpdate.Error
		}
		if requestUpdate.RowsAffected != 1 {
			return fmt.Errorf("content-stage not-required authority changed")
		}
		attemptUpdate := tx.Model(&models.ContentStageAttempt{}).Where("tenant_id=? AND public_id=? AND request_id=? AND fence_token=? AND claim_token=? AND state=? AND lease_expires_at>?", attempt.TenantID, attempt.PublicID, attempt.RequestID, attempt.FenceToken, attempt.ClaimToken, models.ContentStageRunning, now).Updates(map[string]any{"state": models.ContentStageVerified, "finished_at": now, "updated_at": now})
		if attemptUpdate.Error != nil {
			return attemptUpdate.Error
		}
		if attemptUpdate.RowsAffected != 1 {
			return fmt.Errorf("content-stage attempt not-required authority changed")
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

const maxContentStageFailureSummaryBytes = 512

// boundedFailureSummary keeps database writes inside the current schema
// contract while retaining useful diagnostics from noisy subprocess output.
// Strings are truncated by bytes because PostgreSQL's varchar limit is a byte
// limit; avoid splitting a UTF-8 sequence at the boundary.
func boundedFailureSummary(summary string) string {
	summary = strings.TrimSpace(summary)
	if len(summary) <= maxContentStageFailureSummaryBytes {
		return summary
	}
	const marker = "\n...[truncated]...\n"
	keep := maxContentStageFailureSummaryBytes - len(marker)
	if keep <= 0 {
		return summary[:maxContentStageFailureSummaryBytes]
	}
	// Keep more of the tail, where ffmpeg normally prints the exit reason.
	head := keep / 3
	tail := keep - head
	headText := summary[:head]
	tailText := summary[len(summary)-tail:]
	for len(headText) > 0 && !utf8.ValidString(headText) {
		_, size := utf8.DecodeLastRuneInString(headText)
		if size <= 0 || size > len(headText) {
			headText = headText[:len(headText)-1]
		} else {
			headText = headText[:len(headText)-1]
		}
	}
	for len(tailText) > 0 && !utf8.ValidString(tailText) {
		_, size := utf8.DecodeRuneInString(tailText)
		if size <= 0 || size > len(tailText) {
			tailText = tailText[1:]
		} else {
			tailText = tailText[1:]
		}
	}
	return headText + marker + tailText
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
		// Failure summaries are operator diagnostics, not an unbounded log
		// sink.  FFmpeg and other subprocesses can return many kilobytes of
		// progress output; persisting that directly into the legacy varchar(512)
		// column causes the whole terminal transition to roll back, leaving the
		// durable request stuck in `running` forever.  Keep both the beginning
		// (command/error context) and the tail (actual exit reason) within the
		// database contract.
		attemptUpdates := map[string]any{"state": state, "failure_class": failureClass, "failure_summary": boundedFailureSummary(summary), "finished_at": now, "updated_at": now}
		// A capacity defer proves that the downstream effect never started. Begin
		// records the worker-side execution boundary before making the dependency
		// call, so clear that provisional marker when the dependency explicitly
		// rejects admission. This keeps capacity pressure out of the bounded
		// post-effect retry budget.
		if state == models.ContentStageDeferred && failureClass == "capacity_deferred" {
			attemptUpdates["effect_started_at"] = nil
		}
		requestUpdate := tx.Model(&models.ContentStageRequest{}).Where("tenant_id=? AND public_id=? AND state IN ? AND claim_token=? AND claim_expires_at>?", request.TenantID, request.PublicID, []string{models.ContentStageClaimed, models.ContentStageRunning}, request.ClaimToken, now).Updates(requestUpdates)
		if requestUpdate.Error != nil {
			return requestUpdate.Error
		}
		if requestUpdate.RowsAffected != 1 {
			return fmt.Errorf("content-stage terminal authority changed")
		}
		attemptUpdate := tx.Model(&models.ContentStageAttempt{}).Where("tenant_id=? AND public_id=? AND request_id=? AND fence_token=? AND claim_token=? AND state IN ? AND lease_expires_at>?", attempt.TenantID, attempt.PublicID, attempt.RequestID, attempt.FenceToken, attempt.ClaimToken, []string{models.ContentStageClaimed, models.ContentStageRunning}, now).Updates(attemptUpdates)
		if attemptUpdate.Error != nil {
			return attemptUpdate.Error
		}
		if attemptUpdate.RowsAffected != 1 {
			return fmt.Errorf("content-stage attempt terminal authority changed")
		}
		request.State, request.FailureClass, request.ClaimToken, request.ClaimExpiresAt = state, failureClass, nil, nil
		attempt.State, attempt.FailureClass, attempt.FailureSummary, attempt.FinishedAt = state, failureClass, boundedFailureSummary(summary), &now
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
	if tx == nil {
		return models.ContentStageRequest{}, models.ContentStageAttempt{}, fmt.Errorf("content-stage writeback requires a database handle")
	}
	var request models.ContentStageRequest
	var attempt models.ContentStageAttempt
	err := tx.Transaction(func(scope *gorm.DB) error {
		var err error
		request, attempt, err = authorizeWritebackInTransaction(scope, contentID, input, expectedStage)
		return err
	})
	return request, attempt, err
}

func authorizeWritebackInTransaction(tx *gorm.DB, contentID uuid.UUID, input Correlation, expectedStage string) (models.ContentStageRequest, models.ContentStageAttempt, error) {
	requestID, _, _, _, _, err := parseCorrelation(input)
	if err != nil || strings.TrimSpace(input.InputFingerprint) == "" {
		return models.ContentStageRequest{}, models.ContentStageAttempt{}, fmt.Errorf("invalid content-stage correlation")
	}
	// Resolve the resource and acquire its shared lifecycle boundary before
	// locking the stage request/attempt rows. The final persistence helper checks
	// the same boundary again inside the write transaction, closing the gap
	// between worker authorization and the actual content mutation.
	var observedRequest models.ContentStageRequest
	if err := tx.Where("public_id=? AND content_item_id=? AND input_fingerprint=?", requestID, contentID, input.InputFingerprint).First(&observedRequest).Error; err != nil {
		return models.ContentStageRequest{}, models.ContentStageAttempt{}, fmt.Errorf("content-stage request is stale: %w", err)
	}
	var observedItem models.ContentItem
	if err := tx.Where("tenant_id=? AND public_id=? AND processing_generation=?", observedRequest.TenantID, contentID, observedRequest.ProcessingGeneration).First(&observedItem).Error; err != nil {
		return observedRequest, models.ContentStageAttempt{}, fmt.Errorf("content-stage target generation changed: %w", err)
	}
	sourceID := ""
	if observedItem.ContentSourceID != nil {
		sourceID = observedItem.ContentSourceID.String()
	}
	if err := lifecycle.Check(tx, lifecycle.Scope{
		TenantID: observedItem.TenantID,
		Lane:     laneForType(observedItem.Type),
		SourceID: sourceID,
		ItemID:   observedItem.PublicID.String(),
	}, lifecycle.PhaseContentWrite); err != nil {
		return observedRequest, models.ContentStageAttempt{}, err
	}
	request, attempt, err := loadCorrelated(tx, contentID, input, []string{models.ContentStageRunning}, true)
	if err != nil {
		return request, attempt, err
	}
	if request.Stage != expectedStage || request.ProcessingGeneration <= 0 || request.TenantID != observedRequest.TenantID || request.ProcessingGeneration != observedRequest.ProcessingGeneration {
		return request, attempt, fmt.Errorf("content-stage writeback stage mismatch")
	}
	var item models.ContentItem
	if err := tx.Where("tenant_id=? AND public_id=? AND processing_generation=?", request.TenantID, contentID, request.ProcessingGeneration).First(&item).Error; err != nil {
		return request, attempt, fmt.Errorf("content-stage target generation changed: %w", err)
	}
	return request, attempt, nil
}

func RecordPersistence(tx *gorm.DB, request models.ContentStageRequest, attempt models.ContentStageAttempt, input Correlation, owner, artifactDigest string, payload map[string]any) error {
	if tx == nil {
		return fmt.Errorf("content-stage persistence requires a database handle")
	}
	return tx.Transaction(func(scope *gorm.DB) error {
		return recordPersistenceInTransaction(scope, request, attempt, input, owner, artifactDigest, payload)
	})
}

func recordPersistenceInTransaction(tx *gorm.DB, request models.ContentStageRequest, attempt models.ContentStageAttempt, input Correlation, owner, artifactDigest string, payload map[string]any) error {
	_, _, _, _, producerEventID, err := parseCorrelation(input)
	if err != nil || producerEventID == uuid.Nil {
		return fmt.Errorf("producer event id is required")
	}
	// This check is intentionally repeated in the transaction that commits both
	// the content mutation and its immutable receipt. A reset claim acquired
	// after AuthorizeWriteback must roll back the content mutation rather than
	// allowing a late worker to publish into the selected identity.
	var item models.ContentItem
	if err := tx.Where("tenant_id=? AND public_id=? AND processing_generation=?", request.TenantID, request.ContentItemID, request.ProcessingGeneration).First(&item).Error; err != nil {
		return fmt.Errorf("content-stage persistence target changed: %w", err)
	}
	sourceID := ""
	if item.ContentSourceID != nil {
		sourceID = item.ContentSourceID.String()
	}
	if err := lifecycle.Check(tx, lifecycle.Scope{
		TenantID: item.TenantID,
		Lane:     laneForType(item.Type),
		SourceID: sourceID,
		ItemID:   item.PublicID.String(),
	}, lifecycle.PhaseContentWrite); err != nil {
		return err
	}
	// Receipts deliberately keep their artifact digest compact (the schema is
	// varchar(64)).  A few producers historically passed a human-readable list
	// of child UUIDs here instead of a digest.  That value is useful in the
	// payload, but inserting it into the bounded digest column aborts the whole
	// finalization transaction after all external effects have completed.  Keep
	// the stable value when it already fits and hash oversized producer values
	// into the canonical 64-character representation.
	artifactDigest = normalizeArtifactDigest(artifactDigest)
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
		// A lost response is a normal retry path. Re-read the immutable receipt
		// and prove that the caller is replaying the same effect, rather than
		// treating any producer-event collision as success.
		var existing models.ContentStageReceipt
		if err := tx.Where("tenant_id=? AND owner=? AND producer_event_id=?", request.TenantID, owner, producerEventID).First(&existing).Error; err != nil {
			return err
		}
		if existing.RequestID != request.PublicID || existing.AttemptID != attempt.PublicID ||
			existing.ContentItemID != request.ContentItemID || existing.ProcessingGeneration != request.ProcessingGeneration ||
			existing.Lane != request.Lane || existing.Stage != request.Stage || existing.FenceToken != attempt.FenceToken ||
			existing.InputFingerprint != request.InputFingerprint || existing.Outcome != "persisted" ||
			existing.PayloadDigest != receipt.PayloadDigest || existing.ArtifactDigest != artifactDigest {
			return fmt.Errorf("producer event receipt conflicts with current stage authority")
		}
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

func normalizeArtifactDigest(value string) string {
	if len(value) <= 64 {
		return value
	}
	return digest("artifact", value)
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
				if request.Stage == models.ContentStagePodsMediaArtifacts && isTrustedLongForm(item) {
					// Adoption requires all three exact manifests above, plus source
					// provenance from this request's fenced attempt.
					var count int64
					if err := tx.Table("media_artifact_manifests m").Joins("JOIN content_stage_attempts a ON a.public_id=m.attempt_id AND a.tenant_id=m.tenant_id").Where("m.tenant_id=? AND m.public_id=? AND a.request_id=?", request.TenantID, proof["media_artifact_manifest_id"], request.PublicID).Count(&count).Error; err != nil {
						return err
					}
					adopted = count == 1
					if adopted {
						proof["adopted_from"] = "verified_long_parent_manifests"
					}
				}
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
		// Atomization units register immutable upload intent before touching
		// storage. Its reconciliation pass can therefore prove the no-upload
		// case immediately; applying the generic ten-minute uncertainty window
		// here left a failed FFmpeg cut looking stuck even though no external
		// effect existed. Other stages retain the conservative observation
		// window because their providers do not expose the same intent ledger.
		if request.Stage != models.ContentStagePodsAtomization &&
			(request.State == models.ContentStageUncertain || request.State == models.ContentStageVerifying) &&
			time.Since(request.UpdatedAt) < verificationWindow {
			// Do not let one recently absent artifact monopolize VerifyOne. The
			// initial observation happens immediately; the next one is scheduled
			// at the uncertainty window without advancing UpdatedAt.
			return tx.Model(&request).UpdateColumn("not_before_at", request.UpdatedAt.Add(verificationWindow)).Error
		}
		if request.Stage == models.ContentStagePodsMediaArtifacts && !isTrustedLongForm(item) && (request.State == models.ContentStageUncertain || request.State == models.ContentStageReconciling) {
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
		if request.Stage == models.ContentStagePodsAtomization {
			if err := podsflow.ReconcileAbsentUnits(tx, item); err != nil {
				return err
			}
			unresolved, err := podsflow.HasUnresolvedChapterEffects(tx, item)
			if err != nil {
				return err
			}
			if unresolved {
				return tx.Model(&request).Updates(map[string]any{"state": models.ContentStageReconciling, "not_before_at": time.Now().UTC().Add(30 * time.Second), "failure_class": "chapter_effects_reconciling"}).Error
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
		if isTrustedLongForm(item) {
			return preparedLongParent(tx, item)
		}
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
		return podsflow.GenerationArtifacts(tx, item)
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

// Long parents are custody artifacts, not public playback units. Verify the
// exact persisted source, analysis audio and thumbnail instead of requiring a
// public URL that the long-parent delivery policy deliberately omits.
func preparedLongParent(tx *gorm.DB, item models.ContentItem) (bool, map[string]any, error) {
	proof := map[string]any{"long_parent": true, "duration_sec": item.DurationSec}
	var metadata map[string]any
	if json.Unmarshal(item.Metadata, &metadata) != nil {
		return false, proof, nil
	}
	duration, _ := metadata["duration_verification"].(map[string]any)
	measured, _ := duration["duration_sec"].(float64)
	if duration["source"] != "ffprobe" || item.DurationSec == nil || measured != float64(*item.DurationSec) {
		return false, proof, nil
	}
	for _, artifact := range []struct{ key, role string }{
		{"media_artifact_manifest_id", "source"},
		{"analysis_audio_manifest_id", "analysis_audio"},
		{"thumbnail_manifest_id", "thumbnail"},
	} {
		raw, _ := metadata[artifact.key].(string)
		id, err := uuid.Parse(raw)
		if err != nil {
			return false, proof, nil
		}
		var manifest models.MediaArtifactManifest
		err = tx.Where("tenant_id=? AND content_item_id=? AND public_id=? AND artifact_role=? AND state IN ? AND size_bytes>0 AND deleted_at IS NULL", item.TenantID, item.PublicID, id, artifact.role, []string{"verified", "active"}).First(&manifest).Error
		if err == gorm.ErrRecordNotFound {
			return false, proof, nil
		}
		if err != nil {
			return false, proof, err
		}
		if manifest.PublicURL == "" || (artifact.role == "analysis_audio" && !strings.HasPrefix(manifest.ContentType, "audio/")) {
			return false, proof, nil
		}
		proof[artifact.key] = id.String()
	}
	return true, proof, nil
}

// AdoptPresentStages records artifacts created atomically by another
// CMS-governed stage (notably atomized child renditions/transcripts) before a
// child manifest is dispatched. It never invents success: every adopted stage
// passes the same artifact verifier used after worker writeback.
func AdoptPresentStages(tx *gorm.DB, item models.ContentItem, provenance string) error {
	var requests []models.ContentStageRequest
	// A child created by atomization already owns verified playback artifacts.
	// Its source may still resolve to the tenant's manual acquisition mode, so
	// EnsureManifest can legitimately create the media request as
	// awaiting_approval. Include that state here: adopting a CMS-verified child
	// rendition is not a new download and must not ask the operator to approve
	// the same bytes a second time.
	if err := tx.Where("tenant_id=? AND content_item_id=? AND processing_generation=? AND state IN ?", item.TenantID, item.PublicID, item.ProcessingGeneration, []string{models.ContentStageAwaitingApproval, models.ContentStageBlocked, models.ContentStageQueued, models.ContentStageDeferred}).Order("created_at ASC").Find(&requests).Error; err != nil {
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
	// Only these stages are conditional for a short raw media item. Text
	// embedding remains required for every Pods feed unit; the old broad
	// ``stage NOT IN`` query accidentally verified it (and optional image/LLM
	// work) merely because media playback was present.
	if err := tx.Where("tenant_id=? AND content_item_id=? AND processing_generation=? AND stage IN ? AND state NOT IN ?", item.TenantID, item.PublicID, item.ProcessingGeneration, []string{models.ContentStagePodsTranscript, models.ContentStagePodsAtomization, models.ContentStagePodsCaptionReembedding}, []string{models.ContentStageVerified, models.ContentStageCancelled, models.ContentStageSuperseded}).Find(&requests).Error; err != nil {
		return err
	}
	// A metadata key alone is not a caption. The same bounded validity check is
	// used when the dependent stage is promoted, so an empty/stale discovery
	// hint can never suppress the manual-STT gate or leave a transcript stage
	// waiting on an artifact the Media worker cannot import.
	// A caption_state value or metadata key can be stale after a failed import.
	// Verify the linked provider transcript/artifact before treating transcript
	// work as optional; otherwise caption-less media is incorrectly parked as
	// "no STT needed".
	wantsTranscript := HasProviderCaption(tx, item) || automaticSTTEnabled(tx, item.TenantID)
	for _, request := range requests {
		if request.Stage == models.ContentStagePodsTranscript && request.Priority >= 100 {
			wantsTranscript = true
			break
		}
	}
	for _, request := range requests {
		if request.Stage == models.ContentStagePodsTranscript {
			if wantsTranscript {
				if err := tx.Model(&request).Updates(map[string]any{"blocking_scope": models.ContentStageBlockingOptional, "not_before_at": now, "updated_at": now}).Error; err != nil {
					return err
				}
				request.BlockingScope = models.ContentStageBlockingOptional
				if err := appendEvent(tx, request, nil, "made_optional", map[string]any{"reason": "raw_parent_caption_enrichment"}); err != nil {
					return err
				}
				continue
			}
			// A short raw item does not need generated STT for feed delivery.
			// Leave the request's policy-derived approval state alone when an
			// operator explicitly requested a transcript; otherwise settle it as
			// not required below.
		}
		if request.Stage == models.ContentStagePodsCaptionReembedding && wantsTranscript {
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
				// A delivery that never reached the effect boundary is safe to
				// retry, but it must not be immediately claimed again.  Older
				// workers left not_before_at unchanged, so a slow/downstream
				// worker hot-loop reclaimed the same request every few seconds and
				// monopolised the Pods slot.  Use bounded exponential backoff while
				// preserving the durable attempt for reconciliation.
				retryAt := now.Add(preEffectLeaseBackoff(attempt.AttemptNumber))
				if err := tx.Model(&request).Updates(map[string]any{"state": models.ContentStageQueued, "claim_token": nil, "claim_expires_at": nil, "not_before_at": retryAt, "failure_class": "lease_expired_before_effect", "updated_at": now}).Error; err != nil {
					return err
				}
				request.State = models.ContentStageQueued
				request.NotBeforeAt = &retryAt
				if err := appendEvent(tx, request, &attempt, "lease_expired_before_effect", map[string]any{"reclaimable": true, "retry_at": retryAt, "retry_after_sec": int(preEffectLeaseBackoff(attempt.AttemptNumber).Seconds())}); err != nil {
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

// preEffectLeaseBackoff spaces out deliveries that expired before the owner
// could begin an effect.  It is deliberately bounded: a worker outage should
// not create a permanent schedule delay, while repeated immediate reclaims
// must not starve the rest of a tenant or the global Pods execution slot.
func preEffectLeaseBackoff(attemptNumber int) time.Duration {
	if attemptNumber < 1 {
		attemptNumber = 1
	}
	delay := 30 * time.Second
	for i := 1; i < attemptNumber && delay < 5*time.Minute; i++ {
		delay *= 2
	}
	if delay > 5*time.Minute {
		return 5 * time.Minute
	}
	return delay
}

const (
	// A failed request with no effect is safe to repair only after it has been
	// quiescent for a short period.  This prevents a transient upstream error
	// from being immediately requeued in a tight loop while still healing the
	// historical rows created before the durable admission rules were enabled.
	preEffectFailureQuiescence = 5 * time.Minute
	preEffectFailureBatch      = 64
)

// ReconcilePreEffectFailures repairs legacy terminal failures that never
// crossed the effect boundary.  Older workers could exhaust their claim
// budget before starting an effect and leave a blocking request permanently in
// failed, even though the item is merely waiting for media admission or a
// predecessor.  Such rows are not evidence of a failed side effect and may be
// safely returned to the state dictated by the current dependency and
// acquisition policy.  A per-request event makes this repair one-shot so a
// genuinely failing new attempt is not hidden by an automatic retry loop.
func ReconcilePreEffectFailures(db *gorm.DB) error {
	if !SchemaAvailable(db) {
		return nil
	}
	now := time.Now().UTC()
	cutoff := now.Add(-preEffectFailureQuiescence)
	return db.Transaction(func(tx *gorm.DB) error {
		var requests []models.ContentStageRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where(`state=? AND blocking_scope<>? AND failure_class IN ? AND finished_at IS NOT NULL AND finished_at<=?
				AND NOT EXISTS (
					SELECT 1 FROM content_stage_attempts a
					WHERE a.tenant_id=content_stage_requests.tenant_id
					  AND a.request_id=content_stage_requests.public_id
					  AND a.effect_started_at IS NOT NULL
				)
				AND NOT EXISTS (
					SELECT 1 FROM content_stage_events e
					WHERE e.tenant_id=content_stage_requests.tenant_id
					  AND e.request_id=content_stage_requests.public_id
					  AND e.event_type=?
				)`,
				models.ContentStageFailed, models.ContentStageBlockingOptional,
				[]string{"execution_budget_exhausted", "deadline_exceeded", "cms_execution_interrupted"}, cutoff,
				"pre_effect_failure_reclassified").
			Order("finished_at ASC, public_id ASC").Limit(preEffectFailureBatch).Find(&requests).Error; err != nil {
			return err
		}
		for _, request := range requests {
			var item models.ContentItem
			if err := tx.Where("tenant_id=? AND public_id=? AND processing_generation=?", request.TenantID, request.ContentItemID, request.ProcessingGeneration).First(&item).Error; err != nil {
				if err == gorm.ErrRecordNotFound {
					continue
				}
				return err
			}
			if item.Status == models.ContentStatusArchived {
				continue
			}
			next, err := preEffectRecoveryState(tx, item, request)
			if err != nil {
				return err
			}
			if next == "" {
				continue
			}
			previousFailure := request.FailureClass
			proof := jsonValue(map[string]any{
				"reclassified":           true,
				"previous_state":         models.ContentStageFailed,
				"previous_failure_class": previousFailure,
				"reason":                 "no_effect_started",
				"target_state":           next,
				"reclassified_at":        now,
			})
			if err := tx.Model(&request).Updates(map[string]any{
				"state":            next,
				"failure_class":    "",
				"finished_at":      nil,
				"verified_at":      nil,
				"terminal_proof":   proof,
				"claim_owner":      "",
				"claim_token":      nil,
				"claim_expires_at": nil,
				"not_before_at":    nil,
				"deadline_at":      nil,
				"updated_at":       now,
			}).Error; err != nil {
				return err
			}
			request.State, request.FailureClass, request.FinishedAt, request.VerifiedAt = next, "", nil, nil
			request.TerminalProof = proof
			if err := appendEvent(tx, request, nil, "pre_effect_failure_reclassified", map[string]any{
				"previous_failure_class": previousFailure,
				"target_state":           next,
				"reason":                 "no_effect_started",
			}); err != nil {
				return err
			}
			if err := reduceReadiness(tx, request.TenantID, request.ContentItemID, request.ProcessingGeneration); err != nil {
				return err
			}
		}
		return nil
	})
}

func preEffectRecoveryState(tx *gorm.DB, item models.ContentItem, request models.ContentStageRequest) (string, error) {
	if request.Stage == models.ContentStagePodsMediaArtifacts {
		if normalizeAcquisitionMode(ResolveMediaAcquisitionMode(tx, item)) == models.MediaAcquisitionManual {
			return models.ContentStageAwaitingApproval, nil
		}
		return models.ContentStageQueued, nil
	}
	if request.Lane == models.ContentStageLanePods {
		// Metadata and optional work are not independently claimable for a
		// metadata-only parent.  Keep the durable state honest until its media
		// predecessor verifies, even for the legacy text-embedding descriptor
		// whose dependency manifest predates media admission.
		var media models.ContentStageRequest
		if err := tx.Where("tenant_id=? AND content_item_id=? AND processing_generation=? AND stage=?", request.TenantID, request.ContentItemID, request.ProcessingGeneration, models.ContentStagePodsMediaArtifacts).First(&media).Error; err != nil {
			if err == gorm.ErrRecordNotFound {
				return models.ContentStageBlocked, nil
			}
			return "", err
		}
		if media.State != models.ContentStageVerified {
			return models.ContentStageBlocked, nil
		}
	}
	ready, err := dependenciesVerified(tx, request)
	if err != nil {
		return "", err
	}
	if !ready {
		return models.ContentStageBlocked, nil
	}
	if request.Stage == models.ContentStagePodsTranscript && shouldAwaitGeneratedSTT(HasProviderCaption(tx, item), automaticSTTEnabled(tx, item.TenantID), request.Priority) {
		return models.ContentStageAwaitingApproval, nil
	}
	return models.ContentStageQueued, nil
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
	var observed models.ContentItem
	if err := tx.Where("tenant_id=? AND public_id=? AND processing_generation=?", tenantID, contentID, generation).First(&observed).Error; err != nil {
		return err
	}
	sourceID := ""
	if observed.ContentSourceID != nil {
		sourceID = observed.ContentSourceID.String()
	}
	if err := lifecycle.Check(tx, lifecycle.Scope{
		TenantID: tenantID, Lane: laneForType(observed.Type), SourceID: sourceID, ItemID: contentID.String(),
	}, lifecycle.PhaseContentWrite); err != nil {
		return err
	}
	var item models.ContentItem
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=? AND processing_generation=?", tenantID, contentID, generation).First(&item).Error; err != nil {
		return err
	}
	if !item.UpdatedAt.Equal(observed.UpdatedAt) || item.Type != observed.Type || (item.ContentSourceID == nil) != (observed.ContentSourceID == nil) || (item.ContentSourceID != nil && *item.ContentSourceID != *observed.ContentSourceID) {
		return fmt.Errorf("content changed while stage readiness was being reduced")
	}
	mode, err := CutoverMode(tx, tenantID, laneForType(item.Type))
	if err != nil || mode != models.ContentStageCutoverDurableRequired {
		return err
	}
	// Lifecycle is monotonic after publication. Optional failures, duplicate
	// observations, stale fences, and timeout recovery must not unpublish a
	// currently visible item; generation activation/rollback owns replacement.
	if (item.Status == models.ContentStatusReady && !(item.ParentContentItemID != nil && item.FeedVisibility == "embedding_pending")) || item.Status == models.ContentStatusArchived {
		return nil
	}
	var requests []models.ContentStageRequest
	readinessQuery := tx.Where("tenant_id=? AND content_item_id=? AND processing_generation=?", tenantID, contentID, generation)
	if item.ParentContentItemID != nil {
		readinessQuery = readinessQuery.Where("blocking_scope<>?", models.ContentStageBlockingOptional)
	} else {
		readinessQuery = readinessQuery.Where("blocking_scope=?", models.ContentStageBlockingContentReady)
	}
	if err := readinessQuery.Find(&requests).Error; err != nil {
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
	publishChild := next == models.ContentStatusReady && item.ParentContentItemID != nil && item.FeedVisibility == "embedding_pending" && item.IsFeedUnit && item.DurationSec != nil && *item.DurationSec >= 270 && *item.DurationSec <= 2400
	if publishChild {
		var owned int64
		if err := tx.Table("atomization_chapter_units u").Joins("JOIN atomization_generations g ON g.public_id=u.generation_id AND g.tenant_id=u.tenant_id").Where("u.tenant_id=? AND u.candidate_content_item_id=? AND u.state='verified' AND g.parent_content_item_id=? AND g.state=?", tenantID, contentID, item.ParentContentItemID, "active").Count(&owned).Error; err != nil {
			return err
		}
		publishChild = owned == 1
	}
	if item.Status == next && !publishChild {
		return nil
	}
	item.Status = next
	if publishChild {
		item.FeedVisibility = "visible"
		published := "published"
		item.ChapteringStatus = &published
		if err := tx.Model(&models.Chapter{}).Where("tenant_id=? AND child_content_item_id=?", tenantID, contentID).Update("status", published).Error; err != nil {
			return err
		}
	}
	if err := tx.Save(&item).Error; err != nil {
		return err
	}
	if err := feedstate.AttachReadyNewsStory(tx, item); err != nil {
		return err
	}
	if err := feedstate.SyncMediaMembership(tx, item); err != nil {
		return err
	}
	// The activation sweeper runs after this transaction commits. Never take
	// a parent lock while holding a child lock: activation owns parent -> child.
	return nil
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
