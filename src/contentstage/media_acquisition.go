package contentstage

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"content-management-system/src/models"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func normalizeAcquisitionMode(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case models.MediaAcquisitionManual:
		return models.MediaAcquisitionManual
	default:
		return models.MediaAcquisitionAutomatic
	}
}

func initialStageState(descriptor Descriptor, acquisitionMode string) string {
	if descriptor.Stage == models.ContentStagePodsMediaArtifacts && normalizeAcquisitionMode(acquisitionMode) == models.MediaAcquisitionManual {
		return models.ContentStageAwaitingApproval
	}
	if len(descriptor.Dependencies) > 0 {
		return models.ContentStageBlocked
	}
	return models.ContentStageQueued
}

func shouldAwaitGeneratedSTT(captionPresent, autoEnabled bool, priority int16) bool {
	return !captionPresent && !autoEnabled && priority < 100
}

// ResolveMediaAcquisitionMode applies source override -> tenant default ->
// automatic code default. It intentionally does not use source APIConfig.
func ResolveMediaAcquisitionMode(db *gorm.DB, item models.ContentItem) string {
	if db == nil || !db.Migrator().HasTable(&models.MediaAcquisitionConfig{}) {
		return models.MediaAcquisitionAutomatic
	}
	if item.ContentSourceID != nil {
		var source models.ContentSource
		if err := db.Select("media_acquisition_mode").Where("tenant_id=? AND public_id=?", item.TenantID, *item.ContentSourceID).First(&source).Error; err == nil && source.MediaAcquisitionMode != nil {
			return normalizeAcquisitionMode(*source.MediaAcquisitionMode)
		}
	}
	var config models.MediaAcquisitionConfig
	if err := db.Where("tenant_id=?", item.TenantID).First(&config).Error; err == nil {
		return normalizeAcquisitionMode(config.DefaultMode)
	}
	return models.MediaAcquisitionAutomatic
}

func hasCaptionArtifact(item models.ContentItem) bool {
	if len(item.Metadata) == 0 {
		return false
	}
	var metadata map[string]any
	if json.Unmarshal(item.Metadata, &metadata) != nil {
		return false
	}
	caption, ok := metadata["caption_artifact"].(map[string]any)
	if !ok {
		return false
	}
	text, _ := caption["full_text"].(string)
	return strings.TrimSpace(text) != ""
}

func automaticSTTEnabled(db *gorm.DB, tenantID string) bool {
	var cfg models.TranscriptionConfig
	return db.Where("tenant_id=?", tenantID).First(&cfg).Error == nil && cfg.AutoSttEnabled
}

// promoteReadyDependents makes dependency state explicit. A blocked request is
// promoted only when every predecessor is verified. Generated STT has a second
// admission boundary; provider caption import does not.
func promoteReadyDependents(tx *gorm.DB, item models.ContentItem) error {
	var blocked []models.ContentStageRequest
	if err := tx.Where("tenant_id=? AND content_item_id=? AND processing_generation=? AND state=?", item.TenantID, item.PublicID, item.ProcessingGeneration, models.ContentStageBlocked).Order("created_at ASC").Find(&blocked).Error; err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, request := range blocked {
		ready, err := dependenciesVerified(tx, request)
		if err != nil || !ready {
			if err != nil {
				return err
			}
			continue
		}
		next := models.ContentStageQueued
		event := "dependency_released"
		if request.Stage == models.ContentStagePodsTranscript && shouldAwaitGeneratedSTT(hasCaptionArtifact(item), automaticSTTEnabled(tx, item.TenantID), request.Priority) {
			next = models.ContentStageAwaitingApproval
			event = "manual_transcript_required"
		}
		if err := tx.Model(&request).Updates(map[string]any{"state": next, "not_before_at": nil, "updated_at": now}).Error; err != nil {
			return err
		}
		request.State = next
		if err := appendEvent(tx, request, nil, event, map[string]any{"state": next}); err != nil {
			return err
		}
	}
	return nil
}

func MediaAcquisitionRequiresApproval(db *gorm.DB, tenantID string, contentID uuid.UUID, generation int64) bool {
	var count int64
	if db == nil {
		return false
	}
	db.Model(&models.ContentStageRequest{}).Where("tenant_id=? AND content_item_id=? AND processing_generation=? AND stage=? AND state=?", tenantID, contentID, generation, models.ContentStagePodsMediaArtifacts, models.ContentStageAwaitingApproval).Count(&count)
	return count > 0
}

type MediaAcquisitionDisposition struct {
	ContentItemID string `json:"content_item_id"`
	RequestID     string `json:"request_id"`
	State         string `json:"state"`
	Disposition   string `json:"disposition"`
}

// RequestMediaAcquisition releases one metadata-only item into the existing
// durable media stage. It never enqueues Redis directly.
func RequestMediaAcquisition(db *gorm.DB, tenantID string, contentID uuid.UUID, actor string) (MediaAcquisitionDisposition, error) {
	result := MediaAcquisitionDisposition{ContentItemID: contentID.String()}
	err := db.Transaction(func(tx *gorm.DB) error {
		var item models.ContentItem
		if err := tx.Where("tenant_id=? AND public_id=?", tenantID, contentID).First(&item).Error; err != nil {
			return err
		}
		if item.Type != models.ContentTypeVideo && item.Type != models.ContentTypePodcast {
			return fmt.Errorf("media acquisition is only available for Pods media")
		}
		if item.OriginalURL == nil || strings.TrimSpace(*item.OriginalURL) == "" {
			return fmt.Errorf("content item has no provider URL")
		}
		var request models.ContentStageRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND content_item_id=? AND processing_generation=? AND stage=?", tenantID, contentID, item.ProcessingGeneration, models.ContentStagePodsMediaArtifacts).First(&request).Error; err != nil {
			return err
		}
		result.RequestID = request.PublicID.String()
		switch request.State {
		case models.ContentStageAwaitingApproval, models.ContentStageBlocked:
			wasBlocked := request.State == models.ContentStageBlocked
			// pods_media_artifacts is the root durable media stage and has no
			// predecessors. Older/backfilled rows can nevertheless carry the
			// generic blocked state; an explicit operator admission is safe to
			// release those rows and prevents them being misrepresented as a
			// transcript problem in the Console.
			if wasBlocked {
				ready, dependencyErr := dependenciesVerified(tx, request)
				if dependencyErr != nil {
					return dependencyErr
				}
				if !ready {
					return fmt.Errorf("media acquisition is waiting for a predecessor")
				}
			}
			now := time.Now().UTC()
			deadline := now.Add(mediaElapsedBudget)
			if isTrustedLongForm(item) {
				deadline = now.Add(24 * time.Hour)
			}
			if err := tx.Model(&request).Updates(map[string]any{
				"state":         models.ContentStageQueued,
				"not_before_at": nil,
				"deadline_at":   deadline,
				"accepted_at":   now,
				"failure_class": "",
				"updated_at":    now,
			}).Error; err != nil {
				return err
			}
			request.State, request.DeadlineAt, request.AcceptedAt = models.ContentStageQueued, &deadline, &now
			event := "media_acquisition_approved"
			if wasBlocked {
				event = "media_acquisition_released_from_legacy_block"
			}
			if err := appendEvent(tx, request, nil, event, map[string]any{
				"actor": actor, "execution_budget_reset": true, "deadline_at": deadline,
			}); err != nil {
				return err
			}
			result.State, result.Disposition = request.State, "queued"
		case models.ContentStageQueued, models.ContentStageClaimed, models.ContentStageRunning, models.ContentStageVerifying:
			result.State, result.Disposition = request.State, "already_active"
		case models.ContentStageVerified:
			result.State, result.Disposition = request.State, "already_ready"
		case models.ContentStageUncertain, models.ContentStageReconciling, models.ContentStageFailed:
			return fmt.Errorf("media acquisition requires reconciliation before retry")
		default:
			return fmt.Errorf("media acquisition cannot start from state %s", request.State)
		}
		return nil
	})
	return result, err
}

// ReconcileSourceAcquisitionPolicy applies a source policy change to work that
// has not begun an effect. Running attempts are deliberately allowed to finish.
func ReconcileSourceAcquisitionPolicy(tx *gorm.DB, source models.ContentSource, resolvedMode string) error {
	if !SchemaAvailable(tx) {
		return nil
	}
	var requests []models.ContentStageRequest
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Table("content_stage_requests AS csr").Select("csr.*").
		Joins("JOIN content_items ci ON ci.tenant_id=csr.tenant_id AND ci.public_id=csr.content_item_id AND ci.processing_generation=csr.processing_generation").
		Where("csr.tenant_id=? AND ci.content_source_id=? AND csr.stage=? AND csr.state IN ?", source.TenantID, source.PublicID, models.ContentStagePodsMediaArtifacts,
			[]string{
				models.ContentStageAwaitingApproval, models.ContentStageQueued, models.ContentStageDeferred,
				models.ContentStageClaimed, models.ContentStageFailed, models.ContentStageUncertain,
				models.ContentStageReconciling,
			}).Find(&requests).Error; err != nil {
		return err
	}
	now := time.Now().UTC()
	requestIDs := make([]uuid.UUID, 0, len(requests))
	claimedIDs := make([]uuid.UUID, 0)
	reconciliationIDs := make([]uuid.UUID, 0)
	for _, request := range requests {
		requestIDs = append(requestIDs, request.PublicID)
		if request.State == models.ContentStageClaimed {
			claimedIDs = append(claimedIDs, request.PublicID)
		}
		if request.State == models.ContentStageFailed || request.State == models.ContentStageUncertain || request.State == models.ContentStageReconciling {
			reconciliationIDs = append(reconciliationIDs, request.PublicID)
		}
	}
	type requestCount struct {
		RequestID uuid.UUID `gorm:"column:request_id"`
		Count     int64     `gorm:"column:count"`
	}
	startedByRequest := map[uuid.UUID]int64{}
	if len(claimedIDs) > 0 {
		var rows []requestCount
		if err := tx.Model(&models.ContentStageAttempt{}).Select("request_id, COUNT(*) AS count").
			Where("tenant_id=? AND request_id IN ? AND effect_started_at IS NOT NULL", source.TenantID, claimedIDs).
			Group("request_id").Scan(&rows).Error; err != nil {
			return err
		}
		for _, row := range rows {
			startedByRequest[row.RequestID] = row.Count
		}
	}
	manifestsByRequest := map[uuid.UUID]int64{}
	manifestTableAvailable := tx.Migrator().HasTable(&models.MediaArtifactManifest{})
	if manifestTableAvailable && len(reconciliationIDs) > 0 {
		var rows []requestCount
		if err := tx.Table("content_stage_attempts AS csa").
			Select("csa.request_id, COUNT(mam.id) AS count").
			Joins("JOIN media_artifact_manifests AS mam ON mam.tenant_id=csa.tenant_id AND mam.attempt_id=csa.public_id AND mam.state<>?", "deleted").
			Where("csa.tenant_id=? AND csa.request_id IN ?", source.TenantID, reconciliationIDs).
			Group("csa.request_id").Scan(&rows).Error; err != nil {
			return err
		}
		for _, row := range rows {
			manifestsByRequest[row.RequestID] = row.Count
		}
	}
	type transition struct {
		request      models.ContentStageRequest
		next         string
		failureClass string
		event        string
	}
	transitions := make([]transition, 0, len(requests))
	cancelAttemptRequestIDs := make([]uuid.UUID, 0)
	for _, request := range requests {
		next := models.ContentStageQueued
		if normalizeAcquisitionMode(resolvedMode) == models.MediaAcquisitionManual {
			next = models.ContentStageAwaitingApproval
		}
		if request.State == models.ContentStageFailed || request.State == models.ContentStageUncertain || request.State == models.ContentStageReconciling {
			// Every storage upload registers a CMS manifest before PutObject. With
			// no non-deleted manifest tied to any request attempt, absence is
			// authoritative and the request can safely return to admission. If a
			// manifest exists, retain an explicit reconciliation lane so a later
			// adopter/cleaner can resolve the potentially completed side effect.
			if !manifestTableAvailable {
				next = models.ContentStageReconciling
			} else if manifestsByRequest[request.PublicID] > 0 {
				next = models.ContentStageReconciling
			}
		}
		if request.State == models.ContentStageClaimed {
			if startedByRequest[request.PublicID] > 0 {
				continue
			}
			cancelAttemptRequestIDs = append(cancelAttemptRequestIDs, request.PublicID)
		}
		if request.State == next {
			continue
		}
		failureClass := ""
		if next == models.ContentStageReconciling {
			failureClass = "artifact_manifest_reconciliation_required"
		} else if request.State == models.ContentStageFailed || request.State == models.ContentStageUncertain || request.State == models.ContentStageReconciling {
			failureClass = "reconciled_no_artifact_manifest"
		}
		event := "media_acquisition_policy_changed"
		if failureClass == "artifact_manifest_reconciliation_required" {
			event = "media_acquisition_reconciliation_required"
		} else if failureClass == "reconciled_no_artifact_manifest" {
			event = "media_acquisition_reconciled_absent"
		}
		transitions = append(transitions, transition{request: request, next: next, failureClass: failureClass, event: event})
	}
	if len(cancelAttemptRequestIDs) > 0 {
		if err := tx.Model(&models.ContentStageAttempt{}).
			Where("tenant_id=? AND request_id IN ? AND state=?", source.TenantID, cancelAttemptRequestIDs, models.ContentStageClaimed).
			Updates(map[string]any{"state": models.ContentStageCancelled, "finished_at": now, "updated_at": now}).Error; err != nil {
			return err
		}
	}
	grouped := map[string][]uuid.UUID{}
	for _, change := range transitions {
		key := change.next + "\x00" + change.failureClass
		grouped[key] = append(grouped[key], change.request.PublicID)
	}
	for key, ids := range grouped {
		parts := strings.SplitN(key, "\x00", 2)
		if err := tx.Model(&models.ContentStageRequest{}).Where("tenant_id=? AND public_id IN ?", source.TenantID, ids).
			Updates(map[string]any{"state": parts[0], "claim_owner": "", "claim_token": nil, "claim_expires_at": nil, "not_before_at": nil, "failure_class": parts[1], "updated_at": now}).Error; err != nil {
			return err
		}
	}
	if len(transitions) > 0 {
		var sequences []requestCount
		if err := tx.Model(&models.ContentStageEvent{}).Select("request_id, COALESCE(MAX(sequence), 0) AS count").
			Where("tenant_id=? AND request_id IN ?", source.TenantID, requestIDs).Group("request_id").Scan(&sequences).Error; err != nil {
			return err
		}
		sequenceByRequest := map[uuid.UUID]int64{}
		for _, row := range sequences {
			sequenceByRequest[row.RequestID] = row.Count
		}
		events := make([]models.ContentStageEvent, 0, len(transitions))
		for _, change := range transitions {
			events = append(events, models.ContentStageEvent{
				PublicID: uuid.New(), TenantID: change.request.TenantID, RequestID: change.request.PublicID,
				Sequence: sequenceByRequest[change.request.PublicID] + 1, EventType: change.event,
				Payload: jsonValue(map[string]any{"state": change.next}), OccurredAt: now,
			})
		}
		if err := tx.Create(&events).Error; err != nil {
			return err
		}
		contentIDs := make([]uuid.UUID, 0, len(transitions))
		seenContent := map[uuid.UUID]bool{}
		for _, change := range transitions {
			if !seenContent[change.request.ContentItemID] {
				seenContent[change.request.ContentItemID] = true
				contentIDs = append(contentIDs, change.request.ContentItemID)
			}
		}
		// Recompute the non-published lifecycle projection in one statement.
		// A reconciled media failure must not leave a metadata preview falsely
		// labelled FAILED, and policy switches can touch hundreds of items.
		if err := tx.Exec(`UPDATE content_items AS ci
			SET status = CASE
				WHEN EXISTS (
					SELECT 1 FROM content_stage_requests csr
					WHERE csr.tenant_id=ci.tenant_id AND csr.content_item_id=ci.public_id
						AND csr.processing_generation=ci.processing_generation
						AND csr.blocking_scope=? AND csr.state=?
				) THEN ?
				WHEN EXISTS (
					SELECT 1 FROM content_stage_requests csr
					WHERE csr.tenant_id=ci.tenant_id AND csr.content_item_id=ci.public_id
						AND csr.processing_generation=ci.processing_generation
						AND csr.blocking_scope=? AND csr.state NOT IN ?
				) THEN ?
				ELSE ?
			END,
			updated_at = ?
			WHERE ci.tenant_id=? AND ci.public_id IN ? AND ci.status NOT IN ?`,
			models.ContentStageBlockingContentReady, models.ContentStageFailed, models.ContentStatusFailed,
			models.ContentStageBlockingContentReady,
			[]string{models.ContentStageQueued, models.ContentStageVerified, models.ContentStageFailed, models.ContentStageCancelled, models.ContentStageSuperseded},
			models.ContentStatusProcessing, models.ContentStatusPending, now, source.TenantID, contentIDs,
			[]models.ContentStatus{models.ContentStatusReady, models.ContentStatusArchived}).Error; err != nil {
			return err
		}
	}
	return nil
}
