package supply

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"content-management-system/src/models"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const maxUpstreamObservationsPerPage = 100

type UpstreamObservationItem struct {
	UpstreamItemID      string
	UpstreamFingerprint string
}

type RecordUpstreamObservationsInput struct {
	TenantID            string
	RequestID           string
	AttemptID           string
	UnitID              string
	UnitJobID           string
	AttemptFenceToken   string
	ExecutionLeaseToken string
	ProviderCapability  string
	ProviderVersion     string
	ProviderPageID      string
	ProviderCursor      string
	Disposition         string
	Items               []UpstreamObservationItem
}

type RecordUpstreamObservationsResult struct {
	Created        int
	ObservationIDs map[string]string
}

type MaterializeUpstreamObservationInput struct {
	TenantID            string
	RequestID           string
	AttemptID           string
	UnitID              string
	UnitJobID           string
	AttemptFenceToken   string
	ExecutionLeaseToken string
	ObservationID       string
	UpstreamItemID      string
	Disposition         string
	ContentItemID       string
	FilterClass         string
}

// RecordUpstreamObservations preserves bounded provider identity evidence for
// one CMS-authorized fetch page. Raw provider payloads, URLs, queue names, and
// arbitrary replay arguments are never stored.
func RecordUpstreamObservations(db *gorm.DB, input RecordUpstreamObservationsInput) (RecordUpstreamObservationsResult, error) {
	result := RecordUpstreamObservationsResult{ObservationIDs: make(map[string]string)}
	if db == nil {
		return result, fmt.Errorf("upstream observation store requires a database")
	}
	input.TenantID = strings.TrimSpace(input.TenantID)
	input.ProviderCapability = strings.TrimSpace(input.ProviderCapability)
	input.ProviderVersion = strings.TrimSpace(input.ProviderVersion)
	input.ProviderPageID = strings.TrimSpace(input.ProviderPageID)
	input.Disposition = strings.TrimSpace(input.Disposition)
	if input.ProviderCapability != "replayable_listing" && input.ProviderCapability != "peek" {
		return result, fmt.Errorf("provider observation capability is not registered")
	}
	if input.Disposition != "deferred" && input.Disposition != "observed" {
		return result, fmt.Errorf("provider observation disposition is not registered")
	}
	if input.ProviderVersion == "" || len(input.ProviderVersion) > 64 || input.ProviderPageID == "" || len(input.ProviderPageID) > 128 {
		return result, fmt.Errorf("provider observation identity is invalid")
	}
	if len(input.Items) == 0 || len(input.Items) > maxUpstreamObservationsPerPage {
		return result, fmt.Errorf("upstream observation batch is outside its bounded contract")
	}
	lease, err := uuid.Parse(strings.TrimSpace(input.ExecutionLeaseToken))
	if err != nil {
		return result, fmt.Errorf("source-run observation lease is invalid")
	}
	now := time.Now().UTC()
	var replayUntil *time.Time
	if input.ProviderCapability == "replayable_listing" {
		value := now.Add(24 * time.Hour)
		replayUntil = &value
	}
	var currentUnit models.SourceRunExecutionUnit
	err = db.Transaction(func(tx *gorm.DB) error {
		verified, err := VerifyExecutionEnvelope(tx, input.TenantID, input.RequestID, input.AttemptID, input.UnitID, input.UnitJobID, input.AttemptFenceToken)
		if err != nil {
			return err
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id = ? AND public_id = ?", input.TenantID, verified.PublicID).First(&currentUnit).Error; err != nil {
			return err
		}
		if currentUnit.ExecutionLeaseToken == nil || *currentUnit.ExecutionLeaseToken != lease || currentUnit.ExecutionLeaseExpiresAt == nil || !currentUnit.ExecutionLeaseExpiresAt.After(time.Now().UTC()) || currentUnit.State != string(UnitRunning) || currentUnit.UnitType != "fetch_page" || currentUnit.PageID != input.ProviderPageID {
			return fmt.Errorf("source-run observation lease is not current")
		}
		var request models.SourceRunRequest
		if err := tx.Where("tenant_id = ? AND public_id = ? AND content_source_id = ?", input.TenantID, currentUnit.SourceRunRequestID, currentUnit.ContentSourceID).First(&request).Error; err != nil {
			return fmt.Errorf("source-run observation request is unavailable: %w", err)
		}
		if input.Disposition == "deferred" {
			if request.ItemCap != 0 {
				return fmt.Errorf("only a zero-intake source run can defer upstream observations")
			}
		} else if request.Purpose == "deferred_drain" || request.ItemCap == 0 || !sourceObservationDispositionPurpose(request.Purpose) {
			return fmt.Errorf("source-run purpose or intake cap does not admit materialized observations")
		}
		for _, item := range input.Items {
			item.UpstreamItemID = strings.TrimSpace(item.UpstreamItemID)
			item.UpstreamFingerprint = strings.ToLower(strings.TrimSpace(item.UpstreamFingerprint))
			if item.UpstreamItemID == "" || len(item.UpstreamItemID) > 255 || len(item.UpstreamFingerprint) != 64 {
				return fmt.Errorf("upstream observation item is invalid")
			}
			if _, err := hex.DecodeString(item.UpstreamFingerprint); err != nil {
				return fmt.Errorf("upstream observation fingerprint is invalid")
			}
			locator, _ := json.Marshal(map[string]any{"schema_version": "source-run-replay-locator/v1", "upstream_item_id": item.UpstreamItemID})
			observation := models.SourceUpstreamObservation{
				PublicID: uuid.New(), TenantID: input.TenantID, ContentSourceID: currentUnit.ContentSourceID,
				SourceRunRequestID: &currentUnit.SourceRunRequestID, ProviderCapability: input.ProviderCapability,
				ProviderVersion: input.ProviderVersion, UpstreamItemID: item.UpstreamItemID,
				UpstreamFingerprint: item.UpstreamFingerprint, ReplayLocator: datatypes.JSON(locator),
				ReplayUntil: replayUntil, ProviderCursor: boundedObservationCursor(input.ProviderCursor),
				ProviderPageID: input.ProviderPageID, ObservedAt: now,
			}
			insertResult := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&observation)
			if insertResult.Error != nil {
				return insertResult.Error
			}
			if insertResult.RowsAffected > 0 {
				payload, _ := json.Marshal(map[string]any{"schema_version": "source-run-upstream-observation-event/v1", "provider_capability": input.ProviderCapability, "provider_page_id": input.ProviderPageID})
				eventType := input.Disposition
				event := models.SourceUpstreamObservationEvent{
					PublicID: uuid.New(), TenantID: input.TenantID,
					EventKey:      observationEventKey(input.TenantID, observation.PublicID, eventType),
					ObservationID: observation.PublicID, EventType: eventType, CausationID: currentUnit.PublicID.String(),
					Payload: datatypes.JSON(payload), OccurredAt: now,
				}
				if err := tx.Create(&event).Error; err != nil {
					return err
				}
				if err := tx.Create(&models.SourceRunProjectionWork{PublicID: uuid.New(), TenantID: input.TenantID, EvidenceKind: "upstream_observation_event", EvidenceID: event.PublicID, ReducerVersion: "source-run-upstream-observation/v1", State: "queued"}).Error; err != nil {
					return err
				}
				result.Created++
			}
			var persisted models.SourceUpstreamObservation
			if err := tx.Where("tenant_id = ? AND content_source_id = ? AND source_run_request_id = ? AND provider_version = ? AND provider_page_id = ? AND upstream_item_id = ?",
				input.TenantID, currentUnit.ContentSourceID, currentUnit.SourceRunRequestID, input.ProviderVersion, input.ProviderPageID, item.UpstreamItemID).First(&persisted).Error; err != nil {
				return err
			}
			if !strings.EqualFold(persisted.UpstreamFingerprint, item.UpstreamFingerprint) {
				return fmt.Errorf("provider identity changed during an idempotent source-run page")
			}
			result.ObservationIDs[item.UpstreamItemID] = persisted.PublicID.String()
		}
		return nil
	})
	if err != nil {
		return RecordUpstreamObservationsResult{}, err
	}
	return result, nil
}

func AppendUpstreamObservationEvent(db *gorm.DB, observation models.SourceUpstreamObservation, eventType, causationID string, occurredAt time.Time) (bool, error) {
	if db == nil || observation.PublicID == uuid.Nil || observation.TenantID == "" {
		return false, fmt.Errorf("upstream observation event identity is invalid")
	}
	allowed := map[string]bool{"replay_expiring": true, "replay_expired": true, "unrecoverable": true}
	if !allowed[eventType] {
		return false, fmt.Errorf("upstream observation event is not worker-admitted")
	}
	if occurredAt.IsZero() {
		occurredAt = time.Now().UTC()
	}
	created := false
	err := db.Transaction(func(tx *gorm.DB) error {
		payload, _ := json.Marshal(map[string]any{"schema_version": "source-run-upstream-observation-event/v1", "provider_capability": observation.ProviderCapability})
		event := models.SourceUpstreamObservationEvent{PublicID: uuid.New(), TenantID: observation.TenantID, EventKey: observationEventKey(observation.TenantID, observation.PublicID, eventType), ObservationID: observation.PublicID, EventType: eventType, CausationID: strings.TrimSpace(causationID), Payload: datatypes.JSON(payload), OccurredAt: occurredAt.UTC()}
		result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&event)
		if result.Error != nil || result.RowsAffected == 0 {
			return result.Error
		}
		created = true
		return tx.Create(&models.SourceRunProjectionWork{PublicID: uuid.New(), TenantID: observation.TenantID, EvidenceKind: "upstream_observation_event", EvidenceID: event.PublicID, ReducerVersion: "source-run-upstream-observation/v1", State: "queued"}).Error
	})
	return created, err
}

// RecordUpstreamObservationDisposition terminalizes one observed identity only
// from its current fenced normalization unit or CMS-created drain request.
func RecordUpstreamObservationDisposition(db *gorm.DB, input MaterializeUpstreamObservationInput) (bool, error) {
	if input.Disposition != "materialized" && input.Disposition != "filtered" {
		return false, fmt.Errorf("upstream observation disposition is not registered")
	}
	lease, err := uuid.Parse(strings.TrimSpace(input.ExecutionLeaseToken))
	if err != nil {
		return false, fmt.Errorf("source-run materialization lease is not current")
	}
	observationID, err := uuid.Parse(strings.TrimSpace(input.ObservationID))
	if err != nil {
		return false, fmt.Errorf("upstream observation identity is invalid")
	}
	input.UpstreamItemID = strings.TrimSpace(input.UpstreamItemID)
	if input.UpstreamItemID == "" || len(input.UpstreamItemID) > 255 {
		return false, fmt.Errorf("upstream item identity is invalid")
	}
	now := time.Now().UTC()
	created := false
	err = db.Transaction(func(tx *gorm.DB) error {
		verified, err := VerifyExecutionEnvelope(tx, strings.TrimSpace(input.TenantID), input.RequestID, input.AttemptID, input.UnitID, input.UnitJobID, input.AttemptFenceToken)
		if err != nil {
			return err
		}
		var unit models.SourceRunExecutionUnit
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id = ? AND public_id = ?", strings.TrimSpace(input.TenantID), verified.PublicID).First(&unit).Error; err != nil {
			return err
		}
		if unit.ExecutionLeaseToken == nil || *unit.ExecutionLeaseToken != lease || unit.ExecutionLeaseExpiresAt == nil || !unit.ExecutionLeaseExpiresAt.After(time.Now().UTC()) || unit.State != string(UnitRunning) || unit.UnitType != "normalize_batch" {
			return fmt.Errorf("source-run materialization lease is not current")
		}
		var request models.SourceRunRequest
		if err := tx.Where("tenant_id=? AND public_id=?", unit.TenantID, unit.SourceRunRequestID).First(&request).Error; err != nil {
			return err
		}
		if !sourceObservationDispositionPurpose(request.Purpose) {
			return fmt.Errorf("source-run request purpose does not admit upstream observation dispositions")
		}
		if request.Purpose == "deferred_drain" && !requestMetadataContainsObservation(request.Metadata, observationID.String()) {
			return fmt.Errorf("observation is not reserved by this drain request")
		}
		observationQuery := tx.Where("tenant_id=? AND public_id=? AND content_source_id=?", unit.TenantID, observationID, unit.ContentSourceID)
		if request.Purpose != "deferred_drain" {
			observationQuery = observationQuery.Where("source_run_request_id = ?", request.PublicID)
		}
		var observation models.SourceUpstreamObservation
		if err := observationQuery.Clauses(clause.Locking{Strength: "UPDATE"}).First(&observation).Error; err != nil {
			return err
		}
		if observation.UpstreamItemID != input.UpstreamItemID {
			return fmt.Errorf("upstream observation does not match the normalized item")
		}
		if request.Purpose == "deferred_drain" {
			var reservations int64
			if err := tx.Model(&models.SourceUpstreamObservationEvent{}).
				Where("tenant_id=? AND observation_id=? AND event_type=? AND causation_id=?", unit.TenantID, observation.PublicID, "materialization_reserved", request.PublicID.String()).
				Count(&reservations).Error; err != nil {
				return err
			}
			if reservations != 1 {
				return fmt.Errorf("observation reservation is not owned by this drain request")
			}
		}
		if request.Purpose != "deferred_drain" && (observation.SourceRunRequestID == nil || *observation.SourceRunRequestID != request.PublicID || unit.PageID == "" || observation.ProviderPageID != unit.PageID) {
			return fmt.Errorf("upstream observation does not belong to this normalization page")
		}
		payload := map[string]any{"schema_version": "source-run-upstream-observation-event/v1", "source_run_request_id": request.PublicID, "execution_unit_id": unit.PublicID}
		if input.Disposition == "materialized" {
			contentID, parseErr := uuid.Parse(strings.TrimSpace(input.ContentItemID))
			if parseErr != nil {
				return fmt.Errorf("materialized content identity is invalid")
			}
			var item models.ContentItem
			query := tx.Where("tenant_id=? AND public_id=? AND content_source_id=?", unit.TenantID, contentID, unit.ContentSourceID)
			if request.Purpose == "content_reset_replay" && tx.Migrator().HasTable(&models.ContentResetReplayReuse{}) {
				query = query.Where(`source_run_request_id=? OR EXISTS (
                   SELECT 1 FROM content_reset_replay_reuses reuse
                   JOIN source_item_instances instance ON instance.tenant_id=reuse.tenant_id AND instance.content_item_id=reuse.content_item_id AND instance.instance_generation=reuse.instance_generation AND instance.upstream_fingerprint=reuse.fingerprint
                   LEFT JOIN content_reset_reconstruction_grants grant ON grant.tenant_id=reuse.tenant_id AND grant.id=reuse.grant_id AND grant.state='consumed' AND grant.campaign_id=reuse.campaign_id AND grant.revision_id=reuse.revision_id AND grant.replacement_content_item_id=reuse.content_item_id AND grant.expected_fingerprint=reuse.fingerprint
                   LEFT JOIN source_item_identities identity ON identity.tenant_id=instance.tenant_id AND identity.id=instance.identity_id AND identity.current_content_item_id=instance.content_item_id AND identity.current_instance_generation=instance.instance_generation
                   WHERE reuse.tenant_id=content_items.tenant_id AND reuse.content_item_id=content_items.public_id
                     AND reuse.observation_id=? AND reuse.source_run_request_id=? AND reuse.execution_unit_id=? AND reuse.fingerprint=?
                     AND ((reuse.grant_id IS NOT NULL AND grant.id IS NOT NULL AND instance.campaign_id=reuse.campaign_id AND instance.state='staged')
                       OR (reuse.grant_id IS NULL AND identity.id IS NOT NULL AND instance.state='active'
                         AND NOT EXISTS (SELECT 1 FROM content_reset_targets target WHERE target.tenant_id=reuse.tenant_id AND target.revision_id=reuse.revision_id AND target.content_item_id=reuse.content_item_id AND target.disposition='selected' AND NOT target.protected)))
                )`, request.ID, observation.PublicID, request.PublicID, unit.PublicID, strings.ToLower(observation.UpstreamFingerprint))
			} else {
				query = query.Where("source_run_request_id=?", request.ID)
			}
			if err := query.First(&item).Error; err != nil {
				return fmt.Errorf("materialized content provenance is not persisted: %w", err)
			}
			payload["content_item_id"] = item.PublicID
		} else {
			allowed := map[string]bool{"include_keywords": true, "exclude_keywords": true, "min_engagement": true, "moderation_rejected": true, "normalization_unsupported": true, "duration_below_minimum": true, "exact_duplicate": true, "retired_source_identity": true}
			if !allowed[strings.TrimSpace(input.FilterClass)] {
				return fmt.Errorf("observation filter class is not registered")
			}
			payload["filter_class"] = strings.TrimSpace(input.FilterClass)
		}
		var terminal int64
		if err := tx.Model(&models.SourceUpstreamObservationEvent{}).Where("tenant_id=? AND observation_id=? AND event_type IN ?", unit.TenantID, observation.PublicID, []string{"materialized", "filtered", "unrecoverable", "authorized_abandonment"}).Count(&terminal).Error; err != nil {
			return err
		}
		if terminal > 0 {
			return nil
		}
		bytes, _ := json.Marshal(payload)
		event := models.SourceUpstreamObservationEvent{PublicID: uuid.New(), TenantID: unit.TenantID, EventKey: observationCausationEventKey(unit.TenantID, observation.PublicID, input.Disposition, request.PublicID.String()), ObservationID: observation.PublicID, EventType: input.Disposition, CausationID: request.PublicID.String(), Payload: datatypes.JSON(bytes), OccurredAt: now}
		result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&event)
		if result.Error != nil || result.RowsAffected == 0 {
			return result.Error
		}
		created = true
		return tx.Create(&models.SourceRunProjectionWork{PublicID: uuid.New(), TenantID: unit.TenantID, EvidenceKind: "upstream_observation_event", EvidenceID: event.PublicID, ReducerVersion: "source-run-upstream-observation/v1", State: "queued"}).Error
	})
	return created, err
}

func requestMetadataContainsObservation(metadata datatypes.JSON, observationID string) bool {
	var payload struct {
		ObservationIDs []string `json:"deferred_observation_ids"`
	}
	if json.Unmarshal(metadata, &payload) != nil {
		return false
	}
	for _, current := range payload.ObservationIDs {
		if current == observationID {
			return true
		}
	}
	return false
}

func sourceObservationDispositionPurpose(purpose string) bool {
	switch strings.TrimSpace(purpose) {
	case "deferred_drain", "content_reset_replay", "baseline", "exploration", "circulation",
		"operator_run_once", "manual", "missed_admission_repair", "partial_repair":
		return true
	default:
		return false
	}
}

func observationEventKey(tenant string, observationID uuid.UUID, event string) string {
	sum := sha256.Sum256([]byte(tenant + "\n" + observationID.String() + "\n" + event))
	return "upstream-observation:" + hex.EncodeToString(sum[:])
}

func observationCausationEventKey(tenant string, observationID uuid.UUID, event, causation string) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{tenant, observationID.String(), event, causation}, "\n")))
	return "upstream-observation:" + hex.EncodeToString(sum[:])
}

func boundedObservationCursor(value string) string {
	value = strings.TrimSpace(value)
	if len(value) <= 255 {
		return value
	}
	sum := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(sum[:])
}
