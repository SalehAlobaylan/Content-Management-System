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
	Items               []UpstreamObservationItem
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
	Disposition         string
	ContentItemID       string
	FilterClass         string
}

// RecordUpstreamObservations preserves only replay identities and digests for
// a CMS-authorized fetch page whose intake budget is zero. Raw provider
// payloads, URLs, queue names, and arbitrary replay arguments are never stored.
func RecordUpstreamObservations(db *gorm.DB, input RecordUpstreamObservationsInput) (int, error) {
	if db == nil {
		return 0, fmt.Errorf("upstream observation store requires a database")
	}
	input.TenantID = strings.TrimSpace(input.TenantID)
	input.ProviderCapability = strings.TrimSpace(input.ProviderCapability)
	input.ProviderVersion = strings.TrimSpace(input.ProviderVersion)
	input.ProviderPageID = strings.TrimSpace(input.ProviderPageID)
	if input.ProviderCapability != "replayable_listing" && input.ProviderCapability != "peek" {
		return 0, fmt.Errorf("provider observation capability is not registered")
	}
	if input.ProviderVersion == "" || len(input.ProviderVersion) > 64 || input.ProviderPageID == "" || len(input.ProviderPageID) > 128 {
		return 0, fmt.Errorf("provider observation identity is invalid")
	}
	if len(input.Items) == 0 || len(input.Items) > maxUpstreamObservationsPerPage {
		return 0, fmt.Errorf("upstream observation batch is outside its bounded contract")
	}
	unit, err := VerifyExecutionEnvelope(db, input.TenantID, input.RequestID, input.AttemptID, input.UnitID, input.UnitJobID, input.AttemptFenceToken)
	if err != nil {
		return 0, err
	}
	lease, err := uuid.Parse(strings.TrimSpace(input.ExecutionLeaseToken))
	if err != nil || unit.ExecutionLeaseToken == nil || *unit.ExecutionLeaseToken != lease || unit.ExecutionLeaseExpiresAt == nil || !unit.ExecutionLeaseExpiresAt.After(time.Now().UTC()) || unit.State != string(UnitRunning) || unit.UnitType != "fetch_page" {
		return 0, fmt.Errorf("source-run observation lease is not current")
	}
	now := time.Now().UTC()
	var replayUntil *time.Time
	if input.ProviderCapability == "replayable_listing" {
		value := now.Add(24 * time.Hour)
		replayUntil = &value
	}
	created := 0
	err = db.Transaction(func(tx *gorm.DB) error {
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
				PublicID: uuid.New(), TenantID: input.TenantID, ContentSourceID: unit.ContentSourceID,
				SourceRunRequestID: &unit.SourceRunRequestID, ProviderCapability: input.ProviderCapability,
				ProviderVersion: input.ProviderVersion, UpstreamItemID: item.UpstreamItemID,
				UpstreamFingerprint: item.UpstreamFingerprint, ReplayLocator: datatypes.JSON(locator),
				ReplayUntil: replayUntil, ProviderCursor: boundedObservationCursor(input.ProviderCursor),
				ProviderPageID: input.ProviderPageID, ObservedAt: now,
			}
			result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&observation)
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected == 0 {
				continue
			}
			created++
			payload, _ := json.Marshal(map[string]any{"schema_version": "source-run-upstream-observation-event/v1", "provider_capability": input.ProviderCapability, "provider_page_id": input.ProviderPageID})
			event := models.SourceUpstreamObservationEvent{
				PublicID: uuid.New(), TenantID: input.TenantID,
				EventKey:      observationEventKey(input.TenantID, observation.PublicID, "deferred"),
				ObservationID: observation.PublicID, EventType: "deferred", CausationID: unit.PublicID.String(),
				Payload: datatypes.JSON(payload), OccurredAt: now,
			}
			if err := tx.Create(&event).Error; err != nil {
				return err
			}
			if err := tx.Create(&models.SourceRunProjectionWork{PublicID: uuid.New(), TenantID: input.TenantID, EvidenceKind: "upstream_observation_event", EvidenceID: event.PublicID, ReducerVersion: "source-run-upstream-observation/v1", State: "queued"}).Error; err != nil {
				return err
			}
		}
		return nil
	})
	return created, err
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

// RecordUpstreamObservationDisposition terminalizes one deferred identity only
// from the current fenced normalization unit of its CMS-created drain request.
func RecordUpstreamObservationDisposition(db *gorm.DB, input MaterializeUpstreamObservationInput) (bool, error) {
	if input.Disposition != "materialized" && input.Disposition != "filtered" {
		return false, fmt.Errorf("upstream observation disposition is not registered")
	}
	unit, err := VerifyExecutionEnvelope(db, strings.TrimSpace(input.TenantID), input.RequestID, input.AttemptID, input.UnitID, input.UnitJobID, input.AttemptFenceToken)
	if err != nil {
		return false, err
	}
	lease, err := uuid.Parse(strings.TrimSpace(input.ExecutionLeaseToken))
	if err != nil || unit.ExecutionLeaseToken == nil || *unit.ExecutionLeaseToken != lease || unit.ExecutionLeaseExpiresAt == nil || !unit.ExecutionLeaseExpiresAt.After(time.Now().UTC()) || unit.State != string(UnitRunning) || unit.UnitType != "normalize_batch" {
		return false, fmt.Errorf("source-run materialization lease is not current")
	}
	observationID, err := uuid.Parse(strings.TrimSpace(input.ObservationID))
	if err != nil {
		return false, fmt.Errorf("upstream observation identity is invalid")
	}
	now := time.Now().UTC()
	created := false
	err = db.Transaction(func(tx *gorm.DB) error {
		var request models.SourceRunRequest
		if err := tx.Where("tenant_id=? AND public_id=? AND purpose=?", unit.TenantID, unit.SourceRunRequestID, "deferred_drain").First(&request).Error; err != nil {
			return err
		}
		if !requestMetadataContainsObservation(request.Metadata, observationID.String()) {
			return fmt.Errorf("observation is not reserved by this drain request")
		}
		var observation models.SourceUpstreamObservation
		if err := tx.Where("tenant_id=? AND public_id=? AND content_source_id=?", unit.TenantID, observationID, unit.ContentSourceID).First(&observation).Error; err != nil {
			return err
		}
		payload := map[string]any{"schema_version": "source-run-upstream-observation-event/v1", "drain_request_id": request.PublicID, "execution_unit_id": unit.PublicID}
		if input.Disposition == "materialized" {
			contentID, parseErr := uuid.Parse(strings.TrimSpace(input.ContentItemID))
			if parseErr != nil {
				return fmt.Errorf("materialized content identity is invalid")
			}
			var item models.ContentItem
			if err := tx.Where("tenant_id=? AND public_id=? AND content_source_id=? AND source_run_request_id=?", unit.TenantID, contentID, unit.ContentSourceID, request.ID).First(&item).Error; err != nil {
				return fmt.Errorf("materialized content provenance is not persisted: %w", err)
			}
			payload["content_item_id"] = item.PublicID
		} else {
			allowed := map[string]bool{"include_keywords": true, "exclude_keywords": true, "min_engagement": true, "moderation_rejected": true, "normalization_unsupported": true, "exact_duplicate": true}
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
