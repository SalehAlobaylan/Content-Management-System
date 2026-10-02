package sourceidentity

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/supply"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrInvalidObservation = errors.New("source item observation is not authorized for this ingest")
	ErrIdentityConflict   = errors.New("source item identity is already bound to another materialization")
	ErrIdentityCorrupt    = errors.New("source item identity registry is inconsistent")
)

type Input struct {
	TenantID            string
	ContentSourceID     uuid.UUID
	SourceRunRequestID  uuid.UUID
	ObservationID       uuid.UUID
	UpstreamItemID      string
	SourceRunAttemptID  uuid.UUID
	ExecutionUnitID     uuid.UUID
	ExecutionFenceToken uuid.UUID
	ExecutionLeaseToken uuid.UUID
	UnitJobID           string
	PageID              string
	BatchID             string
	ExpectedFingerprint string
}

type Binding struct {
	Input                Input
	IdentityID           uint
	IdentityPublicID     uuid.UUID
	Observation          models.SourceUpstreamObservation
	CurrentInstance      int
	CurrentContentItemID *uuid.UUID
	ReconstructionGrant  *models.ContentResetReconstructionGrant
	HasIdentity          bool
}

func ParseInput(tenantID, sourceID, sourceRunRequestID, observationID, upstreamItemID string) (*Input, error) {
	observationID = strings.TrimSpace(observationID)
	upstreamItemID = strings.TrimSpace(upstreamItemID)
	if observationID == "" && upstreamItemID == "" {
		return nil, nil
	}
	if observationID == "" || upstreamItemID == "" || len(upstreamItemID) > 255 {
		return nil, ErrInvalidObservation
	}
	source, err := uuid.Parse(strings.TrimSpace(sourceID))
	if err != nil {
		return nil, ErrInvalidObservation
	}
	request, err := uuid.Parse(strings.TrimSpace(sourceRunRequestID))
	if err != nil {
		return nil, ErrInvalidObservation
	}
	observation, err := uuid.Parse(observationID)
	if err != nil {
		return nil, ErrInvalidObservation
	}
	return &Input{
		TenantID: strings.TrimSpace(tenantID), ContentSourceID: source,
		SourceRunRequestID: request, ObservationID: observation, UpstreamItemID: upstreamItemID,
	}, nil
}

func Resolve(db *gorm.DB, input *Input) (*Binding, error) {
	if input == nil {
		return nil, nil
	}
	if err := validateObservationUnit(db, *input); err != nil {
		return nil, err
	}
	var observation models.SourceUpstreamObservation
	if err := db.Where("tenant_id = ? AND content_source_id = ? AND public_id = ? AND upstream_item_id = ?",
		input.TenantID, input.ContentSourceID, input.ObservationID, input.UpstreamItemID).First(&observation).Error; err != nil {
		return nil, ErrInvalidObservation
	}
	if input.ExpectedFingerprint != "" && !strings.EqualFold(input.ExpectedFingerprint, observation.UpstreamFingerprint) {
		return nil, ErrInvalidObservation
	}

	binding := &Binding{Input: *input, Observation: observation}
	var identity models.SourceItemIdentity
	err := db.Where("tenant_id = ? AND content_source_id = ? AND upstream_item_id = ?",
		input.TenantID, input.ContentSourceID, input.UpstreamItemID).First(&identity).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return binding, nil
	}
	if err != nil {
		return nil, err
	}
	binding.HasIdentity = true
	binding.IdentityID = identity.ID
	binding.IdentityPublicID = identity.PublicID
	binding.CurrentInstance = identity.CurrentInstanceGeneration
	binding.CurrentContentItemID = identity.CurrentContentItemID
	if identity.CurrentInstanceGeneration == 0 && identity.CurrentContentItemID == nil {
		reserved, err := identityReserved(db, input.TenantID, identity.ID)
		if err != nil {
			return nil, err
		}
		if reserved {
			return nil, ErrIdentityConflict
		}
	}
	if identity.CurrentInstanceGeneration < 1 || identity.CurrentContentItemID == nil {
		return nil, ErrIdentityCorrupt
	}
	var instance models.SourceItemInstance
	if err := db.Where("tenant_id = ? AND identity_id = ? AND instance_generation = ?",
		input.TenantID, identity.ID, identity.CurrentInstanceGeneration).First(&instance).Error; err != nil {
		return nil, ErrIdentityCorrupt
	}
	if instance.ContentItemID != *identity.CurrentContentItemID || (instance.State != "active" && instance.State != "retired") {
		return nil, ErrIdentityCorrupt
	}
	return binding, nil
}

func RegisterMaterialized(tx *gorm.DB, binding *Binding, contentItemID uuid.UUID) error {
	if binding == nil {
		return nil
	}
	if tx == nil || contentItemID == uuid.Nil {
		return ErrInvalidObservation
	}
	if err := validateObservationUnit(tx, binding.Input); err != nil {
		return err
	}
	lockKey := strings.Join([]string{binding.Input.TenantID, binding.Input.ContentSourceID.String(), binding.Input.UpstreamItemID}, "\n")
	if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", lockKey).Error; err != nil {
		return err
	}
	now := time.Now().UTC()
	identity := models.SourceItemIdentity{
		PublicID: uuid.New(), TenantID: binding.Input.TenantID,
		ContentSourceID: binding.Input.ContentSourceID, UpstreamItemID: binding.Input.UpstreamItemID,
		CurrentInstanceGeneration: 0, CreatedAt: now, UpdatedAt: now,
	}
	if err := tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "tenant_id"}, {Name: "content_source_id"}, {Name: "upstream_item_id"}},
		DoNothing: true,
	}).Create(&identity).Error; err != nil {
		return err
	}
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where(
		"tenant_id = ? AND content_source_id = ? AND upstream_item_id = ?",
		binding.Input.TenantID, binding.Input.ContentSourceID, binding.Input.UpstreamItemID,
	).First(&identity).Error; err != nil {
		return err
	}
	if identity.CurrentInstanceGeneration > 0 {
		if identity.CurrentContentItemID != nil && *identity.CurrentContentItemID == contentItemID {
			return nil
		}
		return ErrIdentityConflict
	}
	if identity.CurrentInstanceGeneration != 0 || identity.CurrentContentItemID != nil {
		return ErrIdentityCorrupt
	}
	reserved, err := identityReserved(tx, binding.Input.TenantID, identity.ID)
	if err != nil {
		return err
	}
	if reserved {
		return ErrIdentityConflict
	}
	instance := models.SourceItemInstance{
		PublicID: uuid.New(), TenantID: binding.Input.TenantID, IdentityID: identity.ID,
		InstanceGeneration: 1, ContentItemID: contentItemID,
		SourceObservationID: binding.Observation.PublicID,
		UpstreamFingerprint: strings.ToLower(binding.Observation.UpstreamFingerprint),
		ProviderVersion:     binding.Observation.ProviderVersion, State: "active", CreatedAt: now,
	}
	if err := tx.Create(&instance).Error; err != nil {
		return err
	}
	return tx.Model(&identity).Updates(map[string]any{
		"current_instance_generation": 1,
		"current_content_item_id":     contentItemID,
		"updated_at":                  now,
	}).Error
}

// RetireInstance marks the currently active instance of a content item as
// permanently retired. The identity row and its generation counter are
// preserved, so a later ordinary observation resolves to the retired instance
// and cannot re-materialize a replacement outside an authorized campaign grant.
func RetireInstance(tx *gorm.DB, tenant string, itemID uuid.UUID) error {
	if tx == nil || tenant == "" || itemID == uuid.Nil || !tx.Migrator().HasTable(&models.SourceItemInstance{}) {
		return nil
	}
	now := time.Now().UTC()
	result := tx.Model(&models.SourceItemInstance{}).
		Where("tenant_id=? AND content_item_id=? AND state='active'", tenant, itemID).
		Updates(map[string]any{"state": "retired", "retired_at": now})
	return result.Error
}

func validateObservationUnit(db *gorm.DB, input Input) error {
	if db == nil || strings.TrimSpace(input.TenantID) == "" || input.ContentSourceID == uuid.Nil ||
		input.SourceRunRequestID == uuid.Nil || input.ObservationID == uuid.Nil || strings.TrimSpace(input.UpstreamItemID) == "" {
		return ErrInvalidObservation
	}
	var observation models.SourceUpstreamObservation
	if err := db.Where("tenant_id = ? AND content_source_id = ? AND public_id = ? AND upstream_item_id = ?",
		input.TenantID, input.ContentSourceID, input.ObservationID, input.UpstreamItemID).First(&observation).Error; err != nil {
		return ErrInvalidObservation
	}
	var request models.SourceRunRequest
	if err := db.Where("tenant_id = ? AND content_source_id = ? AND public_id = ?",
		input.TenantID, input.ContentSourceID, input.SourceRunRequestID).First(&request).Error; err != nil {
		return ErrInvalidObservation
	}
	unit, err := verifyCurrentObservationUnit(db, input)
	if err != nil {
		return ErrInvalidObservation
	}
	if request.Purpose == "deferred_drain" {
		if !requestMetadataContainsObservation(request.Metadata, input.ObservationID.String()) || unit.UnitType != "normalize_batch" {
			return ErrInvalidObservation
		}
		var reservations int64
		if err := db.Model(&models.SourceUpstreamObservationEvent{}).
			Where("tenant_id = ? AND observation_id = ? AND event_type = ? AND causation_id = ?", input.TenantID, input.ObservationID, "materialization_reserved", request.PublicID.String()).
			Count(&reservations).Error; err != nil || reservations != 1 {
			return ErrInvalidObservation
		}
		return nil
	}
	if !ordinaryObservationPurpose(request.Purpose) || observation.SourceRunRequestID == nil || *observation.SourceRunRequestID != request.PublicID ||
		observation.ProviderPageID != unit.PageID || unit.UnitType != "normalize_batch" {
		return ErrInvalidObservation
	}
	return nil
}

func ordinaryObservationPurpose(purpose string) bool {
	switch strings.TrimSpace(purpose) {
	case "baseline", "exploration", "circulation", "operator_run_once", "manual", "missed_admission_repair", "partial_repair":
		return true
	default:
		return false
	}
}

func verifyCurrentObservationUnit(db *gorm.DB, input Input) (models.SourceRunExecutionUnit, error) {
	if input.SourceRunAttemptID == uuid.Nil || input.ExecutionUnitID == uuid.Nil || input.ExecutionFenceToken == uuid.Nil ||
		input.ExecutionLeaseToken == uuid.Nil || strings.TrimSpace(input.UnitJobID) == "" ||
		strings.TrimSpace(input.PageID) == "" || strings.TrimSpace(input.BatchID) == "" {
		return models.SourceRunExecutionUnit{}, ErrInvalidObservation
	}
	unit, err := supply.VerifyExecutionEnvelope(db, input.TenantID, input.SourceRunRequestID.String(), input.SourceRunAttemptID.String(), input.ExecutionUnitID.String(), input.UnitJobID, input.ExecutionFenceToken.String())
	if err != nil {
		return models.SourceRunExecutionUnit{}, ErrInvalidObservation
	}
	now := time.Now().UTC()
	if unit.ContentSourceID != input.ContentSourceID || unit.UnitType != "normalize_batch" || unit.State != string(supply.UnitRunning) ||
		unit.ExecutionLeaseToken == nil || *unit.ExecutionLeaseToken != input.ExecutionLeaseToken ||
		unit.ExecutionLeaseExpiresAt == nil || !unit.ExecutionLeaseExpiresAt.After(now) ||
		unit.PageID != input.PageID || unit.BatchID != input.BatchID || unit.ParentUnitID == nil {
		return models.SourceRunExecutionUnit{}, ErrInvalidObservation
	}
	var page models.SourceRunExecutionUnit
	if err := db.Where("tenant_id = ? AND public_id = ? AND source_run_request_id = ? AND source_run_attempt_id = ?",
		input.TenantID, *unit.ParentUnitID, input.SourceRunRequestID, input.SourceRunAttemptID).First(&page).Error; err != nil || page.UnitType != "fetch_page" || page.PageID != unit.PageID {
		return models.SourceRunExecutionUnit{}, ErrInvalidObservation
	}
	return unit, nil
}

func requestMetadataContainsObservation(metadata datatypes.JSON, observationID string) bool {
	var value struct {
		ObservationIDs []string          `json:"deferred_observation_ids"`
		ObservationMap map[string]string `json:"deferred_observation_map"`
	}
	if json.Unmarshal(metadata, &value) != nil {
		return false
	}
	for _, id := range value.ObservationIDs {
		if id == observationID {
			return true
		}
	}
	for _, id := range value.ObservationMap {
		if id == observationID {
			return true
		}
	}
	return false
}
