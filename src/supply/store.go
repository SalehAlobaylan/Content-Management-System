package supply

import (
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

const projectionReducerVersion = "source-run-reducer/v1"

type CreateRequestInput struct {
	Source              models.ContentSource
	Identity            RequestIdentity
	RequestedBy         string
	RequestedByActorID  string
	SourceSuggestionID  *uuid.UUID
	OperatorPlanID      *uuid.UUID
	OperatorStepID      *uuid.UUID
	EvidenceFingerprint string
	Metadata            datatypes.JSON
	NotBeforeAt         *time.Time
	ExpiresAt           *time.Time
	DeadlineAt          *time.Time
}

// CreateRequest commits one CMS-owned request per immutable identity. An
// equivalent concurrent request reads back the existing row instead of
// allocating an additional provider-effecting attempt.
func CreateRequest(db *gorm.DB, input CreateRequestInput) (models.SourceRunRequest, bool, error) {
	if db == nil {
		return models.SourceRunRequest{}, false, fmt.Errorf("source-run store requires a database")
	}
	if err := input.Identity.Validate(); err != nil {
		return models.SourceRunRequest{}, false, err
	}
	if input.Source.PublicID == uuid.Nil || input.Source.PublicID.String() != strings.TrimSpace(input.Identity.ContentSourceID) || input.Source.TenantID != strings.TrimSpace(input.Identity.TenantID) {
		return models.SourceRunRequest{}, false, fmt.Errorf("source-run identity does not match the tenant-scoped source")
	}
	if !validRequester(input.RequestedBy) {
		return models.SourceRunRequest{}, false, fmt.Errorf("invalid source-run requester")
	}
	key, err := input.Identity.IdempotencyKey()
	if err != nil {
		return models.SourceRunRequest{}, false, err
	}
	now := time.Now().UTC()
	request := models.SourceRunRequest{
		PublicID:            uuid.New(),
		TenantID:            input.Source.TenantID,
		ContentSourceID:     input.Source.PublicID,
		SourceSuggestionID:  input.SourceSuggestionID,
		RequestedBy:         strings.TrimSpace(input.RequestedBy),
		RequestedByActorID:  strings.TrimSpace(input.RequestedByActorID),
		State:               models.SourceRunRequested,
		OperatorPlanID:      input.OperatorPlanID,
		OperatorStepID:      input.OperatorStepID,
		CorrelationID:       uuid.NewString(),
		IdempotencyKey:      key,
		Lane:                strings.TrimSpace(input.Identity.Lane),
		Purpose:             strings.TrimSpace(input.Identity.Purpose),
		PolicyFingerprint:   strings.TrimSpace(input.Identity.PolicyFingerprint),
		EvidenceFingerprint: strings.TrimSpace(input.EvidenceFingerprint),
		ArgumentFingerprint: strings.TrimSpace(input.Identity.ArgumentFingerprint),
		CadenceWindowStart:  timePtr(input.Identity.CadenceWindowStart.UTC()),
		NotBeforeAt:         utcTimePtr(input.NotBeforeAt),
		ExpiresAt:           utcTimePtr(input.ExpiresAt),
		DeadlineAt:          utcTimePtr(input.DeadlineAt),
		NextDispatchAt:      utcTimePtr(input.NotBeforeAt),
		ManifestState:       models.SourceRunManifestOpen,
		EvidenceState:       "not_observed",
		FailedScope:         datatypes.JSON([]byte(`{}`)),
		Metadata:            normalizedRequestMetadata(input.Metadata),
		RequestedAt:         now,
	}
	budget, err := deriveSourceRunBudget(request.Metadata, request.Purpose)
	if err != nil {
		return models.SourceRunRequest{}, false, err
	}
	created := false
	err = db.Transaction(func(tx *gorm.DB) error {
		var current models.ContentSource
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", request.TenantID, request.ContentSourceID).First(&current).Error; err != nil {
			return err
		}
		if !current.IsActive || current.Category != request.Lane || current.SourceConfigVersion != input.Source.SourceConfigVersion {
			return fmt.Errorf("source-run source version or authority changed")
		}
		if current.IntakeCircuitUntil != nil && current.IntakeCircuitUntil.After(now) {
			return fmt.Errorf("source-run intake circuit is open")
		}
		if err := RequireDurableAdmission(tx, request.TenantID, request.Lane); err != nil {
			return err
		}
		switch request.Purpose {
		case "baseline", "exploration", "deferred_drain", "circulation":
			enabled, err := MayScheduleNormalIntake(tx, request.TenantID)
			if err != nil || !enabled {
				if err != nil {
					return err
				}
				return fmt.Errorf("normal source-run intake scheduling is disabled")
			}
		}
		var existing models.SourceRunRequest
		if err := tx.Where("tenant_id=? AND idempotency_key=?", request.TenantID, request.IdempotencyKey).First(&existing).Error; err == nil {
			request = existing
			return nil
		} else if err != gorm.ErrRecordNotFound {
			return err
		}
		if err := reserveSourceRunBudget(tx, &request, budget); err != nil {
			return err
		}
		result := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "tenant_id"}, {Name: "idempotency_key"}},
			DoNothing: true,
		}).Create(&request)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return tx.Where("tenant_id = ? AND idempotency_key = ?", request.TenantID, request.IdempotencyKey).First(&request).Error
		}
		created = true
		return tx.Create(&models.ContentProcessingEvent{
			TenantID:           request.TenantID,
			ContentSourceID:    &request.ContentSourceID,
			SourceRunRequestID: &request.ID,
			Stage:              "source_run",
			State:              models.SourceRunRequested,
			Producer:           "cms",
			CorrelationID:      request.CorrelationID,
			IdempotencyKey:     request.IdempotencyKey,
			EventClass:         "source_run_requested",
			Payload:            datatypes.JSON([]byte(`{}`)),
			OccurredAt:         now,
		}).Error
	})
	return request, created, err
}

func normalizedRequestMetadata(value datatypes.JSON) datatypes.JSON {
	if len(value) == 0 || !json.Valid(value) {
		return datatypes.JSON([]byte(`{}`))
	}
	return append(datatypes.JSON(nil), value...)
}

type AttemptLease struct {
	Attempt           models.SourceRunAttempt
	RootExecutionUnit models.SourceRunExecutionUnit
}

// CreateAttemptAndRootUnit allocates the immutable fence and the only root
// execution unit which a future dispatcher may enqueue. It is intentionally
// separate from dispatcher claiming: losing an acknowledgement never changes
// this fence or creates duplicate provider work.
func CreateAttemptAndRootUnit(db *gorm.DB, tenantID, requestID string) (AttemptLease, error) {
	if db == nil || strings.TrimSpace(tenantID) == "" {
		return AttemptLease{}, fmt.Errorf("explicit tenant and database are required")
	}
	requestPublicID, err := uuid.Parse(strings.TrimSpace(requestID))
	if err != nil {
		return AttemptLease{}, fmt.Errorf("source-run request ID is invalid")
	}
	var result AttemptLease
	err = db.Transaction(func(tx *gorm.DB) error {
		var request models.SourceRunRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", requestPublicID, tenantID).First(&request).Error; err != nil {
			return err
		}
		if IsTerminalRequest(RequestState(request.State)) {
			return fmt.Errorf("terminal source-run request cannot receive an attempt")
		}
		var active models.SourceRunAttempt
		if err := tx.Where("tenant_id = ? AND content_source_id = ? AND state IN ?", tenantID, request.ContentSourceID, []string{
			string(AttemptAuthorized), string(AttemptClaimed), string(AttemptRunning), string(AttemptVerificationRequired),
		}).First(&active).Error; err == nil {
			return fmt.Errorf("source already has provider-effecting attempt %s", active.PublicID)
		} else if err != nil && err != gorm.ErrRecordNotFound {
			return err
		}
		var count int64
		if err := tx.Model(&models.SourceRunAttempt{}).Where("tenant_id = ? AND source_run_request_id = ?", tenantID, request.PublicID).Count(&count).Error; err != nil {
			return err
		}
		attempt := models.SourceRunAttempt{
			PublicID:           uuid.New(),
			TenantID:           tenantID,
			SourceRunRequestID: request.PublicID,
			ContentSourceID:    request.ContentSourceID,
			AttemptNumber:      int(count) + 1,
			State:              string(AttemptAuthorized),
			FenceToken:         uuid.New(),
		}
		unitID := uuid.New()
		attempt.RootExecutionUnitID = &unitID
		jobID, err := DeterministicUnitJobID(tenantID, request.PublicID.String(), attempt.PublicID.String(), unitID.String(), attempt.FenceToken.String())
		if err != nil {
			return err
		}
		root := models.SourceRunExecutionUnit{
			PublicID:           unitID,
			TenantID:           tenantID,
			SourceRunRequestID: request.PublicID,
			SourceRunAttemptID: attempt.PublicID,
			ContentSourceID:    request.ContentSourceID,
			UnitType:           "coordinator",
			UnitKey:            "root",
			JobID:              jobID,
			AttemptFenceToken:  attempt.FenceToken,
			State:              string(UnitAuthorized),
		}
		if err := tx.Create(&attempt).Error; err != nil {
			return err
		}
		if err := tx.Create(&root).Error; err != nil {
			return err
		}
		if err := tx.Model(&request).Updates(map[string]any{
			"root_execution_unit_id": root.PublicID,
			"expected_unit_count":    gorm.Expr("expected_unit_count + 1"),
		}).Error; err != nil {
			return err
		}
		result = AttemptLease{Attempt: attempt, RootExecutionUnit: root}
		return nil
	})
	return result, err
}

// ClaimAttempt grants dispatcher ownership but never reissues the immutable
// attempt fence or root job ID. Claims may be recovered after lease expiry.
func ClaimAttempt(db *gorm.DB, tenantID, attemptID, owner string, leaseFor time.Duration) (models.SourceRunAttempt, error) {
	if db == nil || strings.TrimSpace(tenantID) == "" || strings.TrimSpace(owner) == "" || leaseFor <= 0 {
		return models.SourceRunAttempt{}, fmt.Errorf("tenant, owner, database, and positive lease are required")
	}
	publicID, err := uuid.Parse(strings.TrimSpace(attemptID))
	if err != nil {
		return models.SourceRunAttempt{}, fmt.Errorf("source-run attempt ID is invalid")
	}
	now := time.Now().UTC()
	var attempt models.SourceRunAttempt
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", publicID, tenantID).First(&attempt).Error; err != nil {
			return err
		}
		if IsTerminalAttempt(AttemptState(attempt.State)) || attempt.State == string(AttemptVerificationRequired) {
			return fmt.Errorf("source-run attempt cannot be dispatcher claimed from %s", attempt.State)
		}
		if attempt.State == string(AttemptRunning) && (attempt.DispatcherLeaseExpiresAt == nil || !attempt.DispatcherLeaseExpiresAt.After(now)) {
			if err := tx.Model(&attempt).Updates(map[string]any{
				"state":                    string(AttemptVerificationRequired),
				"verification_required_at": now,
			}).Error; err != nil {
				return err
			}
			if err := requireVerificationForAttempt(tx, tenantID, attempt.PublicID, "expired_dispatcher_lease"); err != nil {
				return err
			}
			return fmt.Errorf("expired running source-run attempt requires verification")
		}
		if attempt.DispatcherLeaseExpiresAt != nil && attempt.DispatcherLeaseExpiresAt.After(now) && attempt.DispatcherOwner != owner {
			return fmt.Errorf("source-run attempt is already claimed")
		}
		token := uuid.New()
		expires := now.Add(leaseFor)
		updates := map[string]any{
			"state":                       string(AttemptClaimed),
			"dispatcher_owner":            owner,
			"dispatcher_token":            token,
			"dispatcher_epoch":            gorm.Expr("dispatcher_epoch + 1"),
			"dispatcher_lease_expires_at": expires,
			"heartbeat_at":                now,
		}
		if err := tx.Model(&attempt).Updates(updates).Error; err != nil {
			return err
		}
		if err := tx.Model(&models.ContentSource{}).Where("public_id = ? AND tenant_id = ?", attempt.ContentSourceID, tenantID).Updates(map[string]any{"last_claimed_at": now}).Error; err != nil {
			return err
		}
		attempt.State = string(AttemptClaimed)
		attempt.DispatcherOwner = owner
		attempt.DispatcherToken = &token
		attempt.DispatcherEpoch++
		attempt.DispatcherLeaseExpiresAt = &expires
		attempt.HeartbeatAt = &now
		return nil
	})
	return attempt, err
}

type UnitExecutionLease struct {
	Unit       models.SourceRunExecutionUnit
	LeaseToken uuid.UUID
}

// AcquireUnitExecution is the compare-and-set unit lease. It does not mark a
// provider effect as started; the future begin-unit route performs that second
// CAS immediately before the effect.
func AcquireUnitExecution(db *gorm.DB, tenantID, unitID, owner string, leaseFor time.Duration) (UnitExecutionLease, error) {
	if db == nil || strings.TrimSpace(tenantID) == "" || strings.TrimSpace(owner) == "" || leaseFor <= 0 {
		return UnitExecutionLease{}, fmt.Errorf("tenant, owner, database, and positive lease are required")
	}
	publicID, err := uuid.Parse(strings.TrimSpace(unitID))
	if err != nil {
		return UnitExecutionLease{}, fmt.Errorf("source-run execution-unit ID is invalid")
	}
	now := time.Now().UTC()
	var leased UnitExecutionLease
	err = db.Transaction(func(tx *gorm.DB) error {
		var unit models.SourceRunExecutionUnit
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", publicID, tenantID).First(&unit).Error; err != nil {
			return err
		}
		if IsTerminalUnit(ExecutionUnitState(unit.State)) || unit.State == string(UnitVerificationRequired) || unit.CancellationRequestedAt != nil {
			return fmt.Errorf("source-run execution unit cannot be leased from %s", unit.State)
		}
		if unit.EffectStartedAt != nil && (unit.ExecutionLeaseExpiresAt == nil || !unit.ExecutionLeaseExpiresAt.After(now)) {
			if err := tx.Model(&unit).Updates(map[string]any{
				"state":                 string(UnitVerificationRequired),
				"verification_required": true,
			}).Error; err != nil {
				return err
			}
			if _, err := ensureVerificationTask(tx, unit, "expired_execution_lease"); err != nil {
				return err
			}
			if err := reconcileAttemptAndRequest(tx, tenantID, unit.SourceRunAttemptID, unit.SourceRunRequestID); err != nil {
				return err
			}
			return fmt.Errorf("expired execution unit with an effect start requires verification")
		}
		if unit.ExecutionLeaseExpiresAt != nil && unit.ExecutionLeaseExpiresAt.After(now) && unit.ExecutionOwner != owner {
			return fmt.Errorf("source-run execution unit is already leased")
		}
		token := uuid.New()
		expires := now.Add(leaseFor)
		if err := tx.Model(&unit).Updates(map[string]any{
			"state":                      string(UnitAccepted),
			"execution_owner":            owner,
			"execution_lease_token":      token,
			"execution_lease_epoch":      gorm.Expr("execution_lease_epoch + 1"),
			"execution_lease_expires_at": expires,
			"heartbeat_at":               now,
		}).Error; err != nil {
			return err
		}
		unit.State = string(UnitAccepted)
		unit.ExecutionOwner = owner
		unit.ExecutionLeaseToken = &token
		unit.ExecutionLeaseEpoch++
		unit.ExecutionLeaseExpiresAt = &expires
		unit.HeartbeatAt = &now
		leased = UnitExecutionLease{Unit: unit, LeaseToken: token}
		return nil
	})
	return leased, err
}

type ReceiptInput struct {
	TenantID            string
	ProducerEventKey    string
	SourceRunRequestID  uuid.UUID
	SourceRunAttemptID  uuid.UUID
	ExecutionUnitID     uuid.UUID
	ContentSourceID     uuid.UUID
	UnitJobID           string
	AttemptFenceToken   uuid.UUID
	ExecutionLeaseToken uuid.UUID
	SchemaVersion       string
	Producer            string
	Stage               string
	EventType           string
	Outcome             string
	Sequence            int64
	PageID              string
	BatchID             string
	FinalPage           bool
	CausationID         string
	Payload             datatypes.JSON
	PayloadDigest       string
	ProducedAt          time.Time
}

// RetainReceipt stores an immutable producer envelope before asynchronous CMS
// delivery. It validates the same live unit/fence boundary as RecordReceipt
// but intentionally does not create receipt/projection state. This gives a
// later signed action an exact, CMS-derived redelivery target without making a
// queue job ID, payload, or provider URL actionable.
func RetainReceipt(db *gorm.DB, input ReceiptInput, envelope datatypes.JSON) (models.SourceRunRetainedReceipt, bool, error) {
	if db == nil || !validReceiptInput(input) || !json.Valid(envelope) {
		return models.SourceRunRetainedReceipt{}, false, fmt.Errorf("source-run retained receipt is invalid")
	}
	now := time.Now().UTC()
	var retained models.SourceRunRetainedReceipt
	created := false
	err := db.Transaction(func(tx *gorm.DB) error {
		var unit models.SourceRunExecutionUnit
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", input.ExecutionUnitID, input.TenantID).First(&unit).Error; err != nil {
			return err
		}
		if unit.SourceRunRequestID != input.SourceRunRequestID || unit.SourceRunAttemptID != input.SourceRunAttemptID || unit.ContentSourceID != input.ContentSourceID || unit.JobID != input.UnitJobID || unit.AttemptFenceToken != input.AttemptFenceToken || unit.ExecutionLeaseToken == nil || *unit.ExecutionLeaseToken != input.ExecutionLeaseToken || unit.ExecutionLeaseExpiresAt == nil || !unit.ExecutionLeaseExpiresAt.After(now) {
			return fmt.Errorf("source-run retained receipt fence, lease, or unit identity is stale")
		}
		if err := validateReceiptUnitScope(unit, input); err != nil {
			return err
		}
		retained = models.SourceRunRetainedReceipt{PublicID: uuid.New(), TenantID: input.TenantID, ProducerEventKey: input.ProducerEventKey, SourceRunRequestID: input.SourceRunRequestID, SourceRunAttemptID: input.SourceRunAttemptID, ExecutionUnitID: input.ExecutionUnitID, PayloadDigest: strings.ToLower(input.PayloadDigest), Receipt: append(datatypes.JSON(nil), envelope...), State: "retained"}
		result := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}, {Name: "producer_event_key"}}, DoNothing: true}).Create(&retained)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return tx.Where("tenant_id = ? AND producer_event_key = ?", input.TenantID, input.ProducerEventKey).First(&retained).Error
		}
		created = true
		return nil
	})
	return retained, created, err
}

func MarkRetainedReceiptDelivered(db *gorm.DB, tenantID, producerEventKey string) (models.SourceRunRetainedReceipt, error) {
	if db == nil || strings.TrimSpace(tenantID) == "" || strings.TrimSpace(producerEventKey) == "" {
		return models.SourceRunRetainedReceipt{}, fmt.Errorf("retained receipt delivery identity is invalid")
	}
	var retained models.SourceRunRetainedReceipt
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id = ? AND producer_event_key = ?", tenantID, producerEventKey).First(&retained).Error; err != nil {
			return err
		}
		var receipt models.SourceRunReceipt
		if err := tx.Where("tenant_id = ? AND producer_event_key = ?", tenantID, producerEventKey).First(&receipt).Error; err != nil {
			return err
		}
		if retained.ExecutionUnitID != receipt.ExecutionUnitID || retained.PayloadDigest != receipt.PayloadDigest {
			return fmt.Errorf("retained receipt does not match the CMS receipt ledger")
		}
		if err := tx.Model(&retained).Updates(map[string]any{"state": "delivered", "delivered_receipt_id": receipt.PublicID}).Error; err != nil {
			return err
		}
		retained.State, retained.DeliveredReceiptID = "delivered", &receipt.PublicID
		return nil
	})
	return retained, err
}

func validReceiptInput(input ReceiptInput) bool {
	return strings.TrimSpace(input.TenantID) != "" && strings.TrimSpace(input.ProducerEventKey) != "" && strings.TrimSpace(input.UnitJobID) != "" && strings.TrimSpace(input.SchemaVersion) != "" && strings.TrimSpace(input.Producer) != "" && strings.TrimSpace(input.Stage) != "" && strings.TrimSpace(input.EventType) != "" && strings.TrimSpace(input.Outcome) != "" && strings.TrimSpace(input.PayloadDigest) != "" && input.SourceRunRequestID != uuid.Nil && input.SourceRunAttemptID != uuid.Nil && input.ExecutionUnitID != uuid.Nil && input.ContentSourceID != uuid.Nil && input.AttemptFenceToken != uuid.Nil && input.ExecutionLeaseToken != uuid.Nil && !input.ProducedAt.IsZero() && input.Sequence >= 0 && IsAllowedReceipt(ReceiptStage(input.Stage), ReceiptEvent(input.EventType)) && IsKnownOutcome(SourceRunOutcome(input.Outcome))
}

// RecordReceipt first preserves authenticated producer evidence and queues
// projection work in the same transaction. A duplicate producer event is a
// successful no-op, while any stale lease/fence is rejected before telemetry
// or current-state reducers can be changed.
func RecordReceipt(db *gorm.DB, input ReceiptInput) (models.SourceRunReceipt, bool, error) {
	return recordReceipt(db, input, false)
}

// recordReceipt is shared by first delivery and exact retained-envelope
// recovery. Retained recovery may outlive a lease expiry, but it still binds
// to the original immutable lease token and attempt fence; a newer owner can
// never replay evidence from a superseded lease.
func recordReceipt(db *gorm.DB, input ReceiptInput, allowExpiredLease bool) (models.SourceRunReceipt, bool, error) {
	if db == nil || strings.TrimSpace(input.TenantID) == "" || strings.TrimSpace(input.ProducerEventKey) == "" || strings.TrimSpace(input.UnitJobID) == "" || strings.TrimSpace(input.SchemaVersion) == "" || strings.TrimSpace(input.Producer) == "" || strings.TrimSpace(input.Stage) == "" || strings.TrimSpace(input.EventType) == "" || strings.TrimSpace(input.Outcome) == "" || strings.TrimSpace(input.PayloadDigest) == "" {
		return models.SourceRunReceipt{}, false, fmt.Errorf("source-run receipt is missing required protocol fields")
	}
	if input.SourceRunRequestID == uuid.Nil || input.SourceRunAttemptID == uuid.Nil || input.ExecutionUnitID == uuid.Nil || input.ContentSourceID == uuid.Nil || input.AttemptFenceToken == uuid.Nil || input.ExecutionLeaseToken == uuid.Nil {
		return models.SourceRunReceipt{}, false, fmt.Errorf("source-run receipt is missing required identities")
	}
	if input.ProducedAt.IsZero() {
		return models.SourceRunReceipt{}, false, fmt.Errorf("source-run receipt produced_at is required")
	}
	if input.Sequence < 0 || !IsAllowedReceipt(ReceiptStage(input.Stage), ReceiptEvent(input.EventType)) || !IsKnownOutcome(SourceRunOutcome(input.Outcome)) {
		return models.SourceRunReceipt{}, false, fmt.Errorf("source-run receipt stage, event, or outcome is not admitted")
	}
	if len(input.Payload) == 0 {
		input.Payload = datatypes.JSON([]byte(`{}`))
	}
	now := time.Now().UTC()
	receipt := models.SourceRunReceipt{}
	created := false
	err := db.Transaction(func(tx *gorm.DB) error {
		var unit models.SourceRunExecutionUnit
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", input.ExecutionUnitID, input.TenantID).First(&unit).Error; err != nil {
			return err
		}
		if unit.SourceRunRequestID != input.SourceRunRequestID || unit.SourceRunAttemptID != input.SourceRunAttemptID || unit.ContentSourceID != input.ContentSourceID || unit.JobID != input.UnitJobID || unit.AttemptFenceToken != input.AttemptFenceToken || unit.ExecutionLeaseToken == nil || *unit.ExecutionLeaseToken != input.ExecutionLeaseToken || unit.ExecutionLeaseExpiresAt == nil || (!allowExpiredLease && !unit.ExecutionLeaseExpiresAt.After(now)) {
			return fmt.Errorf("source-run receipt fence, lease, or unit identity is stale")
		}
		if err := validateReceiptUnitScope(unit, input); err != nil {
			return err
		}
		receipt = models.SourceRunReceipt{
			PublicID:            uuid.New(),
			TenantID:            input.TenantID,
			ProducerEventKey:    input.ProducerEventKey,
			SourceRunRequestID:  input.SourceRunRequestID,
			SourceRunAttemptID:  input.SourceRunAttemptID,
			ExecutionUnitID:     input.ExecutionUnitID,
			ContentSourceID:     input.ContentSourceID,
			UnitJobID:           input.UnitJobID,
			AttemptFenceToken:   input.AttemptFenceToken,
			ExecutionLeaseToken: input.ExecutionLeaseToken,
			SchemaVersion:       input.SchemaVersion,
			Producer:            input.Producer,
			Stage:               input.Stage,
			EventType:           input.EventType,
			Outcome:             input.Outcome,
			Sequence:            input.Sequence,
			PageID:              input.PageID,
			BatchID:             input.BatchID,
			FinalPage:           input.FinalPage,
			CausationID:         input.CausationID,
			Payload:             input.Payload,
			PayloadDigest:       input.PayloadDigest,
			ProducedAt:          input.ProducedAt.UTC(),
			ObservedAt:          now,
		}
		result := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "tenant_id"}, {Name: "producer_event_key"}},
			DoNothing: true,
		}).Create(&receipt)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return tx.Where("tenant_id = ? AND producer_event_key = ?", input.TenantID, input.ProducerEventKey).First(&receipt).Error
		}
		created = true
		work := models.SourceRunProjectionWork{
			PublicID:       uuid.New(),
			TenantID:       input.TenantID,
			EvidenceKind:   "receipt",
			EvidenceID:     receipt.PublicID,
			ReducerVersion: projectionReducerVersion,
			State:          "queued",
		}
		return tx.Create(&work).Error
	})
	return receipt, created, err
}

type retainedReceiptEnvelope struct {
	TenantID            string         `json:"tenantId"`
	ProducerEventKey    string         `json:"producerEventKey"`
	SourceRunRequestID  string         `json:"sourceRunRequestId"`
	SourceRunAttemptID  string         `json:"sourceRunAttemptId"`
	ExecutionUnitID     string         `json:"executionUnitId"`
	ContentSourceID     string         `json:"contentSourceId"`
	UnitJobID           string         `json:"unitJobId"`
	AttemptFenceToken   string         `json:"attemptFenceToken"`
	ExecutionLeaseToken string         `json:"executionLeaseToken"`
	SchemaVersion       string         `json:"schemaVersion"`
	Producer            string         `json:"producer"`
	Stage               string         `json:"stage"`
	EventType           string         `json:"eventType"`
	Outcome             string         `json:"outcome"`
	Sequence            int64          `json:"sequence"`
	PageID              string         `json:"pageId"`
	BatchID             string         `json:"batchId"`
	FinalPage           bool           `json:"finalPage"`
	CausationID         string         `json:"causationId"`
	ProducedAt          string         `json:"producedAt"`
	Payload             datatypes.JSON `json:"payload"`
	PayloadDigest       string         `json:"payloadDigest"`
}

func RecordRetainedReceipt(db *gorm.DB, retained models.SourceRunRetainedReceipt) (models.SourceRunReceipt, bool, error) {
	var envelope retainedReceiptEnvelope
	if err := json.Unmarshal(retained.Receipt, &envelope); err != nil {
		return models.SourceRunReceipt{}, false, fmt.Errorf("retained receipt envelope is malformed")
	}
	parseID := func(raw string) (uuid.UUID, error) { return uuid.Parse(strings.TrimSpace(raw)) }
	requestID, err := parseID(envelope.SourceRunRequestID)
	if err != nil {
		return models.SourceRunReceipt{}, false, err
	}
	attemptID, err := parseID(envelope.SourceRunAttemptID)
	if err != nil {
		return models.SourceRunReceipt{}, false, err
	}
	unitID, err := parseID(envelope.ExecutionUnitID)
	if err != nil {
		return models.SourceRunReceipt{}, false, err
	}
	sourceID, err := parseID(envelope.ContentSourceID)
	if err != nil {
		return models.SourceRunReceipt{}, false, err
	}
	fence, err := parseID(envelope.AttemptFenceToken)
	if err != nil {
		return models.SourceRunReceipt{}, false, err
	}
	lease, err := parseID(envelope.ExecutionLeaseToken)
	if err != nil {
		return models.SourceRunReceipt{}, false, err
	}
	producedAt, err := time.Parse(time.RFC3339, envelope.ProducedAt)
	if err != nil {
		return models.SourceRunReceipt{}, false, err
	}
	input := ReceiptInput{TenantID: envelope.TenantID, ProducerEventKey: envelope.ProducerEventKey, SourceRunRequestID: requestID, SourceRunAttemptID: attemptID, ExecutionUnitID: unitID, ContentSourceID: sourceID, UnitJobID: envelope.UnitJobID, AttemptFenceToken: fence, ExecutionLeaseToken: lease, SchemaVersion: envelope.SchemaVersion, Producer: envelope.Producer, Stage: envelope.Stage, EventType: envelope.EventType, Outcome: envelope.Outcome, Sequence: envelope.Sequence, PageID: envelope.PageID, BatchID: envelope.BatchID, FinalPage: envelope.FinalPage, CausationID: envelope.CausationID, Payload: envelope.Payload, PayloadDigest: envelope.PayloadDigest, ProducedAt: producedAt}
	if retained.TenantID != input.TenantID || retained.ProducerEventKey != input.ProducerEventKey || retained.SourceRunRequestID != input.SourceRunRequestID || retained.SourceRunAttemptID != input.SourceRunAttemptID || retained.ExecutionUnitID != input.ExecutionUnitID || retained.PayloadDigest != input.PayloadDigest {
		return models.SourceRunReceipt{}, false, fmt.Errorf("retained receipt identity does not match its envelope")
	}
	return recordReceipt(db, input, true)
}

func validateReceiptUnitScope(unit models.SourceRunExecutionUnit, input ReceiptInput) error {
	switch ReceiptStage(input.Stage) {
	case ReceiptStageDispatch:
		if unit.UnitType != "coordinator" || input.PageID != "" || input.BatchID != "" || input.FinalPage {
			return fmt.Errorf("dispatch receipt does not match its coordinator unit")
		}
	case ReceiptStageFetch:
		if unit.UnitType != "coordinator" && unit.UnitType != "fetch_page" {
			return fmt.Errorf("fetch receipt does not match its execution unit")
		}
		if unit.PageID != "" && input.PageID != "" && unit.PageID != input.PageID {
			return fmt.Errorf("fetch receipt page does not match its execution unit")
		}
		if input.BatchID != "" {
			return fmt.Errorf("fetch receipt cannot claim a normalize batch")
		}
	case ReceiptStageNormalize:
		if unit.UnitType != "normalize_batch" || unit.PageID != input.PageID || unit.BatchID != input.BatchID || input.FinalPage {
			return fmt.Errorf("normalize receipt does not match its execution unit")
		}
	case ReceiptStageDelivery:
		if unit.UnitType != "normalize_batch" || input.PageID != unit.PageID || input.BatchID != unit.BatchID || input.FinalPage {
			return fmt.Errorf("delivery receipt does not match its execution unit")
		}
	default:
		return fmt.Errorf("source-run receipt stage is not admitted")
	}
	if input.FinalPage && input.EventType != string(ReceiptEventProviderTerminal) {
		return fmt.Errorf("only provider-terminal receipts may mark a final page")
	}
	if unit.UnitType == "fetch_page" && isTerminalReceiptEvent(ReceiptEvent(input.EventType)) && !isDigest(unit.DeclaredChildDigest) {
		return fmt.Errorf("fetch-page receipt cannot terminalize before the child manifest is frozen")
	}
	return nil
}

func isTerminalReceiptEvent(event ReceiptEvent) bool {
	switch event {
	case ReceiptEventProviderTerminal, ReceiptEventNormalizeTerminal, ReceiptEventFinalization, ReceiptEventFailed, ReceiptEventCancelled, ReceiptEventDLQ:
		return true
	default:
		return false
	}
}

func validRequester(requestedBy string) bool {
	switch strings.TrimSpace(requestedBy) {
	case "approval_handoff", "manual", "schedule", "system":
		return true
	default:
		return false
	}
}

func timePtr(value time.Time) *time.Time { return &value }

func utcTimePtr(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := value.UTC()
	return &copy
}
