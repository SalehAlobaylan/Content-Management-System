package supply

import (
	"fmt"
	"strings"
	"time"

	"content-management-system/src/models"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const maxProjectionAttempts = 12

// ProjectionLease is a short, fenced lease over immutable evidence. Applying
// a projection never contacts a provider; a crash leaves the source evidence
// intact and the work item reclaimable.
type ProjectionLease struct {
	Work       models.SourceRunProjectionWork
	ClaimToken uuid.UUID
}

func ClaimNextProjectionWork(db *gorm.DB, owner string, leaseFor time.Duration) (ProjectionLease, bool, error) {
	if db == nil || strings.TrimSpace(owner) == "" || leaseFor <= 0 {
		return ProjectionLease{}, false, fmt.Errorf("projection owner, database, and positive lease are required")
	}
	now := time.Now().UTC()
	var lease ProjectionLease
	err := db.Transaction(func(tx *gorm.DB) error {
		var work models.SourceRunProjectionWork
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Where("(state = ? OR (state = ? AND claim_expires_at <= ?) OR (state = ? AND attempt_count < ?))", "queued", "claimed", now, "failed", maxProjectionAttempts).Order("created_at ASC").First(&work).Error; err != nil {
			if err == gorm.ErrRecordNotFound {
				return nil
			}
			return err
		}
		token, expires := uuid.New(), now.Add(leaseFor)
		if err := tx.Model(&work).Updates(map[string]any{"state": "claimed", "claim_owner": owner, "claim_token": token, "claim_expires_at": expires, "attempt_count": gorm.Expr("attempt_count + 1"), "error_summary": ""}).Error; err != nil {
			return err
		}
		work.State, work.ClaimOwner, work.ClaimToken, work.ClaimExpiresAt, work.AttemptCount = "claimed", owner, &token, &expires, work.AttemptCount+1
		lease = ProjectionLease{Work: work, ClaimToken: token}
		return nil
	})
	if err != nil || lease.Work.PublicID == uuid.Nil {
		return lease, false, err
	}
	return lease, true, nil
}

func CompleteProjectionWork(db *gorm.DB, tenantID, workID, owner, claimToken string) error {
	return terminalizeProjectionWork(db, tenantID, workID, owner, claimToken, true, "")
}

func FailProjectionWork(db *gorm.DB, tenantID, workID, owner, claimToken, summary string) error {
	return terminalizeProjectionWork(db, tenantID, workID, owner, claimToken, false, summary)
}

func terminalizeProjectionWork(db *gorm.DB, tenantID, workID, owner, claimToken string, succeeded bool, summary string) error {
	if db == nil || strings.TrimSpace(tenantID) == "" || strings.TrimSpace(owner) == "" {
		return fmt.Errorf("projection completion identity is incomplete")
	}
	publicID, err := uuid.Parse(strings.TrimSpace(workID))
	if err != nil {
		return fmt.Errorf("projection work ID is invalid")
	}
	token, err := uuid.Parse(strings.TrimSpace(claimToken))
	if err != nil {
		return fmt.Errorf("projection claim token is invalid")
	}
	now := time.Now().UTC()
	updates := map[string]any{"state": "failed", "error_summary": truncateProjectionError(summary), "claim_expires_at": now}
	if succeeded {
		updates = map[string]any{"state": "succeeded", "projected_at": now, "error_summary": "", "claim_expires_at": now}
	}
	result := db.Model(&models.SourceRunProjectionWork{}).Where("public_id = ? AND tenant_id = ? AND state = ? AND claim_owner = ? AND claim_token = ? AND claim_expires_at > ?", publicID, tenantID, "claimed", owner, token, now).Updates(updates)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("projection lease is no longer current")
	}
	return nil
}

// ApplyProjectionLease deterministically rebuilds one current-state fragment
// from immutable source evidence, then marks only the exact work lease done.
func ApplyProjectionLease(db *gorm.DB, lease ProjectionLease, owner string) error {
	if db == nil || lease.Work.PublicID == uuid.Nil || strings.TrimSpace(owner) == "" || lease.ClaimToken == uuid.Nil || lease.Work.TenantID == "" {
		return fmt.Errorf("projection lease is incomplete")
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var work models.SourceRunProjectionWork
		now := time.Now().UTC()
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", lease.Work.PublicID, lease.Work.TenantID).First(&work).Error; err != nil {
			return err
		}
		if work.State != "claimed" || work.ClaimOwner != owner || work.ClaimToken == nil || *work.ClaimToken != lease.ClaimToken || work.ClaimExpiresAt == nil || !work.ClaimExpiresAt.After(now) {
			return fmt.Errorf("projection lease is stale")
		}
		var requestID uuid.UUID
		switch work.EvidenceKind {
		case "receipt":
			var receipt models.SourceRunReceipt
			if err := tx.Where("public_id = ? AND tenant_id = ?", work.EvidenceID, work.TenantID).First(&receipt).Error; err != nil {
				return err
			}
			if err := applyReceiptProjection(tx, receipt); err != nil {
				return err
			}
			requestID = receipt.SourceRunRequestID
		case "reconciliation_event":
			var event models.SourceRunReconciliationEvent
			if err := tx.Where("public_id = ? AND tenant_id = ?", work.EvidenceID, work.TenantID).First(&event).Error; err != nil {
				return err
			}
			if err := applyReconciliationProjection(tx, event); err != nil {
				return err
			}
			requestID = event.SourceRunRequestID
		case "upstream_observation_event":
			var event models.SourceUpstreamObservationEvent
			if err := tx.Where("public_id = ? AND tenant_id = ?", work.EvidenceID, work.TenantID).First(&event).Error; err != nil {
				return err
			}
			observation, err := applyUpstreamObservationProjection(tx, event)
			if err != nil {
				return err
			}
			if observation.SourceRunRequestID != nil {
				requestID = *observation.SourceRunRequestID
			}
		default:
			return fmt.Errorf("projection evidence kind %q is not admitted", work.EvidenceKind)
		}
		if err := tx.Model(&work).Updates(map[string]any{"state": "succeeded", "projected_at": now, "error_summary": "", "claim_expires_at": now}).Error; err != nil {
			return err
		}
		if requestID == uuid.Nil {
			return nil
		}
		return reconcileRequestAfterProjection(tx, work.TenantID, requestID)
	})
}

func applyUpstreamObservationProjection(tx *gorm.DB, event models.SourceUpstreamObservationEvent) (models.SourceUpstreamObservation, error) {
	var observation models.SourceUpstreamObservation
	if err := tx.Where("public_id=? AND tenant_id=?", event.ObservationID, event.TenantID).First(&observation).Error; err != nil {
		return observation, err
	}
	disposition := models.SourceUpstreamObservationDisposition{
		PublicID: uuid.New(), TenantID: event.TenantID, ObservationID: observation.PublicID,
		Disposition: event.EventType, LatestEventID: event.PublicID, LatestEventAt: event.OccurredAt,
		ReplayUntil: observation.ReplayUntil, UpdatedAt: time.Now().UTC(),
	}
	if err := tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "tenant_id"}, {Name: "observation_id"}},
		DoUpdates: clause.Assignments(map[string]any{"disposition": event.EventType, "latest_event_id": event.PublicID, "latest_event_at": event.OccurredAt, "replay_until": observation.ReplayUntil, "updated_at": time.Now().UTC()}),
		Where:     clause.Where{Exprs: []clause.Expression{clause.Expr{SQL: "source_upstream_observation_dispositions.latest_event_at < EXCLUDED.latest_event_at OR (source_upstream_observation_dispositions.latest_event_at = EXCLUDED.latest_event_at AND source_upstream_observation_dispositions.latest_event_id::text < EXCLUDED.latest_event_id::text)"}}},
	}).Create(&disposition).Error; err != nil {
		return observation, err
	}
	if event.EventType == "deferred" {
		if err := tx.Model(&models.ContentSource{}).Where("tenant_id=? AND public_id=?", observation.TenantID, observation.ContentSourceID).Updates(map[string]any{"last_upstream_observed_at": observation.ObservedAt}).Error; err != nil {
			return observation, err
		}
	}
	return observation, nil
}

func reconcileRequestAfterProjection(tx *gorm.DB, tenantID string, requestID uuid.UUID) error {
	var attempts []models.SourceRunAttempt
	if err := tx.Where("tenant_id = ? AND source_run_request_id = ?", tenantID, requestID).Find(&attempts).Error; err != nil {
		return err
	}
	for _, attempt := range attempts {
		if err := reconcileAttemptAndRequest(tx, tenantID, attempt.PublicID, requestID); err != nil {
			return err
		}
	}
	return nil
}

func applyReceiptProjection(tx *gorm.DB, receipt models.SourceRunReceipt) error {
	var unit models.SourceRunExecutionUnit
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", receipt.ExecutionUnitID, receipt.TenantID).First(&unit).Error; err != nil {
		return err
	}
	if IsTerminalUnit(ExecutionUnitState(unit.State)) {
		return nil
	}
	now := receipt.ObservedAt.UTC()
	if receipt.EventType == string(ReceiptEventProviderRequestStarted) {
		if err := tx.Model(&models.ContentSource{}).Where("public_id = ? AND tenant_id = ?", receipt.ContentSourceID, receipt.TenantID).Updates(map[string]any{"last_attempted_at": now}).Error; err != nil {
			return err
		}
	}
	terminal := terminalUnitState(ReceiptEvent(receipt.EventType), SourceRunOutcome(receipt.Outcome))
	if terminal == "" {
		return nil
	}
	if terminal == UnitVerificationRequired {
		if err := tx.Model(&unit).Updates(map[string]any{"state": string(UnitVerificationRequired), "verification_required": true}).Error; err != nil {
			return err
		}
		if _, err := ensureVerificationTask(tx, unit, "unknown_terminal_receipt"); err != nil {
			return err
		}
		return reconcileAttemptAndRequest(tx, receipt.TenantID, unit.SourceRunAttemptID, unit.SourceRunRequestID)
	}
	updates := map[string]any{"state": string(terminal), "terminal_outcome": receipt.Outcome, "finished_at": now, "verification_required": false}
	if terminal == UnitSucceeded && (receipt.EventType == string(ReceiptEventProviderTerminal) || receipt.EventType == string(ReceiptEventFinalization)) {
		updates["terminal_outcome"] = receipt.Outcome
		if err := applySourceCheckpoint(tx, receipt); err != nil {
			return err
		}
	}
	if err := tx.Model(&unit).Updates(updates).Error; err != nil {
		return err
	}
	if terminal == UnitFailed {
		if receipt.Stage == string(ReceiptStageFetch) && (receipt.Outcome == string(OutcomeProviderFailed) || receipt.Outcome == string(OutcomeDeadLettered)) {
			if err := applySourceFailureCheckpoint(tx, receipt); err != nil {
				return err
			}
		}
	}
	if err := rebuildRequestCounters(tx, receipt.TenantID, unit.SourceRunRequestID); err != nil {
		return err
	}
	return reconcileAttemptAndRequest(tx, receipt.TenantID, unit.SourceRunAttemptID, unit.SourceRunRequestID)
}

func applySourceFailureCheckpoint(tx *gorm.DB, receipt models.SourceRunReceipt) error {
	var source models.ContentSource
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id=? AND tenant_id=?", receipt.ContentSourceID, receipt.TenantID).First(&source).Error; err != nil {
		return err
	}
	streak := source.FailureStreak + 1
	backoff := 5 * time.Minute
	for index := 1; index < streak && backoff < 6*time.Hour; index++ {
		backoff *= 2
	}
	if backoff > 6*time.Hour {
		backoff = 6 * time.Hour
	}
	observedAt := receipt.ProducedAt.UTC()
	updates := map[string]any{"failure_streak": streak, "next_due_at": observedAt.Add(backoff)}
	if streak >= 3 {
		updates["intake_circuit_until"] = observedAt.Add(backoff)
	}
	return tx.Model(&source).Updates(updates).Error
}

func applyReconciliationProjection(tx *gorm.DB, event models.SourceRunReconciliationEvent) error {
	if event.ExecutionUnitID == nil {
		return fmt.Errorf("reconciliation event is missing an execution unit")
	}
	var unit models.SourceRunExecutionUnit
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", *event.ExecutionUnitID, event.TenantID).First(&unit).Error; err != nil {
		return err
	}
	if event.Verdict == string(VerdictUnknown) {
		return reconcileAttemptAndRequest(tx, event.TenantID, unit.SourceRunAttemptID, unit.SourceRunRequestID)
	}
	return rebuildRequestCounters(tx, event.TenantID, unit.SourceRunRequestID)
}

func terminalUnitState(event ReceiptEvent, outcome SourceRunOutcome) ExecutionUnitState {
	switch event {
	case ReceiptEventFailed, ReceiptEventDLQ:
		return UnitFailed
	case ReceiptEventCancelled:
		return UnitCancelled
	case ReceiptEventProviderTerminal, ReceiptEventNormalizeTerminal, ReceiptEventFinalization:
		switch outcome {
		case OutcomeUnknown:
			return UnitVerificationRequired
		case OutcomeProviderFailed, OutcomeDeadLettered:
			return UnitFailed
		case OutcomeCancelled:
			return UnitCancelled
		default:
			return UnitSucceeded
		}
	default:
		return ""
	}
}

func applySourceCheckpoint(tx *gorm.DB, receipt models.SourceRunReceipt) error {
	observedAt := receipt.ProducedAt.UTC()
	var source models.ContentSource
	if err := tx.Where("public_id = ? AND tenant_id = ?", receipt.ContentSourceID, receipt.TenantID).First(&source).Error; err != nil {
		return err
	}
	if receipt.EventType == string(ReceiptEventFinalization) {
		return tx.Model(&source).Updates(map[string]any{"last_delivery_verified_at": observedAt}).Error
	}
	interval := source.FetchIntervalMinutes
	if interval < 1 {
		interval = 60
	}
	updates := map[string]any{"last_provider_success_at": observedAt, "failure_streak": 0, "intake_circuit_until": nil, "next_due_at": observedAt.Add(time.Duration(interval) * time.Minute)}
	switch SourceRunOutcome(receipt.Outcome) {
	case OutcomeNewItems:
		updates["last_new_item_at"] = observedAt
	case OutcomeNoChange:
		updates["last_no_change_at"] = observedAt
	}
	return tx.Model(&source).Updates(updates).Error
}

func rebuildRequestCounters(tx *gorm.DB, tenantID string, requestID uuid.UUID) error {
	var units []models.SourceRunExecutionUnit
	if err := tx.Where("tenant_id = ? AND source_run_request_id = ?", tenantID, requestID).Find(&units).Error; err != nil {
		return err
	}
	completed, pages, batches := 0, 0, 0
	for _, unit := range units {
		if IsTerminalUnit(ExecutionUnitState(unit.State)) {
			completed++
		}
		if unit.UnitType == "fetch_page" && IsTerminalUnit(ExecutionUnitState(unit.State)) {
			pages++
		}
		if unit.UnitType == "normalize_batch" && IsTerminalUnit(ExecutionUnitState(unit.State)) {
			batches++
		}
	}
	return tx.Model(&models.SourceRunRequest{}).Where("public_id = ? AND tenant_id = ?", requestID, tenantID).Updates(map[string]any{"completed_unit_count": completed, "completed_page_count": pages, "completed_batch_count": batches}).Error
}

func truncateProjectionError(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 1000 {
		return value[:1000]
	}
	return value
}
