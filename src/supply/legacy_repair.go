package supply

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"content-management-system/src/models"

	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const legacyRepairRecentUncertainty = 2 * time.Hour

type LegacySourceRunRepairAction struct {
	RequestID       uuid.UUID `json:"request_id"`
	TenantID        string    `json:"tenant_id"`
	ContentSourceID uuid.UUID `json:"content_source_id"`
	ExpectedState   string    `json:"expected_state"`
	ExpectedUpdated time.Time `json:"expected_updated_at"`
	TargetState     string    `json:"target_state"`
	EvidenceState   string    `json:"evidence_state"`
	Reason          string    `json:"reason"`
}

type LegacySourceRunRepairReport struct {
	SchemaVersion         string                        `json:"schema_version"`
	GeneratedAt           time.Time                     `json:"generated_at"`
	ActiveRequests        int                           `json:"active_requests"`
	DuplicateGroups       int                           `json:"duplicate_groups"`
	RetainedUncertain     int                           `json:"retained_uncertain"`
	TerminalEvidenceCount int                           `json:"terminal_evidence_count"`
	Actions               []LegacySourceRunRepairAction `json:"actions"`
	VerificationDigest    string                        `json:"verification_digest"`
}

// PreviewLegacySourceRunRepair is read-only. It retains at most one recent
// uncertain request per source, reconciles immutable terminal evidence when it
// exists, and expires only stale/duplicate rows whose authority is unknown.
func PreviewLegacySourceRunRepair(db *gorm.DB, now time.Time) (LegacySourceRunRepairReport, error) {
	report := LegacySourceRunRepairReport{SchemaVersion: "legacy-source-run-repair/v1", GeneratedAt: now.UTC(), Actions: []LegacySourceRunRepairAction{}}
	if db == nil || !db.Migrator().HasTable(&models.SourceRunRequest{}) {
		return report, fmt.Errorf("source-run request schema is unavailable")
	}
	var requests []models.SourceRunRequest
	if err := db.Where("state IN ?", models.SourceRunActiveStates).
		Order("tenant_id, content_source_id, updated_at DESC, public_id DESC").Find(&requests).Error; err != nil {
		return report, err
	}
	terminalEvidence, err := loadLegacyTerminalEvidence(db, requests)
	if err != nil {
		return report, err
	}
	report.ActiveRequests = len(requests)
	groups := make(map[string][]models.SourceRunRequest)
	for _, request := range requests {
		key := request.TenantID + ":" + request.ContentSourceID.String()
		groups[key] = append(groups[key], request)
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		group := groups[key]
		if len(group) > 1 {
			report.DuplicateGroups++
		}
		retained := false
		for index, request := range group {
			target, evidence, reason := "", "", ""
			terminalState, hasTerminalEvidence := terminalEvidence[request.ID]
			if hasTerminalEvidence {
				target, evidence, reason = terminalState, "terminal_event", "immutable terminal source-run event"
				report.TerminalEvidenceCount++
			} else {
				deadlinePassed := request.DeadlineAt != nil && !request.DeadlineAt.After(now)
				expiryPassed := request.ExpiresAt != nil && !request.ExpiresAt.After(now)
				stale := deadlinePassed || expiryPassed || request.UpdatedAt.Before(now.Add(-legacyRepairRecentUncertainty))
				if index == 0 && !stale && !retained {
					retained = true
					report.RetainedUncertain++
					continue
				}
				target, evidence = models.SourceRunExpired, "unknown"
				if index > 0 {
					reason = "duplicate active legacy request"
				} else {
					reason = "stale active legacy request"
				}
			}
			report.Actions = append(report.Actions, LegacySourceRunRepairAction{
				RequestID: request.PublicID, TenantID: request.TenantID, ContentSourceID: request.ContentSourceID,
				ExpectedState: request.State, ExpectedUpdated: request.UpdatedAt.UTC(), TargetState: target,
				EvidenceState: evidence, Reason: reason,
			})
		}
	}
	report.VerificationDigest = legacyRepairDigest(report)
	return report, nil
}

func loadLegacyTerminalEvidence(db *gorm.DB, requests []models.SourceRunRequest) (map[uint]string, error) {
	evidence := make(map[uint]string)
	if len(requests) == 0 || !db.Migrator().HasTable(&models.ContentProcessingEvent{}) {
		return evidence, nil
	}
	requestIDs := make([]uint, 0, len(requests))
	for _, request := range requests {
		requestIDs = append(requestIDs, request.ID)
	}
	var events []models.ContentProcessingEvent
	if err := db.Where("source_run_request_id IN ? AND state IN ?", requestIDs,
		[]string{models.SourceRunCompleted, models.SourceRunSucceeded, models.SourceRunFailed, models.SourceRunCancelled, models.SourceRunExpired}).
		Order("source_run_request_id ASC, occurred_at DESC, id DESC").Find(&events).Error; err != nil {
		return nil, err
	}
	for _, event := range events {
		if event.SourceRunRequestID == nil {
			continue
		}
		if _, exists := evidence[*event.SourceRunRequestID]; !exists {
			evidence[*event.SourceRunRequestID] = event.State
		}
	}
	return evidence, nil
}

func legacyRepairDigest(report LegacySourceRunRepairReport) string {
	payload := struct {
		SchemaVersion string                        `json:"schema_version"`
		Actions       []LegacySourceRunRepairAction `json:"actions"`
	}{report.SchemaVersion, report.Actions}
	raw, _ := json.Marshal(payload)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// ApplyLegacySourceRunRepair re-previews and updates every row using exact
// expected state and updated_at comparisons. Concurrent progress rolls back.
func ApplyLegacySourceRunRepair(db *gorm.DB, expectedDigest, actor string, now time.Time) (LegacySourceRunRepairReport, error) {
	expectedDigest, actor = strings.TrimSpace(expectedDigest), strings.TrimSpace(actor)
	if expectedDigest == "" || actor == "" {
		return LegacySourceRunRepairReport{}, fmt.Errorf("repair apply requires digest and actor")
	}
	report, err := PreviewLegacySourceRunRepair(db, now)
	if err != nil {
		return report, err
	}
	if report.VerificationDigest != expectedDigest {
		return report, fmt.Errorf("repair preview digest changed; preview again")
	}
	err = db.Transaction(func(tx *gorm.DB) error {
		for _, action := range report.Actions {
			var request models.SourceRunRequest
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ? AND tenant_id = ?", action.RequestID, action.TenantID).First(&request).Error; err != nil {
				return err
			}
			if request.State != action.ExpectedState || !request.UpdatedAt.UTC().Equal(action.ExpectedUpdated) {
				return fmt.Errorf("source-run request %s changed after preview", action.RequestID)
			}
			finished := now.UTC()
			result := tx.Model(&models.SourceRunRequest{}).
				Where("public_id = ? AND tenant_id = ? AND state = ? AND updated_at = ?", action.RequestID, action.TenantID, action.ExpectedState, action.ExpectedUpdated).
				Updates(map[string]any{"state": action.TargetState, "evidence_state": action.EvidenceState, "finished_at": finished, "failure_class": "legacy_state_reconciled", "failure_summary": action.Reason})
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return fmt.Errorf("source-run request %s changed during repair", action.RequestID)
			}
			payload, _ := json.Marshal(map[string]any{"schema_version": report.SchemaVersion, "reason": action.Reason, "evidence_state": action.EvidenceState, "preview_digest": expectedDigest, "actor": actor})
			if err := tx.Create(&models.ContentProcessingEvent{
				TenantID: action.TenantID, ContentSourceID: &action.ContentSourceID, SourceRunRequestID: &request.ID,
				Stage: "source_run.repair", State: action.TargetState, Producer: "cms", EventClass: "legacy_source_run_repaired",
				Payload: payload, OccurredAt: finished,
			}).Error; err != nil {
				return err
			}
		}
		return nil
	})
	return report, err
}
