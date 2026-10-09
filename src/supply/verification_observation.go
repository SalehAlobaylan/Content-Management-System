package supply

import (
	"content-management-system/src/feedcontract"
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

// VerificationObservation is a bounded CMS read model for one exact,
// Aggregation-claimed verification task. It contains no provider credentials,
// source URL, arbitrary query, or browser state.
type VerificationObservation struct {
	TaskID, TenantID, RequestID, AttemptID, UnitID, ContentSourceID string
	UnitType                                                        string
	Verdict                                                         VerificationVerdict
	VerifiedOutcome                                                 SourceRunOutcome
	EvidenceSnapshot, ProvenanceDigest                              string
	Payload                                                         datatypes.JSON
}

// ClaimNextVerificationTask selects a task globally but derives tenant scope
// solely from the durable row. Claiming is read-only verification work and
// cannot select or revive a source-run execution unit.
func ClaimNextVerificationTask(db *gorm.DB, owner string, leaseFor time.Duration) (VerificationLease, bool, error) {
	if db == nil || strings.TrimSpace(owner) == "" || leaseFor <= 0 {
		return VerificationLease{}, false, fmt.Errorf("verification owner, database, and positive lease are required")
	}
	now := time.Now().UTC()
	var lease VerificationLease
	err := db.Transaction(func(tx *gorm.DB) error {
		var task models.SourceRunVerificationTask
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("(state = ? OR (state IN ? AND claim_expires_at <= ?)) AND (not_before_at IS NULL OR not_before_at <= ?)", models.SourceRunVerificationTaskQueued, []string{models.SourceRunVerificationTaskClaimed, models.SourceRunVerificationTaskRunning}, now, now).
			Order("created_at ASC").First(&task).Error; err != nil {
			if err == gorm.ErrRecordNotFound {
				return nil
			}
			return err
		}
		token, expires := uuid.New(), now.Add(leaseFor)
		if err := tx.Model(&task).Updates(map[string]any{"state": models.SourceRunVerificationTaskClaimed, "claim_owner": owner, "claim_token": token, "claim_epoch": gorm.Expr("claim_epoch + 1"), "claim_expires_at": expires, "heartbeat_at": now, "attempt_count": gorm.Expr("attempt_count + 1")}).Error; err != nil {
			return err
		}
		task.State, task.ClaimOwner, task.ClaimToken, task.ClaimEpoch, task.ClaimExpiresAt, task.HeartbeatAt, task.AttemptCount = models.SourceRunVerificationTaskClaimed, owner, &token, task.ClaimEpoch+1, &expires, &now, task.AttemptCount+1
		lease = VerificationLease{Task: task, ClaimToken: token}
		return nil
	})
	if err != nil || lease.Task.PublicID == uuid.Nil {
		return lease, false, err
	}
	return lease, true, nil
}

// ObserveAndCompleteVerification uses only current CMS evidence, then writes
// the immutable reconciliation event through the existing fenced completion
// path. It is intentionally conservative: an absent terminal receipt is not
// proof of absence, so it produces unknown rather than authorizing a retry.
func ObserveAndCompleteVerification(db *gorm.DB, tenantID, taskID, owner, claimToken string) (models.SourceRunReconciliationEvent, bool, error) {
	if err := BeginVerification(db, tenantID, taskID, owner, claimToken); err != nil {
		return models.SourceRunReconciliationEvent{}, false, err
	}
	observation, err := observeVerificationTask(db, tenantID, taskID, owner, claimToken)
	if err != nil {
		return models.SourceRunReconciliationEvent{}, false, err
	}
	return CompleteVerification(db, VerificationResultInput{
		TenantID: tenantID, TaskID: taskID, Owner: owner, ClaimToken: claimToken,
		EventKey:         "source-run-observation:" + taskID + ":" + observation.ProvenanceDigest,
		EvidenceSnapshot: observation.EvidenceSnapshot, ProvenanceDigest: observation.ProvenanceDigest,
		Verdict: observation.Verdict, Payload: observation.Payload,
		VerifiedOutcome: observation.VerifiedOutcome,
	})
}

func observeVerificationTask(db *gorm.DB, tenantID, taskID, owner, claimToken string) (VerificationObservation, error) {
	if db == nil || strings.TrimSpace(tenantID) == "" || strings.TrimSpace(owner) == "" {
		return VerificationObservation{}, fmt.Errorf("verification observation identity is incomplete")
	}
	taskPublicID, err := uuid.Parse(strings.TrimSpace(taskID))
	if err != nil {
		return VerificationObservation{}, fmt.Errorf("verification task ID is invalid")
	}
	token, err := uuid.Parse(strings.TrimSpace(claimToken))
	if err != nil {
		return VerificationObservation{}, fmt.Errorf("verification claim token is invalid")
	}
	now := time.Now().UTC()
	var task models.SourceRunVerificationTask
	if err := db.Where("public_id = ? AND tenant_id = ? AND claim_owner = ? AND claim_token = ? AND claim_expires_at > ? AND state = ?", taskPublicID, tenantID, owner, token, now, models.SourceRunVerificationTaskRunning).First(&task).Error; err != nil {
		return VerificationObservation{}, err
	}
	if task.ExecutionUnitID == nil {
		return VerificationObservation{}, fmt.Errorf("verification task is missing an execution unit")
	}
	var unit models.SourceRunExecutionUnit
	if err := db.Where("public_id = ? AND tenant_id = ? AND source_run_request_id = ?", *task.ExecutionUnitID, tenantID, task.SourceRunRequestID).First(&unit).Error; err != nil {
		return VerificationObservation{}, err
	}
	var receiptCount, terminalReceiptCount int64
	if err := db.Model(&models.SourceRunReceipt{}).Where("tenant_id = ? AND execution_unit_id = ?", tenantID, unit.PublicID).Count(&receiptCount).Error; err != nil {
		return VerificationObservation{}, err
	}
	if err := db.Model(&models.SourceRunReceipt{}).Where("tenant_id = ? AND execution_unit_id = ? AND event_type IN ?", tenantID, unit.PublicID, []string{string(ReceiptEventProviderTerminal), string(ReceiptEventNormalizeTerminal), string(ReceiptEventFinalization), string(ReceiptEventFailed), string(ReceiptEventCancelled), string(ReceiptEventDLQ)}).Count(&terminalReceiptCount).Error; err != nil {
		return VerificationObservation{}, err
	}
	var observedContentCount int64
	var expectedContentCount int64
	evidenceComplete := true
	observationKind := unit.UnitType
	if isPodsDeliveryTask(task) {
		observationKind = "pods_delivery"
		expectedContentCount, observedContentCount, evidenceComplete, err = observePodsDelivery(db, task, unit)
		if err != nil {
			return VerificationObservation{}, err
		}
	} else if unit.UnitType == "normalize_batch" {
		// The normalizer stamps the exact execution-unit ID into CMS content
		// metadata before upsert. This is a consumer-side fact, not a queue
		// acknowledgement or a worker's in-memory counter.
		if err := db.Model(&models.ContentItem{}).Where("tenant_id = ? AND content_source_id = ? AND metadata ->> ? = ?", tenantID, unit.ContentSourceID, "source_run_execution_unit_id", unit.PublicID.String()).Count(&observedContentCount).Error; err != nil {
			return VerificationObservation{}, err
		}
	}
	if !isPodsDeliveryTask(task) && unit.UnitType == "coordinator" {
		expectedContentCount, observedContentCount, evidenceComplete, err = observeCoordinatorDelivery(db, tenantID, task.SourceRunRequestID, unit.ContentSourceID)
		if err != nil {
			return VerificationObservation{}, err
		}
	}
	verdict, verifiedOutcome := observationVerdict(observationKind, observedContentCount, expectedContentCount, evidenceComplete)
	payloadMap := map[string]any{
		"schema_version": "source-run-observation/v1", "task_id": task.PublicID.String(), "unit_id": unit.PublicID.String(),
		"unit_type": unit.UnitType, "observation_kind": observationKind, "receipt_count": receiptCount, "terminal_receipt_count": terminalReceiptCount,
		"expected_content_count": expectedContentCount, "observed_content_count": observedContentCount, "evidence_complete": evidenceComplete, "observed_at": now.Format(time.RFC3339Nano),
		"verdict": string(verdict), "verified_outcome": string(verifiedOutcome), "reason": observationReason(observationKind, observedContentCount, expectedContentCount, evidenceComplete, terminalReceiptCount),
	}
	payloadBytes, err := json.Marshal(payloadMap)
	if err != nil {
		return VerificationObservation{}, err
	}
	digest := sha256.Sum256(payloadBytes)
	digestText := hex.EncodeToString(digest[:])
	return VerificationObservation{TaskID: task.PublicID.String(), TenantID: tenantID, RequestID: task.SourceRunRequestID.String(), AttemptID: nullableUUID(task.SourceRunAttemptID).String(), UnitID: unit.PublicID.String(), ContentSourceID: unit.ContentSourceID.String(), UnitType: unit.UnitType, Verdict: verdict, VerifiedOutcome: verifiedOutcome, EvidenceSnapshot: "cms-source-run-observation:" + digestText, ProvenanceDigest: digestText, Payload: datatypes.JSON(payloadBytes)}, nil
}

// observeCoordinatorDelivery is a bounded CMS-only consumer readback. It
// aggregates terminal normalization receipts and content rows stamped by those
// exact units. It never queries a provider or assumes that a queue receipt is
// a downstream effect.
func observeCoordinatorDelivery(db *gorm.DB, tenantID string, requestID, sourceID uuid.UUID) (expectedContentCount, observedContentCount int64, evidenceComplete bool, err error) {
	evidenceComplete = true
	var normalizedUnits []models.SourceRunExecutionUnit
	if err = db.Where("tenant_id = ? AND source_run_request_id = ? AND content_source_id = ? AND unit_type = ? AND state = ?", tenantID, requestID, sourceID, "normalize_batch", string(UnitSucceeded)).Find(&normalizedUnits).Error; err != nil {
		return 0, 0, false, err
	}
	unitIDs := make([]uuid.UUID, 0, len(normalizedUnits))
	for _, normalizedUnit := range normalizedUnits {
		unitIDs = append(unitIDs, normalizedUnit.PublicID)
		var receipts []models.SourceRunReceipt
		if err = db.Where("tenant_id = ? AND execution_unit_id = ? AND event_type = ?", tenantID, normalizedUnit.PublicID, string(ReceiptEventNormalizeTerminal)).Find(&receipts).Error; err != nil {
			return 0, 0, false, err
		}
		if len(receipts) != 1 {
			return 0, 0, false, nil
		}
		var terminalPayload struct {
			CMSUpserted *int64 `json:"cms_upserted"`
		}
		if err = json.Unmarshal(receipts[0].Payload, &terminalPayload); err != nil || terminalPayload.CMSUpserted == nil || *terminalPayload.CMSUpserted < 0 {
			return 0, 0, false, nil
		}
		expectedContentCount += *terminalPayload.CMSUpserted
	}
	if len(unitIDs) == 0 {
		return expectedContentCount, 0, evidenceComplete, nil
	}
	if err = db.Model(&models.ContentItem{}).Where("tenant_id = ? AND content_source_id = ? AND metadata ->> ? IN ?", tenantID, sourceID, "source_run_execution_unit_id", unitIDs).Count(&observedContentCount).Error; err != nil {
		return 0, 0, false, err
	}
	return expectedContentCount, observedContentCount, evidenceComplete, nil
}

// observePodsDelivery verifies the public consumer boundary for the exact
// media content established by a prior CMS-ingest reconciliation event. A
// direct item or an atomized child can satisfy the source item's delivery; the
// shared feed predicate also includes active generation membership.
func observePodsDelivery(db *gorm.DB, task models.SourceRunVerificationTask, coordinator models.SourceRunExecutionUnit) (expectedContentCount, observedContentCount int64, evidenceComplete bool, err error) {
	ingestEventID, ok := podsDeliveryIngestEventID(task.CausationID)
	if !ok {
		return 0, 0, false, nil
	}
	var ingestEvent models.SourceRunReconciliationEvent
	if err = db.Where("public_id = ? AND tenant_id = ? AND source_run_request_id = ? AND execution_unit_id = ? AND causation_id = ? AND verdict = ?", ingestEventID, task.TenantID, task.SourceRunRequestID, coordinator.PublicID, "consumer_delivery", string(VerdictPresent)).First(&ingestEvent).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return 0, 0, false, nil
		}
		return 0, 0, false, err
	}
	var ingestPayload struct {
		ExpectedContentCount *int64 `json:"expected_content_count"`
		EvidenceComplete     bool   `json:"evidence_complete"`
	}
	if err = json.Unmarshal(ingestEvent.Payload, &ingestPayload); err != nil || ingestPayload.ExpectedContentCount == nil || *ingestPayload.ExpectedContentCount < 0 || !ingestPayload.EvidenceComplete {
		return 0, 0, false, nil
	}
	expectedContentCount = *ingestPayload.ExpectedContentCount
	if expectedContentCount > podsDeliveryObservationItemLimit {
		return expectedContentCount, 0, false, nil
	}

	var normalizedUnits []models.SourceRunExecutionUnit
	if err = db.Select("public_id").Where("tenant_id = ? AND source_run_request_id = ? AND content_source_id = ? AND unit_type = ? AND state = ?", task.TenantID, task.SourceRunRequestID, task.ContentSourceID, "normalize_batch", string(UnitSucceeded)).Limit(podsDeliveryObservationUnitLimit + 1).Find(&normalizedUnits).Error; err != nil {
		return 0, 0, false, err
	}
	if len(normalizedUnits) > podsDeliveryObservationUnitLimit {
		return expectedContentCount, 0, false, nil
	}
	unitIDs := make([]uuid.UUID, 0, len(normalizedUnits))
	for _, normalizedUnit := range normalizedUnits {
		unitIDs = append(unitIDs, normalizedUnit.PublicID)
	}
	if len(unitIDs) == 0 {
		if expectedContentCount == 0 {
			return 0, 0, true, nil
		}
		return expectedContentCount, 0, false, nil
	}
	var sourceContentIDs []uuid.UUID
	if err = db.Model(&models.ContentItem{}).Where("tenant_id = ? AND content_source_id = ? AND metadata ->> ? IN ?", task.TenantID, task.ContentSourceID, "source_run_execution_unit_id", unitIDs).Pluck("public_id", &sourceContentIDs).Error; err != nil {
		return 0, 0, false, err
	}
	if int64(len(sourceContentIDs)) != expectedContentCount {
		return expectedContentCount, 0, false, nil
	}
	if len(sourceContentIDs) == 0 {
		return 0, 0, true, nil
	}
	query := feedcontract.PodsEligibleMediaQuery(db, task.TenantID, feedcontract.SupportsAtomizedPodsSchema(db))
	query = feedcontract.ApplyActiveGenerationMembership(db, query, task.TenantID, "media", "feed_unit", "content_items.public_id")
	var visibleSourceIDs []uuid.UUID
	if err = query.Select("DISTINCT CASE WHEN content_items.public_id IN ? THEN content_items.public_id ELSE content_items.parent_content_item_id END AS source_item_id", sourceContentIDs).
		Where("content_items.public_id IN ? OR content_items.parent_content_item_id IN ?", sourceContentIDs, sourceContentIDs).
		Scan(&visibleSourceIDs).Error; err != nil {
		return 0, 0, false, err
	}
	return expectedContentCount, int64(len(visibleSourceIDs)), true, nil
}

func podsDeliveryIngestEventID(causationID string) (uuid.UUID, bool) {
	const prefix = "consumer_pods_delivery:"
	if !strings.HasPrefix(causationID, prefix) {
		return uuid.Nil, false
	}
	id, err := uuid.Parse(strings.TrimPrefix(causationID, prefix))
	return id, err == nil
}

func observationVerdict(unitType string, observedContentCount, expectedContentCount int64, evidenceComplete bool) (VerificationVerdict, SourceRunOutcome) {
	if unitType == "normalize_batch" && observedContentCount > 0 {
		return VerdictPresent, OutcomeNewItems
	}
	if (unitType == "coordinator" || unitType == "pods_delivery") && evidenceComplete && observedContentCount == expectedContentCount {
		if expectedContentCount == 0 {
			return VerdictPresent, OutcomeNoChange
		}
		return VerdictPresent, OutcomeNewItems
	}
	return VerdictUnknown, OutcomeUnknown
}

func observationReason(unitType string, observedContentCount, expectedContentCount int64, evidenceComplete bool, terminalReceiptCount int64) string {
	if unitType == "normalize_batch" && observedContentCount > 0 {
		return "CMS has consumer-side content linked to this exact normalization unit"
	}
	if (unitType == "coordinator" || unitType == "pods_delivery") && !evidenceComplete {
		return "CMS cannot establish the expected downstream count from the immutable normalization evidence"
	}
	if unitType == "pods_delivery" && observedContentCount == expectedContentCount {
		return "CMS Pods-serving readback covers every exact source-run media item or its eligible atomized child"
	}
	if unitType == "coordinator" && observedContentCount == expectedContentCount {
		return "CMS downstream content readback matches the exact normalized delivery count"
	}
	if terminalReceiptCount > 0 {
		return "terminal producer receipt exists but this task was already uncertain; no stronger independent evidence was found"
	}
	return "CMS cannot independently establish the effect from current evidence"
}
