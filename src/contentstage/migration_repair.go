package contentstage

import (
	"encoding/json"
	"fmt"
	"time"

	"content-management-system/src/models"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

var migrationAttemptStates = []string{models.ContentStageClaimed, models.ContentStageRunning, models.ContentStageVerifying, models.ContentStageUncertain, models.ContentStageReconciling}

type MigrationAttemptRepair struct {
	TenantID  string    `json:"tenant_id"`
	AttemptID uuid.UUID `json:"attempt_id"`
	RequestID uuid.UUID `json:"request_id"`
	Kind      string    `json:"kind"`
	Evidence  string    `json:"evidence"`
}
type MigrationAttemptRepairPreview struct {
	Digest    string                   `json:"digest"`
	Actions   []MigrationAttemptRepair `json:"actions"`
	Remaining int                      `json:"remaining"`
	Reasons   map[string]int           `json:"reasons"`
}

// A receipt must belong to this exact attempted effect. Request completion or
// another attempt's receipt cannot settle a possibly effecting attempt.
func matchingAttemptReceipt(a models.ContentStageAttempt, r models.ContentStageRequest, c models.ContentStageReceipt) bool {
	producer := r.Owner
	switch r.Stage {
	case models.ContentStageNewsTextEmbedding, models.ContentStagePodsTextEmbedding, models.ContentStagePodsCaptionReembedding, models.ContentStageNewsLLMMetadata, models.ContentStagePodsLLMMetadata:
		producer = models.ContentStageOwnerEnrichment
	case models.ContentStagePodsTranscript, models.ContentStagePodsImageEmbedding:
		producer = models.ContentStageOwnerMedia
	}
	return a.FenceToken != uuid.Nil && c.PublicID != uuid.Nil && a.TenantID == r.TenantID && a.RequestID == r.PublicID && a.Owner == r.Owner && a.Lane == r.Lane && a.Stage == r.Stage && a.InputFingerprint == r.InputFingerprint &&
		c.TenantID == a.TenantID && c.AttemptID == a.PublicID && c.RequestID == a.RequestID && c.FenceToken == a.FenceToken && c.InputFingerprint == a.InputFingerprint &&
		c.ContentItemID == r.ContentItemID && c.ProcessingGeneration == r.ProcessingGeneration && c.Lane == r.Lane && c.Stage == r.Stage && c.Owner == producer && c.Outcome == "persisted" && c.ProducerEventID != uuid.Nil && c.PayloadDigest != ""
}

func classifyMigrationAttempt(a models.ContentStageAttempt, r models.ContentStageRequest, item models.ContentItem, receipts []models.ContentStageReceipt, present bool, now time.Time) (string, string) {
	if a.TenantID != r.TenantID || a.RequestID != r.PublicID || a.Stage != r.Stage || a.Owner != r.Owner || a.Lane != r.Lane || a.InputFingerprint != r.InputFingerprint || a.FenceToken == uuid.Nil || a.ClaimToken == uuid.Nil || a.LeaseEpoch <= 0 || r.ProcessingGeneration <= 0 || a.LeaseExpiresAt.IsZero() {
		return "", "request_correlation_mismatch"
	}
	if a.LeaseExpiresAt.After(now.Add(-5 * time.Minute)) {
		return "", "lease_not_quiescent"
	}
	if a.State == models.ContentStageClaimed && a.EffectStartedAt == nil && len(receipts) == 0 && r.State != models.ContentStageClaimed && r.State != models.ContentStageRunning && (r.ClaimExpiresAt == nil || !r.ClaimExpiresAt.After(now)) {
		return "no_effect", "expired_claim_before_effect"
	}
	if r.State != models.ContentStageVerified || !present || item.TenantID != r.TenantID || item.PublicID != r.ContentItemID || item.ProcessingGeneration != r.ProcessingGeneration {
		return "", "effect_requires_owner_proof"
	}
	for _, receipt := range receipts {
		if matchingAttemptReceipt(a, r, receipt) {
			return "persisted", receipt.PublicID.String()
		}
	}
	return "", "matching_attempt_receipt_missing"
}

// PreviewMigrationAttemptRepair never dispatches, requeues, adopts another
// attempt's effect, or changes schema. Its bounded evidence digest is stable
// across observation times and includes the records and persisted artifacts.
func PreviewMigrationAttemptRepair(tx *gorm.DB, now time.Time) (MigrationAttemptRepairPreview, error) {
	p := MigrationAttemptRepairPreview{Actions: []MigrationAttemptRepair{}, Reasons: map[string]int{}}
	var attempts []models.ContentStageAttempt
	if err := tx.Where("state IN ?", migrationAttemptStates).Order("tenant_id, public_id").Limit(1001).Find(&attempts).Error; err != nil {
		return p, err
	}
	if len(attempts) > 1000 {
		return p, fmt.Errorf("attempt repair inventory exceeds its 1000-record bound")
	}
	if len(attempts) == 0 {
		p.Digest = digest("migration-attempt-repair/v1", "empty")
		return p, nil
	}
	requestIDs, attemptIDs := []uuid.UUID{}, []uuid.UUID{}
	for _, a := range attempts {
		requestIDs = append(requestIDs, a.RequestID)
		attemptIDs = append(attemptIDs, a.PublicID)
	}
	var requests []models.ContentStageRequest
	if err := tx.Where("public_id IN ?", requestIDs).Find(&requests).Error; err != nil {
		return p, err
	}
	byRequest := map[uuid.UUID]models.ContentStageRequest{}
	itemIDs := []uuid.UUID{}
	for _, r := range requests {
		byRequest[r.PublicID] = r
		itemIDs = append(itemIDs, r.ContentItemID)
	}
	var receipts []models.ContentStageReceipt
	if err := tx.Where("attempt_id IN ?", attemptIDs).Order("public_id").Find(&receipts).Error; err != nil {
		return p, err
	}
	byAttempt := map[uuid.UUID][]models.ContentStageReceipt{}
	for _, r := range receipts {
		byAttempt[r.AttemptID] = append(byAttempt[r.AttemptID], r)
	}
	var items []models.ContentItem
	if err := tx.Where("public_id IN ?", itemIDs).Find(&items).Error; err != nil {
		return p, err
	}
	byItem := map[uuid.UUID]models.ContentItem{}
	for _, item := range items {
		byItem[item.PublicID] = item
	}
	// Do not send application rows, tokens or payloads to the Console.
	evidence := []string{"migration-attempt-repair/v1"}
	presentByRequest := map[uuid.UUID]bool{}
	for _, a := range attempts {
		r := byRequest[a.RequestID]
		item := byItem[r.ContentItemID]
		present := false
		if r.State == models.ContentStageVerified && item.PublicID != uuid.Nil && item.ProcessingGeneration == r.ProcessingGeneration {
			var cached bool
			present, cached = presentByRequest[r.PublicID]
			if !cached {
				var err error
				present, _, err = artifactPresent(tx, item, r.Stage)
				if err != nil {
					return p, err
				}
				presentByRequest[r.PublicID] = present
			}
		}
		kind, proof := classifyMigrationAttempt(a, r, item, byAttempt[a.PublicID], present, now)
		// Hash the exact binding and current artifact version without retaining secrets.
		raw, _ := json.Marshal(struct {
			Attempt      models.ContentStageAttempt
			AttemptClaim uuid.UUID
			Request      models.ContentStageRequest
			RequestClaim *uuid.UUID
			Receipts     []models.ContentStageReceipt
			ItemVersion  time.Time
			Present      bool
		}{a, a.ClaimToken, r, r.ClaimToken, byAttempt[a.PublicID], item.UpdatedAt, present})
		evidence = append(evidence, digest(string(raw)), kind, proof)
		if kind == "" {
			p.Remaining++
			p.Reasons[proof]++
			continue
		}
		p.Actions = append(p.Actions, MigrationAttemptRepair{a.TenantID, a.PublicID, a.RequestID, kind, proof})
	}
	p.Digest = digest(evidence...)
	return p, nil
}

// The caller owns quiescence and the database preparation lock. The transaction
// locks the owner ledgers and re-derives the reviewed evidence before any write.
func ApplyMigrationAttemptRepair(tx *gorm.DB, expected, actor string, now time.Time) (MigrationAttemptRepairPreview, error) {
	if expected == "" || actor == "" {
		return MigrationAttemptRepairPreview{}, fmt.Errorf("reviewed digest and actor required")
	}
	if err := tx.Exec("LOCK TABLE content_stage_requests,content_stage_attempts,content_stage_receipts,content_stage_events IN SHARE ROW EXCLUSIVE MODE").Error; err != nil {
		return MigrationAttemptRepairPreview{}, err
	}
	p, err := PreviewMigrationAttemptRepair(tx, now)
	if err != nil {
		return p, err
	}
	if p.Digest != expected {
		return p, fmt.Errorf("attempt repair evidence changed; review again")
	}
	for _, action := range p.Actions {
		state := models.ContentStageVerified
		failure := ""
		if action.Kind == "no_effect" {
			state = models.ContentStageFailed
			failure = "migration_repair_proven_no_effect"
		}
		result := tx.Model(&models.ContentStageAttempt{}).Where("tenant_id=? AND public_id=? AND request_id=? AND state IN ?", action.TenantID, action.AttemptID, action.RequestID, migrationAttemptStates).Updates(map[string]any{"state": state, "finished_at": now, "failure_class": failure, "updated_at": now})
		if result.Error != nil {
			return p, result.Error
		}
		if result.RowsAffected != 1 {
			return p, fmt.Errorf("attempt repair target changed")
		}
		var request models.ContentStageRequest
		if err := tx.Where("tenant_id=? AND public_id=?", action.TenantID, action.RequestID).First(&request).Error; err != nil {
			return p, err
		}
		a := models.ContentStageAttempt{PublicID: action.AttemptID}
		if err := appendEvent(tx, request, &a, "migration_attempt_reconciled", map[string]any{"actor": actor, "review_digest": expected, "outcome": action.Kind, "evidence": action.Evidence}); err != nil {
			return p, err
		}
	}
	return p, nil
}

// Close only receipt-bound attempts when normal verification proves the result.
func settleVerifiedAttemptReceipts(tx *gorm.DB, request models.ContentStageRequest) error {
	var attempts []models.ContentStageAttempt
	if err := tx.Where("tenant_id=? AND request_id=? AND state IN ?", request.TenantID, request.PublicID, []string{models.ContentStageVerifying, models.ContentStageUncertain}).Find(&attempts).Error; err != nil {
		return err
	}
	if len(attempts) == 0 {
		return nil
	}
	ids := make([]uuid.UUID, 0, len(attempts))
	for _, a := range attempts {
		ids = append(ids, a.PublicID)
	}
	var receipts []models.ContentStageReceipt
	if err := tx.Where("tenant_id=? AND attempt_id IN ?", request.TenantID, ids).Find(&receipts).Error; err != nil {
		return err
	}
	byAttempt := map[uuid.UUID][]models.ContentStageReceipt{}
	for _, receipt := range receipts {
		byAttempt[receipt.AttemptID] = append(byAttempt[receipt.AttemptID], receipt)
	}
	for _, a := range attempts {
		for _, receipt := range byAttempt[a.PublicID] {
			if matchingAttemptReceipt(a, request, receipt) {
				now := time.Now().UTC()
				result := tx.Model(&models.ContentStageAttempt{}).Where("tenant_id=? AND public_id=? AND request_id=? AND state IN ?", a.TenantID, a.PublicID, request.PublicID, []string{models.ContentStageVerifying, models.ContentStageUncertain}).Updates(map[string]any{"state": models.ContentStageVerified, "finished_at": now, "updated_at": now})
				if err := result.Error; err != nil {
					return err
				}
				if result.RowsAffected != 1 {
					return fmt.Errorf("verified attempt target changed")
				}
				if err := appendEvent(tx, request, &a, "attempt_effect_verified", map[string]any{"receipt_id": receipt.PublicID.String()}); err != nil {
					return err
				}
				break
			}
		}
	}
	return nil
}
