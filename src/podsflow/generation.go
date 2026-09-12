package podsflow

import (
	"content-management-system/src/feedstate"
	"content-management-system/src/models"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"time"
)

const finalizationRecoveryBackoff = 2 * time.Minute

// GenerationArtifacts requires the current durable generation and every exact
// unit-to-child link. Historical READY children are never completion evidence.
func GenerationArtifacts(tx *gorm.DB, item models.ContentItem) (bool, map[string]any, error) {
	proof := map[string]any{}
	var generation models.AtomizationGeneration
	err := tx.Where("tenant_id=? AND parent_content_item_id=? AND processing_generation=? AND state IN ?", item.TenantID, item.PublicID, item.ProcessingGeneration, []string{"verifying", "active"}).Order("generation_number DESC").First(&generation).Error
	if err == gorm.ErrRecordNotFound {
		return false, proof, nil
	}
	if err != nil {
		return false, proof, err
	}
	proof["generation_id"] = generation.PublicID
	proof["expected_units"] = generation.ExpectedUnits
	if generation.ExpectedUnits < 1 {
		return false, proof, nil
	}
	var terminalProof map[string]any
	if json.Unmarshal(generation.TerminalProof, &terminalProof) != nil || terminalProof["artifacts_verified"] != true {
		proof["artifacts_verified"] = false
		return false, proof, nil
	}
	var units []models.AtomizationChapterUnit
	if err = tx.Where("tenant_id=? AND generation_id=?", item.TenantID, generation.PublicID).Order("unit_index ASC").Find(&units).Error; err != nil {
		return false, proof, err
	}
	if len(units) != generation.ExpectedUnits || generation.CompletedUnits != generation.ExpectedUnits {
		return false, proof, nil
	}
	children := make(map[string]struct{}, len(units))
	for _, unit := range units {
		if unit.State != "verified" || unit.CandidateContentItemID == nil {
			return false, proof, nil
		}
		var child models.ContentItem
		if err := tx.Where("tenant_id=? AND public_id=? AND parent_content_item_id=? AND status<>'ARCHIVED' AND metadata->>'atomization_generation_id'=?", item.TenantID, *unit.CandidateContentItemID, item.PublicID, generation.PublicID.String()).First(&child).Error; err != nil {
			if err == gorm.ErrRecordNotFound {
				return false, proof, nil
			}
			return false, proof, err
		}
		if _, exists := children[child.PublicID.String()]; exists {
			return false, proof, nil
		}
		children[child.PublicID.String()] = struct{}{}
	}
	proof["verified_children"] = len(children)
	if generation.ContentStageRequestID != nil {
		attemptID, ok := terminalProof["content_stage_attempt_id"].(string)
		if !ok || attemptID == "" {
			return false, proof, nil
		}
		var receipts int64
		if err = tx.Model(&models.ContentStageReceipt{}).Where("tenant_id=? AND request_id=? AND attempt_id=? AND outcome='persisted' AND payload->>'generation_id'=?", item.TenantID, *generation.ContentStageRequestID, attemptID, generation.PublicID.String()).Count(&receipts).Error; err != nil {
			return false, proof, err
		}
		if receipts != 1 {
			return false, proof, nil
		}
	}
	return len(children) == generation.ExpectedUnits, proof, nil
}

// ActivateReady replaces a published generation only after every frozen unit's
// child is ready and editorially admitted. Caller supplies the transaction.
func ActivateReady(tx *gorm.DB, root models.ContentItem) error {
	if root.ParentContentItemID != nil {
		return nil
	}
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", root.TenantID, root.PublicID).First(&root).Error; err != nil {
		return err
	}
	var candidate models.AtomizationGeneration
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND parent_content_item_id=? AND processing_generation=? AND state='verifying' AND completed_units=expected_units AND expected_units>0", root.TenantID, root.PublicID, root.ProcessingGeneration).Order("generation_number DESC").First(&candidate).Error
	if err == gorm.ErrRecordNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	var candidateProof map[string]any
	if json.Unmarshal(candidate.TerminalProof, &candidateProof) != nil || candidateProof["artifacts_verified"] != true {
		return nil
	}
	var children []models.ContentItem
	if err := tx.Table("content_items c").Clauses(clause.Locking{Strength: "UPDATE", Table: clause.Table{Name: "c"}}).Select("c.*").Joins("JOIN atomization_chapter_units u ON u.candidate_content_item_id=c.public_id AND u.tenant_id=c.tenant_id").Where("u.tenant_id=? AND u.generation_id=? AND u.state='verified' AND c.parent_content_item_id=? AND c.metadata->>'atomization_generation_id'=?", root.TenantID, candidate.PublicID, root.PublicID, candidate.PublicID.String()).Order("c.public_id ASC").Find(&children).Error; err != nil {
		return err
	}
	if len(children) != candidate.ExpectedUnits {
		return nil
	}
	var units []models.AtomizationChapterUnit
	if err := tx.Where("tenant_id=? AND generation_id=? AND state='verified' AND candidate_content_item_id IS NOT NULL", root.TenantID, candidate.PublicID).Order("unit_index ASC").Find(&units).Error; err != nil {
		return err
	}
	if len(units) != candidate.ExpectedUnits {
		return nil
	}
	var unresolved int64
	if err := tx.Model(&models.MediaArtifactManifest{}).Where("tenant_id=? AND atomization_generation_id=? AND state IN ? AND deleted_at IS NULL", root.TenantID, candidate.PublicID, []string{"uploading", "uploaded", "uncertain"}).Count(&unresolved).Error; err != nil {
		return err
	}
	if unresolved > 0 {
		return nil
	}
	for _, child := range children {
		if !childReadyForActivation(child) {
			return nil
		}
	}
	now := time.Now().UTC()
	var previous []models.AtomizationGeneration
	if err := tx.Where("tenant_id=? AND parent_content_item_id=? AND state='active'", root.TenantID, root.PublicID).Find(&previous).Error; err != nil {
		return err
	}
	for _, old := range previous {
		var oldChildren []models.ContentItem
		if err := tx.Where("tenant_id=? AND parent_content_item_id=? AND metadata->>'atomization_generation_id'=?", root.TenantID, root.PublicID, old.PublicID.String()).Find(&oldChildren).Error; err != nil {
			return err
		}
		for _, child := range oldChildren {
			child.Status, child.FeedVisibility, child.IsFeedUnit = models.ContentStatusArchived, "hidden", false
			if err := tx.Save(&child).Error; err != nil {
				return err
			}
			if err := feedstate.SyncMediaMembership(tx, child); err != nil {
				return err
			}
		}
		if err := tx.Model(&old).Updates(map[string]any{"state": "superseded", "updated_at": now}).Error; err != nil {
			return err
		}
	}
	if err := tx.Model(&candidate).Updates(map[string]any{"state": "active", "activation_at": now, "updated_at": now}).Error; err != nil {
		return err
	}
	// Publish the complete replacement in the same transaction that withdraws
	// its predecessor. A single ready child must never expose a partial plan.
	for _, child := range children {
		child.FeedVisibility = "visible"
		published := "published"
		child.ChapteringStatus = &published
		if err := tx.Save(&child).Error; err != nil {
			return err
		}
		if err := tx.Model(&models.Chapter{}).Where("tenant_id=? AND child_content_item_id=?", root.TenantID, child.PublicID).Update("status", published).Error; err != nil {
			return err
		}
		if err := feedstate.SyncMediaMembership(tx, child); err != nil {
			return err
		}
	}
	return tx.Model(&root).Update("chaptering_status", "completed").Error
}

func childReadyForActivation(child models.ContentItem) bool {
	return child.Status == models.ContentStatusReady &&
		(child.FeedVisibility == "embedding_pending" || child.FeedVisibility == "visible") &&
		child.IsFeedUnit && child.DurationSec != nil && *child.DurationSec >= 270 && *child.DurationSec <= 2400
}

// ReconcileReadyGenerations also covers interrupted completion callbacks and
// the final episode of an otherwise empty queue. Rotate checked candidates so
// an older review hold cannot starve later completed generations.
func ReconcileReadyGenerations(db *gorm.DB) error {
	var candidates []models.AtomizationGeneration
	if err := db.Where("state='verifying' AND completed_units=expected_units AND expected_units>0").Order("updated_at ASC, public_id ASC").Limit(32).Find(&candidates).Error; err != nil {
		return err
	}
	for _, candidate := range candidates {
		if err := db.Transaction(func(tx *gorm.DB) error {
			var root models.ContentItem
			if err := tx.Where("tenant_id=? AND public_id=? AND processing_generation=? AND status<>'ARCHIVED'", candidate.TenantID, candidate.ParentContentItemID, candidate.ProcessingGeneration).First(&root).Error; err != nil {
				if err == gorm.ErrRecordNotFound {
					return tx.Model(&candidate).Update("updated_at", time.Now().UTC()).Error
				}
				return err
			}
			if err := ActivateReady(tx, root); err != nil {
				return err
			}
			return tx.Model(&candidate).Update("updated_at", time.Now().UTC()).Error
		}); err != nil {
			return err
		}
	}
	return nil
}

// ReconcileFinalizationGaps repairs the narrow but important state where all
// chapter effects have been durably verified, but the outer atomization
// finalization transaction was lost (for example after a worker lease expired
// or a receipt insert failed).  The old generation remains running and the
// stage request is terminal, so no normal dispatcher will claim it again.  We
// only requeue when the immutable evidence proves that every unit has its
// current fenced manifests and that no child or persistence receipt exists.
// Any ambiguous ownership is deliberately left for the explicit recovery
// report/operator path.
func ReconcileFinalizationGaps(db *gorm.DB) error {
	var candidates []models.AtomizationGeneration
	if err := db.Where("state=? AND content_stage_request_id IS NOT NULL AND expected_units>0 AND completed_units<expected_units", "running").
		Order("updated_at ASC, public_id ASC").Limit(32).Find(&candidates).Error; err != nil {
		return err
	}
	for _, candidate := range candidates {
		if err := db.Transaction(func(tx *gorm.DB) error {
			if candidate.ContentStageRequestID == nil {
				return nil
			}
			var request models.ContentStageRequest
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
				Where("tenant_id=? AND public_id=?", candidate.TenantID, *candidate.ContentStageRequestID).
				First(&request).Error; err != nil {
				if err == gorm.ErrRecordNotFound {
					return nil
				}
				return err
			}
			// A live claim may still be completing the outer callback. Never
			// compete with it; the worker will either persist the receipt or move
			// the request into the normal reconciliation states. A deferred request
			// is our own bounded backoff state and may be requeued only after its
			// not-before time; this prevents a deterministic finalization error from
			// hot-looping attempts and monopolising the global episode slot.
			if request.State != models.ContentStageFailed && request.State != models.ContentStageUncertain && request.State != models.ContentStageReconciling && !(request.State == models.ContentStageDeferred && request.FailureClass == "finalization_recovery_backoff") {
				return nil
			}
			if request.CancellationRequestedAt != nil || request.Stage != models.ContentStagePodsAtomization || request.Lane != models.ContentStageLanePods {
				return nil
			}
			if request.FailureClass == "finalization_recovery_exhausted" {
				return nil
			}

			var root models.ContentItem
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
				Where("tenant_id=? AND public_id=?", candidate.TenantID, candidate.ParentContentItemID).
				First(&root).Error; err != nil {
				if err == gorm.ErrRecordNotFound {
					return nil
				}
				return err
			}
			var generation models.AtomizationGeneration
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
				Where("tenant_id=? AND public_id=?", candidate.TenantID, candidate.PublicID).
				First(&generation).Error; err != nil {
				return err
			}
			if generation.State != "running" || generation.ContentStageRequestID == nil || *generation.ContentStageRequestID != request.PublicID || generation.ProcessingGeneration != root.ProcessingGeneration || root.Status == models.ContentStatusArchived || root.DurationSec == nil || *root.DurationSec <= 2400 {
				return nil
			}

			var units []models.AtomizationChapterUnit
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
				Where("tenant_id=? AND generation_id=?", generation.TenantID, generation.PublicID).
				Order("unit_index ASC").Find(&units).Error; err != nil {
				return err
			}
			if len(units) != generation.ExpectedUnits {
				return nil
			}
			for index, unit := range units {
				if unit.UnitIndex != index || unit.State != "verified" || unit.CandidateContentItemID != nil || len(unit.Result) == 0 || unit.FenceToken == nil {
					return nil
				}
				var manifestIDs []string
				if json.Unmarshal(unit.ArtifactManifestIDs, &manifestIDs) != nil || len(manifestIDs) == 0 || hasDuplicateStrings(manifestIDs) {
					return nil
				}
				var manifests []models.MediaArtifactManifest
				if err := tx.Where("tenant_id=? AND atomization_generation_id=? AND atomization_chapter_unit_id=? AND unit_fence_token=? AND public_id IN ? AND state IN ? AND deleted_at IS NULL", generation.TenantID, generation.PublicID, unit.PublicID, unit.FenceToken, manifestIDs, []string{"verified", "active"}).Find(&manifests).Error; err != nil {
					return err
				}
				if len(manifests) != len(manifestIDs) || !sameManifestIDs(manifestIDs, manifests) {
					return nil
				}
			}
			var unresolved int64
			if err := tx.Model(&models.MediaArtifactManifest{}).
				Where("tenant_id=? AND atomization_generation_id=? AND deleted_at IS NULL AND state IN ?", generation.TenantID, generation.PublicID, []string{"uploading", "uploaded", "uncertain"}).
				Count(&unresolved).Error; err != nil {
				return err
			}
			if unresolved != 0 {
				return nil
			}
			// A child row or receipt means the callback crossed a persistence
			// boundary. It requires the normal idempotent verifier, not a fresh
			// claim that could create a second set of children.
			var children int64
			if err := tx.Model(&models.ContentItem{}).Where("tenant_id=? AND parent_content_item_id=? AND metadata->>'atomization_generation_id'=?", generation.TenantID, generation.ParentContentItemID, generation.PublicID.String()).Count(&children).Error; err != nil {
				return err
			}
			if children != 0 {
				return nil
			}
			var receipts int64
			if err := tx.Model(&models.ContentStageReceipt{}).Where("tenant_id=? AND request_id=? AND outcome='persisted'", request.TenantID, request.PublicID).Count(&receipts).Error; err != nil {
				return err
			}
			if receipts != 0 {
				return nil
			}

			now := time.Now().UTC()
			// Automatic recovery never grants itself an unlimited retry budget.
			// Only a new explicit operator admission opens another window.
			allowed, err := finalizationRecoveryAllowed(tx, request)
			if err != nil {
				return err
			}
			if !allowed {
				if err := tx.Model(&root).Update("chaptering_status", "failed").Error; err != nil {
					return err
				}
				return tx.Model(&request).Updates(map[string]any{"state": models.ContentStageFailed, "failure_class": "finalization_recovery_exhausted", "terminal_proof": datatypes.JSON(mustJSON(map[string]any{"recovery": "finalization_recovery_exhausted", "generation_id": generation.PublicID, "artifacts_verified": true, "summary": "Automatic finalization recovery exhausted; explicit operator retry required"})), "claim_owner": "", "claim_token": nil, "claim_expires_at": nil, "not_before_at": nil, "finished_at": now, "updated_at": now}).Error
			}
			if request.State == models.ContentStageUncertain || request.State == models.ContentStageReconciling || request.State == models.ContentStageFailed {
				// All unit/object proofs above are complete, so this is a safe
				// finalization retry boundary rather than an unknown media effect.
				// Park it as deferred instead of immediately claiming another
				// attempt. Observe treats deferred work as yielded, allowing another
				// episode family to use the installation-wide slot.
				retryAt := now.Add(finalizationRecoveryBackoff)
				proof := map[string]any{
					"recovery":       "finalization_gap_backoff",
					"generation_id":  generation.PublicID.String(),
					"unit_count":     len(units),
					"previous_state": request.State,
					"retry_at":       retryAt,
				}
				if err := tx.Model(&request).Updates(map[string]any{
					"state":            models.ContentStageDeferred,
					"claim_owner":      "",
					"claim_token":      nil,
					"claim_expires_at": nil,
					"not_before_at":    retryAt,
					"finished_at":      nil,
					"failure_class":    "finalization_recovery_backoff",
					"terminal_proof":   datatypes.JSON(mustJSON(proof)),
					"updated_at":       now,
				}).Error; err != nil {
					return err
				}
				if err := tx.Model(&root).Updates(map[string]any{"chaptering_status": "planning", "updated_at": now}).Error; err != nil {
					return err
				}
				if err := appendGenerationRecoveryEvent(tx, request, proof, now); err != nil {
					return err
				}
				return nil
			}
			if request.NotBeforeAt != nil && request.NotBeforeAt.After(now) {
				return nil
			}

			deadline := now.Add(24 * time.Hour)
			proof := map[string]any{
				"recovery":       "finalization_gap_requeued",
				"generation_id":  generation.PublicID.String(),
				"unit_count":     len(units),
				"previous_state": request.State,
				"requeued_at":    now,
			}
			if err := tx.Model(&request).Updates(map[string]any{
				"state":            models.ContentStageQueued,
				"claim_owner":      "",
				"claim_token":      nil,
				"claim_expires_at": nil,
				"not_before_at":    nil,
				"deadline_at":      deadline,
				"finished_at":      nil,
				"verified_at":      nil,
				"failure_class":    "",
				"terminal_proof":   datatypes.JSON(mustJSON(proof)),
				"updated_at":       now,
			}).Error; err != nil {
				return err
			}
			if err := tx.Model(&generation).Updates(map[string]any{
				"state":           "running",
				"completed_units": 0,
				"terminal_proof":  datatypes.JSON(mustJSON(proof)),
				"updated_at":      now,
			}).Error; err != nil {
				return err
			}
			if err := tx.Model(&root).Updates(map[string]any{"chaptering_status": "planning", "updated_at": now}).Error; err != nil {
				return err
			}
			if err := appendGenerationRecoveryEvent(tx, request, proof, now); err != nil {
				return err
			}
			return Resume(tx, root)
		}); err != nil {
			return fmt.Errorf("reconcile atomization finalization gap %s: %w", candidate.PublicID, err)
		}
	}
	return nil
}

// ValidateFinalizationRetry rechecks persisted artifacts instead of trusting
// the historical exhaustion proof. It must run inside the admission transaction.
func ValidateFinalizationRetry(tx *gorm.DB, request models.ContentStageRequest) error {
	var gen models.AtomizationGeneration
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND content_stage_request_id=? AND processing_generation=? AND state='running'", request.TenantID, request.PublicID, request.ProcessingGeneration).First(&gen).Error; err != nil {
		return err
	}
	var units []models.AtomizationChapterUnit
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND generation_id=?", request.TenantID, gen.PublicID).Order("unit_index ASC").Find(&units).Error; err != nil {
		return err
	}
	if len(units) == 0 || len(units) != gen.ExpectedUnits {
		return fmt.Errorf("finalization unit set incomplete")
	}
	for index, unit := range units {
		if unit.UnitIndex != index || unit.State != "verified" || unit.CandidateContentItemID != nil || unit.FenceToken == nil || !unitResultHasPlaybackReceipt(unit) {
			return fmt.Errorf("finalization unit requires reconciliation")
		}
		var ids []string
		if json.Unmarshal(unit.ArtifactManifestIDs, &ids) != nil || len(ids) == 0 || hasDuplicateStrings(ids) {
			return fmt.Errorf("finalization manifest set invalid")
		}
		var manifests []models.MediaArtifactManifest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND atomization_generation_id=? AND atomization_chapter_unit_id=? AND unit_fence_token=? AND state IN ? AND deleted_at IS NULL", request.TenantID, gen.PublicID, unit.PublicID, unit.FenceToken, []string{"verified", "active"}).Find(&manifests).Error; err != nil {
			return err
		}
		if !sameManifestIDs(ids, manifests) {
			return fmt.Errorf("finalization artifacts require reconciliation")
		}
	}
	var unresolved int64
	if err := tx.Model(&models.MediaArtifactManifest{}).Where("tenant_id=? AND atomization_generation_id=? AND state IN ? AND deleted_at IS NULL", request.TenantID, gen.PublicID, []string{"uploading", "uploaded", "uncertain"}).Count(&unresolved).Error; err != nil {
		return err
	}
	if unresolved != 0 {
		return fmt.Errorf("finalization has uncertain effects")
	}
	var children, receipts int64
	if err := tx.Model(&models.ContentItem{}).Where("tenant_id=? AND parent_content_item_id=? AND metadata->>'atomization_generation_id'=?", request.TenantID, request.ContentItemID, gen.PublicID.String()).Count(&children).Error; err != nil {
		return err
	}
	if err := tx.Model(&models.ContentStageReceipt{}).Where("tenant_id=? AND request_id=? AND outcome='persisted'", request.TenantID, request.PublicID).Count(&receipts).Error; err != nil {
		return err
	}
	if children != 0 || receipts != 0 {
		return fmt.Errorf("persisted finalization requires receipt reconciliation")
	}
	return nil
}

func finalizationRecoveryAllowed(tx *gorm.DB, request models.ContentStageRequest) (bool, error) {
	var approval models.ContentStageEvent
	err := tx.Where("tenant_id=? AND request_id=? AND event_type=?", request.TenantID, request.PublicID, "manual_atomization_retry_approved").Order("sequence DESC").First(&approval).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		return false, err
	}
	var recoveries int64
	err = tx.Model(&models.ContentStageEvent{}).Where("tenant_id=? AND request_id=? AND event_type=? AND sequence>? AND payload->>'recovery'=?", request.TenantID, request.PublicID, "atomization_finalization_requeued", approval.Sequence, "finalization_gap_requeued").Count(&recoveries).Error
	return recoveries < 2, err
}

func hasDuplicateStrings(values []string) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			return true
		}
		seen[value] = struct{}{}
	}
	return false
}

func sameManifestIDs(ids []string, manifests []models.MediaArtifactManifest) bool {
	if len(ids) != len(manifests) {
		return false
	}
	seen := make(map[string]struct{}, len(manifests))
	for _, manifest := range manifests {
		seen[manifest.PublicID.String()] = struct{}{}
	}
	for _, id := range ids {
		if _, ok := seen[strings.TrimSpace(id)]; !ok {
			return false
		}
	}
	return true
}

func appendGenerationRecoveryEvent(tx *gorm.DB, request models.ContentStageRequest, proof map[string]any, now time.Time) error {
	var sequence int64
	if err := tx.Model(&models.ContentStageEvent{}).Where("tenant_id=? AND request_id=?", request.TenantID, request.PublicID).Select("COALESCE(MAX(sequence),0)").Scan(&sequence).Error; err != nil {
		return err
	}
	return tx.Create(&models.ContentStageEvent{
		PublicID: uuid.New(), TenantID: request.TenantID, RequestID: request.PublicID,
		Sequence: sequence + 1, EventType: "atomization_finalization_requeued",
		Payload: datatypes.JSON(mustJSON(proof)), OccurredAt: now,
	}).Error
}
