package contentstage

import (
	"encoding/json"
	"fmt"
	"time"

	"content-management-system/src/lifecycle"
	"content-management-system/src/models"
	"content-management-system/src/podsflow"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// RequestManualAtomization keeps operator retries in the durable ledger. It
// never retries uncertain effects or resets the history of previous attempts.
func RequestManualAtomization(db *gorm.DB, tenant string, itemID uuid.UUID, actor string, replacement ...bool) (models.ContentStageRequest, bool, error) {
	var request models.ContentStageRequest
	handled := false
	err := db.Transaction(func(tx *gorm.DB) error {
		var item models.ContentItem
		if err := tx.Where("tenant_id=? AND public_id=?", tenant, itemID).First(&item).Error; err != nil {
			return err
		}
		scope := lifecycle.Scope{TenantID: item.TenantID, Lane: "pods", ItemID: item.PublicID.String()}
		if item.ContentSourceID != nil {
			scope.SourceID = item.ContentSourceID.String()
		}
		if err := lifecycle.Check(tx, scope, lifecycle.PhaseSourceDispatch); err != nil {
			return err
		}
		if err := lifecycle.Check(tx, scope, lifecycle.PhaseContentWrite); err != nil {
			return err
		}
		var lockedStages []models.ContentStageRequest
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND content_item_id=? AND processing_generation=?", tenant, itemID, item.ProcessingGeneration).Order("public_id ASC").Find(&lockedStages).Error; err != nil {
			return err
		}
		err := tx.Where("tenant_id=? AND content_item_id=? AND processing_generation=? AND stage=?", tenant, itemID, item.ProcessingGeneration, models.ContentStagePodsAtomization).First(&request).Error
		if err == gorm.ErrRecordNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		handled = true
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", tenant, itemID).First(&item).Error; err != nil {
			return err
		}
		if item.ProcessingGeneration != request.ProcessingGeneration || item.Status == models.ContentStatusArchived {
			return fmt.Errorf("atomization target changed")
		}
		if err := CheckJourneyGeneration(tx, tenant, itemID); err != nil {
			return err
		}
		if item.DurationSec == nil || *item.DurationSec <= 2400 {
			return fmt.Errorf("parent must be longer than 40 minutes")
		}
		if len(replacement) > 0 && replacement[0] && (request.State == models.ContentStageVerified || request.State == models.ContentStageFailed) {
			var err error
			request, err = replaceAtomizationIntent(tx, item, actor)
			return err
		}
		ready, err := dependenciesVerified(tx, request)
		if err != nil {
			return err
		}
		if !ready {
			return fmt.Errorf("media and transcript prerequisites must verify before atomization")
		}
		switch request.State {
		case models.ContentStageQueued, models.ContentStageClaimed, models.ContentStageRunning, models.ContentStageVerifying:
			return nil // Repeated clicks do not create a second attempt.
		case models.ContentStageUncertain, models.ContentStageReconciling:
			// A worker can lose its lease after crossing the effect boundary. Do
			// not blindly restart that work, but do let an explicit operator
			// retry complete the same reconciliation that VerifyRequest performs.
			// Chapter manifests are registered before any object upload, so once
			// every expired unit has no unresolved manifest it is safe to resume
			// the immutable generation.
			if request.Stage != models.ContentStagePodsAtomization {
				return fmt.Errorf("atomization stage %s cannot be retried through Queue", request.State)
			}
			if err := podsflow.ReconcileAbsentUnits(tx, item); err != nil {
				return err
			}
			unresolved, err := podsflow.HasUnresolvedChapterEffects(tx, item)
			if err != nil {
				return err
			}
			if unresolved {
				return fmt.Errorf("atomization effects still require reconciliation")
			}
			// Continue through the generation/unit proof below. It will reject
			// any unit that was not proven absent or already verified.
		case models.ContentStageFailed:
			var proof map[string]any
			_ = json.Unmarshal(request.TerminalProof, &proof)
			if request.FailureClass == "finalization_recovery_exhausted" {
				if err := podsflow.ValidateFinalizationRetry(tx, request); err != nil {
					return err
				}
			} else if request.FailureClass == "contextual_plan_invalid" {
				// A planner failure may retry only before any generation/effect
				// was persisted. Never trust the worker's failure label alone.
				var effects int64
				if err := tx.Model(&models.AtomizationGeneration{}).Where("tenant_id=? AND content_stage_request_id=?", tenant, request.PublicID).Count(&effects).Error; err != nil {
					return err
				}
				if effects != 0 {
					return fmt.Errorf("existing generation requires reconciliation")
				}
			} else if request.FailureClass != "verified_absent_budget_exhausted" || proof["effect"] != "absent" {
				return fmt.Errorf("atomization failure requires reconciliation before retry")
			}
		default:
			return fmt.Errorf("atomization stage %s cannot be retried through Queue", request.State)
		}
		// Resume the immutable generation only when every unfinished unit has
		// proven absence. Verified units and their artifact history remain intact.
		var count int64
		var generations []models.AtomizationGeneration
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND content_stage_request_id=?", tenant, request.PublicID).Find(&generations).Error; err != nil {
			return err
		}
		if len(generations) > 1 {
			return fmt.Errorf("ambiguous generation ownership requires reconciliation")
		}
		if len(generations) == 1 {
			gen := generations[0]
			if gen.State == "active" || gen.State == "superseded" {
				return fmt.Errorf("completed generation requires explicit re-atomization")
			}
			var units []models.AtomizationChapterUnit
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND generation_id=?", tenant, gen.PublicID).Order("unit_index ASC").Find(&units).Error; err != nil {
				return err
			}
			if len(units) != gen.ExpectedUnits {
				return fmt.Errorf("generation unit set is incomplete")
			}
			for _, unit := range units {
				if unit.State == "verified" {
					continue
				}
				if unit.State != "queued" && unit.State != "deferred" && !(unit.State == "failed" && unit.FailureClass == "verified_absent") {
					return fmt.Errorf("chapter %d requires reconciliation", unit.UnitIndex)
				}
				if err := tx.Model(&models.MediaArtifactManifest{}).Where("tenant_id=? AND atomization_chapter_unit_id=? AND deleted_at IS NULL AND NOT (state='failed' AND terminal_proof @> '{\"reconciled\":true,\"object_present\":false}'::jsonb)", tenant, unit.PublicID).Count(&count).Error; err != nil {
					return err
				}
				if count != 0 {
					return fmt.Errorf("unfinished chapter %d has unresolved objects", unit.UnitIndex)
				}
				if err := tx.Model(&unit).Updates(map[string]any{"state": "queued", "not_before_at": nil, "claim_token": nil, "lease_expires_at": nil, "effect_started_at": nil}).Error; err != nil {
					return err
				}
			}
			if err := tx.Model(&gen).Update("state", "running").Error; err != nil {
				return err
			}
		} else {
			if err := tx.Table("media_artifact_manifests m").Joins("JOIN content_stage_attempts a ON a.tenant_id=m.tenant_id AND a.public_id=m.attempt_id").Where("a.tenant_id=? AND a.request_id=? AND m.deleted_at IS NULL", tenant, request.PublicID).Count(&count).Error; err != nil {
				return err
			}
			if count != 0 {
				return fmt.Errorf("existing attempt artifacts require reconciliation")
			}
			if err := retireUnstartedLegacyAtomization(tx, request, actor); err != nil {
				return err
			}
		}
		if request.CancellationRequestedAt != nil {
			return fmt.Errorf("cancelled atomization requires a new request")
		}
		now := time.Now().UTC()
		previousFailure := request.FailureClass
		deadline := now.Add(24 * time.Hour)
		if err := tx.Model(&request).Updates(map[string]any{"state": models.ContentStageQueued, "deadline_at": deadline, "not_before_at": nil, "finished_at": nil, "failure_class": "", "terminal_proof": jsonValue(map[string]any{}), "priority": 100, "updated_at": now}).Error; err != nil {
			return err
		}
		request.State = models.ContentStageQueued
		if err := podsflow.Resume(tx, item); err != nil {
			return err
		}
		if err := appendEvent(tx, request, nil, "manual_atomization_retry_approved", map[string]any{"actor": actor, "previous_failure": previousFailure, "new_budget_started_at": now}); err != nil {
			return err
		}
		return tx.Model(&item).Updates(map[string]any{"chaptering_status": "planning", "manual_atomization_requested_at": now, "updated_at": now}).Error
	})
	return request, handled, err
}

// Legacy plans without durable ownership cannot be adopted merely by assigning
// a request ID: their input and plan digests used a different contract. Retire
// only proven-unstarted plans; preserve every row and audit the replacement.
func retireUnstartedLegacyAtomization(tx *gorm.DB, request models.ContentStageRequest, actor string) error {
	var generations []models.AtomizationGeneration
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND parent_content_item_id=? AND content_stage_request_id IS NULL AND state<>'superseded'", request.TenantID, request.ContentItemID).Order("public_id ASC").Find(&generations).Error; err != nil {
		return err
	}
	for _, gen := range generations {
		if gen.State == "active" || gen.CompletedUnits != 0 {
			return fmt.Errorf("legacy generation %s has completed output; explicit ownership reconciliation required", gen.PublicID)
		}
		var units []models.AtomizationChapterUnit
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND generation_id=?", request.TenantID, gen.PublicID).Find(&units).Error; err != nil {
			return err
		}
		if len(units) != gen.ExpectedUnits {
			return fmt.Errorf("legacy generation %s has incomplete unit evidence", gen.PublicID)
		}
		for _, unit := range units {
			if !legacyUnitNeverStarted(unit) {
				return fmt.Errorf("legacy generation %s has attempted work; ownership reconciliation required", gen.PublicID)
			}
		}
		var evidence int64
		// Include deleted manifests: deleting an object is not proof that this
		// generation never performed an effect.
		if err := tx.Model(&models.MediaArtifactManifest{}).Where("tenant_id=? AND (atomization_generation_id=? OR atomization_chapter_unit_id IN (SELECT public_id FROM atomization_chapter_units WHERE tenant_id=? AND generation_id=?))", request.TenantID, gen.PublicID, request.TenantID, gen.PublicID).Count(&evidence).Error; err != nil {
			return err
		}
		if evidence > 0 {
			return fmt.Errorf("legacy generation %s has artifact history; ownership reconciliation required", gen.PublicID)
		}
		if err := tx.Model(&models.ContentItem{}).Where("tenant_id=? AND parent_content_item_id=? AND metadata->>'atomization_generation_id'=?", request.TenantID, request.ContentItemID, gen.PublicID.String()).Count(&evidence).Error; err != nil {
			return err
		}
		if evidence > 0 {
			return fmt.Errorf("legacy generation %s has child history; ownership reconciliation required", gen.PublicID)
		}
		// A scheduled/live legacy outer owner could still start work. This
		// compatibility recovery only handles orphaned plans, not owned work.
		if err := tx.Model(&models.AtomizationWorkRequest{}).Where("tenant_id=? AND public_id=?", request.TenantID, gen.WorkRequestID).Count(&evidence).Error; err != nil {
			return err
		}
		if evidence > 0 {
			return fmt.Errorf("legacy generation %s retains a work owner; ownership reconciliation required", gen.PublicID)
		}
		if err := tx.Model(&models.ContentStageAttempt{}).Where("tenant_id=? AND public_id=?", request.TenantID, gen.WorkRequestID).Count(&evidence).Error; err != nil {
			return err
		}
		if evidence > 0 {
			return fmt.Errorf("legacy generation %s retains an attempt owner; ownership reconciliation required", gen.PublicID)
		}
		if err := tx.Model(&models.AtomizationChapterUnit{}).Where("tenant_id=? AND generation_id=?", request.TenantID, gen.PublicID).Updates(map[string]any{"state": "superseded", "updated_at": time.Now().UTC()}).Error; err != nil {
			return err
		}
		if err := tx.Model(&gen).Updates(map[string]any{"state": "superseded", "updated_at": time.Now().UTC()}).Error; err != nil {
			return err
		}
		if err := appendEvent(tx, request, nil, "legacy_atomization_plan_retired", map[string]any{"actor": actor, "generation_id": gen.PublicID, "reason": "never_claimed_no_artifacts_no_children_no_owner"}); err != nil {
			return err
		}
	}
	return nil
}

func legacyUnitNeverStarted(unit models.AtomizationChapterUnit) bool {
	empty := func(value []byte) bool {
		var decoded any
		if len(value) == 0 {
			return true
		}
		if json.Unmarshal(value, &decoded) != nil {
			return false
		}
		switch v := decoded.(type) {
		case nil:
			return true
		case map[string]any:
			return len(v) == 0
		case []any:
			return len(v) == 0
		default:
			return false
		}
	}
	return unit.State == "queued" && unit.AttemptCount == 0 && unit.ClaimToken == nil && unit.FenceToken == nil && unit.EffectStartedAt == nil && unit.LeaseExpiresAt == nil && unit.CandidateContentItemID == nil && (empty(unit.Result) || legacyResultIsPlanOnly(unit)) && empty(unit.TerminalProof) && empty(unit.ArtifactManifestIDs)
}

// Generation creation historically wrote planner input into Result while the
// unit was still queued. Only that known input shape is safe to retire; URLs,
// artifact receipts, unknown fields and changed boundaries remain evidence.
func legacyResultIsPlanOnly(unit models.AtomizationChapterUnit) bool {
	var plan map[string]json.RawMessage
	if json.Unmarshal(unit.Result, &plan) != nil || len(plan) == 0 {
		return false
	}
	for key := range plan {
		switch key {
		case "title", "summary", "start_ms", "end_ms", "confidence", "context_label",
			"boundary_reason", "standalone_score", "needs_review_reason", "needs_review_code",
			"needs_review_codes", "contains_sponsor_intro":
		default:
			return false
		}
	}
	var start, end int64
	if json.Unmarshal(plan["start_ms"], &start) != nil || json.Unmarshal(plan["end_ms"], &end) != nil {
		return false
	}
	return start == unit.StartMs && end == unit.EndMs && start >= 0 && end > start
}

func replaceAtomizationIntent(tx *gorm.DB, item models.ContentItem, actor string) (models.ContentStageRequest, error) {
	var result models.ContentStageRequest
	disposition, reason, err := podsflow.Observe(tx, item)
	if err != nil {
		return result, err
	}
	if disposition == "active" || disposition == "reconciling" {
		return result, fmt.Errorf("episode must settle before replacement: %s", reason)
	}
	unresolved, err := podsflow.HasUnresolvedChapterEffects(tx, item)
	if err != nil {
		return result, err
	}
	if unresolved {
		return result, fmt.Errorf("existing effects must reconcile before replacement")
	}
	var previous []models.ContentStageRequest
	if err := tx.Where("tenant_id=? AND content_item_id=? AND processing_generation=?", item.TenantID, item.PublicID, item.ProcessingGeneration).Find(&previous).Error; err != nil {
		return result, err
	}
	for _, stage := range previous {
		if stage.State == models.ContentStageClaimed || stage.State == models.ContentStageRunning || stage.State == models.ContentStageVerifying || stage.State == models.ContentStageUncertain || stage.State == models.ContentStageReconciling {
			return result, fmt.Errorf("stage %s must settle before replacement", stage.Stage)
		}
	}
	oldGeneration := item.ProcessingGeneration
	item.ProcessingGeneration++
	requests, err := EnsureManifest(tx, &item)
	if err != nil {
		return result, err
	}
	if err := tx.Model(&models.AtomizationGeneration{}).Where("tenant_id=? AND parent_content_item_id=? AND processing_generation=? AND state<>'active'", item.TenantID, item.PublicID, oldGeneration).Update("state", "superseded").Error; err != nil {
		return result, err
	}
	now := time.Now().UTC()
	for _, next := range requests {
		if next.Stage == models.ContentStagePodsAtomization {
			result = next
			continue
		}
		for _, old := range previous {
			if old.Stage != next.Stage || old.State != models.ContentStageVerified || old.InputFingerprint != next.InputFingerprint {
				continue
			}
			present, proof, err := artifactPresent(tx, item, next.Stage)
			if err != nil {
				return result, err
			}
			if !present {
				continue
			}
			if err := tx.Model(&next).Updates(map[string]any{"state": models.ContentStageVerified, "verified_at": now, "finished_at": now, "terminal_proof": jsonValue(proof)}).Error; err != nil {
				return result, err
			}
			if err := appendEvent(tx, next, nil, "verified_from_predecessor", map[string]any{"previous_request_id": old.PublicID, "previous_generation": oldGeneration}); err != nil {
				return result, err
			}
		}
	}
	if result.PublicID == uuid.Nil {
		return result, fmt.Errorf("replacement atomization intent missing")
	}
	for _, old := range previous {
		if old.State == models.ContentStageQueued || old.State == models.ContentStageBlocked || old.State == models.ContentStageDeferred || old.State == models.ContentStageAwaitingApproval {
			if err := supersede(tx, old, "explicit_atomization_replacement"); err != nil {
				return result, err
			}
		}
	}
	if err := promoteReadyDependents(tx, item); err != nil {
		return result, err
	}
	if err := tx.Model(&result).Updates(map[string]any{"priority": 100, "accepted_at": now}).Error; err != nil {
		return result, err
	}
	if err := appendEvent(tx, result, nil, "manual_atomization_retry_approved", map[string]any{"actor": actor, "replacement_of_generation": oldGeneration}); err != nil {
		return result, err
	}
	if err := podsflow.Resume(tx, item); err != nil {
		return result, err
	}
	err = tx.Where("public_id=? AND tenant_id=?", result.PublicID, item.TenantID).First(&result).Error
	return result, err
}
