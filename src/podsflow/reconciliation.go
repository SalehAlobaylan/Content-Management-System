package podsflow

import (
	"content-management-system/src/models"
	"encoding/json"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"time"
)

// ReconcileAbsentUnits handles the no-upload case even when there are no
// manifests for the object observer to visit. Legacy unfenced effects wait.
func ReconcileAbsentUnits(tx *gorm.DB, root models.ContentItem) error {
	now := time.Now().UTC()
	var units []models.AtomizationChapterUnit
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Where("tenant_id=? AND generation_id IN (SELECT public_id FROM atomization_generations WHERE tenant_id=? AND parent_content_item_id=? AND processing_generation=?) AND state IN ? AND lease_expires_at<?", root.TenantID, root.TenantID, root.PublicID, root.ProcessingGeneration, []string{"running", "verifying", "uncertain"}, now).Find(&units).Error; err != nil {
		return err
	}
	var requeuedUnits []string
	var requeuedManifests []string
	for _, unit := range units {
		// Historical units without a persisted fence cannot be safely classified
		// as absent. Leave them for the explicit recovery report/operator path.
		if unit.FenceToken == nil {
			continue
		}
		if adopted, err := adoptVerifiedUnitReceipt(tx, unit); err != nil {
			return err
		} else if adopted {
			continue
		}
		// Count both live rows and historical rows. A prior implementation only
		// inspected live manifests, then classified a partial upload as
		// verified_absent after the worker lost its lease. That destroyed the
		// retry path even though the provider objects were still present.
		var history int64
		if err := tx.Model(&models.MediaArtifactManifest{}).Where("tenant_id=? AND atomization_chapter_unit_id=?", unit.TenantID, unit.PublicID).Count(&history).Error; err != nil {
			return err
		}
		var unresolved int64
		if err := tx.Model(&models.MediaArtifactManifest{}).Where("tenant_id=? AND atomization_chapter_unit_id=? AND deleted_at IS NULL AND state IN ?", unit.TenantID, unit.PublicID, []string{"uploading", "uploaded", "uncertain"}).Count(&unresolved).Error; err != nil {
			return err
		}
		if unresolved == 0 && history > 0 && unit.CandidateContentItemID == nil && !unitResultHasPlaybackReceipt(unit) {
			// Retiring objects is not a new admission. Keep the lifetime unit
			// attempt limit even when the old upload has been reconciled.
			if unit.AttemptCount >= 2 {
				if err := tx.Model(&models.AtomizationChapterUnit{}).Where("tenant_id=? AND public_id=?", unit.TenantID, unit.PublicID).Updates(map[string]any{"state": "failed", "failure_class": "partial_attempt_budget_exhausted", "claim_token": nil, "lease_expires_at": nil, "updated_at": now}).Error; err != nil {
					return err
				}
				continue
			}
			// The worker crossed the effect boundary but never persisted a
			// complete chapter receipt. Terminal manifests are not publishable on
			// their own; retire those exact objects and put only this unit back in
			// the sequential queue. This preserves the immutable audit trail while
			// allowing the next attempt to use a fresh fence and object prefix.
			var manifests []models.MediaArtifactManifest
			if err := tx.Where("tenant_id=? AND atomization_chapter_unit_id=?", unit.TenantID, unit.PublicID).Find(&manifests).Error; err != nil {
				return err
			}
			for _, manifest := range manifests {
				if manifest.DeletedAt != nil || manifest.State == "deleted" || manifest.State == "failed" || manifest.State == "cleanup_eligible" {
					continue
				}
				proof := map[string]any{
					"reconciled":     true,
					"object_present": true,
					"reason":         "partial_chapter_attempt_without_receipt",
					"unit_id":        unit.PublicID.String(),
				}
				changed := tx.Model(&models.MediaArtifactManifest{}).Where("tenant_id=? AND public_id=? AND state IN ?", manifest.TenantID, manifest.PublicID, []string{"verified", "active"}).Updates(map[string]any{
					"state":               "cleanup_eligible",
					"cleanup_eligible_at": now,
					"terminal_proof":      datatypes.JSON(mustJSON(proof)),
					"updated_at":          now,
				})
				if changed.Error != nil {
					return changed.Error
				}
				if changed.RowsAffected == 1 {
					requeuedManifests = append(requeuedManifests, manifest.PublicID.String())
				}
			}
			proof := map[string]any{
				"effect":       "reconciled",
				"reason":       "partial_chapter_attempt_without_receipt",
				"manifest_ids": requeuedManifests,
			}
			changed := tx.Model(&models.AtomizationChapterUnit{}).Where("tenant_id=? AND public_id=? AND state IN ? AND fence_token IS NOT DISTINCT FROM ? AND lease_expires_at<?", unit.TenantID, unit.PublicID, []string{"running", "verifying", "uncertain"}, unit.FenceToken, now).Updates(map[string]any{
				"state":                 "queued",
				"failure_class":         "partial_attempt_reconciled",
				"claim_token":           nil,
				"lease_expires_at":      nil,
				"effect_started_at":     nil,
				"not_before_at":         nil,
				"artifact_manifest_ids": datatypes.JSON([]byte("[]")),
				"terminal_proof":        datatypes.JSON(mustJSON(proof)),
				"updated_at":            now,
			})
			if changed.Error != nil {
				return changed.Error
			}
			if changed.RowsAffected == 1 {
				requeuedUnits = append(requeuedUnits, unit.PublicID.String())
				if err := tx.Model(&models.AtomizationGeneration{}).Where("tenant_id=? AND public_id=? AND state NOT IN ?", unit.TenantID, unit.GenerationID, []string{"active", "superseded"}).Update("state", "running").Error; err != nil {
					return err
				}
			}
			continue
		}
		if unresolved > 0 || unit.EffectStartedAt == nil {
			changed := tx.Model(&models.AtomizationChapterUnit{}).Where("tenant_id=? AND public_id=? AND state=? AND fence_token IS NOT DISTINCT FROM ? AND lease_expires_at<?", unit.TenantID, unit.PublicID, unit.State, unit.FenceToken, now).Update("state", "uncertain")
			if changed.Error != nil {
				return changed.Error
			}
			if changed.RowsAffected != 1 {
				return gorm.ErrRecordNotFound
			}
			continue
		}
		if history > 0 {
			// Any historical manifest is evidence that an effect was attempted.
			// Never turn that evidence into a false absence proof, even when the
			// provider object has already been deleted by a prior cleanup.
			continue
		}
		state := "queued"
		if unit.AttemptCount >= 2 {
			state = "failed"
		}
		proof, _ := json.Marshal(map[string]any{"effect": "absent", "reason": "no unresolved upload intents"})
		changed := tx.Model(&models.AtomizationChapterUnit{}).Where("tenant_id=? AND public_id=? AND state=? AND fence_token IS NOT DISTINCT FROM ? AND lease_expires_at<?", unit.TenantID, unit.PublicID, unit.State, unit.FenceToken, now).Updates(map[string]any{"state": state, "failure_class": "verified_absent", "claim_token": nil, "lease_expires_at": nil, "effect_started_at": nil, "not_before_at": now.Add(30 * time.Second), "terminal_proof": datatypes.JSON(proof)})
		if changed.Error != nil {
			return changed.Error
		}
		if changed.RowsAffected != 1 {
			return gorm.ErrRecordNotFound
		}
	}
	if len(requeuedUnits) > 0 {
		if err := appendRecoveryBoundary(tx, root, now, requeuedUnits, requeuedManifests); err != nil {
			return err
		}
	}
	return nil
}

func unitResultHasPlaybackReceipt(unit models.AtomizationChapterUnit) bool {
	var result map[string]any
	if len(unit.Result) == 0 || json.Unmarshal(unit.Result, &result) != nil {
		return false
	}
	for _, key := range []string{"media_url", "playback_url"} {
		if value, ok := result[key].(string); ok && value != "" {
			return true
		}
	}
	return false
}

func mustJSON(value any) []byte {
	raw, _ := json.Marshal(value)
	return raw
}

// A reconciliation boundary starts a fresh bounded execution budget without
// pretending that the old attempt succeeded. The lifecycle verifier counts
// effects after this event when deciding whether a retry is admissible.
func appendRecoveryBoundary(tx *gorm.DB, root models.ContentItem, now time.Time, unitIDs, manifestIDs []string) error {
	var request models.ContentStageRequest
	if err := tx.Where("tenant_id=? AND content_item_id=? AND processing_generation=? AND stage=?", root.TenantID, root.PublicID, root.ProcessingGeneration, models.ContentStagePodsAtomization).Order("updated_at DESC").First(&request).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil
		}
		return err
	}
	var sequence int64
	if err := tx.Model(&models.ContentStageEvent{}).Where("tenant_id=? AND request_id=?", request.TenantID, request.PublicID).Select("COALESCE(MAX(sequence),0)").Scan(&sequence).Error; err != nil {
		return err
	}
	payload := map[string]any{
		"reason":         "partial_chapter_attempt_without_receipt",
		"unit_ids":       unitIDs,
		"manifest_ids":   manifestIDs,
		"previous_state": request.State,
	}
	return tx.Create(&models.ContentStageEvent{
		PublicID: uuid.New(), TenantID: request.TenantID, RequestID: request.PublicID,
		Sequence: sequence + 1, EventType: "chapter_effects_reconciled",
		Payload: datatypes.JSON(mustJSON(payload)), OccurredAt: now,
	}).Error
}

func adoptVerifiedUnitReceipt(tx *gorm.DB, unit models.AtomizationChapterUnit) (bool, error) {
	if unit.FenceToken == nil {
		return false, nil
	}
	var ids []string
	var receipt struct {
		StartMs     int64  `json:"start_ms"`
		EndMs       int64  `json:"end_ms"`
		MediaURL    string `json:"media_url"`
		PlaybackURL string `json:"playback_url"`
	}
	if json.Unmarshal(unit.ArtifactManifestIDs, &ids) != nil || len(ids) == 0 || json.Unmarshal(unit.Result, &receipt) != nil || receipt.StartMs != unit.StartMs || receipt.EndMs != unit.EndMs {
		return false, nil
	}
	if len(uniqueIDs(ids)) != len(ids) {
		return false, nil
	}
	var manifests []models.MediaArtifactManifest
	if err := tx.Where("tenant_id=? AND atomization_generation_id=? AND atomization_chapter_unit_id=? AND unit_fence_token=? AND state IN ? AND deleted_at IS NULL", unit.TenantID, unit.GenerationID, unit.PublicID, unit.FenceToken, []string{"verified", "active"}).Find(&manifests).Error; err != nil {
		return false, err
	}
	if len(manifests) != len(ids) || !sameIDs(ids, manifests) {
		return false, nil
	}
	media, playback := false, false
	for _, manifest := range manifests {
		playback = playback || (receipt.PlaybackURL != "" && manifest.PublicURL == receipt.PlaybackURL)
		if manifest.PublicURL == receipt.MediaURL && receipt.MediaURL != "" && manifest.DurationMs != nil {
			drift := *manifest.DurationMs - (unit.EndMs - unit.StartMs)
			if drift < 0 {
				drift = -drift
			}
			media = drift <= 1000 && *manifest.DurationMs >= 270000 && *manifest.DurationMs <= 2400000
		}
	}
	if !media || !playback {
		return false, nil
	}
	proof, _ := json.Marshal(map[string]any{"adopted": true, "manifest_ids": ids})
	changed := tx.Model(&models.AtomizationChapterUnit{}).Where("tenant_id=? AND public_id=? AND state IN ? AND fence_token=? AND lease_expires_at<?", unit.TenantID, unit.PublicID, []string{"running", "verifying", "uncertain"}, unit.FenceToken, time.Now().UTC()).Updates(map[string]any{"state": "verified", "terminal_proof": datatypes.JSON(proof), "updated_at": time.Now().UTC()})
	if changed.Error != nil {
		return false, changed.Error
	}
	if changed.RowsAffected != 1 {
		return false, nil
	}
	return true, nil
}

func uniqueIDs(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		seen[value] = struct{}{}
	}
	result := make([]string, 0, len(seen))
	for value := range seen {
		result = append(result, value)
	}
	return result
}

func sameIDs(values []string, manifests []models.MediaArtifactManifest) bool {
	want := make(map[string]struct{}, len(values))
	for _, value := range values {
		want[value] = struct{}{}
	}
	for _, manifest := range manifests {
		if _, ok := want[manifest.PublicID.String()]; !ok {
			return false
		}
	}
	return len(want) == len(manifests)
}

func HasUnresolvedChapterEffects(tx *gorm.DB, root models.ContentItem) (bool, error) {
	var count int64
	if err := tx.Table("atomization_chapter_units u").Joins("JOIN atomization_generations g ON g.public_id=u.generation_id AND g.tenant_id=u.tenant_id").Where("g.tenant_id=? AND g.parent_content_item_id=? AND u.state IN ?", root.TenantID, root.PublicID, []string{"claimed", "running", "verifying", "uncertain"}).Count(&count).Error; err != nil {
		return false, err
	}
	if count > 0 {
		return true, nil
	}
	err := tx.Model(&models.MediaArtifactManifest{}).Where("tenant_id=? AND parent_content_item_id=? AND state IN ? AND deleted_at IS NULL", root.TenantID, root.PublicID, []string{"uploading", "uploaded", "uncertain"}).Count(&count).Error
	return count > 0, err
}
