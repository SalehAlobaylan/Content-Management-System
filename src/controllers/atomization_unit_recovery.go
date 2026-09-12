package controllers

import (
	"content-management-system/src/models"
	"encoding/json"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"time"
)

// Reconciliation never rotates authority over an unresolved external effect.
// Uploads register immutable intent before contacting storage, so no manifest
// after expired ownership proves that this unit started no managed upload.
func reconcileChapterUnits(tx *gorm.DB, generation string, now time.Time) error {
	q := tx.Model(&models.AtomizationChapterUnit{}).Where("state IN ? AND lease_expires_at<=?", []string{unitStateRunning, unitStateVerifying}, now)
	if generation != "" {
		q = q.Where("generation_id=?", generation)
	}
	if err := q.Updates(map[string]any{"state": unitStateUncertain, "failure_class": "lease_expired_after_effect", "updated_at": now}).Error; err != nil {
		return err
	}
	var units []models.AtomizationChapterUnit
	q = tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Where("state=?", unitStateUncertain)
	if generation != "" {
		q = q.Where("generation_id=?", generation)
	}
	if err := q.Limit(32).Find(&units).Error; err != nil {
		return err
	}
	for _, u := range units {
		if u.FenceToken == nil {
			// Historical units have no nested authority; do not infer ownership
			// from an object key during reconciliation.
			continue
		}
		var n int64
		if err := tx.Model(&models.MediaArtifactManifest{}).Where("tenant_id=? AND atomization_chapter_unit_id=? AND deleted_at IS NULL AND NOT (state='failed' AND terminal_proof @> '{\"reconciled\":true,\"object_present\":false}'::jsonb)", u.TenantID, u.PublicID).Count(&n).Error; err != nil {
			return err
		}
		if n == 0 {
			// A zero live-row count is not absence evidence when the unit has any
			// manifest history. A partial attempt may have already verified (or
			// later cleaned) an object while its chapter receipt was lost; only the
			// Pods reconciliation path may retire that history and requeue safely.
			var history int64
			if err := tx.Model(&models.MediaArtifactManifest{}).Where("tenant_id=? AND atomization_chapter_unit_id=?", u.TenantID, u.PublicID).Count(&history).Error; err != nil {
				return err
			}
			if history > 0 {
				continue
			}
			// Historical attempts did not persist effect-start or implement the
			// deadline controller. Their silence is not absence evidence.
			if u.EffectStartedAt == nil {
				continue
			}
			state := unitStateQueued
			if u.AttemptCount >= 2 {
				state = unitStateFailed
			}
			changed := tx.Model(&u).Where("tenant_id=? AND public_id=? AND state=? AND fence_token IS NOT DISTINCT FROM ?", u.TenantID, u.PublicID, unitStateUncertain, u.FenceToken).Updates(map[string]any{"state": state, "claim_token": nil, "lease_expires_at": nil, "failure_class": "verified_absent", "not_before_at": now.Add(30 * time.Second), "terminal_proof": longFormJSON(map[string]any{"effect": "absent", "reason": "no registered upload intents"})})
			if changed.Error != nil {
				return changed.Error
			}
			if changed.RowsAffected != 1 {
				continue
			}
			continue
		}
		var result atomizationChapterRequest
		if json.Unmarshal(u.Result, &result) != nil || result.PlaybackURL == nil {
			continue
		}
		var gen models.AtomizationGeneration
		var parent models.ContentItem
		if err := tx.Where("tenant_id=? AND public_id=?", u.TenantID, u.GenerationID).First(&gen).Error; err != nil {
			return err
		}
		if err := tx.Where("tenant_id=? AND public_id=?", u.TenantID, gen.ParentContentItemID).First(&parent).Error; err != nil {
			return err
		}
		ids := atomizationUnitManifestIDs(u)
		if len(ids) == 0 {
			continue
		}
		if len(uniqueStrings(ids)) != len(ids) {
			continue
		}
		var manifests []models.MediaArtifactManifest
		if err := tx.Where("tenant_id=? AND atomization_generation_id=? AND atomization_chapter_unit_id=? AND unit_fence_token=? AND state IN ? AND deleted_at IS NULL", u.TenantID, u.GenerationID, u.PublicID, u.FenceToken, []string{manifestStateVerified, manifestStateActive}).Find(&manifests).Error; err != nil {
			return err
		}
		if len(manifests) == len(ids) && sameStringSet(ids, manifestPublicIDs(manifests)) && int64(result.StartMs) == u.StartMs && int64(result.EndMs) == u.EndMs {
			changed := tx.Model(&u).Where("tenant_id=? AND public_id=? AND state=? AND fence_token=?", u.TenantID, u.PublicID, unitStateUncertain, u.FenceToken).Updates(map[string]any{"state": unitStateVerified, "terminal_proof": longFormJSON(map[string]any{"adopted": true, "manifest_ids": ids}), "updated_at": now})
			if changed.Error != nil {
				return changed.Error
			}
			if changed.RowsAffected != 1 {
				continue
			}
		}
	}
	return nil
}
