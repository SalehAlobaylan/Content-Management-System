package controllers

import (
	"encoding/json"
	"fmt"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/podsreset"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

func splitPodsResetProtectionBlockers(blockers []podsResetBlocker, codes []string) (remaining, overridden []podsResetBlocker) {
	selected := make(map[string]bool, len(codes))
	for _, code := range codes {
		selected[code] = true
	}
	remaining = []podsResetBlocker{}
	overridden = []podsResetBlocker{}
	for _, blocker := range blockers {
		if selected[blocker.Code] {
			overridden = append(overridden, blocker)
		} else {
			remaining = append(remaining, blocker)
		}
	}
	return
}

func applyPodsResetProtectionOverrides(targets []podsResetPlanTarget, codes []string) {
	for index := range targets {
		target := &targets[index]
		target.Blockers, target.OverriddenBlockers = splitPodsResetProtectionBlockers(target.Blockers, codes)
		for _, blocker := range target.OverriddenBlockers {
			for decisionIndex := range target.Decisions {
				decision := &target.Decisions[decisionIndex]
				if decision.Class == "editorial_transcript_or_chapter" && (blocker.Code == "approved_transcript" || blocker.Code == "manual_chapter_metadata") {
					decision.Disposition = podsreset.DeleteOwnedPayload
					decision.Reason = "administrator explicitly overrode the recorded editorial protection"
					decision.Postcondition = "exclusively owned approved captions and chapters are cleared; shared transcript payload and approval provenance remain"
				}
				if decision.Class == "user_or_moderation_data" && blocker.Code == "protected_content" {
					decision.Disposition = podsreset.RetainIdentity
					decision.Reason = "administrator explicitly allowed content retirement despite the recorded protections"
					decision.Postcondition = "interaction, hold, report and flag records remain linked to the retired content identity"
				}
				if decision.Class == "active_work" && (blocker.Code == "content_not_terminal" || blocker.Code == "active_content_stage" || blocker.Code == "active_atomization_run" || blocker.Code == "active_circulation_recommendation") {
					decision.Disposition = podsreset.DetachRecompute
					decision.Reason = "administrator explicitly approved discarding the selected unfinished records"
					decision.Postcondition = "selected stage requests are cancelled, legacy atomization runs failed and circulation recommendations superseded after the retirement fence; attempt history is retained and new writes rejected"
				}
				if decision.Class == "artifact_objects" && blocker.Code == "artifact_ownership_unproven" {
					decision.Reason = "administrator approved the exact verified content-folder inventory despite stale registry metadata"
					decision.Postcondition = "every frozen key is absent in every configured tier; external file references prevent deletion"
				}
			}
		}
	}
}

func podsResetDiscardApprovedWork(tx *gorm.DB, run models.PodsResetRun, id uuid.UUID) error {
	for _, change := range []struct {
		code, table, predicate string
		updates                map[string]interface{}
	}{
		{"active_content_stage", "content_stage_requests", "content_item_id=? AND state NOT IN ('verified','deferred','failed','cancelled','superseded')", map[string]interface{}{"state": "cancelled", "cancellation_requested_at": time.Now().UTC(), "cancellation_reason": "discarded by approved fresh-start reset", "claim_epoch": gorm.Expr("claim_epoch + 1"), "claim_token": nil, "claim_expires_at": nil}},
		{"active_atomization_run", "media_atomization_runs", "parent_content_item_id=? AND status IN ('queued','processing','running')", map[string]interface{}{"status": "failed", "phase": "reset_cancelled", "completed_at": time.Now().UTC(), "error_message": "discarded by an explicitly approved fresh-start reset"}},
		{"active_circulation_recommendation", "media_circulation_recommendations", "unit_type='item_family' AND subject_id=? AND status NOT IN ('applied','dismissed','superseded')", map[string]interface{}{"status": "superseded"}},
	} {
		remaining, err := podsResetRevalidateProtectionOverrides(run, id, []podsResetBlocker{{Code: change.code}})
		if err != nil {
			return err
		}
		if len(remaining) != 0 {
			continue
		}
		change.updates["updated_at"] = time.Now().UTC()
		if err := tx.Table(change.table).Where("tenant_id=? AND ("+change.predicate+")", run.TenantID, id).Updates(change.updates).Error; err != nil {
			return err
		}
	}
	return nil
}

func podsResetRevalidateArtifactOwnership(db *gorm.DB, run models.PodsResetRun, id uuid.UUID, objects []podsreset.ObjectIdentity) error {
	var manifest struct {
		IDs []uuid.UUID `json:"requested_ids"`
	}
	if err := json.Unmarshal(run.Manifest, &manifest); err != nil || len(manifest.IDs) == 0 {
		return fmt.Errorf("approved file selection is unavailable")
	}
	err := podsResetArtifactOwnershipBlockers(db, run.TenantID, id, objects)
	if err == nil {
		return podsResetExclusiveFileScope(db, run.TenantID, id, objects, uuidSet(manifest.IDs))
	}
	remaining, overrideErr := podsResetRevalidateProtectionOverrides(run, id, []podsResetBlocker{{Code: "artifact_ownership_unproven"}})
	if overrideErr != nil {
		return overrideErr
	}
	if len(remaining) != 0 {
		return err
	}
	return podsResetExclusiveFileScope(db, run.TenantID, id, objects, uuidSet(manifest.IDs))
}

// The explicit legacy-registry override never grants rights over another
// content item's files. Inventory and exact UUID folder validation stay required.
func podsResetExclusiveFileScope(db *gorm.DB, tenant string, id uuid.UUID, objects []podsreset.ObjectIdentity, selected map[uuid.UUID]bool) error {
	if err := podsreset.ValidateObjectSet(id.String(), objects); err != nil {
		return err
	}
	if len(objects) == 0 {
		return nil
	}
	identities := make([][]interface{}, 0, len(objects))
	for _, object := range objects {
		identities = append(identities, []interface{}{object.StorageTier, object.Bucket, object.ObjectKey})
	}
	var shared int64
	if err := db.Table("media_artifact_manifests").Where("(storage_tier,bucket,object_key) IN ? AND state<>'deleted' AND deleted_at IS NULL", identities).
		Where("NOT COALESCE(tenant_id=? AND (content_item_id=? OR (content_item_id IS NULL AND parent_content_item_id=?)), FALSE)", tenant, id, id).Count(&shared).Error; err != nil {
		return err
	}
	if shared > 0 {
		return fmt.Errorf("verified folder contains objects referenced by another artifact owner")
	}
	prefix := "content/" + id.String() + "/"
	selectedIDs := []uuid.UUID{id}
	for selectedID, included := range selected {
		if included && selectedID != id {
			selectedIDs = append(selectedIDs, selectedID)
		}
	}
	for _, pattern := range []string{"%" + prefix + "%", "%content%2f" + id.String() + "%2f%"} {
		if err := db.Table("content_items").Where("public_id<>?", id).
			Where("NOT (tenant_id=? AND public_id IN ?)", tenant, selectedIDs).
			Where("lower(concat_ws(' ',media_url,thumbnail_url,playback_url,fallback_playback_url,media_renditions::text)) LIKE ?", pattern).Count(&shared).Error; err != nil {
			return err
		}
		if shared > 0 {
			return fmt.Errorf("verified folder is referenced by content outside the approved selection")
		}
	}
	return nil
}

// Selected chapter references may share their parent's folder only after every
// selected identity is permanently fenced and hidden. No provider deletion may
// start while a selected reference can still be served or accept new work.
func podsResetSelectionFenced(db *gorm.DB, run models.PodsResetRun) (bool, error) {
	var manifest struct {
		IDs []uuid.UUID `json:"requested_ids"`
	}
	if err := json.Unmarshal(run.Manifest, &manifest); err != nil || len(manifest.IDs) == 0 || run.FencingToken == nil {
		return false, fmt.Errorf("approved retirement selection is unavailable")
	}
	var count int64
	err := db.Table("pods_reset_items item").
		Joins("JOIN pods_reset_retirements retirement ON retirement.content_item_id=item.content_item_id AND retirement.tenant_id=item.tenant_id").
		Joins("JOIN content_items content ON content.public_id=item.content_item_id AND content.tenant_id=item.tenant_id").
		Where("item.run_id=? AND item.tenant_id=? AND item.content_item_id IN ? AND item.state IN ('fenced','verification_pending','objects_deleted','complete')", run.PublicID, run.TenantID, manifest.IDs).
		Where("retirement.run_public_id=? AND retirement.manifest_hash=? AND retirement.fencing_token=? AND content.status='ARCHIVED' AND content.feed_visibility='hidden'", run.PublicID, run.ManifestHash, run.FencingToken).
		Count(&count).Error
	return count == int64(len(manifest.IDs)), err
}

// Only a protection observed for this exact item in the approved manifest may
// be bypassed on revalidation. A newly encountered protection still blocks.
func podsResetRevalidateProtectionOverrides(run models.PodsResetRun, id uuid.UUID, blockers []podsResetBlocker) ([]podsResetBlocker, error) {
	var manifest struct {
		Codes   []string              `json:"protection_override_codes"`
		Targets []podsResetPlanTarget `json:"targets"`
	}
	if err := json.Unmarshal(run.Manifest, &manifest); err != nil {
		return nil, err
	}
	codes, err := podsreset.ValidateProtectionOverrides(manifest.Codes)
	if err != nil {
		return nil, err
	}
	if len(codes) == 0 {
		return blockers, nil
	}
	for _, target := range manifest.Targets {
		if target.ID != id {
			continue
		}
		observed := []string{}
		for _, blocker := range target.OverriddenBlockers {
			if !containsString(codes, blocker.Code) {
				return nil, fmt.Errorf("item override is outside the approved protection choices")
			}
			observed = append(observed, blocker.Code)
		}
		remaining, _ := splitPodsResetProtectionBlockers(blockers, observed)
		return remaining, nil
	}
	return nil, fmt.Errorf("approved protection target is missing")
}
