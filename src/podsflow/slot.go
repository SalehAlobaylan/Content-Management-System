// Package podsflow owns installation-wide episode-family execution admission.
// It does not enqueue work or execute media effects.
package podsflow

import (
	"content-management-system/src/models"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"time"
)

// A queued request normally keeps its admitted family at the front of the
// execution order. Once it has sat without an effect for this long, however,
// it is almost always a dead delivery (for example an ARQ payload whose CMS
// lease expired before the worker started). Treating that row as yielded lets
// another family make progress while CMS recovery requeues the owner.
const queuedAdmissionYieldAfter = 5 * time.Minute

type Slot struct {
	Singleton            bool `gorm:"primaryKey"`
	TenantID             *string
	RootContentItemID    *uuid.UUID
	ProcessingGeneration int64
	Epoch                int64
	UpdatedAt            time.Time
}

func (Slot) TableName() string { return "pods_episode_execution_slot" }

type Disposition struct {
	TenantID             string    `gorm:"primaryKey" json:"tenant_id"`
	RootContentItemID    uuid.UUID `gorm:"primaryKey" json:"root_content_item_id"`
	ProcessingGeneration int64     `gorm:"primaryKey" json:"processing_generation"`
	Disposition          string    `json:"disposition"`
	Reason               string    `json:"reason"`
	UpdatedAt            time.Time `json:"updated_at"`
}

func (Disposition) TableName() string { return "pods_episode_dispositions" }

func family(tx *gorm.DB, tenant string, root uuid.UUID) *gorm.DB {
	return tx.Table("content_stage_requests r").Joins("JOIN content_items i ON i.tenant_id=r.tenant_id AND i.public_id=r.content_item_id AND i.processing_generation=r.processing_generation").Where("i.tenant_id=? AND (i.public_id=? OR i.parent_content_item_id=?) AND i.status<>'ARCHIVED' AND (i.parent_content_item_id IS NULL OR EXISTS (SELECT 1 FROM atomization_generations g JOIN content_items root ON root.public_id=g.parent_content_item_id AND root.tenant_id=g.tenant_id WHERE g.public_id::text=i.metadata->>'atomization_generation_id' AND g.tenant_id=i.tenant_id AND g.processing_generation=root.processing_generation AND g.state<>'superseded'))", tenant, root, root)
}

// Observe uses only durable current-generation state. An expired effect is
// still an effect until reconciliation supplies a terminal outcome.
func Observe(tx *gorm.DB, item models.ContentItem) (string, string, error) {
	var n int64
	if err := family(tx, item.TenantID, item.PublicID).Where("r.state IN ?", []string{"claimed", "running", "verifying", "uncertain", "reconciling"}).Count(&n).Error; err != nil {
		return "", "", err
	}
	if n > 0 {
		return "active", "effects pending or reconciling", nil
	}
	if err := tx.Table("atomization_chapter_units u").Joins("JOIN atomization_generations g ON g.public_id=u.generation_id AND g.tenant_id=u.tenant_id").Where("g.tenant_id=? AND g.parent_content_item_id=? AND u.state IN ?", item.TenantID, item.PublicID, []string{"claimed", "running", "verifying", "uncertain"}).Count(&n).Error; err != nil {
		return "", "", err
	}
	if n > 0 {
		return "reconciling", "chapter effects have not settled", nil
	}
	if err := tx.Model(&models.MediaArtifactManifest{}).Where("tenant_id=? AND parent_content_item_id=? AND state IN ? AND deleted_at IS NULL", item.TenantID, item.PublicID, []string{"uploading", "uploaded", "uncertain"}).Count(&n).Error; err != nil {
		return "", "", err
	}
	if n > 0 {
		return "reconciling", "object outcomes are unresolved", nil
	}
	if err := tx.Table("transcription_segment_units u").Joins("JOIN transcription_generations g ON g.public_id=u.generation_id AND g.tenant_id=u.tenant_id").Where("g.tenant_id=? AND g.content_item_id=? AND u.state IN ?", item.TenantID, item.PublicID, []string{"claimed", "running", "verifying", "uncertain"}).Count(&n).Error; err != nil {
		return "", "", err
	}
	if n > 0 {
		return "reconciling", "transcription effects have not settled", nil
	}
	if err := tx.Model(&models.AtomizationWorkRequest{}).Where("tenant_id=? AND parent_content_item_id=? AND state IN ?", item.TenantID, item.PublicID, []string{"claimed", "running", "verifying", "uncertain"}).Count(&n).Error; err != nil {
		return "", "", err
	}
	if n > 0 {
		return "active", "governed effects pending", nil
	}
	if item.Status == models.ContentStatusArchived {
		return "cancelled", "archived", nil
	}
	// Acquisition may return to approval after absence reconciliation or a
	// policy change. Its blocked descendants cannot progress until an operator
	// admits it again, so a quiescent family must not monopolize the slot.
	// Keep this after every unsettled-effect check above.
	if err := family(tx, item.TenantID, item.PublicID).Where("r.stage=? AND r.state=?", models.ContentStagePodsMediaArtifacts, models.ContentStageAwaitingApproval).Count(&n).Error; err != nil {
		return "", "", err
	}
	if n > 0 {
		return "parked_review", "media acquisition approval required", nil
	}
	if err := family(tx, item.TenantID, item.PublicID).Where("r.blocking_scope<>? AND r.state=?", models.ContentStageBlockingOptional, models.ContentStageFailed).Count(&n).Error; err != nil {
		return "", "", err
	}
	if n > 0 {
		return "parked_failed", "required stage exhausted its budget", nil
	}
	if err := family(tx, item.TenantID, item.PublicID).Where("r.stage=? AND r.state=?", models.ContentStagePodsTranscript, models.ContentStageAwaitingApproval).Count(&n).Error; err != nil {
		return "", "", err
	}
	if n > 0 {
		return "parked_transcript", "generated transcript approval required", nil
	}
	if err := family(tx, item.TenantID, item.PublicID).Where("r.blocking_scope<>? AND r.state=?", models.ContentStageBlockingOptional, models.ContentStageDeferred).Count(&n).Error; err != nil {
		return "", "", err
	}
	if n > 0 {
		return "yielded", "deferred work rejoins global admission order", nil
	}
	// A fresh queued request is still protected by the FIFO admission rule (and
	// is covered by the DB tests). An old queued request with no active effect is
	// different: its delivery may have expired before execution, so retaining
	// the global slot would block every other episode until an operator notices.
	// CMS lease recovery remains authoritative; this only releases admission.
	if err := family(tx, item.TenantID, item.PublicID).
		Where("r.blocking_scope<>? AND r.state=? AND (r.not_before_at IS NOT NULL OR r.updated_at<=?)", models.ContentStageBlockingOptional, models.ContentStageQueued, time.Now().UTC().Add(-queuedAdmissionYieldAfter)).
		Count(&n).Error; err != nil {
		return "", "", err
	}
	if n > 0 {
		return "yielded", "queued delivery is stale or waiting for worker capacity", nil
	}
	if err := family(tx, item.TenantID, item.PublicID).Where("r.blocking_scope<>? AND r.state NOT IN ?", models.ContentStageBlockingOptional, []string{"verified", "cancelled", "superseded"}).Count(&n).Error; err != nil {
		return "", "", err
	}
	if n > 0 {
		return "active", "required stages remain", nil
	}
	if err := tx.Model(&models.ContentItem{}).Where("tenant_id=? AND parent_content_item_id=? AND status<>'ARCHIVED' AND feed_visibility='review' AND metadata->>'atomization_generation_id' IN (SELECT public_id::text FROM atomization_generations WHERE tenant_id=? AND parent_content_item_id=? AND processing_generation=? AND state<>'superseded')", item.TenantID, item.PublicID, item.TenantID, item.PublicID, item.ProcessingGeneration).Count(&n).Error; err != nil {
		return "", "", err
	}
	if n > 0 {
		return "parked_review", "editorial approval required", nil
	}
	if item.DurationSec != nil && *item.DurationSec > 2400 {
		if err := tx.Model(&models.AtomizationGeneration{}).Where("tenant_id=? AND parent_content_item_id=? AND processing_generation=? AND state='active' AND expected_units>0 AND completed_units=expected_units", item.TenantID, item.PublicID, item.ProcessingGeneration).Count(&n).Error; err != nil {
			return "", "", err
		}
		if n != 1 {
			return "parked_failed", "current generation has not reached publication", nil
		}
	} else if item.Status != models.ContentStatusReady || item.FeedVisibility != "visible" {
		return "parked_review", "raw media not published", nil
	}
	return "published", "all required work settled", nil
}

// Acquire must run inside the caller transaction. All owners share this row;
// a heartbeat timeout alone never releases it.
func Acquire(tx *gorm.DB, item models.ContentItem) (bool, error) {
	root := item
	if item.ParentContentItemID != nil {
		root = models.ContentItem{}
		if err := tx.Where("tenant_id=? AND public_id=?", item.TenantID, item.ParentContentItemID).First(&root).Error; err != nil {
			return false, err
		}
		var current int64
		if err := tx.Model(&models.AtomizationGeneration{}).Where("tenant_id=? AND parent_content_item_id=? AND processing_generation=? AND state<>'superseded' AND public_id::text=(SELECT metadata->>'atomization_generation_id' FROM content_items WHERE tenant_id=? AND public_id=?)", root.TenantID, root.PublicID, root.ProcessingGeneration, item.TenantID, item.PublicID).Count(&current).Error; err != nil {
			return false, err
		}
		if current != 1 {
			return false, nil
		}
	}
	var slot Slot
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("singleton=true").First(&slot).Error; err != nil {
		return false, err
	}
	if slot.RootContentItemID == nil {
		// During rollout an empty new slot is not evidence that old workers are
		// idle. Unsettled effects from another family must first reconcile.
		var effects int64
		if err := tx.Table("content_stage_requests r").Joins("JOIN content_items i ON i.public_id=r.content_item_id AND i.tenant_id=r.tenant_id").Where("r.lane='pods' AND r.state IN ? AND NOT (i.tenant_id=? AND COALESCE(i.parent_content_item_id,i.public_id)=?)", []string{"claimed", "running", "verifying", "uncertain", "reconciling"}, root.TenantID, root.PublicID).Count(&effects).Error; err != nil {
			return false, err
		}
		if effects > 0 {
			return false, nil
		}
		if err := tx.Table("atomization_chapter_units u").Joins("JOIN atomization_generations g ON g.public_id=u.generation_id AND g.tenant_id=u.tenant_id").Where("u.state IN ? AND NOT (g.tenant_id=? AND g.parent_content_item_id=?)", []string{"claimed", "running", "verifying", "uncertain"}, root.TenantID, root.PublicID).Count(&effects).Error; err != nil {
			return false, err
		}
		if effects > 0 {
			return false, nil
		}
	}
	if slot.RootContentItemID != nil {
		if slot.TenantID != nil && *slot.TenantID == root.TenantID && *slot.RootContentItemID == root.PublicID {
			state, _, err := Observe(tx, root)
			if err != nil {
				return false, err
			}
			if state != "yielded" {
				if slot.ProcessingGeneration != root.ProcessingGeneration {
					if err := tx.Model(&slot).Updates(map[string]any{"processing_generation": root.ProcessingGeneration, "epoch": gorm.Expr("epoch+1"), "updated_at": time.Now().UTC()}).Error; err != nil {
						return false, err
					}
				}
				return true, nil
			}
		}
		var active models.ContentItem
		if err := tx.Where("tenant_id=? AND public_id=?", slot.TenantID, slot.RootContentItemID).First(&active).Error; err != nil {
			return false, err
		}
		if err := ActivateReady(tx, active); err != nil {
			return false, err
		}
		state, reason, err := Observe(tx, active)
		if err != nil {
			return false, err
		}
		if state == "active" || state == "reconciling" {
			return false, nil
		}
		if state == "yielded" {
			state = "active"
		}
		if err := tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(&Disposition{active.TenantID, active.PublicID, active.ProcessingGeneration, state, reason, time.Now().UTC()}).Error; err != nil {
			return false, err
		}
	}
	var prior Disposition
	if err := tx.Where("tenant_id=? AND root_content_item_id=? AND processing_generation=?", root.TenantID, root.PublicID, root.ProcessingGeneration).First(&prior).Error; err == nil && prior.Disposition != "active" {
		return false, nil
	} else if err != nil && err != gorm.ErrRecordNotFound {
		return false, err
	}
	// Decide across owners and stages while holding the installation-wide
	// slot lock. A faster download dispatcher must not beat queued atomization.
	winner, err := NextCandidate(tx)
	if err != nil {
		return false, err
	}
	if winner == nil || winner.TenantID != root.TenantID || winner.PublicID != root.PublicID {
		return false, nil
	}
	if err := tx.Model(&slot).Updates(map[string]any{"tenant_id": root.TenantID, "root_content_item_id": root.PublicID, "processing_generation": root.ProcessingGeneration, "epoch": gorm.Expr("epoch+1"), "updated_at": time.Now().UTC()}).Error; err != nil {
		return false, err
	}
	return true, tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(&Disposition{root.TenantID, root.PublicID, root.ProcessingGeneration, "active", "admitted", time.Now().UTC()}).Error
}

// NextCandidate is the shared admission order, independent of the caller's
// worker role. It is advisory for candidate loading and authoritative only
// when Acquire rechecks it under the global slot lock.
func NextCandidate(tx *gorm.DB) (*models.ContentItem, error) {
	var selected struct {
		TenantID string
		RootID   uuid.UUID
	}
	err := tx.Raw(`SELECT root.tenant_id, root.public_id AS root_id
		FROM content_stage_requests r
		JOIN content_items leaf ON leaf.tenant_id=r.tenant_id AND leaf.public_id=r.content_item_id AND leaf.processing_generation=r.processing_generation
		JOIN content_items root ON root.tenant_id=leaf.tenant_id AND root.public_id=COALESCE(leaf.parent_content_item_id,leaf.public_id)
		LEFT JOIN pods_episode_dispositions d ON d.tenant_id=root.tenant_id AND d.root_content_item_id=root.public_id AND d.processing_generation=root.processing_generation
		LEFT JOIN content_stage_controls ctl ON ctl.tenant_id=root.tenant_id AND ctl.lane='pods'
		WHERE r.lane='pods' AND r.state IN ('queued','deferred') AND r.blocking_scope<>'optional'
		AND (r.not_before_at IS NULL OR r.not_before_at<=NOW()) AND r.cancellation_requested_at IS NULL
		AND leaf.status<>'ARCHIVED' AND root.status<>'ARCHIVED' AND (d.disposition IS NULL OR d.disposition='active')
		AND COALESCE(ctl.scheduling_enabled,true) AND COALESCE(ctl.execution_enabled,true)
		AND (r.stage<>'pods_transcript' OR COALESCE(ctl.transcript_execution_enabled,true))
		AND (leaf.parent_content_item_id IS NULL OR EXISTS (SELECT 1 FROM atomization_generations g WHERE g.tenant_id=root.tenant_id AND g.parent_content_item_id=root.public_id AND g.public_id::text=leaf.metadata->>'atomization_generation_id' AND g.processing_generation=root.processing_generation AND g.state<>'superseded'))
		AND (r.dependency_manifest IS NULL OR jsonb_typeof(r.dependency_manifest) IN ('array','null'))
		AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements_text(CASE WHEN jsonb_typeof(r.dependency_manifest)='array' THEN r.dependency_manifest ELSE '[]'::jsonb END) dependency(stage)
			WHERE NOT EXISTS (SELECT 1 FROM content_stage_requests predecessor WHERE predecessor.tenant_id=r.tenant_id AND predecessor.content_item_id=r.content_item_id AND predecessor.processing_generation=r.processing_generation AND predecessor.stage=dependency.stage AND predecessor.state='verified'))
		AND (r.stage='pods_media_artifacts' OR EXISTS (SELECT 1 FROM content_stage_requests media WHERE media.tenant_id=r.tenant_id AND media.content_item_id=r.content_item_id AND media.processing_generation=r.processing_generation AND media.stage='pods_media_artifacts' AND media.state='verified'))
		GROUP BY root.tenant_id,root.public_id
		ORDER BY MAX(r.priority) DESC, MIN(COALESCE(r.accepted_at,r.created_at)) ASC,root.public_id ASC,root.tenant_id ASC LIMIT 1`).Scan(&selected).Error
	if err != nil {
		return nil, err
	}
	if selected.RootID == uuid.Nil {
		return nil, nil
	}
	return &models.ContentItem{TenantID: selected.TenantID, PublicID: selected.RootID}, nil
}

func Resume(tx *gorm.DB, item models.ContentItem) error {
	// Admission is durable intent, not a reservation of execution capacity.
	// Only the worker's fenced claim may acquire the global slot.
	if item.ParentContentItemID != nil {
		var root models.ContentItem
		if err := tx.Where("tenant_id=? AND public_id=?", item.TenantID, item.ParentContentItemID).First(&root).Error; err != nil {
			return err
		}
		item = root
	}
	return tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(&Disposition{item.TenantID, item.PublicID, item.ProcessingGeneration, "active", "operator resumed", time.Now().UTC()}).Error
}
