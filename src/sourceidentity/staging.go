package sourceidentity

import (
	"errors"
	"fmt"
	"time"

	"content-management-system/src/contentreset"
	"content-management-system/src/models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func verifyConsumedInstance(db *gorm.DB, identity models.SourceItemIdentity, grant models.ContentResetReconstructionGrant) error {
	if grant.ReplacementContentItemID == nil {
		return ErrIdentityCorrupt
	}
	var instance models.SourceItemInstance
	if err := db.Where("tenant_id=? AND identity_id=? AND instance_generation=? AND content_item_id=? AND campaign_id=?", identity.TenantID, identity.ID, grant.ReplacementInstanceGeneration, *grant.ReplacementContentItemID, grant.CampaignID).First(&instance).Error; err != nil {
		return ErrIdentityCorrupt
	}
	switch instance.State {
	case "staged":
		if identity.CurrentInstanceGeneration != grant.BaseInstanceGeneration || !sameOptionalUUID(identity.CurrentContentItemID, grant.TargetContentItemID) {
			return ErrReconstructionGrantScope
		}
	case "active":
		if identity.CurrentInstanceGeneration != grant.ReplacementInstanceGeneration || !sameOptionalUUID(identity.CurrentContentItemID, grant.ReplacementContentItemID) {
			return ErrReconstructionGrantScope
		}
	default:
		return ErrReconstructionGrantScope
	}
	return nil
}

// RequireRunningCampaign is the admission check for new reconstruction grants.
// Already-admitted owner work may finish during a pause; issuing new grants may
// not. A bare campaign state is insufficient authorization.
func RequireRunningCampaign(tx *gorm.DB, campaign models.ContentResetCampaign, revision models.ContentResetRevision) error {
	var run models.ContentResetExecution
	if err := tx.Where("tenant_id=? AND campaign_id=? AND revision_id=?", campaign.TenantID, campaign.ID, revision.ID).First(&run).Error; err != nil {
		return ErrReconstructionGrantScope
	}
	if run.StartedAt == nil || run.PauseRequested || revision.ManifestHash == nil || run.ManifestHash != *revision.ManifestHash || run.PublishedAt != nil {
		return ErrReconstructionGrantScope
	}
	if err := contentreset.ValidateExecutionEnvironment(tx, run); err != nil {
		return ErrReconstructionGrantScope
	}
	return nil
}

// PromoteStagedInstances changes only source identity routing. The publication
// owner MUST call it in the transaction which CAS-publishes every selected feed
// head and routing branch, after source handoff/readiness validation. This helper
// does not authorize publication or provide readiness proof.
//
// The caller holds the campaign lock before locking source identities, and must
// lock identities before feed heads. Grant consumption takes the same campaign
// lock, preventing a new staged instance from appearing between pages.
func PromoteStagedInstances(tx *gorm.DB, tenant string, campaignID, revisionID uint) (int64, error) {
	if tx == nil || tenant == "" || campaignID == 0 || revisionID == 0 {
		return 0, ErrReconstructionGrantScope
	}
	var campaign models.ContentResetCampaign
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND id=?", tenant, campaignID).First(&campaign).Error; err != nil {
		return 0, err
	}
	if campaign.State != "executing" || campaign.Operation != "fresh_start" {
		return 0, ErrReconstructionGrantScope
	}
	var run models.ContentResetExecution
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND campaign_id=? AND revision_id=?", tenant, campaignID, revisionID).First(&run).Error; err != nil {
		return 0, err
	}
	if run.StartedAt == nil || run.Phase != "publishing" || run.PauseRequested || run.PublishedAt != nil {
		return 0, ErrReconstructionGrantScope
	}
	var revision models.ContentResetRevision
	if err := tx.Where("tenant_id=? AND campaign_id=? AND id=? AND revision=?", tenant, campaignID, revisionID, campaign.CurrentRevision).First(&revision).Error; err != nil {
		return 0, err
	}
	if revision.ManifestHash == nil || run.ManifestHash != *revision.ManifestHash {
		return 0, ErrReconstructionGrantScope
	}
	var cursor uint
	var promoted int64
	for {
		var page []models.SourceItemInstance
		if err := tx.Where("tenant_id=? AND campaign_id=? AND state='staged' AND id>?", tenant, campaignID, cursor).Order("id ASC").Limit(500).Find(&page).Error; err != nil {
			return 0, err
		}
		if len(page) == 0 {
			break
		}
		for _, instance := range page {
			var identity models.SourceItemIdentity
			if err := tx.Where("tenant_id=? AND id=?", tenant, instance.IdentityID).First(&identity).Error; err != nil {
				return 0, err
			}
			lock := identityLockKey(&Input{TenantID: tenant, ContentSourceID: identity.ContentSourceID, UpstreamItemID: identity.UpstreamItemID})
			if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?,0))", lock).Error; err != nil {
				return 0, err
			}
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND id=?", tenant, instance.IdentityID).First(&identity).Error; err != nil {
				return 0, err
			}
			var grant models.ContentResetReconstructionGrant
			if err := tx.Where("tenant_id=? AND campaign_id=? AND revision_id=? AND identity_id=? AND replacement_instance_generation=? AND replacement_content_item_id=? AND state='consumed'", tenant, campaignID, revisionID, identity.ID, instance.InstanceGeneration, instance.ContentItemID).First(&grant).Error; err != nil {
				return 0, ErrReconstructionGrantScope
			}
			if err := verifyConsumedInstance(tx, identity, grant); err != nil {
				return 0, err
			}
			if grant.TargetContentItemID != nil {
				var old models.SourceItemInstance
				if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND identity_id=? AND instance_generation=? AND content_item_id=?", tenant, identity.ID, grant.BaseInstanceGeneration, *grant.TargetContentItemID).First(&old).Error; err != nil {
					return 0, err
				}
				if old.State == "active" {
					if err := tx.Model(&old).Update("state", "superseded").Error; err != nil {
						return 0, err
					}
				} else if old.State != "retired" {
					return 0, ErrReconstructionGrantScope
				}
			}
			var exists int64
			if err := tx.Model(&models.ContentItem{}).Where("tenant_id=? AND public_id=?", tenant, instance.ContentItemID).Count(&exists).Error; err != nil {
				return 0, err
			}
			if exists != 1 {
				return 0, errors.New("staged replacement payload is missing")
			}
			result := tx.Model(&instance).Where("state='staged'").Update("state", "active")
			if result.Error != nil {
				return 0, result.Error
			}
			if result.RowsAffected != 1 {
				return 0, ErrReconstructionGrantScope
			}
			result = tx.Model(&identity).Where("current_instance_generation=?", grant.BaseInstanceGeneration).Updates(map[string]any{"current_instance_generation": instance.InstanceGeneration, "current_content_item_id": instance.ContentItemID, "updated_at": time.Now().UTC()})
			if result.Error != nil {
				return 0, result.Error
			}
			if result.RowsAffected != 1 {
				return 0, fmt.Errorf("source identity publication fence lost: %w", ErrReconstructionGrantScope)
			}
			promoted++
			cursor = instance.ID
		}
	}
	return promoted, nil
}

// DemotePublishedInstances reverses instance promotion for an approved
// rollback. It only runs with the publication/rollback owner's transaction and
// refuses identities that had no prior instance (a brand-new discovery has no
// old view to restore; rollback must use forward reconciliation instead).
// Head switching is the caller's responsibility after this transaction step.
func DemotePublishedInstances(tx *gorm.DB, tenant string, campaignID, revisionID uint) (int64, error) {
	if tx == nil || tenant == "" || campaignID == 0 || revisionID == 0 {
		return 0, ErrReconstructionGrantScope
	}
	var rev models.ContentResetRevision
	if err := tx.Where("tenant_id=? AND id=? AND campaign_id=?", tenant, revisionID, campaignID).First(&rev).Error; err != nil {
		return 0, err
	}
	// Transaction-local fence: the canonical instance trigger authorizes
	// active->staged only for instances bound to this exact campaign while the
	// admitted rollback owner holds the transaction.
	if err := tx.Exec("SELECT set_config('wahb.content_reset.rollback_campaign', ?, true)", fmt.Sprint(campaignID)).Error; err != nil {
		return 0, err
	}
	var cursor uint
	var demoted int64
	for {
		var page []models.SourceItemInstance
		if err := tx.Where("tenant_id=? AND campaign_id=? AND state='active' AND id>?", tenant, campaignID, cursor).Order("id ASC").Limit(500).Find(&page).Error; err != nil {
			return 0, err
		}
		if len(page) == 0 {
			return demoted, nil
		}
		for _, instance := range page {
			var grant models.ContentResetReconstructionGrant
			if err := tx.Where("tenant_id=? AND campaign_id=? AND revision_id=? AND identity_id=? AND replacement_instance_generation=? AND replacement_content_item_id=? AND state='consumed'",
				tenant, campaignID, revisionID, instance.IdentityID, instance.InstanceGeneration, instance.ContentItemID).First(&grant).Error; err != nil {
				return 0, ErrReconstructionGrantScope
			}
			if grant.TargetContentItemID == nil {
				return 0, errors.New("rollback cannot restore a newly discovered identity; forward reconciliation is required")
			}
			var identity models.SourceItemIdentity
			lock := identityLockKeyLocked(tx, tenant, instance.IdentityID)
			if lock == "" {
				return 0, ErrIdentityCorrupt
			}
			if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?,0))", lock).Error; err != nil {
				return 0, err
			}
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND id=?", tenant, instance.IdentityID).First(&identity).Error; err != nil {
				return 0, err
			}
			old := models.SourceItemInstance{}
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND identity_id=? AND instance_generation=? AND content_item_id=?",
				tenant, identity.ID, grant.BaseInstanceGeneration, *grant.TargetContentItemID).First(&old).Error; err != nil {
				return 0, err
			}
			if identity.CurrentInstanceGeneration != instance.InstanceGeneration {
				return 0, ErrReconstructionGrantScope
			}
			// Only a promoted replacement (its prior instance is superseded)
			// may roll back. A permanently retired prior instance must never be
			// revived; that requires forward reconciliation.
			if old.State != "superseded" {
				return 0, errors.New("rollback cannot revive permanently retired content; forward reconciliation is required")
			}
			var oldPayload int64
			if err := tx.Model(&models.ContentItem{}).Where("tenant_id=? AND public_id=? AND retired_payload_at IS NULL", tenant, old.ContentItemID).Count(&oldPayload).Error; err != nil {
				return 0, err
			}
			if oldPayload != 1 {
				return 0, errors.New("rollback requires intact old content payload")
			}
			result := tx.Model(&instance).Where("state='active'").Update("state", "staged")
			if result.Error != nil {
				return 0, result.Error
			}
			if result.RowsAffected != 1 {
				return 0, ErrReconstructionGrantScope
			}
			result = tx.Model(&old).Where("state='superseded'").Update("state", "active")
			if result.Error != nil {
				return 0, result.Error
			}
			if result.RowsAffected != 1 {
				return 0, ErrReconstructionGrantScope
			}
			result = tx.Model(&identity).Where("current_instance_generation=?", instance.InstanceGeneration).
				Updates(map[string]any{"current_instance_generation": old.InstanceGeneration, "current_content_item_id": old.ContentItemID, "updated_at": time.Now().UTC()})
			if result.Error != nil {
				return 0, result.Error
			}
			if result.RowsAffected != 1 {
				return 0, fmt.Errorf("source identity rollback fence lost: %w", ErrReconstructionGrantScope)
			}
			demoted++
			cursor = instance.ID
		}
	}
}

func identityLockKeyLocked(db *gorm.DB, tenant string, identityID uint) string {
	var identity models.SourceItemIdentity
	if err := db.Where("tenant_id=? AND id=?", tenant, identityID).First(&identity).Error; err != nil {
		return ""
	}
	return identityLockKey(&Input{TenantID: tenant, ContentSourceID: identity.ContentSourceID, UpstreamItemID: identity.UpstreamItemID})
}

// A new identity reserved by replay cannot be independently materialized by a
// live run. Keep its observation deferred until the owning campaign settles.
func identityReserved(db *gorm.DB, tenant string, identityID uint) (bool, error) {
	var count int64
	if err := db.Model(&models.ContentResetReconstructionGrant{}).Where("tenant_id=? AND identity_id=?", tenant, identityID).Count(&count).Error; err != nil {
		return false, err
	}
	return count > 0, nil
}

// lockGrantCampaign is called before identity locks at the consume boundary.
func lockGrantCampaign(tx *gorm.DB, tenant, token string) error {
	var grant models.ContentResetReconstructionGrant
	if err := tx.Where("tenant_id=? AND grant_token_hash=?", tenant, hashOpaqueToken(token)).First(&grant).Error; err != nil {
		return ErrInvalidReconstructionGrant
	}
	var campaign models.ContentResetCampaign
	return tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND id=?", tenant, grant.CampaignID).First(&campaign).Error
}
