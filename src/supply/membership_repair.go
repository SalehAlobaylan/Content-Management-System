package supply

import (
	"encoding/json"
	"fmt"
	"time"

	"content-management-system/src/feedcontract"
	"content-management-system/src/models"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type FeedMembershipRepairCandidate struct {
	Item       models.ContentItem
	Generation models.FeedGeneration
	Head       models.FeedGenerationHead
}

// CandidateForFeedMembership derives one exact, currently base-eligible item
// that is absent from the current active media generation. It does not accept
// a lane, member type, or generation from a caller.
func CandidateForFeedMembership(db *gorm.DB, tenant string, itemID uuid.UUID) (FeedMembershipRepairCandidate, error) {
	if db == nil || tenant == "" || itemID == uuid.Nil {
		return FeedMembershipRepairCandidate{}, fmt.Errorf("feed membership candidate identity is invalid")
	}
	var item models.ContentItem
	if err := feedcontract.PodsEligibleMediaQuery(db, tenant, feedcontract.SupportsAtomizedPodsSchema(db)).Where("content_items.public_id=?", itemID).First(&item).Error; err != nil {
		return FeedMembershipRepairCandidate{}, fmt.Errorf("content item is not currently base eligible: %w", err)
	}
	var holds int64
	if err := db.Model(&models.MediaCirculationOverride{}).Where("tenant_id=? AND subject_id IN ? AND override_type=? AND (expires_at IS NULL OR expires_at>?)", tenant, []uuid.UUID{item.PublicID}, models.MediaCirculationOverrideEditorialHold, time.Now().UTC()).Count(&holds).Error; err != nil {
		return FeedMembershipRepairCandidate{}, err
	}
	if holds > 0 {
		return FeedMembershipRepairCandidate{}, fmt.Errorf("content item has an editorial hold")
	}
	var head models.FeedGenerationHead
	if err := db.Where("tenant_id=? AND lane=?", tenant, "media").First(&head).Error; err != nil || head.ActiveGenerationID == nil {
		return FeedMembershipRepairCandidate{}, fmt.Errorf("active media generation is unavailable")
	}
	var generation models.FeedGeneration
	if err := db.Where("public_id=? AND tenant_id=? AND lane=? AND state=?", *head.ActiveGenerationID, tenant, "media", "active").First(&generation).Error; err != nil {
		return FeedMembershipRepairCandidate{}, fmt.Errorf("active media generation is invalid")
	}
	var membership int64
	if err := db.Model(&models.FeedGenerationMembership{}).Where("generation_id=? AND member_type=? AND member_id=?", generation.PublicID, "feed_unit", item.PublicID).Count(&membership).Error; err != nil {
		return FeedMembershipRepairCandidate{}, err
	}
	if membership != 0 {
		return FeedMembershipRepairCandidate{}, fmt.Errorf("content item is already reachable")
	}
	return FeedMembershipRepairCandidate{Item: item, Generation: generation, Head: head}, nil
}

func CreateApprovedFeedMembershipRepair(db *gorm.DB, action models.MediaSupplyActionRequest) (models.FeedGenerationMembershipRepair, error) {
	if action.ActionKey != SupplyActionFeedGenerationAttachVerifiedMember || action.TargetType != "content_item" || action.State != models.MediaSupplyActionRequestQueued {
		return models.FeedGenerationMembershipRepair{}, fmt.Errorf("feed membership action is invalid")
	}
	candidate, err := CandidateForFeedMembership(db, action.TenantID, action.TargetID)
	if err != nil {
		return models.FeedGenerationMembershipRepair{}, err
	}
	repair := models.FeedGenerationMembershipRepair{PublicID: uuid.New(), TenantID: action.TenantID, ActionRequestID: action.PublicID, ContentItemID: candidate.Item.PublicID, ExpectedGenerationID: candidate.Generation.PublicID, ExpectedHeadVersion: candidate.Head.Generation, ExpectedItemUpdatedAt: candidate.Item.UpdatedAt, State: "queued", BeforeEffects: datatypes.JSON([]byte(`{}`)), AfterEffects: datatypes.JSON([]byte(`{}`)), VerifiedEffects: datatypes.JSON([]byte(`{}`))}
	if err := db.Create(&repair).Error; err != nil {
		return models.FeedGenerationMembershipRepair{}, err
	}
	return repair, nil
}

func CancelFeedMembershipRepairByAction(db *gorm.DB, tenant string, actionID uuid.UUID) error {
	now := time.Now().UTC()
	result := db.Model(&models.FeedGenerationMembershipRepair{}).
		Where("tenant_id=? AND action_request_id=? AND state=?", tenant, actionID, "queued").
		Updates(map[string]any{"state": "cancelled", "finished_at": now})
	return result.Error
}

func executeFeedMembershipRepair(db *gorm.DB, lease SupplyActionLease) error {
	var repair models.FeedGenerationMembershipRepair
	if err := db.Where("tenant_id=? AND action_request_id=? AND state=?", lease.Request.TenantID, lease.Request.PublicID, "queued").First(&repair).Error; err != nil {
		return fmt.Errorf("feed membership repair ledger is unavailable: %w", err)
	}
	candidate, err := CandidateForFeedMembership(db, repair.TenantID, repair.ContentItemID)
	if err != nil || candidate.Generation.PublicID != repair.ExpectedGenerationID || candidate.Head.Generation != repair.ExpectedHeadVersion || !candidate.Item.UpdatedAt.Equal(repair.ExpectedItemUpdatedAt) {
		return fmt.Errorf("feed membership repair preconditions changed")
	}
	before := map[string]any{"schema_version": "feed-membership-effects/v1", "content_item_id": repair.ContentItemID, "generation_id": repair.ExpectedGenerationID, "head_version": repair.ExpectedHeadVersion, "membership_present": false}
	if err := recordSupplyActionEffects(db, lease, "before_effects", before); err != nil {
		return err
	}
	if _, err := BeginSupplyActionEffect(db, lease.Request.TenantID, lease.Request.PublicID.String(), lease.Request.ClaimOwner, lease.ClaimToken.String()); err != nil {
		return err
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		var head models.FeedGenerationHead
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND lane=? AND active_generation_id=? AND generation=?", repair.TenantID, "media", repair.ExpectedGenerationID, repair.ExpectedHeadVersion).First(&head).Error; err != nil {
			return fmt.Errorf("feed generation head changed before effect: %w", err)
		}
		allowed, _, err := MayExecuteSupplyAction(tx, repair.TenantID, SupplyActionFeedGenerationAttachVerifiedMember)
		if err != nil || !allowed {
			return fmt.Errorf("feed membership action is disabled")
		}
		var current models.ContentItem
		if err := feedcontract.PodsEligibleMediaQuery(tx, repair.TenantID, feedcontract.SupportsAtomizedPodsSchema(tx)).Where("content_items.public_id=? AND content_items.updated_at=?", repair.ContentItemID, repair.ExpectedItemUpdatedAt).First(&current).Error; err != nil {
			return fmt.Errorf("content item changed before membership effect: %w", err)
		}
		membership := models.FeedGenerationMembership{GenerationID: repair.ExpectedGenerationID, MemberType: "feed_unit", MemberID: repair.ContentItemID, AttachedAt: time.Now().UTC()}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&membership).Error; err != nil {
			return err
		}
		after, _ := json.Marshal(map[string]any{"schema_version": "feed-membership-effects/v1", "content_item_id": repair.ContentItemID, "generation_id": repair.ExpectedGenerationID, "head_version": repair.ExpectedHeadVersion, "membership_present": true})
		return tx.Model(&repair).Updates(map[string]any{"state": "verifying", "before_effects": jsonValueForMembership(before), "after_effects": datatypes.JSON(after)}).Error
	}); err != nil {
		// The insert may have committed before an acknowledgement was lost. Move
		// the signed action to verification through normal lease recovery; never
		// repeat it here.
		return err
	}
	var head models.FeedGenerationHead
	var count int64
	verificationErr := db.Where("tenant_id=? AND lane=? AND active_generation_id=? AND generation=?", repair.TenantID, "media", repair.ExpectedGenerationID, repair.ExpectedHeadVersion).First(&head).Error
	if verificationErr == nil {
		verificationErr = db.Model(&models.FeedGenerationMembership{}).Where("generation_id=? AND member_type=? AND member_id=?", repair.ExpectedGenerationID, "feed_unit", repair.ContentItemID).Count(&count).Error
	}
	succeeded := verificationErr == nil && count == 1
	proof, _ := json.Marshal(map[string]any{"schema_version": "feed-membership-proof/v1", "content_item_id": repair.ContentItemID, "generation_id": repair.ExpectedGenerationID, "head_version": repair.ExpectedHeadVersion, "membership_present": succeeded})
	if err := terminalizeSupplyAction(db, lease, succeeded, datatypes.JSON(proof)); err != nil {
		return err
	}
	now := time.Now().UTC()
	state := "failed"
	if succeeded {
		state = "succeeded"
	}
	return db.Model(&repair).Updates(map[string]any{"state": state, "verified_effects": datatypes.JSON(proof), "finished_at": now}).Error
}

func jsonValueForMembership(value any) datatypes.JSON {
	bytes, _ := json.Marshal(value)
	return datatypes.JSON(bytes)
}
