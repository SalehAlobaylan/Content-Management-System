package feedstate

import (
	"errors"
	"fmt"
	"time"

	"content-management-system/src/feedcontract"
	"content-management-system/src/lifecycle"
	"content-management-system/src/models"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const resetViewPageSize = 500

type PodsFreshStartCandidateProof struct {
	GenerationID               uuid.UUID `json:"generation_id"`
	CampaignID                 uuid.UUID `json:"campaign_id"`
	TotalUnits                 int64     `json:"total_units"`
	ReplacementUnits           int64     `json:"replacement_units"`
	SelectedTargets            int64     `json:"selected_targets"`
	ReplacedTargets            int64     `json:"replaced_targets"`
	ReplacedTargetsInCandidate int64     `json:"replaced_targets_in_candidate"`
	PreservedUnits             int64     `json:"preserved_units"`
	SelectedOldUnits           int64     `json:"selected_old_units"`
	StagedParents              int64     `json:"staged_parents"`
	UndeliveredStagedParents   int64     `json:"undelivered_staged_parents"`
	InvalidMaterializations    int64     `json:"invalid_materializations"`
}

// BeginPodsFreshStartCandidate installs an isolated media-lane generation.
// The caller must hold the transaction that admits the campaign phase. New
// campaign-bound instances enter this candidate through SyncMediaMembership;
// normal live intake remains in the old active view until an approved switch.
func BeginPodsFreshStartCandidate(tx *gorm.DB, tenant string, campaignID uint) (models.FeedGeneration, error) {
	if tx == nil || campaignID == 0 || tenant == "" ||
		!tx.Migrator().HasTable(&models.FeedGenerationHead{}) ||
		!tx.Migrator().HasTable(&models.FeedGeneration{}) ||
		!tx.Migrator().HasTable(&models.FeedGenerationMembership{}) ||
		!tx.Migrator().HasColumn(&models.FeedGeneration{}, "purpose") ||
		!tx.Migrator().HasColumn(&models.FeedGeneration{}, "content_reset_campaign_id") {
		return models.FeedGeneration{}, errors.New("isolated Pods feed-generation schema is unavailable")
	}
	var campaign models.ContentResetCampaign
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id = ? AND id = ?", tenant, campaignID).First(&campaign).Error; err != nil {
		return models.FeedGeneration{}, err
	}
	if campaign.Operation != "fresh_start" || (campaign.Lane != "pods" && campaign.Lane != "both") || campaign.State != "executing" {
		return models.FeedGeneration{}, errors.New("Pods candidate requires an executing Pods Fresh Start campaign")
	}
	var revision models.ContentResetRevision
	if err := tx.Where("tenant_id = ? AND campaign_id = ? AND revision = ?", tenant, campaign.ID, campaign.CurrentRevision).First(&revision).Error; err != nil {
		return models.FeedGeneration{}, err
	}
	if revision.State != "previewed" || revision.ManifestHash == nil || *revision.ManifestHash == "" {
		return models.FeedGeneration{}, errors.New("Pods candidate requires a frozen, unblocked revision")
	}
	if !tx.Migrator().HasTable(&models.LifecycleOperationClaim{}) {
		return models.FeedGeneration{}, errors.New("Content Reset lifecycle claim schema is unavailable")
	}
	var claimCount int64
	if err := tx.Model(&models.LifecycleOperationClaim{}).Where(
		"tenant_id = ? AND campaign_id = ? AND owner = ? AND resource_type = ? AND resource_key = ? AND state = ?",
		tenant, campaign.ID, "cms/content-reset", lifecycle.ResourceLane, "pods", lifecycle.ClaimActive,
	).Count(&claimCount).Error; err != nil {
		return models.FeedGeneration{}, err
	}
	if claimCount != 1 {
		return models.FeedGeneration{}, errors.New("Pods candidate requires the campaign's active lane claim")
	}

	var head models.FeedGenerationHead
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id = ? AND lane = ?", tenant, mediaLane).First(&head).Error; err != nil {
		return models.FeedGeneration{}, err
	}
	if head.CandidateGenerationID != nil {
		var existing models.FeedGeneration
		if err := tx.Where("tenant_id=? AND public_id=? AND lane=?", tenant, *head.CandidateGenerationID, mediaLane).First(&existing).Error; err != nil {
			return models.FeedGeneration{}, err
		}
		if existing.Purpose == "content_reset" && existing.State == "candidate" && existing.ContentResetCampaignID != nil && *existing.ContentResetCampaignID == campaign.ID && existing.PreviousGenerationID != nil && head.ActiveGenerationID != nil && *existing.PreviousGenerationID == *head.ActiveGenerationID {
			return existing, nil
		}
		return models.FeedGeneration{}, errors.New("another candidate holds the Pods feed head")
	}
	if head.ActiveGenerationID == nil || *head.ActiveGenerationID == uuid.Nil || head.CandidateGenerationID != nil {
		return models.FeedGeneration{}, errors.New("Pods feed head must have one active generation and no candidate")
	}
	generation := models.FeedGeneration{
		PublicID: uuid.New(), TenantID: tenant, Lane: mediaLane, State: "candidate",
		Purpose: "content_reset", ContentResetCampaignID: &campaign.ID,
		PreviousGenerationID: head.ActiveGenerationID, BuildWatermark: time.Now().UTC(),
		Verification: datatypes.JSON([]byte(`{"schema_version":"pods-fresh-start-candidate/v1","complete":false}`)),
	}
	if err := tx.Create(&generation).Error; err != nil {
		return models.FeedGeneration{}, err
	}
	if err := copyUnaffectedPodsMembership(tx, tenant, revision.ID, *head.ActiveGenerationID, generation.PublicID); err != nil {
		return models.FeedGeneration{}, err
	}
	if err := attachPreservedPodsTargets(tx, tenant, revision.ID, generation.PublicID); err != nil {
		return models.FeedGeneration{}, err
	}
	result := tx.Model(&head).Where("tenant_id = ? AND lane = ? AND generation = ? AND candidate_generation_id IS NULL",
		tenant, mediaLane, head.Generation).Updates(map[string]any{
		"candidate_generation_id": generation.PublicID,
		"updated_at":              time.Now().UTC(),
	})
	if result.Error != nil {
		return models.FeedGeneration{}, result.Error
	}
	if result.RowsAffected != 1 {
		return models.FeedGeneration{}, errors.New("Pods candidate lost its feed-head fence")
	}
	return generation, nil
}

func copyUnaffectedPodsMembership(tx *gorm.DB, tenant string, revisionID uint, activeID, candidateID uuid.UUID) error {
	result := tx.Exec(`INSERT INTO feed_generation_memberships (generation_id, member_type, member_id, attached_at)
		SELECT ?, membership.member_type, membership.member_id, NOW()
		  FROM feed_generation_memberships membership
		 WHERE membership.generation_id = ? AND membership.member_type = 'feed_unit'
		   AND NOT EXISTS (
		       SELECT 1 FROM content_reset_targets target
		        WHERE target.tenant_id = ? AND target.revision_id = ?
		          AND target.content_item_id = membership.member_id
		          AND target.disposition = 'selected' AND target.protected = FALSE
		   )
		ON CONFLICT DO NOTHING`, candidateID, activeID, tenant, revisionID)
	return result.Error
}

// VerifyPodsFreshStartCandidate proves the replacement view contains eligible
// campaign instances and approved protected survivors, and contains none of
// the old selected content. It is read-only; it does not publish or clean up.
func VerifyPodsFreshStartCandidate(db *gorm.DB, tenant string, campaignID uint, candidateID uuid.UUID) (PodsFreshStartCandidateProof, error) {
	proof := PodsFreshStartCandidateProof{GenerationID: candidateID}
	var campaign models.ContentResetCampaign
	if err := db.Where("tenant_id = ? AND id = ?", tenant, campaignID).First(&campaign).Error; err != nil {
		return proof, err
	}
	proof.CampaignID = campaign.PublicID
	var revision models.ContentResetRevision
	if err := db.Where("tenant_id = ? AND campaign_id = ? AND revision = ?", tenant, campaign.ID, campaign.CurrentRevision).First(&revision).Error; err != nil {
		return proof, err
	}
	var generation models.FeedGeneration
	if err := db.Where("tenant_id = ? AND public_id = ? AND lane = ?", tenant, candidateID, mediaLane).First(&generation).Error; err != nil {
		return proof, err
	}
	if generation.State != "candidate" || generation.Purpose != "content_reset" || generation.ContentResetCampaignID == nil || *generation.ContentResetCampaignID != campaign.ID {
		return proof, errors.New("Pods candidate does not belong to this Content Reset campaign")
	}
	eligible := feedcontract.PodsEligibleMediaQuery(db, tenant, feedcontract.SupportsAtomizedPodsSchema(db)).
		Joins("JOIN feed_generation_memberships reset_membership ON reset_membership.member_id = content_items.public_id AND reset_membership.generation_id = ? AND reset_membership.member_type = 'feed_unit'", candidateID)
	if err := eligible.Count(&proof.TotalUnits).Error; err != nil {
		return proof, err
	}
	if !db.Migrator().HasTable(&models.SourceItemInstance{}) {
		return proof, errors.New("source-item instance registry is unavailable")
	}
	if err := db.Raw(`SELECT COUNT(*) FROM content_reset_reconstruction_grants grant_row
      LEFT JOIN source_item_instances instance ON instance.tenant_id=grant_row.tenant_id
        AND instance.identity_id=grant_row.identity_id AND instance.instance_generation=grant_row.replacement_instance_generation
        AND instance.content_item_id=grant_row.replacement_content_item_id AND instance.campaign_id=grant_row.campaign_id
      LEFT JOIN content_items payload ON payload.tenant_id=instance.tenant_id AND payload.public_id=instance.content_item_id
      WHERE grant_row.tenant_id=? AND grant_row.campaign_id=? AND grant_row.revision_id=? AND grant_row.state='consumed'
        AND (instance.id IS NULL OR instance.state<>'staged' OR payload.public_id IS NULL)`, tenant, campaign.ID, revision.ID).Scan(&proof.InvalidMaterializations).Error; err != nil {
		return proof, err
	}
	if proof.InvalidMaterializations != 0 {
		return proof, errors.New("campaign has consumed reconstruction grants without valid staged payloads")
	}
	replacementQuery := feedcontract.PodsEligibleMediaQuery(db, tenant, feedcontract.SupportsAtomizedPodsSchema(db)).
		Joins("JOIN source_item_instances reset_instance ON reset_instance.tenant_id = content_items.tenant_id AND (reset_instance.content_item_id = content_items.public_id OR reset_instance.content_item_id=content_items.parent_content_item_id) AND reset_instance.campaign_id = ? AND reset_instance.state = 'staged'", campaign.ID).
		Joins("JOIN feed_generation_memberships reset_membership ON reset_membership.member_id = content_items.public_id AND reset_membership.generation_id = ? AND reset_membership.member_type = 'feed_unit'", candidateID)
	if err := replacementQuery.Count(&proof.ReplacementUnits).Error; err != nil {
		return proof, err
	}
	if err := db.Table("feed_generation_memberships membership").
		Joins("JOIN content_reset_targets target ON target.tenant_id = ? AND target.revision_id = ? AND target.content_item_id = membership.member_id AND target.disposition = 'preserve' AND target.protected = TRUE", tenant, revision.ID).
		Where("membership.generation_id = ? AND membership.member_type = ?", candidateID, "feed_unit").
		Count(&proof.PreservedUnits).Error; err != nil {
		return proof, err
	}
	if err := db.Table("feed_generation_memberships membership").
		Joins("JOIN content_reset_targets target ON target.tenant_id = ? AND target.revision_id = ? AND target.content_item_id = membership.member_id AND target.disposition = 'selected'", tenant, revision.ID).
		Where("membership.generation_id = ? AND membership.member_type = ?", candidateID, "feed_unit").
		Count(&proof.SelectedOldUnits).Error; err != nil {
		return proof, err
	}
	selectedTargets := db.Model(&models.ContentResetTarget{}).
		Where("tenant_id = ? AND revision_id = ? AND lane = ? AND disposition = ? AND protected = FALSE", tenant, revision.ID, "pods", "selected")
	if err := selectedTargets.Count(&proof.SelectedTargets).Error; err != nil {
		return proof, err
	}
	replacedTargets := db.Table("content_reset_targets target").
		Joins("JOIN source_item_instances prior ON prior.tenant_id = target.tenant_id AND prior.content_item_id = target.content_item_id").
		Joins("JOIN source_item_instances replacement ON replacement.tenant_id = prior.tenant_id AND replacement.identity_id = prior.identity_id AND replacement.instance_generation > prior.instance_generation AND replacement.campaign_id = ? AND replacement.state = 'staged'", campaign.ID).
		Where("target.tenant_id = ? AND target.revision_id = ? AND target.lane = ? AND target.disposition = ? AND target.protected = FALSE AND prior.state IN ?", tenant, revision.ID, "pods", "selected", []string{"active", "retired"})
	if err := replacedTargets.Distinct("target.content_item_id").Count(&proof.ReplacedTargets).Error; err != nil {
		return proof, err
	}
	inCandidateTargets := db.Table("content_reset_targets target").
		Joins("JOIN source_item_instances prior ON prior.tenant_id = target.tenant_id AND prior.content_item_id = target.content_item_id").
		Joins("JOIN source_item_instances replacement ON replacement.tenant_id = prior.tenant_id AND replacement.identity_id = prior.identity_id AND replacement.instance_generation > prior.instance_generation AND replacement.campaign_id = ? AND replacement.state = 'staged'", campaign.ID).
		Joins("JOIN feed_generation_memberships reset_membership ON reset_membership.generation_id = ? AND reset_membership.member_type = 'feed_unit' AND reset_membership.member_id = replacement.content_item_id", candidateID).
		Where("target.tenant_id = ? AND target.revision_id = ? AND target.lane = ? AND target.disposition = ? AND target.protected = FALSE AND prior.state IN ?", tenant, revision.ID, "pods", "selected", []string{"active", "retired"})
	if err := inCandidateTargets.Distinct("target.content_item_id").Count(&proof.ReplacedTargetsInCandidate).Error; err != nil {
		return proof, err
	}
	// Recent/history replay defines the new population independently of the
	// old removal selection. It need not reconstruct every old selected item.
	// Delivery coverage instead binds every campaign materialization, including
	// a long parent's canonical eligible chapters, to this exact candidate.
	staged := db.Table("source_item_instances instance").
		Joins("JOIN content_items parent ON parent.tenant_id=instance.tenant_id AND parent.public_id=instance.content_item_id AND parent.type IN ('VIDEO','PODCAST')").
		Where("instance.tenant_id=? AND instance.campaign_id=? AND instance.state='staged'", tenant, campaign.ID)
	if err := staged.Count(&proof.StagedParents).Error; err != nil {
		return proof, err
	}
	delivered := feedcontract.PodsEligibleMediaQuery(db, tenant, feedcontract.SupportsAtomizedPodsSchema(db)).
		Select("content_items.public_id, content_items.parent_content_item_id").
		Joins("JOIN feed_generation_memberships candidate_membership ON candidate_membership.generation_id=? AND candidate_membership.member_type='feed_unit' AND candidate_membership.member_id=content_items.public_id", candidateID)
	missing := db.Table("source_item_instances instance").
		Joins("JOIN content_items parent ON parent.tenant_id=instance.tenant_id AND parent.public_id=instance.content_item_id AND parent.type IN ('VIDEO','PODCAST')").
		Where("instance.tenant_id=? AND instance.campaign_id=? AND instance.state='staged'", tenant, campaign.ID).
		Where("NOT EXISTS (SELECT 1 FROM (?) AS ready WHERE ready.public_id=parent.public_id OR ready.parent_content_item_id=parent.public_id)", delivered).
		Where(`NOT (parent.status='ARCHIVED' AND EXISTS (
            SELECT 1 FROM source_upstream_observation_events decision
             WHERE decision.tenant_id=instance.tenant_id AND decision.observation_id=instance.source_observation_id
               AND decision.event_type='filtered' AND decision.payload->>'filter_class' IN ('moderation_rejected','duration_below_minimum')
        ))`)
	if err := missing.Count(&proof.UndeliveredStagedParents).Error; err != nil {
		return proof, err
	}
	if proof.UndeliveredStagedParents != 0 {
		return proof, fmt.Errorf("Pods candidate lacks delivery for %d campaign media parents", proof.UndeliveredStagedParents)
	}
	if proof.TotalUnits == 0 {
		return proof, errors.New("Fresh Start cannot publish an empty Pods replacement; use an explicitly approved Empty operation")
	}
	if proof.SelectedOldUnits != 0 {
		return proof, fmt.Errorf("isolated Pods candidate contains %d selected old items", proof.SelectedOldUnits)
	}
	return proof, nil
}

func attachPreservedPodsTargets(tx *gorm.DB, tenant string, revisionID uint, generationID uuid.UUID) error {
	cursor := int64(0)
	for {
		var targets []models.ContentResetTarget
		if err := tx.Where("tenant_id = ? AND revision_id = ? AND lane = ? AND disposition = ? AND protected = TRUE AND item_ordinal > ?",
			tenant, revisionID, "pods", "preserve", cursor).Order("item_ordinal ASC").Limit(resetViewPageSize).Find(&targets).Error; err != nil {
			return err
		}
		if len(targets) == 0 {
			return nil
		}
		ids := make([]uuid.UUID, 0, len(targets))
		for _, target := range targets {
			ids = append(ids, target.ContentItemID)
			cursor = target.ItemOrdinal
		}
		var eligibleIDs []uuid.UUID
		query := feedcontract.PodsEligibleMediaQuery(tx, tenant, feedcontract.SupportsAtomizedPodsSchema(tx)).Where("content_items.public_id IN ?", ids)
		if err := query.Pluck("content_items.public_id", &eligibleIDs).Error; err != nil {
			return err
		}
		memberships := make([]models.FeedGenerationMembership, 0, len(eligibleIDs))
		for _, id := range eligibleIDs {
			memberships = append(memberships, models.FeedGenerationMembership{GenerationID: generationID, MemberType: "feed_unit", MemberID: id})
		}
		if len(memberships) > 0 {
			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&memberships).Error; err != nil {
				return err
			}
		}
	}
}
