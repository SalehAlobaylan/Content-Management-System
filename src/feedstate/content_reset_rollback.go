package feedstate

import (
	"errors"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/sourceidentity"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ResetRollbackFence is an owner-internal binding captured by rollback
// approval. ActiveID is the generation currently serving the lane; RestoreID
// is the retained previous view that becomes active again.
type ResetRollbackFence struct {
	Lane        string    `json:"lane"`
	HeadVersion int64     `json:"head_version"`
	ActiveID    uuid.UUID `json:"active_id"`
	RestoreID   uuid.UUID `json:"restore_id"`
}

func ValidateResetRollbackFences(lane string, fences []ResetRollbackFence) error {
	expected := map[string]bool{}
	switch lane {
	case "news":
		expected["news"] = true
	case "pods":
		expected["media"] = true
	case "both":
		expected["media"] = true
		expected["news"] = true
	default:
		return errors.New("unknown rollback lane")
	}
	if len(fences) != len(expected) {
		return errors.New("rollback requires the exact complete lane set")
	}
	seen := map[uuid.UUID]bool{}
	for _, fence := range fences {
		if !expected[fence.Lane] || fence.HeadVersion < 1 || fence.ActiveID == uuid.Nil || fence.RestoreID == uuid.Nil || fence.ActiveID == fence.RestoreID || seen[fence.ActiveID] || seen[fence.RestoreID] {
			return errors.New("rollback lane/head binding is invalid")
		}
		delete(expected, fence.Lane)
		seen[fence.ActiveID] = true
		seen[fence.RestoreID] = true
	}
	if len(expected) != 0 {
		return errors.New("rollback omits an affected lane")
	}
	return nil
}

// RollbackContentResetViews restores the retained previous view and demotes the
// promoted campaign instances in one transaction. It refuses to lose admitted
// live intake: any member of the published generation that is neither a
// campaign replacement nor part of the retained view blocks rollback and
// requires forward reconciliation.
func RollbackContentResetViews(tx *gorm.DB, tenant string, campaignID, revisionID uint, fences []ResetRollbackFence, rolledBackAt time.Time) (int64, error) {
	if tx == nil || tenant == "" || campaignID == 0 || revisionID == 0 || rolledBackAt.IsZero() {
		return 0, errors.New("invalid rollback boundary")
	}
	if _, transactional := tx.Statement.ConnPool.(gorm.TxCommitter); !transactional {
		return 0, errors.New("Content Reset rollback requires an existing transaction")
	}
	var campaign models.ContentResetCampaign
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND id=? AND state='published' AND operation='fresh_start'", tenant, campaignID).First(&campaign).Error; err != nil {
		return 0, err
	}
	if err := ValidateResetRollbackFences(campaign.Lane, fences); err != nil {
		return 0, err
	}
	var run models.ContentResetExecution
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND campaign_id=? AND revision_id=?", tenant, campaignID, revisionID).First(&run).Error; err != nil {
		return 0, err
	}
	if run.StartedAt == nil || (run.Phase != "published" && run.Phase != "awaiting_cleanup" && run.Phase != "rolling_back") ||
		run.PublishedAt == nil || run.RolledBackAt != nil || run.PauseRequested ||
		run.CleanupNotBefore == nil || !run.CleanupNotBefore.After(rolledBackAt) {
		return 0, errors.New("the recovery window is closed; rollback requires forward reconciliation")
	}
	demoted, err := sourceidentity.DemotePublishedInstances(tx, tenant, campaignID, revisionID)
	if err != nil {
		return 0, err
	}
	ordered := append([]ResetRollbackFence(nil), fences...)
	if len(ordered) == 2 && ordered[0].Lane > ordered[1].Lane {
		ordered[0], ordered[1] = ordered[1], ordered[0]
	}
	for _, fence := range ordered {
		var head models.FeedGenerationHead
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND lane=?", tenant, fence.Lane).First(&head).Error; err != nil {
			return 0, err
		}
		if head.Generation != fence.HeadVersion || head.ActiveGenerationID == nil || *head.ActiveGenerationID != fence.ActiveID || head.CandidateGenerationID != nil {
			return 0, errors.New("approved rollback head changed")
		}
		var current models.FeedGeneration
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=? AND lane=? AND state='active'", tenant, fence.ActiveID, fence.Lane).First(&current).Error; err != nil {
			return 0, err
		}
		if current.PreviousGenerationID == nil || *current.PreviousGenerationID != fence.RestoreID {
			return 0, errors.New("published generation previous-view fence changed")
		}
		var restore models.FeedGeneration
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=? AND lane=? AND state='rollback'", tenant, fence.RestoreID, fence.Lane).First(&restore).Error; err != nil {
			return 0, err
		}
		if restore.RollbackDeadline == nil || !restore.RollbackDeadline.After(rolledBackAt) {
			return 0, errors.New("retained recovery window expired")
		}
		// A live arrival that entered the published generation and was not part
		// of the retained view has no representation after rollback. Only
		// content-bearing memberships count: campaign rebuilds create new
		// story UUIDs whose provenance comes from their campaign article
		// memberships, and atomized children carry their parent's campaign
		// instance. Story-only membership rows themselves are not arrivals.
		var intervening int64
		if err := tx.Raw(`SELECT COUNT(*) FROM feed_generation_memberships published_membership
			WHERE published_membership.generation_id=?
			  AND published_membership.member_type IN ('news_item','feed_unit')
			  AND NOT EXISTS (
			    SELECT 1 FROM feed_generation_memberships retained_membership
			    WHERE retained_membership.generation_id=?
			      AND retained_membership.member_type=published_membership.member_type
			      AND retained_membership.member_id=published_membership.member_id
			  )
			  AND NOT EXISTS (
			    SELECT 1 FROM source_item_instances campaign_instance
			    JOIN content_items member_item
			      ON member_item.tenant_id=campaign_instance.tenant_id
			     AND member_item.public_id=published_membership.member_id
			    WHERE campaign_instance.tenant_id=? AND campaign_instance.campaign_id=?
			      AND campaign_instance.state IN ('staged','active')
			      AND (campaign_instance.content_item_id=member_item.public_id
			           OR campaign_instance.content_item_id=member_item.parent_content_item_id)
			  )`, fence.ActiveID, fence.RestoreID, tenant, campaignID).Scan(&intervening).Error; err != nil {
			return 0, err
		}
		if intervening != 0 {
			return 0, errors.New("live intake arrived after publication; rollback requires forward reconciliation")
		}
		result := tx.Model(&current).Where("state='active'").Updates(map[string]any{"state": "superseded"})
		if result.Error != nil {
			return 0, result.Error
		}
		if result.RowsAffected != 1 {
			return 0, errors.New("published generation activation changed")
		}
		result = tx.Model(&restore).Where("state='rollback'").Updates(map[string]any{"state": "active", "rollback_deadline": nil, "cutover_at": rolledBackAt})
		if result.Error != nil {
			return 0, result.Error
		}
		if result.RowsAffected != 1 {
			return 0, errors.New("retained generation activation changed")
		}
		result = tx.Model(&head).Where("generation=? AND active_generation_id=? AND candidate_generation_id IS NULL", fence.HeadVersion, fence.ActiveID).
			Updates(map[string]any{"active_generation_id": fence.RestoreID, "candidate_generation_id": nil, "generation": gorm.Expr("generation+1"), "updated_at": rolledBackAt})
		if result.Error != nil {
			return 0, result.Error
		}
		if result.RowsAffected != 1 {
			return 0, errors.New("atomic rollback fence lost")
		}
	}
	// The admitted rollback may be persisted as rolling_back by the operator
	// control or awaiting_cleanup by the ordinary planner before this owner
	// runs. All three are the same authorized rollback transition.
	result := tx.Model(&models.ContentResetExecution{}).
		Where("tenant_id=? AND campaign_id=? AND revision_id=? AND phase IN ('published','awaiting_cleanup','rolling_back') AND published_at IS NOT NULL AND rolled_back_at IS NULL AND pause_requested=FALSE", tenant, campaignID, revisionID).
		Updates(map[string]any{"rolled_back_at": rolledBackAt, "phase": "rolled_back"})
	if result.Error != nil {
		return 0, result.Error
	}
	if result.RowsAffected != 1 {
		return 0, errors.New("rollback execution boundary changed")
	}
	result = tx.Model(&campaign).Where("state='published'").Update("state", "closed_partial")
	if result.Error != nil {
		return 0, result.Error
	}
	if result.RowsAffected != 1 {
		return 0, errors.New("rollback campaign boundary changed")
	}
	return demoted, nil
}
