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

// ResetPublicationFence is an owner-internal value captured by publication
// approval. This primitive does not authorize publication: its caller must
// prove the operator decision, native handoff, protection and storage evidence.
type ResetPublicationFence struct {
	Lane        string    `json:"lane"`
	HeadVersion int64     `json:"head_version"`
	ActiveID    uuid.UUID `json:"active_id"`
	CandidateID uuid.UUID `json:"candidate_id"`
}

func ValidateResetPublicationFences(lane string, fences []ResetPublicationFence) error {
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
		return errors.New("unknown publication lane")
	}
	if len(fences) != len(expected) {
		return errors.New("publication requires the exact complete lane set")
	}
	seen := map[uuid.UUID]bool{}
	for _, fence := range fences {
		if !expected[fence.Lane] || fence.HeadVersion < 1 || fence.ActiveID == uuid.Nil || fence.CandidateID == uuid.Nil || fence.ActiveID == fence.CandidateID || seen[fence.ActiveID] || seen[fence.CandidateID] {
			return errors.New("publication lane/head binding is invalid")
		}
		delete(expected, fence.Lane)
		seen[fence.ActiveID] = true
		seen[fence.CandidateID] = true
	}
	if len(expected) != 0 {
		return errors.New("publication omits an affected lane")
	}
	return nil
}

// PublishContentResetViews switches the exact complete lane set in the same
// transaction as instance promotion. A failure on either lane aborts both.
// Call only inside the admitted publication owner's transaction; campaign and
// instance locks precede lane heads, matching reconstruction writebacks.
//
// retainOldView=true keeps the previous generation readable for the approved
// recovery window (state 'rollback'). clear-first publication sets it false:
// the old payload is already irreversibly retired, so the previous generation
// becomes 'superseded' with no rollback deadline.
func PublishContentResetViews(tx *gorm.DB, tenant string, campaignID, revisionID uint, fences []ResetPublicationFence, publishedAt, rollbackDeadline time.Time, retainOldView bool) (int64, error) {
	// Without a retained view there is no recovery deadline to validate; the
	// no-rollback boundary is defined by the actual publication timestamp, not
	// by the earlier approval timestamp carried in the milestone.
	if tx == nil || tenant == "" || campaignID == 0 || revisionID == 0 || publishedAt.IsZero() ||
		(retainOldView && !rollbackDeadline.After(publishedAt)) {
		return 0, errors.New("invalid publication boundary")
	}
	if _, transactional := tx.Statement.ConnPool.(gorm.TxCommitter); !transactional {
		return 0, errors.New("Content Reset publication requires an existing transaction")
	}
	var campaign models.ContentResetCampaign
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND id=? AND state='executing' AND operation='fresh_start'", tenant, campaignID).First(&campaign).Error; err != nil {
		return 0, err
	}
	if err := ValidateResetPublicationFences(campaign.Lane, fences); err != nil {
		return 0, err
	}
	// This helper already checks the started, unpaused, manifest-bound
	// execution in publishing phase. Promotion rolls back with every head.
	promoted, err := sourceidentity.PromoteStagedInstances(tx, tenant, campaignID, revisionID)
	if err != nil {
		return 0, err
	}
	ordered := append([]ResetPublicationFence(nil), fences...)
	// Deterministic media/news head order prevents Both campaigns from taking
	// opposite locks. The caller's approved ordering does not carry authority.
	if len(ordered) == 2 && ordered[0].Lane > ordered[1].Lane {
		ordered[0], ordered[1] = ordered[1], ordered[0]
	}
	for _, fence := range ordered {
		var head models.FeedGenerationHead
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND lane=?", tenant, fence.Lane).First(&head).Error; err != nil {
			return 0, err
		}
		if head.Generation != fence.HeadVersion || head.ActiveGenerationID == nil || *head.ActiveGenerationID != fence.ActiveID || head.CandidateGenerationID == nil || *head.CandidateGenerationID != fence.CandidateID {
			return 0, errors.New("approved publication head changed")
		}
		var candidate models.FeedGeneration
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=? AND lane=? AND state='candidate' AND purpose='content_reset' AND content_reset_campaign_id=?", tenant, fence.CandidateID, fence.Lane, campaignID).First(&candidate).Error; err != nil {
			return 0, err
		}
		if candidate.PreviousGenerationID == nil || *candidate.PreviousGenerationID != fence.ActiveID {
			return 0, errors.New("publication candidate previous-view fence changed")
		}
		var old models.FeedGeneration
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=? AND lane=? AND state='active'", tenant, fence.ActiveID, fence.Lane).First(&old).Error; err != nil {
			return 0, err
		}
		// Snapshot the old News presentation before shared canonical metadata
		// can change following cutover. A retained view must stay self-contained.
		if fence.Lane == newsLane {
			if err := tx.Exec(`INSERT INTO news_feed_story_projections(tenant_id,generation_id,story_id,projection,member_hash,updated_at)
              SELECT ?,?,s.public_id,to_jsonb(s),encode(sha256(convert_to((
                SELECT COALESCE(jsonb_agg(jsonb_build_array(c.public_id,c.processing_generation,c.embedding_space_id,
                  encode(sha256(convert_to(c.embedding::text,'UTF8')),'hex'),c.title,c.published_at,c.status) ORDER BY c.public_id),'[]'::jsonb)::text
                FROM content_items c JOIN feed_generation_memberships exact_member ON exact_member.member_id=c.public_id
                  AND exact_member.generation_id=? AND exact_member.member_type='news_item'
                WHERE c.tenant_id=s.tenant_id AND c.story_id=s.public_id AND c.type='NEWS' AND c.status='READY'
              ),'UTF8')),'hex'),NOW()
              FROM stories s JOIN feed_generation_memberships m ON m.member_id=s.public_id AND m.member_type='story' AND m.generation_id=?
              WHERE s.tenant_id=? ON CONFLICT(generation_id,story_id) DO NOTHING`, tenant, fence.ActiveID, fence.ActiveID, fence.ActiveID, tenant).Error; err != nil {
				return 0, err
			}
		}
		oldState := "superseded"
		oldUpdates := map[string]any{"state": oldState}
		if retainOldView {
			oldState = "rollback"
			oldUpdates = map[string]any{"state": oldState, "rollback_deadline": rollbackDeadline}
		}
		result := tx.Model(&old).Where("state='active'").Updates(oldUpdates)
		if result.Error != nil {
			return 0, result.Error
		}
		if result.RowsAffected != 1 {
			return 0, errors.New("previous generation activation changed")
		}
		result = tx.Model(&candidate).Where("state='candidate'").Updates(map[string]any{"state": "active", "cutover_at": publishedAt, "caught_up_at": publishedAt})
		if result.Error != nil {
			return 0, result.Error
		}
		if result.RowsAffected != 1 {
			return 0, errors.New("candidate activation changed")
		}
		result = tx.Model(&head).Where("generation=? AND active_generation_id=? AND candidate_generation_id=?", fence.HeadVersion, fence.ActiveID, fence.CandidateID).Updates(map[string]any{"active_generation_id": fence.CandidateID, "candidate_generation_id": nil, "generation": gorm.Expr("generation+1"), "updated_at": publishedAt})
		if result.Error != nil {
			return 0, result.Error
		}
		if result.RowsAffected != 1 {
			return 0, errors.New("atomic publication fence lost")
		}
	}
	cleanupNotBefore := rollbackDeadline
	if !retainOldView {
		// Already-retired payload has no recovery window; delayed cleanup is not
		// required and the previous view is not addressable.
		cleanupNotBefore = publishedAt
	}
	result := tx.Model(&models.ContentResetExecution{}).Where("tenant_id=? AND campaign_id=? AND revision_id=? AND phase='publishing' AND published_at IS NULL AND pause_requested=FALSE", tenant, campaignID, revisionID).
		Updates(map[string]any{"published_at": publishedAt, "cleanup_not_before": cleanupNotBefore, "phase": "published"})
	if result.Error != nil {
		return 0, result.Error
	}
	if result.RowsAffected != 1 {
		return 0, errors.New("publication execution boundary changed")
	}
	result = tx.Model(&campaign).Where("state='executing'").Update("state", "published")
	if result.Error != nil {
		return 0, result.Error
	}
	if result.RowsAffected != 1 {
		return 0, errors.New("publication campaign boundary changed")
	}
	return promoted, nil
}
