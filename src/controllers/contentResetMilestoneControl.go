package controllers

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"content-management-system/src/contentreset"
	"content-management-system/src/feedstate"
	"content-management-system/src/models"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	contentResetRecoveryWindow             = 24 * time.Hour
	contentResetCleanupAuthorizationWindow = 72 * time.Hour
	contentResetMilestoneTTL               = 6 * time.Hour
)

// contentResetStagedDeliveryPrivate reports whether candidate-owned media is
// staged behind a private or signed delivery boundary. The current build stages
// candidate assets through the ordinary public media path, so it is false:
// Pods Fresh Start previews stay blocked and the authenticated staged-items
// surface withholds playback URLs. Flip this only together with a qualified
// private/signed boundary (including thumbnails and HLS segments) and its
// publication/abandonment delivery-authority transitions.
func contentResetStagedDeliveryPrivate() bool { return false }

func contentResetEngine() (*contentreset.Engine, error) {
	owners := make([]contentreset.Owner, 0, len(contentResetOwners))
	for _, owner := range contentResetOwners {
		owners = append(owners, owner)
	}
	return contentreset.NewEngine(owners...)
}

func contentResetMilestoneConfirmation(action string, campaign models.ContentResetCampaign, revision models.ContentResetRevision) string {
	if revision.ManifestHash == nil || len(*revision.ManifestHash) < 12 {
		return ""
	}
	return strings.ToUpper(action) + " " + strings.ToUpper(campaign.Lane) + " " + strconv.FormatInt(revision.TargetCount, 10) + " ITEMS " + strings.ToUpper((*revision.ManifestHash)[:12])
}

func contentResetReadinessHash(observation contentreset.Observation) string {
	return contentreset.Hash(map[string]any{"state": observation.State, "reason_code": observation.ReasonCode, "evidence": json.RawMessage(observation.Evidence)})
}

// writeContentResetMilestone revokes any unused prior milestone of the same
// kind and persists the new immutable approval. The actor's proof hash is kept
// only as a one-use decision receipt; it is never part of the milestone.
func writeContentResetMilestone(tx *gorm.DB, tenant string, campaign models.ContentResetCampaign, revision models.ContentResetRevision, kind string, payload any, approvedBy string) (models.ContentResetMilestone, error) {
	var row models.ContentResetMilestone
	if revision.ManifestHash == nil {
		return row, errors.New("milestone requires a frozen manifest")
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return row, err
	}
	now := time.Now().UTC()
	if err := tx.Model(&models.ContentResetMilestone{}).
		Where("tenant_id=? AND campaign_id=? AND revision_id=? AND kind=? AND state='active'", tenant, campaign.ID, revision.ID, kind).
		Updates(map[string]any{"state": "revoked", "revoked_at": now}).Error; err != nil {
		return row, err
	}
	row = models.ContentResetMilestone{
		PublicID: uuid.New(), TenantID: tenant, CampaignID: campaign.ID, RevisionID: revision.ID,
		Kind: kind, State: "active", ManifestHash: *revision.ManifestHash,
		Payload: raw, PayloadHash: contentreset.Hash(payload),
		ApprovedBy: approvedBy, ApprovedAt: now, ExpiresAt: now.Add(contentResetMilestoneTTL),
	}
	if err := tx.Create(&row).Error; err != nil {
		return row, err
	}
	return row, nil
}

func captureContentResetPublicationFences(tx *gorm.DB, tenant string, campaign models.ContentResetCampaign) ([]feedstate.ResetPublicationFence, error) {
	fences := make([]feedstate.ResetPublicationFence, 0, 2)
	for _, lane := range contentResetGenerationLanes(campaign.Lane) {
		var head models.FeedGenerationHead
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND lane=?", tenant, lane).First(&head).Error; err != nil {
			return nil, err
		}
		if head.ActiveGenerationID == nil || head.CandidateGenerationID == nil {
			return nil, errors.New("publication requires an active generation and a candidate")
		}
		var candidate models.FeedGeneration
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("tenant_id=? AND public_id=? AND lane=? AND state='candidate' AND purpose='content_reset' AND content_reset_campaign_id=?", tenant, *head.CandidateGenerationID, lane, campaign.ID).
			First(&candidate).Error; err != nil {
			return nil, err
		}
		if candidate.PreviousGenerationID == nil || *candidate.PreviousGenerationID != *head.ActiveGenerationID {
			return nil, errors.New("publication candidate does not descend from the current active view")
		}
		var active models.FeedGeneration
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=? AND lane=? AND state='active'", tenant, *head.ActiveGenerationID, lane).First(&active).Error; err != nil {
			return nil, err
		}
		fences = append(fences, feedstate.ResetPublicationFence{Lane: lane, HeadVersion: head.Generation, ActiveID: *head.ActiveGenerationID, CandidateID: candidate.PublicID})
	}
	if err := feedstate.ValidateResetPublicationFences(campaign.Lane, fences); err != nil {
		return nil, err
	}
	return fences, nil
}

func captureContentResetRollbackFences(tx *gorm.DB, tenant string, campaign models.ContentResetCampaign) ([]feedstate.ResetRollbackFence, error) {
	fences := make([]feedstate.ResetRollbackFence, 0, 2)
	now := time.Now().UTC()
	for _, lane := range contentResetGenerationLanes(campaign.Lane) {
		var head models.FeedGenerationHead
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND lane=?", tenant, lane).First(&head).Error; err != nil {
			return nil, err
		}
		if head.ActiveGenerationID == nil || head.CandidateGenerationID != nil {
			return nil, errors.New("rollback requires a single published generation")
		}
		var current models.FeedGeneration
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=? AND lane=? AND state='active'", tenant, *head.ActiveGenerationID, lane).First(&current).Error; err != nil {
			return nil, err
		}
		if current.PreviousGenerationID == nil {
			return nil, errors.New("published generation has no retained previous view")
		}
		var restore models.FeedGeneration
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("tenant_id=? AND public_id=? AND lane=? AND state='rollback' AND rollback_deadline>?", tenant, *current.PreviousGenerationID, lane, now).
			First(&restore).Error; err != nil {
			return nil, err
		}
		fences = append(fences, feedstate.ResetRollbackFence{Lane: lane, HeadVersion: head.Generation, ActiveID: current.PublicID, RestoreID: restore.PublicID})
	}
	if err := feedstate.ValidateResetRollbackFences(campaign.Lane, fences); err != nil {
		return nil, err
	}
	return fences, nil
}

func contentResetGenerationLanes(lane string) []string {
	switch lane {
	case "news":
		return []string{"news"}
	case "pods":
		return []string{"media"}
	default:
		return []string{"news", "media"}
	}
}
