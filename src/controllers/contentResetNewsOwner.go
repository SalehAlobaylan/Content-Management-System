package controllers

import (
	"content-management-system/src/contentreset"
	"content-management-system/src/feedstate"
	"content-management-system/src/models"
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type contentResetNewsCandidateOwner struct{}

func (contentResetNewsCandidateOwner) Contract() contentreset.Contract {
	return contentreset.Contract{Owner: "cms/news", Effect: "build_candidate", TargetType: "campaign", Version: "v1"}
}
func (owner contentResetNewsCandidateOwner) Execute(ctx context.Context, db *gorm.DB, command contentreset.Command, token uuid.UUID) (contentreset.Observation, error) {
	var generation models.FeedGeneration
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		campaign, revision, err := contentreset.LockOwnerCommand(tx, command, token)
		if err != nil {
			return err
		}
		if command.Contract != owner.Contract() || command.TargetID != campaign.PublicID.String() || contentreset.Hash(command.Parameters) != contentreset.Hash(json.RawMessage(`{}`)) {
			return errors.New("invalid News candidate command")
		}
		var req contentResetPlanRequest
		if json.Unmarshal(revision.Request, &req) != nil {
			return errors.New("News candidate requires a frozen Fresh Start intent")
		}
		if err := contentResetCandidateModeAllowed(tx, campaign, revision, req); err != nil {
			return err
		}
		generation, err = feedstate.BeginNewsFreshStartCandidate(tx, campaign.TenantID, campaign.ID)
		return err
	})
	if err != nil {
		return contentreset.Observation{}, err
	}
	return contentResetCandidateObservation(command, generation), nil
}
func (owner contentResetNewsCandidateOwner) Reconcile(ctx context.Context, db *gorm.DB, command contentreset.Command) (contentreset.Observation, error) {
	if command.Contract != owner.Contract() || command.TargetID != command.CampaignID.String() || contentreset.Hash(command.Parameters) != contentreset.Hash(json.RawMessage(`{}`)) {
		return contentreset.Observation{}, errors.New("invalid News candidate readback")
	}
	var observation contentreset.Observation
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		campaign, _, err := lockContentResetReadback(tx, command)
		if err != nil {
			return err
		}
		var candidates []models.FeedGeneration
		if err := tx.Where("tenant_id=? AND lane='news' AND purpose='content_reset' AND content_reset_campaign_id=?", command.TenantID, campaign.ID).Limit(2).Find(&candidates).Error; err != nil {
			return err
		}
		if len(candidates) == 0 {
			observation = resetObservation(command, "failed", "candidate_not_committed", map[string]any{"candidate_created": false})
			observation.NoEffectProven = true
			return nil
		}
		if len(candidates) != 1 {
			observation = resetObservation(command, "outcome_unknown", "candidate_readback_ambiguous", map[string]any{"verified": false})
			return nil
		}
		candidate := candidates[0]
		var head models.FeedGenerationHead
		if err := tx.Where("tenant_id=? AND lane='news'", command.TenantID).First(&head).Error; err != nil {
			return err
		}
		if candidate.State != "candidate" || head.CandidateGenerationID == nil || *head.CandidateGenerationID != candidate.PublicID || head.ActiveGenerationID == nil || candidate.PreviousGenerationID == nil || *head.ActiveGenerationID != *candidate.PreviousGenerationID {
			observation = resetObservation(command, "outcome_unknown", "candidate_head_fence_changed", map[string]any{"generation_id": candidate.PublicID, "candidate_created": true})
			return nil
		}
		observation = contentResetCandidateObservation(command, candidate)
		return nil
	})
	return observation, err
}
