package controllers

import (
	"context"
	"encoding/json"
	"errors"

	"content-management-system/src/contentreset"
	"content-management-system/src/feedstate"
	"content-management-system/src/models"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type contentResetPodsCandidateOwner struct{}

func (contentResetPodsCandidateOwner) Contract() contentreset.Contract {
	return contentreset.Contract{Owner: "cms/pods", Effect: "build_candidate", TargetType: "campaign", Version: "v1"}
}

func (owner contentResetPodsCandidateOwner) Execute(ctx context.Context, db *gorm.DB, command contentreset.Command, token uuid.UUID) (contentreset.Observation, error) {
	var generation models.FeedGeneration
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		campaign, revision, err := contentreset.LockOwnerCommand(tx, command, token)
		if err != nil {
			return err
		}
		if command.Contract != owner.Contract() || command.TargetID != campaign.PublicID.String() || contentreset.Hash(command.Parameters) != contentreset.Hash(json.RawMessage(`{}`)) {
			return errors.New("invalid Pods candidate command")
		}
		var req contentResetPlanRequest
		if json.Unmarshal(revision.Request, &req) != nil {
			return errors.New("Pods candidate owner requires a frozen Fresh Start intent")
		}
		if err := contentResetCandidateModeAllowed(tx, campaign, revision, req); err != nil {
			return err
		}
		generation, err = feedstate.BeginPodsFreshStartCandidate(tx, campaign.TenantID, campaign.ID)
		return err
	})
	if err != nil {
		return contentreset.Observation{}, err
	}
	return contentResetCandidateObservation(command, generation), nil
}

func (owner contentResetPodsCandidateOwner) Reconcile(ctx context.Context, db *gorm.DB, command contentreset.Command) (contentreset.Observation, error) {
	if command.Contract != owner.Contract() || command.TargetID != command.CampaignID.String() || contentreset.Hash(command.Parameters) != contentreset.Hash(json.RawMessage(`{}`)) {
		return contentreset.Observation{}, errors.New("invalid Pods candidate command")
	}
	var observation contentreset.Observation
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		campaign, _, err := lockContentResetReadback(tx, command)
		if err != nil {
			return err
		}
		// The creator holds the campaign lock through its full SQL effect.
		// After waiting for that transaction, no row proves no candidate was
		// committed. An existing row with a lost head is uncertainty instead.
		var generations []models.FeedGeneration
		if err := tx.Where("tenant_id=? AND lane='media' AND content_reset_campaign_id=? AND purpose='content_reset'", command.TenantID, campaign.ID).Limit(2).Find(&generations).Error; err != nil {
			return err
		}
		if len(generations) == 0 {
			observation = resetObservation(command, "failed", "candidate_not_committed", map[string]any{"candidate_created": false})
			observation.NoEffectProven = true
			return nil
		}
		if len(generations) != 1 {
			observation = resetObservation(command, "outcome_unknown", "candidate_readback_ambiguous", map[string]any{"verified": false})
			return nil
		}
		generation := generations[0]
		var head models.FeedGenerationHead
		if err := tx.Where("tenant_id=? AND lane='media'", command.TenantID).First(&head).Error; err != nil {
			return err
		}
		if generation.State != "candidate" || head.CandidateGenerationID == nil || *head.CandidateGenerationID != generation.PublicID || head.ActiveGenerationID == nil || generation.PreviousGenerationID == nil || *head.ActiveGenerationID != *generation.PreviousGenerationID {
			observation = resetObservation(command, "outcome_unknown", "candidate_head_fence_changed", map[string]any{"generation_id": generation.PublicID, "candidate_created": true, "verified": false})
			return nil
		}
		observation = contentResetCandidateObservation(command, generation)
		return nil
	})
	return observation, err
}

func contentResetCandidateObservation(command contentreset.Command, generation models.FeedGeneration) contentreset.Observation {
	evidence, _ := json.Marshal(map[string]any{"generation_id": generation.PublicID, "previous_generation_id": generation.PreviousGenerationID, "candidate_created": true, "replacement_ready": false, "published": false})
	return contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "succeeded", ReasonCode: "candidate_created", Evidence: evidence}
}
