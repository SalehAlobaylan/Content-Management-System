package controllers

import (
	"context"
	"encoding/json"
	"errors"

	"content-management-system/src/contentreset"
	"content-management-system/src/models"
	"content-management-system/src/supply"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// This adapter prepares immutable replay identity only. Page execution,
// checkpoint handoff and publication require their own qualified contracts.
type contentResetReplayBranchOwner struct{}

func (contentResetReplayBranchOwner) Contract() contentreset.Contract {
	return contentreset.Contract{Owner: "cms/source-run", Effect: "prepare_replay_branch", TargetType: "source", Version: "v1"}
}

func (owner contentResetReplayBranchOwner) Execute(ctx context.Context, db *gorm.DB, command contentreset.Command, token uuid.UUID) (contentreset.Observation, error) {
	var branch models.ContentResetReplayBranch
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		branch, err = supply.CreateContentResetReplayBranch(tx, command, token)
		return err
	})
	if err != nil {
		return contentreset.Observation{}, err
	}
	return contentResetReplayBranchObservation(command, branch), nil
}

func (owner contentResetReplayBranchOwner) Reconcile(ctx context.Context, db *gorm.DB, command contentreset.Command) (contentreset.Observation, error) {
	if command.Contract != owner.Contract() || contentreset.Hash(command.Parameters) != contentreset.Hash(json.RawMessage(`{}`)) {
		return contentreset.Observation{}, errors.New("invalid replay branch command")
	}
	sourceID, err := uuid.Parse(command.TargetID)
	if err != nil {
		return contentreset.Observation{}, err
	}
	var observation contentreset.Observation
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// The writer holds this lock through its entire SQL effect. Readback
		// waits for that transaction, so absence proves no branch was committed.
		var campaign models.ContentResetCampaign
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", command.TenantID, command.CampaignID).First(&campaign).Error; err != nil {
			return err
		}
		var revision models.ContentResetRevision
		if err := tx.Where("tenant_id=? AND campaign_id=? AND public_id=? AND revision=?", command.TenantID, campaign.ID, command.RevisionID, campaign.CurrentRevision).First(&revision).Error; err != nil {
			return err
		}
		if revision.ManifestHash == nil || *revision.ManifestHash != command.ManifestHash {
			return errors.New("replay branch manifest changed")
		}
		var branch models.ContentResetReplayBranch
		err := tx.Where("tenant_id=? AND campaign_id=? AND revision_id=? AND content_source_id=? AND manifest_hash=?", command.TenantID, campaign.ID, revision.ID, sourceID, command.ManifestHash).First(&branch).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			observation = contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "failed", ReasonCode: "replay_branch_not_committed", Evidence: json.RawMessage(`{"branch_created":false}`), NoEffectProven: true}
			return nil
		}
		if err != nil {
			return err
		}
		var spec supply.ReplaySpec
		if json.Unmarshal(branch.Spec, &spec) != nil || spec.Validate() != nil || contentreset.Hash(spec) != branch.SpecHash {
			return errors.New("replay branch readback integrity failure")
		}
		observation = contentResetReplayBranchObservation(command, branch)
		return nil
	})
	return observation, err
}

func contentResetReplayBranchObservation(command contentreset.Command, branch models.ContentResetReplayBranch) contentreset.Observation {
	evidence, _ := json.Marshal(map[string]any{"branch_id": branch.PublicID, "content_source_id": branch.ContentSourceID, "spec_hash": branch.SpecHash, "branch_created": true, "delivery_verified": false, "handoff_verified": false})
	return contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "succeeded", ReasonCode: "replay_branch_created", Evidence: evidence}
}
