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

// One command admits exactly one provider page. Once admitted it is only
// reconciled; neither worker restarts nor operator retries fetch it again.
type contentResetReplayPageOwner struct{}

func (contentResetReplayPageOwner) Contract() contentreset.Contract {
	return contentreset.Contract{Owner: "cms/source-run", Effect: "replay_page", TargetType: "source_branch", Version: "v1"}
}

func (owner contentResetReplayPageOwner) parameters(command contentreset.Command) (uuid.UUID, int, error) {
	branch, err := uuid.Parse(command.TargetID)
	var parameters struct {
		Ordinal int `json:"ordinal"`
	}
	if err != nil || branch == uuid.Nil || command.Contract != owner.Contract() ||
		json.Unmarshal(command.Parameters, &parameters) != nil || parameters.Ordinal < 1 || parameters.Ordinal > supply.ReplayMaxPages ||
		contentreset.Hash(command.Parameters) != contentreset.Hash(map[string]int{"ordinal": parameters.Ordinal}) {
		return uuid.Nil, 0, errors.New("invalid replay page command")
	}
	return branch, parameters.Ordinal, nil
}

func (owner contentResetReplayPageOwner) Execute(ctx context.Context, db *gorm.DB, command contentreset.Command, token uuid.UUID) (contentreset.Observation, error) {
	_, ordinal, err := owner.parameters(command)
	if err != nil {
		return contentreset.Observation{}, err
	}
	var page models.ContentResetReplayPage
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		page, err = supply.AdmitContentResetReplayPage(tx, command, token, ordinal)
		return err
	})
	if errors.Is(err, supply.ErrReplayYield) {
		// The admission transaction rolled back; no provider request exists.
		return resetObservation(command, "deferred", "replay_yield_to_live", map[string]any{"admitted": false}), nil
	}
	if err != nil {
		return contentreset.Observation{}, err
	}
	return resetObservation(command, "waiting", "replay_page_admitted", map[string]any{"page_id": page.PublicID, "request_id": page.SourceRunRequestID, "ordinal": page.Ordinal}), nil
}

func (owner contentResetReplayPageOwner) Reconcile(ctx context.Context, db *gorm.DB, command contentreset.Command) (contentreset.Observation, error) {
	branchID, ordinal, err := owner.parameters(command)
	if err != nil {
		return contentreset.Observation{}, err
	}
	var observation contentreset.Observation
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		campaign, revision, err := lockContentResetReadback(tx, command)
		if err != nil {
			return err
		}
		var branch models.ContentResetReplayBranch
		if err := tx.Where("tenant_id=? AND public_id=? AND campaign_id=? AND revision_id=? AND manifest_hash=?", command.TenantID, branchID, campaign.ID, revision.ID, command.ManifestHash).First(&branch).Error; err != nil {
			return err
		}
		var page models.ContentResetReplayPage
		err = tx.Where("tenant_id=? AND branch_id=? AND ordinal=?", command.TenantID, branchID, ordinal).First(&page).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			observation = resetObservation(command, "failed", "replay_page_not_committed", map[string]any{"admitted": false})
			observation.NoEffectProven = true
			return nil
		}
		if err != nil {
			return err
		}
		var request models.SourceRunRequest
		if err := tx.Where("tenant_id=? AND public_id=?", command.TenantID, page.SourceRunRequestID).First(&request).Error; err != nil {
			return err
		}
		if request.OperatorStepID == nil || *request.OperatorStepID != command.CommandID {
			return errors.New("replay page command binding changed")
		}
		// An open manifest or unsettled fetch is ordinary asynchronous progress.
		// A failed native request is terminal owner evidence, never exhaustion.
		switch request.State {
		case "failed", "cancelled", "expired", "partial", "blocked":
			observation = resetObservation(command, "failed", "replay_native_request_failed", map[string]any{"page_id": page.PublicID, "request_id": request.PublicID, "request_state": request.State})
			return nil
		}
		if request.ManifestState != "sealed" {
			observation = resetObservation(command, "waiting", "replay_manifest_pending", map[string]any{"page_id": page.PublicID, "request_state": request.State})
			return nil
		}
		var receipts int64
		if err := tx.Model(&models.SourceRunReceipt{}).Where("tenant_id=? AND source_run_request_id=? AND stage='fetch' AND event_type='provider_terminal'", command.TenantID, request.PublicID).Count(&receipts).Error; err != nil {
			return err
		}
		if receipts == 0 {
			if models.IsSourceRunTerminal(request.State) {
				observation = resetObservation(command, "failed", "replay_terminal_evidence_missing", map[string]any{"page_id": page.PublicID, "request_state": request.State})
				return nil
			}
			observation = resetObservation(command, "waiting", "replay_provider_pending", map[string]any{"page_id": page.PublicID})
			return nil
		}
		checkpoint, err := supply.RecordContentResetReplayCheckpoint(tx, page)
		if errors.Is(err, supply.ErrReplayYield) {
			observation = resetObservation(command, "waiting", "replay_reducer_pending", map[string]any{"page_id": page.PublicID})
			return nil
		}
		if errors.Is(err, supply.ErrReplayEvidenceInvalid) {
			observation = resetObservation(command, "failed", "replay_provider_evidence_invalid", map[string]any{"page_id": page.PublicID, "request_id": request.PublicID})
			return nil
		}
		if err != nil {
			return err
		}
		observation = resetObservation(command, "succeeded", "replay_page_observed", map[string]any{"page_id": page.PublicID, "request_id": request.PublicID, "checkpoint_id": checkpoint.PublicID, "ordinal": page.Ordinal, "exhausted": checkpoint.Exhausted, "observed": checkpoint.Observed, "admitted": checkpoint.Admitted, "delivery_verified": false})
		return nil
	})
	return observation, err
}

func resetObservation(command contentreset.Command, state, reason string, evidence any) contentreset.Observation {
	raw, _ := json.Marshal(evidence)
	return contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: state, ReasonCode: reason, Evidence: raw, NoEffectProven: state == "deferred"}
}

// Serialize with the full local SQL effect before interpreting its absence.
// Readback may persist evidence, but cannot perform the admitted domain effect.
func lockContentResetReadback(tx *gorm.DB, command contentreset.Command) (models.ContentResetCampaign, models.ContentResetRevision, error) {
	var campaign models.ContentResetCampaign
	var revision models.ContentResetRevision
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", command.TenantID, command.CampaignID).First(&campaign).Error; err != nil {
		return campaign, revision, err
	}
	var run models.ContentResetExecution
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND campaign_id=?", command.TenantID, campaign.ID).First(&run).Error; err != nil {
		return campaign, revision, err
	}
	if run.StartedAt == nil || run.ManifestHash != command.ManifestHash {
		return campaign, revision, contentreset.ErrFenced
	}
	if err := tx.Where("tenant_id=? AND campaign_id=? AND id=? AND public_id=? AND revision=?", command.TenantID, campaign.ID, run.RevisionID, command.RevisionID, campaign.CurrentRevision).First(&revision).Error; err != nil {
		return campaign, revision, err
	}
	if revision.ManifestHash == nil || *revision.ManifestHash != command.ManifestHash {
		return campaign, revision, contentreset.ErrFenced
	}
	if err := contentreset.ValidateExecutionEnvironment(tx, run); err != nil {
		return campaign, revision, err
	}
	return campaign, revision, nil
}
