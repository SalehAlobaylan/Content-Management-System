package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"content-management-system/src/contentreset"
	"content-management-system/src/feedstate"
	"content-management-system/src/lifecycle"
	"content-management-system/src/models"
	"content-management-system/src/supply"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ---- Replay handoff owner ----

type contentResetHandoffOwner struct{}

func (contentResetHandoffOwner) Contract() contentreset.Contract {
	return contentreset.Contract{Owner: "cms/source-run", Effect: "replay_and_handoff", TargetType: "source_branch", Version: "v1"}
}

func (owner contentResetHandoffOwner) Execute(ctx context.Context, db *gorm.DB, command contentreset.Command, token uuid.UUID) (contentreset.Observation, error) {
	var handoff models.ContentResetReplayHandoff
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		handoff, err = supply.HandoffContentResetReplayBranch(tx, command, token)
		if err != nil {
			return err
		}
		return appendHandoffEvidence(tx, command, handoff)
	})
	if errors.Is(err, supply.ErrReplayYield) || errors.Is(err, supply.ErrReplayHandoffPending) {
		return resetObservation(command, "deferred", "replay_handoff_pending_provider_settlement", map[string]any{"handoff_verified": false}), nil
	}
	if err != nil {
		return contentreset.Observation{}, err
	}
	return resetObservation(command, "succeeded", "replay_handoff_complete", map[string]any{
		"branch_id": handoff.BranchID, "content_source_id": handoff.ContentSourceID, "pages": handoff.Pages,
		"observed_until": handoff.ObservedUntil, "handoff_verified": true,
	}), nil
}

func (owner contentResetHandoffOwner) Reconcile(ctx context.Context, db *gorm.DB, command contentreset.Command) (contentreset.Observation, error) {
	var observation contentreset.Observation
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		handoff, err := supply.ReadContentResetReplayHandoff(tx, command)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			observation = resetObservation(command, "failed", "replay_handoff_not_committed", map[string]any{"handoff_verified": false})
			observation.NoEffectProven = true
			return nil
		}
		if errors.Is(err, supply.ErrReplayHandoffPending) || errors.Is(err, supply.ErrReplayYield) {
			observation = resetObservation(command, "waiting", "replay_handoff_pending_provider_settlement", map[string]any{"handoff_verified": false})
			return nil
		}
		if err != nil {
			return err
		}
		observation = resetObservation(command, "succeeded", "replay_handoff_complete", map[string]any{
			"branch_id": handoff.BranchID, "content_source_id": handoff.ContentSourceID, "pages": handoff.Pages,
			"observed_until": handoff.ObservedUntil, "handoff_verified": true,
		})
		return nil
	})
	return observation, err
}

func appendHandoffEvidence(tx *gorm.DB, command contentreset.Command, handoff models.ContentResetReplayHandoff) error {
	var campaign models.ContentResetCampaign
	if err := tx.Where("tenant_id=? AND public_id=?", command.TenantID, command.CampaignID).First(&campaign).Error; err != nil {
		return err
	}
	var revision models.ContentResetRevision
	if err := tx.Where("tenant_id=? AND campaign_id=? AND public_id=? AND revision=?", command.TenantID, campaign.ID, command.RevisionID, campaign.CurrentRevision).First(&revision).Error; err != nil {
		return err
	}
	return appendContentResetEvidence(tx, campaign, &revision, "replay-handoff/"+handoff.BranchID.String(), "owner_handoff", "cms/source-run", map[string]any{
		"branch_id": handoff.BranchID, "content_source_id": handoff.ContentSourceID, "pages": handoff.Pages,
		"observed_until": handoff.ObservedUntil, "spec_hash": handoff.SpecHash,
	})
}

// ---- Durable intake pause owner ----

type contentResetPauseOwner struct{}

func (contentResetPauseOwner) Contract() contentreset.Contract {
	return contentreset.Contract{Owner: "cms/source-run", Effect: "acquire_intake_pause", TargetType: "campaign", Version: "v1"}
}

func contentResetPauseLane(command contentreset.Command) (string, error) {
	var parameters struct {
		Lane string `json:"lane"`
	}
	if json.Unmarshal(command.Parameters, &parameters) != nil || (parameters.Lane != "news" && parameters.Lane != "pods") ||
		contentreset.Hash(command.Parameters) != contentreset.Hash(parameters) {
		return "", errors.New("invalid Content Reset intake pause command")
	}
	return parameters.Lane, nil
}

func (owner contentResetPauseOwner) Execute(ctx context.Context, db *gorm.DB, command contentreset.Command, token uuid.UUID) (contentreset.Observation, error) {
	lane, err := contentResetPauseLane(command)
	if err != nil {
		return contentreset.Observation{}, err
	}
	var pause lifecycle.IntakePause
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		campaign, _, lockErr := contentreset.LockOwnerCommand(tx, command, token)
		if lockErr != nil {
			return lockErr
		}
		if campaign.Operation != "empty" {
			return errors.New("intake pause acquisition is only authorized for an Empty operation")
		}
		var acquireErr error
		pause, acquireErr = lifecycle.AcquireIntakePause(tx, lifecycle.Scope{TenantID: command.TenantID, Lane: lane}, campaign.ID, "cms/content-reset", "content_reset_empty")
		return acquireErr
	})
	if lifecycle.IsConflict(err) || lifecycle.IsIntakePaused(err) {
		return resetObservation(command, "deferred", "intake_pause_waiting_for_source_work", map[string]any{"pause_active": false, "lane": lane}), nil
	}
	if err != nil {
		return contentreset.Observation{}, err
	}
	return resetObservation(command, "succeeded", "intake_pause_active", map[string]any{"pause_id": pause.PublicID, "lane": lane, "pause_active": true}), nil
}

func (owner contentResetPauseOwner) Reconcile(ctx context.Context, db *gorm.DB, command contentreset.Command) (contentreset.Observation, error) {
	lane, err := contentResetPauseLane(command)
	if err != nil {
		return contentreset.Observation{}, err
	}
	var observation contentreset.Observation
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		campaign, _, err := lockContentResetReadback(tx, command)
		if err != nil {
			return err
		}
		var pause models.ContentResetIntakePause
		err = tx.Where("tenant_id=? AND campaign_id=? AND lane=? AND state='active'", command.TenantID, campaign.ID, lane).First(&pause).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			observation = resetObservation(command, "failed", "intake_pause_not_committed", map[string]any{"pause_active": false, "lane": lane})
			observation.NoEffectProven = true
			return nil
		}
		if err != nil {
			return err
		}
		observation = resetObservation(command, "succeeded", "intake_pause_active", map[string]any{"pause_id": pause.PublicID, "lane": lane, "pause_active": true})
		return nil
	})
	return observation, err
}

// ---- Serving verification owner ----

type contentResetServingOwner struct{ lane string }

func (owner contentResetServingOwner) Contract() contentreset.Contract {
	return contentreset.Contract{Owner: "cms/" + owner.lane, Effect: "verify_serving", TargetType: "campaign", Version: "v1"}
}

func contentResetServingLane(command contentreset.Command, expected string) (string, error) {
	var parameters struct {
		Lane string `json:"lane"`
	}
	if json.Unmarshal(command.Parameters, &parameters) != nil || parameters.Lane != expected ||
		contentreset.Hash(command.Parameters) != contentreset.Hash(parameters) {
		return "", errors.New("invalid Content Reset serving verification command")
	}
	return parameters.Lane, nil
}

func (owner contentResetServingOwner) Execute(ctx context.Context, db *gorm.DB, command contentreset.Command, token uuid.UUID) (contentreset.Observation, error) {
	if _, err := contentResetServingLane(command, owner.lane); err != nil {
		return contentreset.Observation{}, err
	}
	var observation contentreset.Observation
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		campaign, revision, lockErr := contentreset.LockOwnerCommand(tx, command, token)
		if lockErr != nil {
			return lockErr
		}
		observation = verifyContentResetServing(tx, campaign, revision, owner.lane, command)
		return nil
	})
	return observation, err
}

func (owner contentResetServingOwner) Reconcile(ctx context.Context, db *gorm.DB, command contentreset.Command) (contentreset.Observation, error) {
	if _, err := contentResetServingLane(command, owner.lane); err != nil {
		return contentreset.Observation{}, err
	}
	var observation contentreset.Observation
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		campaign, revision, lockErr := lockContentResetReadback(tx, command)
		if lockErr != nil {
			return lockErr
		}
		observation = verifyContentResetServing(tx, campaign, revision, owner.lane, command)
		return nil
	})
	return observation, err
}

func verifyContentResetServing(tx *gorm.DB, campaign models.ContentResetCampaign, revision models.ContentResetRevision, lane string, command contentreset.Command) contentreset.Observation {
	var cursor int64
	var stillServing int64
	for {
		var targets []models.ContentResetTarget
		if err := tx.Where("tenant_id=? AND revision_id=? AND lane=? AND disposition='selected' AND protected=FALSE AND item_ordinal>?",
			campaign.TenantID, revision.ID, lane, cursor).Order("item_ordinal ASC").Limit(contentResetPlanPageSize).Find(&targets).Error; err != nil {
			return contentreset.Observation{}
		}
		if len(targets) == 0 {
			break
		}
		ids := make([]uuid.UUID, 0, len(targets))
		for _, target := range targets {
			ids = append(ids, target.ContentItemID)
			cursor = target.ItemOrdinal
		}
		var count int64
		if err := publicContentQuery(tx).Where("content_items.public_id IN ?", ids).Count(&count).Error; err != nil {
			return contentreset.Observation{}
		}
		stillServing += count
	}
	if stillServing != 0 {
		evidence, _ := json.Marshal(map[string]any{"lane": lane, "still_serving": stillServing})
		return contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "waiting", ReasonCode: "selected_content_still_serving", Evidence: evidence}
	}
	evidence, _ := json.Marshal(map[string]any{"lane": lane, "still_serving": 0})
	return contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "succeeded", ReasonCode: "serving_scope_withdrawn", Evidence: evidence}
}

// ---- Storage cleanup verification owner ----

type contentResetCleanupOwner struct{}

func (contentResetCleanupOwner) Contract() contentreset.Contract {
	return contentreset.Contract{Owner: "cms/storage", Effect: "cleanup_exact", TargetType: "manifest_batch", Version: "v1"}
}

func (owner contentResetCleanupOwner) Execute(ctx context.Context, db *gorm.DB, command contentreset.Command, token uuid.UUID) (contentreset.Observation, error) {
	var observation contentreset.Observation
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		campaign, revision, lockErr := contentreset.LockOwnerCommand(tx, command, token)
		if lockErr != nil {
			return lockErr
		}
		var verifyErr error
		observation, verifyErr = verifyContentResetStorageCleanup(tx, campaign, revision, command)
		return verifyErr
	})
	return observation, err
}

func (owner contentResetCleanupOwner) Reconcile(ctx context.Context, db *gorm.DB, command contentreset.Command) (contentreset.Observation, error) {
	var observation contentreset.Observation
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		campaign, revision, lockErr := lockContentResetReadback(tx, command)
		if lockErr != nil {
			return lockErr
		}
		var verifyErr error
		observation, verifyErr = verifyContentResetStorageCleanup(tx, campaign, revision, command)
		return verifyErr
	})
	return observation, err
}

func verifyContentResetStorageCleanup(tx *gorm.DB, campaign models.ContentResetCampaign, revision models.ContentResetRevision, command contentreset.Command) (contentreset.Observation, error) {
	var cursor int64
	residual := int64(0)
	unrecoverable := int64(0)
	mediaLane := campaign.Lane == "pods" || campaign.Lane == "both"
	if mediaLane {
		for {
			var targets []models.ContentResetTarget
			if err := tx.Where("tenant_id=? AND revision_id=? AND lane='pods' AND disposition='selected' AND protected=FALSE AND item_ordinal>?",
				campaign.TenantID, revision.ID, cursor).Order("item_ordinal ASC").Limit(contentResetPlanPageSize).Find(&targets).Error; err != nil {
				return contentreset.Observation{}, err
			}
			if len(targets) == 0 {
				break
			}
			ids := make([]uuid.UUID, 0, len(targets))
			for _, target := range targets {
				ids = append(ids, target.ContentItemID)
				cursor = target.ItemOrdinal
			}
			if tx.Migrator().HasTable(&models.MediaArtifactManifest{}) {
				var count int64
				if err := tx.Model(&models.MediaArtifactManifest{}).
					Where("tenant_id=? AND (content_item_id IN ? OR (content_item_id IS NULL AND parent_content_item_id IN ?)) AND state<>'deleted' AND deleted_at IS NULL", campaign.TenantID, ids, ids).
					Count(&count).Error; err != nil {
					return contentreset.Observation{}, err
				}
				residual += count
			}
			var recoverable int64
			if err := tx.Model(&models.ContentItem{}).Where("tenant_id=? AND public_id IN ? AND (storage_state IS NULL OR storage_state NOT IN ?)", campaign.TenantID, ids, []string{models.StorageStateUnrecoverable}).
				Count(&recoverable).Error; err != nil {
				return contentreset.Observation{}, err
			}
			unrecoverable += recoverable
		}
	}
	if residual != 0 || unrecoverable != 0 {
		evidence, _ := json.Marshal(map[string]any{"residual_artifact_manifests": residual, "recoverable_payloads": unrecoverable})
		return contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "waiting", ReasonCode: "cleanup_obligations_pending", Evidence: evidence}, nil
	}
	evidence, _ := json.Marshal(map[string]any{"residual_artifact_manifests": 0, "recoverable_payloads": 0, "exact_cleanup_verified": true})
	return contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "succeeded", ReasonCode: "cleanup_exact_verified", Evidence: evidence}, nil
}

// contentResetCandidateModeAllowed admits candidate creation for build-first
// immediately, and for clear-first only after every initial retirement batch
// succeeded. Removing the strategy check alone would let a clear-first campaign
// build before its irreversible retirement ran; rebuilding after retirement is
// the only supported clear-first order.
func contentResetCandidateModeAllowed(tx *gorm.DB, campaign models.ContentResetCampaign, revision models.ContentResetRevision, req contentResetPlanRequest) error {
	if req.Operation != "fresh_start" {
		return errors.New("candidate creation requires Fresh Start intent")
	}
	if req.CapacityStrategy == "build_first" {
		return nil
	}
	if req.CapacityStrategy != "clear_first" {
		return errors.New("candidate creation requires a supported capacity strategy")
	}
	var run models.ContentResetExecution
	if err := tx.Where("tenant_id=? AND campaign_id=? AND revision_id=?", campaign.TenantID, campaign.ID, revision.ID).First(&run).Error; err != nil {
		return err
	}
	if run.PublishedAt != nil {
		return errors.New("clear-first candidate creation is already past publication")
	}
	_, total, err := contentResetRetirementPairs(tx, campaign, revision, req)
	if err != nil {
		return err
	}
	var steps, succeeded, failed int64
	if err := tx.Model(&models.ContentResetStep{}).
		Where("tenant_id=? AND campaign_id=? AND revision_id=? AND step_key LIKE 'retire/%'", campaign.TenantID, campaign.ID, revision.ID).
		Count(&steps).Error; err != nil {
		return err
	}
	if err := tx.Model(&models.ContentResetStep{}).
		Where("tenant_id=? AND campaign_id=? AND revision_id=? AND step_key LIKE 'retire/%' AND state='succeeded'", campaign.TenantID, campaign.ID, revision.ID).
		Count(&succeeded).Error; err != nil {
		return err
	}
	if err := tx.Model(&models.ContentResetStep{}).
		Where("tenant_id=? AND campaign_id=? AND revision_id=? AND step_key LIKE 'retire/%' AND state IN ?", campaign.TenantID, campaign.ID, revision.ID, []string{"failed", "blocked"}).
		Count(&failed).Error; err != nil {
		return err
	}
	if int(steps) != total || int(succeeded) != total || failed != 0 {
		return errors.New("clear-first candidate creation requires every initial retirement batch to succeed")
	}
	return nil
}

// ---- Replacement readiness owner ----

type contentResetReadinessOwner struct{}

func (contentResetReadinessOwner) Contract() contentreset.Contract {
	return contentreset.Contract{Owner: "cms/content-reset", Effect: "verify_readiness", TargetType: "campaign", Version: "v1"}
}

func (owner contentResetReadinessOwner) Execute(ctx context.Context, db *gorm.DB, command contentreset.Command, token uuid.UUID) (contentreset.Observation, error) {
	var observation contentreset.Observation
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		campaign, revision, lockErr := contentreset.LockOwnerCommand(tx, command, token)
		if lockErr != nil {
			return lockErr
		}
		var readinessErr error
		observation, readinessErr = verifyContentResetReplacementReadiness(tx, campaign, revision, command)
		return readinessErr
	})
	return observation, err
}

func (owner contentResetReadinessOwner) Reconcile(ctx context.Context, db *gorm.DB, command contentreset.Command) (contentreset.Observation, error) {
	var observation contentreset.Observation
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		campaign, revision, lockErr := lockContentResetReadback(tx, command)
		if lockErr != nil {
			return lockErr
		}
		var readinessErr error
		observation, readinessErr = verifyContentResetReplacementReadiness(tx, campaign, revision, command)
		return readinessErr
	})
	return observation, err
}

func verifyContentResetReplacementReadiness(tx *gorm.DB, campaign models.ContentResetCampaign, revision models.ContentResetRevision, command contentreset.Command) (contentreset.Observation, error) {
	if campaign.Operation != "fresh_start" {
		evidence, _ := json.Marshal(map[string]any{"operation": campaign.Operation})
		return contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "succeeded", ReasonCode: "no_replacement_required", Evidence: evidence}, nil
	}
	// Readiness verifies only the build/replay predecessors. Publication,
	// rollback, cleanup and completion commands run after readiness and must not
	// make the replacement look unfinished (the publication owner itself calls
	// this while its publish step is claimed).
	unsettled := int64(0)
	if err := tx.Model(&models.ContentResetStep{}).
		Where("tenant_id=? AND campaign_id=? AND revision_id=? AND state<>'succeeded' AND (step_key LIKE 'replay/%' OR step_key LIKE 'candidate/%')", campaign.TenantID, campaign.ID, revision.ID).
		Count(&unsettled).Error; err != nil {
		return contentreset.Observation{}, err
	}
	if unsettled != 0 {
		evidence, _ := json.Marshal(map[string]any{"unsettled_steps": unsettled})
		return contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "waiting", ReasonCode: "replay_or_build_steps_pending", Evidence: evidence}, nil
	}
	lanes := contentResetLanes(campaign.Lane)
	proofs := map[string]any{}
	for _, lane := range lanes {
		var generation models.FeedGeneration
		err := tx.Where("tenant_id=? AND lane=? AND purpose='content_reset' AND state='candidate' AND content_reset_campaign_id=?",
			campaign.TenantID, mapContentResetGenerationLane(lane), campaign.ID).First(&generation).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			evidence, _ := json.Marshal(map[string]any{"lane": lane, "reason": "candidate_generation_missing"})
			return contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "blocked", ReasonCode: "replacement_candidate_missing", Evidence: evidence}, nil
		}
		if err != nil {
			return contentreset.Observation{}, err
		}
		switch lane {
		case "pods":
			proof, verifyErr := feedstate.VerifyPodsFreshStartCandidate(tx, campaign.TenantID, campaign.ID, generation.PublicID)
			if verifyErr != nil {
				evidence, _ := json.Marshal(map[string]any{"lane": lane, "reason": verifyErr.Error(), "proof": proof})
				return contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "waiting", ReasonCode: "replacement_not_ready", Evidence: evidence}, nil
			}
			proofs["pods"] = proof
		case "news":
			proof, verifyErr := feedstate.VerifyNewsFreshStartCandidate(tx, campaign.TenantID, campaign.ID, generation.PublicID)
			if verifyErr != nil {
				evidence, _ := json.Marshal(map[string]any{"lane": lane, "reason": verifyErr.Error(), "proof": proof})
				return contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "waiting", ReasonCode: "replacement_not_ready", Evidence: evidence}, nil
			}
			proofs["news"] = proof
		}
	}
	evidence, _ := json.Marshal(map[string]any{"lanes": lanes, "proofs": proofs, "replacement_ready": true})
	return contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "succeeded", ReasonCode: "replacement_ready", Evidence: evidence}, nil
}

func mapContentResetGenerationLane(lane string) string {
	if lane == "pods" {
		return "media"
	}
	return lane
}

// ---- Campaign completion owner ----

type contentResetCompleteOwner struct{}

func (contentResetCompleteOwner) Contract() contentreset.Contract {
	return contentreset.Contract{Owner: "cms/content-reset", Effect: "verify_complete", TargetType: "campaign", Version: "v1"}
}

func (owner contentResetCompleteOwner) Execute(ctx context.Context, db *gorm.DB, command contentreset.Command, token uuid.UUID) (contentreset.Observation, error) {
	var observation contentreset.Observation
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		campaign, revision, lockErr := contentreset.LockOwnerCommand(tx, command, token)
		if lockErr != nil {
			return lockErr
		}
		var completeErr error
		observation, completeErr = completeContentResetCampaign(tx, campaign, revision, command)
		return completeErr
	})
	return observation, err
}

func (owner contentResetCompleteOwner) Reconcile(ctx context.Context, db *gorm.DB, command contentreset.Command) (contentreset.Observation, error) {
	var observation contentreset.Observation
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		campaign, revision, lockErr := lockContentResetReadback(tx, command)
		if lockErr != nil {
			return lockErr
		}
		var run models.ContentResetExecution
		if err := tx.Where("tenant_id=? AND campaign_id=? AND revision_id=?", command.TenantID, campaign.ID, revision.ID).First(&run).Error; err != nil {
			return err
		}
		// A later Empty intake-resume changes the execution phase, so terminal
		// recovery binds the durable completion timestamp and campaign state,
		// not the mutable phase.
		terminal := campaign.State == "complete" && run.CompletedAt != nil
		if terminal {
			evidence, _ := json.Marshal(map[string]any{"campaign_state": campaign.State, "revision": revision.Revision, "completed_at": run.CompletedAt})
			observation = contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "succeeded", ReasonCode: "campaign_complete", Evidence: evidence}
			return nil
		}
		if run.CompletedAt != nil || campaign.State == "complete" {
			evidence, _ := json.Marshal(map[string]any{"campaign_state": campaign.State, "execution_phase": run.Phase, "completed_at": run.CompletedAt})
			observation = contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "outcome_unknown", ReasonCode: "completion_state_inconsistent", Evidence: evidence}
			return nil
		}
		evidence, _ := json.Marshal(map[string]any{"campaign_state": campaign.State, "execution_phase": run.Phase})
		observation = contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "failed", ReasonCode: "completion_not_committed", Evidence: evidence, NoEffectProven: true}
		return nil
	})
	return observation, err
}

func completeContentResetCampaign(tx *gorm.DB, campaign models.ContentResetCampaign, revision models.ContentResetRevision, command contentreset.Command) (contentreset.Observation, error) {
	var run models.ContentResetExecution
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND campaign_id=? AND revision_id=?", campaign.TenantID, campaign.ID, revision.ID).First(&run).Error; err != nil {
		return contentreset.Observation{}, err
	}
	if run.PublishedAt == nil && campaign.Operation == "fresh_start" {
		evidence, _ := json.Marshal(map[string]any{"reason": "publication_required"})
		return contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "waiting", ReasonCode: "publication_pending", Evidence: evidence}, nil
	}
	if campaign.Operation == "fresh_start" && run.PublishedAt != nil && run.RolledBackAt != nil {
		evidence, _ := json.Marshal(map[string]any{"reason": "campaign_was_rolled_back"})
		return contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "blocked", ReasonCode: "campaign_rolled_back", Evidence: evidence}, nil
	}
	if campaign.Operation == "empty" {
		for _, lane := range contentResetLanes(campaign.Lane) {
			var pauses int64
			if err := tx.Model(&models.ContentResetIntakePause{}).Where("tenant_id=? AND campaign_id=? AND lane=? AND state='active'", campaign.TenantID, campaign.ID, lane).Count(&pauses).Error; err != nil {
				return contentreset.Observation{}, err
			}
			if pauses != 1 {
				evidence, _ := json.Marshal(map[string]any{"lane": lane, "pause_active": false})
				return contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "blocked", ReasonCode: "intake_pause_missing", Evidence: evidence}, nil
			}
		}
	}
	if campaign.Operation == "fresh_start" && run.PublishedAt != nil {
		cleanupObservation, err := verifyContentResetStorageCleanup(tx, campaign, revision, command)
		if err != nil {
			return contentreset.Observation{}, err
		}
		if cleanupObservation.State != "succeeded" {
			return cleanupObservation, nil
		}
		if run.CleanupAuthorizedUntil == nil || !run.CleanupAuthorizedUntil.After(time.Now().UTC()) {
			evidence, _ := json.Marshal(map[string]any{"reason": "cleanup_authorization_expired"})
			return contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "blocked", ReasonCode: "cleanup_authorization_expired", Evidence: evidence}, nil
		}
	}
	var pending int64
	if err := tx.Model(&models.ContentResetStep{}).
		Where("tenant_id=? AND campaign_id=? AND revision_id=? AND state<>'succeeded' AND step_key<>'verify/complete'", campaign.TenantID, campaign.ID, revision.ID).
		Count(&pending).Error; err != nil {
		return contentreset.Observation{}, err
	}
	if pending != 0 {
		evidence, _ := json.Marshal(map[string]any{"unsettled_steps": pending})
		return contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "waiting", ReasonCode: "obligations_pending", Evidence: evidence}, nil
	}
	now := time.Now().UTC()
	if err := tx.Model(&models.ContentResetExecution{}).Where("tenant_id=? AND campaign_id=? AND revision_id=?", campaign.TenantID, campaign.ID, revision.ID).
		Updates(map[string]any{"phase": "completed", "completed_at": now, "pause_requested": false}).Error; err != nil {
		return contentreset.Observation{}, err
	}
	if err := tx.Model(&models.ContentResetCampaign{}).Where("tenant_id=? AND id=? AND state IN ?", campaign.TenantID, campaign.ID, []string{"executing", "published", "cleanup_pending", "partial"}).Update("state", "complete").Error; err != nil {
		return contentreset.Observation{}, err
	}
	if err := releaseContentResetLaneClaims(tx, campaign); err != nil {
		return contentreset.Observation{}, err
	}
	evidence, _ := json.Marshal(map[string]any{"completed_at": now, "initial_request": campaign.Operation, "lane": campaign.Lane})
	return contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "succeeded", ReasonCode: "campaign_complete", Evidence: evidence}, nil
}

// releaseContentResetIntakePauses removes only pauses owned by this campaign.
// It is used when an execution is abandoned so no unreleasable pause outlives
// its owner; a completed Empty keeps its pause for the separate resume action.
func releaseContentResetIntakePauses(tx *gorm.DB, campaign models.ContentResetCampaign) error {
	if !lifecycle.SchemaAvailable(tx) {
		return nil
	}
	var pauses []models.ContentResetIntakePause
	if err := tx.Where("tenant_id=? AND campaign_id=? AND state='active'", campaign.TenantID, campaign.ID).Find(&pauses).Error; err != nil {
		return err
	}
	for _, pause := range pauses {
		if err := lifecycle.ReleaseIntakePause(tx, campaign.TenantID, campaign.ID, pause.PublicID, pause.FencingToken); err != nil {
			return err
		}
	}
	return nil
}

func releaseContentResetLaneClaims(tx *gorm.DB, campaign models.ContentResetCampaign) error {
	if !lifecycle.SchemaAvailable(tx) {
		return nil
	}
	var claims []models.LifecycleOperationClaim
	if err := tx.Where("tenant_id=? AND campaign_id=? AND owner=? AND state=?", campaign.TenantID, campaign.ID, "cms/content-reset", lifecycle.ClaimActive).Find(&claims).Error; err != nil {
		return err
	}
	if len(claims) == 0 {
		return nil
	}
	tokens := make(map[uuid.UUID]uuid.UUID, len(claims))
	for _, claim := range claims {
		tokens[claim.PublicID] = claim.FencingToken
	}
	return lifecycle.Release(tx, campaign.TenantID, campaign.ID, tokens)
}
