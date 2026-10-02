package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"content-management-system/src/contentreset"
	"content-management-system/src/models"
	"content-management-system/src/supply"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type contentResetPrepareOwner struct{}

func (contentResetPrepareOwner) Contract() contentreset.Contract {
	return contentreset.Contract{Owner: "cms/content-reset", Effect: "prepare", TargetType: "campaign", Version: "v1"}
}

func (owner contentResetPrepareOwner) Execute(ctx context.Context, db *gorm.DB, command contentreset.Command, token uuid.UUID) (contentreset.Observation, error) {
	var workflow models.ContentResetWorkflow
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		campaign, revision, err := contentreset.LockOwnerCommand(tx, command, token)
		if err != nil {
			return err
		}
		if command.Contract != owner.Contract() || command.TargetID != campaign.PublicID.String() || contentreset.Hash(command.Parameters) != contentreset.Hash(json.RawMessage(`{}`)) {
			return errors.New("invalid campaign preparation command")
		}
		err = tx.Where("tenant_id=? AND campaign_id=?", command.TenantID, campaign.ID).First(&workflow).Error
		if err == nil {
			return validateContentResetWorkflow(workflow, campaign, revision, command.CommandID)
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		workflow = models.ContentResetWorkflow{TenantID: campaign.TenantID, CampaignID: campaign.ID, RevisionID: revision.ID, ManifestHash: command.ManifestHash, PreparedCommandID: command.CommandID, Phase: "seed"}
		return tx.Create(&workflow).Error
	})
	if err != nil {
		return contentreset.Observation{}, err
	}
	return resetObservation(command, "succeeded", "campaign_prepared", map[string]any{"workflow_created": true, "manifest_hash": workflow.ManifestHash}), nil
}

func (owner contentResetPrepareOwner) Reconcile(ctx context.Context, db *gorm.DB, command contentreset.Command) (contentreset.Observation, error) {
	if command.Contract != owner.Contract() || command.TargetID != command.CampaignID.String() || contentreset.Hash(command.Parameters) != contentreset.Hash(json.RawMessage(`{}`)) {
		return contentreset.Observation{}, errors.New("invalid campaign preparation readback")
	}
	var observation contentreset.Observation
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		campaign, revision, err := lockContentResetReadback(tx, command)
		if err != nil {
			return err
		}
		var workflow models.ContentResetWorkflow
		err = tx.Where("tenant_id=? AND campaign_id=?", command.TenantID, campaign.ID).First(&workflow).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			observation = resetObservation(command, "failed", "campaign_prepare_not_committed", map[string]any{"workflow_created": false})
			observation.NoEffectProven = true
			return nil
		}
		if err != nil {
			return err
		}
		if err := validateContentResetWorkflow(workflow, campaign, revision, command.CommandID); err != nil {
			return err
		}
		observation = resetObservation(command, "succeeded", "campaign_prepared", map[string]any{"workflow_created": true, "manifest_hash": workflow.ManifestHash})
		return nil
	})
	return observation, err
}

func validateContentResetWorkflow(workflow models.ContentResetWorkflow, campaign models.ContentResetCampaign, revision models.ContentResetRevision, commandID uuid.UUID) error {
	if revision.ManifestHash == nil || workflow.TenantID != campaign.TenantID || workflow.CampaignID != campaign.ID || workflow.RevisionID != revision.ID || workflow.ManifestHash != *revision.ManifestHash || workflow.PreparedCommandID != commandID {
		return errors.New("campaign workflow binding changed")
	}
	return nil
}

// Compose at most one bounded outbox page per pass. Cursors and commands commit
// together; a restart can never skip a frozen source, batch or phase, or
// create a changed graph.
func planContentResetWorkflow(ctx context.Context, db *gorm.DB, engine *contentreset.Engine, tenant string, campaignID uint) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var campaign models.ContentResetCampaign
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND id=?", tenant, campaignID).First(&campaign).Error; err != nil {
			return err
		}
		if campaign.State != "executing" && campaign.State != "published" && campaign.State != "cleanup_pending" {
			return nil
		}
		var run models.ContentResetExecution
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND campaign_id=?", tenant, campaignID).First(&run).Error; err != nil {
			return err
		}
		if run.PauseRequested || run.StartedAt == nil {
			return nil
		}
		var approved contentResetExecutionContract
		if json.Unmarshal(run.Contract, &approved) != nil || len(approved.Owners) == 0 {
			return contentreset.ErrUnavailable
		}
		for _, owner := range approved.Owners {
			if !contentResetEffectAdmission(owner, run.Contract) {
				return nil
			}
		}
		if err := contentreset.ValidateExecutionEnvironment(tx, run); err != nil {
			return err
		}
		var workflow models.ContentResetWorkflow
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND campaign_id=?", tenant, campaignID).First(&workflow).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		var revision models.ContentResetRevision
		if err := tx.Where("tenant_id=? AND campaign_id=? AND id=? AND revision=?", tenant, campaignID, run.RevisionID, campaign.CurrentRevision).First(&revision).Error; err != nil {
			return err
		}
		if err := validateContentResetWorkflow(workflow, campaign, revision, workflow.PreparedCommandID); err != nil {
			return err
		}
		var prepared models.ContentResetStep
		if err := tx.Where("tenant_id=? AND campaign_id=? AND revision_id=? AND public_id=? AND step_key='prepare'", tenant, campaignID, revision.ID, workflow.PreparedCommandID).First(&prepared).Error; err != nil {
			return err
		}
		if prepared.State != "succeeded" {
			return nil
		}
		var req contentResetPlanRequest
		if json.Unmarshal(revision.Request, &req) != nil {
			return errors.New("invalid workflow intent")
		}
		switch workflow.Phase {
		case "seed":
			return seedContentResetWorkflow(tx, engine, campaign, revision, req, &workflow)
		case "candidates":
			return enqueueContentResetFreshStartBuild(tx, engine, campaign, revision, req, &workflow)
		case "replay":
			return planContentResetReplay(tx, engine, campaign, revision, req, &workflow)
		case "awaiting_publication":
			return planContentResetPublicationWait(tx, engine, campaign, revision, req, &workflow, &run)
		case "publishing":
			return planContentResetPublishingWait(tx, campaign, &workflow, &run)
		case "cleanup_window":
			return planContentResetCleanupWindow(tx, engine, campaign, revision, req, &workflow, &run)
		case "retiring":
			return planContentResetRetiring(tx, engine, campaign, revision, req, &workflow, &run)
		case "serving":
			return planContentResetServingPhase(tx, engine, campaign, revision, req, &workflow, &run)
		case "storage":
			return planContentResetStoragePhase(tx, engine, campaign, revision, &workflow, &run)
		case "verifying":
			return planContentResetVerifyingPhase(tx, campaign, revision, &workflow, &run)
		case "complete", "rolled_back":
			return nil
		default:
			return errors.New("unknown Content Reset workflow phase")
		}
	})
}

func contentResetLanes(lane string) []string {
	if lane == "both" {
		return []string{"news", "pods"}
	}
	return []string{lane}
}

func contentResetSourceKey(id uuid.UUID) string { return "replay/source/" + id.String() }
func contentResetPageKey(id uuid.UUID, ordinal int) string {
	return fmt.Sprintf("replay/branch/%s/page/%d", id, ordinal)
}

// seedContentResetWorkflow is the first durable planning phase. Build-first
// Fresh Start starts its isolated replacement immediately; Clear, Empty and
// clear-first Fresh Start acquire their destructive authority first.
func seedContentResetWorkflow(tx *gorm.DB, engine *contentreset.Engine, campaign models.ContentResetCampaign, revision models.ContentResetRevision, req contentResetPlanRequest, workflow *models.ContentResetWorkflow) error {
	if req.Operation == "fresh_start" && req.CapacityStrategy == "build_first" {
		return enqueueContentResetFreshStartBuild(tx, engine, campaign, revision, req, workflow)
	}
	page := make([]contentreset.PlannedStep, 0, 30)
	pauseKeys := map[string]string{}
	if req.Operation == "empty" {
		// Pause commands are enqueued exactly once on the first pass so they do
		// not consume the bounded page budget on every later page.
		for _, lane := range contentResetLanes(req.Lane) {
			key := "pause/" + lane
			pauseKeys[lane] = key
			if workflow.TargetCursor == 0 {
				parameters, _ := json.Marshal(map[string]string{"lane": lane})
				page = append(page, contentreset.PlannedStep{Key: key, Contract: (contentResetPauseOwner{}).Contract(), TargetID: campaign.PublicID.String(), Parameters: parameters, Requires: []string{"prepare"}})
			}
		}
	}
	pairs, total, err := contentResetRetirementPairs(tx, campaign, revision, req)
	if err != nil {
		return err
	}
	// Resume from the durable cursor. Without this a large selection would
	// re-enqueue its first page forever and never reach retiring.
	cursor := int(workflow.TargetCursor)
	if cursor < 0 || cursor > len(pairs) {
		return errors.New("Content Reset retirement cursor is outside the frozen target set")
	}
	retirePage, next, err := contentResetRetirePage(pairs, cursor, 30-len(page))
	if err != nil {
		return err
	}
	purpose := "clear"
	for _, pair := range retirePage {
		requires := []string{"prepare"}
		if req.Operation == "empty" {
			requires = []string{pauseKeys[pair.Lane]}
		}
		page = append(page, contentResetRetirePlan(campaign, pair.Lane, pair.Ordinal, purpose, requires))
	}
	if err := engine.Enqueue(tx, campaign, revision, page); err != nil {
		return err
	}
	phase := "seed"
	if next == total {
		phase = "retiring"
	}
	runPhase := "retiring"
	if phase == "seed" {
		runPhase = "preparing"
	}
	if err := tx.Model(workflow).Updates(map[string]any{"target_cursor": int64(next), "phase": phase}).Error; err != nil {
		return err
	}
	return tx.Model(&models.ContentResetExecution{}).Where("tenant_id=? AND campaign_id=?", campaign.TenantID, campaign.ID).
		Updates(map[string]any{"phase": runPhase, "irreversible_at": time.Now().UTC()}).Error
}

// enqueueContentResetFreshStartBuild seeds the candidate view and every frozen
// replay branch. It is shared by build-first and by clear-first after its
// retirement phase completes.
func enqueueContentResetFreshStartBuild(tx *gorm.DB, engine *contentreset.Engine, campaign models.ContentResetCampaign, revision models.ContentResetRevision, req contentResetPlanRequest, workflow *models.ContentResetWorkflow) error {
	if req.Operation != "fresh_start" {
		return contentreset.ErrUnavailable
	}
	page := make([]contentreset.PlannedStep, 0, 30)
	for _, lane := range contentResetLanes(req.Lane) {
		if lane == "news" {
			page = append(page, contentreset.PlannedStep{Key: "candidate/news", Contract: (contentResetNewsCandidateOwner{}).Contract(), TargetID: campaign.PublicID.String(), Parameters: json.RawMessage(`{}`), Requires: []string{"prepare"}})
		} else {
			page = append(page, contentreset.PlannedStep{Key: "candidate/pods", Contract: (contentResetPodsCandidateOwner{}).Contract(), TargetID: campaign.PublicID.String(), Parameters: json.RawMessage(`{}`), Requires: []string{"prepare"}})
		}
	}
	var sources []struct {
		ID            uuid.UUID         `json:"id"`
		Category      string            `json:"category"`
		Type          models.SourceType `json:"type"`
		ConfigVersion int64             `json:"config_version"`
	}
	if json.Unmarshal(revision.ReplaySourceSnapshot, &sources) != nil || contentreset.Hash(sources) != revision.ReplaySourceHash || workflow.SourceCursor > len(sources) {
		return errors.New("workflow replay snapshot integrity failure")
	}
	for workflow.SourceCursor < len(sources) && len(page) < 30 {
		source := sources[workflow.SourceCursor]
		page = append(page, contentreset.PlannedStep{Key: contentResetSourceKey(source.ID), Contract: (contentResetReplayBranchOwner{}).Contract(), TargetID: source.ID.String(), Parameters: json.RawMessage(`{}`), Requires: []string{"prepare"}})
		workflow.SourceCursor++
	}
	if err := engine.Enqueue(tx, campaign, revision, page); err != nil {
		return err
	}
	phase := "seed"
	if workflow.SourceCursor == len(sources) {
		phase = "replay"
	}
	if err := tx.Model(workflow).Updates(map[string]any{"source_cursor": workflow.SourceCursor, "phase": phase}).Error; err != nil {
		return err
	}
	return tx.Model(&models.ContentResetExecution{}).Where("tenant_id=? AND campaign_id=?", campaign.TenantID, campaign.ID).Update("phase", "building").Error
}

type contentResetRetirementPair struct {
	Lane    string
	Ordinal int
}

// contentResetRetirePage slices the deterministic frozen batch list from the
// persisted cursor under a bounded page budget. Every pass must resume exactly
// where the previous durable page stopped.
func contentResetRetirePage(pairs []contentResetRetirementPair, cursor, budget int) ([]contentResetRetirementPair, int, error) {
	if cursor < 0 || cursor > len(pairs) {
		return nil, cursor, errors.New("Content Reset retirement cursor is outside the frozen target set")
	}
	if budget <= 0 {
		return nil, cursor, nil
	}
	end := cursor + budget
	if end > len(pairs) {
		end = len(pairs)
	}
	return pairs[cursor:end], end, nil
}

func contentResetRetirementPairs(tx *gorm.DB, campaign models.ContentResetCampaign, revision models.ContentResetRevision, req contentResetPlanRequest) ([]contentResetRetirementPair, int, error) {
	pairs := []contentResetRetirementPair{}
	for _, lane := range contentResetLanes(req.Lane) {
		var count int64
		if err := tx.Model(&models.ContentResetTarget{}).
			Where("tenant_id=? AND revision_id=? AND lane=? AND disposition='selected' AND protected=FALSE", campaign.TenantID, revision.ID, lane).
			Count(&count).Error; err != nil {
			return nil, 0, err
		}
		batches := int((count + contentResetRetireBatchSize - 1) / contentResetRetireBatchSize)
		for ordinal := 1; ordinal <= batches; ordinal++ {
			pairs = append(pairs, contentResetRetirementPair{Lane: lane, Ordinal: ordinal})
		}
	}
	return pairs, len(pairs), nil
}

func contentResetRetirePlan(campaign models.ContentResetCampaign, lane string, ordinal int, purpose string, requires []string) contentreset.PlannedStep {
	parameters, _ := json.Marshal(contentResetRetireParameters{Lane: lane, Ordinal: ordinal, Purpose: purpose})
	effect := "retire_exact"
	owner := "cms/" + lane
	return contentreset.PlannedStep{
		Key:        contentResetRetireKey(lane, ordinal),
		Contract:   contentreset.Contract{Owner: owner, Effect: effect, TargetType: "manifest_batch", Version: "v1"},
		TargetID:   campaign.PublicID.String(),
		Parameters: parameters,
		Requires:   requires,
	}
}

func contentResetPendingSteps(tx *gorm.DB, campaign models.ContentResetCampaign, revision models.ContentResetRevision, pattern string) (int64, error) {
	var pending int64
	err := tx.Model(&models.ContentResetStep{}).
		Where("tenant_id=? AND campaign_id=? AND revision_id=? AND state<>'succeeded' AND step_key LIKE ?", campaign.TenantID, campaign.ID, revision.ID, pattern).
		Count(&pending).Error
	return pending, err
}

func contentResetStepSucceeded(tx *gorm.DB, campaign models.ContentResetCampaign, revision models.ContentResetRevision, stepKey string) (bool, error) {
	var step models.ContentResetStep
	if err := tx.Where("tenant_id=? AND campaign_id=? AND revision_id=? AND step_key=?", campaign.TenantID, campaign.ID, revision.ID, stepKey).First(&step).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, err
	}
	return step.State == "succeeded", nil
}

// retirement is the shared wait/completion phase for initial clear, Empty and
// post-publication cleanup retirement. When the last batch succeeds it either
// starts the replacement build (clear-first) or the serving verification.
func planContentResetRetiring(tx *gorm.DB, engine *contentreset.Engine, campaign models.ContentResetCampaign, revision models.ContentResetRevision, req contentResetPlanRequest, workflow *models.ContentResetWorkflow, run *models.ContentResetExecution) error {
	pending, err := contentResetPendingSteps(tx, campaign, revision, "retire/%")
	if err != nil {
		return err
	}
	if pending != 0 {
		return nil
	}
	published := run != nil && run.PublishedAt != nil
	if req.Operation == "fresh_start" && req.CapacityStrategy == "clear_first" && !published {
		// Clear-first retirement finishes before the isolated replacement build.
		workflow.Phase = "candidates"
		return tx.Model(workflow).Update("phase", workflow.Phase).Error
	}
	// All operations verify that the selected scope is withdrawn before any
	// storage/cleanup accounting.
	page := make([]contentreset.PlannedStep, 0, 4)
	for _, lane := range contentResetLanes(req.Lane) {
		parameters, _ := json.Marshal(map[string]string{"lane": lane})
		page = append(page, contentreset.PlannedStep{
			Key: "verify/serving/" + lane, Contract: (contentResetServingOwner{lane: lane}).Contract(),
			TargetID: campaign.PublicID.String(), Parameters: parameters, Requires: []string{"prepare"},
		})
	}
	if err := engine.Enqueue(tx, campaign, revision, page); err != nil {
		return err
	}
	workflow.Phase = "serving"
	if err := tx.Model(workflow).Update("phase", workflow.Phase).Error; err != nil {
		return err
	}
	return tx.Model(&models.ContentResetExecution{}).Where("tenant_id=? AND campaign_id=?", campaign.TenantID, campaign.ID).Update("phase", "verifying").Error
}

func planContentResetPublicationWait(tx *gorm.DB, engine *contentreset.Engine, campaign models.ContentResetCampaign, revision models.ContentResetRevision, req contentResetPlanRequest, workflow *models.ContentResetWorkflow, run *models.ContentResetExecution) error {
	ready, err := contentResetStepSucceeded(tx, campaign, revision, "verify/readiness")
	if err != nil {
		return err
	}
	if !ready {
		return nil
	}
	milestone, err := activeContentResetMilestone(tx, campaign.TenantID, campaign.ID, revision.ID, "publication")
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if milestone.ManifestHash != *revision.ManifestHash {
		return errors.New("publication milestone manifest changed")
	}
	if err := engine.Enqueue(tx, campaign, revision, []contentreset.PlannedStep{{
		Key: "publish", Contract: (contentResetPublishOwner{}).Contract(), TargetID: campaign.PublicID.String(),
		Parameters: json.RawMessage(`{}`), Requires: []string{"verify/readiness"},
	}}); err != nil {
		return err
	}
	workflow.Phase = "publishing"
	if err := tx.Model(workflow).Update("phase", workflow.Phase).Error; err != nil {
		return err
	}
	return tx.Model(&models.ContentResetExecution{}).Where("tenant_id=? AND campaign_id=?", campaign.TenantID, campaign.ID).Update("phase", "publishing").Error
}

func planContentResetPublishingWait(tx *gorm.DB, campaign models.ContentResetCampaign, workflow *models.ContentResetWorkflow, run *models.ContentResetExecution) error {
	if run.RolledBackAt != nil {
		workflow.Phase = "rolled_back"
		return tx.Model(workflow).Update("phase", workflow.Phase).Error
	}
	// Do not overwrite an admitted rollback transition while its owner runs.
	if run.Phase == "rolling_back" {
		return nil
	}
	if run.PublishedAt == nil {
		return nil
	}
	workflow.Phase = "cleanup_window"
	if err := tx.Model(workflow).Update("phase", workflow.Phase).Error; err != nil {
		return err
	}
	return tx.Model(&models.ContentResetExecution{}).Where("tenant_id=? AND campaign_id=?", campaign.TenantID, campaign.ID).Update("phase", "published").Error
}

func planContentResetCleanupWindow(tx *gorm.DB, engine *contentreset.Engine, campaign models.ContentResetCampaign, revision models.ContentResetRevision, req contentResetPlanRequest, workflow *models.ContentResetWorkflow, run *models.ContentResetExecution) error {
	if run.RolledBackAt != nil {
		workflow.Phase = "rolled_back"
		return tx.Model(workflow).Update("phase", workflow.Phase).Error
	}
	if run.PublishedAt == nil {
		return errors.New("cleanup window requires a published replacement")
	}
	// Do not overwrite an admitted rollback transition while its owner runs.
	if run.Phase == "rolling_back" {
		return nil
	}
	now := time.Now().UTC()
	if run.CleanupNotBefore != nil && now.Before(*run.CleanupNotBefore) {
		return tx.Model(&models.ContentResetExecution{}).Where("tenant_id=? AND campaign_id=?", campaign.TenantID, campaign.ID).Update("phase", "awaiting_cleanup").Error
	}
	if run.CleanupAuthorizedUntil == nil || !run.CleanupAuthorizedUntil.After(now) {
		return tx.Model(run).Updates(map[string]any{"phase": "cleanup_authorization_expired", "pause_requested": true, "version": gorm.Expr("version+1")}).Error
	}
	pairs, total, err := contentResetRetirementPairs(tx, campaign, revision, req)
	if err != nil {
		return err
	}
	purpose := "cleanup"
	if req.Operation == "fresh_start" && req.CapacityStrategy == "clear_first" {
		purpose = "cleanup"
	}
	page := make([]contentreset.PlannedStep, 0, 30)
	next := int(workflow.TargetCursor)
	for next < len(pairs) && len(page) < 30 {
		pair := pairs[next]
		page = append(page, contentResetRetirePlan(campaign, pair.Lane, pair.Ordinal, purpose, []string{"publish"}))
		next++
	}
	if len(page) > 0 {
		if err := engine.Enqueue(tx, campaign, revision, page); err != nil {
			return err
		}
	}
	phase := "cleanup_window"
	if next == total {
		phase = "retiring"
	}
	if err := tx.Model(workflow).Updates(map[string]any{"target_cursor": int64(next), "phase": phase}).Error; err != nil {
		return err
	}
	return tx.Model(&models.ContentResetExecution{}).Where("tenant_id=? AND campaign_id=?", campaign.TenantID, campaign.ID).Update("phase", "cleaning_up").Error
}

func planContentResetServingPhase(tx *gorm.DB, engine *contentreset.Engine, campaign models.ContentResetCampaign, revision models.ContentResetRevision, req contentResetPlanRequest, workflow *models.ContentResetWorkflow, run *models.ContentResetExecution) error {
	pending, err := contentResetPendingSteps(tx, campaign, revision, "verify/serving/%")
	if err != nil {
		return err
	}
	if pending != 0 {
		return nil
	}
	if err := engine.Enqueue(tx, campaign, revision, []contentreset.PlannedStep{{
		Key: "cleanup/storage", Contract: (contentResetCleanupOwner{}).Contract(), TargetID: campaign.PublicID.String(),
		Parameters: json.RawMessage(`{}`), Requires: []string{"prepare"},
	}}); err != nil {
		return err
	}
	workflow.Phase = "storage"
	return tx.Model(workflow).Update("phase", workflow.Phase).Error
}

func planContentResetStoragePhase(tx *gorm.DB, engine *contentreset.Engine, campaign models.ContentResetCampaign, revision models.ContentResetRevision, workflow *models.ContentResetWorkflow, run *models.ContentResetExecution) error {
	pending, err := contentResetPendingSteps(tx, campaign, revision, "cleanup/storage")
	if err != nil {
		return err
	}
	if pending != 0 {
		return nil
	}
	if err := engine.Enqueue(tx, campaign, revision, []contentreset.PlannedStep{{
		Key: "verify/complete", Contract: (contentResetCompleteOwner{}).Contract(), TargetID: campaign.PublicID.String(),
		Parameters: json.RawMessage(`{}`), Requires: []string{"prepare"},
	}}); err != nil {
		return err
	}
	workflow.Phase = "verifying"
	return tx.Model(workflow).Update("phase", workflow.Phase).Error
}

func planContentResetVerifyingPhase(tx *gorm.DB, campaign models.ContentResetCampaign, revision models.ContentResetRevision, workflow *models.ContentResetWorkflow, run *models.ContentResetExecution) error {
	done, err := contentResetStepSucceeded(tx, campaign, revision, "verify/complete")
	if err != nil || !done {
		return err
	}
	workflow.Phase = "complete"
	return tx.Model(workflow).Update("phase", workflow.Phase).Error
}

// ---- Replay planning (unchanged ownership; page/handoff/release chain) ----

func planContentResetReplay(tx *gorm.DB, engine *contentreset.Engine, campaign models.ContentResetCampaign, revision models.ContentResetRevision, req contentResetPlanRequest, workflow *models.ContentResetWorkflow) error {
	ready, err := contentResetStepSucceeded(tx, campaign, revision, "verify/readiness")
	if err != nil {
		return err
	}
	if ready {
		workflow.Phase = "awaiting_publication"
		if err := tx.Model(workflow).Update("phase", workflow.Phase).Error; err != nil {
			return err
		}
		return tx.Model(&models.ContentResetExecution{}).Where("tenant_id=? AND campaign_id=?", campaign.TenantID, campaign.ID).Update("phase", "awaiting_publication").Error
	}
	var branches []models.ContentResetReplayBranch
	if err := tx.Where("tenant_id=? AND campaign_id=? AND revision_id=? AND id>?", campaign.TenantID, campaign.ID, revision.ID, workflow.BranchCursor).Order("id ASC").Limit(8).Find(&branches).Error; err != nil {
		return err
	}
	if len(branches) == 0 {
		// A sweep boundary is also a chance to advance readiness. Source seeds
		// still pending cannot be mistaken for an empty, successful replay.
		if err := tx.Model(workflow).Update("branch_cursor", 0).Error; err != nil {
			return err
		}
		return planContentResetReplayReadiness(tx, engine, campaign, revision, req)
	}
	page := make([]contentreset.PlannedStep, 0, 16)
	for _, branch := range branches {
		workflow.BranchCursor = branch.ID
		var spec supply.ReplaySpec
		if branch.ManifestHash != *revision.ManifestHash || json.Unmarshal(branch.Spec, &spec) != nil || spec.Validate() != nil || contentreset.Hash(spec) != branch.SpecHash {
			return errors.New("workflow branch integrity failure")
		}
		var last models.ContentResetReplayPage
		err := tx.Where("tenant_id=? AND branch_id=?", campaign.TenantID, branch.PublicID).Order("ordinal DESC").First(&last).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			requires := []string{contentResetSourceKey(branch.ContentSourceID)}
			for _, lane := range contentResetLanes(req.Lane) {
				requires = append(requires, "candidate/"+lane)
			}
			page = append(page, replayPagePlan(branch.PublicID, 1, requires))
			continue
		}
		if err != nil {
			return err
		}
		var checkpoint models.ContentResetReplayCheckpoint
		err = tx.Where("tenant_id=? AND page_id=?", campaign.TenantID, last.PublicID).First(&checkpoint).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		key := contentResetPageKey(branch.PublicID, last.Ordinal)
		parameters, _ := json.Marshal(map[string]uuid.UUID{"page_id": last.PublicID})
		releaseKey := key + "/release"
		page = append(page, contentreset.PlannedStep{Key: releaseKey, Contract: (contentResetProviderReleaseOwner{}).Contract(), TargetID: branch.PublicID.String(), Parameters: parameters, Requires: []string{key}})
		handoffKey := "replay/branch/" + branch.PublicID.String() + "/handoff"
		if checkpoint.Exhausted {
			page = append(page, contentreset.PlannedStep{Key: handoffKey, Contract: (contentResetHandoffOwner{}).Contract(), TargetID: branch.PublicID.String(), Parameters: json.RawMessage(`{}`), Requires: []string{releaseKey}})
			continue
		}
		if last.Ordinal >= spec.MaxPages {
			var release models.ContentResetStep
			err := tx.Where("tenant_id=? AND campaign_id=? AND revision_id=? AND step_key=?", campaign.TenantID, campaign.ID, revision.ID, releaseKey).First(&release).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if release.State != "succeeded" {
				continue
			}
			// Exhaustion is provider proof; reaching our safety budget is a
			// blocker and does not silently convert available-history to recent.
			if err := tx.Model(&models.ContentResetExecution{}).Where("tenant_id=? AND campaign_id=?", campaign.TenantID, campaign.ID).Updates(map[string]any{"phase": "replay_budget_exhausted", "pause_requested": true, "version": gorm.Expr("version+1")}).Error; err != nil {
				return err
			}
			return tx.Model(&campaign).Update("state", "partial").Error
		}
		page = append(page, replayPagePlan(branch.PublicID, last.Ordinal+1, []string{releaseKey}))
	}
	if len(page) > 0 {
		if err := engine.Enqueue(tx, campaign, revision, page); err != nil {
			return err
		}
	}
	return tx.Model(workflow).Update("branch_cursor", workflow.BranchCursor).Error
}

func replayPagePlan(branch uuid.UUID, ordinal int, requires []string) contentreset.PlannedStep {
	parameters, _ := json.Marshal(map[string]int{"ordinal": ordinal})
	return contentreset.PlannedStep{Key: contentResetPageKey(branch, ordinal), Contract: (contentResetReplayPageOwner{}).Contract(), TargetID: branch.String(), Parameters: parameters, Requires: requires}
}

func planContentResetReplayReadiness(tx *gorm.DB, engine *contentreset.Engine, campaign models.ContentResetCampaign, revision models.ContentResetRevision, req contentResetPlanRequest) error {
	var pending int64
	if err := tx.Model(&models.ContentResetStep{}).Where("tenant_id=? AND campaign_id=? AND revision_id=? AND (step_key LIKE 'replay/source/%' OR step_key LIKE 'candidate/%') AND state<>'succeeded'", campaign.TenantID, campaign.ID, revision.ID).Count(&pending).Error; err != nil {
		return err
	}
	if pending != 0 {
		return nil
	}
	var incomplete int64
	if err := tx.Raw(`SELECT COUNT(*) FROM content_reset_replay_branches b
        WHERE b.tenant_id=? AND b.campaign_id=? AND b.revision_id=? AND NOT EXISTS (
          SELECT 1 FROM content_reset_replay_pages p JOIN content_reset_replay_checkpoints c ON c.tenant_id=p.tenant_id AND c.page_id=p.public_id
          JOIN content_reset_steps s ON s.tenant_id=p.tenant_id AND s.campaign_id=b.campaign_id AND s.revision_id=b.revision_id
            AND s.step_key=('replay/branch/' || b.public_id::text || '/handoff') AND s.state='succeeded'
          WHERE p.tenant_id=b.tenant_id AND p.branch_id=b.public_id AND c.exhausted
        )`, campaign.TenantID, campaign.ID, revision.ID).Scan(&incomplete).Error; err != nil {
		return err
	}
	if incomplete != 0 {
		return nil
	}
	var branches int64
	if err := tx.Model(&models.ContentResetReplayBranch{}).Where("tenant_id=? AND campaign_id=? AND revision_id=?", campaign.TenantID, campaign.ID, revision.ID).Count(&branches).Error; err != nil {
		return err
	}
	var sources []json.RawMessage
	if json.Unmarshal(revision.ReplaySourceSnapshot, &sources) != nil || branches != int64(len(sources)) {
		return errors.New("Fresh Start branch coverage does not match frozen sources")
	}
	if branches == 0 {
		return errors.New("Fresh Start replay has no frozen source branches")
	}
	// Handoffs are checked over the complete scope, rather than collecting at
	// most 30 sources and silently omitting the remainder. This owner must
	// independently read every handoff and revalidate both candidate lanes.
	requires := []string{}
	for _, lane := range contentResetLanes(req.Lane) {
		requires = append(requires, "candidate/"+lane)
	}
	return engine.Enqueue(tx, campaign, revision, []contentreset.PlannedStep{{
		Key: "verify/readiness", Contract: (contentResetReadinessOwner{}).Contract(), TargetID: campaign.PublicID.String(),
		Parameters: json.RawMessage(`{}`), Requires: requires,
	}})
}
