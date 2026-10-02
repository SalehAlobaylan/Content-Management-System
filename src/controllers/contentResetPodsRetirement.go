package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"content-management-system/src/contentreset"
	"content-management-system/src/models"
	"content-management-system/src/podsreset"
	"content-management-system/src/utils"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// errDelegationMissing aborts a read-only validation transaction so target
// inventory can run outside the database transaction.
var errDelegationMissing = errors.New("delegated Pods retirement batch is not materialized")

type contentResetPodsRetireOwner struct{}

func (contentResetPodsRetireOwner) Contract() contentreset.Contract {
	return contentreset.Contract{Owner: "cms/pods", Effect: "retire_exact", TargetType: "manifest_batch", Version: "v1"}
}

func (owner contentResetPodsRetireOwner) Execute(ctx context.Context, db *gorm.DB, command contentreset.Command, token uuid.UUID) (contentreset.Observation, error) {
	parameters, err := parseContentResetRetireParameters(command, owner.Contract().Effect)
	if err != nil || parameters.Lane != "pods" {
		return contentreset.Observation{}, errors.New("invalid Pods retirement command")
	}
	// The campaign-bound Pods reset executor is the Plan 119 state machine. It
	// must not be replaced by a second implementation, and it must not run when
	// Plan 119's own qualification gate is closed.
	if err := requireRetentionCapability(db.WithContext(ctx), command.TenantID, retentionCapabilityPodsReset); err != nil {
		evidence, _ := json.Marshal(map[string]any{"lane": "pods", "ordinal": parameters.Ordinal, "reason": "plan_119_unqualified"})
		return contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "blocked", ReasonCode: "pods_reset_unqualified", Evidence: evidence}, nil
	}
	var batchTargetIDs []uuid.UUID
	var run models.PodsResetRun
	// Phase 1: verify the campaign fence and frozen target set without network.
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		campaign, revision, lockErr := contentreset.LockOwnerCommand(tx, command, token)
		if lockErr != nil {
			return lockErr
		}
		batch, batchErr := loadContentResetRetireBatch(tx, campaign, revision, parameters)
		if batchErr != nil {
			return batchErr
		}
		for _, target := range batch.Targets {
			batchTargetIDs = append(batchTargetIDs, target.ContentItemID)
		}
		existing, findErr := findDelegatedPodsResetRun(tx, campaign.TenantID, campaign.ID, revision.ID, "pods", parameters.Ordinal)
		if findErr == nil {
			run = existing
			return nil
		}
		if !errors.Is(findErr, gorm.ErrRecordNotFound) {
			return findErr
		}
		return errDelegationMissing
	})
	if errors.Is(err, errContentResetTargetDrift) || errors.Is(err, errContentResetProtectionDrift) {
		evidence, _ := json.Marshal(map[string]any{"lane": "pods", "ordinal": parameters.Ordinal, "drift": err.Error()})
		return contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "blocked", ReasonCode: "target_protection_changed", Evidence: evidence}, nil
	}
	if err != nil && !errors.Is(err, errDelegationMissing) {
		return contentreset.Observation{}, err
	}
	if errors.Is(err, errDelegationMissing) {
		targets, _, _, buildErr := buildPodsResetPlanTargets(db.WithContext(ctx), command.TenantID, batchTargetIDs)
		if buildErr != nil {
			return contentreset.Observation{}, buildErr
		}
		blockers := []map[string]string{}
		for _, target := range targets {
			for _, blocker := range target.Blockers {
				blockers = append(blockers, map[string]string{"code": blocker.Code, "message": blocker.Message, "content_item_id": target.ID.String()})
			}
		}
		if len(blockers) > 0 {
			evidence, _ := json.Marshal(map[string]any{"lane": "pods", "ordinal": parameters.Ordinal, "blockers": blockers})
			return contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "blocked", ReasonCode: "pods_retirement_targets_blocked", Evidence: evidence}, nil
		}
		// Phase 2: persist the immutable delegated run under the campaign lock.
		err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			campaign, revision, lockErr := contentreset.LockOwnerCommand(tx, command, token)
			if lockErr != nil {
				return lockErr
			}
			existing, findErr := findDelegatedPodsResetRun(tx, campaign.TenantID, campaign.ID, revision.ID, "pods", parameters.Ordinal)
			if findErr == nil {
				run = existing
				return nil
			}
			if !errors.Is(findErr, gorm.ErrRecordNotFound) {
				return findErr
			}
			created, createErr := createDelegatedPodsResetRun(tx, campaign, revision, parameters, targets)
			if createErr != nil {
				return createErr
			}
			run = created
			return nil
		})
		if err != nil {
			return contentreset.Observation{}, err
		}
	}
	if run.PublicID == uuid.Nil {
		return contentreset.Observation{}, errors.New("delegated Pods retirement run is unavailable")
	}
	principal := utils.AdminPrincipal{
		UserID: "cms/content-reset", Email: "content-reset+" + command.CampaignID.String() + "@cms.internal",
		TenantID: command.TenantID, TenantClaimed: true, Role: "admin",
	}
	outcome, execErr := executePodsResetRunInProcess(db.WithContext(ctx), principal, run)
	if execErr != nil {
		var classified *podsResetExecutionError
		if errors.As(execErr, &classified) {
			switch classified.Code {
			case "RESET_DISABLED", "OPERATION_CONFLICT", "CAMPAIGN_INACTIVE":
				evidence, _ := json.Marshal(map[string]any{"lane": "pods", "ordinal": parameters.Ordinal, "reason": classified.Message, "code": classified.Code})
				return contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "blocked", ReasonCode: "pods_retirement_blocked", Evidence: evidence}, nil
			case "RESET_PAUSED":
				return resetObservation(command, "waiting", "pods_retirement_campaign_paused", map[string]any{"run_id": run.PublicID, "lane": "pods", "ordinal": parameters.Ordinal}), nil
			case "RESET_EXECUTOR_BUSY":
				return resetObservation(command, "waiting", "pods_retirement_executor_busy", map[string]any{"run_id": run.PublicID}), nil
			}
		}
		return contentreset.Observation{}, execErr
	}
	evidence, _ := json.Marshal(map[string]any{"run_id": run.PublicID, "state": outcome.State, "phase": outcome.Phase, "lane": "pods", "ordinal": parameters.Ordinal, "items": len(outcome.Items), "totals": outcome.Totals})
	switch outcome.State {
	case "complete":
		return contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "succeeded", ReasonCode: "pods_retirement_batch_complete", Evidence: evidence}, nil
	case "cancelled":
		return contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "failed", ReasonCode: "pods_retirement_cancelled_before_effect", Evidence: evidence, NoEffectProven: true}, nil
	default:
		return contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "waiting", ReasonCode: "pods_retirement_pending", Evidence: evidence}, nil
	}
}

func (owner contentResetPodsRetireOwner) Reconcile(ctx context.Context, db *gorm.DB, command contentreset.Command) (contentreset.Observation, error) {
	parameters, err := parseContentResetRetireParameters(command, owner.Contract().Effect)
	if err != nil || parameters.Lane != "pods" {
		return contentreset.Observation{}, errors.New("invalid Pods retirement readback")
	}
	var observation contentreset.Observation
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		campaign, revision, err := lockContentResetReadback(tx, command)
		if err != nil {
			return err
		}
		run, err := findDelegatedPodsResetRun(tx, campaign.TenantID, campaign.ID, revision.ID, "pods", parameters.Ordinal)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			observation = resetObservation(command, "failed", "pods_retirement_not_committed", map[string]any{"run_created": false, "no_effect_proven": true})
			observation.NoEffectProven = true
			return nil
		}
		if err != nil {
			return err
		}
		var items []models.PodsResetItem
		if err := tx.Where("run_id=?", run.PublicID).Find(&items).Error; err != nil {
			return err
		}
		complete := 0
		pending := 0
		for _, item := range items {
			switch item.State {
			case "complete":
				complete++
			case "blocked":
				pending++
			default:
				pending++
			}
		}
		evidence, _ := json.Marshal(map[string]any{"run_id": run.PublicID, "state": run.State, "phase": run.Phase, "items_complete": complete, "items_pending": pending, "lane": "pods", "ordinal": parameters.Ordinal})
		switch run.State {
		case "complete":
			observation = contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "succeeded", ReasonCode: "pods_retirement_batch_complete", Evidence: evidence}
		case "cancelled":
			observation = contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "failed", ReasonCode: "pods_retirement_cancelled_before_effect", Evidence: evidence}
			observation.NoEffectProven = complete == 0
		default:
			observation = contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "waiting", ReasonCode: "pods_retirement_pending", Evidence: evidence}
		}
		return nil
	})
	return observation, err
}

// delegatedPodsCampaignAdmission is the parent-authority fence for a delegated
// Pods retirement run. It is called at the executor entry and at every item
// boundary so a campaign pause or terminal transition committed while a pass is
// running stops admission of the next destructive item. `terminal` distinguishes
// an authoritative refusal that cannot resolve on its own from a pause.
func delegatedPodsCampaignAdmission(db *gorm.DB, run models.PodsResetRun) (open bool, reason string, terminal bool, err error) {
	if run.ContentResetCampaignID == nil {
		return true, "", false, nil
	}
	var campaign models.ContentResetCampaign
	if err := db.Where("tenant_id=? AND id=?", run.TenantID, *run.ContentResetCampaignID).First(&campaign).Error; err != nil {
		return false, "campaign_unavailable", true, err
	}
	switch campaign.State {
	case "executing", "published", "cleanup_pending", "partial":
	default:
		return false, "campaign_inactive", true, nil
	}
	var execution models.ContentResetExecution
	if err := db.Where("tenant_id=? AND campaign_id=?", run.TenantID, campaign.ID).First(&execution).Error; err != nil {
		return false, "campaign_execution_unavailable", true, err
	}
	if execution.PauseRequested {
		return false, "campaign_paused", false, nil
	}
	if execution.CompletedAt != nil || execution.RolledBackAt != nil {
		return false, "campaign_terminal", true, nil
	}
	return true, "", false, nil
}

// delegatedPodsCampaignAdmissionLocked is the same parent fence under the
// campaign execution row lock, for use inside a destructive owner transaction.
func delegatedPodsCampaignAdmissionLocked(tx *gorm.DB, run models.PodsResetRun) error {
	if run.ContentResetCampaignID == nil {
		return nil
	}
	var execution models.ContentResetExecution
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND campaign_id=?", run.TenantID, *run.ContentResetCampaignID).First(&execution).Error; err != nil {
		return fmt.Errorf("delegating Content Reset campaign is unavailable: %w", err)
	}
	if execution.PauseRequested {
		return errors.New("delegating Content Reset campaign is paused")
	}
	if execution.CompletedAt != nil || execution.RolledBackAt != nil {
		return errors.New("delegating Content Reset campaign is terminal")
	}
	var campaign models.ContentResetCampaign
	if err := tx.Where("tenant_id=? AND id=?", run.TenantID, *run.ContentResetCampaignID).First(&campaign).Error; err != nil {
		return fmt.Errorf("delegating Content Reset campaign is unavailable: %w", err)
	}
	switch campaign.State {
	case "executing", "published", "cleanup_pending", "partial":
		return nil
	default:
		return errors.New("delegating Content Reset campaign is not active")
	}
}

func findDelegatedPodsResetRun(tx *gorm.DB, tenant string, campaignID, revisionID uint, lane string, batch int) (models.PodsResetRun, error) {
	var run models.PodsResetRun
	err := tx.Where("tenant_id=? AND content_reset_campaign_id=? AND content_reset_revision_id=? AND content_reset_lane=? AND content_reset_batch=?",
		tenant, campaignID, revisionID, lane, batch).First(&run).Error
	return run, err
}

func createDelegatedPodsResetRun(tx *gorm.DB, campaign models.ContentResetCampaign, revision models.ContentResetRevision, parameters contentResetRetireParameters, targets []podsResetPlanTarget) (models.PodsResetRun, error) {
	if revision.ManifestHash == nil {
		return models.PodsResetRun{}, errors.New("delegated Pods retirement requires a frozen manifest")
	}
	schemaFingerprint, err := podsResetSchemaFingerprint(tx)
	if err != nil {
		return models.PodsResetRun{}, err
	}
	environment, err := podsResetEnvironment(tx)
	if err != nil {
		return models.PodsResetRun{}, err
	}
	totalObjects := 0
	var totalBytes int64
	for _, target := range targets {
		totalObjects += len(target.Objects)
		for _, object := range target.Objects {
			totalBytes += object.SizeBytes
		}
	}
	if totalObjects > podsreset.MaxRunObjects || totalBytes > podsreset.MaxRunBytes {
		return models.PodsResetRun{}, errors.New("delegated Pods retirement batch exceeds the Plan 119 run budget")
	}
	ids := make([]uuid.UUID, 0, len(targets))
	for _, target := range targets {
		ids = append(ids, target.ID)
	}
	reason := "Content Reset " + campaign.Operation + " " + parameters.Lane + " retirement batch " + fmt.Sprint(parameters.Ordinal)
	manifest := map[string]any{
		"policy_version": podsreset.PolicyVersion, "schema_fingerprint": schemaFingerprint, "tenant_id": campaign.TenantID, "environment_identity": environment,
		"relationship_dispositions": podsreset.RelationshipPolicies(),
		"requested_ids":             ids, "selection_reason": reason, "targets": targets,
		"counts":     map[string]any{"selected_ids": len(ids), "object_count": totalObjects, "object_bytes": totalBytes},
		"outcome":    "purge_only",
		"rollback":   "unavailable_after_retirement_fence",
		"delegation": map[string]any{"campaign_id": campaign.PublicID, "revision": revision.Revision, "lane": parameters.Lane, "batch": parameters.Ordinal, "purpose": parameters.Purpose},
	}
	rawManifest, err := json.Marshal(manifest)
	if err != nil {
		return models.PodsResetRun{}, err
	}
	manifestHash, err := podsResetManifestHash(rawManifest)
	if err != nil {
		return models.PodsResetRun{}, err
	}
	now := time.Now().UTC()
	delegatedBy := "cms/content-reset/" + campaign.PublicID.String()
	lane := parameters.Lane
	batch := parameters.Ordinal
	run := models.PodsResetRun{
		TenantID: campaign.TenantID, State: "approved", Phase: "approved",
		ManifestHash: manifestHash, SchemaFingerprint: schemaFingerprint, PolicyVersion: podsreset.PolicyVersion,
		Manifest: datatypes.JSON(rawManifest), CreatedBy: delegatedBy, ApprovedBy: delegatedBy, ApprovedAt: &now,
		ExpiresAt:              now.Add(30 * 24 * time.Hour),
		ContentResetCampaignID: &campaign.ID, ContentResetRevisionID: &revision.ID, ContentResetLane: &lane, ContentResetBatch: &batch,
	}
	if err := tx.Create(&run).Error; err != nil {
		return models.PodsResetRun{}, err
	}
	for ordinal, target := range targets {
		snapshotHash := ""
		if target.Snapshot.PublicID != "" {
			snapshotHash, err = podsreset.Hash(target.Snapshot)
			if err != nil {
				return models.PodsResetRun{}, err
			}
		}
		decision, _ := json.Marshal(target.Decisions)
		blockers, _ := json.Marshal(target.Blockers)
		resetItem := models.PodsResetItem{RunID: run.PublicID, TenantID: campaign.TenantID, ContentItemID: target.ID, Ordinal: ordinal, SnapshotHash: snapshotHash, Decision: datatypes.JSON(decision), State: "planned", BlockedReasons: datatypes.JSON(blockers)}
		if err := tx.Create(&resetItem).Error; err != nil {
			return models.PodsResetRun{}, err
		}
		for _, object := range target.Objects {
			row := models.PodsResetObject{RunID: run.PublicID, ContentItemID: target.ID, StorageTier: object.StorageTier, Bucket: object.Bucket, ObjectKey: object.ObjectKey, ETag: object.ETag, SizeBytes: object.SizeBytes, State: "planned"}
			if err := tx.Create(&row).Error; err != nil {
				return models.PodsResetRun{}, err
			}
		}
	}
	if err := appendContentResetEvidence(tx, campaign, &revision, "delegated-pods/"+fmt.Sprint(parameters.Ordinal), "delegated_owner", "cms/pods", map[string]any{
		"run_id": run.PublicID, "manifest_hash": manifestHash, "lane": parameters.Lane, "batch": parameters.Ordinal, "items": len(targets),
	}); err != nil {
		return models.PodsResetRun{}, err
	}
	return run, nil
}
