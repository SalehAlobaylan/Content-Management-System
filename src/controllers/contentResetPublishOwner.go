package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"content-management-system/src/contentreset"
	"content-management-system/src/feedstate"
	"content-management-system/src/models"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type contentResetPublicationPayload struct {
	Fences                 []feedstate.ResetPublicationFence `json:"fences"`
	PublishedAt            *time.Time                        `json:"published_at,omitempty"`
	RecoveryDeadline       time.Time                         `json:"recovery_deadline"`
	CleanupAuthorizedUntil time.Time                         `json:"cleanup_authorized_until"`
	RetainOldView          bool                              `json:"retain_old_view"`
	ReadinessHash          string                            `json:"readiness_hash,omitempty"`
}

type contentResetRollbackPayload struct {
	Fences           []feedstate.ResetRollbackFence `json:"fences"`
	RollbackDeadline time.Time                      `json:"rollback_deadline"`
}

func activeContentResetMilestone(tx *gorm.DB, tenant string, campaignID, revisionID uint, kind string) (models.ContentResetMilestone, error) {
	var milestone models.ContentResetMilestone
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("tenant_id=? AND campaign_id=? AND revision_id=? AND kind=? AND state='active'", tenant, campaignID, revisionID, kind).
		First(&milestone).Error
	return milestone, err
}

// ---- Publication owner ----

type contentResetPublishOwner struct{}

func (contentResetPublishOwner) Contract() contentreset.Contract {
	return contentreset.Contract{Owner: "cms/feedstate", Effect: "publish", TargetType: "campaign", Version: "v1"}
}

func (owner contentResetPublishOwner) Execute(ctx context.Context, db *gorm.DB, command contentreset.Command, token uuid.UUID) (contentreset.Observation, error) {
	var observation contentreset.Observation
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		campaign, revision, lockErr := contentreset.LockOwnerCommand(tx, command, token)
		if lockErr != nil {
			return lockErr
		}
		milestone, err := activeContentResetMilestone(tx, command.TenantID, campaign.ID, revision.ID, "publication")
		if errors.Is(err, gorm.ErrRecordNotFound) {
			observation = resetObservation(command, "failed", "publication_approval_missing", map[string]any{"published": false})
			observation.NoEffectProven = true
			return nil
		}
		if err != nil {
			return err
		}
		var payload contentResetPublicationPayload
		if json.Unmarshal(milestone.Payload, &payload) != nil || contentreset.Hash(payload) != milestone.PayloadHash {
			return errors.New("publication approval payload integrity failure")
		}
		if milestone.ManifestHash != command.ManifestHash || len(payload.Fences) == 0 || payload.CleanupAuthorizedUntil.IsZero() ||
			(payload.RetainOldView && payload.RecoveryDeadline.IsZero()) {
			return errors.New("publication approval binding changed")
		}
		if !milestone.ExpiresAt.After(time.Now().UTC()) {
			evidence, _ := json.Marshal(map[string]any{"reason": "publication_approval_expired"})
			observation = contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "blocked", ReasonCode: "publication_approval_expired", Evidence: evidence}
			return nil
		}
		readiness, readinessErr := verifyContentResetReplacementReadiness(tx, campaign, revision, command)
		if readinessErr != nil {
			return readinessErr
		}
		if readiness.State != "succeeded" {
			// No publication effect is admitted yet. Deferred (not waiting)
			// keeps this effect-bearing step eligible for a later Execute pass
			// once the replacement becomes ready.
			observation = contentreset.Observation{
				CommandID: command.CommandID, CommandHash: contentreset.Hash(command),
				State: "deferred", ReasonCode: readiness.ReasonCode, Evidence: readiness.Evidence, NoEffectProven: true,
			}
			return nil
		}
		if payload.ReadinessHash != "" && payload.ReadinessHash != contentResetReadinessHash(readiness) {
			evidence, _ := json.Marshal(map[string]any{"reason": "readiness_changed_since_approval"})
			observation = contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "blocked", ReasonCode: "publication_readiness_changed", Evidence: evidence}
			return nil
		}
		if err := validatePublicationFencesLive(tx, command.TenantID, campaign, payload.Fences); err != nil {
			evidence, _ := json.Marshal(map[string]any{"reason": err.Error()})
			observation = contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "blocked", ReasonCode: "publication_head_changed", Evidence: evidence}
			return nil
		}
		publishedAt := time.Now().UTC()
		promoted, err := feedstate.PublishContentResetViews(tx, command.TenantID, campaign.ID, revision.ID, payload.Fences, publishedAt, payload.RecoveryDeadline, payload.RetainOldView)
		if err != nil {
			return err
		}
		result := tx.Model(&models.ContentResetExecution{}).Where("tenant_id=? AND campaign_id=? AND revision_id=? AND phase='published'", command.TenantID, campaign.ID, revision.ID).
			Update("cleanup_authorized_until", payload.CleanupAuthorizedUntil)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return errors.New("publication execution cleanup authorization changed")
		}
		consumed := time.Now().UTC()
		result = tx.Model(&models.ContentResetMilestone{}).Where("id=? AND state='active'", milestone.ID).
			Updates(map[string]any{"state": "consumed", "consumed_at": consumed})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return errors.New("publication approval already consumed")
		}
		if err := appendContentResetEvidence(tx, campaign, &revision, "publication/"+milestone.PublicID.String(), "owner_publication", "cms/feedstate", map[string]any{
			"milestone_id": milestone.PublicID, "published_at": publishedAt, "promoted_instances": promoted,
			"fences": payload.Fences, "recovery_deadline": payload.RecoveryDeadline, "cleanup_authorized_until": payload.CleanupAuthorizedUntil,
		}); err != nil {
			return err
		}
		evidence, _ := json.Marshal(map[string]any{"published_at": publishedAt, "promoted_instances": promoted, "fences": payload.Fences, "published": true})
		observation = contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "succeeded", ReasonCode: "publication_complete", Evidence: evidence}
		return nil
	})
	return observation, err
}

func (owner contentResetPublishOwner) Reconcile(ctx context.Context, db *gorm.DB, command contentreset.Command) (contentreset.Observation, error) {
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
		if run.PublishedAt != nil {
			evidence, _ := json.Marshal(map[string]any{"published_at": *run.PublishedAt, "published": true})
			observation = contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "succeeded", ReasonCode: "publication_complete", Evidence: evidence}
			return nil
		}
		var milestone models.ContentResetMilestone
		err := tx.Where("tenant_id=? AND campaign_id=? AND revision_id=? AND kind='publication' ORDER BY id DESC", command.TenantID, campaign.ID, revision.ID).First(&milestone).Error
		if err == nil && milestone.State == "consumed" {
			observation = resetObservation(command, "outcome_unknown", "publication_consumed_without_execution", map[string]any{"published": false})
			return nil
		}
		readiness, readinessErr := verifyContentResetReplacementReadiness(tx, campaign, revision, command)
		if readinessErr != nil {
			return readinessErr
		}
		if readiness.State != "succeeded" {
			observation = readiness
			return nil
		}
		observation = resetObservation(command, "failed", "publication_not_committed", map[string]any{"published": false})
		observation.NoEffectProven = true
		return nil
	})
	return observation, err
}

func validatePublicationFencesLive(tx *gorm.DB, tenant string, campaign models.ContentResetCampaign, fences []feedstate.ResetPublicationFence) error {
	if err := feedstate.ValidateResetPublicationFences(campaign.Lane, fences); err != nil {
		return err
	}
	for _, fence := range fences {
		var head models.FeedGenerationHead
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND lane=?", tenant, fence.Lane).First(&head).Error; err != nil {
			return err
		}
		if head.Generation != fence.HeadVersion || head.ActiveGenerationID == nil || *head.ActiveGenerationID != fence.ActiveID || head.CandidateGenerationID == nil || *head.CandidateGenerationID != fence.CandidateID {
			return errors.New("publication feed head changed")
		}
	}
	return nil
}

// ---- Rollback owner ----

type contentResetRollbackOwner struct{}

func (contentResetRollbackOwner) Contract() contentreset.Contract {
	return contentreset.Contract{Owner: "cms/feedstate", Effect: "rollback", TargetType: "campaign", Version: "v1"}
}

func (owner contentResetRollbackOwner) Execute(ctx context.Context, db *gorm.DB, command contentreset.Command, token uuid.UUID) (contentreset.Observation, error) {
	var observation contentreset.Observation
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		campaign, revision, lockErr := contentreset.LockOwnerCommand(tx, command, token)
		if lockErr != nil {
			return lockErr
		}
		milestone, err := activeContentResetMilestone(tx, command.TenantID, campaign.ID, revision.ID, "rollback")
		if errors.Is(err, gorm.ErrRecordNotFound) {
			observation = resetObservation(command, "failed", "rollback_approval_missing", map[string]any{"rolled_back": false})
			observation.NoEffectProven = true
			return nil
		}
		if err != nil {
			return err
		}
		var payload contentResetRollbackPayload
		if json.Unmarshal(milestone.Payload, &payload) != nil || contentreset.Hash(payload) != milestone.PayloadHash {
			return errors.New("rollback approval payload integrity failure")
		}
		if milestone.ManifestHash != command.ManifestHash || len(payload.Fences) == 0 || payload.RollbackDeadline.IsZero() {
			return errors.New("rollback approval binding changed")
		}
		if !milestone.ExpiresAt.After(time.Now().UTC()) {
			evidence, _ := json.Marshal(map[string]any{"reason": "rollback_approval_expired"})
			observation = contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "blocked", ReasonCode: "rollback_approval_expired", Evidence: evidence}
			return nil
		}
		demoted, err := feedstate.RollbackContentResetViews(tx, command.TenantID, campaign.ID, revision.ID, payload.Fences, time.Now().UTC())
		if err != nil {
			return err
		}
		consumedAt := time.Now().UTC()
		result := tx.Model(&models.ContentResetMilestone{}).Where("id=? AND state='active'", milestone.ID).
			Updates(map[string]any{"state": "consumed", "consumed_at": consumedAt})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return errors.New("rollback approval already consumed")
		}
		var residualStaged int64
		if err := tx.Model(&models.SourceItemInstance{}).Where("tenant_id=? AND campaign_id=? AND state='staged'", command.TenantID, campaign.ID).Count(&residualStaged).Error; err != nil {
			return err
		}
		if err := releaseContentResetLaneClaims(tx, campaign); err != nil {
			return err
		}
		if err := appendContentResetEvidence(tx, campaign, &revision, "rollback/"+milestone.PublicID.String(), "owner_rollback", "cms/feedstate", map[string]any{
			"milestone_id": milestone.PublicID, "rolled_back_at": consumedAt, "demoted_instances": demoted,
			"residual_staged_instances": residualStaged, "fences": payload.Fences,
		}); err != nil {
			return err
		}
		evidence, _ := json.Marshal(map[string]any{"rolled_back_at": consumedAt, "demoted_instances": demoted, "residual_staged_instances": residualStaged, "rolled_back": true})
		observation = contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "succeeded", ReasonCode: "rollback_complete", Evidence: evidence}
		return nil
	})
	return observation, err
}

func (owner contentResetRollbackOwner) Reconcile(ctx context.Context, db *gorm.DB, command contentreset.Command) (contentreset.Observation, error) {
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
		if run.RolledBackAt != nil {
			evidence, _ := json.Marshal(map[string]any{"rolled_back_at": *run.RolledBackAt, "rolled_back": true})
			observation = contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "succeeded", ReasonCode: "rollback_complete", Evidence: evidence}
			return nil
		}
		observation = resetObservation(command, "failed", "rollback_not_committed", map[string]any{"rolled_back": false})
		observation.NoEffectProven = true
		return nil
	})
	return observation, err
}
