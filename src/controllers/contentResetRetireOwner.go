package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"content-management-system/src/contentreset"
	"content-management-system/src/feedstate"
	"content-management-system/src/models"
	"content-management-system/src/sourceidentity"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const contentResetRetireBatchSize = 30

// Drift sentinels classify a refusal before any destructive statement. They map
// to a durable blocked observation rather than owner uncertainty.
var (
	errContentResetTargetDrift     = errors.New("content reset target changed after approval")
	errContentResetProtectionDrift = errors.New("content reset target protection changed after approval")
)

// contentResetRetireParameters identify one bounded, deterministic page of the
// frozen selected target set. The owner never accepts an arbitrary item list.
type contentResetRetireParameters struct {
	Lane    string `json:"lane"`
	Ordinal int    `json:"ordinal"`
	Purpose string `json:"purpose"`
}

func parseContentResetRetireParameters(command contentreset.Command, effect string) (contentResetRetireParameters, error) {
	var parameters contentResetRetireParameters
	if json.Unmarshal(command.Parameters, &parameters) != nil || parameters.Ordinal < 1 ||
		(parameters.Lane != "news" && parameters.Lane != "pods") ||
		(parameters.Purpose != "clear" && parameters.Purpose != "cleanup") ||
		contentreset.Hash(command.Parameters) != contentreset.Hash(parameters) {
		return parameters, errors.New("invalid Content Reset retirement batch")
	}
	return parameters, nil
}

type contentResetRetireBatch struct {
	Targets []models.ContentResetTarget
	Items   []models.ContentItem
}

// loadContentResetRetireBatch returns the exact immutable page and verifies the
// current content still matches the frozen target snapshot and protection set.
// It does not lock the rows: read-only planning/combined checks use it, while a
// destructive owner must use loadAndLockContentResetRetireBatch so a protection
// committed by a writer that takes the content-row lock cannot slip between the
// check and the mutation.
func loadContentResetRetireBatch(tx *gorm.DB, campaign models.ContentResetCampaign, revision models.ContentResetRevision, parameters contentResetRetireParameters) (contentResetRetireBatch, error) {
	batch, err := loadContentResetRetireTargets(tx, campaign, revision, parameters)
	if err != nil {
		return batch, err
	}
	ids := contentResetTargetIDs(batch.Targets)
	if err := tx.Where("tenant_id=? AND public_id IN ?", campaign.TenantID, ids).Find(&batch.Items).Error; err != nil {
		return batch, err
	}
	if len(batch.Items) != len(batch.Targets) {
		return batch, errContentResetTargetDrift
	}
	if err := verifyContentResetRetireBatch(tx, campaign.TenantID, revision, batch); err != nil {
		return batch, err
	}
	return batch, nil
}

// loadAndLockContentResetRetireBatch acquires the deterministic content-row
// locks before re-reading and re-verifying the destructive snapshot and
// protections. Consumer interaction creation takes the same row lock, so a new
// interaction or typed update cannot commit after this check and before the
// retirement mutation in the same transaction.
func loadAndLockContentResetRetireBatch(tx *gorm.DB, campaign models.ContentResetCampaign, revision models.ContentResetRevision, parameters contentResetRetireParameters) (contentResetRetireBatch, error) {
	batch, err := loadContentResetRetireTargets(tx, campaign, revision, parameters)
	if err != nil {
		return batch, err
	}
	ids := contentResetTargetIDs(batch.Targets)
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("tenant_id=? AND public_id IN ?", campaign.TenantID, ids).
		Order("public_id ASC").Find(&batch.Items).Error; err != nil {
		return batch, err
	}
	if len(batch.Items) != len(batch.Targets) {
		return batch, errContentResetTargetDrift
	}
	if err := verifyContentResetRetireBatch(tx, campaign.TenantID, revision, batch); err != nil {
		return batch, err
	}
	return batch, nil
}

func loadContentResetRetireTargets(tx *gorm.DB, campaign models.ContentResetCampaign, revision models.ContentResetRevision, parameters contentResetRetireParameters) (contentResetRetireBatch, error) {
	batch := contentResetRetireBatch{}
	offset := (parameters.Ordinal - 1) * contentResetRetireBatchSize
	if err := tx.Where("tenant_id=? AND revision_id=? AND lane=? AND disposition='selected' AND protected=FALSE",
		campaign.TenantID, revision.ID, parameters.Lane).Order("item_ordinal ASC").Offset(offset).Limit(contentResetRetireBatchSize).Find(&batch.Targets).Error; err != nil {
		return batch, err
	}
	if len(batch.Targets) == 0 {
		return batch, errors.New("Content Reset retirement batch is outside the frozen target set")
	}
	return batch, nil
}

func contentResetTargetIDs(targets []models.ContentResetTarget) []uuid.UUID {
	ids := make([]uuid.UUID, 0, len(targets))
	for _, target := range targets {
		ids = append(ids, target.ContentItemID)
	}
	return ids
}

func verifyContentResetRetireBatch(tx *gorm.DB, tenant string, revision models.ContentResetRevision, batch contentResetRetireBatch) error {
	itemByID := make(map[uuid.UUID]models.ContentItem, len(batch.Items))
	ids := make([]uuid.UUID, 0, len(batch.Items))
	for _, item := range batch.Items {
		itemByID[item.PublicID] = item
		ids = append(ids, item.PublicID)
	}
	registered, err := contentResetRegisteredIdentities(tx, tenant, contentResetItemsAsCandidates(batch.Items))
	if err != nil {
		return err
	}
	protected, err := retentionProtectedContentIDs(tx, tenant, ids)
	if err != nil {
		return err
	}
	reasons, err := contentResetProtectionReasons(tx, tenant, ids)
	if err != nil {
		return err
	}
	for _, target := range batch.Targets {
		item, found := itemByID[target.ContentItemID]
		if !found {
			return errContentResetTargetDrift
		}
		if item.ProcessingGeneration != 0 && target.Snapshot != nil {
			var snapshot struct {
				ProcessingGeneration *int64 `json:"processing_generation"`
			}
			if json.Unmarshal(target.Snapshot, &snapshot) == nil && snapshot.ProcessingGeneration != nil && *snapshot.ProcessingGeneration != item.ProcessingGeneration {
				return errContentResetTargetDrift
			}
		}
		current := contentResetTargetSnapshot(contentResetCandidateForItem(item))
		if identity, exists := registered[item.PublicID]; exists {
			current["source_identity_hash"] = identity.Hash
			current["source_identity_quality"] = identity.Quality
		}
		if hashContentResetValue(current) != target.SnapshotHash || int64(item.ID) != target.ItemOrdinal || contentResetLaneForType(item.Type) != target.Lane {
			return errContentResetTargetDrift
		}
		currentReasons := reasons[item.PublicID]
		if currentReasons == nil {
			currentReasons = []string{}
		}
		if protected[item.PublicID] || hashContentResetValue(currentReasons) != hashContentResetValue(decodeContentResetProtectionEvidence(target.ProtectionEvidence)) {
			return errContentResetProtectionDrift
		}
	}
	return nil
}

func contentResetCandidateForItem(item models.ContentItem) contentResetCandidate {
	return contentResetCandidate{
		ID: int64(item.ID), PublicID: item.PublicID, TenantID: item.TenantID, Type: item.Type, Source: item.Source,
		Status: item.Status, ProcessingGeneration: item.ProcessingGeneration, ContentSourceID: item.ContentSourceID,
		IdempotencyKey: item.IdempotencyKey, OriginalURL: item.OriginalURL, SourceEpisodeID: item.SourceEpisodeID,
		Metadata: item.Metadata, UpdatedAt: item.UpdatedAt, CreatedAt: item.CreatedAt, PublishedAt: item.PublishedAt,
		StoryID: item.StoryID, ParentContentItemID: item.ParentContentItemID,
	}
}

func contentResetItemsAsCandidates(items []models.ContentItem) []contentResetCandidate {
	candidates := make([]contentResetCandidate, 0, len(items))
	for _, item := range items {
		candidates = append(candidates, contentResetCandidateForItem(item))
	}
	return candidates
}

// ---- News retirement owner ----

type contentResetNewsRetireOwner struct{}

func (contentResetNewsRetireOwner) Contract() contentreset.Contract {
	return contentreset.Contract{Owner: "cms/news", Effect: "retire_exact", TargetType: "manifest_batch", Version: "v1"}
}

func (owner contentResetNewsRetireOwner) Execute(ctx context.Context, db *gorm.DB, command contentreset.Command, token uuid.UUID) (contentreset.Observation, error) {
	parameters, err := parseContentResetRetireParameters(command, owner.Contract().Effect)
	if err != nil || parameters.Lane != "news" {
		return contentreset.Observation{}, errors.New("invalid News retirement command")
	}
	var retired, tombstones int
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		campaign, revision, err := contentreset.LockOwnerCommand(tx, command, token)
		if err != nil {
			return err
		}
		// Lock the exact content rows before re-checking protections so an
		// interaction or typed update committed under the same row lock cannot
		// slip between the check and the retirement mutation.
		batch, err := loadAndLockContentResetRetireBatch(tx, campaign, revision, parameters)
		if err != nil {
			return err
		}
		for index := range batch.Items {
			item := &batch.Items[index]
			if item.Type != models.ContentTypeNews && item.Type != models.ContentTypeArticle && item.Type != models.ContentTypeTweet && item.Type != models.ContentTypeComment {
				return errors.New("News retirement encountered a non-News target")
			}
			if item.Status == models.ContentStatusPending || item.Status == models.ContentStatusProcessing {
				return errors.New("News retirement requires a terminal processing state")
			}
			var activeStages int64
			if tx.Migrator().HasTable(&models.ContentStageRequest{}) {
				if err := tx.Model(&models.ContentStageRequest{}).
					Where("tenant_id=? AND content_item_id=? AND state NOT IN ?", campaign.TenantID, item.PublicID,
						[]string{models.ContentStageVerified, models.ContentStageDeferred, models.ContentStageFailed, models.ContentStageCancelled, models.ContentStageSuperseded}).
					Count(&activeStages).Error; err != nil {
					return err
				}
			}
			if activeStages != 0 {
				return errors.New("News retirement requires every processing stage to be terminal")
			}
			locked := *item
			if err := createContentResetNewsTombstone(tx, campaign, revision, parameters, locked); err != nil {
				return err
			}
			tombstones++
			storyID := locked.StoryID
			now := time.Now().UTC()
			updates := map[string]any{
				"status":             models.ContentStatusArchived,
				"feed_visibility":    "hidden",
				"is_feed_unit":       false,
				"retired_payload_at": now,
			}
			for _, column := range []string{"title", "body_text", "excerpt", "original_url", "source_feed_url", "author", "source_name", "metadata", "story_id"} {
				updates[column] = nil
			}
			for _, column := range []string{"embedding", "embedding_model", "embedding_space_id", "embedding_producer_id", "image_embedding", "image_embedding_model", "image_embedding_space_id", "image_embedding_producer_id"} {
				updates[column] = nil
			}
			if err := tx.Model(&models.ContentItem{}).Where("tenant_id=? AND public_id=?", campaign.TenantID, locked.PublicID).Updates(updates).Error; err != nil {
				return err
			}
			if tx.Migrator().HasTable(&models.ContentItemTopic{}) {
				if err := tx.Where("content_item_id=?", locked.PublicID).Delete(&models.ContentItemTopic{}).Error; err != nil {
					return err
				}
			}
			var generationIDs []uuid.UUID
			if err := tx.Model(&models.FeedGenerationMembership{}).
				Where("member_id=? AND member_type='news_item'", locked.PublicID).
				Distinct().Pluck("generation_id", &generationIDs).Error; err != nil {
				return err
			}
			if err := tx.Where("member_id=? AND member_type='news_item'", locked.PublicID).Delete(&models.FeedGenerationMembership{}).Error; err != nil {
				return err
			}
			for _, generationID := range generationIDs {
				var generation models.FeedGeneration
				if err := tx.Where("tenant_id=? AND public_id=?", campaign.TenantID, generationID).First(&generation).Error; err != nil {
					continue
				}
				if generation.Purpose == "content_reset" && storyID != nil {
					if err := feedstate.RebuildNewsStoryProjection(tx, campaign.TenantID, generationID, *storyID); err != nil {
						return err
					}
				}
			}
			if err := retireContentResetInstance(tx, campaign.TenantID, locked.PublicID); err != nil {
				return err
			}
			retired++
		}
		return appendContentResetEvidence(tx, campaign, &revision, "retire/"+parameters.Lane+"/"+fmt.Sprint(parameters.Ordinal)+"/"+command.CommandID.String(), "owner_retirement", owner.Contract().Owner, map[string]any{
			"lane": parameters.Lane, "ordinal": parameters.Ordinal, "purpose": parameters.Purpose,
			"retired": retired, "tombstones": tombstones,
		})
	})
	if errors.Is(err, errContentResetTargetDrift) || errors.Is(err, errContentResetProtectionDrift) {
		evidence, _ := json.Marshal(map[string]any{"lane": "news", "ordinal": parameters.Ordinal, "drift": err.Error()})
		return contentreset.Observation{CommandID: command.CommandID, CommandHash: contentreset.Hash(command), State: "blocked", ReasonCode: "target_protection_changed", Evidence: evidence}, nil
	}
	if err != nil {
		return contentreset.Observation{}, err
	}
	return resetObservation(command, "succeeded", "news_retirement_batch_complete", map[string]any{"retired": retired, "tombstones": tombstones, "lane": "news", "ordinal": parameters.Ordinal}), nil
}

func (owner contentResetNewsRetireOwner) Reconcile(ctx context.Context, db *gorm.DB, command contentreset.Command) (contentreset.Observation, error) {
	parameters, err := parseContentResetRetireParameters(command, owner.Contract().Effect)
	if err != nil || parameters.Lane != "news" {
		return contentreset.Observation{}, errors.New("invalid News retirement readback")
	}
	var observation contentreset.Observation
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		campaign, _, err := lockContentResetReadback(tx, command)
		if err != nil {
			return err
		}
		var evidence models.ContentResetEvidence
		key := "retire/" + parameters.Lane + "/" + fmt.Sprint(parameters.Ordinal) + "/" + command.CommandID.String()
		if err := tx.Where("tenant_id=? AND campaign_id=? AND evidence_key=?", campaign.TenantID, campaign.ID, key).First(&evidence).Error; errors.Is(err, gorm.ErrRecordNotFound) {
			// No batch evidence means no destructive statement committed.
			observation = resetObservation(command, "failed", "news_retirement_not_committed", map[string]any{"retired": 0, "no_effect_proven": true})
			observation.NoEffectProven = true
			return nil
		} else if err != nil {
			return err
		}
		var payload map[string]any
		if json.Unmarshal(evidence.Payload, &payload) != nil {
			return errors.New("News retirement evidence is invalid")
		}
		observation = resetObservation(command, "succeeded", "news_retirement_batch_complete", payload)
		return nil
	})
	return observation, err
}

func createContentResetNewsTombstone(tx *gorm.DB, campaign models.ContentResetCampaign, revision models.ContentResetRevision, parameters contentResetRetireParameters, item models.ContentItem) error {
	identity, source, originalURL, err := retentionTombstoneIdentity(campaign.TenantID, item)
	if err != nil {
		return err
	}
	row := models.NewsIngestTombstone{
		TenantID: campaign.TenantID, IdentityHash: identity, SourceIdentityHash: source,
		OriginalURLHash: originalURL, OriginalContentID: item.PublicID, ManifestHash: *revision.ManifestHash,
		Reason: "content_reset_" + parameters.Purpose,
	}
	result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&row)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 1 {
		return nil
	}
	var existing models.NewsIngestTombstone
	if err := tx.Where("tenant_id=? AND identity_hash=?", campaign.TenantID, identity).First(&existing).Error; err != nil {
		return err
	}
	if existing.OriginalContentID != item.PublicID || existing.ManifestHash != *revision.ManifestHash {
		return errors.New("News retirement tombstone identity collision")
	}
	return nil
}

func retireContentResetInstance(tx *gorm.DB, tenant string, itemID uuid.UUID) error {
	return sourceidentity.RetireInstance(tx, tenant, itemID)
}
