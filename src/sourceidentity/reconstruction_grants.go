package sourceidentity

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/supply"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrInvalidReconstructionGrant = errors.New("source-item reconstruction grant is invalid")
	ErrReconstructionGrantUsed    = errors.New("source-item reconstruction grant was already consumed")
	ErrReconstructionGrantScope   = errors.New("source-item reconstruction grant does not match the approved campaign scope")
)

const maxReconstructionGrantTTL = 20 * time.Minute

// GrantTokenDerivationKey separates reconstruction capabilities from the
// broader CMS authentication secret without adding another deployment secret.
func GrantTokenDerivationKey(secret []byte) []byte {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte("wahb/content-reset/reconstruction-grant-key/v1"))
	return mac.Sum(nil)
}

type ReconstructionGrantIssueInput struct {
	TenantID            string
	CampaignID          uuid.UUID
	RevisionID          uuid.UUID
	TargetContentID     uuid.UUID
	ObservationID       uuid.UUID
	ReplayRequestID     uuid.UUID
	SourceRunAttemptID  uuid.UUID
	ExecutionUnitID     uuid.UUID
	ExecutionFenceToken uuid.UUID
	UnitJobID           string
	ExecutionLeaseToken uuid.UUID
	PageID              string
	BatchID             string
	CreatedBy           string
	ExpiresAt           time.Time
	TokenDerivationKey  []byte
}

type IssuedReconstructionGrant struct {
	Grant                 models.ContentResetReconstructionGrant
	Token                 string
	ExistingContentItemID *uuid.UUID
	ReferenceID           uuid.UUID
}

func persistReplayReuse(tx *gorm.DB, reuse models.ContentResetReplayReuse) (models.ContentResetReplayReuse, error) {
	var previous models.ContentResetReplayReuse
	err := tx.Where("tenant_id=? AND observation_id=?", reuse.TenantID, reuse.ObservationID).First(&previous).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return reuse, tx.Create(&reuse).Error
	}
	if err != nil {
		return previous, err
	}
	grantMatches := (previous.GrantID == nil && reuse.GrantID == nil) || (previous.GrantID != nil && reuse.GrantID != nil && *previous.GrantID == *reuse.GrantID)
	if previous.CampaignID != reuse.CampaignID || previous.RevisionID != reuse.RevisionID || !grantMatches || previous.SourceRunRequestID != reuse.SourceRunRequestID || previous.ExecutionUnitID != reuse.ExecutionUnitID || previous.ContentItemID != reuse.ContentItemID || previous.InstanceGeneration != reuse.InstanceGeneration || previous.Fingerprint != reuse.Fingerprint {
		return previous, ErrReconstructionGrantScope
	}
	return previous, nil
}

// IssueReconstructionGrant mints one short-lived capability for exactly one
// selected identity, replay observation, replacement generation, and frozen
// Content Reset revision. Callers must invoke it inside the transaction that
// admits the approved campaign's replay effect.
func IssueReconstructionGrant(tx *gorm.DB, input ReconstructionGrantIssueInput) (IssuedReconstructionGrant, error) {
	if tx == nil || len(input.TokenDerivationKey) < 32 || strings.TrimSpace(input.TenantID) == "" ||
		input.CampaignID == uuid.Nil || input.RevisionID == uuid.Nil ||
		input.ObservationID == uuid.Nil || input.ReplayRequestID == uuid.Nil || input.SourceRunAttemptID == uuid.Nil ||
		input.ExecutionUnitID == uuid.Nil || input.ExecutionFenceToken == uuid.Nil || input.ExecutionLeaseToken == uuid.Nil ||
		strings.TrimSpace(input.UnitJobID) == "" || strings.TrimSpace(input.PageID) == "" || strings.TrimSpace(input.BatchID) == "" ||
		strings.TrimSpace(input.CreatedBy) == "" {
		return IssuedReconstructionGrant{}, ErrInvalidReconstructionGrant
	}
	now := time.Now().UTC()
	expiresAt := input.ExpiresAt.UTC().Truncate(time.Microsecond)
	if !expiresAt.After(now) || expiresAt.After(now.Add(maxReconstructionGrantTTL)) {
		return IssuedReconstructionGrant{}, fmt.Errorf("%w: expiry must be within the next 20 minutes", ErrInvalidReconstructionGrant)
	}

	var campaign models.ContentResetCampaign
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where(
		"tenant_id = ? AND public_id = ?", input.TenantID, input.CampaignID,
	).First(&campaign).Error; err != nil {
		return IssuedReconstructionGrant{}, err
	}
	if campaign.Operation != "fresh_start" || campaign.State != "executing" {
		return IssuedReconstructionGrant{}, ErrReconstructionGrantScope
	}
	var revision models.ContentResetRevision
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where(
		"tenant_id = ? AND campaign_id = ? AND public_id = ? AND revision = ?",
		input.TenantID, campaign.ID, input.RevisionID, campaign.CurrentRevision,
	).First(&revision).Error; err != nil {
		return IssuedReconstructionGrant{}, err
	}
	if revision.State != "previewed" || revision.ManifestHash == nil || strings.TrimSpace(*revision.ManifestHash) == "" {
		return IssuedReconstructionGrant{}, ErrReconstructionGrantScope
	}
	if err := RequireRunningCampaign(tx, campaign, revision); err != nil {
		return IssuedReconstructionGrant{}, err
	}

	var request models.SourceRunRequest
	if err := tx.Where("tenant_id = ? AND public_id = ? AND purpose = ?",
		input.TenantID, input.ReplayRequestID, "content_reset_replay").First(&request).Error; err != nil {
		return IssuedReconstructionGrant{}, ErrReconstructionGrantScope
	}
	if err := validateReplayRequestBinding(request, campaign, revision); err != nil {
		return IssuedReconstructionGrant{}, err
	}
	if _, err := supply.ValidateContentResetReplayRequest(tx, request); err != nil {
		return IssuedReconstructionGrant{}, ErrReconstructionGrantScope
	}

	var observation models.SourceUpstreamObservation
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where(
		"tenant_id = ? AND public_id = ? AND content_source_id = ? AND source_run_request_id = ?",
		input.TenantID, input.ObservationID, request.ContentSourceID, input.ReplayRequestID,
	).First(&observation).Error; err != nil {
		return IssuedReconstructionGrant{}, ErrReconstructionGrantScope
	}
	if observation.ProviderCapability != "replayable_listing" || observation.ProviderVersion == "" ||
		observation.ReplayUntil != nil && !observation.ReplayUntil.After(now) || !isSHA256Hex(observation.UpstreamFingerprint) {
		return IssuedReconstructionGrant{}, ErrInvalidReconstructionGrant
	}
	if observation.ReplayUntil != nil && expiresAt.After(observation.ReplayUntil.UTC()) {
		expiresAt = observation.ReplayUntil.UTC().Truncate(time.Microsecond)
	}
	if !expiresAt.After(now) {
		return IssuedReconstructionGrant{}, ErrInvalidReconstructionGrant
	}

	var unit models.SourceRunExecutionUnit
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where(
		"tenant_id = ? AND public_id = ? AND source_run_request_id = ? AND source_run_attempt_id = ?",
		input.TenantID, input.ExecutionUnitID, input.ReplayRequestID, input.SourceRunAttemptID,
	).First(&unit).Error; err != nil {
		return IssuedReconstructionGrant{}, ErrReconstructionGrantScope
	}
	if unit.ContentSourceID != request.ContentSourceID || unit.UnitType != "normalize_batch" || unit.State != "running" ||
		unit.ExecutionOwner != "aggregation" || unit.JobID != strings.TrimSpace(input.UnitJobID) || unit.AttemptFenceToken != input.ExecutionFenceToken ||
		unit.ExecutionLeaseToken == nil || *unit.ExecutionLeaseToken != input.ExecutionLeaseToken || unit.ExecutionLeaseExpiresAt == nil || !unit.ExecutionLeaseExpiresAt.After(now) ||
		unit.PageID != strings.TrimSpace(input.PageID) || unit.BatchID != strings.TrimSpace(input.BatchID) {
		return IssuedReconstructionGrant{}, ErrReconstructionGrantScope
	}
	if unit.ParentUnitID == nil {
		return IssuedReconstructionGrant{}, ErrReconstructionGrantScope
	}
	var page models.SourceRunExecutionUnit
	if err := tx.Where("tenant_id = ? AND public_id = ? AND source_run_request_id = ? AND source_run_attempt_id = ?",
		input.TenantID, *unit.ParentUnitID, input.ReplayRequestID, input.SourceRunAttemptID,
	).First(&page).Error; err != nil || page.UnitType != "fetch_page" || page.PageID != unit.PageID || page.DeclaredChildCount < 1 || len(page.DeclaredChildDigest) != 64 {
		return IssuedReconstructionGrant{}, ErrReconstructionGrantScope
	}
	var childKeys []string
	if err := tx.Model(&models.SourceRunExecutionUnit{}).Where(
		"tenant_id = ? AND parent_unit_id = ? AND unit_type = ?", input.TenantID, page.PublicID, "normalize_batch",
	).Order("unit_key ASC").Pluck("unit_key", &childKeys).Error; err != nil {
		return IssuedReconstructionGrant{}, err
	}
	childDigest, err := supply.ManifestChildDigest(childKeys)
	if err != nil || len(childKeys) != page.DeclaredChildCount || childDigest != page.DeclaredChildDigest || !containsString(childKeys, unit.UnitKey) {
		return IssuedReconstructionGrant{}, ErrReconstructionGrantScope
	}
	if observation.ProviderPageID != unit.PageID {
		return IssuedReconstructionGrant{}, ErrReconstructionGrantScope
	}
	if !replaySourceVersionMatches(revision.ReplaySourceSnapshot, request.ContentSourceID, tx, input.TenantID) {
		return IssuedReconstructionGrant{}, ErrReconstructionGrantScope
	}

	identityLock := strings.Join([]string{input.TenantID, request.ContentSourceID.String(), observation.UpstreamItemID}, "\n")
	if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", identityLock).Error; err != nil {
		return IssuedReconstructionGrant{}, err
	}
	var identity models.SourceItemIdentity
	identityErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where(
		"tenant_id = ? AND content_source_id = ? AND upstream_item_id = ?",
		input.TenantID, request.ContentSourceID, observation.UpstreamItemID,
	).First(&identity).Error
	if errors.Is(identityErr, gorm.ErrRecordNotFound) {
		identity = models.SourceItemIdentity{
			PublicID: uuid.New(), TenantID: input.TenantID, ContentSourceID: request.ContentSourceID,
			UpstreamItemID: observation.UpstreamItemID, CreatedAt: now, UpdatedAt: now,
		}
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "tenant_id"}, {Name: "content_source_id"}, {Name: "upstream_item_id"}},
			DoNothing: true,
		}).Create(&identity).Error; err != nil {
			return IssuedReconstructionGrant{}, err
		}
		identityErr = tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where(
			"tenant_id = ? AND content_source_id = ? AND upstream_item_id = ?",
			input.TenantID, request.ContentSourceID, observation.UpstreamItemID,
		).First(&identity).Error
	}
	if identityErr != nil {
		return IssuedReconstructionGrant{}, identityErr
	}
	// A consumed grant can be retried while its replacement is still staged.
	// The live identity pointer deliberately continues to name the old instance.
	var consumed models.ContentResetReconstructionGrant
	consumedErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where(
		"tenant_id = ? AND campaign_id = ? AND identity_id = ? AND state = ?",
		input.TenantID, campaign.ID, identity.ID, "consumed",
	).Order("id DESC").First(&consumed).Error
	if consumedErr == nil {
		if consumed.RevisionID != revision.ID || consumed.ReplacementContentItemID == nil ||
			consumed.ExpectedFingerprint != strings.ToLower(observation.UpstreamFingerprint) || consumed.ProviderVersion != observation.ProviderVersion ||
			input.TargetContentID != uuid.Nil && (consumed.TargetContentItemID == nil || *consumed.TargetContentItemID != input.TargetContentID) {
			return IssuedReconstructionGrant{}, ErrReconstructionGrantScope
		}
		if err := verifyConsumedInstance(tx, identity, consumed); err != nil {
			return IssuedReconstructionGrant{}, err
		}
		if consumed.SourceObservationID != observation.PublicID {
			// A later replay page may observe the same unchanged upstream item.
			// Persist a separate reference under that page's current native
			// lease; never expose a grant bound to a different execution unit.
			var exists int64
			if err := tx.Model(&models.ContentItem{}).Where("tenant_id=? AND public_id=? AND content_source_id=?", input.TenantID, *consumed.ReplacementContentItemID, request.ContentSourceID).Count(&exists).Error; err != nil {
				return IssuedReconstructionGrant{}, err
			}
			if exists != 1 {
				return IssuedReconstructionGrant{}, ErrReconstructionGrantScope
			}
			reference, err := persistReplayReuse(tx, models.ContentResetReplayReuse{PublicID: uuid.New(), TenantID: input.TenantID, CampaignID: campaign.ID, RevisionID: revision.ID, GrantID: &consumed.ID, ObservationID: observation.PublicID, SourceRunRequestID: request.PublicID, ExecutionUnitID: unit.PublicID, ContentItemID: *consumed.ReplacementContentItemID, InstanceGeneration: consumed.ReplacementInstanceGeneration, Fingerprint: strings.ToLower(observation.UpstreamFingerprint)})
			if err != nil {
				return IssuedReconstructionGrant{}, err
			}
			return IssuedReconstructionGrant{Grant: consumed, ExistingContentItemID: consumed.ReplacementContentItemID, ReferenceID: reference.PublicID}, nil
		}
		if consumed.SourceRunRequestID != input.ReplayRequestID || consumed.SourceRunAttemptID != input.SourceRunAttemptID ||
			consumed.ExecutionUnitID != input.ExecutionUnitID || consumed.ExecutionFenceToken != input.ExecutionFenceToken ||
			consumed.PageID != input.PageID || consumed.BatchID != input.BatchID || consumed.ReplacementContentItemID == nil ||
			consumed.ExpectedFingerprint != strings.ToLower(observation.UpstreamFingerprint) || consumed.ProviderVersion != observation.ProviderVersion ||
			input.TargetContentID != uuid.Nil && (consumed.TargetContentItemID == nil || *consumed.TargetContentItemID != input.TargetContentID) {
			return IssuedReconstructionGrant{}, ErrReconstructionGrantScope
		}
		if err := verifyConsumedInstance(tx, identity, consumed); err != nil {
			return IssuedReconstructionGrant{}, err
		}
		token := deriveGrantToken(input.TokenDerivationKey, consumed)
		if !hmac.Equal([]byte(hashOpaqueToken(token)), []byte(consumed.GrantTokenHash)) {
			return IssuedReconstructionGrant{}, ErrInvalidReconstructionGrant
		}
		return IssuedReconstructionGrant{Grant: consumed, Token: token}, nil
	} else if !errors.Is(consumedErr, gorm.ErrRecordNotFound) {
		return IssuedReconstructionGrant{}, consumedErr
	}
	var targetContentID *uuid.UUID
	grantKind := "new_identity"
	if identity.CurrentInstanceGeneration == 0 {
		if identity.CurrentContentItemID != nil {
			return IssuedReconstructionGrant{}, ErrIdentityCorrupt
		}
		if input.TargetContentID != uuid.Nil {
			return IssuedReconstructionGrant{}, ErrReconstructionGrantScope
		}
	} else {
		if identity.CurrentInstanceGeneration < 1 || identity.CurrentContentItemID == nil {
			return IssuedReconstructionGrant{}, ErrIdentityCorrupt
		}
		grantKind = "replacement"
		currentID := *identity.CurrentContentItemID
		targetContentID = &currentID
		if input.TargetContentID != uuid.Nil && input.TargetContentID != currentID {
			return IssuedReconstructionGrant{}, ErrReconstructionGrantScope
		}
		var target models.ContentResetTarget
		targetErr := tx.Where(
			"tenant_id = ? AND revision_id = ? AND content_item_id = ? AND disposition = ? AND protected = FALSE",
			input.TenantID, revision.ID, currentID, "selected",
		).First(&target).Error
		if errors.Is(targetErr, gorm.ErrRecordNotFound) {
			// Replay sources include protected survivors and unrelated live
			// arrivals. Their unchanged active instances are carried forward;
			// only selected identities may receive replacement grants.
			var instance models.SourceItemInstance
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND identity_id=? AND instance_generation=? AND content_item_id=? AND state='active'", input.TenantID, identity.ID, identity.CurrentInstanceGeneration, currentID).First(&instance).Error; err != nil {
				return IssuedReconstructionGrant{}, ErrReconstructionGrantScope
			}
			if instance.UpstreamFingerprint != strings.ToLower(observation.UpstreamFingerprint) || instance.ProviderVersion != observation.ProviderVersion {
				return IssuedReconstructionGrant{}, ErrReconstructionGrantScope
			}
			lane := "pods"
			if request.Lane == "news" {
				lane = "news"
			}
			if err := requireCandidateView(tx, campaign, lane); err != nil {
				return IssuedReconstructionGrant{}, err
			}
			var exists int64
			if err := tx.Model(&models.ContentItem{}).Where("tenant_id=? AND public_id=? AND content_source_id=?", input.TenantID, currentID, request.ContentSourceID).Count(&exists).Error; err != nil {
				return IssuedReconstructionGrant{}, err
			}
			if exists != 1 {
				return IssuedReconstructionGrant{}, ErrReconstructionGrantScope
			}
			reference, err := persistReplayReuse(tx, models.ContentResetReplayReuse{PublicID: uuid.New(), TenantID: input.TenantID, CampaignID: campaign.ID, RevisionID: revision.ID, ObservationID: observation.PublicID, SourceRunRequestID: request.PublicID, ExecutionUnitID: unit.PublicID, ContentItemID: currentID, InstanceGeneration: instance.InstanceGeneration, Fingerprint: instance.UpstreamFingerprint})
			if err != nil {
				return IssuedReconstructionGrant{}, err
			}
			return IssuedReconstructionGrant{ExistingContentItemID: &currentID, ReferenceID: reference.PublicID, Grant: models.ContentResetReconstructionGrant{ReplacementInstanceGeneration: instance.InstanceGeneration}}, nil
		}
		if targetErr != nil {
			return IssuedReconstructionGrant{}, ErrReconstructionGrantScope
		}
		if err := requireCandidateView(tx, campaign, target.Lane); err != nil {
			return IssuedReconstructionGrant{}, err
		}
		var snapshot struct {
			ContentSourceID       *uuid.UUID `json:"content_source_id"`
			SourceType            string     `json:"source_type"`
			SourceIdentityHash    string     `json:"source_identity_hash"`
			SourceIdentityQuality string     `json:"source_identity_quality"`
		}
		if json.Unmarshal(target.Snapshot, &snapshot) != nil || snapshot.ContentSourceID == nil ||
			*snapshot.ContentSourceID != request.ContentSourceID || snapshot.SourceIdentityQuality != "registered_provider_identity" || strings.TrimSpace(snapshot.SourceIdentityHash) == "" {
			return IssuedReconstructionGrant{}, ErrReconstructionGrantScope
		}
		identityHash := hashIdentityCandidate(input.TenantID, request.ContentSourceID, snapshot.SourceType, identity.UpstreamItemID)
		if !hmac.Equal([]byte(identityHash), []byte(snapshot.SourceIdentityHash)) {
			return IssuedReconstructionGrant{}, ErrReconstructionGrantScope
		}
		var currentInstance models.SourceItemInstance
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where(
			"tenant_id = ? AND identity_id = ? AND instance_generation = ? AND content_item_id = ?",
			input.TenantID, identity.ID, identity.CurrentInstanceGeneration, currentID,
		).First(&currentInstance).Error; err != nil {
			return IssuedReconstructionGrant{}, ErrIdentityCorrupt
		}
		if currentInstance.State != "active" && currentInstance.State != "retired" {
			return IssuedReconstructionGrant{}, ErrReconstructionGrantScope
		}
	}
	candidateLane := "pods"
	if request.Lane == "news" {
		candidateLane = "news"
	} else if request.Lane != "media" {
		return IssuedReconstructionGrant{}, ErrReconstructionGrantScope
	}
	if err := requireCandidateView(tx, campaign, candidateLane); err != nil {
		return IssuedReconstructionGrant{}, err
	}
	if err := tx.Model(&models.ContentResetReconstructionGrant{}).Where(
		"tenant_id = ? AND campaign_id = ? AND identity_id = ? AND state = ? AND expires_at <= ?",
		input.TenantID, campaign.ID, identity.ID, "issued", now,
	).Update("state", "expired").Error; err != nil {
		return IssuedReconstructionGrant{}, err
	}
	var existing models.ContentResetReconstructionGrant
	err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where(
		"tenant_id = ? AND campaign_id = ? AND identity_id = ? AND state = ?",
		input.TenantID, campaign.ID, identity.ID, "issued",
	).Order("id DESC").First(&existing).Error
	if err == nil {
		if existing.BaseInstanceGeneration != identity.CurrentInstanceGeneration || existing.RevisionID != revision.ID || existing.GrantKind != grantKind || !sameOptionalUUID(existing.TargetContentItemID, targetContentID) || existing.SourceObservationID != observation.PublicID ||
			existing.ExpectedFingerprint != strings.ToLower(observation.UpstreamFingerprint) || existing.ProviderVersion != observation.ProviderVersion ||
			existing.SourceRunRequestID != input.ReplayRequestID || existing.SourceRunAttemptID != input.SourceRunAttemptID ||
			existing.ExecutionUnitID != input.ExecutionUnitID || existing.ExecutionFenceToken != input.ExecutionFenceToken ||
			existing.PageID != input.PageID || existing.BatchID != input.BatchID ||
			!existing.ExpiresAt.After(now) {
			return IssuedReconstructionGrant{}, ErrReconstructionGrantScope
		}
		token := deriveGrantToken(input.TokenDerivationKey, existing)
		if !hmac.Equal([]byte(hashOpaqueToken(token)), []byte(existing.GrantTokenHash)) {
			return IssuedReconstructionGrant{}, fmt.Errorf("%w: token derivation key changed", ErrInvalidReconstructionGrant)
		}
		return IssuedReconstructionGrant{Grant: existing, Token: token}, nil
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return IssuedReconstructionGrant{}, err
	}

	// Never recycle an abandoned or expired generation. Both materialized
	// instances and unconsumed grants reserve their generation permanently.
	var reserved int64
	if err := tx.Raw(`SELECT GREATEST(
		COALESCE((SELECT MAX(instance_generation) FROM source_item_instances WHERE tenant_id=? AND identity_id=?),0),
		COALESCE((SELECT MAX(replacement_instance_generation) FROM content_reset_reconstruction_grants WHERE tenant_id=? AND identity_id=?),0), ?)
		`, input.TenantID, identity.ID, input.TenantID, identity.ID, identity.CurrentInstanceGeneration).Scan(&reserved).Error; err != nil {
		return IssuedReconstructionGrant{}, err
	}
	if reserved >= 2147483647 {
		return IssuedReconstructionGrant{}, ErrIdentityCorrupt
	}
	nextGeneration := int(reserved) + 1
	grant := models.ContentResetReconstructionGrant{
		PublicID: uuid.New(), TenantID: input.TenantID, CampaignID: campaign.ID,
		RevisionID: revision.ID, IdentityID: identity.ID, GrantKind: grantKind, TargetContentItemID: targetContentID,
		SourceRunRequestID: input.ReplayRequestID, SourceRunAttemptID: input.SourceRunAttemptID,
		ExecutionUnitID: input.ExecutionUnitID, ExecutionFenceToken: input.ExecutionFenceToken,
		PageID: input.PageID, BatchID: input.BatchID,
		SourceObservationID:           observation.PublicID,
		ReplacementInstanceGeneration: nextGeneration, BaseInstanceGeneration: identity.CurrentInstanceGeneration, ExpectedFingerprint: strings.ToLower(observation.UpstreamFingerprint),
		ProviderVersion: observation.ProviderVersion, State: "issued", ExpiresAt: expiresAt,
		CreatedBy: strings.TrimSpace(input.CreatedBy), CreatedAt: now,
	}
	token := deriveGrantToken(input.TokenDerivationKey, grant)
	grant.GrantTokenHash = hashOpaqueToken(token)
	if err := tx.Create(&grant).Error; err != nil {
		return IssuedReconstructionGrant{}, err
	}
	return IssuedReconstructionGrant{Grant: grant, Token: token}, nil
}

// ResolveReconstructionGrant validates a one-use grant before the controller
// decides whether to return its already-created replacement or materialize the
// next instance. An issued grant is usable only while its campaign is running;
// a consumed grant remains readable for idempotent retry after campaign close.
func ResolveReconstructionGrant(db *gorm.DB, input *Input, token string) (*Binding, error) {
	if db == nil || input == nil || strings.TrimSpace(token) == "" {
		return nil, ErrInvalidReconstructionGrant
	}
	tokenHash := hashOpaqueToken(token)
	var grant models.ContentResetReconstructionGrant
	if err := db.Where("tenant_id = ? AND grant_token_hash = ?", input.TenantID, tokenHash).First(&grant).Error; err != nil {
		return nil, ErrInvalidReconstructionGrant
	}
	return resolveGrantRecord(db, input, grant, tokenHash)
}

// ConsumeReconstructionGrant records a staged replacement in the same
// transaction as its content row. Publication alone advances the live identity.
func ConsumeReconstructionGrant(tx *gorm.DB, input *Input, token string, contentItemID uuid.UUID) error {
	if tx == nil || input == nil || contentItemID == uuid.Nil {
		return ErrInvalidReconstructionGrant
	}
	if err := lockGrantCampaign(tx, input.TenantID, token); err != nil {
		return err
	}
	binding, err := ResolveReconstructionGrant(tx, input, token)
	if err != nil {
		return err
	}
	grant := binding.ReconstructionGrant
	if grant == nil {
		return ErrInvalidReconstructionGrant
	}
	if grant.State == "consumed" {
		if grant.ReplacementContentItemID != nil && *grant.ReplacementContentItemID == contentItemID {
			return nil
		}
		return ErrReconstructionGrantUsed
	}
	if grant.State != "issued" || !grant.ExpiresAt.After(time.Now().UTC()) || grant.BaseInstanceGeneration != binding.CurrentInstance || grant.ReplacementInstanceGeneration <= binding.CurrentInstance {
		return ErrInvalidReconstructionGrant
	}
	if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", identityLockKey(input)).Error; err != nil {
		return err
	}
	var identity models.SourceItemIdentity
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id = ? AND id = ?", input.TenantID, grant.IdentityID).First(&identity).Error; err != nil {
		return err
	}
	if identity.CurrentInstanceGeneration != binding.CurrentInstance || !sameOptionalUUID(identity.CurrentContentItemID, binding.CurrentContentItemID) {
		return ErrReconstructionGrantScope
	}
	var replacement models.ContentItem
	if err := tx.Where("tenant_id = ? AND public_id = ? AND content_source_id = ?", input.TenantID, contentItemID, input.ContentSourceID).First(&replacement).Error; err != nil {
		return fmt.Errorf("replacement content row is not persisted: %w", err)
	}
	now := time.Now().UTC()
	if grant.GrantKind == "replacement" {
		if binding.CurrentContentItemID == nil || grant.TargetContentItemID == nil || *binding.CurrentContentItemID != *grant.TargetContentItemID {
			return ErrReconstructionGrantScope
		}
		var oldInstance models.SourceItemInstance
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where(
			"tenant_id = ? AND identity_id = ? AND instance_generation = ? AND content_item_id = ?",
			input.TenantID, identity.ID, identity.CurrentInstanceGeneration, *binding.CurrentContentItemID,
		).First(&oldInstance).Error; err != nil {
			return ErrIdentityCorrupt
		}
		if oldInstance.State != "active" && oldInstance.State != "retired" {
			return ErrReconstructionGrantScope
		}
	} else if grant.GrantKind != "new_identity" || binding.CurrentInstance != 0 || binding.CurrentContentItemID != nil {
		return ErrReconstructionGrantScope
	}
	newInstance := models.SourceItemInstance{
		PublicID: uuid.New(), TenantID: input.TenantID, IdentityID: identity.ID,
		InstanceGeneration: grant.ReplacementInstanceGeneration, ContentItemID: replacement.PublicID,
		SourceObservationID: binding.Observation.PublicID,
		UpstreamFingerprint: strings.ToLower(binding.Observation.UpstreamFingerprint),
		ProviderVersion:     binding.Observation.ProviderVersion, CampaignID: &grant.CampaignID,
		State: "staged", CreatedAt: now,
	}
	if err := tx.Create(&newInstance).Error; err != nil {
		return err
	}
	grantResult := tx.Model(&grant).Where("state = ? AND grant_token_hash = ?", "issued", hashOpaqueToken(token)).Updates(map[string]any{
		"state": "consumed", "consumed_at": now, "replacement_content_item_id": replacement.PublicID,
	})
	if grantResult.Error != nil {
		return grantResult.Error
	}
	if grantResult.RowsAffected != 1 {
		return ErrReconstructionGrantUsed
	}
	return nil
}

func resolveGrantRecord(db *gorm.DB, input *Input, grant models.ContentResetReconstructionGrant, tokenHash string) (*Binding, error) {
	if grant.State != "issued" && grant.State != "consumed" {
		return nil, ErrInvalidReconstructionGrant
	}
	now := time.Now().UTC()
	if grant.State == "issued" && !grant.ExpiresAt.After(now) {
		return nil, ErrInvalidReconstructionGrant
	}
	if !hmac.Equal([]byte(grant.GrantTokenHash), []byte(tokenHash)) || grant.IdentityID == 0 || grant.ReplacementInstanceGeneration < 1 {
		return nil, ErrInvalidReconstructionGrant
	}
	if (grant.GrantKind == "replacement" && (grant.TargetContentItemID == nil || grant.BaseInstanceGeneration < 1 || grant.ReplacementInstanceGeneration <= grant.BaseInstanceGeneration)) ||
		(grant.GrantKind == "new_identity" && (grant.TargetContentItemID != nil || grant.BaseInstanceGeneration != 0)) ||
		(grant.GrantKind != "replacement" && grant.GrantKind != "new_identity") {
		return nil, ErrInvalidReconstructionGrant
	}
	if grant.SourceRunRequestID != input.SourceRunRequestID || grant.SourceRunAttemptID != input.SourceRunAttemptID ||
		grant.ExecutionUnitID != input.ExecutionUnitID || grant.ExecutionFenceToken != input.ExecutionFenceToken ||
		grant.PageID != input.PageID || grant.BatchID != input.BatchID {
		return nil, ErrReconstructionGrantScope
	}
	var observation models.SourceUpstreamObservation
	if err := db.Where("tenant_id = ? AND content_source_id = ? AND public_id = ? AND upstream_item_id = ? AND source_run_request_id = ?",
		input.TenantID, input.ContentSourceID, input.ObservationID, input.UpstreamItemID, input.SourceRunRequestID).First(&observation).Error; err != nil {
		return nil, ErrInvalidReconstructionGrant
	}
	if observation.PublicID != grant.SourceObservationID || strings.ToLower(observation.UpstreamFingerprint) != strings.ToLower(grant.ExpectedFingerprint) || observation.ProviderVersion != grant.ProviderVersion {
		return nil, ErrReconstructionGrantScope
	}
	if !isSHA256Hex(input.ExpectedFingerprint) || !hmac.Equal([]byte(strings.ToLower(input.ExpectedFingerprint)), []byte(strings.ToLower(grant.ExpectedFingerprint))) {
		return nil, ErrReconstructionGrantScope
	}
	var campaign models.ContentResetCampaign
	if err := db.Where("tenant_id = ? AND id = ?", input.TenantID, grant.CampaignID).First(&campaign).Error; err != nil {
		return nil, ErrReconstructionGrantScope
	}
	var revision models.ContentResetRevision
	if err := db.Where("tenant_id = ? AND id = ?", input.TenantID, grant.RevisionID).First(&revision).Error; err != nil {
		return nil, ErrReconstructionGrantScope
	}
	if err := validateReplayRequestBindingForIdentity(db, input, campaign, revision); err != nil {
		return nil, err
	}
	if grant.State == "issued" && (campaign.Operation != "fresh_start" || campaign.State != "executing" || campaign.CurrentRevision != revision.Revision || revision.State != "previewed") {
		return nil, ErrReconstructionGrantScope
	}
	var identity models.SourceItemIdentity
	if err := db.Where("tenant_id = ? AND id = ? AND content_source_id = ? AND upstream_item_id = ?",
		input.TenantID, grant.IdentityID, input.ContentSourceID, input.UpstreamItemID).First(&identity).Error; err != nil {
		return nil, ErrReconstructionGrantScope
	}
	if observation.UpstreamItemID != identity.UpstreamItemID {
		return nil, ErrReconstructionGrantScope
	}
	if grant.State == "issued" {
		if identity.CurrentInstanceGeneration != grant.BaseInstanceGeneration || !sameOptionalUUID(identity.CurrentContentItemID, grant.TargetContentItemID) {
			return nil, ErrReconstructionGrantScope
		}
	}
	if grant.State == "consumed" {
		if err := verifyConsumedInstance(db, identity, grant); err != nil {
			return nil, err
		}
	}
	var previousContentID *uuid.UUID
	if grant.GrantKind == "replacement" {
		if grant.TargetContentItemID == nil {
			return nil, ErrInvalidReconstructionGrant
		}
		var target models.ContentResetTarget
		if err := db.Where("tenant_id = ? AND revision_id = ? AND content_item_id = ? AND disposition = ? AND protected = FALSE",
			input.TenantID, revision.ID, *grant.TargetContentItemID, "selected").First(&target).Error; err != nil {
			return nil, ErrReconstructionGrantScope
		}
		var snapshot struct {
			ContentSourceID       *uuid.UUID `json:"content_source_id"`
			SourceType            string     `json:"source_type"`
			SourceIdentityHash    string     `json:"source_identity_hash"`
			SourceIdentityQuality string     `json:"source_identity_quality"`
		}
		if json.Unmarshal(target.Snapshot, &snapshot) != nil || snapshot.ContentSourceID == nil ||
			*snapshot.ContentSourceID != input.ContentSourceID || snapshot.SourceIdentityQuality != "registered_provider_identity" ||
			!hmac.Equal([]byte(snapshot.SourceIdentityHash), []byte(hashIdentityCandidate(input.TenantID, input.ContentSourceID, snapshot.SourceType, identity.UpstreamItemID))) {
			return nil, ErrReconstructionGrantScope
		}
		var previous models.SourceItemInstance
		if err := db.Where("tenant_id = ? AND identity_id = ? AND instance_generation = ? AND content_item_id = ?", input.TenantID, identity.ID, grant.BaseInstanceGeneration, *grant.TargetContentItemID).First(&previous).Error; err != nil {
			return nil, ErrIdentityCorrupt
		}
		if previous.State != "active" && previous.State != "retired" && previous.State != "superseded" {
			return nil, ErrIdentityCorrupt
		}
		previousContentID = previousContentItemID(previous.ContentItemID)
	}
	binding := &Binding{
		Input: *input, IdentityID: identity.ID, IdentityPublicID: identity.PublicID,
		Observation: observation, CurrentInstance: grant.BaseInstanceGeneration,
		CurrentContentItemID: previousContentID, HasIdentity: true,
		ReconstructionGrant: &grant,
	}
	if grant.State == "consumed" {
		binding.CurrentContentItemID = grant.ReplacementContentItemID
		binding.CurrentInstance = grant.ReplacementInstanceGeneration
	}
	return binding, nil
}

func validateReplayRequestBindingForIdentity(db *gorm.DB, input *Input, campaign models.ContentResetCampaign, revision models.ContentResetRevision) error {
	var request models.SourceRunRequest
	if err := db.Where("tenant_id = ? AND content_source_id = ? AND public_id = ? AND purpose = ?",
		input.TenantID, input.ContentSourceID, input.SourceRunRequestID, "content_reset_replay").First(&request).Error; err != nil {
		return ErrReconstructionGrantScope
	}
	return validateReplayRequestBinding(request, campaign, revision)
}

func validateReplayRequestBinding(request models.SourceRunRequest, campaign models.ContentResetCampaign, revision models.ContentResetRevision) error {
	var metadata struct {
		CampaignID   string `json:"content_reset_campaign_id"`
		RevisionID   string `json:"content_reset_revision_id"`
		ManifestHash string `json:"content_reset_manifest_hash"`
	}
	if json.Unmarshal(request.Metadata, &metadata) != nil || metadata.CampaignID != campaign.PublicID.String() ||
		metadata.RevisionID != revision.PublicID.String() || revision.ManifestHash == nil || metadata.ManifestHash != *revision.ManifestHash {
		return ErrReconstructionGrantScope
	}
	return nil
}

func requireCandidateView(tx *gorm.DB, campaign models.ContentResetCampaign, targetLane string) error {
	if tx == nil || (targetLane != "pods" && targetLane != "news") || campaign.Lane != "both" && campaign.Lane != targetLane {
		return ErrReconstructionGrantScope
	}
	// Candidate story projection is not implemented yet. Do not issue grants
	// that could materialize News replacement rows without an isolated view.
	if targetLane == "news" {
		return ErrReconstructionGrantScope
	}
	if !tx.Migrator().HasTable(&models.FeedGenerationHead{}) || !tx.Migrator().HasTable(&models.FeedGeneration{}) ||
		!tx.Migrator().HasColumn(&models.FeedGeneration{}, "purpose") || !tx.Migrator().HasColumn(&models.FeedGeneration{}, "content_reset_campaign_id") {
		return ErrReconstructionGrantScope
	}
	var head models.FeedGenerationHead
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id = ? AND lane = ?", campaign.TenantID, "media").First(&head).Error; err != nil {
		return ErrReconstructionGrantScope
	}
	if head.CandidateGenerationID == nil || *head.CandidateGenerationID == uuid.Nil {
		return ErrReconstructionGrantScope
	}
	var candidate models.FeedGeneration
	if err := tx.Where("tenant_id = ? AND public_id = ? AND lane = ?", campaign.TenantID, *head.CandidateGenerationID, "media").First(&candidate).Error; err != nil {
		return ErrReconstructionGrantScope
	}
	if candidate.State != "candidate" || candidate.Purpose != "content_reset" || candidate.ContentResetCampaignID == nil || *candidate.ContentResetCampaignID != campaign.ID {
		return ErrReconstructionGrantScope
	}
	return nil
}

func replaySourceVersionMatches(snapshotJSON []byte, sourceID uuid.UUID, tx *gorm.DB, tenant string) bool {
	var sources []struct {
		ID            uuid.UUID `json:"id"`
		ConfigVersion int64     `json:"config_version"`
	}
	if json.Unmarshal(snapshotJSON, &sources) != nil {
		return false
	}
	var expected int64
	found := false
	for _, source := range sources {
		if source.ID == sourceID {
			expected, found = source.ConfigVersion, true
			break
		}
	}
	if !found {
		return false
	}
	var current models.ContentSource
	if tx.Where("tenant_id = ? AND public_id = ? AND is_active = TRUE", tenant, sourceID).First(&current).Error != nil {
		return false
	}
	return current.SourceConfigVersion == expected
}

func hashIdentityCandidate(tenant string, sourceID uuid.UUID, sourceType, upstreamID string) string {
	value, _ := json.Marshal([]string{"source-item-identity-candidate/v1", tenant, sourceID.String(), sourceType, "source_upstream_observation", upstreamID})
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}

func deriveGrantToken(key []byte, grant models.ContentResetReconstructionGrant) string {
	material := strings.Join([]string{
		"content-reset-reconstruction-grant/v2", grant.TenantID, grant.PublicID.String(),
		fmt.Sprint(grant.CampaignID), fmt.Sprint(grant.RevisionID), fmt.Sprint(grant.IdentityID),
		grant.GrantKind, optionalUUIDString(grant.TargetContentItemID),
		grant.SourceRunRequestID.String(), grant.SourceRunAttemptID.String(), grant.ExecutionUnitID.String(),
		grant.ExecutionFenceToken.String(), grant.PageID, grant.BatchID,
		grant.SourceObservationID.String(), fmt.Sprint(grant.BaseInstanceGeneration), fmt.Sprint(grant.ReplacementInstanceGeneration),
		grant.ExpectedFingerprint, grant.ProviderVersion, fmt.Sprint(grant.ExpiresAt.UTC().UnixNano()),
	}, "\n")
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(material))
	return hex.EncodeToString(mac.Sum(nil))
}

func hashOpaqueToken(token string) string {
	digest := sha256.Sum256([]byte(strings.TrimSpace(token)))
	return hex.EncodeToString(digest[:])
}

func isSHA256Hex(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func identityLockKey(input *Input) string {
	return strings.Join([]string{input.TenantID, input.ContentSourceID.String(), input.UpstreamItemID}, "\n")
}

func previousContentItemID(id uuid.UUID) *uuid.UUID { return &id }

func sameOptionalUUID(left, right *uuid.UUID) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func optionalUUIDString(value *uuid.UUID) string {
	if value == nil {
		return ""
	}
	return value.String()
}

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}
