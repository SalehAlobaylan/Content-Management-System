package supply

import (
	"encoding/json"
	"errors"
	"sort"
	"time"

	"content-management-system/src/contentreset"
	"content-management-system/src/models"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// CountSourceAdmissionBlockers keeps delivery state intact while excluding
// provider slots released by immutable native proof. Before the explicit
// migration, every active request continues to block admission.
func CountSourceAdmissionBlockers(tx *gorm.DB, tenant string, source uuid.UUID) (int64, error) {
	query := tx.Model(&models.SourceRunRequest{}).Where("tenant_id=? AND content_source_id=? AND state IN ?", tenant, source, models.SourceRunActiveStates)
	if tx.Migrator().HasColumn(&models.SourceRunRequest{}, "provider_effects_released_at") && tx.Migrator().HasTable(&models.ContentResetProviderRelease{}) {
		query = query.Where("provider_effects_released_at IS NULL")
	}
	var count int64
	err := query.Count(&count).Error
	return count, err
}

// A complete sealed graph is the release prerequisite. Unknown, partial,
// failed, cancelled, leased or running effects keep the source slot held.
func validateReplayProviderGraph(request models.SourceRunRequest, attempt models.SourceRunAttempt, units []models.SourceRunExecutionUnit) (bool, error) {
	if request.Purpose != "content_reset_replay" || request.ManifestState != string(ManifestSealed) || request.ExpectedPageCount != 1 ||
		request.ExpectedUnitCount != len(units) || attempt.RootExecutionUnitID == nil || request.RootExecutionUnitID == nil ||
		*request.RootExecutionUnitID != *attempt.RootExecutionUnitID || request.TenantID != attempt.TenantID || request.PublicID != attempt.SourceRunRequestID || request.ContentSourceID != attempt.ContentSourceID {
		return false, errors.New("provider release requires a complete sealed replay manifest")
	}
	nativeTerminal := request.State == string(RequestSucceeded) && request.VerifiedAt != nil && attempt.State == string(AttemptSucceeded)
	if !nativeTerminal && (request.State != string(RequestVerificationRequired) || attempt.State != string(AttemptVerificationRequired)) {
		return false, ErrReplayYield
	}
	var root, page *models.SourceRunExecutionUnit
	var keys []string
	seen := make(map[uuid.UUID]bool, len(units))
	for i := range units {
		unit := &units[i]
		if unit.PublicID == uuid.Nil || seen[unit.PublicID] || unit.TenantID != request.TenantID || unit.SourceRunRequestID != request.PublicID || unit.SourceRunAttemptID != attempt.PublicID || unit.ContentSourceID != request.ContentSourceID || unit.AttemptFenceToken != attempt.FenceToken {
			return false, errors.New("provider graph identity mismatch")
		}
		seen[unit.PublicID] = true
		if unit.PublicID == *attempt.RootExecutionUnitID {
			if unit.UnitType != "coordinator" || unit.ParentUnitID != nil || nativeTerminal && unit.State != string(UnitSucceeded) || !nativeTerminal && unit.State != string(UnitVerificationRequired) {
				return false, errors.New("provider graph coordinator is not settled for release")
			}
			root = unit
			continue
		}
		if unit.State != string(UnitSucceeded) || unit.TerminalOutcome != string(OutcomeNewItems) && unit.TerminalOutcome != string(OutcomeNoChange) {
			return false, ErrReplayYield
		}
		switch unit.UnitType {
		case "fetch_page":
			if page != nil || unit.PageID != "initial" || unit.UnitKey != "fetch:initial" || unit.ParentUnitID == nil || *unit.ParentUnitID != *attempt.RootExecutionUnitID {
				return false, errors.New("provider graph has an invalid page")
			}
			page = unit
		case "normalize_batch":
			keys = append(keys, unit.UnitKey)
		default:
			return false, errors.New("provider graph contains an unsupported effect")
		}
	}
	if root == nil || page == nil || request.ExpectedBatchCount != len(keys) || page.DeclaredChildCount != len(keys) {
		return false, errors.New("provider graph coverage incomplete")
	}
	digest, err := ManifestChildDigest(keys)
	if err != nil || digest != page.DeclaredChildDigest {
		return false, errors.New("provider graph child digest mismatch")
	}
	for _, unit := range units {
		if unit.UnitType == "normalize_batch" && (unit.ParentUnitID == nil || *unit.ParentUnitID != page.PublicID || unit.PageID != "initial") {
			return false, errors.New("provider graph normalize attribution mismatch")
		}
	}
	return nativeTerminal, nil
}

// ReleaseContentResetProviderSlot runs under the campaign's exact owner lease.
// It never terminalizes a request, releases downstream budgets, or changes
// live checkpoints. Both exclusion stamps and proof commit in one transaction.
func ReleaseContentResetProviderSlot(tx *gorm.DB, command contentreset.Command, lease uuid.UUID, pageID uuid.UUID) (*models.ContentResetProviderRelease, bool, error) {
	campaign, revision, err := contentreset.LockOwnerCommand(tx, command, lease)
	if err != nil {
		return nil, false, err
	}
	if campaign.State != "executing" || campaign.Operation != "fresh_start" {
		return nil, false, contentreset.ErrFenced
	}
	if command.Contract != (contentreset.Contract{Owner: "cms/source-run", Effect: "release_provider_slot", TargetType: "source_branch", Version: "v1"}) || pageID == uuid.Nil ||
		contentreset.Hash(command.Parameters) != contentreset.Hash(map[string]uuid.UUID{"page_id": pageID}) {
		return nil, false, errors.New("invalid provider release command")
	}
	if !tx.Migrator().HasTable(&models.ContentResetProviderRelease{}) {
		return nil, false, contentreset.ErrUnavailable
	}
	branchID, err := uuid.Parse(command.TargetID)
	if err != nil {
		return nil, false, err
	}
	var branch models.ContentResetReplayBranch
	if err := tx.Where("tenant_id=? AND public_id=? AND campaign_id=? AND revision_id=? AND manifest_hash=?", campaign.TenantID, branchID, campaign.ID, revision.ID, command.ManifestHash).First(&branch).Error; err != nil {
		return nil, false, err
	}
	var page models.ContentResetReplayPage
	if err := tx.Where("tenant_id=? AND public_id=? AND branch_id=?", campaign.TenantID, pageID, branchID).First(&page).Error; err != nil {
		return nil, false, err
	}
	// Match source admission lock order before checkpoint/request settlement.
	var source models.ContentSource
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", campaign.TenantID, branch.ContentSourceID).First(&source).Error; err != nil {
		return nil, false, err
	}
	var request models.SourceRunRequest
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", campaign.TenantID, page.SourceRunRequestID).First(&request).Error; err != nil {
		return nil, false, err
	}
	if _, err := validateContentResetReplayRequest(tx, request, true); err != nil {
		return nil, false, err
	}
	var attempts []models.SourceRunAttempt
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND source_run_request_id=?", campaign.TenantID, request.PublicID).Limit(2).Find(&attempts).Error; err != nil {
		return nil, false, err
	}
	if len(attempts) != 1 {
		return nil, false, errors.New("replay page has no unique provider attempt")
	}
	attempt := attempts[0]
	var existing models.ContentResetProviderRelease
	if err := tx.Where("tenant_id=? AND page_id=?", campaign.TenantID, pageID).First(&existing).Error; err == nil {
		if err := validateProviderReleaseReadback(existing, command, page, request, attempt); err != nil {
			return nil, false, err
		}
		return &existing, false, nil
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, false, err
	}
	checkpoint, err := RecordContentResetReplayCheckpoint(tx, page)
	if err != nil {
		return nil, false, err
	}
	var units []models.SourceRunExecutionUnit
	if err := tx.Where("tenant_id=? AND source_run_request_id=?", campaign.TenantID, request.PublicID).Order("public_id ASC").Limit(103).Find(&units).Error; err != nil {
		return nil, false, err
	}
	nativeTerminal, err := validateReplayProviderGraph(request, attempt, units)
	if err != nil {
		return nil, false, err
	}
	if nativeTerminal {
		return nil, true, nil
	}
	if request.ProviderEffectsReleasedAt != nil || attempt.ProviderEffectsReleasedAt != nil {
		return nil, false, errors.New("provider release stamp lacks its immutable proof")
	}
	settled, err := requestProjectionsSettled(tx, campaign.TenantID, request.PublicID)
	if err != nil {
		return nil, false, err
	}
	if !settled {
		return nil, false, ErrReplayYield
	}
	ids := make([]string, 0, len(units))
	for _, unit := range units {
		ids = append(ids, unit.PublicID.String())
	}
	sort.Strings(ids)
	proof, _ := json.Marshal(map[string]any{"version": "content-reset-provider-release/v1", "branch_id": branchID, "page_id": pageID, "checkpoint_id": checkpoint.PublicID, "request_id": request.PublicID, "attempt_id": attempt.PublicID, "attempt_fence_token": attempt.FenceToken, "unit_ids": ids, "manifest_version": request.ManifestVersion, "provider_effects_settled": true, "delivery_verified": false})
	releasedAt := time.Now().UTC().Truncate(time.Microsecond)
	release := models.ContentResetProviderRelease{PublicID: uuid.New(), TenantID: campaign.TenantID, PageID: pageID, SourceRunRequestID: request.PublicID, SourceRunAttemptID: attempt.PublicID, CommandID: command.CommandID, Proof: proof, ProofHash: contentreset.Hash(json.RawMessage(proof)), ReleasedAt: releasedAt}
	if err := tx.Create(&release).Error; err != nil {
		return nil, false, err
	}
	result := tx.Table("source_run_attempts").Where("tenant_id=? AND public_id=? AND state=? AND provider_effects_released_at IS NULL", campaign.TenantID, attempt.PublicID, string(AttemptVerificationRequired)).Updates(map[string]any{"provider_effects_released_at": releasedAt, "updated_at": releasedAt})
	if result.Error != nil {
		return nil, false, result.Error
	}
	if result.RowsAffected != 1 {
		return nil, false, errors.New("provider attempt changed during release")
	}
	result = tx.Table("source_run_requests").Where("tenant_id=? AND public_id=? AND state=? AND provider_effects_released_at IS NULL", campaign.TenantID, request.PublicID, string(RequestVerificationRequired)).Updates(map[string]any{"provider_effects_released_at": releasedAt, "updated_at": releasedAt})
	if result.Error != nil {
		return nil, false, result.Error
	}
	if result.RowsAffected != 1 {
		return nil, false, errors.New("provider request changed during release")
	}
	return &release, false, nil
}

// ReadContentResetProviderRelease serializes with the entire SQL owner effect.
// Absence after this lock proves no release committed.
func ReadContentResetProviderRelease(tx *gorm.DB, command contentreset.Command, pageID uuid.UUID) (*models.ContentResetProviderRelease, bool, error) {
	var campaign models.ContentResetCampaign
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", command.TenantID, command.CampaignID).First(&campaign).Error; err != nil {
		return nil, false, err
	}
	var revision models.ContentResetRevision
	if err := tx.Where("tenant_id=? AND campaign_id=? AND public_id=? AND revision=?", command.TenantID, campaign.ID, command.RevisionID, campaign.CurrentRevision).First(&revision).Error; err != nil {
		return nil, false, err
	}
	if revision.ManifestHash == nil || *revision.ManifestHash != command.ManifestHash {
		return nil, false, contentreset.ErrFenced
	}
	branchID, err := uuid.Parse(command.TargetID)
	if err != nil {
		return nil, false, err
	}
	var branch models.ContentResetReplayBranch
	if err := tx.Where("tenant_id=? AND public_id=? AND campaign_id=? AND revision_id=? AND manifest_hash=?", command.TenantID, branchID, campaign.ID, revision.ID, command.ManifestHash).First(&branch).Error; err != nil {
		return nil, false, err
	}
	var page models.ContentResetReplayPage
	if err := tx.Where("tenant_id=? AND public_id=? AND branch_id=?", command.TenantID, pageID, branchID).First(&page).Error; err != nil {
		return nil, false, err
	}
	var request models.SourceRunRequest
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", command.TenantID, page.SourceRunRequestID).First(&request).Error; err != nil {
		return nil, false, err
	}
	var attempts []models.SourceRunAttempt
	if err := tx.Where("tenant_id=? AND source_run_request_id=?", command.TenantID, request.PublicID).Limit(2).Find(&attempts).Error; err != nil {
		return nil, false, err
	}
	if len(attempts) != 1 {
		return nil, false, errors.New("provider release readback attempt ambiguous")
	}
	attempt := attempts[0]
	var release models.ContentResetProviderRelease
	if err := tx.Where("tenant_id=? AND page_id=? AND command_id=?", command.TenantID, pageID, command.CommandID).First(&release).Error; err == nil {
		if err := validateProviderReleaseReadback(release, command, page, request, attempt); err != nil {
			return nil, false, err
		}
		return &release, false, nil
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, false, err
	}
	if request.ProviderEffectsReleasedAt != nil || attempt.ProviderEffectsReleasedAt != nil {
		return nil, false, errors.New("provider release stamps lack their owner receipt")
	}
	if request.State == string(RequestSucceeded) {
		var units []models.SourceRunExecutionUnit
		if err := tx.Where("tenant_id=? AND source_run_request_id=?", command.TenantID, request.PublicID).Limit(103).Find(&units).Error; err != nil {
			return nil, false, err
		}
		nativeTerminal, err := validateReplayProviderGraph(request, attempt, units)
		if err != nil {
			return nil, false, err
		}
		if nativeTerminal {
			// A terminal native request alone does not prove a complete bounded
			// replay observation. Readback must match the execution prerequisite.
			var checkpoint models.ContentResetReplayCheckpoint
			if err := tx.Where("tenant_id=? AND page_id=?", command.TenantID, pageID).First(&checkpoint).Error; err != nil {
				return nil, false, errors.New("native terminal replay lacks its observation checkpoint")
			}
			if err := validateReplayCheckpoint(checkpoint, page); err != nil {
				return nil, false, err
			}
			settled, err := requestProjectionsSettled(tx, command.TenantID, request.PublicID)
			if err != nil {
				return nil, false, err
			}
			if !settled {
				return nil, false, ErrReplayYield
			}
			return nil, true, nil
		}
	}
	return nil, false, gorm.ErrRecordNotFound
}

func validateProviderReleaseReadback(release models.ContentResetProviderRelease, command contentreset.Command, page models.ContentResetReplayPage, request models.SourceRunRequest, attempt models.SourceRunAttempt) error {
	if release.PublicID == uuid.Nil || release.TenantID != command.TenantID || page.TenantID != command.TenantID || request.TenantID != command.TenantID || attempt.TenantID != command.TenantID ||
		release.PageID != page.PublicID || release.CommandID != command.CommandID || page.SourceRunRequestID != request.PublicID ||
		release.SourceRunRequestID != request.PublicID || release.SourceRunAttemptID != attempt.PublicID || attempt.SourceRunRequestID != request.PublicID ||
		release.ProofHash == "" || release.ProofHash != contentreset.Hash(release.Proof) || request.ProviderEffectsReleasedAt == nil || attempt.ProviderEffectsReleasedAt == nil ||
		!request.ProviderEffectsReleasedAt.Equal(release.ReleasedAt) || !attempt.ProviderEffectsReleasedAt.Equal(release.ReleasedAt) {
		return errors.New("provider release readback integrity failure")
	}
	return nil
}
