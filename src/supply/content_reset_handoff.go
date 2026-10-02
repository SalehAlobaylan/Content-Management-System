package supply

import (
	"encoding/json"
	"errors"
	"time"

	"content-management-system/src/contentreset"
	"content-management-system/src/models"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrReplayHandoffPending = errors.New("replay branch has unsettled provider evidence")
	// ErrReplayCompletedEarly marks a branch whose continuation exists after a
	// page already proved provider exhaustion: the branch graph is invalid.
	ErrReplayCompletedEarly = errors.New("replay branch has pages after an exhausted checkpoint")
)

// HandoffContentResetReplayBranch is the only path that returns a replay
// branch's source to ordinary live scheduling. It requires an exhausted branch
// where every page either carries an immutable provider release or is natively
// terminal, and the source has no other active provider effect. It advances the
// live next-due time to now so the following live poll creates the deliberate,
// identity-deduplicated overlap; it never copies an opaque replay cursor.
func HandoffContentResetReplayBranch(tx *gorm.DB, command contentreset.Command, lease uuid.UUID) (models.ContentResetReplayHandoff, error) {
	var handoff models.ContentResetReplayHandoff
	if tx == nil || command.Contract != (contentreset.Contract{Owner: "cms/source-run", Effect: "replay_and_handoff", TargetType: "source_branch", Version: "v1"}) ||
		contentreset.Hash(command.Parameters) != contentreset.Hash(json.RawMessage(`{}`)) {
		return handoff, errors.New("invalid replay handoff command")
	}
	if !tx.Migrator().HasTable(&models.ContentResetReplayHandoff{}) {
		return handoff, contentreset.ErrUnavailable
	}
	branchID, err := uuid.Parse(command.TargetID)
	if err != nil || branchID == uuid.Nil {
		return handoff, errors.New("invalid replay handoff branch")
	}
	campaign, revision, err := contentreset.LockOwnerCommand(tx, command, lease)
	if err != nil {
		return handoff, err
	}
	if campaign.Operation != "fresh_start" || (campaign.Lane != "news" && campaign.Lane != "pods" && campaign.Lane != "both") {
		return handoff, contentreset.ErrUnavailable
	}
	var branch models.ContentResetReplayBranch
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("tenant_id=? AND public_id=? AND campaign_id=? AND revision_id=? AND manifest_hash=?", command.TenantID, branchID, campaign.ID, revision.ID, command.ManifestHash).
		First(&branch).Error; err != nil {
		return handoff, err
	}
	var spec ReplaySpec
	if json.Unmarshal(branch.Spec, &spec) != nil || spec.Validate() != nil || contentreset.Hash(spec) != branch.SpecHash {
		return handoff, errors.New("replay branch spec integrity failure")
	}
	existing, err := readHandoffRow(tx, command.TenantID, branchID)
	if err == nil {
		if existing.CampaignID != campaign.ID || existing.RevisionID != revision.ID || existing.SpecHash != branch.SpecHash || existing.ContentSourceID != branch.ContentSourceID {
			return handoff, errors.New("replay handoff binding changed")
		}
		return existing, nil
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return handoff, err
	}
	var pages []models.ContentResetReplayPage
	if err := tx.Where("tenant_id=? AND branch_id=?", command.TenantID, branchID).Order("ordinal ASC").Find(&pages).Error; err != nil {
		return handoff, err
	}
	if len(pages) == 0 || pages[0].Ordinal != 1 {
		return handoff, ErrReplayHandoffPending
	}
	observedUntil := time.Time{}
	for index, page := range pages {
		if page.Ordinal != index+1 {
			return handoff, ErrReplayHandoffPending
		}
		var checkpoint models.ContentResetReplayCheckpoint
		if err := tx.Where("tenant_id=? AND page_id=?", command.TenantID, page.PublicID).First(&checkpoint).Error; err != nil {
			return handoff, ErrReplayHandoffPending
		}
		if err := replayPageContinuationValid(index, len(pages), checkpoint.Exhausted); err != nil {
			return handoff, err
		}
		settled, err := replayPageProviderSettled(tx, command.TenantID, page)
		if err != nil {
			return handoff, err
		}
		if !settled {
			return handoff, ErrReplayHandoffPending
		}
		if checkpoint.CreatedAt.After(observedUntil) {
			observedUntil = checkpoint.CreatedAt
		}
	}
	if observedUntil.IsZero() {
		return handoff, ErrReplayHandoffPending
	}
	if !spec.WindowEnd.IsZero() && observedUntil.After(spec.WindowEnd.Add(24*time.Hour)) {
		return handoff, errors.New("replay observation is outside the approved window")
	}
	// The provider slot must be quiet. Released replay requests are already
	// excluded by the blocker count; a live poll or an unsettled replay attempt
	// keeps this owner deferred instead of advancing the live schedule.
	blockers, err := CountSourceAdmissionBlockers(tx, command.TenantID, branch.ContentSourceID)
	if err != nil {
		return handoff, err
	}
	if blockers != 0 {
		return handoff, ErrReplayYield
	}
	var source models.ContentSource
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", command.TenantID, branch.ContentSourceID).First(&source).Error; err != nil {
		return handoff, err
	}
	if !source.IsActive {
		return handoff, errors.New("replay handoff requires an active source")
	}
	if source.SourceConfigVersion != spec.ConfigVersion {
		return handoff, errors.New("replay handoff source configuration changed")
	}
	now := time.Now().UTC()
	handoff = models.ContentResetReplayHandoff{
		PublicID: uuid.New(), TenantID: command.TenantID, CampaignID: campaign.ID, RevisionID: revision.ID,
		BranchID: branch.PublicID, ContentSourceID: branch.ContentSourceID, Pages: len(pages),
		ObservedUntil: observedUntil, SpecHash: branch.SpecHash, CommandID: command.CommandID, HandedOffAt: now,
	}
	if err := tx.Create(&handoff).Error; err != nil {
		return handoff, err
	}
	updates := map[string]any{
		"next_due_at":               now,
		"failure_streak":            0,
		"intake_circuit_until":      nil,
		"last_upstream_observed_at": observedUntil,
	}
	if err := tx.Model(&source).Updates(updates).Error; err != nil {
		return handoff, err
	}
	return handoff, nil
}

// ReadContentResetReplayHandoff serializes with the full owner effect through
// the campaign lock and returns the immutable row if it committed.
func ReadContentResetReplayHandoff(tx *gorm.DB, command contentreset.Command) (models.ContentResetReplayHandoff, error) {
	var handoff models.ContentResetReplayHandoff
	if tx == nil || command.Contract != (contentreset.Contract{Owner: "cms/source-run", Effect: "replay_and_handoff", TargetType: "source_branch", Version: "v1"}) {
		return handoff, contentreset.ErrUnavailable
	}
	if !tx.Migrator().HasTable(&models.ContentResetReplayHandoff{}) {
		return handoff, contentreset.ErrUnavailable
	}
	branchID, err := uuid.Parse(command.TargetID)
	if err != nil || branchID == uuid.Nil {
		return handoff, errors.New("invalid replay handoff branch")
	}
	var campaign models.ContentResetCampaign
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", command.TenantID, command.CampaignID).First(&campaign).Error; err != nil {
		return handoff, err
	}
	var revision models.ContentResetRevision
	if err := tx.Where("tenant_id=? AND campaign_id=? AND public_id=? AND revision=?", command.TenantID, campaign.ID, command.RevisionID, campaign.CurrentRevision).First(&revision).Error; err != nil {
		return handoff, err
	}
	handoff, err = readHandoffRow(tx, command.TenantID, branchID)
	if err != nil {
		return handoff, err
	}
	if handoff.CampaignID != campaign.ID || handoff.RevisionID != revision.ID || handoff.CommandID != command.CommandID {
		return handoff, errors.New("replay handoff binding changed")
	}
	return handoff, nil
}

func readHandoffRow(tx *gorm.DB, tenant string, branchID uuid.UUID) (models.ContentResetReplayHandoff, error) {
	var handoff models.ContentResetReplayHandoff
	err := tx.Where("tenant_id=? AND branch_id=?", tenant, branchID).First(&handoff).Error
	return handoff, err
}

// replayPageContinuationValid encodes the branch continuation rule: every
// non-final page must be a non-terminal listing whose next page exists, and
// only the final page may prove provider exhaustion.
func replayPageContinuationValid(index, total int, exhausted bool) error {
	if index < total-1 {
		if exhausted {
			return ErrReplayCompletedEarly
		}
		return nil
	}
	if !exhausted {
		return ErrReplayHandoffPending
	}
	return nil
}

func replayPageProviderSettled(tx *gorm.DB, tenant string, page models.ContentResetReplayPage) (bool, error) {
	var releases int64
	if err := tx.Model(&models.ContentResetProviderRelease{}).Where("tenant_id=? AND page_id=?", tenant, page.PublicID).Count(&releases).Error; err != nil {
		return false, err
	}
	if releases == 1 {
		return true, nil
	}
	if releases > 1 {
		return false, errors.New("replay page has multiple provider releases")
	}
	var request models.SourceRunRequest
	if err := tx.Where("tenant_id=? AND public_id=?", tenant, page.SourceRunRequestID).First(&request).Error; err != nil {
		return false, err
	}
	return request.State == string(RequestSucceeded) && request.VerifiedAt != nil, nil
}
