package supply

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"content-management-system/src/contentreset"
	"content-management-system/src/models"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	ReplayPageContractVersion = "content-reset-page/v1"
	ReplayMaxPages            = 100
	ReplayMaxPageItems        = 100
	ReplayMaxPageBytes        = 8 << 20
)

var ErrReplayYield = errors.New("replay must yield to live or unresolved source work")
var ErrReplayEvidenceInvalid = errors.New("replay provider evidence is invalid")

type ReplaySpec struct {
	Version          string     `json:"version"`
	ProviderContract string     `json:"providerContract"`
	SourceType       string     `json:"sourceType"`
	ConfigVersion    int64      `json:"configVersion"`
	ConfigHash       string     `json:"configHash"`
	Mode             string     `json:"mode"`
	WindowStart      *time.Time `json:"windowStart,omitempty"`
	WindowEnd        time.Time  `json:"windowEnd"`
	MaxPages         int        `json:"maxPages"`
	MaxItems         int        `json:"maxItems"`
	MaxBytes         int64      `json:"maxBytes"`
}

// ReplayPageContext travels through the fenced dispatcher, fetch and normalize
// jobs. Opaque cursors are private execution data, never sortable checkpoints.
type ReplayPageContext struct {
	BranchID    uuid.UUID  `json:"branchId"`
	PageID      uuid.UUID  `json:"pageId"`
	Ordinal     int        `json:"ordinal"`
	SpecHash    string     `json:"specHash"`
	InputCursor string     `json:"inputCursor"`
	Spec        ReplaySpec `json:"spec"`
}

func replayConfigHash(source models.ContentSource) string {
	return contentreset.Hash(struct {
		Type   models.SourceType
		Lane   string
		URL    *string
		Config datatypes.JSON
	}{source.Type, source.Category, source.FeedURL, source.APIConfig})
}

func (spec ReplaySpec) Validate() error {
	if spec.Version != ReplayPageContractVersion || spec.ConfigVersion < 1 || !isDigest(spec.ConfigHash) || strings.ToLower(spec.ConfigHash) != spec.ConfigHash ||
		spec.MaxPages < 1 || spec.MaxPages > ReplayMaxPages || spec.MaxItems < 1 || spec.MaxItems > ReplayMaxPageItems ||
		spec.MaxBytes < 1 || spec.MaxBytes > ReplayMaxPageBytes || spec.WindowEnd.IsZero() {
		return errors.New("replay spec is outside its bounded contract")
	}
	// These adapters perform a single listing operation. A general observation
	// capability does not authorize RSS article scraping or YouTube multi-call
	// filtering as a bounded provider page.
	if (spec.SourceType != "PODCAST" && spec.SourceType != "REDDIT") || spec.ProviderContract != spec.SourceType+":reset-page/v1" {
		return errors.New("provider has no installed bounded replay-page contract")
	}
	switch spec.Mode {
	case "bounded_recent":
		if spec.WindowStart == nil || !spec.WindowStart.Before(spec.WindowEnd) || spec.WindowEnd.Sub(*spec.WindowStart) > 365*24*time.Hour {
			return errors.New("replay date interval is invalid")
		}
	case "available_history":
		if spec.WindowStart != nil {
			return errors.New("available-history replay cannot claim a date lower bound")
		}
	default:
		return errors.New("replay mode requires a separately installed boundary or exact-refetch contract")
	}
	return nil
}

func (page ReplayPageContext) Validate() error {
	if page.BranchID == uuid.Nil || page.PageID == uuid.Nil || page.Ordinal < 1 || page.Ordinal > page.Spec.MaxPages ||
		page.SpecHash != contentreset.Hash(page.Spec) || len(page.InputCursor) > 4096 || page.Ordinal == 1 && page.InputCursor != "" || page.Ordinal > 1 && page.InputCursor == "" {
		return errors.New("replay page binding is invalid")
	}
	return page.Spec.Validate()
}

// CreateContentResetReplayBranch is internal owner work, not an admin request.
// Scope, dates and source versions come from the approved immutable revision.
func CreateContentResetReplayBranch(tx *gorm.DB, command contentreset.Command, lease uuid.UUID) (models.ContentResetReplayBranch, error) {
	campaign, revision, err := contentreset.LockOwnerCommand(tx, command, lease)
	if err != nil {
		return models.ContentResetReplayBranch{}, err
	}
	if command.Contract != (contentreset.Contract{Owner: "cms/source-run", Effect: "prepare_replay_branch", TargetType: "source", Version: "v1"}) || campaign.Operation != "fresh_start" || contentreset.Hash(command.Parameters) != contentreset.Hash(json.RawMessage(`{}`)) {
		return models.ContentResetReplayBranch{}, errors.New("invalid replay branch command")
	}
	sourceID, err := uuid.Parse(command.TargetID)
	if err != nil {
		return models.ContentResetReplayBranch{}, err
	}
	var snapshot []struct {
		ID            uuid.UUID         `json:"id"`
		Category      string            `json:"category"`
		Type          models.SourceType `json:"type"`
		ConfigVersion int64             `json:"config_version"`
	}
	if json.Unmarshal(revision.ReplaySourceSnapshot, &snapshot) != nil || contentreset.Hash(snapshot) != revision.ReplaySourceHash {
		return models.ContentResetReplayBranch{}, errors.New("replay source snapshot is invalid")
	}
	var source models.ContentSource
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=? AND is_active=TRUE", campaign.TenantID, sourceID).First(&source).Error; err != nil {
		return models.ContentResetReplayBranch{}, err
	}
	selected := false
	for _, frozen := range snapshot {
		if frozen.ID == sourceID && frozen.Category == source.Category && frozen.Type == source.Type && frozen.ConfigVersion == source.SourceConfigVersion {
			selected = true
			break
		}
	}
	if !selected {
		return models.ContentResetReplayBranch{}, errors.New("source is outside the frozen replay scope or changed configuration")
	}
	var intent struct {
		Replay struct {
			Mode       string `json:"mode"`
			WindowDays int    `json:"window_days"`
		} `json:"replay"`
	}
	if json.Unmarshal(revision.Request, &intent) != nil {
		return models.ContentResetReplayBranch{}, errors.New("invalid frozen replay intent")
	}
	spec := ReplaySpec{Version: ReplayPageContractVersion, ProviderContract: string(source.Type) + ":reset-page/v1", SourceType: string(source.Type), ConfigVersion: source.SourceConfigVersion, ConfigHash: replayConfigHash(source), Mode: intent.Replay.Mode, WindowEnd: revision.PlanningStartedAt.UTC(), MaxPages: ReplayMaxPages, MaxItems: ReplayMaxPageItems, MaxBytes: ReplayMaxPageBytes}
	if spec.Mode == "bounded_recent" {
		start := spec.WindowEnd.Add(-time.Duration(intent.Replay.WindowDays) * 24 * time.Hour)
		spec.WindowStart = &start
	}
	if err := spec.Validate(); err != nil {
		return models.ContentResetReplayBranch{}, err
	}
	var existing models.ContentResetReplayBranch
	if err := tx.Where("tenant_id=? AND revision_id=? AND content_source_id=?", campaign.TenantID, revision.ID, sourceID).First(&existing).Error; err == nil {
		if existing.SpecHash != contentreset.Hash(spec) || existing.ManifestHash != command.ManifestHash {
			return existing, errors.New("replay branch idempotency conflict")
		}
		return existing, nil
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return existing, err
	}
	raw, _ := json.Marshal(spec)
	branch := models.ContentResetReplayBranch{PublicID: uuid.New(), TenantID: campaign.TenantID, CampaignID: campaign.ID, RevisionID: revision.ID, ContentSourceID: sourceID, ManifestHash: command.ManifestHash, Spec: raw, SpecHash: contentreset.Hash(spec)}
	err = tx.Create(&branch).Error
	return branch, err
}

// ValidateContentResetReplayRequest checks durable branch identity at every
// execution admission. Old metadata-only replay requests are quarantined.
// It reads campaign authority without reversing the source-run lock order.
func ValidateContentResetReplayRequest(tx *gorm.DB, request models.SourceRunRequest) (ReplayPageContext, error) {
	return validateContentResetReplayRequest(tx, request, false)
}

func validateContentResetReplayRequest(tx *gorm.DB, request models.SourceRunRequest, settlement bool) (ReplayPageContext, error) {
	var metadata struct {
		CampaignID   uuid.UUID         `json:"content_reset_campaign_id"`
		RevisionID   uuid.UUID         `json:"content_reset_revision_id"`
		ManifestHash string            `json:"content_reset_manifest_hash"`
		Page         ReplayPageContext `json:"content_reset_replay"`
	}
	if request.Purpose != "content_reset_replay" || json.Unmarshal(request.Metadata, &metadata) != nil {
		return metadata.Page, errors.New("invalid replay request")
	}
	if err := metadata.Page.Validate(); err != nil {
		return metadata.Page, err
	}
	var branch models.ContentResetReplayBranch
	if err := tx.Where("tenant_id=? AND public_id=? AND content_source_id=?", request.TenantID, metadata.Page.BranchID, request.ContentSourceID).First(&branch).Error; err != nil {
		return metadata.Page, err
	}
	var spec ReplaySpec
	if json.Unmarshal(branch.Spec, &spec) != nil || branch.SpecHash != contentreset.Hash(spec) || branch.SpecHash != metadata.Page.SpecHash || branch.ManifestHash != metadata.ManifestHash {
		return metadata.Page, errors.New("replay branch spec changed")
	}
	var authority int64
	if err := tx.Table("content_reset_campaigns c").Joins("JOIN content_reset_revisions v ON v.tenant_id=c.tenant_id AND v.campaign_id=c.id AND v.revision=c.current_revision").Joins("JOIN content_reset_executions e ON e.tenant_id=c.tenant_id AND e.campaign_id=c.id AND e.revision_id=v.id").Where("c.tenant_id=? AND c.id=? AND c.public_id=? AND (c.state='executing' OR (c.state='partial' AND ?)) AND c.operation='fresh_start' AND v.id=? AND v.public_id=? AND v.manifest_hash=? AND e.manifest_hash=v.manifest_hash AND e.started_at IS NOT NULL AND e.published_at IS NULL AND (e.pause_requested=FALSE OR ?)", request.TenantID, branch.CampaignID, metadata.CampaignID, settlement, branch.RevisionID, metadata.RevisionID, metadata.ManifestHash, settlement).Count(&authority).Error; err != nil {
		return metadata.Page, err
	}
	if authority != 1 || request.OperatorPlanID == nil || *request.OperatorPlanID != metadata.CampaignID || request.ItemCap != spec.MaxItems || request.ByteCap != spec.MaxBytes || request.ProviderCallCap != 1 {
		return metadata.Page, errors.New("replay campaign authority or page budget changed")
	}
	var run models.ContentResetExecution
	if err := tx.Where("tenant_id=? AND campaign_id=? AND revision_id=?", request.TenantID, branch.CampaignID, branch.RevisionID).First(&run).Error; err != nil {
		return metadata.Page, err
	}
	if err := contentreset.ValidateExecutionEnvironment(tx, run); err != nil {
		return metadata.Page, err
	}
	var page models.ContentResetReplayPage
	if err := tx.Where("tenant_id=? AND public_id=? AND branch_id=? AND source_run_request_id=?", request.TenantID, metadata.Page.PageID, branch.PublicID, request.PublicID).First(&page).Error; err != nil {
		return metadata.Page, err
	}
	if page.Ordinal != metadata.Page.Ordinal || page.InputCursor != metadata.Page.InputCursor || page.InputCursorHash != contentreset.Hash(page.InputCursor) {
		return metadata.Page, errors.New("replay page identity changed")
	}
	var source models.ContentSource
	if err := tx.Where("tenant_id=? AND public_id=? AND is_active=TRUE", request.TenantID, branch.ContentSourceID).First(&source).Error; err != nil {
		return metadata.Page, err
	}
	if source.SourceConfigVersion != spec.ConfigVersion || replayConfigHash(source) != spec.ConfigHash || source.Category != request.Lane {
		return metadata.Page, errors.New("replay source configuration changed")
	}
	return metadata.Page, nil
}

// AdmitContentResetReplayPage reserves a single native source-run request.
// A source slot stays held until native completion or an immutable provider
// release proof; downstream delivery remains independently bounded.
func AdmitContentResetReplayPage(tx *gorm.DB, command contentreset.Command, lease uuid.UUID, ordinal int) (models.ContentResetReplayPage, error) {
	if err := validateReplayPageCommand(command, ordinal); err != nil {
		return models.ContentResetReplayPage{}, err
	}
	campaign, revision, err := contentreset.LockOwnerCommand(tx, command, lease)
	if err != nil {
		return models.ContentResetReplayPage{}, err
	}
	if campaign.Operation != "fresh_start" || campaign.State != "executing" {
		return models.ContentResetReplayPage{}, contentreset.ErrFenced
	}
	branchID, err := uuid.Parse(command.TargetID)
	if err != nil {
		return models.ContentResetReplayPage{}, err
	}
	var branch models.ContentResetReplayBranch
	if err := tx.Where("tenant_id=? AND public_id=? AND campaign_id=? AND revision_id=? AND manifest_hash=?", campaign.TenantID, branchID, campaign.ID, revision.ID, command.ManifestHash).First(&branch).Error; err != nil {
		return models.ContentResetReplayPage{}, err
	}
	var spec ReplaySpec
	if json.Unmarshal(branch.Spec, &spec) != nil || spec.Validate() != nil || branch.SpecHash != contentreset.Hash(spec) || ordinal < 1 || ordinal > spec.MaxPages {
		return models.ContentResetReplayPage{}, errors.New("replay branch bounds invalid or exhausted")
	}
	// Source admission and checkpoint settlement use source -> request lock
	// order. Retain the source lock while reading the preceding checkpoint.
	var source models.ContentSource
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=? AND is_active=TRUE", campaign.TenantID, branch.ContentSourceID).First(&source).Error; err != nil {
		return models.ContentResetReplayPage{}, err
	}
	if source.SourceConfigVersion != spec.ConfigVersion || replayConfigHash(source) != spec.ConfigHash {
		return models.ContentResetReplayPage{}, errors.New("replay source config changed")
	}
	var existing models.ContentResetReplayPage
	if err := tx.Where("tenant_id=? AND branch_id=? AND ordinal=?", campaign.TenantID, branchID, ordinal).First(&existing).Error; err == nil {
		return existing, nil
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return existing, err
	}
	cursor := ""
	if ordinal > 1 {
		var previous models.ContentResetReplayPage
		if err := tx.Where("tenant_id=? AND branch_id=? AND ordinal=?", campaign.TenantID, branchID, ordinal-1).First(&previous).Error; err != nil {
			return existing, err
		}
		checkpoint, err := RecordContentResetReplayCheckpoint(tx, previous)
		if err != nil {
			return existing, err
		}
		if checkpoint.Exhausted {
			return existing, errors.New("replay provider observation is already exhausted")
		}
		cursor = checkpoint.NextCursor
	}
	active, err := CountSourceAdmissionBlockers(tx, campaign.TenantID, source.PublicID)
	if err != nil {
		return existing, err
	}
	if active > 0 || source.NextDueAt == nil || !source.NextDueAt.After(time.Now().UTC()) {
		return existing, ErrReplayYield
	}
	// The campaign lock serializes replay admission; leave native tenant
	// reservation headroom for ordinary source work.
	if err := tx.Model(&models.SourceRunRequest{}).Where("tenant_id=? AND purpose='content_reset_replay' AND state IN ? AND provider_effects_released_at IS NULL", campaign.TenantID, models.SourceRunActiveStates).Count(&active).Error; err != nil {
		return existing, err
	}
	if active >= 2 {
		return existing, ErrReplayYield
	}
	page := models.ContentResetReplayPage{PublicID: uuid.New(), TenantID: campaign.TenantID, BranchID: branchID, Ordinal: ordinal, InputCursor: cursor, InputCursorHash: contentreset.Hash(cursor)}
	context := ReplayPageContext{BranchID: branchID, PageID: page.PublicID, Ordinal: ordinal, SpecHash: branch.SpecHash, InputCursor: cursor, Spec: spec}
	if err := context.Validate(); err != nil {
		return existing, err
	}
	metadata, _ := json.Marshal(map[string]any{"schema_version": ContractVersion, "content_reset_campaign_id": campaign.PublicID, "content_reset_revision_id": revision.PublicID, "content_reset_manifest_hash": command.ManifestHash, "content_reset_replay": context, "max_results": spec.MaxItems, "max_bytes": spec.MaxBytes, "max_provider_calls": 1})
	request, created, err := CreateRequest(tx, CreateRequestInput{replayPageAdmission: true, Source: source, RequestedBy: "system", RequestedByActorID: campaign.CreatedBy, OperatorPlanID: &campaign.PublicID, OperatorStepID: &command.CommandID, Identity: RequestIdentity{TenantID: campaign.TenantID, ContentSourceID: source.PublicID.String(), Lane: source.Category, Purpose: "content_reset_replay", CadenceWindowStart: spec.WindowEnd, SourceConfigVersion: spec.ConfigVersion, PolicyFingerprint: branch.SpecHash, ArgumentFingerprint: contentreset.Hash(context), PlanStepToolTarget: fmt.Sprintf("%s/%d", branchID, ordinal)}, Metadata: metadata, EvidenceFingerprint: command.ManifestHash})
	if err != nil {
		return existing, err
	}
	if !created {
		return existing, errors.New("replay request exists without its atomic page binding")
	}
	page.SourceRunRequestID = request.PublicID
	err = tx.Create(&page).Error
	return page, err
}

func validateReplayPageCommand(command contentreset.Command, ordinal int) error {
	if command.Contract != (contentreset.Contract{Owner: "cms/source-run", Effect: "replay_page", TargetType: "source_branch", Version: "v1"}) ||
		ordinal < 1 || ordinal > ReplayMaxPages || contentreset.Hash(command.Parameters) != contentreset.Hash(map[string]int{"ordinal": ordinal}) {
		return errors.New("replay page command must bind its exact ordinal")
	}
	branch, err := uuid.Parse(command.TargetID)
	if err != nil || branch == uuid.Nil {
		return errors.New("invalid replay page command target")
	}
	return nil
}

// RecordContentResetReplayCheckpoint accepts only complete, reducer-settled
// native provider evidence. It cannot promote staged content or prove delivery.
func RecordContentResetReplayCheckpoint(tx *gorm.DB, page models.ContentResetReplayPage) (models.ContentResetReplayCheckpoint, error) {
	var existing models.ContentResetReplayCheckpoint
	// Never take request/cursor identity from a caller-supplied page struct.
	var persisted models.ContentResetReplayPage
	if err := tx.Where("tenant_id=? AND public_id=?", page.TenantID, page.PublicID).First(&persisted).Error; err != nil {
		return existing, err
	}
	if persisted.BranchID != page.BranchID || persisted.SourceRunRequestID != page.SourceRunRequestID || persisted.Ordinal != page.Ordinal ||
		persisted.InputCursor != page.InputCursor || persisted.InputCursorHash != page.InputCursorHash || persisted.InputCursorHash != contentreset.Hash(persisted.InputCursor) {
		return existing, errors.New("replay checkpoint page identity mismatch")
	}
	if err := tx.Where("tenant_id=? AND page_id=?", page.TenantID, page.PublicID).First(&existing).Error; err == nil {
		if err := validateReplayCheckpoint(existing, page); err != nil {
			return existing, err
		}
		return existing, nil
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return existing, err
	}
	var branch models.ContentResetReplayBranch
	if err := tx.Where("tenant_id=? AND public_id=?", page.TenantID, page.BranchID).First(&branch).Error; err != nil {
		return existing, err
	}
	var source models.ContentSource
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", page.TenantID, branch.ContentSourceID).First(&source).Error; err != nil {
		return existing, err
	}
	var request models.SourceRunRequest
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND public_id=?", page.TenantID, page.SourceRunRequestID).First(&request).Error; err != nil {
		return existing, err
	}
	context, err := validateContentResetReplayRequest(tx, request, true)
	if err != nil {
		return existing, err
	}
	if context.PageID != page.PublicID || request.ManifestState != string(ManifestSealed) || request.ExpectedPageCount != 1 {
		return existing, errors.New("replay page manifest not sealed exactly")
	}
	settled, err := requestProjectionsSettled(tx, page.TenantID, request.PublicID)
	if err != nil {
		return existing, err
	}
	if !settled {
		return existing, ErrReplayYield
	}
	var receipts []models.SourceRunReceipt
	if err := tx.Where("tenant_id=? AND source_run_request_id=? AND stage='fetch' AND event_type='provider_terminal'", page.TenantID, request.PublicID).Limit(2).Find(&receipts).Error; err != nil {
		return existing, err
	}
	if len(receipts) != 1 {
		return existing, fmt.Errorf("%w: replay page lacks a unique provider terminal receipt", ErrReplayEvidenceInvalid)
	}
	receipt := receipts[0]
	if !receipt.FinalPage || receipt.PageID != "initial" || receipt.Outcome != string(OutcomeNewItems) && receipt.Outcome != string(OutcomeNoChange) {
		return existing, fmt.Errorf("%w: replay page is partial or uncertain", ErrReplayEvidenceInvalid)
	}
	var payload struct {
		Replay *struct {
			BranchID      uuid.UUID `json:"branch_id"`
			PageID        uuid.UUID `json:"page_id"`
			SpecHash      string    `json:"spec_hash"`
			Complete      bool      `json:"complete"`
			Observed      int       `json:"observed"`
			Admitted      int       `json:"admitted"`
			OutsideWindow int       `json:"outside_window"`
			ObservedBytes int64     `json:"observed_bytes"`
			NextCursor    string    `json:"next_cursor"`
			Exhausted     bool      `json:"exhausted"`
		} `json:"content_reset_replay"`
	}
	if json.Unmarshal(receipt.Payload, &payload) != nil || payload.Replay == nil {
		return existing, fmt.Errorf("%w: replay page receipt lacks bounded observation proof", ErrReplayEvidenceInvalid)
	}
	proof := payload.Replay
	if !proof.Complete || proof.BranchID != context.BranchID || proof.PageID != page.PublicID || proof.SpecHash != context.SpecHash || proof.Observed < 0 || proof.Observed > context.Spec.MaxItems || proof.Admitted < 0 || proof.OutsideWindow < 0 || proof.Observed != proof.Admitted+proof.OutsideWindow || proof.ObservedBytes < 0 || proof.ObservedBytes > context.Spec.MaxBytes || len(proof.NextCursor) > 4096 || proof.Exhausted != (proof.NextCursor == "") || !proof.Exhausted && proof.NextCursor == context.InputCursor {
		return existing, fmt.Errorf("%w: replay observation proof is invalid", ErrReplayEvidenceInvalid)
	}
	if proof.NextCursor != "" {
		var seen int64
		if err := tx.Model(&models.ContentResetReplayPage{}).Where("tenant_id=? AND branch_id=? AND input_cursor_hash=?", page.TenantID, page.BranchID, contentreset.Hash(proof.NextCursor)).Count(&seen).Error; err != nil {
			return existing, err
		}
		if seen > 0 {
			return existing, fmt.Errorf("%w: replay cursor cycle detected", ErrReplayEvidenceInvalid)
		}
	}
	checkpoint := models.ContentResetReplayCheckpoint{PublicID: uuid.New(), TenantID: page.TenantID, PageID: page.PublicID, ProviderReceiptID: receipt.PublicID, NextCursor: proof.NextCursor, NextCursorHash: contentreset.Hash(proof.NextCursor), Exhausted: proof.Exhausted, Observed: proof.Observed, Admitted: proof.Admitted, OutsideWindow: proof.OutsideWindow, ObservedBytes: proof.ObservedBytes}
	err = tx.Create(&checkpoint).Error
	return checkpoint, err
}

func validateReplayCheckpoint(checkpoint models.ContentResetReplayCheckpoint, page models.ContentResetReplayPage) error {
	if checkpoint.PublicID == uuid.Nil || checkpoint.TenantID != page.TenantID || checkpoint.PageID != page.PublicID || checkpoint.ProviderReceiptID == uuid.Nil ||
		checkpoint.NextCursorHash != contentreset.Hash(checkpoint.NextCursor) || len(checkpoint.NextCursor) > 4096 || checkpoint.Exhausted != (checkpoint.NextCursor == "") ||
		!checkpoint.Exhausted && checkpoint.NextCursor == page.InputCursor || checkpoint.Observed < 0 || checkpoint.Observed > ReplayMaxPageItems ||
		checkpoint.Admitted < 0 || checkpoint.OutsideWindow < 0 || checkpoint.Observed != checkpoint.Admitted+checkpoint.OutsideWindow ||
		checkpoint.ObservedBytes < 0 || checkpoint.ObservedBytes > ReplayMaxPageBytes {
		return errors.New("replay checkpoint integrity failure")
	}
	return nil
}

func validateReplayUnitAdmission(tx *gorm.DB, unit models.SourceRunExecutionUnit) error {
	var request models.SourceRunRequest
	if err := tx.Where("tenant_id=? AND public_id=?", unit.TenantID, unit.SourceRunRequestID).First(&request).Error; err != nil {
		return err
	}
	if request.Purpose != "content_reset_replay" {
		return nil
	}
	if request.ProviderEffectsReleasedAt != nil {
		return errors.New("released replay request cannot begin another effect")
	}
	if _, err := ValidateContentResetReplayRequest(tx, request); err != nil {
		return err
	}
	if unit.UnitType == "fetch_page" && (unit.PageID != "initial" || unit.UnitKey != "fetch:initial") {
		return errors.New("replay unit exceeds its single-page contract")
	}
	return nil
}
