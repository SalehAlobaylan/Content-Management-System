package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"net/http"
	"strings"
	"sync"
	"time"

	"content-management-system/src/models"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Media Studio Clearance Autopilot — stage 6, Slice 2 (deterministic runner).
//
// One run = preconditions → before-snapshot → collect review cases → classify
// (safe-clear / propose / hold) → execute the safe tier through the SAME apply
// path humans use (applyAtomizedChapterReview / triggerTranscription) under
// guardrails → after-snapshot + summary. Observe suppresses the terminal side
// effect and writes would_apply / would_skip rows (G9).
//
// The lead/helper doctrine (H1): this helper never atomizes and never chains
// back into Circulation; re-atomize desires are emitted as recommendations into
// the lead's ledger (Slice 3). See docs/media-studio-autopilot-plan.md.

const studioAutopilotMaxCaseScan = 300

var errStudioAutopilotDisabled = errors.New("media studio autopilot is not enabled for this tenant")
var errStudioAutopilotAlreadyRunning = errors.New("media studio autopilot is already running for this tenant")
var errStudioAutopilotPaused = errors.New("media studio autopilot is paused for this tenant")

var (
	studioAutopilotRunMu       sync.Mutex
	studioAutopilotRunInFlight = map[string]bool{}
)

func tryStartStudioRun(tenantID string) bool {
	studioAutopilotRunMu.Lock()
	defer studioAutopilotRunMu.Unlock()
	if studioAutopilotRunInFlight[tenantID] {
		return false
	}
	studioAutopilotRunInFlight[tenantID] = true
	return true
}

func finishStudioRunLock(tenantID string) {
	studioAutopilotRunMu.Lock()
	defer studioAutopilotRunMu.Unlock()
	delete(studioAutopilotRunInFlight, tenantID)
}

// tryAcquireStudioAutopilotLock keeps a PostgreSQL session advisory lock for a
// tenant's entire run. Unlike the in-process mutex, this survives replica
// boundaries; closing the owning session releases the lock after a crash.
func tryAcquireStudioAutopilotLock(db *gorm.DB, tenantID string) (func(), bool) {
	sqlDB, err := db.DB()
	if err != nil {
		return func() {}, false
	}
	conn, err := sqlDB.Conn(context.Background())
	if err != nil {
		return func() {}, false
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte("media-studio-autopilot/v1/" + tenantID))
	key := int64(h.Sum64())
	var acquired bool
	if err := conn.QueryRowContext(context.Background(), "SELECT pg_try_advisory_lock($1)", key).Scan(&acquired); err != nil || !acquired {
		_ = conn.Close()
		return func() {}, false
	}
	return func() {
		_, _ = conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", key)
		_ = conn.Close()
	}, true
}

type studioAutopilotRunOptions struct {
	Trigger   string
	CreatedBy string
}

// ---------------------------------------------------------------
// Health snapshot (studio-tab read-model at a point in time, G10)
// ---------------------------------------------------------------

type studioHealthSnapshot struct {
	ReviewQueueDepth     int            `json:"review_queue_depth"`
	AgedCount            int            `json:"aged_count"`
	OldestCaseAgeHours   float64        `json:"oldest_case_age_hours"`
	ByCode               map[string]int `json:"by_code"`
	TranscriptAutoRepair int            `json:"transcript_auto_repair"`
	Headline             string         `json:"headline"`
}

func collectStudioHealth(db *gorm.DB, tenantID string, agedThresholdDays int) (studioHealthSnapshot, error) {
	snap := studioHealthSnapshot{ByCode: map[string]int{}}
	now := time.Now().UTC()
	agedCutoff := now.Add(-time.Duration(agedThresholdDays) * 24 * time.Hour)
	base := db.Model(&models.Chapter{}).
		Where("tenant_id = ? AND status = ? AND child_content_item_id IS NOT NULL", tenantID, chapterStatusReview)
	var aggregate struct {
		Depth  int64
		Aged   int64
		Oldest *time.Time
	}
	if err := base.Select("COUNT(*) AS depth, COUNT(*) FILTER (WHERE created_at < ?) AS aged, MIN(created_at) AS oldest", agedCutoff).
		Scan(&aggregate).Error; err != nil {
		return snap, err
	}
	snap.ReviewQueueDepth = int(aggregate.Depth)
	snap.AgedCount = int(aggregate.Aged)
	if aggregate.Oldest != nil {
		snap.OldestCaseAgeHours = now.Sub(*aggregate.Oldest).Hours()
	}
	var codeRows []struct {
		Code string
		N    int64
	}
	if err := base.Select("COALESCE(NULLIF(needs_review_code, ''), 'unclassified') AS code, COUNT(*) AS n").
		Group("COALESCE(NULLIF(needs_review_code, ''), 'unclassified')").Scan(&codeRows).Error; err != nil {
		return snap, err
	}
	for _, row := range codeRows {
		snap.ByCode[row.Code] = int(row.N)
	}

	var autoRepair int64
	if err := db.Model(&models.TranscriptQuality{}).
		Where("tenant_id = ? AND status = ?", tenantID, models.TranscriptQualityAutoRepair).
		Count(&autoRepair).Error; err != nil {
		return snap, err
	}
	snap.TranscriptAutoRepair = int(autoRepair)

	snap.Headline = studioHealthHeadline(snap)
	return snap, nil
}

func studioHealthHeadline(s studioHealthSnapshot) string {
	switch {
	case s.ReviewQueueDepth == 0:
		return "clear"
	case s.ReviewQueueDepth <= 10 && s.AgedCount == 0:
		return "manageable"
	default:
		return "backlog"
	}
}

// ---------------------------------------------------------------
// Runner
// ---------------------------------------------------------------

func runMediaStudioAutopilot(db *gorm.DB, tenantID string, opts studioAutopilotRunOptions) (models.MediaStudioRun, []models.MediaStudioAction, error) {
	if strings.TrimSpace(tenantID) == "" {
		tenantID = defaultCirculationTenant
	}
	if !tryStartStudioRun(tenantID) {
		return models.MediaStudioRun{}, nil, errStudioAutopilotAlreadyRunning
	}
	defer finishStudioRunLock(tenantID)
	releaseLock, acquired := tryAcquireStudioAutopilotLock(db, tenantID)
	if !acquired {
		return models.MediaStudioRun{}, nil, errStudioAutopilotAlreadyRunning
	}
	defer releaseLock()
	var activeRun *models.MediaStudioRun
	defer func() {
		if recovered := recover(); recovered != nil {
			if activeRun != nil {
				if err := finishStudioRun(db, activeRun, models.StudioRunStatusFailed,
					"Studio runner panicked; operator reconciliation required.", nil, fmt.Sprint(recovered)); err != nil {
					activeRun.Error = fmt.Sprintf("panic: %v; failure persistence also failed: %v", recovered, err)
				}
			}
			panic(recovered)
		}
	}()

	policy := loadEffectiveMediaStudioAutopilotPolicy(db, tenantID)
	if !policy.AutopilotEnabled {
		return models.MediaStudioRun{}, nil, errStudioAutopilotDisabled
	}
	// Pause gates ALL triggers, including manual (S7).
	if policy.PausedUntil != nil && policy.PausedUntil.After(time.Now().UTC()) {
		return models.MediaStudioRun{}, nil, errStudioAutopilotPaused
	}
	// Advisory locks release when a replica dies. Any row left running by that
	// crash is history, not live ownership; recover it before opening a new run.
	if err := recoverAbandonedStudioRuns(db, tenantID); err != nil {
		return models.MediaStudioRun{}, nil, fmt.Errorf("recover abandoned Studio runs: %w", err)
	}

	trigger := strings.TrimSpace(opts.Trigger)
	if trigger == "" {
		trigger = models.StudioRunTriggerInterval
	}
	observe := policy.AutopilotMode != models.StudioAutopilotModeSafeAuto
	createdBy := opts.CreatedBy
	if createdBy == "" {
		createdBy = "automation"
	}

	// Create the durable run before reading the snapshot. If schema or query
	// access has failed, operators still receive an explicit failed run rather
	// than a misleading clear/completed result.
	run, err := startStudioRun(db, tenantID, trigger, policy.AutopilotMode, createdBy, nil)
	if err != nil {
		return models.MediaStudioRun{}, nil, err
	}
	activeRun = run
	healthBefore, err := collectStudioHealth(db, tenantID, policy.AgedThresholdDays)
	if err != nil {
		finishErr := finishStudioRun(db, run, models.StudioRunStatusFailed, "Studio health snapshot unavailable.", nil, err.Error())
		return *run, nil, errors.Join(err, finishErr)
	}
	if raw, marshalErr := json.Marshal(healthBefore); marshalErr == nil {
		run.HealthBefore = datatypes.JSON(raw)
		if err := db.Save(run).Error; err != nil {
			return *run, nil, err
		}
	}

	rn := &studioRunner{
		db:       db,
		run:      run,
		tenantID: tenantID,
		policy:   policy,
		observe:  observe,
	}

	if err := rn.processChapterCases(); err != nil {
		rn.fatalErr = fmt.Errorf("collect chapter review cases: %w", err)
	}
	if rn.fatalErr == nil {
		if err := rn.processTranscriptCases(); err != nil {
			rn.fatalErr = fmt.Errorf("collect transcript repair cases: %w", err)
		}
	}
	if rn.fatalErr == nil {
		rn.runProposalPhase()
	}

	healthAfter, healthErr := collectStudioHealth(db, tenantID, policy.AgedThresholdDays)
	status := models.StudioRunStatusCompleted
	runErr := ""
	if rn.fatalErr != nil {
		status = models.StudioRunStatusFailed
		runErr = rn.fatalErr.Error()
	} else if healthErr != nil {
		status = models.StudioRunStatusFailed
		runErr = healthErr.Error()
	} else if rn.errorCount > 0 {
		status = models.StudioRunStatusPartial
	}
	summary := rn.summary(healthBefore, healthAfter)
	if healthErr != nil {
		summary = "Studio completion health snapshot unavailable."
	}
	if err := finishStudioRun(db, run, status, summary, healthAfter, runErr); err != nil {
		return *run, nil, fmt.Errorf("finish Studio run: %w", err)
	}
	if err := touchStudioLastRun(db, tenantID, time.Now().UTC()); err != nil {
		return *run, nil, fmt.Errorf("checkpoint Studio run: %w", err)
	}

	var actions []models.MediaStudioAction
	if err := db.Where("run_id = ?", run.ID).Order("started_at ASC, id ASC").Find(&actions).Error; err != nil {
		return *run, nil, fmt.Errorf("load Studio actions: %w", err)
	}
	if rn.fatalErr != nil {
		return *run, actions, rn.fatalErr
	}
	return *run, actions, healthErr
}

func recoverAbandonedStudioRuns(db *gorm.DB, tenantID string) error {
	now := time.Now().UTC()
	return db.Model(&models.MediaStudioRun{}).
		Where("tenant_id = ? AND status = ?", tenantID, "running").
		Updates(map[string]interface{}{
			"status": models.StudioRunStatusFailed, "finished_at": now,
			"error": "Recovered after Studio runner ownership was lost.",
		}).Error
}

func touchStudioLastRun(db *gorm.DB, tenantID string, at time.Time) error {
	return db.Model(&models.MediaStudioAutopilotPolicy{}).
		Where("tenant_id = ?", tenantID).
		Update("last_run_at", at).Error
}

// ---------------------------------------------------------------
// Runner state + case processing
// ---------------------------------------------------------------

type studioRunner struct {
	db          *gorm.DB
	run         *models.MediaStudioRun
	tenantID    string
	policy      models.MediaStudioAutopilotPolicy
	observe     bool
	clears      int
	publishes   int
	rejects     int
	sttRuns     int
	errorCount  int
	approvalCnt int
	fatalErr    error

	proposalQueue []studioProposalCase
}

// record is the side-effect boundary for runner ledger writes. A missing
// ledger row after an apply is an integrity incident: stop processing rather
// than retrying the action or reporting a completed run.
func (r *studioRunner) record(in studioActionInput) *models.MediaStudioAction {
	if r.fatalErr != nil {
		return nil
	}
	action, err := recordStudioAction(r.db, r.run, in)
	if err != nil {
		r.fatalErr = fmt.Errorf("persist Studio action: %w", err)
	}
	return action
}

// studioProposalCase pairs an approval-tier ledger row with the chapter data the
// LLM needs to draft a proposal (Slice 4).
type studioProposalCase struct {
	ActionID uint
	Chapter  models.Chapter
	Child    models.ContentItem
}

// terminalStatus maps a would-execute outcome to the mode-correct status: in
// Observe every apply becomes would_apply; every guardrail skip becomes
// would_skip (§12 — same rows, only the terminal execute flips).
func (r *studioRunner) applyStatus() string {
	if r.observe {
		return models.StudioActionStatusWouldApply
	}
	return models.StudioActionStatusSuccess
}

func (r *studioRunner) skipStatus() string {
	if r.observe {
		return models.StudioActionStatusWouldSkip
	}
	return models.StudioActionStatusSkipped
}

func (r *studioRunner) processChapterCases() error {
	var chapters []models.Chapter
	if err := r.db.Where("tenant_id = ? AND status = ? AND child_content_item_id IS NOT NULL", r.tenantID, chapterStatusReview).
		Order("created_at ASC").Limit(studioAutopilotMaxCaseScan).Find(&chapters).Error; err != nil {
		return err
	}

	for i := range chapters {
		r.classifyChapter(&chapters[i])
		if r.fatalErr != nil {
			break
		}
	}
	return nil
}

func (r *studioRunner) classifyChapter(ch *models.Chapter) {
	if r.fatalErr != nil {
		return
	}
	chID := ch.PublicID
	var childID *uuid.UUID
	if ch.ChildContentItemID != nil {
		childID = ch.ChildContentItemID
	}
	base := studioActionInput{
		UnitType:      models.StudioUnitChapterReview,
		ChapterID:     &chID,
		ContentItemID: childID,
	}

	// Load child for duration + override + stale checks.
	var child models.ContentItem
	if childID != nil {
		if err := r.db.Where("public_id = ? AND tenant_id = ?", *childID, r.tenantID).First(&child).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				r.emitHoldStale(base, ch)
				return
			}
			r.fatalErr = fmt.Errorf("load Studio chapter child: %w", err)
			return
		}
	}

	// hold_stale: child already archived/hidden while chapter lingers in review.
	if child.ID != 0 && child.Status == models.ContentStatusArchived {
		r.emitHoldStale(base, ch)
		return
	}

	// Overrides consulted before classification (§9).
	blockingOverride, err := r.hasBlockingOverride(&child)
	if err != nil {
		r.fatalErr = fmt.Errorf("load Studio overrides: %w", err)
		return
	}
	if blockingOverride {
		base.Verdict = models.StudioVerdictProposeReject
		base.ToolName = "chapter.review"
		base.Status = models.StudioActionStatusApprovalRequired
		base.Guardrail = models.StudioGuardOverride
		base.Reason = "Item has a standing editorial override; human-only."
		r.approvalCnt++
		r.record(base)
		return
	}

	// Dirty-workbench courtesy guard (S6): a human is actively editing the parent.
	recentlyEdited, err := r.recentlyEdited(&child)
	if err != nil {
		r.fatalErr = fmt.Errorf("load Studio recent edits: %w", err)
		return
	}
	if recentlyEdited {
		base.Verdict = models.StudioVerdictProposeReject
		base.ToolName = "chapter.review"
		base.Status = r.skipStatus()
		base.Guardrail = models.StudioGuardRecentlyEdited
		base.Reason = "Parent recently edited in the studio; deferring."
		r.record(base)
		return
	}

	codes := []string(ch.NeedsReviewCodes)
	primary := ""
	if ch.NeedsReviewCode != nil {
		primary = *ch.NeedsReviewCode
	}
	childDur := 0
	if child.DurationSec != nil {
		childDur = *child.DurationSec
	}

	switch decideStudioChapterPath(primary, codes, childDur) {
	// auto_reject_impossible — structural, not trust-gated (S3).
	case studioPathAutoReject:
		r.emitAutoReject(base, ch)

	// auto_publish_mechanical — trust-gated, single-code merged_short only (S16).
	case studioPathAutoPublish:
		r.emitAutoPublishCandidate(base, ch, &child)

	// Everything editorial or multi-code → approval tier (proposals in Slice 4).
	default:
		r.emitProposalTier(base, ch, &child, len(codes) == 1)
	}
}

// studioChapterPath is the pure classification of a review chapter into which
// tier the runner should route it. Runtime concerns (mode, caps, overrides,
// recent edits, trust, upstream flag) are applied by the emit* methods; this
// captures only the S2/S3/S5/S16 code-shape decision so it is unit-testable.
type studioChapterPath int

const (
	studioPathApproval studioChapterPath = iota
	studioPathAutoReject
	studioPathAutoPublish
)

func decideStudioChapterPath(primary string, codes []string, childDurationSec int) studioChapterPath {
	single := len(codes) == 1
	switch {
	case primary == models.StudioReviewCodeShortUnmergeable && single && childDurationSec < forYouMinDurationSec:
		return studioPathAutoReject
	case primary == models.StudioReviewCodeMergedShort && single:
		return studioPathAutoPublish
	default:
		return studioPathApproval
	}
}

func (r *studioRunner) emitHoldStale(base studioActionInput, ch *models.Chapter) {
	base.Verdict = models.StudioVerdictHoldStale
	base.ToolName = "chapter.hold_stale"
	base.Reason = "Child superseded/archived; clearing stale review case."
	if r.observe {
		base.Status = models.StudioActionStatusWouldApply
		r.record(base)
		return
	}
	// Guarded conditional cleanup: only if still needs_review.
	res := r.db.Model(&models.Chapter{}).
		Where("public_id = ? AND tenant_id = ? AND status = ?", ch.PublicID, r.tenantID, chapterStatusReview).
		Update("status", chapterStatusRejected)
	if res.Error != nil {
		base.Status = models.StudioActionStatusError
		base.Err = res.Error.Error()
		r.errorCount++
	} else if res.RowsAffected == 0 {
		base.Status = models.StudioActionStatusSkipped
		base.Guardrail = models.StudioGuardStaleness
	} else {
		base.Status = models.StudioActionStatusSuccess
	}
	r.record(base)
}

func (r *studioRunner) emitAutoReject(base studioActionInput, ch *models.Chapter) {
	base.Verdict = models.StudioVerdictAutoRejectImpossible
	base.ToolName = "chapter.reject"
	base.Reason = "Chapter below 4:30 and cannot merge — structurally unpublishable."

	if r.clears >= r.policy.MaxClearsPerRun || r.rejects >= r.policy.MaxRejectsPerRun {
		base.Status = r.skipStatus()
		base.Guardrail = models.StudioGuardRejectLimit
		r.record(base)
		return
	}
	if r.observe {
		base.Status = models.StudioActionStatusWouldApply
		r.record(base)
		return
	}
	out, revErr := applyAtomizedChapterReviewWithOptions(r.db, r.tenantID, ch.PublicID, false,
		chapterReviewActor{UserID: "", Email: models.StudioAuditPrincipal}, chapterReviewApplyOptions{
			RequireNeedsReview:        true,
			ExpectedChildID:           ch.ChildContentItemID,
			ExpectedReviewCodes:       []string{models.StudioReviewCodeShortUnmergeable},
			RequireNoBlockingOverride: true,
		})
	r.finalizeApply(&base, out, revErr, false)
}

func (r *studioRunner) emitAutoPublishCandidate(base studioActionInput, ch *models.Chapter, child *models.ContentItem) {
	base.Verdict = models.StudioVerdictAutoPublishMechanical
	base.ToolName = "chapter.publish"

	// Publication authority follows the chapter's parent, not a tenant-wide
	// snapshot. A missing parent is held for a human; a valid parent is resolved
	// through the canonical tenant → source → episode policy resolver.
	upstreamAutoPublish, parentResolved := studioParentAutoPublishPolicy(r.db, r.tenantID, child)
	if !parentResolved {
		base.Status = models.StudioActionStatusApprovalRequired
		base.Guardrail = models.StudioGuardParentContext
		base.Reason = "Chapter parent context is unavailable; publish requires human approval."
		r.approvalCnt++
		r.record(base)
		return
	}
	if !upstreamAutoPublish {
		base.Status = models.StudioActionStatusApprovalRequired
		base.Guardrail = models.StudioGuardUpstreamDisabled
		base.Reason = "Upstream auto-publish is disabled; publish requires human approval."
		r.approvalCnt++
		r.record(base)
		return
	}
	// Trust gate (H5): category must be earned.
	if !studioReasonCodeTrustEarned(r.db, r.tenantID, models.StudioReviewCodeMergedShort, r.policy) {
		base.Status = models.StudioActionStatusApprovalRequired
		base.Guardrail = models.StudioGuardTrustGate
		base.Reason = "merged_short has not yet earned auto-publish trust."
		r.approvalCnt++
		r.record(base)
		return
	}
	// Cap check.
	if r.clears >= r.policy.MaxClearsPerRun || r.publishes >= r.policy.MaxPublishesPerRun {
		base.Status = r.skipStatus()
		base.Guardrail = models.StudioGuardPublishLimit
		r.record(base)
		return
	}
	base.Reason = "merged_short earned trust; invariants pass."
	if r.observe {
		base.Status = models.StudioActionStatusWouldApply
		r.record(base)
		return
	}
	out, revErr := applyAtomizedChapterReviewWithOptions(r.db, r.tenantID, ch.PublicID, true,
		chapterReviewActor{UserID: "", Email: models.StudioAuditPrincipal}, chapterReviewApplyOptions{
			RequireNeedsReview:        true,
			RequireParentAutoPublish:  true,
			ExpectedChildID:           ch.ChildContentItemID,
			ExpectedReviewCodes:       []string{models.StudioReviewCodeMergedShort},
			RequireMergeProvenance:    true,
			RequireNoSponsor:          true,
			RequireNoBlockingOverride: true,
		})
	r.finalizeApply(&base, out, revErr, true)
}

// finalizeApply records the ledger row for an executed clear, mapping the shared
// apply-path result onto the skip taxonomy (S6 staleness, S2 invalid duration).
func (r *studioRunner) finalizeApply(base *studioActionInput, out *chapterReviewOutcome, revErr *chapterReviewError, publish bool) {
	switch {
	case revErr == nil:
		base.Status = models.StudioActionStatusSuccess
		base.FeedImpact = 1
		r.clears++
		if publish {
			r.publishes++
		} else {
			r.rejects++
		}
	case revErr.code == chapterReviewErrStale:
		base.Status = models.StudioActionStatusSkipped
		base.Guardrail = models.StudioGuardStaleness
		base.Reason = revErr.message
	case revErr.code == chapterReviewErrInvalidDuration:
		base.Status = models.StudioActionStatusSkipped
		base.Guardrail = models.StudioGuardInvalidDuration
		base.Reason = revErr.message
	case revErr.code == chapterReviewErrMultiCode:
		base.Status = models.StudioActionStatusSkipped
		base.Guardrail = models.StudioGuardMultiCode
		base.Reason = revErr.message
	case revErr.code == chapterReviewErrOverride:
		base.Status = models.StudioActionStatusSkipped
		base.Guardrail = models.StudioGuardOverride
		base.Reason = revErr.message
	case revErr.code == chapterReviewErrEditorialReason:
		base.Status = models.StudioActionStatusSkipped
		base.Guardrail = models.StudioGuardEditorialReason
		base.Reason = revErr.message
	case revErr.code == chapterReviewErrUpstreamDisabled:
		base.Status = models.StudioActionStatusApprovalRequired
		base.Guardrail = models.StudioGuardUpstreamDisabled
		base.Reason = revErr.message
		r.approvalCnt++
	case revErr.code == chapterReviewErrParentContext:
		base.Status = models.StudioActionStatusApprovalRequired
		base.Guardrail = models.StudioGuardParentContext
		base.Reason = revErr.message
		r.approvalCnt++
	default:
		base.Status = models.StudioActionStatusError
		base.Err = revErr.message
		r.errorCount++
	}
	r.record(*base)
}

// studioParentAutoPublishPolicy resolves the publication flag from the parent
// item. The parent carries the source and episode context used by the canonical
// resolver; looking at the child would silently bypass source-level controls.
func studioParentAutoPublishPolicy(db *gorm.DB, tenantID string, child *models.ContentItem) (bool, bool) {
	if child == nil || child.ID == 0 || child.ParentContentItemID == nil {
		return false, false
	}
	var parent models.ContentItem
	if err := db.Where("public_id = ? AND tenant_id = ?", *child.ParentContentItemID, tenantID).First(&parent).Error; err != nil {
		return false, false
	}
	return atomizationPolicyForItem(db, &parent).AutoPublishHighConfidence, true
}

func (r *studioRunner) emitProposalTier(base studioActionInput, ch *models.Chapter, child *models.ContentItem, single bool) {
	base.ToolName = "chapter.review"
	base.Status = models.StudioActionStatusApprovalRequired
	base.Verdict = models.StudioVerdictProposePublish
	if !single {
		base.Guardrail = models.StudioGuardMultiCode
		base.Reason = "Multiple review flags; compound uncertainty — human decides."
	} else {
		base.Guardrail = models.StudioGuardEditorialReason
		primary := ""
		if ch.NeedsReviewCode != nil {
			primary = *ch.NeedsReviewCode
		}
		base.Reason = fmt.Sprintf("Editorial review reason (%s); human decides.", primary)
	}
	r.approvalCnt++
	action := r.record(base)
	// Queue for the LLM proposal phase (Slice 4). The row exists now; the phase
	// attaches a draft and ranks the case once a proposal comes back.
	if action != nil {
		childCopy := models.ContentItem{}
		if child != nil {
			childCopy = *child
		}
		r.proposalQueue = append(r.proposalQueue, studioProposalCase{
			ActionID: action.ID,
			Chapter:  *ch,
			Child:    childCopy,
		})
	}
}

// ---------------------------------------------------------------
// LLM proposal phase (Slice 4, S10/S13/H2) — Level 2 advisory
// ---------------------------------------------------------------

const studioProposalTranscriptHeadChars = 750
const studioProposalTranscriptTailChars = 750

// runProposalPhase attaches advisory publish/reject proposals to the queued
// approval-tier chapter cases. Observe skips the LLM (zero spend) unless
// observe_proposals is set (S13). Enrichment failure degrades to an unranked
// queue (S10). Containment: invalid proposals and invariant-violating publishes
// are discarded and ledgered llm_invalid_output (S10).
func (r *studioRunner) runProposalPhase() {
	if len(r.proposalQueue) == 0 {
		return
	}
	// Case rows stay approval_required in every mode (stage-5 convention: the
	// approval lane is the human's queue regardless of mode). The proposal PHASE
	// gets its own ledger row, like the lead's tool rows.
	if r.observe && !r.policy.ObserveProposals {
		// Zero spend, zero side effect (S13): one would_propose phase row.
		r.record(studioActionInput{
			UnitType: models.StudioUnitChapterReview,
			Verdict:  "proposal_phase",
			ToolName: "proposals.generate",
			Status:   models.StudioActionStatusWouldPropose,
			Reason:   fmt.Sprintf("%d approval cases queued; LLM drafting skipped in Observe (observe_proposals off).", len(r.proposalQueue)),
		})
		return
	}

	// Bound the batch to the per-run proposal cap (H2). Overflow cases stay
	// approval-tier but are labeled so the ledger explains the missing draft.
	queue := r.proposalQueue
	if len(queue) > r.policy.MaxProposalsPerRun {
		for _, pc := range queue[r.policy.MaxProposalsPerRun:] {
			r.markProposalGuardrail(pc.ActionID, models.StudioGuardProposalLimit, "Per-run proposal cap reached; will draft next run.")
		}
		queue = queue[:r.policy.MaxProposalsPerRun]
	}
	items := make([]studioProposalItem, 0, len(queue))
	for _, pc := range queue {
		items = append(items, r.buildProposalItem(pc))
	}

	proposals, err := generateChapterProposalsViaEnrichment(items)
	if err != nil {
		// Degrade: queue appears unranked, nothing blocked (S10).
		for _, pc := range queue {
			r.markProposalGuardrail(pc.ActionID, models.StudioGuardLLMUnavailable, "Enrichment unavailable; case left unranked.")
		}
		r.record(studioActionInput{
			UnitType:  models.StudioUnitChapterReview,
			Verdict:   "proposal_phase",
			ToolName:  "proposals.generate",
			Status:    models.StudioActionStatusError,
			Guardrail: models.StudioGuardLLMUnavailable,
			Reason:    "Enrichment unavailable; approval queue left unranked.",
			Err:       err.Error(),
		})
		return
	}

	attached, discarded := 0, 0
	for _, pc := range queue {
		p, ok := proposals[pc.Chapter.PublicID.String()]
		if !ok {
			r.markProposalGuardrail(pc.ActionID, models.StudioGuardLLMInvalidOutput, "No valid proposal produced.")
			discarded++
			continue
		}
		// Containment (S10): re-check invariants in code; a publish proposal on a
		// structurally-invalid chapter is discarded, never rendered.
		if p.Proposal == "publish" && !r.childDurationValid(&pc.Child) {
			r.markProposalGuardrail(pc.ActionID, models.StudioGuardLLMInvalidOutput, "Proposal violates duration invariant; discarded.")
			discarded++
			continue
		}
		r.attachProposal(pc.ActionID, p)
		attached++
	}
	r.record(studioActionInput{
		UnitType: models.StudioUnitChapterReview,
		Verdict:  "proposal_phase",
		ToolName: "proposals.generate",
		Status:   models.StudioActionStatusSuccess,
		Reason:   fmt.Sprintf("Drafted %d proposals (%d discarded) for %d queued cases.", attached, discarded, len(r.proposalQueue)),
	})
}

func (r *studioRunner) buildProposalItem(pc studioProposalCase) studioProposalItem {
	reason := ""
	if pc.Chapter.NeedsReviewReason != nil {
		reason = *pc.Chapter.NeedsReviewReason
	}
	code := ""
	if pc.Chapter.NeedsReviewCode != nil {
		code = *pc.Chapter.NeedsReviewCode
	}
	summary := ""
	if pc.Chapter.Summary != nil {
		summary = *pc.Chapter.Summary
	}
	item := studioProposalItem{
		ID:              pc.Chapter.PublicID.String(),
		Transcript:      r.childTranscriptSlice(&pc.Child),
		Title:           pc.Chapter.Title,
		Summary:         summary,
		ReviewReason:    reason,
		ReviewCode:      code,
		Confidence:      pc.Chapter.Confidence,
		StandaloneScore: pc.Chapter.StandaloneScore,
		ContainsSponsor: pc.Chapter.ContainsSponsorIntro,
		DurationSec:     pc.Child.DurationSec,
	}
	if pc.Child.ParentContentItemID != nil {
		var parent models.ContentItem
		if err := r.db.Select("title").Where("public_id = ?", *pc.Child.ParentContentItemID).First(&parent).Error; err == nil && parent.Title != nil {
			item.ParentTitle = *parent.Title
		}
	}
	return item
}

// childTranscriptSlice returns the head + tail of the child's transcript, the
// deciding evidence for coherence, bounded to the token budget (H2).
func (r *studioRunner) childTranscriptSlice(child *models.ContentItem) string {
	if child == nil || child.TranscriptID == nil {
		return ""
	}
	var t models.Transcript
	if err := r.db.Select("full_text").Where("public_id = ?", *child.TranscriptID).First(&t).Error; err != nil {
		return ""
	}
	text := strings.TrimSpace(t.FullText)
	if len(text) <= studioProposalTranscriptHeadChars+studioProposalTranscriptTailChars {
		return text
	}
	head := text[:studioProposalTranscriptHeadChars]
	tail := text[len(text)-studioProposalTranscriptTailChars:]
	return head + "\n…\n" + tail
}

func (r *studioRunner) childDurationValid(child *models.ContentItem) bool {
	return child != nil && child.DurationSec != nil &&
		*child.DurationSec >= forYouMinDurationSec && *child.DurationSec <= forYouHardMaxDurationSec
}

func (r *studioRunner) attachProposal(actionID uint, p studioProposal) {
	verdict := models.StudioVerdictProposePublish
	if p.Proposal == "reject" {
		verdict = models.StudioVerdictProposeReject
	}
	raw, _ := json.Marshal(p)
	conf := p.Confidence
	r.db.Model(&models.MediaStudioAction{}).Where("id = ?", actionID).Updates(map[string]interface{}{
		"verdict":             verdict,
		"proposal":            datatypes.JSON(raw),
		"proposal_model":      "enrichment_llm",
		"proposal_confidence": conf,
	})
}

// markProposalGuardrail annotates a case row with the proposal-step outcome,
// preserving the original classification reason (append, don't overwrite).
func (r *studioRunner) markProposalGuardrail(actionID uint, guardrail, note string) {
	r.db.Model(&models.MediaStudioAction{}).Where("id = ?", actionID).Updates(map[string]interface{}{
		"guardrail": guardrail,
		"reason":    gorm.Expr("COALESCE(NULLIF(reason, ''), '') || CASE WHEN COALESCE(reason,'') = '' THEN '' ELSE ' · ' END || ?", note),
	})
}

// ---------------------------------------------------------------
// Transcript cases (V1: rerun_stt only — H3)
// ---------------------------------------------------------------

func (r *studioRunner) processTranscriptCases() error {
	var qualities []models.TranscriptQuality
	if err := r.db.Where("tenant_id = ? AND status = ?", r.tenantID, models.TranscriptQualityAutoRepair).
		Order("computed_at ASC").Limit(studioAutopilotMaxCaseScan).Find(&qualities).Error; err != nil {
		return err
	}

	for i := range qualities {
		r.classifyTranscriptCase(qualities[i])
		if r.fatalErr != nil {
			break
		}
	}
	return nil
}

type studioTranscriptRepairSnapshot struct {
	QualityID     uuid.UUID
	ContentItemID uuid.UUID
	TranscriptID  uuid.UUID
	ObservedState string
}

func studioTranscriptRepairSnapshotFromQuality(quality models.TranscriptQuality) studioTranscriptRepairSnapshot {
	return studioTranscriptRepairSnapshot{
		QualityID: quality.PublicID, ContentItemID: quality.ContentItemID,
		TranscriptID: quality.TranscriptID, ObservedState: quality.Status,
	}
}

var errStudioTranscriptSnapshotStale = errors.New("studio transcript quality snapshot is stale")
var errStudioTranscriptJobInFlight = errors.New("studio transcript repair already has a job in flight")

// classifyTranscriptCase carries the exact quality observation into admission;
// a quality row is not merely a content-id hint because transcription may have
// been replaced or repaired between collection and action.
func (r *studioRunner) classifyTranscriptCase(observed models.TranscriptQuality) {
	snapshot := studioTranscriptRepairSnapshotFromQuality(observed)
	item, snapshotErr := inspectStudioTranscriptRepairSnapshot(r.db, r.tenantID, snapshot)
	if snapshotErr != nil && !errors.Is(snapshotErr, errStudioTranscriptSnapshotStale) && !errors.Is(snapshotErr, errStudioTranscriptJobInFlight) {
		r.fatalErr = fmt.Errorf("inspect Studio transcript snapshot: %w", snapshotErr)
		return
	}
	if snapshotErr != nil && item.ID == 0 {
		return
	}
	cid := item.PublicID
	base := studioActionInput{
		UnitType:      models.StudioUnitTranscriptCase,
		ContentItemID: &cid,
		ToolName:      "transcript.rerun_stt",
	}
	if errors.Is(snapshotErr, errStudioTranscriptSnapshotStale) {
		base.Verdict = models.StudioVerdictRerunSTT
		base.Status = r.skipStatus()
		base.Guardrail = models.StudioGuardStaleness
		base.Reason = "Transcript quality observation is stale; skipping STT admission."
		r.record(base)
		return
	}
	if errors.Is(snapshotErr, errStudioTranscriptJobInFlight) {
		base.Verdict = models.StudioVerdictBlockedJobInFlight
		base.Status = r.skipStatus()
		base.Guardrail = models.StudioGuardJobInFlight
		base.Reason = "A transcription job is already in flight for this item."
		r.record(base)
		return
	}

	base.Verdict = models.StudioVerdictRerunSTT

	if r.sttRuns >= r.policy.MaxSTTPerRun {
		base.Status = r.skipStatus()
		base.Guardrail = models.StudioGuardSTTLimit
		base.Reason = "Per-run STT cap reached."
		r.record(base)
		return
	}
	if r.observe {
		admit, reason := evaluateSTTAdmissionReadOnly(r.db, &item, models.TranscriptionTriggerStudioAutopilot)
		if !admit {
			base.Status = r.skipStatus()
			base.Guardrail = studioSTTGuardrail(reason)
			base.Reason = reason
		} else {
			base.Status = models.StudioActionStatusWouldApply
			base.Reason = "Quality auto_repair; would re-run STT."
		}
		r.record(base)
		return
	}

	// Single choke point (S11): the existing guarded trigger path enforces the
	// monthly STT budget identically to humans; budget cap surfaces as a skip.
	_, err := triggerStudioTranscriptRepairSnapshot(r.db, r.tenantID, snapshot)
	switch {
	case err == nil:
		base.Status = models.StudioActionStatusSuccess
		base.STTImpact = 1
		r.sttRuns++
		base.Reason = "Re-ran STT on auto_repair transcript; re-atomization remains pending verified completion."
		r.record(base)
		return
	case isSTTSkipped(err):
		base.Status = models.StudioActionStatusSkipped
		base.Guardrail = studioSTTGuardrail(err.Error())
		base.Reason = err.Error()
	default:
		base.Status = models.StudioActionStatusError
		base.Err = err.Error()
		r.errorCount++
	}
	r.record(base)
}

func inspectStudioTranscriptRepairSnapshot(db *gorm.DB, tenantID string, snapshot studioTranscriptRepairSnapshot) (models.ContentItem, error) {
	var item models.ContentItem
	if snapshot.ObservedState != models.TranscriptQualityAutoRepair {
		return item, errStudioTranscriptSnapshotStale
	}
	if err := db.Where("public_id = ? AND tenant_id = ?", snapshot.ContentItemID, tenantID).First(&item).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return item, errStudioTranscriptSnapshotStale
		}
		return item, err
	}
	if item.TranscriptID == nil || *item.TranscriptID != snapshot.TranscriptID {
		return item, errStudioTranscriptSnapshotStale
	}
	var quality models.TranscriptQuality
	if err := db.Where("public_id = ? AND tenant_id = ? AND content_item_id = ? AND transcript_id = ? AND status = ?",
		snapshot.QualityID, tenantID, snapshot.ContentItemID, snapshot.TranscriptID, models.TranscriptQualityAutoRepair).First(&quality).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return item, errStudioTranscriptSnapshotStale
		}
		return item, err
	}
	var active int64
	if err := db.Model(&models.TranscriptionJob{}).
		Where("content_item_id = ? AND status IN ?", item.PublicID, []string{models.TranscriptionJobStatusQueued, models.TranscriptionJobStatusRunning}).
		Count(&active).Error; err != nil {
		return item, err
	}
	if active > 0 {
		return item, errStudioTranscriptJobInFlight
	}
	return item, nil
}

func triggerStudioTranscriptRepairSnapshot(db *gorm.DB, tenantID string, snapshot studioTranscriptRepairSnapshot) (string, error) {
	item, job, triggered, reason, kind, err := claimStudioTranscriptRepairSnapshot(db, tenantID, snapshot)
	if err != nil {
		return "", err
	}
	if !triggered {
		return "", &sttSkippedError{reason: reason, kind: kind}
	}
	jobID := job.PublicID.String()
	if err := submitTranscriptionJobToMedia(db, &item, jobID); err != nil {
		return jobID, err
	}
	return jobID, nil
}

// claimStudioTranscriptRepairSnapshot atomically proves that the collected
// quality observation still owns the item, then creates the normal queued STT
// job (the durable claim) through the established admission/budget code.
func claimStudioTranscriptRepairSnapshot(db *gorm.DB, tenantID string, snapshot studioTranscriptRepairSnapshot) (models.ContentItem, models.TranscriptionJob, bool, string, sttSkipKind, error) {
	var item models.ContentItem
	var job models.TranscriptionJob
	var triggered bool
	var reason string
	var kind sttSkipKind
	err := db.Transaction(func(tx *gorm.DB) error {
		var quality models.TranscriptQuality
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("public_id = ? AND tenant_id = ? AND content_item_id = ? AND transcript_id = ? AND status = ?",
				snapshot.QualityID, tenantID, snapshot.ContentItemID, snapshot.TranscriptID, models.TranscriptQualityAutoRepair).First(&quality).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return errStudioTranscriptSnapshotStale
			}
			return err
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("public_id = ? AND tenant_id = ? AND transcript_id = ?", snapshot.ContentItemID, tenantID, snapshot.TranscriptID).First(&item).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return errStudioTranscriptSnapshotStale
			}
			return err
		}
		var active int64
		if err := tx.Model(&models.TranscriptionJob{}).Where("content_item_id = ? AND status IN ?", item.PublicID, []string{models.TranscriptionJobStatusQueued, models.TranscriptionJobStatusRunning}).Count(&active).Error; err != nil {
			return err
		}
		if active > 0 {
			return errStudioTranscriptJobInFlight
		}
		var claimErr error
		job, triggered, reason, kind, claimErr = createTranscriptionJobForItem(tx, &item, models.TranscriptionTriggerStudioAutopilot, false)
		return claimErr
	})
	if err != nil {
		return item, job, false, "", sttSkipNone, err
	}
	return item, job, triggered, reason, kind, nil
}

func studioSTTGuardrail(reason string) string {
	if strings.Contains(strings.ToLower(reason), "budget") {
		return models.StudioGuardBudget
	}
	if strings.Contains(strings.ToLower(reason), "stale") {
		return models.StudioGuardStaleness
	}
	return models.StudioGuardJobInFlight
}

// maybeEmitReatomize emits an atomize_now recommendation into the lead's ledger
// for an atomization-eligible parent whose transcript just improved (H1). In
// Observe it only ledgers the intent (would_apply), no recommendation written.
func (r *studioRunner) maybeEmitReatomize(item *models.ContentItem) {
	if item.DurationSec == nil || *item.DurationSec <= forYouHardMaxDurationSec {
		return // not an atomization-eligible parent
	}
	cid := item.PublicID
	base := studioActionInput{
		UnitType:      models.StudioUnitTranscriptCase,
		ContentItemID: &cid,
		Verdict:       models.StudioVerdictEmitReatomize,
		ToolName:      "transcript.emit_reatomize",
		Reason:        "Transcript improved; recommending re-atomization to the lead.",
	}
	if r.observe {
		base.Status = models.StudioActionStatusWouldApply
		r.record(base)
		return
	}
	rec := emitReatomizeRecommendation(r.db, r.tenantID, item)
	if rec == nil {
		base.Status = models.StudioActionStatusSkipped
		base.Guardrail = models.StudioGuardStaleness
		base.Reason = "No recommendation emitted (ineligible or already pending)."
		r.record(base)
		return
	}
	recID := rec.PublicID
	base.RecommendationID = &recID
	base.Status = models.StudioActionStatusSuccess
	r.record(base)
}

// ---------------------------------------------------------------
// Overrides, recent-edit, summary helpers
// ---------------------------------------------------------------

func (r *studioRunner) hasBlockingOverride(child *models.ContentItem) (bool, error) {
	if child == nil || child.ID == 0 {
		return false, nil
	}
	subjects := []uuid.UUID{child.PublicID}
	if child.ParentContentItemID != nil {
		subjects = append(subjects, *child.ParentContentItemID)
	}
	var count int64
	err := r.db.Model(&models.MediaCirculationOverride{}).
		Where("tenant_id = ? AND subject_id IN ? AND override_type IN ? AND (expires_at IS NULL OR expires_at > ?)",
			r.tenantID, subjects,
			[]string{models.MediaCirculationOverrideEditorialHold, models.MediaCirculationOverrideNoAtomize}, time.Now().UTC()).
		Count(&count).Error
	return count > 0, err
}

func (r *studioRunner) recentlyEdited(child *models.ContentItem) (bool, error) {
	if r.policy.DirtyWorkbenchMinutes <= 0 || child == nil || child.ID == 0 {
		return false, nil
	}
	cutoff := time.Now().UTC().Add(-time.Duration(r.policy.DirtyWorkbenchMinutes) * time.Minute)
	resources := []string{child.PublicID.String()}
	if child.ParentContentItemID != nil {
		resources = append(resources, child.ParentContentItemID.String())
	}
	var count int64
	err := r.db.Model(&models.AuditLog{}).
		Where("tenant_id = ? AND target_resource IN ? AND user_email <> ? AND action IN ? AND created_at > ?",
			r.tenantID, resources, models.StudioAuditPrincipal,
			[]string{"media_studio.chapters_save", "media_studio.transcript_edit", "media_studio.transcript_approve"},
			cutoff).
		Count(&count).Error
	return count > 0, err
}

func (r *studioRunner) summary(before, after studioHealthSnapshot) string {
	parts := []string{
		fmt.Sprintf("%d→%d cases", before.ReviewQueueDepth, after.ReviewQueueDepth),
	}
	if r.publishes > 0 {
		parts = append(parts, fmt.Sprintf("%d published", r.publishes))
	}
	if r.rejects > 0 {
		parts = append(parts, fmt.Sprintf("%d rejected", r.rejects))
	}
	if r.sttRuns > 0 {
		parts = append(parts, fmt.Sprintf("%d STT re-runs", r.sttRuns))
	}
	if r.approvalCnt > 0 {
		parts = append(parts, fmt.Sprintf("%d awaiting approval", r.approvalCnt))
	}
	if after.AgedCount > 0 {
		parts = append(parts, fmt.Sprintf("%d aged", after.AgedCount))
	}
	if r.observe {
		return "Observe: " + strings.Join(parts, ", ")
	}
	return strings.Join(parts, ", ")
}

// ---------------------------------------------------------------
// Admin endpoints
// ---------------------------------------------------------------

func RunMediaStudioAutopilotNow(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	run, actions, err := runMediaStudioAutopilot(db, principal.TenantID, studioAutopilotRunOptions{
		Trigger:   models.StudioRunTriggerManual,
		CreatedBy: principal.Email,
	})
	if err != nil {
		switch {
		case errors.Is(err, errStudioAutopilotDisabled):
			c.JSON(http.StatusConflict, authErrorResponse{Message: err.Error(), Code: "AUTOPILOT_DISABLED"})
		case errors.Is(err, errStudioAutopilotPaused):
			c.JSON(http.StatusConflict, authErrorResponse{Message: err.Error(), Code: "AUTOPILOT_PAUSED"})
		case errors.Is(err, errStudioAutopilotAlreadyRunning):
			c.JSON(http.StatusConflict, authErrorResponse{Message: err.Error(), Code: "AUTOPILOT_ALREADY_RUNNING"})
		default:
			c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Autopilot run failed: " + err.Error(), Code: "RUN_FAILED"})
		}
		return
	}
	writeStudioAutopilotAudit(db, principal, "media_studio.autopilot.run", principal.TenantID, map[string]interface{}{
		"run_id": run.PublicID.String(), "status": run.Status, "summary": run.Summary,
	})
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"run": run, "actions": actions}})
}

func ListMediaStudioAutopilotRuns(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var runs []models.MediaStudioRun
	if err := db.Where("tenant_id = ?", principal.TenantID).
		Order("started_at DESC").Limit(boundedLimit(c.Query("limit"), 20, 100)).
		Find(&runs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to list runs", Code: "QUERY_FAILED"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"items": runs}})
}

func GetMediaStudioAutopilotRun(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	runID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid run ID", Code: "INVALID_ID"})
		return
	}
	var run models.MediaStudioRun
	if err := db.Where("tenant_id = ? AND public_id = ?", principal.TenantID, runID).First(&run).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Run not found", Code: "NOT_FOUND"})
		return
	}
	var actions []models.MediaStudioAction
	_ = db.Where("tenant_id = ? AND run_id = ?", principal.TenantID, run.ID).
		Order("started_at ASC, id ASC").Find(&actions).Error
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"run": run, "actions": actions}})
}
