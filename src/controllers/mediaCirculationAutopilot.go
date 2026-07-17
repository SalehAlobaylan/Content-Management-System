package controllers

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"

	"content-management-system/src/intelligence"
	"content-management-system/src/models"
)

// Media Circulation Autopilot — stage 5, Slice 2 (deterministic runner).
//
// One run = precondition check → before-snapshot → generate recommendations →
// targeted intelligence refresh of evict candidates (G13) → gated selection →
// apply through the SAME applyRecommendation path humans use → bounded storage
// sweep as the storage-execution phase (G4) → after-snapshot + summary.
//
// Observe mode runs the entire pipeline and suppresses only the terminal side
// effect, writing would_apply/would_skip rows (G9). recoverable_delete is hard
// approval-only regardless of mode or trust (G3).
// See docs/media-autopilot-stage5-plan.md.

const (
	mediaAutopilotStalenessWindow = 24 * time.Hour
	mediaAutopilotSetBy           = "autopilot"
	// mediaAutopilotMaxCandidates bounds the pending-recommendation scan: only a
	// few dozen actions can be applied per run, so there is no reason to load a
	// larger backlog. Comfortably above any per-run action cap.
	mediaAutopilotMaxCandidates = 200
)

// mediaRelevantQueueDepth mirrors the News relevantQueueDepth but over the
// media-pipeline queues an Autopilot action would load.
func mediaRelevantQueueDepth(stats []autopilotQueueStat) int {
	relevant := map[string]bool{
		"fetch-queue":             true,
		"normalize-queue":         true,
		"media-queue":             true,
		"ai-queue":                true,
		"atomization-queue":       true,
		"atomization-sweep-queue": true,
		"storage-sweep-queue":     true,
		"quality-reencode-queue":  true,
	}
	total := 0
	for _, stat := range stats {
		if !relevant[stat.Queue] {
			continue
		}
		total += stat.Waiting + stat.Active + stat.Delayed
	}
	return total
}

// ----------------------------------------------------------------
// Verdict classification (pure)
// ----------------------------------------------------------------

// isMediaApprovalTierVerdict: never auto-applied, regardless of mode or trust.
// recoverable_delete is a structural rule (G3), not a threshold.
func isMediaApprovalTierVerdict(verdict string) bool {
	switch verdict {
	case mediaCircVerdictRecoverableDelete, mediaCircVerdictPauseSource, mediaCircVerdictNeedsAdminReview:
		return true
	}
	return false
}

// isMediaEvictValueVerdict: evict actions justified by the intelligence value
// estimate — these require established + fresh + confident scores (G7/G13).
func isMediaEvictValueVerdict(verdict string) bool {
	switch verdict {
	case mediaCircVerdictRankDown, mediaCircVerdictReEncode, mediaCircVerdictMoveToCold:
		return true
	}
	return false
}

// isMediaStorageTierVerdict: two-phase actions executed by the bounded sweep.
func isMediaStorageTierVerdict(verdict string) bool {
	switch verdict {
	case mediaCircVerdictReEncode, mediaCircVerdictMoveToCold:
		return true
	}
	return false
}

// isMediaQueueLoadVerdict: actions that enqueue Aggregation work.
func isMediaQueueLoadVerdict(verdict string) bool {
	switch verdict {
	case mediaCircVerdictPullNow, mediaCircVerdictPullLimited, mediaCircVerdictDeepPull, mediaCircVerdictAtomizeNow:
		return true
	}
	return false
}

func mediaRecByteSize(rec models.MediaCirculationRecommendation) int64 {
	if len(rec.Metrics) == 0 {
		return 0
	}
	metrics := map[string]interface{}{}
	if err := json.Unmarshal(rec.Metrics, &metrics); err != nil {
		return 0
	}
	if v, ok := metrics["file_size_bytes"].(float64); ok {
		return int64(v)
	}
	return 0
}

// ----------------------------------------------------------------
// Trust gate (G1/G12)
// ----------------------------------------------------------------

type mediaAutopilotTrustStat struct {
	Verdict   string  `json:"verdict"`
	Decisions int64   `json:"decisions"`
	Applied   int64   `json:"applied"`
	Reverts   int64   `json:"reverts"`
	RevertPct float64 `json:"revert_pct"`
	Earned    bool    `json:"earned"`
}

// computeMediaAutopilotTrust walks the FULL recommendation history (G1): a
// verdict type is earned after >= min successful human applies with a revert
// rate under the cap. Dismissals are still shown as decisions in the cockpit,
// but they cannot teach Safe Auto to execute a verdict admins kept rejecting.
// Only the explicit `reverted` outcome counts as a revert (G12) — demotion
// self-decay never does. Autopilot's own applies are excluded from the applied
// trust count, but reverts of autopilot applies still count against the type.
func computeMediaAutopilotTrust(db *gorm.DB, tenantID string, policy models.MediaCirculationPolicy) map[string]mediaAutopilotTrustStat {
	out := map[string]mediaAutopilotTrustStat{}
	if db == nil {
		return out
	}
	type row struct {
		Verdict      string
		Decisions    int64
		Applied      int64
		TotalApplied int64
		Reverts      int64
	}
	var rows []row
	if err := db.Model(&models.MediaCirculationRecommendation{}).
		Select(`verdict,
			COUNT(*) FILTER (WHERE status IN ('applied','dismissed') AND COALESCE(applied_by,'') <> ?) AS decisions,
			COUNT(*) FILTER (WHERE status = 'applied' AND COALESCE(applied_by,'') <> ?) AS applied,
			COUNT(*) FILTER (WHERE status = 'applied') AS total_applied,
			COUNT(*) FILTER (WHERE outcome = ?) AS reverts`,
			mediaAutopilotSetBy, mediaAutopilotSetBy, mediaCircOutcomeReverted).
		Where("tenant_id = ?", tenantID).
		Group("verdict").
		Scan(&rows).Error; err != nil {
		return out
	}
	for _, r := range rows {
		stat := mediaAutopilotTrustStat{
			Verdict:   r.Verdict,
			Decisions: r.Decisions,
			Applied:   r.Applied,
			Reverts:   r.Reverts,
		}
		// Revert rate is reverts over ALL applies (human + autopilot), since a
		// revert can follow either — dividing by human-only applies produced
		// nonsense percentages (>100%) for verdicts autopilot applied and a
		// human then reverted.
		if r.TotalApplied > 0 {
			stat.RevertPct = float64(r.Reverts) * 100 / float64(r.TotalApplied)
		}
		stat.Earned = evaluateMediaAutopilotTrust(stat, policy)
		out[r.Verdict] = stat
	}
	return out
}

// evaluateMediaAutopilotTrust is the pure earn rule — split out for tests.
func evaluateMediaAutopilotTrust(stat mediaAutopilotTrustStat, policy models.MediaCirculationPolicy) bool {
	if isMediaApprovalTierVerdict(stat.Verdict) {
		return false // structurally untrustable (G3 + approval tier)
	}
	return stat.Applied >= int64(policy.AutopilotTrustMinDecisions) &&
		stat.RevertPct < float64(policy.AutopilotTrustMaxRevertPct)
}

// ----------------------------------------------------------------
// Gated selection (pure — the heart of the runner, fully unit-testable)
// ----------------------------------------------------------------

type mediaAutopilotGates struct {
	Trust             map[string]mediaAutopilotTrustStat
	Scores            map[uuid.UUID]intelligence.ScoreSnapshot
	ConfidenceFloor   float64
	QueueDepth        int
	MaxQueueDepth     int
	MaxAtomize        int
	MaxBytes          int64
	TrustMinDecisions int
	TrustMaxRevertPct int
	Now               time.Time
	StalenessWindow   time.Duration
	// IntakePaused: storage_relief elevated mode pauses the intake side so the
	// whole run budget goes to cost relief (G6).
	IntakePaused bool

	// mutable per-run usage
	atomizeUsed  int
	bytesPlanned int64
}

// mediaAutopilotElevatedCaps applies the time-boxed preset multipliers (G6).
// The preset→multiplier table is a code default, not env (Config Discipline).
// Returns the adjusted policy plus whether intake is paused for this run.
func mediaAutopilotElevatedCaps(policy models.MediaCirculationPolicy) (models.MediaCirculationPolicy, bool) {
	if policy.AutopilotElevatedMode == "" || policy.AutopilotElevatedUntil == nil ||
		!policy.AutopilotElevatedUntil.After(time.Now()) {
		policy.AutopilotElevatedMode = ""
		return policy, false
	}
	switch policy.AutopilotElevatedMode {
	case models.MediaAutopilotElevatedStorageRelief:
		// Evict-side caps up, intake paused: the run works the bill down.
		policy.AutopilotMaxActionsPerRun *= 2
		policy.AutopilotMaxBytesPerRun *= 4
		return policy, true
	case models.MediaAutopilotElevatedQualityRepair:
		// Re-encode throughput up.
		policy.AutopilotMaxActionsPerRun *= 2
		policy.AutopilotMaxBytesPerRun *= 4
		return policy, false
	case models.MediaAutopilotElevatedAtomizationCatchup:
		// Atomization backlog burn-down.
		policy.AutopilotMaxActionsPerRun *= 2
		policy.AutopilotMaxAtomizePerRun *= 3
		return policy, false
	}
	return policy, false
}

const (
	mediaAutopilotDecisionApply    = "apply"
	mediaAutopilotDecisionSkip     = "skip"
	mediaAutopilotDecisionApproval = "approval"
)

type mediaAutopilotDecision struct {
	Kind      string
	Guardrail string
	Reason    string
	Executes  bool // whether apply performs a live side effect (plannedApplyOutcome)
}

// decideMediaAutopilotRec applies the full gate ladder to one recommendation.
// Order: staleness → approval tier → acknowledge-only shortcut → trust gate →
// confidence/freshness gates (evict) → queue depth → atomize cap → byte cap.
func decideMediaAutopilotRec(rec models.MediaCirculationRecommendation, g *mediaAutopilotGates) mediaAutopilotDecision {
	_, executes := plannedApplyOutcome(rec.UnitType, rec.Verdict)

	if rec.Status != models.MediaCirculationRecStatusPending {
		return mediaAutopilotDecision{Kind: mediaAutopilotDecisionSkip, Guardrail: models.MediaAutopilotGuardStaleness,
			Reason: fmt.Sprintf("Recommendation is %s, not pending.", rec.Status)}
	}
	if g.StalenessWindow > 0 && g.Now.Sub(rec.CreatedAt) > g.StalenessWindow {
		return mediaAutopilotDecision{Kind: mediaAutopilotDecisionSkip, Guardrail: models.MediaAutopilotGuardStaleness,
			Reason: "Recommendation is older than the staleness window; regenerate first."}
	}
	if isMediaApprovalTierVerdict(rec.Verdict) {
		reason := "Approval-tier verdict — a human must apply or dismiss this."
		if rec.Verdict == mediaCircVerdictRecoverableDelete {
			reason = "recoverable_delete is hard approval-only (G3): never auto-applied regardless of mode, trust, or history."
		}
		return mediaAutopilotDecision{Kind: mediaAutopilotDecisionApproval, Guardrail: models.MediaAutopilotGuardApprovalTier, Reason: reason}
	}
	// Storage-tier verdicts are two-phase: apply marks deferred_to_sweep
	// (executes=false in plannedApplyOutcome) and the run's bounded sweep
	// executes them (G4) — so they gate like executing actions, never like
	// acknowledge-only bookkeeping.
	if !executes && !isMediaStorageTierVerdict(rec.Verdict) {
		// Acknowledge-only verdicts (protect, skip_source, blocked_transcript,
		// atomization_leak): no side effect — auto-acknowledging de-noises the
		// queue and needs no trust or confidence.
		return mediaAutopilotDecision{Kind: mediaAutopilotDecisionApply, Executes: false,
			Reason: "Acknowledge-only verdict; no side effect."}
	}
	if stat, ok := g.Trust[rec.Verdict]; !ok || !stat.Earned {
		return mediaAutopilotDecision{Kind: mediaAutopilotDecisionSkip, Guardrail: models.MediaAutopilotGuardTrustGate,
			Reason: fmt.Sprintf("Verdict %q has not earned trust yet (need ≥ %d successful human applies with revert rate < %d%%; have %d applies / %d decisions at %.0f%%).",
				rec.Verdict, g.TrustMinDecisions, g.TrustMaxRevertPct, stat.Applied, stat.Decisions, stat.RevertPct)}
	}
	if isMediaEvictValueVerdict(rec.Verdict) {
		snap, ok := g.Scores[rec.SubjectID]
		if !ok || !snap.Fresh {
			return mediaAutopilotDecision{Kind: mediaAutopilotDecisionSkip, Guardrail: models.MediaAutopilotGuardStaleScore,
				Reason: "Value score is stale or missing after the targeted refresh (G13); surfaced for approval instead."}
		}
		if snap.ExplorationState == intelligence.ExplorationExploring {
			return mediaAutopilotDecision{Kind: mediaAutopilotDecisionSkip, Guardrail: models.MediaAutopilotGuardExplorationGuard,
				Reason: "Item is still exploring — never auto-evict below the impression target."}
		}
		if snap.ExplorationState != intelligence.ExplorationEstablished || snap.Confidence < g.ConfidenceFloor {
			return mediaAutopilotDecision{Kind: mediaAutopilotDecisionSkip, Guardrail: models.MediaAutopilotGuardLowConfidence,
				Reason: fmt.Sprintf("Value estimate is provisional (state=%s, confidence=%.2f < %.2f floor); never auto-evict on it (G7).",
					snap.ExplorationState, snap.Confidence, g.ConfidenceFloor)}
		}
	}
	if g.IntakePaused && rec.UnitType == models.MediaCirculationUnitSource {
		return mediaAutopilotDecision{Kind: mediaAutopilotDecisionSkip, Guardrail: models.MediaAutopilotGuardElevatedMode,
			Reason: "storage_relief elevated mode pauses intake — this run's budget goes to cost relief."}
	}
	if isMediaQueueLoadVerdict(rec.Verdict) && g.QueueDepth > g.MaxQueueDepth {
		return mediaAutopilotDecision{Kind: mediaAutopilotDecisionSkip, Guardrail: models.MediaAutopilotGuardQueueDepth,
			Reason: fmt.Sprintf("Aggregation queue depth %d exceeds the %d cap.", g.QueueDepth, g.MaxQueueDepth)}
	}
	if rec.Verdict == mediaCircVerdictAtomizeNow {
		if g.atomizeUsed >= g.MaxAtomize {
			return mediaAutopilotDecision{Kind: mediaAutopilotDecisionSkip, Guardrail: models.MediaAutopilotGuardAtomizeLimit,
				Reason: fmt.Sprintf("Atomize cap (%d per run) reached — one atomize fans out a whole pipeline (G8).", g.MaxAtomize)}
		}
		g.atomizeUsed++
	}
	if isMediaStorageTierVerdict(rec.Verdict) {
		size := mediaRecByteSize(rec)
		if g.MaxBytes > 0 && g.bytesPlanned+size > g.MaxBytes {
			return mediaAutopilotDecision{Kind: mediaAutopilotDecisionSkip, Guardrail: models.MediaAutopilotGuardBudget,
				Reason: fmt.Sprintf("Byte budget: %d planned + %d would exceed the %d cap.", g.bytesPlanned, size, g.MaxBytes)}
		}
		g.bytesPlanned += size
	}
	// Storage-tier applies count as executing: the side effect lands via the
	// run's bounded sweep even though the apply itself only marks the rec.
	return mediaAutopilotDecision{Kind: mediaAutopilotDecisionApply, Executes: true}
}

// mediaAutopilotActionCost: deep_pull counts double against the action cap.
func mediaAutopilotActionCost(verdict string) int {
	if verdict == mediaCircVerdictDeepPull {
		return 2
	}
	return 1
}

// ----------------------------------------------------------------
// Ledger runner
// ----------------------------------------------------------------

type mediaAutopilotRunner struct {
	db            *gorm.DB
	run           *models.MediaCirculationRun
	policy        models.MediaCirculationPolicy
	observe       bool
	authorization string // minted service token forwarded to Aggregation calls

	used      int
	success   int // tool + recommendation successes (drives run status)
	applied   int // recommendations that executed a side effect (or would, in Observe)
	acked     int // acknowledge-only recommendations (no side effect)
	skipped   int
	errored   int
	approvals int

	storageSubjectsByVerdict map[string][]string
}

// terminalStatus maps a decision kind to its ledger status: same rows in both
// modes, only the terminal execute flips (G9).
func (r *mediaAutopilotRunner) terminalStatus(kind string) string {
	switch kind {
	case mediaAutopilotDecisionApply:
		if r.observe {
			return models.MediaAutopilotActionStatusWouldApply
		}
		return models.MediaAutopilotActionStatusSuccess
	case mediaAutopilotDecisionSkip:
		if r.observe {
			return models.MediaAutopilotActionStatusWouldSkip
		}
		return models.MediaAutopilotActionStatusSkipped
	case mediaAutopilotDecisionApproval:
		return models.MediaAutopilotActionStatusApprovalRequired
	}
	return models.MediaAutopilotActionStatusSkipped
}

func (r *mediaAutopilotRunner) writeAction(a models.MediaCirculationAction) {
	a.RunID = r.run.ID
	a.TenantID = r.run.TenantID
	if a.StartedAt.IsZero() {
		a.StartedAt = time.Now()
	}
	if a.FinishedAt == nil {
		now := time.Now()
		a.FinishedAt = &now
	}
	_ = r.db.Create(&a).Error
}

// executeTool runs a non-recommendation tool (health, generation, sweeps) and
// ledgers it. In Observe mode side-effectful tools are recorded as would_apply
// without executing (sideEffect=true marks those).
func (r *mediaAutopilotRunner) executeTool(toolName, reason string, sideEffect bool, input interface{}, fn func() (interface{}, error)) {
	startedAt := time.Now()
	if sideEffect && r.observe {
		r.writeAction(models.MediaCirculationAction{
			ToolName: toolName, Status: models.MediaAutopilotActionStatusWouldApply,
			Reason: reason + " (Observe: dry-run, not executed.)", Input: marshalAutopilotJSON(input), StartedAt: startedAt,
		})
		r.success++
		return
	}
	output, err := fn()
	finishedAt := time.Now()
	action := models.MediaCirculationAction{
		ToolName: toolName, Reason: reason,
		Input: marshalAutopilotJSON(input), Output: marshalAutopilotJSON(output),
		StartedAt: startedAt, FinishedAt: &finishedAt,
		Status: models.MediaAutopilotActionStatusSuccess,
	}
	if err != nil {
		action.Status = models.MediaAutopilotActionStatusError
		action.Error = err.Error()
		r.errored++
	} else {
		r.success++
	}
	r.writeAction(action)
}

// dispatchRecommendation ledgers + (in Safe Auto) executes one gated decision.
func (r *mediaAutopilotRunner) dispatchRecommendation(rec models.MediaCirculationRecommendation, decision mediaAutopilotDecision) {
	recID := rec.PublicID
	base := models.MediaCirculationAction{
		RecommendationID: &recID,
		ToolName:         "recommendation." + rec.Verdict,
		Reason:           decision.Reason,
		Guardrail:        decision.Guardrail,
		Input:            marshalAutopilotJSON(gin.H{"unit_type": rec.UnitType, "verdict": rec.Verdict, "subject_id": rec.SubjectID, "score": rec.Score}),
	}
	if isMediaStorageTierVerdict(rec.Verdict) || rec.Verdict == mediaCircVerdictRecoverableDelete {
		base.ByteImpact = mediaRecByteSize(rec)
	}
	if isMediaQueueLoadVerdict(rec.Verdict) {
		base.QueueImpact = 1
	}
	if rec.Verdict == mediaCircVerdictRankDown {
		base.FeedImpact = 1
	}

	switch decision.Kind {
	case mediaAutopilotDecisionApproval:
		base.Status = r.terminalStatus(decision.Kind)
		r.approvals++
		r.writeAction(base)
	case mediaAutopilotDecisionSkip:
		base.Status = r.terminalStatus(decision.Kind)
		r.skipped++
		r.writeAction(base)
	case mediaAutopilotDecisionApply:
		if r.observe {
			base.Status = r.terminalStatus(decision.Kind)
			if base.Reason == "" {
				base.Reason = "Would auto-apply (Observe dry-run)."
			}
			r.countApply(decision.Executes)
			r.success++
			r.writeAction(base)
			return
		}
		// Claim the row atomically so a human dismiss/apply landing mid-run is
		// not clobbered: only a still-pending row is ours to act on.
		claim := r.db.Model(&models.MediaCirculationRecommendation{}).
			Where("tenant_id = ? AND public_id = ? AND status = ?", r.run.TenantID, rec.PublicID, models.MediaCirculationRecStatusPending).
			Update("status", models.MediaCirculationRecStatusProcessing)
		if claim.Error != nil || claim.RowsAffected == 0 {
			base.Status = models.MediaAutopilotActionStatusSkipped
			base.Guardrail = models.MediaAutopilotGuardStaleness
			base.Reason = "Recommendation was no longer pending at apply time (resolved concurrently)."
			r.skipped++
			r.writeAction(base)
			return
		}
		outcome, err := applyRecommendation(r.db, r.run.TenantID, mediaAutopilotSetBy, r.authorization, rec)
		if err != nil {
			// Release the claim back to pending so it can be retried next run.
			_ = r.db.Model(&models.MediaCirculationRecommendation{}).
				Where("tenant_id = ? AND public_id = ?", r.run.TenantID, rec.PublicID).
				Update("status", models.MediaCirculationRecStatusPending).Error
			if errors.Is(err, errMediaCircIntakeBudgetExhausted) {
				base.Status = models.MediaAutopilotActionStatusSkipped
				base.Guardrail = models.MediaAutopilotGuardBudget
				base.Reason = "Rolling 24h intake budget exhausted."
				r.skipped++
			} else {
				base.Status = models.MediaAutopilotActionStatusError
				base.Error = err.Error()
				r.errored++
			}
			r.writeAction(base)
			return
		}
		now := time.Now().UTC()
		if err := r.db.Model(&models.MediaCirculationRecommendation{}).
			Where("tenant_id = ? AND public_id = ?", r.run.TenantID, rec.PublicID).
			Updates(map[string]interface{}{
				"status": models.MediaCirculationRecStatusApplied, "applied": true,
				"applied_at": now, "applied_by": mediaAutopilotSetBy, "outcome": outcome, "updated_at": now,
			}).Error; err != nil {
			// The side effect ran but we could not record it — surface the action
			// as an error (not success) so the run is partial and the divergence
			// is visible, and leave the row in PROCESSING (not pending) so the
			// next run does NOT re-execute the same side effect.
			base.Status = models.MediaAutopilotActionStatusError
			base.Error = "side effect executed (outcome=" + outcome + ") but recording the recommendation state failed: " + err.Error()
			base.Output = marshalAutopilotJSON(gin.H{"outcome": outcome})
			r.errored++
			r.writeAction(base)
			return
		}
		base.Status = models.MediaAutopilotActionStatusSuccess
		base.Output = marshalAutopilotJSON(gin.H{"outcome": outcome})
		if base.Reason == "" {
			base.Reason = "Auto-applied through the shared apply path."
		}
		if isMediaStorageTierVerdict(rec.Verdict) {
			if r.storageSubjectsByVerdict == nil {
				r.storageSubjectsByVerdict = map[string][]string{}
			}
			r.storageSubjectsByVerdict[rec.Verdict] = append(r.storageSubjectsByVerdict[rec.Verdict], rec.SubjectID.String())
		}
		r.countApply(decision.Executes)
		r.success++
		r.writeAction(base)
	}
}

// countApply tallies a successful recommendation apply (or would-apply) into the
// executing vs acknowledge-only buckets so the run summary is honest (G9): only
// executing applies are what Safe Auto would actually act on.
func (r *mediaAutopilotRunner) countApply(executes bool) {
	if executes {
		r.applied++
	} else {
		r.acked++
	}
}

// ----------------------------------------------------------------
// The run
// ----------------------------------------------------------------

type mediaAutopilotRunOptions struct {
	Trigger   string
	CreatedBy string
}

var errMediaAutopilotDisabled = errors.New("media circulation autopilot is not enabled for this tenant")
var errMediaAutopilotAlreadyRunning = errors.New("media circulation autopilot is already running for this tenant")

var (
	mediaAutopilotRunMu       sync.Mutex
	mediaAutopilotRunInFlight = map[string]bool{}
)

func tryStartMediaAutopilotRun(tenantID string) bool {
	mediaAutopilotRunMu.Lock()
	defer mediaAutopilotRunMu.Unlock()
	if mediaAutopilotRunInFlight[tenantID] {
		return false
	}
	mediaAutopilotRunInFlight[tenantID] = true
	return true
}

func finishMediaAutopilotRun(tenantID string) {
	mediaAutopilotRunMu.Lock()
	defer mediaAutopilotRunMu.Unlock()
	delete(mediaAutopilotRunInFlight, tenantID)
}

func runMediaCirculationAutopilot(db *gorm.DB, tenantID string, opts mediaAutopilotRunOptions) (models.MediaCirculationRun, []models.MediaCirculationAction, error) {
	if strings.TrimSpace(tenantID) == "" {
		tenantID = defaultCirculationTenant
	}
	if !tryStartMediaAutopilotRun(tenantID) {
		return models.MediaCirculationRun{}, nil, errMediaAutopilotAlreadyRunning
	}
	defer finishMediaAutopilotRun(tenantID)

	policy := loadEffectiveMediaCirculationPolicy(db, tenantID)
	if !policy.Enabled || !policy.AutopilotEnabled {
		return models.MediaCirculationRun{}, nil, errMediaAutopilotDisabled
	}
	trigger := strings.TrimSpace(opts.Trigger)
	if trigger == "" {
		trigger = "scheduled"
	}
	now := time.Now()
	observe := policy.AutopilotMode != models.MediaAutopilotModeSafeAuto
	var intakePaused bool
	policy, intakePaused = mediaAutopilotElevatedCaps(policy)

	// Scheduled automation is a named machine caller, never a CMS-minted human
	// admin JWT. Aggregation accepts this credential only on its explicitly
	// registered automation endpoint.
	automationToken := strings.TrimSpace(os.Getenv("AGGREGATION_AUTOMATION_TOKEN"))
	bearer := ""
	if automationToken != "" {
		bearer = "Bearer " + automationToken
	}

	run := models.MediaCirculationRun{
		TenantID:     tenantID,
		Trigger:      trigger,
		Mode:         policy.AutopilotMode,
		ElevatedMode: policy.AutopilotElevatedMode,
		Status:       models.MediaAutopilotRunStatusRunning,
		StartedAt:    now,
		CreatedBy:    opts.CreatedBy,
	}
	if run.CreatedBy == "" {
		run.CreatedBy = "automation"
	}

	// Precondition check (G11): op budget capped or Aggregation unreachable
	// aborts the whole run before any side effect.
	opBudget := getStorageOpBudgetStatus(db, tenantID)
	queueStats, queueErr := fetchAggregationQueueStats()
	var precondition string
	if automationToken == "" {
		precondition = "aggregation automation credential is not configured"
	} else if opBudget.ClassAStatus == "cap" {
		precondition = "Class A op budget is capped for this month"
	} else if queueErr != nil {
		precondition = "Aggregation is unreachable: " + queueErr.Error()
	}

	healthBefore := collectMediaAutopilotHealth(db, tenantID, bearer)
	run.HealthBefore = marshalAutopilotJSON(healthBefore)
	if err := db.Create(&run).Error; err != nil {
		return run, nil, err
	}

	if precondition != "" {
		finishedAt := time.Now()
		_ = db.Model(&models.MediaCirculationRun{}).Where("id = ?", run.ID).Updates(map[string]interface{}{
			"status": models.MediaAutopilotRunStatusFailed, "finished_at": finishedAt,
			"summary": "Precondition failed: " + precondition, "error": precondition, "updated_at": finishedAt,
		}).Error
		run.Status = models.MediaAutopilotRunStatusFailed
		run.FinishedAt = &finishedAt
		run.Summary = "Precondition failed: " + precondition
		run.Error = precondition
		touchMediaAutopilotLastRun(db, tenantID, finishedAt)
		return run, nil, nil
	}

	runner := &mediaAutopilotRunner{db: db, run: &run, policy: policy, observe: observe, authorization: bearer}
	queueDepth := mediaRelevantQueueDepth(queueStats)

	runner.executeTool("health.evaluate", "Read library, storage, op-budget, and intelligence health before acting.", false,
		gin.H{"queue_depth": queueDepth}, func() (interface{}, error) { return healthBefore, nil })

	// Generation is read+persist (advisory), safe in both modes.
	runner.executeTool("recommendations.generate", "Run one full generation pass (evict + atomization + intake) through the shared path.", false,
		nil, func() (interface{}, error) {
			evict, intake, err := generateMediaCircRecommendationSets(db, tenantID, policy, bearer)
			if err != nil {
				return nil, err
			}
			return gin.H{"item_family": len(evict), "source": len(intake)}, nil
		})

	// Bounded pull: at most a few dozen actions can be taken per run, so cap the
	// candidate scan rather than loading an entire pending backlog into memory.
	// Highest-score rows come first; the remainder simply waits for next run.
	var pending []models.MediaCirculationRecommendation
	_ = db.Where("tenant_id = ? AND status = ?", tenantID, models.MediaCirculationRecStatusPending).
		Order("score DESC, id ASC").Limit(mediaAutopilotMaxCandidates).Find(&pending).Error

	// Targeted pre-flight refresh (G13): only the evict candidates this run judges.
	evictIDs := make([]uuid.UUID, 0)
	for _, rec := range pending {
		if isMediaEvictValueVerdict(rec.Verdict) {
			evictIDs = append(evictIDs, rec.SubjectID)
		}
	}
	engine := intelligence.Engine{DB: db}
	var scores map[uuid.UUID]intelligence.ScoreSnapshot
	runner.executeTool("intelligence.refresh", "Targeted refresh of the evict candidates' value scores (fresh-or-no-autonomy).", false,
		gin.H{"candidates": len(evictIDs)}, func() (interface{}, error) {
			scores = engine.FreshScores(tenantID, evictIDs)
			fresh := 0
			for _, s := range scores {
				if s.Fresh {
					fresh++
				}
			}
			return gin.H{"scored": len(scores), "fresh": fresh}, nil
		})

	trust := computeMediaAutopilotTrust(db, tenantID, policy)
	gates := &mediaAutopilotGates{
		Trust:             trust,
		Scores:            scores,
		ConfidenceFloor:   policy.AutopilotEvictConfidenceFloor,
		QueueDepth:        queueDepth,
		MaxQueueDepth:     policy.AutopilotMaxQueueDepth,
		MaxAtomize:        policy.AutopilotMaxAtomizePerRun,
		MaxBytes:          policy.AutopilotMaxBytesPerRun,
		TrustMinDecisions: policy.AutopilotTrustMinDecisions,
		TrustMaxRevertPct: policy.AutopilotTrustMaxRevertPct,
		Now:               time.Now(),
		StalenessWindow:   mediaAutopilotStalenessWindow,
		IntakePaused:      intakePaused,
	}

	blockedTranscripts := 0
	for _, rec := range pending {
		if rec.Verdict == mediaCircVerdictBlockedTranscript {
			blockedTranscripts++
		}
		decision := decideMediaAutopilotRec(rec, gates)
		if decision.Kind == mediaAutopilotDecisionApply && decision.Executes {
			cost := mediaAutopilotActionCost(rec.Verdict)
			if runner.used+cost > policy.AutopilotMaxActionsPerRun {
				decision = mediaAutopilotDecision{Kind: mediaAutopilotDecisionSkip,
					Guardrail: models.MediaAutopilotGuardActionLimit,
					Reason:    fmt.Sprintf("Action cap (%d per run) reached.", policy.AutopilotMaxActionsPerRun)}
			} else {
				runner.used += cost
			}
		}
		runner.dispatchRecommendation(rec, decision)
	}

	// Storage-execution phase (G4): the sweep is the sole storage executor;
	// the run triggers bounded passes for only successfully deferred applies.
	for verdict, ids := range runner.storageSubjectsByVerdict {
		if len(ids) == 0 {
			continue
		}
		archiveAction := verdict
		runner.executeTool("storage.sweep."+archiveAction, "Bounded storage sweep executes only the Autopilot-selected "+archiveAction+" applies (single-actor rule).", true,
			gin.H{"deferred_applies": len(ids), "candidate_ids": ids, "archive_action": archiveAction, "max_bytes": policy.AutopilotMaxBytesPerRun}, func() (interface{}, error) {
				if err := callAggregationRunSweepScoped(bearer, tenantID, archiveAction, ids, policy.AutopilotMaxBytesPerRun); err != nil {
					return nil, err
				}
				return gin.H{"triggered": true, "candidate_count": len(ids), "archive_action": archiveAction}, nil
			})
	}

	// Transcript unblock phase (G8): the atomization sweep auto-requests
	// transcripts for CMS-listed candidates, so blocked parents progress.
	if blockedTranscripts > 0 {
		runner.executeTool("atomization.transcript_sweep", "Atomization sweep auto-requests transcripts for blocked >40m parents.", true,
			gin.H{"blocked_transcripts": blockedTranscripts}, func() (interface{}, error) {
				body, status, err := proxyAggregationPost(bearer, "/admin/atomization/sweep-now", map[string]any{"trigger": "autopilot"})
				if err != nil {
					return nil, err
				}
				if status >= http.StatusMultipleChoices {
					return nil, fmt.Errorf("atomization sweep responded with %d: %s", status, string(body))
				}
				return gin.H{"triggered": true}, nil
			})
	}

	finishedAt := time.Now()
	healthAfter := collectMediaAutopilotHealth(db, tenantID, bearer)
	status := models.MediaAutopilotRunStatusCompleted
	if runner.errored > 0 && runner.success == 0 {
		status = models.MediaAutopilotRunStatusFailed
	} else if runner.errored > 0 {
		status = models.MediaAutopilotRunStatusPartial
	}
	// Summary counts only executing (would-)applies as the headline action figure
	// so the promotion nudge is honest (G9); acknowledge-only recs are reported
	// separately and never inflate "would apply N".
	verb := "applied"
	if observe {
		verb = "would apply"
	}
	summary := fmt.Sprintf("%s %d (+%d acknowledged), skipped %d, approval-required %d, errors %d",
		verb, runner.applied, runner.acked, runner.skipped, runner.approvals, runner.errored)
	errText := ""
	if status == models.MediaAutopilotRunStatusFailed {
		errText = "all executable autopilot actions failed"
	}
	_ = db.Model(&models.MediaCirculationRun{}).Where("id = ?", run.ID).Updates(map[string]interface{}{
		"status": status, "finished_at": finishedAt, "summary": summary,
		"health_after": marshalAutopilotJSON(healthAfter), "error": errText, "updated_at": finishedAt,
	}).Error
	touchMediaAutopilotLastRun(db, tenantID, finishedAt)
	invalidateMediaCircHealth(tenantID)

	run.Status = status
	run.FinishedAt = &finishedAt
	run.Summary = summary
	run.HealthAfter = marshalAutopilotJSON(healthAfter)
	run.Error = errText

	var actions []models.MediaCirculationAction
	_ = db.Where("tenant_id = ? AND run_id = ?", tenantID, run.ID).
		Order("started_at ASC, id ASC").Find(&actions).Error
	return run, actions, nil
}

// collectMediaAutopilotHealth is the run snapshot (G10): the cockpit health
// read-model plus the intelligence corpus diagnostics — the same numbers the
// admin already trusts, at two timestamps.
func collectMediaAutopilotHealth(db *gorm.DB, tenantID, authorization string) gin.H {
	health := buildMediaCirculationHealth(db, tenantID, authorization)
	diag := intelligence.Engine{DB: db}.DiagnosticsSnapshot(tenantID)
	return gin.H{"health": health, "intelligence": diag, "generated_at": time.Now().UTC().Format(time.RFC3339)}
}

func touchMediaAutopilotLastRun(db *gorm.DB, tenantID string, at time.Time) {
	_ = db.Model(&models.MediaCirculationPolicy{}).
		Where("tenant_id = ?", tenantID).
		Updates(map[string]interface{}{"autopilot_last_run_at": at, "updated_at": at}).Error
}

// ----------------------------------------------------------------
// Admin endpoints (manual trigger + history)
// ----------------------------------------------------------------

// ----------------------------------------------------------------
// Cockpit status block (Slice 4)
// ----------------------------------------------------------------

type mediaAutopilotStatusBlock struct {
	Enabled           bool                        `json:"enabled"`
	Mode              string                      `json:"mode"`
	State             string                      `json:"state"` // off | observe | safe_auto | elevated | paused
	IntervalMinutes   int                         `json:"interval_minutes"`
	ElevatedMode      string                      `json:"elevated_mode,omitempty"`
	ElevatedUntil     *time.Time                  `json:"elevated_until,omitempty"`
	PausedUntil       *time.Time                  `json:"paused_until,omitempty"`
	LastRunAt         *time.Time                  `json:"last_run_at,omitempty"`
	NextRunAt         *time.Time                  `json:"next_run_at,omitempty"`
	LastRun           *models.MediaCirculationRun `json:"last_run,omitempty"`
	Trust             []mediaAutopilotTrustStat   `json:"trust"`
	RecommendedAction string                      `json:"recommended_action,omitempty"`
}

func buildMediaAutopilotStatus(db *gorm.DB, tenantID string, policy models.MediaCirculationPolicy) mediaAutopilotStatusBlock {
	now := time.Now()
	block := mediaAutopilotStatusBlock{
		Enabled:         policy.AutopilotEnabled,
		Mode:            policy.AutopilotMode,
		IntervalMinutes: policy.AutopilotIntervalMinutes,
		ElevatedMode:    policy.AutopilotElevatedMode,
		ElevatedUntil:   policy.AutopilotElevatedUntil,
		PausedUntil:     policy.AutopilotPausedUntil,
		LastRunAt:       policy.AutopilotLastRunAt,
		Trust:           []mediaAutopilotTrustStat{},
	}
	switch {
	case !policy.AutopilotEnabled:
		block.State = "off"
	case policy.AutopilotPausedUntil != nil && policy.AutopilotPausedUntil.After(now):
		block.State = "paused"
	case policy.AutopilotElevatedMode != "" && policy.AutopilotElevatedUntil != nil && policy.AutopilotElevatedUntil.After(now):
		block.State = "elevated"
	default:
		block.State = policy.AutopilotMode
	}
	if policy.AutopilotEnabled && block.State != "paused" {
		next := now
		if policy.AutopilotLastRunAt != nil {
			next = policy.AutopilotLastRunAt.Add(time.Duration(policy.AutopilotIntervalMinutes) * time.Minute)
		}
		block.NextRunAt = &next
	}

	var lastRun models.MediaCirculationRun
	if err := db.Where("tenant_id = ?", tenantID).Order("started_at DESC").First(&lastRun).Error; err == nil {
		block.LastRun = &lastRun
	}

	trust := computeMediaAutopilotTrust(db, tenantID, policy)
	verdicts := make([]string, 0, len(trust))
	for v := range trust {
		verdicts = append(verdicts, v)
	}
	sort.Strings(verdicts)
	earned := []string{}
	for _, v := range verdicts {
		stat := trust[v]
		block.Trust = append(block.Trust, stat)
		if stat.Earned {
			earned = append(earned, v)
		}
	}

	// The one recommended action (G5 nudge): mode changes are always human.
	switch {
	case !policy.Enabled:
		block.RecommendedAction = "Enable Media Circulation first — Autopilot supervises the circulation engine."
	case !policy.AutopilotEnabled:
		block.RecommendedAction = "Enable Autopilot in Observe mode — it runs the full pipeline as a dry-run ledger with zero side effects."
	case policy.AutopilotMode == models.MediaAutopilotModeObserve && len(earned) > 0:
		block.RecommendedAction = fmt.Sprintf("Enable Safe Auto — %s earned trust from your decision history (%s).",
			pluralizeVerdicts(earned), strings.Join(earned, ", "))
	case policy.AutopilotMode == models.MediaAutopilotModeObserve:
		block.RecommendedAction = "Keep observing — no verdict type has earned trust yet; successful human applies build the execution track record."
	}
	return block
}

func pluralizeVerdicts(earned []string) string {
	if len(earned) == 1 {
		return "1 verdict type has"
	}
	return fmt.Sprintf("%d verdict types have", len(earned))
}

// ----------------------------------------------------------------
// Scheduler heartbeat (Slice 3)
// ----------------------------------------------------------------

// StartMediaCirculationAutopilotHeartbeat launches the scheduled loop: a
// one-minute ticker fires runs for tenants whose interval has elapsed. One run
// per tenant at a time; pause and disable are respected without touching
// policy. Mirrors the News StartCirculationAutomation pattern.
func StartMediaCirculationAutopilotHeartbeat(db *gorm.DB) {
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			runMediaAutopilotDue(db)
		}
	}()
}

func runMediaAutopilotDue(db *gorm.DB) {
	var policies []models.MediaCirculationPolicy
	if err := db.Where("enabled = ? AND autopilot_enabled = ?", true, true).Find(&policies).Error; err != nil {
		return
	}
	now := time.Now()
	for _, raw := range policies {
		policy := sanitizeMediaCirculationPolicy(raw)
		if policy.AutopilotPausedUntil != nil && policy.AutopilotPausedUntil.After(now) {
			continue
		}
		if raw.AutopilotLastRunAt != nil &&
			now.Sub(*raw.AutopilotLastRunAt) < time.Duration(policy.AutopilotIntervalMinutes)*time.Minute {
			continue
		}
		run, _, err := runMediaCirculationAutopilot(db, policy.TenantID, mediaAutopilotRunOptions{
			Trigger:   "scheduled",
			CreatedBy: "automation",
		})
		if errors.Is(err, errMediaAutopilotAlreadyRunning) {
			continue
		}
		payload := map[string]interface{}{"status": run.Status, "summary": run.Summary}
		if err != nil {
			payload["error"] = err.Error()
		}
		writeCirculationAuditSystem(db, policy.TenantID, "media_circulation.autopilot.scheduled", policy.TenantID, payload)
	}
}

// ----------------------------------------------------------------
// Pause + elevated-mode endpoints (Slice 3)
// ----------------------------------------------------------------

func PauseMediaCirculationAutopilot(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var req struct {
		Minutes int `json:"minutes"` // 0 = resume
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request: " + err.Error(), Code: "INVALID_REQUEST"})
		return
	}
	updates := map[string]interface{}{"updated_at": time.Now()}
	var until *time.Time
	if req.Minutes > 0 {
		minutes := req.Minutes
		if minutes > 10080 { // one week
			minutes = 10080
		}
		t := time.Now().Add(time.Duration(minutes) * time.Minute)
		until = &t
	}
	updates["autopilot_paused_until"] = until
	if err := db.Model(&models.MediaCirculationPolicy{}).
		Where("tenant_id = ?", principal.TenantID).Updates(updates).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to update pause state", Code: "SAVE_FAILED"})
		return
	}
	action := "media_circulation.autopilot.pause"
	if until == nil {
		action = "media_circulation.autopilot.resume"
	}
	writeCirculationAudit(db, principal, action, principal.TenantID, map[string]interface{}{"paused_until": until})
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"paused_until": until}})
}

func ElevateMediaCirculationAutopilot(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var req struct {
		Mode    string `json:"mode"` // "" = clear elevation
		Minutes int    `json:"minutes"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request: " + err.Error(), Code: "INVALID_REQUEST"})
		return
	}
	updates := map[string]interface{}{"updated_at": time.Now()}
	var until *time.Time
	switch req.Mode {
	case "":
		updates["autopilot_elevated_mode"] = ""
		updates["autopilot_elevated_until"] = nil
	case models.MediaAutopilotElevatedStorageRelief,
		models.MediaAutopilotElevatedQualityRepair,
		models.MediaAutopilotElevatedAtomizationCatchup:
		minutes := req.Minutes
		if minutes <= 0 {
			minutes = 120 // default 2h time-box
		}
		if minutes < 15 {
			minutes = 15
		}
		if minutes > 720 {
			minutes = 720 // max 12h — elevated modes are time-boxed by design
		}
		t := time.Now().Add(time.Duration(minutes) * time.Minute)
		until = &t
		updates["autopilot_elevated_mode"] = req.Mode
		updates["autopilot_elevated_until"] = until
	default:
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Unknown elevated mode: " + req.Mode, Code: "INVALID_MODE"})
		return
	}
	if err := db.Model(&models.MediaCirculationPolicy{}).
		Where("tenant_id = ?", principal.TenantID).Updates(updates).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to update elevated mode", Code: "SAVE_FAILED"})
		return
	}
	writeCirculationAudit(db, principal, "media_circulation.autopilot.elevate", principal.TenantID, map[string]interface{}{
		"mode": req.Mode, "until": until,
	})
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"mode": req.Mode, "until": until}})
}

func RunMediaCirculationAutopilotNow(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	run, actions, err := runMediaCirculationAutopilot(db, principal.TenantID, mediaAutopilotRunOptions{
		Trigger:   "manual",
		CreatedBy: principal.Email,
	})
	if err != nil {
		if errors.Is(err, errMediaAutopilotDisabled) {
			c.JSON(http.StatusConflict, authErrorResponse{Message: err.Error(), Code: "AUTOPILOT_DISABLED"})
			return
		}
		if errors.Is(err, errMediaAutopilotAlreadyRunning) {
			c.JSON(http.StatusConflict, authErrorResponse{Message: err.Error(), Code: "AUTOPILOT_ALREADY_RUNNING"})
			return
		}
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Autopilot run failed: " + err.Error(), Code: "RUN_FAILED"})
		return
	}
	writeCirculationAudit(db, principal, "media_circulation.autopilot.run", principal.TenantID, map[string]interface{}{
		"run_id": run.PublicID.String(), "status": run.Status, "summary": run.Summary,
	})
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"run": run, "actions": actions}})
}

func ListMediaCirculationAutopilotRuns(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var runs []models.MediaCirculationRun
	if err := db.Where("tenant_id = ?", principal.TenantID).
		Order("started_at DESC").Limit(boundedLimit(c.Query("limit"), 20, 100)).
		Find(&runs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to list runs", Code: "QUERY_FAILED"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"items": runs}})
}

func GetMediaCirculationAutopilotRun(c *gin.Context) {
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
	var run models.MediaCirculationRun
	if err := db.Where("tenant_id = ? AND public_id = ?", principal.TenantID, runID).First(&run).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Run not found", Code: "NOT_FOUND"})
		return
	}
	var actions []models.MediaCirculationAction
	_ = db.Where("tenant_id = ? AND run_id = ?", principal.TenantID, run.ID).
		Order("started_at ASC, id ASC").Find(&actions).Error
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"run": run, "actions": actions}})
}
