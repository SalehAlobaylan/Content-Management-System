package controllers

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"

	"content-management-system/src/models"
)

// Enrichment Coverage Autopilot — Slices 2 & 3 (deterministic runner + scheduler).
//
// One run = precondition check (health / bulk-run mutex) → stats before-snapshot
// → per-class gap selection (buildMissingQuery + age floor + transcript duration
// scope) → class gates (service / trust / queue-depth / embedding stall) →
// per-item trigger through the SAME triggerItemArtifacts path humans use →
// stats after-snapshot + headline. Observe suppresses only the terminal trigger,
// writing would_trigger/would_skip rows. Nothing here forces STT, chases the dead
// sparse lane, mutates status, or touches queues.
// See docs/enrichment-autopilot-plan.md.

const (
	// enrichmentTrustDemotionFloor: a class needs at least this many recorded
	// attempts before a high failure rate can DEMOTE it — one early flake must
	// not park a whole artifact class.
	enrichmentTrustDemotionFloor = 10
	// Trust is recent execution history, not a permanent verdict on a class.
	enrichmentTrustWindowDays = 14
	// enrichmentBackfillCatchupMultiplier scales the item caps in the one elevated
	// preset (code default, not env — Config Discipline).
	enrichmentBackfillCatchupMultiplier = 5
)

// enrichmentManagedArtifacts is the ordered class list the autopilot works.
// `sparse` is deliberately absent (dead post-Qwen).
var enrichmentManagedArtifacts = []string{
	models.EnrichmentArtifactTranscript,
	models.EnrichmentArtifactEmbedding,
	models.EnrichmentArtifactImage,
}

// enrichmentArtifactService maps a class to the AI service that must be healthy
// for it: transcript + image (CLIP) live in Media, dense embedding in Enrichment.
func enrichmentArtifactService(artifact string) string {
	switch artifact {
	case models.EnrichmentArtifactEmbedding:
		return "enrichment"
	default: // transcript, image
		return "media"
	}
}

var (
	errEnrichmentAutopilotDisabled       = errors.New("enrichment autopilot is not enabled for this tenant")
	errEnrichmentAutopilotAlreadyRunning = errors.New("enrichment autopilot is already running for this tenant")
	errEnrichmentAutopilotBusy           = errors.New("a manual bulk enrichment run is in flight; autopilot deferred")
)

var (
	enrichmentAutopilotRunMu       sync.Mutex
	enrichmentAutopilotRunInFlight = map[string]bool{}
)

func tryStartEnrichmentAutopilotRun(tenantID string) bool {
	enrichmentAutopilotRunMu.Lock()
	defer enrichmentAutopilotRunMu.Unlock()
	if enrichmentAutopilotRunInFlight[tenantID] {
		return false
	}
	enrichmentAutopilotRunInFlight[tenantID] = true
	return true
}

func finishEnrichmentAutopilotRun(tenantID string) {
	enrichmentAutopilotRunMu.Lock()
	defer enrichmentAutopilotRunMu.Unlock()
	delete(enrichmentAutopilotRunInFlight, tenantID)
}

func enrichmentAutopilotAnyRunInFlight() bool {
	enrichmentAutopilotRunMu.Lock()
	defer enrichmentAutopilotRunMu.Unlock()
	return len(enrichmentAutopilotRunInFlight) > 0
}

// ----------------------------------------------------------------
// Policy load + sanitize (mirrors the media/news clamp pattern)
// ----------------------------------------------------------------

func loadEnrichmentAutopilotPolicy(db *gorm.DB, tenantID string) models.EnrichmentAutopilotPolicy {
	if strings.TrimSpace(tenantID) == "" {
		tenantID = defaultCirculationTenant
	}
	var policy models.EnrichmentAutopilotPolicy
	if err := db.Where("tenant_id = ?", tenantID).First(&policy).Error; err != nil {
		policy = models.DefaultEnrichmentAutopilotPolicy(tenantID)
	}
	return sanitizeEnrichmentAutopilotPolicy(policy)
}

func sanitizeEnrichmentAutopilotPolicy(p models.EnrichmentAutopilotPolicy) models.EnrichmentAutopilotPolicy {
	if p.Mode != models.EnrichmentAutopilotModeSafeAuto {
		p.Mode = models.EnrichmentAutopilotModeObserve
	}
	p.IntervalMinutes = clampIntOrDefault(p.IntervalMinutes, 15, 1440, 360)
	p.MaxItemsPerRun = clampIntOrDefault(p.MaxItemsPerRun, 10, 2000, 200)
	p.MaxItemsPerClass = clampIntOrDefault(p.MaxItemsPerClass, 5, 1000, 100)
	p.MaxTranscriptsPerRun = clampInt(p.MaxTranscriptsPerRun, 0, 500)
	p.MaxQueueDepth = clampIntOrDefault(p.MaxQueueDepth, 1, 10000, 100)
	p.FailureBreakerPct = clampIntOrDefault(p.FailureBreakerPct, 1, 100, 30)
	p.StallWindowRuns = clampIntOrDefault(p.StallWindowRuns, 1, 20, 2)
	p.AgeFloorMinutes = clampInt(p.AgeFloorMinutes, 0, 1440)
	p.TrustMinAttempts = clampIntOrDefault(p.TrustMinAttempts, 1, 10000, 50)
	p.TrustMaxFailurePct = clampIntOrDefault(p.TrustMaxFailurePct, 1, 100, 15)
	if p.ElevatedMode != "" && p.ElevatedMode != models.EnrichmentAutopilotElevatedBackfillCatchup {
		p.ElevatedMode = ""
	}
	return p
}

func clampIntOrDefault(v, lo, hi, def int) int {
	if v == 0 {
		return def
	}
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// enrichmentAutopilotElevatedCaps applies the time-boxed backfill_catchup preset.
func enrichmentAutopilotElevatedCaps(p models.EnrichmentAutopilotPolicy) models.EnrichmentAutopilotPolicy {
	if p.ElevatedMode == "" || p.ElevatedUntil == nil || !p.ElevatedUntil.After(time.Now()) {
		p.ElevatedMode = ""
		return p
	}
	if p.ElevatedMode == models.EnrichmentAutopilotElevatedBackfillCatchup {
		p.MaxItemsPerRun *= enrichmentBackfillCatchupMultiplier
		p.MaxItemsPerClass *= enrichmentBackfillCatchupMultiplier
	}
	return p
}

// ----------------------------------------------------------------
// Trust gate (enrichment self-seeding: probation → trusted / demoted)
// ----------------------------------------------------------------

type enrichmentTrustStat struct {
	Artifact   string  `json:"artifact"`
	Attempts   int64   `json:"attempts"`
	Failures   int64   `json:"failures"`
	FailurePct float64 `json:"failure_pct"`
	State      string  `json:"state"`  // trusted | probation | demoted
	Earned     bool    `json:"earned"` // auto-allowed (state != demoted)
}

// computeEnrichmentTrust walks the recorded ledger. Only real executed attempts
// (success/error) count — Observe would_* rows never do. A class self-seeds
// through probation and is only demoted once it proves unreliable.
func computeEnrichmentTrust(db *gorm.DB, tenantID string, policy models.EnrichmentAutopilotPolicy) map[string]enrichmentTrustStat {
	out := map[string]enrichmentTrustStat{}
	type row struct {
		Artifact string
		Attempts int64
		Failures int64
	}
	var rows []row
	_ = db.Model(&models.EnrichmentAutopilotAction{}).
		Select(`artifact,
			COUNT(*) FILTER (WHERE status IN ('success','error')) AS attempts,
			COUNT(*) FILTER (WHERE status = 'error') AS failures`).
		Where("tenant_id = ? AND created_at >= ?", tenantID, time.Now().AddDate(0, 0, -enrichmentTrustWindowDays)).
		Group("artifact").
		Scan(&rows).Error
	seen := map[string]bool{}
	for _, r := range rows {
		stat := enrichmentTrustStat{Artifact: r.Artifact, Attempts: r.Attempts, Failures: r.Failures}
		if r.Attempts > 0 {
			stat.FailurePct = float64(r.Failures) * 100 / float64(r.Attempts)
		}
		stat.State = evaluateEnrichmentTrust(stat, policy)
		stat.Earned = stat.State != models.EnrichmentTrustStateDemoted
		out[r.Artifact] = stat
		seen[r.Artifact] = true
	}
	// Managed classes with no history yet start in probation (earned).
	for _, a := range enrichmentManagedArtifacts {
		if !seen[a] {
			out[a] = enrichmentTrustStat{Artifact: a, State: models.EnrichmentTrustStateProbation, Earned: true}
		}
	}
	return out
}

// evaluateEnrichmentTrust is the pure state rule — split out for tests.
func evaluateEnrichmentTrust(stat enrichmentTrustStat, policy models.EnrichmentAutopilotPolicy) string {
	if stat.Attempts >= int64(policy.TrustMinAttempts) && stat.FailurePct < float64(policy.TrustMaxFailurePct) {
		return models.EnrichmentTrustStateTrusted
	}
	if stat.Attempts >= enrichmentTrustDemotionFloor && stat.FailurePct >= float64(policy.TrustMaxFailurePct) {
		return models.EnrichmentTrustStateDemoted
	}
	return models.EnrichmentTrustStateProbation
}

// ----------------------------------------------------------------
// Embedding stall detection (single-actor rule vs. the reconcile sweep)
// ----------------------------------------------------------------

// embeddingDrainingOverWindow reports whether the READY-missing-embedding count
// is draining across the stall window. The reconcile sweep is the primary
// embedding actor; the autopilot only steps in when it stalls. Cold start
// (insufficient history) is treated as draining — defer to reconcile.
func embeddingDrainingOverWindow(db *gorm.DB, tenantID string, currentMissing int64, window int) (draining bool, haveHistory bool) {
	var runs []models.EnrichmentAutopilotRun
	_ = db.Where("tenant_id = ? AND status IN ?", tenantID,
		[]string{models.EnrichmentAutopilotRunStatusCompleted, models.EnrichmentAutopilotRunStatusPartial}).
		Order("started_at DESC").Limit(window).Find(&runs).Error
	if len(runs) < window {
		return true, false
	}
	oldest := runs[len(runs)-1]
	prevMissing, ok := missingEmbeddingFromSnapshot(oldest.StatsAfter)
	if !ok {
		return true, false
	}
	// Draining = strictly fewer gaps now than at the window's start.
	return currentMissing < prevMissing, true
}

func missingEmbeddingFromSnapshot(raw []byte) (int64, bool) {
	return classMissingFromSnapshot(raw, models.EnrichmentArtifactEmbedding)
}

func classMissingFromSnapshot(raw []byte, artifact string) (int64, bool) {
	if len(raw) == 0 {
		return 0, false
	}
	var stats enrichmentStatsResponse
	if err := json.Unmarshal(raw, &stats); err != nil {
		return 0, false
	}
	switch artifact {
	case models.EnrichmentArtifactTranscript:
		return stats.MissingTranscriptActionable, true
	case models.EnrichmentArtifactEmbedding:
		return stats.MissingEmbedding, true
	case models.EnrichmentArtifactImage:
		return stats.MissingImageEmbedding, true
	default:
		return 0, false
	}
}

// classStuckOverWindow identifies a non-draining class only when it has actually
// been attempted in the same history window; intentionally gated classes are not stuck.
func classStuckOverWindow(db *gorm.DB, tenantID, artifact string, currentMissing int64, window int) bool {
	var runs []models.EnrichmentAutopilotRun
	_ = db.Where("tenant_id = ? AND status IN ?", tenantID, []string{models.EnrichmentAutopilotRunStatusCompleted, models.EnrichmentAutopilotRunStatusPartial}).
		Order("started_at DESC").Limit(window).Find(&runs).Error
	if len(runs) < window {
		return false
	}
	oldest, ok := classMissingFromSnapshot(runs[len(runs)-1].StatsAfter, artifact)
	if !ok || currentMissing < oldest {
		return false
	}
	ids := make([]uint, 0, len(runs))
	for _, run := range runs {
		ids = append(ids, run.ID)
	}
	var attempts int64
	_ = db.Model(&models.EnrichmentAutopilotAction{}).Where("run_id IN ? AND artifact = ? AND status IN ?", ids, artifact,
		[]string{models.EnrichmentAutopilotActionStatusSuccess, models.EnrichmentAutopilotActionStatusError, models.EnrichmentAutopilotActionStatusWouldTrigger}).Count(&attempts).Error
	return attempts > 0
}

func missingForArtifact(stats enrichmentStatsResponse, artifact string) int64 {
	switch artifact {
	case models.EnrichmentArtifactTranscript:
		return stats.MissingTranscriptActionable
	case models.EnrichmentArtifactEmbedding:
		return stats.MissingEmbedding
	case models.EnrichmentArtifactImage:
		return stats.MissingImageEmbedding
	default:
		return 0
	}
}

// ----------------------------------------------------------------
// Ledger runner
// ----------------------------------------------------------------

type enrichmentAutopilotRunner struct {
	db      *gorm.DB
	run     *models.EnrichmentAutopilotRun
	policy  models.EnrichmentAutopilotPolicy
	observe bool

	usedTotal      int // items acted on/would-act across all classes (run cap)
	transcriptUsed int // transcript triggers (dedicated cap)

	success  int
	skipped  int
	errored  int
	acked    int // already-present
	attempts int // real executed attempts (breaker denominator)
	failures int // real errors (breaker numerator)

	budgetCapped bool
	breakerFired bool
	serviceGated bool
}

func (r *enrichmentAutopilotRunner) writeAction(a models.EnrichmentAutopilotAction) {
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

// classBlock writes one summary row for a class-level gate (service/trust/queue/
// stall) so the ledger stays readable instead of N identical per-item rows.
func (r *enrichmentAutopilotRunner) classBlock(artifact, guardrail, reason string) {
	status := models.EnrichmentAutopilotActionStatusSkipped
	if r.observe {
		status = models.EnrichmentAutopilotActionStatusWouldSkip
	}
	r.skipped++
	r.writeAction(models.EnrichmentAutopilotAction{
		Artifact: artifact, Status: status, Guardrail: guardrail, Reason: reason,
	})
}

// itemSkip ledgers a per-item skip (age handled at selection; this covers
// circulation_scope, budget, already_present, caps).
func (r *enrichmentAutopilotRunner) itemSkip(item *models.ContentItem, artifact, guardrail, reason string) {
	status := models.EnrichmentAutopilotActionStatusSkipped
	if r.observe {
		status = models.EnrichmentAutopilotActionStatusWouldSkip
	}
	r.skipped++
	id := item.PublicID
	r.writeAction(models.EnrichmentAutopilotAction{
		ContentID: &id, Artifact: artifact, Status: status, Guardrail: guardrail, Reason: reason,
	})
}

type enrichmentAutopilotRunOptions struct {
	Trigger   string
	CreatedBy string
}

func runEnrichmentAutopilot(db *gorm.DB, tenantID string, opts enrichmentAutopilotRunOptions) (models.EnrichmentAutopilotRun, []models.EnrichmentAutopilotAction, error) {
	if strings.TrimSpace(tenantID) == "" {
		tenantID = defaultCirculationTenant
	}
	if !tryStartEnrichmentAutopilotRun(tenantID) {
		return models.EnrichmentAutopilotRun{}, nil, errEnrichmentAutopilotAlreadyRunning
	}
	defer finishEnrichmentAutopilotRun(tenantID)

	policy := loadEnrichmentAutopilotPolicy(db, tenantID)
	if !policy.Enabled {
		return models.EnrichmentAutopilotRun{}, nil, errEnrichmentAutopilotDisabled
	}
	// Never double-load the model services against a human bulk run (plan §9).
	if bulkEnrichRunning() {
		return models.EnrichmentAutopilotRun{}, nil, errEnrichmentAutopilotBusy
	}

	trigger := strings.TrimSpace(opts.Trigger)
	if trigger == "" {
		trigger = "scheduled"
	}
	observe := policy.Mode != models.EnrichmentAutopilotModeSafeAuto
	policy = enrichmentAutopilotElevatedCaps(policy)
	now := time.Now()

	statsBefore, _ := computeEnrichmentStats(db)
	run := models.EnrichmentAutopilotRun{
		TenantID:     tenantID,
		Trigger:      trigger,
		Mode:         policy.Mode,
		ElevatedMode: policy.ElevatedMode,
		Status:       models.EnrichmentAutopilotRunStatusRunning,
		StartedAt:    now,
		CreatedBy:    opts.CreatedBy,
		StatsBefore:  marshalAutopilotJSON(statsBefore),
	}
	if run.CreatedBy == "" {
		run.CreatedBy = "automation"
	}

	// Health preconditions. A TOTAL AI outage aborts the run failed before any
	// side effect; a single service down only blocks the classes that need it.
	_, enrichErr := checkEnrichmentHealth()
	_, mediaErr := checkMediaHealth()
	if enrichErr != nil && mediaErr != nil {
		return failEnrichmentAutopilotRun(db, &run,
			fmt.Sprintf("both AI services unreachable (enrichment: %v; media: %v)", enrichErr, mediaErr))
	}
	serviceDown := map[string]error{"enrichment": enrichErr, "media": mediaErr}

	// Queue depth is advisory: Aggregation being unreachable does not stop
	// triggers (they go direct to Enrichment/Media), so treat unknown as 0.
	queueDepth := 0
	if stats, err := fetchAggregationQueueStats(); err == nil {
		queueDepth = enrichmentAIQueueDepth(stats)
	}

	if err := db.Create(&run).Error; err != nil {
		return run, nil, err
	}

	runner := &enrichmentAutopilotRunner{db: db, run: &run, policy: policy, observe: observe}
	trust := computeEnrichmentTrust(db, tenantID, policy)

	for _, artifact := range enrichmentManagedArtifacts {
		if runner.breakerFired || runner.usedTotal >= policy.MaxItemsPerRun {
			break
		}
		runner.runArtifactClass(artifact, trust[artifact], serviceDown, queueDepth, statsBefore)
	}

	finishedAt := time.Now()
	statsAfter, _ := computeEnrichmentStats(db)
	status := models.EnrichmentAutopilotRunStatusCompleted
	if runner.errored > 0 && runner.success == 0 {
		status = models.EnrichmentAutopilotRunStatusFailed
	} else if runner.errored > 0 {
		status = models.EnrichmentAutopilotRunStatusPartial
	}
	headline := runner.computeHeadline(statsAfter)

	verb := "triggered"
	if observe {
		verb = "would trigger"
	}
	summary := fmt.Sprintf("%s %d (+%d already present), skipped %d, errors %d",
		verb, runner.success, runner.acked, runner.skipped, runner.errored)
	stuck := []string{}
	for _, artifact := range enrichmentManagedArtifacts {
		if classStuckOverWindow(db, tenantID, artifact, missingForArtifact(statsAfter, artifact), policy.StallWindowRuns) {
			status := models.EnrichmentAutopilotActionStatusSkipped
			if observe {
				status = models.EnrichmentAutopilotActionStatusWouldSkip
			}
			runner.writeAction(models.EnrichmentAutopilotAction{Artifact: artifact, Status: status, Guardrail: models.EnrichmentAutopilotGuardEscalateStuck,
				Reason: fmt.Sprintf("%s gaps remain at %d across %d runs despite attempted automation; human attention required.", artifact, missingForArtifact(statsAfter, artifact), policy.StallWindowRuns)})
			stuck = append(stuck, artifact)
		}
	}
	if len(stuck) > 0 {
		summary += " · escalated: " + strings.Join(stuck, ", ")
	}
	errText := ""
	if status == models.EnrichmentAutopilotRunStatusFailed {
		errText = "all executed autopilot triggers failed"
	}

	_ = db.Model(&models.EnrichmentAutopilotRun{}).Where("id = ?", run.ID).Updates(map[string]interface{}{
		"status": status, "headline": headline, "finished_at": finishedAt, "summary": summary,
		"stats_after": marshalAutopilotJSON(statsAfter), "error": errText, "updated_at": finishedAt,
	}).Error
	touchEnrichmentAutopilotLastRun(db, tenantID, finishedAt)

	run.Status = status
	run.Headline = headline
	run.FinishedAt = &finishedAt
	run.Summary = summary
	run.StatsAfter = marshalAutopilotJSON(statsAfter)
	run.Error = errText

	var actions []models.EnrichmentAutopilotAction
	_ = db.Where("tenant_id = ? AND run_id = ?", tenantID, run.ID).
		Order("started_at ASC, id ASC").Find(&actions).Error
	return run, actions, nil
}

// runArtifactClass evaluates the class-level gates, then per-item triggers under
// caps + the failure breaker.
func (r *enrichmentAutopilotRunner) runArtifactClass(artifact string, trust enrichmentTrustStat, serviceDown map[string]error, queueDepth int, statsBefore enrichmentStatsResponse) {
	// Service gate.
	if err := serviceDown[enrichmentArtifactService(artifact)]; err != nil {
		r.serviceGated = true
		r.classBlock(artifact, models.EnrichmentAutopilotGuardServiceDown,
			fmt.Sprintf("%s service unreachable: %v", enrichmentArtifactService(artifact), err))
		return
	}
	// Trust gate (demoted classes held to approval).
	if !trust.Earned {
		r.classBlock(artifact, models.EnrichmentAutopilotGuardTrustGate,
			fmt.Sprintf("Class %q demoted: failure rate %.0f%% over %d attempts ≥ %d%% cap; held for review.",
				artifact, trust.FailurePct, trust.Attempts, r.policy.TrustMaxFailurePct))
		return
	}
	// Queue-depth gate (embedding + image ride the ai-queue/embedder).
	if artifact != models.EnrichmentArtifactTranscript && queueDepth > r.policy.MaxQueueDepth {
		r.classBlock(artifact, models.EnrichmentAutopilotGuardQueueDepth,
			fmt.Sprintf("Aggregation ai-queue depth %d exceeds the %d cap — pipeline is already saturating the embedder.", queueDepth, r.policy.MaxQueueDepth))
		return
	}
	// Embedding stall gate (single-actor rule vs. the reconcile sweep).
	if artifact == models.EnrichmentArtifactEmbedding {
		draining, _ := embeddingDrainingOverWindow(r.db, r.run.TenantID, statsBefore.MissingEmbedding, r.policy.StallWindowRuns)
		if draining {
			r.classBlock(artifact, models.EnrichmentAutopilotGuardReconcileDraining,
				fmt.Sprintf("Embedding backlog is draining (missing=%d) — the reconcile sweep is handling it; autopilot steps in only on a stall.", statsBefore.MissingEmbedding))
			return
		}
	}

	if artifact == models.EnrichmentArtifactTranscript {
		var excluded int64
		_ = buildMissingQuery(r.db, artifact, "VIDEO,PODCAST", "READY").Where("duration_sec > 2400").Count(&excluded).Error
		if excluded > 0 {
			status := models.EnrichmentAutopilotActionStatusSkipped
			if r.observe {
				status = models.EnrichmentAutopilotActionStatusWouldSkip
			}
			r.skipped++
			r.writeAction(models.EnrichmentAutopilotAction{Artifact: artifact, Status: status, Guardrail: models.EnrichmentAutopilotGuardCirculationScope,
				Reason: fmt.Sprintf("%d parents >40m excluded — atomization/circulation owns their transcripts.", excluded)})
		}
	}

	// Select gap candidates for this class (age floor and circulation scope in SQL).
	items := r.selectClassCandidates(artifact)
	for i := range items {
		if r.breakerFired {
			return
		}
		if r.usedTotal >= r.policy.MaxItemsPerRun {
			r.classBlock(artifact, models.EnrichmentAutopilotGuardRunCap,
				fmt.Sprintf("Run item cap (%d) reached.", r.policy.MaxItemsPerRun))
			return
		}
		if bulkEnrichRunning() {
			r.classBlock(artifact, models.EnrichmentAutopilotGuardBulkInFlight,
				"A manual bulk run started — autopilot yields the lane (manual pre-empts).")
			return
		}
		r.dispatchItem(&items[i], artifact)
		r.maybeTripBreaker()
	}
}

// selectClassCandidates loads the gap items for one artifact class under the
// per-class cap. Transcript selection excludes circulation-scope parents; the
// in-loop check remains an invariant belt against accidental query drift.
func (r *enrichmentAutopilotRunner) selectClassCandidates(artifact string) []models.ContentItem {
	contentType := ""
	if artifact == models.EnrichmentArtifactTranscript {
		contentType = "VIDEO,PODCAST"
	}
	query := buildMissingQuery(r.db, artifact, contentType, "READY")
	if artifact == models.EnrichmentArtifactTranscript {
		query = query.Where("(duration_sec IS NULL OR duration_sec <= 2400)")
	}
	if r.policy.AgeFloorMinutes > 0 {
		floor := time.Now().Add(-time.Duration(r.policy.AgeFloorMinutes) * time.Minute)
		query = query.Where("created_at < ?", floor)
	}
	var items []models.ContentItem
	_ = query.Order("created_at DESC").Limit(r.policy.MaxItemsPerClass).Find(&items).Error
	return items
}

// dispatchItem handles one (item × artifact): pure pre-checks, then the terminal
// trigger (suppressed in Observe). Caps are consumed only on a real attempt.
func (r *enrichmentAutopilotRunner) dispatchItem(item *models.ContentItem, artifact string) {
	startedAt := time.Now()

	// Invariant belt: selection already excludes >40m circulation-scope parents.
	if artifact == models.EnrichmentArtifactTranscript && item.DurationSec != nil && *item.DurationSec > 2400 {
		r.itemSkip(item, artifact, models.EnrichmentAutopilotGuardCirculationScope,
			fmt.Sprintf("Parent is %ds (>40m) — atomization/circulation owns its transcript.", *item.DurationSec))
		return
	}
	// A queued/running STT job has already committed the spend; never double bill.
	if artifact == models.EnrichmentArtifactTranscript && hasActiveTranscriptionJob(r.db, item.PublicID) {
		r.itemSkip(item, artifact, models.EnrichmentAutopilotGuardAlreadyPresent, "An STT job for this item is already queued/running.")
		return
	}
	// Transcript dedicated per-run cap (each is a billable Deepgram call).
	if artifact == models.EnrichmentArtifactTranscript && r.transcriptUsed >= r.policy.MaxTranscriptsPerRun {
		r.itemSkip(item, artifact, models.EnrichmentAutopilotGuardClassCap,
			fmt.Sprintf("Transcript cap (%d per run) reached.", r.policy.MaxTranscriptsPerRun))
		return
	}

	id := item.PublicID

	// Observe proves the same read-only guards as Safe Auto without triggering.
	if r.observe {
		if artifact == models.EnrichmentArtifactTranscript {
			if admit, reason := evaluateSTTAdmission(r.db, item, models.TranscriptionTriggerEnrichmentAutopilot); !admit {
				guardrail := models.EnrichmentAutopilotGuardAlreadyPresent
				if reason == "monthly STT budget cap reached" {
					guardrail = models.EnrichmentAutopilotGuardBudget
					r.budgetCapped = true
				}
				r.itemSkip(item, artifact, guardrail, reason)
				return
			}
		}
		if artifact == models.EnrichmentArtifactEmbedding && item.Embedding != nil {
			r.itemSkip(item, artifact, models.EnrichmentAutopilotGuardAlreadyPresent, "already exists")
			return
		}
		if artifact == models.EnrichmentArtifactImage && item.ImageEmbedding != nil {
			r.itemSkip(item, artifact, models.EnrichmentAutopilotGuardAlreadyPresent, "already exists")
			return
		}
		r.usedTotal++
		if artifact == models.EnrichmentArtifactTranscript {
			r.transcriptUsed++
		}
		r.success++
		finishedAt := time.Now()
		r.writeAction(models.EnrichmentAutopilotAction{
			ContentID: &id, Artifact: artifact,
			Status:    models.EnrichmentAutopilotActionStatusWouldTrigger,
			Reason:    "Would trigger (Observe dry-run).",
			StartedAt: startedAt, FinishedAt: &finishedAt,
			DurationMs: int(finishedAt.Sub(startedAt).Milliseconds()),
		})
		return
	}

	// Safe Auto: execute through the shared traced path with autopilot attribution.
	outcomes := triggerItemArtifactsTraced(r.db, item, []string{artifact}, false, models.TranscriptionTriggerEnrichmentAutopilot)
	finishedAt := time.Now()
	action := models.EnrichmentAutopilotAction{
		ContentID: &id, Artifact: artifact,
		StartedAt: startedAt, FinishedAt: &finishedAt,
		DurationMs: int(finishedAt.Sub(startedAt).Milliseconds()),
	}
	if len(outcomes) == 0 {
		action.Status = models.EnrichmentAutopilotActionStatusSkipped
		action.Guardrail = models.EnrichmentAutopilotGuardAlreadyPresent
		action.Reason = "Nothing to do for this artifact."
		r.skipped++
		r.writeAction(action)
		return
	}
	o := outcomes[0]
	switch o.Status {
	case artifactOutcomeTriggered:
		r.usedTotal++
		r.attempts++
		r.success++
		if artifact == models.EnrichmentArtifactTranscript {
			r.transcriptUsed++
			if o.JobID != "" {
				if jid, err := uuid.Parse(o.JobID); err == nil {
					action.TranscriptionJobID = &jid
				}
			}
		}
		action.Status = models.EnrichmentAutopilotActionStatusSuccess
		action.Reason = "Triggered through the shared enrichment path."
		r.writeAction(action)
	case artifactOutcomeAlready:
		r.acked++
		action.Status = models.EnrichmentAutopilotActionStatusSkipped
		action.Guardrail = models.EnrichmentAutopilotGuardAlreadyPresent
		action.Reason = o.Reason
		r.writeAction(action)
	case artifactOutcomeSkipped:
		r.skipped++
		action.Status = models.EnrichmentAutopilotActionStatusSkipped
		if o.SkipKind == string(sttSkipBudget) {
			action.Guardrail = models.EnrichmentAutopilotGuardBudget
			r.budgetCapped = true
		} else {
			action.Guardrail = models.EnrichmentAutopilotGuardAlreadyPresent
		}
		action.Reason = o.Reason
		r.writeAction(action)
	default: // error
		r.usedTotal++
		r.attempts++
		r.failures++
		r.errored++
		action.Status = models.EnrichmentAutopilotActionStatusError
		action.Reason = o.Reason
		r.writeAction(action)
	}
}

// maybeTripBreaker stops the whole run early if the executed error rate spikes
// (min 10 attempts) — don't grind through a broken downstream.
func (r *enrichmentAutopilotRunner) maybeTripBreaker() {
	if r.attempts < 10 {
		return
	}
	if float64(r.failures)*100/float64(r.attempts) > float64(r.policy.FailureBreakerPct) {
		r.breakerFired = true
		r.writeAction(models.EnrichmentAutopilotAction{
			Artifact: "run", Status: models.EnrichmentAutopilotActionStatusSkipped,
			Guardrail: models.EnrichmentAutopilotGuardBreakerTripped,
			Reason: fmt.Sprintf("Failure breaker: %d/%d attempts errored (> %d%%). Run stopped.",
				r.failures, r.attempts, r.policy.FailureBreakerPct),
		})
	}
}

func (r *enrichmentAutopilotRunner) computeHeadline(statsAfter enrichmentStatsResponse) string {
	if r.serviceGated || r.breakerFired || (r.attempts >= 10 && float64(r.failures)*100/float64(r.attempts) > float64(r.policy.FailureBreakerPct)) {
		return models.EnrichmentAutopilotHeadlineDegraded
	}
	gaps := statsAfter.MissingTranscriptActionable + statsAfter.MissingEmbedding + statsAfter.MissingImageEmbedding
	if gaps == 0 {
		return models.EnrichmentAutopilotHeadlineFullyEnriched
	}
	if r.budgetCapped {
		return models.EnrichmentAutopilotHeadlineBudgetCapped
	}
	return models.EnrichmentAutopilotHeadlineBacklog
}

// enrichmentAIQueueDepth: the ai-queue is where enrichment triggers land.
func enrichmentAIQueueDepth(stats []autopilotQueueStat) int {
	for _, s := range stats {
		if s.Queue == "ai-queue" {
			return s.Waiting + s.Active + s.Delayed
		}
	}
	return 0
}

func failEnrichmentAutopilotRun(db *gorm.DB, run *models.EnrichmentAutopilotRun, reason string) (models.EnrichmentAutopilotRun, []models.EnrichmentAutopilotAction, error) {
	finishedAt := time.Now()
	run.Status = models.EnrichmentAutopilotRunStatusFailed
	run.Headline = models.EnrichmentAutopilotHeadlineDegraded
	run.FinishedAt = &finishedAt
	run.Summary = "Precondition failed: " + reason
	run.Error = reason
	// The run row may not be persisted yet (precondition runs before Create in the
	// happy path); create-or-update so the failure is always recorded.
	if run.ID == 0 {
		_ = db.Create(run).Error
	} else {
		_ = db.Model(&models.EnrichmentAutopilotRun{}).Where("id = ?", run.ID).Updates(map[string]interface{}{
			"status": run.Status, "headline": run.Headline, "finished_at": finishedAt,
			"summary": run.Summary, "error": run.Error, "updated_at": finishedAt,
		}).Error
	}
	touchEnrichmentAutopilotLastRun(db, run.TenantID, finishedAt)
	return *run, nil, nil
}

func touchEnrichmentAutopilotLastRun(db *gorm.DB, tenantID string, at time.Time) {
	_ = db.Model(&models.EnrichmentAutopilotPolicy{}).
		Where("tenant_id = ?", tenantID).
		Updates(map[string]interface{}{"last_run_at": at, "updated_at": at}).Error
}

// ----------------------------------------------------------------
// Status block (cockpit read model)
// ----------------------------------------------------------------

type enrichmentAutopilotStatusBlock struct {
	Enabled           bool                             `json:"enabled"`
	Mode              string                           `json:"mode"`
	State             string                           `json:"state"` // off | observe | safe_auto | elevated | paused
	IntervalMinutes   int                              `json:"interval_minutes"`
	ElevatedMode      string                           `json:"elevated_mode,omitempty"`
	ElevatedUntil     *time.Time                       `json:"elevated_until,omitempty"`
	PausedUntil       *time.Time                       `json:"paused_until,omitempty"`
	LastRunAt         *time.Time                       `json:"last_run_at,omitempty"`
	NextRunAt         *time.Time                       `json:"next_run_at,omitempty"`
	LastRun           *models.EnrichmentAutopilotRun   `json:"last_run,omitempty"`
	Trust             []enrichmentTrustStat            `json:"trust"`
	RecommendedAction string                           `json:"recommended_action,omitempty"`
	Attention         []enrichmentAttentionItem        `json:"attention"`
	Policy            models.EnrichmentAutopilotPolicy `json:"policy"`
}

type enrichmentAttentionItem struct {
	Kind     string `json:"kind"`
	Artifact string `json:"artifact,omitempty"`
	Message  string `json:"message"`
	Target   string `json:"target"`
}

func attentionTargetFor(artifact, lastGuardrail string) string {
	if artifact == models.EnrichmentArtifactTranscript {
		return "media_studio"
	}
	if (artifact == models.EnrichmentArtifactEmbedding || artifact == models.EnrichmentArtifactImage) && lastGuardrail == models.EnrichmentAutopilotGuardQueueDepth {
		return "pipeline"
	}
	return "missing_panel"
}

func lastGuardrailForArtifact(db *gorm.DB, tenantID, artifact string) string {
	var action models.EnrichmentAutopilotAction
	if err := db.Where("tenant_id = ? AND artifact = ?", tenantID, artifact).Order("created_at DESC, id DESC").First(&action).Error; err != nil {
		return ""
	}
	return action.Guardrail
}

func buildEnrichmentAutopilotStatus(db *gorm.DB, tenantID string, policy models.EnrichmentAutopilotPolicy) enrichmentAutopilotStatusBlock {
	now := time.Now()
	block := enrichmentAutopilotStatusBlock{
		Enabled:         policy.Enabled,
		Mode:            policy.Mode,
		IntervalMinutes: policy.IntervalMinutes,
		ElevatedMode:    policy.ElevatedMode,
		ElevatedUntil:   policy.ElevatedUntil,
		PausedUntil:     policy.PausedUntil,
		LastRunAt:       policy.LastRunAt,
		Trust:           []enrichmentTrustStat{},
		Attention:       []enrichmentAttentionItem{},
		Policy:          policy,
	}
	switch {
	case !policy.Enabled:
		block.State = "off"
	case policy.PausedUntil != nil && policy.PausedUntil.After(now):
		block.State = "paused"
	case policy.ElevatedMode != "" && policy.ElevatedUntil != nil && policy.ElevatedUntil.After(now):
		block.State = "elevated"
	default:
		block.State = policy.Mode
	}
	if policy.Enabled && block.State != "paused" {
		next := now
		if policy.LastRunAt != nil {
			next = policy.LastRunAt.Add(time.Duration(policy.IntervalMinutes) * time.Minute)
		}
		block.NextRunAt = &next
	}

	var lastRun models.EnrichmentAutopilotRun
	if err := db.Where("tenant_id = ?", tenantID).Order("started_at DESC").First(&lastRun).Error; err == nil {
		block.LastRun = &lastRun
	}

	trust := computeEnrichmentTrust(db, tenantID, policy)
	artifacts := make([]string, 0, len(trust))
	for a := range trust {
		artifacts = append(artifacts, a)
	}
	sort.Strings(artifacts)
	trusted := []string{}
	for _, a := range artifacts {
		block.Trust = append(block.Trust, trust[a])
		if trust[a].State == models.EnrichmentTrustStateTrusted {
			trusted = append(trusted, a)
		}
	}
	stats, _ := computeEnrichmentStats(db)
	for _, a := range enrichmentManagedArtifacts {
		if classStuckOverWindow(db, tenantID, a, missingForArtifact(stats, a), policy.StallWindowRuns) {
			block.Attention = append(block.Attention, enrichmentAttentionItem{Kind: "stuck_class", Artifact: a,
				Message: fmt.Sprintf("%s coverage is not draining despite attempted automation.", a), Target: attentionTargetFor(a, lastGuardrailForArtifact(db, tenantID, a))})
		}
	}
	if block.LastRun != nil && block.LastRun.Headline == models.EnrichmentAutopilotHeadlineBudgetCapped {
		block.Attention = append(block.Attention, enrichmentAttentionItem{Kind: "budget_capped", Message: "STT budget is capped; review transcription configuration.", Target: "transcription_config"})
	}
	for _, a := range artifacts {
		if trust[a].State == models.EnrichmentTrustStateDemoted {
			block.Attention = append(block.Attention, enrichmentAttentionItem{Kind: "demoted_class", Artifact: a, Message: fmt.Sprintf("%s is held after failures; use manual triggers or reset trust.", a), Target: "missing_panel"})
		}
	}

	switch {
	case !policy.Enabled:
		block.RecommendedAction = "Enable Autopilot in Observe mode — it runs the full pipeline as a dry-run ledger with zero side effects."
	case policy.Mode == models.EnrichmentAutopilotModeObserve && len(trusted) > 0:
		block.RecommendedAction = fmt.Sprintf("Enable Safe Auto — %s earned trust (%s).",
			pluralizeArtifacts(trusted), strings.Join(trusted, ", "))
	case policy.Mode == models.EnrichmentAutopilotModeObserve:
		block.RecommendedAction = "Keep observing — successful triggers build each class's execution track record."
	}
	return block
}

func pluralizeArtifacts(trusted []string) string {
	if len(trusted) == 1 {
		return "1 artifact class has"
	}
	return fmt.Sprintf("%d artifact classes have", len(trusted))
}

// ----------------------------------------------------------------
// Scheduler heartbeat (Slice 3)
// ----------------------------------------------------------------

// StartEnrichmentAutopilotHeartbeat launches the scheduled loop: a one-minute
// ticker fires runs for tenants whose interval has elapsed. One run per tenant at
// a time; pause and disable are respected without touching policy. Mirrors the
// Media/News heartbeat pattern.
func StartEnrichmentAutopilotHeartbeat(db *gorm.DB) {
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			withEnrichmentAutopilotRecovery("heartbeat", func() { runEnrichmentAutopilotDue(db) })
		}
	}()
}

// withEnrichmentAutopilotRecovery prevents one malformed row or downstream edge
// case from permanently killing the heartbeat goroutine.
func withEnrichmentAutopilotRecovery(tenantID string, fn func()) {
	defer func() {
		if rec := recover(); rec != nil {
			log.Printf("enrichment autopilot: recovered panic for tenant %s: %v", tenantID, rec)
		}
	}()
	fn()
}

func runEnrichmentAutopilotDue(db *gorm.DB) {
	var policies []models.EnrichmentAutopilotPolicy
	if err := db.Where("enabled = ?", true).Find(&policies).Error; err != nil {
		return
	}
	now := time.Now()
	for _, raw := range policies {
		policy := sanitizeEnrichmentAutopilotPolicy(raw)
		if policy.PausedUntil != nil && policy.PausedUntil.After(now) {
			continue
		}
		if raw.LastRunAt != nil &&
			now.Sub(*raw.LastRunAt) < time.Duration(policy.IntervalMinutes)*time.Minute {
			continue
		}
		policyCopy := policy
		withEnrichmentAutopilotRecovery(policy.TenantID, func() { runEnrichmentAutopilotForTenantSafely(db, policyCopy) })
	}
}

func runEnrichmentAutopilotForTenantSafely(db *gorm.DB, policy models.EnrichmentAutopilotPolicy) {
	run, _, err := runEnrichmentAutopilot(db, policy.TenantID, enrichmentAutopilotRunOptions{Trigger: "scheduled", CreatedBy: "automation"})
	if errors.Is(err, errEnrichmentAutopilotAlreadyRunning) || errors.Is(err, errEnrichmentAutopilotBusy) {
		return
	}
	payload := map[string]interface{}{"status": run.Status, "summary": run.Summary, "headline": run.Headline}
	if err != nil {
		payload["error"] = err.Error()
	}
	writeCirculationAuditSystem(db, policy.TenantID, "enrichment.autopilot.scheduled", policy.TenantID, payload)
}

// ----------------------------------------------------------------
// Admin endpoints
// ----------------------------------------------------------------

// GET /admin/enrichment/autopilot
func GetEnrichmentAutopilot(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	policy := loadEnrichmentAutopilotPolicy(db, principal.TenantID)
	c.JSON(http.StatusOK, gin.H{"data": buildEnrichmentAutopilotStatus(db, principal.TenantID, policy)})
}

type updateEnrichmentAutopilotRequest struct {
	Enabled              *bool   `json:"enabled"`
	Mode                 *string `json:"mode"`
	IntervalMinutes      *int    `json:"interval_minutes"`
	MaxItemsPerRun       *int    `json:"max_items_per_run"`
	MaxItemsPerClass     *int    `json:"max_items_per_class"`
	MaxTranscriptsPerRun *int    `json:"max_transcripts_per_run"`
	MaxQueueDepth        *int    `json:"max_queue_depth"`
	FailureBreakerPct    *int    `json:"failure_breaker_pct"`
	StallWindowRuns      *int    `json:"stall_window_runs"`
	AgeFloorMinutes      *int    `json:"age_floor_minutes"`
	TrustMinAttempts     *int    `json:"trust_min_attempts"`
	TrustMaxFailurePct   *int    `json:"trust_max_failure_pct"`
}

// PUT /admin/enrichment/autopilot/policy
func UpdateEnrichmentAutopilotPolicy(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var req updateEnrichmentAutopilotRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request: " + err.Error(), Code: "INVALID_REQUEST"})
		return
	}

	var policy models.EnrichmentAutopilotPolicy
	if err := db.Where("tenant_id = ?", principal.TenantID).First(&policy).Error; err != nil {
		policy = models.DefaultEnrichmentAutopilotPolicy(principal.TenantID)
	}

	if req.Enabled != nil {
		policy.Enabled = *req.Enabled
	}
	if req.Mode != nil {
		policy.Mode = *req.Mode
	}
	if req.IntervalMinutes != nil {
		policy.IntervalMinutes = *req.IntervalMinutes
	}
	if req.MaxItemsPerRun != nil {
		policy.MaxItemsPerRun = *req.MaxItemsPerRun
	}
	if req.MaxItemsPerClass != nil {
		policy.MaxItemsPerClass = *req.MaxItemsPerClass
	}
	if req.MaxTranscriptsPerRun != nil {
		policy.MaxTranscriptsPerRun = *req.MaxTranscriptsPerRun
	}
	if req.MaxQueueDepth != nil {
		policy.MaxQueueDepth = *req.MaxQueueDepth
	}
	if req.FailureBreakerPct != nil {
		policy.FailureBreakerPct = *req.FailureBreakerPct
	}
	if req.StallWindowRuns != nil {
		policy.StallWindowRuns = *req.StallWindowRuns
	}
	if req.AgeFloorMinutes != nil {
		policy.AgeFloorMinutes = *req.AgeFloorMinutes
	}
	if req.TrustMinAttempts != nil {
		policy.TrustMinAttempts = *req.TrustMinAttempts
	}
	if req.TrustMaxFailurePct != nil {
		policy.TrustMaxFailurePct = *req.TrustMaxFailurePct
	}
	policy.TenantID = principal.TenantID
	policy = sanitizeEnrichmentAutopilotPolicy(policy)

	if err := db.Save(&policy).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to save policy", Code: "SAVE_FAILED"})
		return
	}
	writeCirculationAudit(db, principal, "enrichment.autopilot.policy", principal.TenantID, map[string]interface{}{
		"enabled": policy.Enabled, "mode": policy.Mode,
	})
	c.JSON(http.StatusOK, gin.H{"data": buildEnrichmentAutopilotStatus(db, principal.TenantID, policy)})
}

// POST /admin/enrichment/autopilot/run
func RunEnrichmentAutopilotNow(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	run, actions, err := runEnrichmentAutopilot(db, principal.TenantID, enrichmentAutopilotRunOptions{
		Trigger:   "manual",
		CreatedBy: principal.Email,
	})
	if err != nil {
		switch {
		case errors.Is(err, errEnrichmentAutopilotDisabled):
			c.JSON(http.StatusConflict, authErrorResponse{Message: err.Error(), Code: "AUTOPILOT_DISABLED"})
		case errors.Is(err, errEnrichmentAutopilotAlreadyRunning):
			c.JSON(http.StatusConflict, authErrorResponse{Message: err.Error(), Code: "AUTOPILOT_ALREADY_RUNNING"})
		case errors.Is(err, errEnrichmentAutopilotBusy):
			c.JSON(http.StatusConflict, authErrorResponse{Message: err.Error(), Code: "BULK_IN_FLIGHT"})
		default:
			c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Autopilot run failed: " + err.Error(), Code: "RUN_FAILED"})
		}
		return
	}
	writeCirculationAudit(db, principal, "enrichment.autopilot.run", principal.TenantID, map[string]interface{}{
		"run_id": run.PublicID.String(), "status": run.Status, "headline": run.Headline, "summary": run.Summary,
	})
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"run": run, "actions": actions}})
}

// POST /admin/enrichment/autopilot/pause
func PauseEnrichmentAutopilot(c *gin.Context) {
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
	var until *time.Time
	if req.Minutes > 0 {
		minutes := req.Minutes
		if minutes > 10080 {
			minutes = 10080
		}
		t := time.Now().Add(time.Duration(minutes) * time.Minute)
		until = &t
	}
	ensureEnrichmentAutopilotPolicyRow(db, principal.TenantID)
	res := db.Model(&models.EnrichmentAutopilotPolicy{}).
		Where("tenant_id = ?", principal.TenantID).
		Updates(map[string]interface{}{"paused_until": until, "updated_at": time.Now()})
	if res.Error != nil || res.RowsAffected == 0 {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to update pause state", Code: "SAVE_FAILED"})
		return
	}
	action := "enrichment.autopilot.pause"
	if until == nil {
		action = "enrichment.autopilot.resume"
	}
	writeCirculationAudit(db, principal, action, principal.TenantID, map[string]interface{}{"paused_until": until})
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"paused_until": until}})
}

// POST /admin/enrichment/autopilot/elevate
func ElevateEnrichmentAutopilot(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var req struct {
		Mode    string `json:"mode"` // "" = clear
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
		updates["elevated_mode"] = ""
		updates["elevated_until"] = nil
	case models.EnrichmentAutopilotElevatedBackfillCatchup:
		minutes := req.Minutes
		if minutes <= 0 {
			minutes = 1440 // default 24h catch-up window
		}
		if minutes < 15 {
			minutes = 15
		}
		if minutes > 4320 { // max 3 days
			minutes = 4320
		}
		t := time.Now().Add(time.Duration(minutes) * time.Minute)
		until = &t
		updates["elevated_mode"] = req.Mode
		updates["elevated_until"] = until
	default:
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Unknown elevated mode: " + req.Mode, Code: "INVALID_MODE"})
		return
	}
	ensureEnrichmentAutopilotPolicyRow(db, principal.TenantID)
	res := db.Model(&models.EnrichmentAutopilotPolicy{}).Where("tenant_id = ?", principal.TenantID).Updates(updates)
	if res.Error != nil || res.RowsAffected == 0 {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to update elevated mode", Code: "SAVE_FAILED"})
		return
	}
	writeCirculationAudit(db, principal, "enrichment.autopilot.elevate", principal.TenantID, map[string]interface{}{
		"mode": req.Mode, "until": until,
	})
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"mode": req.Mode, "until": until}})
}

// ensureEnrichmentAutopilotPolicyRow makes pause/elevate meaningful even before
// settings have been saved. The default is inert: disabled Observe mode.
func ensureEnrichmentAutopilotPolicyRow(db *gorm.DB, tenantID string) {
	p := models.DefaultEnrichmentAutopilotPolicy(tenantID)
	_ = db.Where("tenant_id = ?", tenantID).FirstOrCreate(&p).Error
}

func isManagedEnrichmentArtifact(artifact string) bool {
	for _, managed := range enrichmentManagedArtifacts {
		if artifact == managed {
			return true
		}
	}
	return false
}

// POST /admin/enrichment/autopilot/trust/reset
func ResetEnrichmentAutopilotTrust(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	var req struct {
		Artifact string `json:"artifact"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || !isManagedEnrichmentArtifact(req.Artifact) {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "A managed artifact is required", Code: "INVALID_ARTIFACT"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	res := db.Model(&models.EnrichmentAutopilotAction{}).
		Where("tenant_id = ? AND artifact = ? AND status = ?", principal.TenantID, req.Artifact, models.EnrichmentAutopilotActionStatusError).
		Updates(map[string]interface{}{"status": models.EnrichmentAutopilotActionStatusErrorAcknowledged, "updated_at": time.Now()})
	if res.Error != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to reset trust", Code: "SAVE_FAILED"})
		return
	}
	writeCirculationAudit(db, principal, "enrichment.autopilot.trust_reset", principal.TenantID, map[string]interface{}{"artifact": req.Artifact, "rows": res.RowsAffected})
	c.JSON(http.StatusOK, gin.H{"data": buildEnrichmentAutopilotStatus(db, principal.TenantID, loadEnrichmentAutopilotPolicy(db, principal.TenantID))})
}

// GET /admin/enrichment/autopilot/runs
func ListEnrichmentAutopilotRuns(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var runs []models.EnrichmentAutopilotRun
	if err := db.Where("tenant_id = ?", principal.TenantID).
		Order("started_at DESC").Limit(boundedLimit(c.Query("limit"), 20, 100)).
		Find(&runs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to list runs", Code: "QUERY_FAILED"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"items": runs}})
}

// GET /admin/enrichment/autopilot/runs/:id
func GetEnrichmentAutopilotRun(c *gin.Context) {
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
	var run models.EnrichmentAutopilotRun
	if err := db.Where("tenant_id = ? AND public_id = ?", principal.TenantID, runID).First(&run).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Run not found", Code: "NOT_FOUND"})
		return
	}
	var actions []models.EnrichmentAutopilotAction
	_ = db.Where("tenant_id = ? AND run_id = ?", principal.TenantID, run.ID).
		Order("started_at ASC, id ASC").Find(&actions).Error
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"run": run, "actions": actions}})
}
