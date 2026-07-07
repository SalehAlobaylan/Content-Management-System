package controllers

import (
	"encoding/json"
	"errors"
	"fmt"
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

const (
	pipelineBacklogDrainMultiplier = 3
	pipelineOutcomeDeadlineHours   = 48
	pipelineSourceBrokenMinFailed  = 10
	pipelineSourceBrokenPct        = 60
)

var (
	errPipelineAutopilotDisabled       = errors.New("pipeline autopilot is not enabled for this tenant")
	errPipelineAutopilotAlreadyRunning = errors.New("pipeline autopilot is already running for this tenant")
)

var (
	pipelineAutopilotRunMu       sync.Mutex
	pipelineAutopilotRunInFlight = map[string]bool{}
)

func tryStartPipelineAutopilotRun(tenantID string) bool {
	pipelineAutopilotRunMu.Lock()
	defer pipelineAutopilotRunMu.Unlock()
	if pipelineAutopilotRunInFlight[tenantID] {
		return false
	}
	pipelineAutopilotRunInFlight[tenantID] = true
	return true
}

func finishPipelineAutopilotRun(tenantID string) {
	pipelineAutopilotRunMu.Lock()
	defer pipelineAutopilotRunMu.Unlock()
	delete(pipelineAutopilotRunInFlight, tenantID)
}

type pipelineRetryResponse struct {
	Success  bool     `json:"success"`
	Message  string   `json:"message"`
	Requeued int      `json:"requeued"`
	Total    int      `json:"total"`
	Errors   []string `json:"errors"`
}

type pipelineHealthSnapshot struct {
	Timestamp          string               `json:"timestamp"`
	StatusCounts       map[string]int64     `json:"status_counts"`
	StuckCount         int64                `json:"stuck_count"`
	OldestUnprocessed  *string              `json:"oldest_unprocessed,omitempty"`
	Queues             []autopilotQueueStat `json:"queues"`
	QueueDepth         int                  `json:"queue_depth"`
	DLQDepth           int                  `json:"dlq_depth"`
	AggregationHealthy bool                 `json:"aggregation_healthy"`
}

type pipelineTrustStat struct {
	Lane       string  `json:"lane"`
	Outcomes   int64   `json:"outcomes"`
	Recovered  int64   `json:"recovered"`
	Failed     int64   `json:"failed"`
	SuccessPct float64 `json:"success_pct"`
	State      string  `json:"state"`
	Earned     bool    `json:"earned"`
}

type pipelineCohortSummary struct {
	Lane        string   `json:"lane"`
	Verdict     string   `json:"verdict"`
	Count       int      `json:"count"`
	TargetQueue string   `json:"target_queue,omitempty"`
	Source      string   `json:"source,omitempty"`
	ItemIDs     []string `json:"item_ids,omitempty"`
}

type pipelineAutopilotStatusBlock struct {
	Enabled           bool                           `json:"enabled"`
	Mode              string                         `json:"mode"`
	State             string                         `json:"state"`
	IntervalMinutes   int                            `json:"interval_minutes"`
	ElevatedMode      string                         `json:"elevated_mode,omitempty"`
	ElevatedUntil     *time.Time                     `json:"elevated_until,omitempty"`
	PausedUntil       *time.Time                     `json:"paused_until,omitempty"`
	LastRunAt         *time.Time                     `json:"last_run_at,omitempty"`
	LastHealthOKAt    *time.Time                     `json:"last_health_ok_at,omitempty"`
	NextRunAt         *time.Time                     `json:"next_run_at,omitempty"`
	LastRun           *models.PipelineAutopilotRun   `json:"last_run,omitempty"`
	Trust             []pipelineTrustStat            `json:"trust"`
	Cohorts           []pipelineCohortSummary        `json:"cohorts"`
	Attention         []pipelineCohortSummary        `json:"attention"`
	RecommendedAction string                         `json:"recommended_action,omitempty"`
	Policy            models.PipelineAutopilotPolicy `json:"policy"`
}

type pipelineCandidate struct {
	Item        models.ContentItem
	Lane        string
	Verdict     string
	SourceKey   string
	TargetQueue string
}

type pipelineBatch struct {
	Lane        string
	Verdict     string
	SourceKey   string
	TargetQueue string
	Items       []models.ContentItem
}

type pipelineAutopilotRunOptions struct {
	Trigger   string
	CreatedBy string
}

func loadPipelineAutopilotPolicy(db *gorm.DB, tenantID string) models.PipelineAutopilotPolicy {
	if strings.TrimSpace(tenantID) == "" {
		tenantID = defaultCirculationTenant
	}
	var policy models.PipelineAutopilotPolicy
	if err := db.Where("tenant_id = ?", tenantID).First(&policy).Error; err != nil {
		policy = models.DefaultPipelineAutopilotPolicy(tenantID)
	}
	return sanitizePipelineAutopilotPolicy(policy)
}

func sanitizePipelineAutopilotPolicy(p models.PipelineAutopilotPolicy) models.PipelineAutopilotPolicy {
	if strings.TrimSpace(p.TenantID) == "" {
		p.TenantID = defaultCirculationTenant
	}
	if p.Mode != models.PipelineAutopilotModeSafeAuto {
		p.Mode = models.PipelineAutopilotModeObserve
	}
	p.IntervalMinutes = clampIntOrDefault(p.IntervalMinutes, 15, 1440, 180)
	p.MaxItemsPerRun = clampIntOrDefault(p.MaxItemsPerRun, 1, 500, 200)
	p.MaxBatchesPerRun = clampIntOrDefault(p.MaxBatchesPerRun, 1, 100, 4)
	p.MaxAttempts = clampIntOrDefault(p.MaxAttempts, 1, 20, 3)
	p.RetryBackoffHours = clampIntOrDefault(p.RetryBackoffHours, 1, 168, 12)
	p.PendingAgeFloorMinutes = clampIntOrDefault(p.PendingAgeFloorMinutes, 0, 1440, 30)
	p.ProcessingStuckHours = clampIntOrDefault(p.ProcessingStuckHours, 1, 24, 4)
	p.MaxQueueDepth = clampIntOrDefault(p.MaxQueueDepth, 1, 10000, 100)
	p.PerSourceDailyRetries = clampIntOrDefault(p.PerSourceDailyRetries, 1, 10000, 100)
	p.RecoveryCooldownMinutes = clampIntOrDefault(p.RecoveryCooldownMinutes, 0, 1440, 60)
	p.TrustMinOutcomes = clampIntOrDefault(p.TrustMinOutcomes, 1, 10000, 20)
	p.TrustMinSuccessPct = clampIntOrDefault(p.TrustMinSuccessPct, 1, 100, 40)
	if p.ElevatedMode != "" && p.ElevatedMode != models.PipelineAutopilotElevatedBacklogDrain {
		p.ElevatedMode = ""
	}
	return p
}

func pipelineAutopilotElevatedCaps(p models.PipelineAutopilotPolicy) models.PipelineAutopilotPolicy {
	if p.ElevatedMode == "" || p.ElevatedUntil == nil || !p.ElevatedUntil.After(time.Now()) {
		p.ElevatedMode = ""
		return p
	}
	if p.ElevatedMode == models.PipelineAutopilotElevatedBacklogDrain {
		p.MaxItemsPerRun *= pipelineBacklogDrainMultiplier
		p.MaxBatchesPerRun *= pipelineBacklogDrainMultiplier
	}
	return p
}

// evaluatePipelineTrust is the pure lane-state rule (split out for tests). A lane
// self-seeds in probation and executes while building a record; it is trusted once
// it clears the success bar over enough resolved outcomes, and demoted (held for
// human review) once it fails that bar over enough outcomes (G5/§8).
func evaluatePipelineTrust(outcomes int64, successPct float64, policy models.PipelineAutopilotPolicy) string {
	if outcomes >= int64(policy.TrustMinOutcomes) {
		if successPct >= float64(policy.TrustMinSuccessPct) {
			return models.PipelineTrustStateTrusted
		}
		return models.PipelineTrustStateDemoted
	}
	return models.PipelineTrustStateProbation
}

// pipelineCooldownActive reports whether retry execution is held after a recovery
// (G8). The window is anchored on the recovery-transition stamp and runs for the
// full duration regardless of run cadence — it does not depend on the current run
// being the transition itself.
func pipelineCooldownActive(healthOKAt *time.Time, cooldownMinutes int, now time.Time) bool {
	return healthOKAt != nil && cooldownMinutes > 0 &&
		now.Before(healthOKAt.Add(time.Duration(cooldownMinutes)*time.Minute))
}

func computePipelineTrust(db *gorm.DB, tenantID string, policy models.PipelineAutopilotPolicy) map[string]pipelineTrustStat {
	lanes := []string{models.PipelineLanePendingStuck, models.PipelineLaneFailedRetryable, models.PipelineLaneProcessingStuck}
	out := map[string]pipelineTrustStat{}
	type row struct {
		Lane      string
		Outcomes  int64
		Recovered int64
		Failed    int64
	}
	var rows []row
	_ = db.Model(&models.PipelineAutopilotAction{}).
		Select(`lane,
			COUNT(*) FILTER (WHERE outcome IN ('recovered','failed_again','unresolved')) AS outcomes,
			COUNT(*) FILTER (WHERE outcome = 'recovered') AS recovered,
			COUNT(*) FILTER (WHERE outcome IN ('failed_again','unresolved')) AS failed`).
		Where("tenant_id = ? AND status = ?", tenantID, models.PipelineAutopilotActionStatusSuccess).
		Group("lane").Scan(&rows).Error
	for _, r := range rows {
		stat := pipelineTrustStat{Lane: r.Lane, Outcomes: r.Outcomes, Recovered: r.Recovered, Failed: r.Failed}
		if r.Outcomes > 0 {
			stat.SuccessPct = float64(r.Recovered) * 100 / float64(r.Outcomes)
		}
		stat.State = evaluatePipelineTrust(stat.Outcomes, stat.SuccessPct, policy)
		stat.Earned = stat.State != models.PipelineTrustStateDemoted
		out[r.Lane] = stat
	}
	for _, lane := range lanes {
		if _, ok := out[lane]; !ok {
			out[lane] = pipelineTrustStat{Lane: lane, State: models.PipelineTrustStateProbation, Earned: true}
		}
	}
	return out
}

func runPipelineAutopilot(db *gorm.DB, tenantID string, opts pipelineAutopilotRunOptions) (models.PipelineAutopilotRun, []models.PipelineAutopilotAction, error) {
	if strings.TrimSpace(tenantID) == "" {
		tenantID = defaultCirculationTenant
	}
	if !tryStartPipelineAutopilotRun(tenantID) {
		return models.PipelineAutopilotRun{}, nil, errPipelineAutopilotAlreadyRunning
	}
	defer finishPipelineAutopilotRun(tenantID)

	policy := loadPipelineAutopilotPolicy(db, tenantID)
	if !policy.Enabled {
		return models.PipelineAutopilotRun{}, nil, errPipelineAutopilotDisabled
	}
	trigger := strings.TrimSpace(opts.Trigger)
	if trigger == "" {
		trigger = "scheduled"
	}
	observe := policy.Mode != models.PipelineAutopilotModeSafeAuto
	now := time.Now()
	execPolicy := pipelineAutopilotElevatedCaps(policy)

	run := models.PipelineAutopilotRun{
		TenantID:     tenantID,
		Trigger:      trigger,
		Mode:         policy.Mode,
		ElevatedMode: execPolicy.ElevatedMode,
		Status:       models.PipelineAutopilotRunStatusRunning,
		StartedAt:    now,
		CreatedBy:    opts.CreatedBy,
		ErrorClass:   models.PipelineAutopilotErrorClassNone,
	}
	if run.CreatedBy == "" {
		run.CreatedBy = "automation"
	}

	if err := db.Exec("SELECT 1").Error; err != nil {
		return failPipelineAutopilotRun(db, &run, "CMS database precondition failed: "+err.Error(), models.PipelineAutopilotErrorClassCMSDB)
	}
	queues, queueErr := fetchAggregationQueueStats()
	if queueErr != nil {
		errorClass := models.PipelineAutopilotErrorClassAggregationUnreachable
		if strings.Contains(strings.ToLower(queueErr.Error()), "token") {
			errorClass = models.PipelineAutopilotErrorClassToken
		}
		return failPipelineAutopilotRun(db, &run, "Aggregation precondition failed: "+queueErr.Error(), errorClass)
	}
	healthBefore := buildPipelineHealthSnapshot(db, tenantID, queues)
	run.HealthBefore = marshalAutopilotJSON(healthBefore)
	if err := db.Create(&run).Error; err != nil {
		return run, nil, err
	}

	// Recovery cooldown (G8): last_health_ok_at marks the most recent down→up
	// recovery transition — stamped ONLY when the previous run aborted on a health
	// failure, never on ordinary healthy runs or the very first run. The cooldown
	// then holds retries for the full window after that stamp, independent of the
	// run schedule; tying it to `recovered` (true for only the single transition
	// run) would collapse the window to one run and let a short interval resume
	// retries early.
	healthOKAt := policy.LastHealthOKAt
	if lastPipelineRunHadHealthFailure(db, tenantID) {
		t := now
		healthOKAt = &t
		_ = db.Model(&models.PipelineAutopilotPolicy{}).Where("tenant_id = ?", tenantID).
			Updates(map[string]interface{}{"last_health_ok_at": t, "updated_at": t}).Error
	}
	cooldownActive := pipelineCooldownActive(healthOKAt, execPolicy.RecoveryCooldownMinutes, now)

	resolvePipelineOutcomes(db, tenantID, now)
	runner := &pipelineAutopilotRunner{
		db: db, run: &run, policy: execPolicy, observe: observe,
		queues: queues, queueDepths: queueDepthMap(queues), cooldownActive: cooldownActive,
	}
	runner.trust = computePipelineTrust(db, tenantID, execPolicy)
	runner.execute()

	finishedAt := time.Now()
	queuesAfter, _ := fetchAggregationQueueStats()
	healthAfter := buildPipelineHealthSnapshot(db, tenantID, queuesAfter)
	status := models.PipelineAutopilotRunStatusCompleted
	if runner.errors > 0 && runner.enqueued == 0 && !observe {
		status = models.PipelineAutopilotRunStatusFailed
	} else if runner.errors > 0 {
		status = models.PipelineAutopilotRunStatusPartial
	}
	headline := runner.headline()
	verb := "repaired"
	if observe {
		verb = "would repair"
	}
	summary := fmt.Sprintf("%s %d items, skipped %d, attention %d, errors %d",
		verb, runner.enqueued, runner.skipped, runner.attention, runner.errors)
	errText := ""
	if status == models.PipelineAutopilotRunStatusFailed {
		errText = "all executed pipeline repair batches failed"
	}
	_ = db.Model(&models.PipelineAutopilotRun{}).Where("id = ?", run.ID).Updates(map[string]interface{}{
		"status": status, "headline": headline, "finished_at": finishedAt,
		"summary": summary, "health_after": marshalAutopilotJSON(healthAfter),
		"error": errText, "error_class": models.PipelineAutopilotErrorClassNone, "updated_at": finishedAt,
	}).Error
	touchPipelineAutopilotLastRun(db, tenantID, finishedAt)

	run.Status = status
	run.Headline = headline
	run.FinishedAt = &finishedAt
	run.Summary = summary
	run.HealthAfter = marshalAutopilotJSON(healthAfter)
	run.Error = errText

	var actions []models.PipelineAutopilotAction
	_ = db.Where("tenant_id = ? AND run_id = ?", tenantID, run.ID).
		Order("started_at ASC, id ASC").Find(&actions).Error
	return run, actions, nil
}

type pipelineAutopilotRunner struct {
	db             *gorm.DB
	run            *models.PipelineAutopilotRun
	policy         models.PipelineAutopilotPolicy
	observe        bool
	trust          map[string]pipelineTrustStat
	queues         []autopilotQueueStat
	queueDepths    map[string]int
	cooldownActive bool

	batches      int
	usedItems    int
	enqueued     int
	skipped      int
	errors       int
	attention    int
	queueBlocked bool
}

func (r *pipelineAutopilotRunner) execute() {
	r.writeAttentionRows()
	batches := r.buildBatches()
	for _, batch := range batches {
		if r.usedItems >= r.policy.MaxItemsPerRun {
			r.batchBlock(batch, models.PipelineAutopilotGuardBudget, fmt.Sprintf("Run item cap (%d) reached.", r.policy.MaxItemsPerRun))
			continue
		}
		if r.batches >= r.policy.MaxBatchesPerRun {
			r.batchBlock(batch, models.PipelineAutopilotGuardBudget, fmt.Sprintf("Run batch cap (%d) reached.", r.policy.MaxBatchesPerRun))
			continue
		}
		if stat := r.trust[batch.Lane]; !stat.Earned {
			r.batchBlock(batch, models.PipelineAutopilotGuardTrustGate,
				fmt.Sprintf("Lane %s demoted: %.0f%% success over %d outcomes; held for human review.", batch.Lane, stat.SuccessPct, stat.Outcomes))
			continue
		}
		if r.cooldownActive {
			r.batchBlock(batch, models.PipelineAutopilotGuardRecoveryCooldown,
				fmt.Sprintf("Service recovered recently; retry execution held for %d minutes.", r.policy.RecoveryCooldownMinutes))
			continue
		}
		if depth := r.queueDepths[batch.TargetQueue]; depth > r.policy.MaxQueueDepth {
			r.queueBlocked = true
			r.batchBlock(batch, models.PipelineAutopilotGuardQueueDepth,
				fmt.Sprintf("%s depth %d exceeds cap %d.", batch.TargetQueue, depth, r.policy.MaxQueueDepth))
			continue
		}
		if r.sourceRetryCount(batch.SourceKey) >= int64(r.policy.PerSourceDailyRetries) {
			r.batchBlock(batch, models.PipelineAutopilotGuardSourceCeiling,
				fmt.Sprintf("Source %s reached %d autopilot retries in the last 24h.", batch.SourceKey, r.policy.PerSourceDailyRetries))
			continue
		}
		remaining := r.policy.MaxItemsPerRun - r.usedItems
		if remaining <= 0 {
			continue
		}
		if len(batch.Items) > remaining {
			batch.Items = batch.Items[:remaining]
		}
		r.dispatchBatch(batch)
	}
}

func (r *pipelineAutopilotRunner) writeAction(a models.PipelineAutopilotAction) {
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

func (r *pipelineAutopilotRunner) batchBlock(batch pipelineBatch, guardrail, reason string) {
	status := models.PipelineAutopilotActionStatusSkipped
	if r.observe {
		status = models.PipelineAutopilotActionStatusWouldSkip
	}
	r.skipped++
	r.writeAction(models.PipelineAutopilotAction{
		Lane: batch.Lane, Verdict: batch.Verdict, SourceFilter: batch.SourceKey,
		TargetQueue: batch.TargetQueue, Status: status, Guardrail: guardrail,
		RequestedCount: len(batch.Items), Reason: reason,
	})
}

func (r *pipelineAutopilotRunner) writeAttentionRows() {
	if _, exhausted := pipelineExhaustedFailed(r.db, r.run.TenantID, r.policy.MaxAttempts, 0); exhausted > 0 {
		r.attention++
		r.writeAction(models.PipelineAutopilotAction{
			Lane: models.PipelineLaneFailedExhausted, Verdict: models.PipelineVerdictFailedExhaust,
			Status: models.PipelineAutopilotActionStatusAttention, Guardrail: models.PipelineAutopilotGuardAttemptCap,
			RequestedCount: int(exhausted), Reason: fmt.Sprintf("%d FAILED items reached the %d-attempt cap and stay FAILED for human attention.", exhausted, r.policy.MaxAttempts),
		})
	}
	if source, count, pct := sourceBrokenSignal(r.db, r.run.TenantID); source != "" {
		r.attention++
		r.writeAction(models.PipelineAutopilotAction{
			Lane: models.PipelineLaneSourceBroken, Verdict: models.PipelineVerdictSourceBroken,
			SourceFilter: source, Status: models.PipelineAutopilotActionStatusAttention,
			RequestedCount: count, Reason: fmt.Sprintf("%s owns %d recent failures (%.0f%% of 7-day FAILED items).", source, count, pct),
		})
	}
	if depth := pipelineQueueDepth(r.queues, "aggregation-dlq"); depth > 0 {
		r.attention++
		r.writeAction(models.PipelineAutopilotAction{
			Lane: models.PipelineLaneDLQReview, Verdict: models.PipelineVerdictDLQReview,
			TargetQueue: "aggregation-dlq", Status: models.PipelineAutopilotActionStatusAttention,
			RequestedCount: depth, Reason: fmt.Sprintf("DLQ has %d waiting/active/delayed/failed jobs; review only.", depth),
		})
	}
}

func (r *pipelineAutopilotRunner) buildBatches() []pipelineBatch {
	candidates := r.selectCandidates()
	batchesByKey := map[string]*pipelineBatch{}
	order := []string{}
	for _, c := range candidates {
		key := c.Lane + "|" + c.TargetQueue + "|" + c.SourceKey
		if _, ok := batchesByKey[key]; !ok {
			batchesByKey[key] = &pipelineBatch{Lane: c.Lane, Verdict: c.Verdict, TargetQueue: c.TargetQueue, SourceKey: c.SourceKey}
			order = append(order, key)
		}
		batchesByKey[key].Items = append(batchesByKey[key].Items, c.Item)
	}
	out := make([]pipelineBatch, 0, len(order))
	for _, key := range order {
		out = append(out, *batchesByKey[key])
	}
	return out
}

func (r *pipelineAutopilotRunner) selectCandidates() []pipelineCandidate {
	out := []pipelineCandidate{}
	now := time.Now()
	pendingFloor := now.Add(-time.Duration(r.policy.PendingAgeFloorMinutes) * time.Minute)
	processingFloor := now.Add(-time.Duration(r.policy.ProcessingStuckHours) * time.Hour)
	// FAILED items float their age off updated_at (≈ when they failed), not
	// created_at — an old item that just failed should still respect the floor.
	lanes := []struct {
		status  models.ContentStatus
		lane    string
		verdict string
		before  time.Time
		column  string
	}{
		{models.ContentStatusPending, models.PipelineLanePendingStuck, models.PipelineVerdictRetryPending, pendingFloor, "created_at"},
		{models.ContentStatusFailed, models.PipelineLaneFailedRetryable, models.PipelineVerdictRetryFailed, pendingFloor, "updated_at"},
		{models.ContentStatusProcessing, models.PipelineLaneProcessingStuck, models.PipelineVerdictResetStuck, processingFloor, "updated_at"},
	}
	cap := r.policy.MaxItemsPerRun * r.policy.MaxBatchesPerRun
	for _, lane := range lanes {
		var items []models.ContentItem
		_ = r.db.Where("tenant_id = ? AND status = ? AND "+lane.column+" < ?", r.run.TenantID, lane.status, lane.before).
			Order(lane.column + " ASC").Limit(r.policy.MaxItemsPerRun * 5).Find(&items).Error
		// One batched attempt-state lookup for the whole lane instead of 2 queries
		// per item — the status endpoint polls this and the FAILED backlog can be
		// large, so per-item round-trips were a DB-load hazard on constrained infra.
		ids := make([]uuid.UUID, 0, len(items))
		for _, item := range items {
			ids = append(ids, item.PublicID)
		}
		states := pipelineAttemptStates(r.db, r.run.TenantID, ids)
		backoff := time.Duration(r.policy.RetryBackoffHours) * time.Hour
		for _, item := range items {
			st := states[item.PublicID]
			if st.Attempts >= int64(r.policy.MaxAttempts) {
				continue
			}
			if st.LastAttempt != nil && now.Sub(*st.LastAttempt) < backoff {
				continue
			}
			out = append(out, pipelineCandidate{
				Item: item, Lane: lane.lane, Verdict: lane.verdict,
				SourceKey: pipelineSourceKey(item), TargetQueue: pipelineTargetQueue(item),
			})
			if len(out) >= cap {
				return out
			}
		}
	}
	return out
}

type pipelineAttemptState struct {
	Attempts    int64
	LastAttempt *time.Time
}

// pipelineAttemptStates returns per-item executed-attempt counts and the most
// recent attempt time for a set of ids in a single grouped query. Only real
// executed retries (status=success) count — Observe would_execute rows never do,
// so Observe writes no attempt memory (G6).
func pipelineAttemptStates(db *gorm.DB, tenantID string, ids []uuid.UUID) map[uuid.UUID]pipelineAttemptState {
	out := map[uuid.UUID]pipelineAttemptState{}
	if len(ids) == 0 {
		return out
	}
	type row struct {
		ContentItemID uuid.UUID
		Attempts      int64
		LastAttempt   *time.Time
	}
	var rows []row
	_ = db.Model(&models.PipelineAutopilotAction{}).
		Select("content_item_id, COUNT(*) AS attempts, MAX(started_at) AS last_attempt").
		Where("tenant_id = ? AND status = ? AND content_item_id IN ?", tenantID, models.PipelineAutopilotActionStatusSuccess, ids).
		Group("content_item_id").Scan(&rows).Error
	for _, r := range rows {
		out[r.ContentItemID] = pipelineAttemptState{Attempts: r.Attempts, LastAttempt: r.LastAttempt}
	}
	return out
}

// pipelineExhaustedFailed returns the FAILED items whose executed-attempt count
// has reached the cap (the attention cohort) plus their total, in two set-based
// queries — a JOIN against the ledger's grouped attempt counts rather than an
// N+1 scan. limit<=0 returns just the count.
func pipelineExhaustedFailed(db *gorm.DB, tenantID string, maxAttempts, limit int) ([]string, int64) {
	base := func() *gorm.DB {
		sub := db.Model(&models.PipelineAutopilotAction{}).
			Select("content_item_id, COUNT(*) AS c").
			Where("tenant_id = ? AND status = ?", tenantID, models.PipelineAutopilotActionStatusSuccess).
			Group("content_item_id")
		return db.Table("content_items AS ci").
			Joins("JOIN (?) AS a ON a.content_item_id = ci.public_id", sub).
			Where("ci.tenant_id = ? AND ci.status = ? AND a.c >= ?", tenantID, models.ContentStatusFailed, maxAttempts)
	}
	var total int64
	_ = base().Count(&total).Error
	ids := []string{}
	if limit > 0 && total > 0 {
		var rows []struct {
			PublicID uuid.UUID `gorm:"column:public_id"`
		}
		_ = base().Select("ci.public_id").Order("ci.updated_at ASC").Limit(limit).Scan(&rows).Error
		for _, row := range rows {
			ids = append(ids, row.PublicID.String())
		}
	}
	return ids, total
}

func (r *pipelineAutopilotRunner) sourceRetryCount(source string) int64 {
	if strings.TrimSpace(source) == "" {
		source = "unknown"
	}
	var count int64
	_ = r.db.Model(&models.PipelineAutopilotAction{}).
		Where("tenant_id = ? AND source_filter = ? AND status = ? AND created_at >= ?",
			r.run.TenantID, source, models.PipelineAutopilotActionStatusSuccess, time.Now().Add(-24*time.Hour)).
		Count(&count).Error
	return count
}

func (r *pipelineAutopilotRunner) dispatchBatch(batch pipelineBatch) {
	r.batches++
	startedAt := time.Now()
	ids := make([]string, 0, len(batch.Items))
	for _, item := range batch.Items {
		ids = append(ids, item.PublicID.String())
	}
	if len(ids) == 0 {
		return
	}

	if r.observe {
		finishedAt := time.Now()
		for _, item := range batch.Items {
			id := item.PublicID
			r.usedItems++
			r.enqueued++
			r.writeAction(models.PipelineAutopilotAction{
				Lane: batch.Lane, Verdict: batch.Verdict, SourceFilter: batch.SourceKey,
				TargetQueue: batch.TargetQueue, ContentItemID: &id,
				Status:         models.PipelineAutopilotActionStatusWouldExecute,
				RequestedCount: 1, Reason: "Would execute by explicit ids (Observe dry-run).",
				StartedAt: startedAt, FinishedAt: &finishedAt,
			})
		}
		return
	}

	if batch.Lane == models.PipelineLaneProcessingStuck {
		_ = r.db.Model(&models.ContentItem{}).
			Where("tenant_id = ? AND public_id IN ? AND status = ?", r.run.TenantID, ids, models.ContentStatusProcessing).
			Update("status", models.ContentStatusPending).Error
	}

	endpoint := "/internal/retry-pending"
	if batch.Lane == models.PipelineLaneFailedRetryable {
		endpoint = "/internal/retry-failed"
	}
	resp, err := callAggregationPipelineRetry(endpoint, ids)
	finishedAt := time.Now()
	errorSet := map[string]string{}
	if resp != nil {
		for _, raw := range resp.Errors {
			for _, id := range ids {
				if strings.Contains(raw, id) {
					errorSet[id] = raw
				}
			}
		}
	}
	if err != nil {
		r.errors++
		r.writeAction(models.PipelineAutopilotAction{
			Lane: batch.Lane, Verdict: batch.Verdict, SourceFilter: batch.SourceKey, TargetQueue: batch.TargetQueue,
			Status: models.PipelineAutopilotActionStatusError, Reason: err.Error(),
			RequestedCount: len(ids), ErrorCount: len(ids), Output: marshalAutopilotJSON(resp),
			StartedAt: startedAt, FinishedAt: &finishedAt,
		})
		return
	}
	// Aggregation reports an aggregate requeued count and per-id error strings, not
	// a per-id success list. Credit success (which consumes an attempt and starts an
	// outcome) only up to the reported requeued count: if an id was silently dropped
	// (its status changed between selection and dispatch, so CMS's list didn't return
	// it) it never actually ran and must not burn an attempt toward the cap.
	requeuedBudget := 0
	if resp != nil {
		requeuedBudget = resp.Requeued
	}
	for _, item := range batch.Items {
		id := item.PublicID
		if msg, failed := errorSet[id.String()]; failed {
			r.errors++
			r.writeAction(models.PipelineAutopilotAction{
				Lane: batch.Lane, Verdict: batch.Verdict, SourceFilter: batch.SourceKey,
				TargetQueue: batch.TargetQueue, ContentItemID: &id,
				Status: models.PipelineAutopilotActionStatusError, Reason: msg,
				RequestedCount: 1, ErrorCount: 1, Output: marshalAutopilotJSON(resp),
				StartedAt: startedAt, FinishedAt: &finishedAt,
			})
			continue
		}
		if requeuedBudget <= 0 {
			r.skipped++
			r.writeAction(models.PipelineAutopilotAction{
				Lane: batch.Lane, Verdict: batch.Verdict, SourceFilter: batch.SourceKey,
				TargetQueue: batch.TargetQueue, ContentItemID: &id,
				Status: models.PipelineAutopilotActionStatusSkipped, Guardrail: models.PipelineAutopilotGuardStale,
				Reason:         "Aggregation did not requeue this id (its status changed before dispatch); no attempt consumed.",
				RequestedCount: 1, Output: marshalAutopilotJSON(resp),
				StartedAt: startedAt, FinishedAt: &finishedAt,
			})
			continue
		}
		requeuedBudget--
		r.usedItems++
		r.enqueued++
		r.writeAction(models.PipelineAutopilotAction{
			Lane: batch.Lane, Verdict: batch.Verdict, SourceFilter: batch.SourceKey,
			TargetQueue: batch.TargetQueue, ContentItemID: &id,
			Status:         models.PipelineAutopilotActionStatusSuccess,
			Outcome:        models.PipelineAutopilotOutcomePending,
			Reason:         "Requeued through Aggregation by explicit ids.",
			RequestedCount: 1, EnqueuedCount: 1, Output: marshalAutopilotJSON(resp),
			StartedAt: startedAt, FinishedAt: &finishedAt,
		})
	}
}

func (r *pipelineAutopilotRunner) headline() string {
	if r.errors > 0 && r.enqueued == 0 && !r.observe {
		return models.PipelineAutopilotHeadlineDegraded
	}
	if r.queueBlocked {
		return models.PipelineAutopilotHeadlineBacklogged
	}
	if r.enqueued > 0 {
		return models.PipelineAutopilotHeadlineRepairing
	}
	if r.attention > 0 {
		return models.PipelineAutopilotHeadlineClogged
	}
	return models.PipelineAutopilotHeadlineFlowing
}

func callAggregationPipelineRetry(path string, ids []string) (*pipelineRetryResponse, error) {
	if len(ids) == 0 {
		return &pipelineRetryResponse{Success: true, Message: "No ids", Requeued: 0, Total: 0}, nil
	}
	body, status, err := callAggregationInternal(http.MethodPost, path, map[string]interface{}{"ids": ids, "limit": len(ids)})
	if err != nil {
		return nil, err
	}
	var decoded pipelineRetryResponse
	if len(body) > 0 {
		_ = json.Unmarshal(body, &decoded)
	}
	if status < http.StatusOK || status >= http.StatusMultipleChoices {
		return &decoded, fmt.Errorf("aggregation %s responded with %d: %s", path, status, string(body))
	}
	return &decoded, nil
}

func buildPipelineHealthSnapshot(db *gorm.DB, tenantID string, queues []autopilotQueueStat) pipelineHealthSnapshot {
	counts := map[string]int64{"PENDING": 0, "PROCESSING": 0, "READY": 0, "FAILED": 0, "ARCHIVED": 0}
	type row struct {
		Status string
		Count  int64
	}
	var rows []row
	_ = db.Model(&models.ContentItem{}).Select("status, COUNT(*) AS count").
		Where("tenant_id = ?", tenantID).Group("status").Scan(&rows).Error
	for _, r := range rows {
		counts[r.Status] = r.Count
	}
	var freshness struct {
		Oldest *time.Time
		Stuck  int64
	}
	stuckBefore := time.Now().Add(-24 * time.Hour)
	_ = db.Model(&models.ContentItem{}).
		Where("tenant_id = ? AND status IN ?", tenantID, []models.ContentStatus{models.ContentStatusPending, models.ContentStatusProcessing}).
		Select("MIN(created_at) AS oldest, COALESCE(SUM(CASE WHEN created_at < ? THEN 1 ELSE 0 END), 0) AS stuck", stuckBefore).
		Scan(&freshness).Error
	var oldest *string
	if freshness.Oldest != nil {
		value := freshness.Oldest.UTC().Format(time.RFC3339)
		oldest = &value
	}
	return pipelineHealthSnapshot{
		Timestamp: time.Now().UTC().Format(time.RFC3339), StatusCounts: counts,
		StuckCount: freshness.Stuck, OldestUnprocessed: oldest, Queues: queues,
		QueueDepth: pipelineAllQueueDepth(queues), DLQDepth: pipelineQueueDepth(queues, "aggregation-dlq"),
		AggregationHealthy: true,
	}
}

func pipelineAllQueueDepth(stats []autopilotQueueStat) int {
	total := 0
	for _, s := range stats {
		total += s.Waiting + s.Active + s.Delayed
	}
	return total
}

func queueDepthMap(stats []autopilotQueueStat) map[string]int {
	out := map[string]int{}
	for _, s := range stats {
		out[s.Queue] = s.Waiting + s.Active + s.Delayed
	}
	return out
}

func pipelineQueueDepth(stats []autopilotQueueStat, queue string) int {
	for _, s := range stats {
		if s.Queue == queue {
			return s.Waiting + s.Active + s.Delayed + s.Failed
		}
	}
	return 0
}

func pipelineSourceKey(item models.ContentItem) string {
	if item.SourceName != nil && strings.TrimSpace(*item.SourceName) != "" {
		return strings.TrimSpace(*item.SourceName)
	}
	if item.Source != "" {
		return string(item.Source)
	}
	return "unknown"
}

func pipelineTargetQueue(item models.ContentItem) string {
	mediaKind := ""
	if len(item.Metadata) > 0 {
		var meta map[string]interface{}
		if err := json.Unmarshal(item.Metadata, &meta); err == nil {
			if raw, ok := meta["mediaKind"].(string); ok {
				mediaKind = raw
			}
		}
	}
	if item.Type == models.ContentTypeVideo || item.Type == models.ContentTypePodcast ||
		(item.Source == models.SourceTypeTelegram && mediaKind == "photo") {
		return "media-queue"
	}
	return "ai-queue"
}

func sourceBrokenSignal(db *gorm.DB, tenantID string) (string, int, float64) {
	type row struct {
		Source string
		Count  int
	}
	var rows []row
	since := time.Now().Add(-7 * 24 * time.Hour)
	_ = db.Model(&models.ContentItem{}).
		Select("COALESCE(NULLIF(source_name, ''), source::text, 'unknown') AS source, COUNT(*) AS count").
		Where("tenant_id = ? AND status = ? AND created_at >= ?", tenantID, models.ContentStatusFailed, since).
		Group("COALESCE(NULLIF(source_name, ''), source::text, 'unknown')").
		Order("count DESC").Scan(&rows).Error
	total := 0
	for _, r := range rows {
		total += r.Count
	}
	if total == 0 || len(rows) == 0 {
		return "", 0, 0
	}
	top := rows[0]
	pct := float64(top.Count) * 100 / float64(total)
	if top.Count >= pipelineSourceBrokenMinFailed && pct > pipelineSourceBrokenPct {
		return top.Source, top.Count, pct
	}
	return "", 0, 0
}

func resolvePipelineOutcomes(db *gorm.DB, tenantID string, now time.Time) {
	var actions []models.PipelineAutopilotAction
	_ = db.Where("tenant_id = ? AND status = ? AND outcome = ? AND content_item_id IS NOT NULL",
		tenantID, models.PipelineAutopilotActionStatusSuccess, models.PipelineAutopilotOutcomePending).
		Limit(1000).Find(&actions).Error
	for _, action := range actions {
		var item models.ContentItem
		if err := db.Where("tenant_id = ? AND public_id = ?", tenantID, *action.ContentItemID).First(&item).Error; err != nil {
			continue
		}
		outcome := ""
		switch item.Status {
		case models.ContentStatusReady:
			outcome = models.PipelineAutopilotOutcomeRecovered
		case models.ContentStatusFailed:
			outcome = models.PipelineAutopilotOutcomeFailedAgain
		default:
			if now.Sub(action.StartedAt) >= pipelineOutcomeDeadlineHours*time.Hour {
				outcome = models.PipelineAutopilotOutcomeUnresolved
			}
		}
		if outcome != "" {
			_ = db.Model(&models.PipelineAutopilotAction{}).Where("id = ?", action.ID).
				Updates(map[string]interface{}{"outcome": outcome, "updated_at": now}).Error
		}
	}
}

func failPipelineAutopilotRun(db *gorm.DB, run *models.PipelineAutopilotRun, reason, errorClass string) (models.PipelineAutopilotRun, []models.PipelineAutopilotAction, error) {
	finishedAt := time.Now()
	run.Status = models.PipelineAutopilotRunStatusFailed
	run.Headline = models.PipelineAutopilotHeadlineDegraded
	run.FinishedAt = &finishedAt
	run.Summary = "Precondition failed: " + reason
	run.Error = reason
	run.ErrorClass = errorClass
	if run.ID == 0 {
		_ = db.Create(run).Error
	} else {
		_ = db.Model(&models.PipelineAutopilotRun{}).Where("id = ?", run.ID).Updates(map[string]interface{}{
			"status": run.Status, "headline": run.Headline, "finished_at": finishedAt,
			"summary": run.Summary, "error": run.Error, "error_class": run.ErrorClass, "updated_at": finishedAt,
		}).Error
	}
	touchPipelineAutopilotLastRun(db, run.TenantID, finishedAt)
	return *run, nil, nil
}

func touchPipelineAutopilotLastRun(db *gorm.DB, tenantID string, at time.Time) {
	_ = db.Model(&models.PipelineAutopilotPolicy{}).
		Where("tenant_id = ?", tenantID).
		Updates(map[string]interface{}{"last_run_at": at, "updated_at": at}).Error
}

func lastPipelineRunHadHealthFailure(db *gorm.DB, tenantID string) bool {
	var run models.PipelineAutopilotRun
	if err := db.Where("tenant_id = ?", tenantID).Order("started_at DESC").First(&run).Error; err != nil {
		return false
	}
	return run.ErrorClass == models.PipelineAutopilotErrorClassAggregationUnreachable ||
		run.ErrorClass == models.PipelineAutopilotErrorClassCMSDB ||
		run.ErrorClass == models.PipelineAutopilotErrorClassToken
}

func buildPipelineAutopilotStatus(db *gorm.DB, tenantID string, policy models.PipelineAutopilotPolicy) pipelineAutopilotStatusBlock {
	now := time.Now()
	block := pipelineAutopilotStatusBlock{
		Enabled: policy.Enabled, Mode: policy.Mode, IntervalMinutes: policy.IntervalMinutes,
		ElevatedMode: policy.ElevatedMode, ElevatedUntil: policy.ElevatedUntil,
		PausedUntil: policy.PausedUntil, LastRunAt: policy.LastRunAt,
		LastHealthOKAt: policy.LastHealthOKAt, Policy: policy, Trust: []pipelineTrustStat{},
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
	var lastRun models.PipelineAutopilotRun
	if err := db.Where("tenant_id = ?", tenantID).Order("started_at DESC").First(&lastRun).Error; err == nil {
		block.LastRun = &lastRun
	}
	trust := computePipelineTrust(db, tenantID, policy)
	lanes := make([]string, 0, len(trust))
	for lane := range trust {
		lanes = append(lanes, lane)
	}
	sort.Strings(lanes)
	for _, lane := range lanes {
		block.Trust = append(block.Trust, trust[lane])
	}
	pseudoRun := &models.PipelineAutopilotRun{TenantID: tenantID}
	r := &pipelineAutopilotRunner{db: db, run: pseudoRun, policy: policy}
	for _, batch := range r.buildBatches() {
		block.Cohorts = append(block.Cohorts, pipelineCohortSummary{
			Lane: batch.Lane, Verdict: batch.Verdict, Count: len(batch.Items),
			TargetQueue: batch.TargetQueue, Source: batch.SourceKey,
		})
	}
	if exhaustedIDs, exhaustedTotal := pipelineExhaustedFailed(db, tenantID, policy.MaxAttempts, 20); exhaustedTotal > 0 {
		block.Attention = append(block.Attention, pipelineCohortSummary{
			Lane: models.PipelineLaneFailedExhausted, Verdict: models.PipelineVerdictFailedExhaust,
			Count: int(exhaustedTotal), ItemIDs: exhaustedIDs,
		})
	}
	if source, count, _ := sourceBrokenSignal(db, tenantID); source != "" {
		block.Attention = append(block.Attention, pipelineCohortSummary{Lane: models.PipelineLaneSourceBroken, Verdict: models.PipelineVerdictSourceBroken, Count: count, Source: source})
	}
	switch {
	case !policy.Enabled:
		block.RecommendedAction = "Enable Observe mode to start dry-run repair ledgers with zero side effects."
	case policy.Mode == models.PipelineAutopilotModeObserve:
		block.RecommendedAction = "Review Observe runs, then promote to Safe Auto when cohort selection looks calm."
	case len(block.Attention) > 0:
		block.RecommendedAction = "Review the attention lane for exhausted items, broken sources, or DLQ growth."
	default:
		block.RecommendedAction = "Pipeline Repair is supervising stuck and failed cohorts."
	}
	return block
}

func StartPipelineAutopilotHeartbeat(db *gorm.DB) {
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			runPipelineAutopilotDue(db)
		}
	}()
}

func runPipelineAutopilotDue(db *gorm.DB) {
	var policies []models.PipelineAutopilotPolicy
	if err := db.Where("enabled = ?", true).Find(&policies).Error; err != nil {
		return
	}
	now := time.Now()
	for _, raw := range policies {
		policy := sanitizePipelineAutopilotPolicy(raw)
		if policy.PausedUntil != nil && policy.PausedUntil.After(now) {
			continue
		}
		if raw.LastRunAt != nil && now.Sub(*raw.LastRunAt) < time.Duration(policy.IntervalMinutes)*time.Minute {
			continue
		}
		run, _, err := runPipelineAutopilot(db, policy.TenantID, pipelineAutopilotRunOptions{Trigger: "scheduled", CreatedBy: "automation"})
		if errors.Is(err, errPipelineAutopilotAlreadyRunning) {
			continue
		}
		payload := map[string]interface{}{"status": run.Status, "summary": run.Summary, "headline": run.Headline}
		if err != nil {
			payload["error"] = err.Error()
		}
		writeCirculationAuditSystem(db, policy.TenantID, "pipeline.autopilot.scheduled", policy.TenantID, payload)
	}
}

func GetPipelineAutopilotStatus(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	policy := loadPipelineAutopilotPolicy(db, principal.TenantID)
	c.JSON(http.StatusOK, gin.H{"data": buildPipelineAutopilotStatus(db, principal.TenantID, policy)})
}

func GetPipelineAutopilotPolicy(c *gin.Context) {
	GetPipelineAutopilotStatus(c)
}

type updatePipelineAutopilotRequest struct {
	Enabled                 *bool   `json:"enabled"`
	Mode                    *string `json:"mode"`
	IntervalMinutes         *int    `json:"interval_minutes"`
	MaxItemsPerRun          *int    `json:"max_items_per_run"`
	MaxBatchesPerRun        *int    `json:"max_batches_per_run"`
	MaxAttempts             *int    `json:"max_attempts"`
	RetryBackoffHours       *int    `json:"retry_backoff_hours"`
	PendingAgeFloorMinutes  *int    `json:"pending_age_floor_minutes"`
	ProcessingStuckHours    *int    `json:"processing_stuck_hours"`
	MaxQueueDepth           *int    `json:"max_queue_depth"`
	PerSourceDailyRetries   *int    `json:"per_source_daily_retries"`
	RecoveryCooldownMinutes *int    `json:"recovery_cooldown_minutes"`
	TrustMinOutcomes        *int    `json:"trust_min_outcomes"`
	TrustMinSuccessPct      *int    `json:"trust_min_success_pct"`
}

func UpdatePipelineAutopilotPolicy(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var req updatePipelineAutopilotRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, authErrorResponse{Message: "Invalid request: " + err.Error(), Code: "INVALID_REQUEST"})
		return
	}
	var policy models.PipelineAutopilotPolicy
	if err := db.Where("tenant_id = ?", principal.TenantID).First(&policy).Error; err != nil {
		policy = models.DefaultPipelineAutopilotPolicy(principal.TenantID)
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
	if req.MaxBatchesPerRun != nil {
		policy.MaxBatchesPerRun = *req.MaxBatchesPerRun
	}
	if req.MaxAttempts != nil {
		policy.MaxAttempts = *req.MaxAttempts
	}
	if req.RetryBackoffHours != nil {
		policy.RetryBackoffHours = *req.RetryBackoffHours
	}
	if req.PendingAgeFloorMinutes != nil {
		policy.PendingAgeFloorMinutes = *req.PendingAgeFloorMinutes
	}
	if req.ProcessingStuckHours != nil {
		policy.ProcessingStuckHours = *req.ProcessingStuckHours
	}
	if req.MaxQueueDepth != nil {
		policy.MaxQueueDepth = *req.MaxQueueDepth
	}
	if req.PerSourceDailyRetries != nil {
		policy.PerSourceDailyRetries = *req.PerSourceDailyRetries
	}
	if req.RecoveryCooldownMinutes != nil {
		policy.RecoveryCooldownMinutes = *req.RecoveryCooldownMinutes
	}
	if req.TrustMinOutcomes != nil {
		policy.TrustMinOutcomes = *req.TrustMinOutcomes
	}
	if req.TrustMinSuccessPct != nil {
		policy.TrustMinSuccessPct = *req.TrustMinSuccessPct
	}
	policy.TenantID = principal.TenantID
	policy = sanitizePipelineAutopilotPolicy(policy)
	if err := db.Save(&policy).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to save policy", Code: "SAVE_FAILED"})
		return
	}
	writeCirculationAudit(db, principal, "pipeline.autopilot.policy", principal.TenantID, map[string]interface{}{"enabled": policy.Enabled, "mode": policy.Mode})
	c.JSON(http.StatusOK, gin.H{"data": buildPipelineAutopilotStatus(db, principal.TenantID, policy)})
}

func RunPipelineAutopilotNow(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	run, actions, err := runPipelineAutopilot(db, principal.TenantID, pipelineAutopilotRunOptions{Trigger: "manual", CreatedBy: principal.Email})
	if err != nil {
		switch {
		case errors.Is(err, errPipelineAutopilotDisabled):
			c.JSON(http.StatusConflict, authErrorResponse{Message: err.Error(), Code: "AUTOPILOT_DISABLED"})
		case errors.Is(err, errPipelineAutopilotAlreadyRunning):
			c.JSON(http.StatusConflict, authErrorResponse{Message: err.Error(), Code: "AUTOPILOT_ALREADY_RUNNING"})
		default:
			c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Autopilot run failed: " + err.Error(), Code: "RUN_FAILED"})
		}
		return
	}
	writeCirculationAudit(db, principal, "pipeline.autopilot.run", run.PublicID.String(), map[string]interface{}{"status": run.Status, "headline": run.Headline, "summary": run.Summary})
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"run": run, "actions": actions}})
}

func PausePipelineAutopilot(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var req struct {
		Minutes int `json:"minutes"`
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
	if err := db.Model(&models.PipelineAutopilotPolicy{}).Where("tenant_id = ?", principal.TenantID).
		Updates(map[string]interface{}{"paused_until": until, "updated_at": time.Now()}).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to update pause state", Code: "SAVE_FAILED"})
		return
	}
	action := "pipeline.autopilot.pause"
	if until == nil {
		action = "pipeline.autopilot.resume"
	}
	writeCirculationAudit(db, principal, action, principal.TenantID, map[string]interface{}{"paused_until": until})
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"paused_until": until}})
}

func ElevatePipelineAutopilot(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var req struct {
		Mode    string `json:"mode"`
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
	case models.PipelineAutopilotElevatedBacklogDrain:
		minutes := req.Minutes
		if minutes <= 0 {
			minutes = 720
		}
		if minutes < 15 {
			minutes = 15
		}
		if minutes > 4320 {
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
	if err := db.Model(&models.PipelineAutopilotPolicy{}).Where("tenant_id = ?", principal.TenantID).Updates(updates).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to update elevated mode", Code: "SAVE_FAILED"})
		return
	}
	writeCirculationAudit(db, principal, "pipeline.autopilot.elevate", principal.TenantID, map[string]interface{}{"mode": req.Mode, "until": until})
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"mode": req.Mode, "until": until}})
}

func ListPipelineAutopilotRuns(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var runs []models.PipelineAutopilotRun
	if err := db.Where("tenant_id = ?", principal.TenantID).Order("started_at DESC").
		Limit(boundedLimit(c.Query("limit"), 20, 100)).Find(&runs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, authErrorResponse{Message: "Failed to list runs", Code: "QUERY_FAILED"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"items": runs}})
}

func GetPipelineAutopilotRun(c *gin.Context) {
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
	var run models.PipelineAutopilotRun
	if err := db.Where("tenant_id = ? AND public_id = ?", principal.TenantID, runID).First(&run).Error; err != nil {
		c.JSON(http.StatusNotFound, authErrorResponse{Message: "Run not found", Code: "NOT_FOUND"})
		return
	}
	var actions []models.PipelineAutopilotAction
	_ = db.Where("tenant_id = ? AND run_id = ?", principal.TenantID, run.ID).Order("started_at ASC, id ASC").Find(&actions).Error
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"run": run, "actions": actions}})
}
