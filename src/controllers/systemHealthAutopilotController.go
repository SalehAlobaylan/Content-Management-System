package controllers

import (
	"bytes"
	"content-management-system/src/models"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

const (
	systemAutopilotScope       = "platform"
	systemProbeTimeout         = 3 * time.Second
	systemProbePhaseTimeout    = 10 * time.Second
	systemProbeBodyLimit       = 256 * 1024
	systemQueueProbeBodyLimit  = 1024 * 1024
	systemQueueWaitingWarn     = 100
	systemAutopilotHistoryRuns = 12
	systemAutopilotAdvisoryKey = 7_070_000_001
)

var (
	systemAutopilotMu      sync.Mutex
	systemAutopilotRunning bool
	errSystemAutopilotBusy = fmt.Errorf("system health autopilot already running")
)

type systemAutopilotRunOptions struct {
	Trigger   string
	CreatedBy string
}

// systemAutopilotDeps keeps the runner deterministic in DB tests without
// changing the handlers or making live probe behavior mutable global state.
type systemAutopilotDeps struct {
	now     func() time.Time
	collect func(*gorm.DB) (systemHealthSnapshot, []systemAnomaly)
}

var defaultSystemAutopilotDeps = systemAutopilotDeps{
	now:     func() time.Time { return time.Now().UTC() },
	collect: collectSystemHealthSnapshot,
}

type systemAnomaly struct {
	Key       string                 `json:"key"`
	Service   string                 `json:"service"`
	Verdict   string                 `json:"verdict"`
	Severity  string                 `json:"severity"`
	Summary   string                 `json:"summary"`
	Evidence  map[string]interface{} `json:"evidence,omitempty"`
	Confirmed bool                   `json:"confirmed"`
}

type systemRunSnapshot struct {
	Timestamp string              `json:"timestamp"`
	Overall   string              `json:"overall"`
	Services  []systemProbeResult `json:"services"`
	Anomalies []systemAnomaly     `json:"anomalies"`
}

type systemSiblingAutopilot struct {
	Key          string
	Label        string
	Table        string
	PauseColumn  string
	Dependencies []string
}

var systemSiblingAutopilots = []systemSiblingAutopilot{
	{Key: "pipeline", Label: "Pipeline Repair", Table: "pipeline_autopilot_policies", PauseColumn: "paused_until", Dependencies: []string{"aggregation"}},
	{Key: "enrichment", Label: "Enrichment Coverage", Table: "enrichment_autopilot_policies", PauseColumn: "paused_until", Dependencies: []string{"aggregation", "enrichment", "media"}},
	{Key: "embedding_lifecycle", Label: "Embedding Lifecycle", Table: "embedding_lifecycle_policies", PauseColumn: "campaigns_paused_until", Dependencies: []string{"cms", "enrichment", "media"}},
	{Key: "news_circulation", Label: "News Circulation", Table: "news_circulation_policies", PauseColumn: "autopilot_paused_until", Dependencies: []string{"aggregation"}},
	{Key: "media_circulation", Label: "Media Circulation", Table: "media_circulation_policies", PauseColumn: "autopilot_paused_until", Dependencies: []string{"aggregation"}},
	{Key: "media_studio", Label: "Media Studio", Table: "media_studio_autopilot_policies", PauseColumn: "paused_until", Dependencies: []string{"cms", "media", "enrichment"}},
	{Key: "redundancy", Label: "Redundancy Hygiene", Table: "redundancy_policies", PauseColumn: "paused_until", Dependencies: []string{"cms", "aggregation"}},
}

func tryStartSystemAutopilotRun() bool {
	systemAutopilotMu.Lock()
	defer systemAutopilotMu.Unlock()
	if systemAutopilotRunning {
		return false
	}
	systemAutopilotRunning = true
	return true
}

func finishSystemAutopilotRun() {
	systemAutopilotMu.Lock()
	systemAutopilotRunning = false
	systemAutopilotMu.Unlock()
}

func loadSystemAutopilotPolicy(db *gorm.DB) models.SystemAutopilotPolicy {
	var policy models.SystemAutopilotPolicy
	if err := db.Where("scope = ?", systemAutopilotScope).First(&policy).Error; err != nil {
		policy = models.DefaultSystemAutopilotPolicy()
		_ = db.Where("scope = ?", systemAutopilotScope).FirstOrCreate(&policy).Error
	}
	return sanitizeSystemAutopilotPolicy(policy)
}

func sanitizeSystemAutopilotPolicy(p models.SystemAutopilotPolicy) models.SystemAutopilotPolicy {
	p.Scope = systemAutopilotScope
	if p.Mode != models.SystemAutopilotModeSafeAuto {
		p.Mode = models.SystemAutopilotModeObserve
	}
	if p.IntervalMinutes < 2 {
		p.IntervalMinutes = 10
	}
	if p.IntervalMinutes > 60 {
		p.IntervalMinutes = 60
	}
	if p.ConfirmProbes < 1 {
		p.ConfirmProbes = 2
	}
	if p.ConfirmProbes > 6 {
		p.ConfirmProbes = 6
	}
	if p.ResolveProbes < 1 {
		p.ResolveProbes = 3
	}
	if p.ResolveProbes > 12 {
		p.ResolveProbes = 12
	}
	if p.FlapCycles24h < 1 {
		p.FlapCycles24h = 3
	}
	if p.FlapCycles24h > 12 {
		p.FlapCycles24h = 12
	}
	if p.ContainmentTTLMinutes < 15 {
		p.ContainmentTTLMinutes = 60
	}
	if p.ContainmentTTLMinutes > 1440 {
		p.ContainmentTTLMinutes = 1440
	}
	if len(p.ContainmentDisabledFor) == 0 || !json.Valid(p.ContainmentDisabledFor) {
		p.ContainmentDisabledFor = models.DefaultSystemAutopilotPolicy().ContainmentDisabledFor
	}
	return p
}

func runSystemHealthAutopilot(db *gorm.DB, opts systemAutopilotRunOptions) (models.SystemAutopilotRun, []models.SystemAutopilotAction, error) {
	return runSystemHealthAutopilotWithDeps(db, opts, defaultSystemAutopilotDeps)
}

func runSystemHealthAutopilotWithDeps(db *gorm.DB, opts systemAutopilotRunOptions, deps systemAutopilotDeps) (models.SystemAutopilotRun, []models.SystemAutopilotAction, error) {
	if deps.now == nil {
		deps.now = defaultSystemAutopilotDeps.now
	}
	if deps.collect == nil {
		deps.collect = defaultSystemAutopilotDeps.collect
	}
	if opts.Trigger == "" {
		opts.Trigger = "manual"
	}
	if !tryStartSystemAutopilotRun() {
		return models.SystemAutopilotRun{}, nil, errSystemAutopilotBusy
	}
	releaseLock, acquired := tryAcquireSystemAutopilotAdvisoryLock(db)
	if !acquired {
		finishSystemAutopilotRun()
		return models.SystemAutopilotRun{}, nil, errSystemAutopilotBusy
	}
	defer finishSystemAutopilotRun()
	defer releaseLock()

	now := deps.now()
	policy := loadSystemAutopilotPolicy(db)
	run := models.SystemAutopilotRun{
		Trigger:    opts.Trigger,
		Mode:       policy.Mode,
		Status:     models.SystemAutopilotRunStatusRunning,
		Headline:   models.SystemAutopilotHeadlineWatching,
		StartedAt:  now,
		CreatedBy:  opts.CreatedBy,
		ErrorClass: models.SystemAutopilotErrorClassNone,
	}
	if err := db.Create(&run).Error; err != nil {
		return run, nil, err
	}

	actions := []models.SystemAutopilotAction{}
	storeAction := func(actionDB *gorm.DB, a models.SystemAutopilotAction) (models.SystemAutopilotAction, error) {
		t := deps.now()
		if a.StartedAt.IsZero() {
			a.StartedAt = t
		}
		if a.FinishedAt == nil {
			a.FinishedAt = &t
		}
		a.RunID = run.ID
		if a.Status == "" {
			a.Status = "success"
		}
		if err := actionDB.Create(&a).Error; err != nil {
			return a, err
		}
		return a, nil
	}
	writeAction := func(a models.SystemAutopilotAction) {
		if stored, err := storeAction(db, a); err == nil {
			actions = append(actions, stored)
		}
	}

	snapshot, anomalies := deps.collect(db)
	prev := recentSystemRunSnapshots(db, systemAutopilotHistoryRuns)
	confirmed := confirmSystemAnomalies(anomalies, prev, policy.ConfirmProbes)
	confirmed = correlateSystemAnomalies(confirmed)
	confirmed = applySystemFlapGuard(db, confirmed, policy.FlapCycles24h, now, writeAction)
	observed := correlateSystemAnomalies(anomalies)

	for _, anomaly := range observed {
		if anomaly.Verdict == models.SystemVerdictQueueBacklog {
			if systemAnomalyStreak(anomaly, prev) < 3 {
				continue
			}
			writeAction(models.SystemAutopilotAction{
				Target:    anomaly.Service,
				Action:    models.SystemAutopilotActionSkipped,
				Verdict:   anomaly.Verdict,
				Status:    "attention",
				Guardrail: models.SystemAutopilotGuardQueueBacklogNoIncident,
				Reason:    anomaly.Summary,
				Output:    marshalAutopilotJSON(anomaly),
			})
		}
	}

	openEpisodes := openSystemIncidentEpisodes(db)
	episodesByKey := map[string]models.SystemIncidentEpisode{}
	for _, ep := range openEpisodes {
		episodesByKey[systemIncidentKey(ep.RootService, ep.Verdict)] = ep
	}
	// A confirmed episode in recovery relapses immediately when its own evidence
	// returns. Opening needs N probes; a known incident never forgets its signal.
	for _, anomaly := range anomalies {
		if ep, exists := episodesByKey[systemIncidentKey(anomaly.Service, anomaly.Verdict)]; exists && ep.Status == models.SystemIncidentStatusRecovering {
			already := false
			for _, current := range confirmed {
				if current.Key == anomaly.Key {
					already = true
					break
				}
			}
			if !already {
				confirmed = append(confirmed, anomaly)
			}
		}
	}

	contained := false
	episodeWriteErrors := 0
	handledConfirmed := []systemAnomaly{}
	for _, anomaly := range confirmed {
		if anomaly.Verdict == models.SystemVerdictQueueBacklog {
			continue
		}
		key := systemIncidentKey(anomaly.Service, anomaly.Verdict)
		ep, exists := episodesByKey[key]
		if exists {
			transition := ""
			if ep.Status == models.SystemIncidentStatusRecovering {
				transition = "relapsed"
			} else if systemEpisodeScopeChanged(ep, anomaly) || ep.Severity != anomaly.Severity || ep.Summary != anomaly.Summary {
				transition = "scope_changed"
			}
			ep.LastSeenAt = now
			ep.Status = models.SystemIncidentStatusOpen
			ep.Shadow = policy.Mode != models.SystemAutopilotModeSafeAuto
			ep.RecoveringSince = nil
			if transition != "" {
				ep.Severity = anomaly.Severity
				ep.Summary = anomaly.Summary
				ep.RootCauseHint = systemRootCauseHint(anomaly)
				ep.Evidence = marshalAutopilotJSON(anomaly.Evidence)
				ep.Timeline = appendSystemEpisodeTimeline(ep.Timeline, transition, now, anomaly, snapshot)
			}
			if err := db.Save(&ep).Error; err != nil {
				episodeWriteErrors++
				writeAction(models.SystemAutopilotAction{
					Target:  anomaly.Service,
					Action:  models.SystemAutopilotActionUpdateEpisode,
					Verdict: anomaly.Verdict,
					Status:  "error",
					Reason:  "failed to update incident episode: " + err.Error(),
					Output:  marshalAutopilotJSON(anomaly),
				})
				continue
			}
			handledConfirmed = append(handledConfirmed, anomaly)
			if transition != "" {
				writeAction(models.SystemAutopilotAction{
					EpisodeID: &ep.ID,
					Target:    anomaly.Service,
					Action:    models.SystemAutopilotActionUpdateEpisode,
					Verdict:   anomaly.Verdict,
					Reason:    anomaly.Summary,
					Output:    marshalAutopilotJSON(anomaly),
				})
			}
		} else {
			ep = models.SystemIncidentEpisode{
				RootService:     anomaly.Service,
				Verdict:         anomaly.Verdict,
				Status:          models.SystemIncidentStatusOpen,
				Severity:        anomaly.Severity,
				Shadow:          policy.Mode != models.SystemAutopilotModeSafeAuto,
				Summary:         anomaly.Summary,
				RootCauseHint:   systemRootCauseHint(anomaly),
				Evidence:        marshalAutopilotJSON(anomaly.Evidence),
				FirstDetectedAt: now,
				LastSeenAt:      now,
			}
			ep.Timeline = appendSystemEpisodeTimeline(nil, "opened", now, anomaly, snapshot)
			if err := db.Create(&ep).Error; err != nil {
				episodeWriteErrors++
				writeAction(models.SystemAutopilotAction{
					Target:  anomaly.Service,
					Action:  models.SystemAutopilotActionOpenEpisode,
					Verdict: anomaly.Verdict,
					Status:  "error",
					Reason:  "failed to create incident episode: " + err.Error(),
					Output:  marshalAutopilotJSON(anomaly),
				})
				continue
			}
			episodesByKey[key] = ep
			handledConfirmed = append(handledConfirmed, anomaly)
			writeAction(models.SystemAutopilotAction{
				EpisodeID: &ep.ID,
				Target:    anomaly.Service,
				Action:    models.SystemAutopilotActionOpenEpisode,
				Verdict:   anomaly.Verdict,
				Reason:    anomaly.Summary,
				Output:    marshalAutopilotJSON(anomaly),
			})
		}
		if isSystemHardDownVerdict(anomaly.Verdict) {
			applied, containmentWriteErrors := handleSystemContainment(db, policy, anomaly, &ep, now, storeAction, func(action models.SystemAutopilotAction) {
				actions = append(actions, action)
			})
			episodeWriteErrors += containmentWriteErrors
			if applied {
				contained = true
			}
		} else {
			writeAction(models.SystemAutopilotAction{
				EpisodeID: &ep.ID,
				Target:    anomaly.Service,
				Action:    models.SystemAutopilotActionSkipped,
				Verdict:   anomaly.Verdict,
				Status:    "skipped",
				Guardrail: models.SystemAutopilotGuardDegradedNoContainment,
				Reason:    "degraded signal opens an incident but cannot pause sibling autopilots",
			})
		}
	}

	resolvedEpisodes, resolutionWriteErrors := resolveRecoveredSystemEpisodes(db, openEpisodes, snapshot, prev, policy.ResolveProbes, now, writeAction)
	episodeWriteErrors += resolutionWriteErrors
	if len(resolvedEpisodes) > 0 {
		episodeWriteErrors += resumeRecoveredSystemContainment(db, policy, resolvedEpisodes, now, storeAction, func(action models.SystemAutopilotAction) {
			actions = append(actions, action)
		})
	}

	runSnapshot := systemRunSnapshot{Timestamp: snapshot.Timestamp, Overall: snapshot.Overall, Services: snapshot.Services, Anomalies: anomalies}
	run.ProbeResults = marshalAutopilotJSON(gin.H{
		"snapshot":  snapshot,
		"anomalies": anomalies,
		"policy": gin.H{
			"confirm_probes": policy.ConfirmProbes,
			"resolve_probes": policy.ResolveProbes,
		},
		"run_snapshot": runSnapshot,
	})
	run.Headline = systemHeadline(snapshot, handledConfirmed, contained, len(resolvedEpisodes))
	run.Summary = systemRunSummary(snapshot, handledConfirmed, len(resolvedEpisodes), contained)
	run.Status = models.SystemAutopilotRunStatusCompleted
	if episodeWriteErrors > 0 {
		run.Status = models.SystemAutopilotRunStatusPartial
		run.Headline = models.SystemAutopilotHeadlineWatching
		run.Error = fmt.Sprintf("failed to persist %d incident episode write(s)", episodeWriteErrors)
		run.ErrorClass = models.SystemAutopilotErrorClassEpisodePersistence
		if len(handledConfirmed) == 0 && len(resolvedEpisodes) == 0 {
			if len(confirmed) == 0 {
				run.Summary = "Episode persistence failed during incident recovery"
			} else {
				run.Summary = fmt.Sprintf("Confirmed %d incident signal(s), but episode persistence failed", len(confirmed))
			}
		} else {
			run.Summary = fmt.Sprintf("%s; %d episode write(s) failed", run.Summary, episodeWriteErrors)
		}
	}
	finished := deps.now()
	run.FinishedAt = &finished
	if err := db.Save(&run).Error; err != nil {
		return run, actions, err
	}
	_ = db.Model(&models.SystemAutopilotPolicy{}).Where("scope = ?", systemAutopilotScope).Updates(map[string]interface{}{
		"last_run_at": finished,
		"updated_at":  finished,
	}).Error
	return run, actions, nil
}

// tryAcquireSystemAutopilotAdvisoryLock holds a PostgreSQL session advisory
// lock for the entire run. The in-process mutex is only a fast path; this lock
// prevents two CMS replicas from creating duplicate incident state.
func tryAcquireSystemAutopilotAdvisoryLock(db *gorm.DB) (func(), bool) {
	sqlDB, err := db.DB()
	if err != nil {
		return func() {}, false
	}
	conn, err := sqlDB.Conn(context.Background())
	if err != nil {
		return func() {}, false
	}
	var acquired bool
	if err := conn.QueryRowContext(context.Background(), "SELECT pg_try_advisory_lock($1)", systemAutopilotAdvisoryKey).Scan(&acquired); err != nil || !acquired {
		_ = conn.Close()
		return func() {}, false
	}
	return func() {
		_, _ = conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", systemAutopilotAdvisoryKey)
		_ = conn.Close()
	}, true
}

func checkSystemCMS(ctx context.Context, db *gorm.DB) systemProbeResult {
	start := time.Now()
	result := systemProbeResult{Name: "cms", DisplayName: "CMS", EndpointURL: "local", Status: "healthy"}
	sqlDB, err := db.DB()
	if err != nil {
		result.Status = "unhealthy"
		result.RawError = err.Error()
		result.Verdicts = []string{models.SystemVerdictServiceDown}
		return result
	}
	err = sqlDB.PingContext(ctx)
	latency := time.Since(start).Milliseconds()
	result.LatencyMS = &latency
	if err != nil {
		result.Status = "unhealthy"
		result.RawError = err.Error()
		result.Deps = []systemProbeDependency{{Name: "postgres", Status: "unhealthy", Detail: err.Error()}}
		result.Verdicts = []string{models.SystemVerdictDependencyDown}
		return result
	}
	result.Deps = []systemProbeDependency{{Name: "postgres", Status: "healthy"}}
	result.Verdicts = []string{models.SystemVerdictHealthy}
	return result
}

func checkSystemIAM(ctx context.Context) systemProbeResult {
	display := systemProbeResult{Name: "iam", DisplayName: "IAM", EndpointURL: systemBaseURL("IAM_BASE_URL")}
	if display.EndpointURL == "" {
		return systemMissingProbe(display, "IAM_BASE_URL")
	}
	r := systemHTTPProbe(ctx, display.EndpointURL+"/health", false)
	body := asSystemRecord(r.Body)
	reported := systemString(body["status"])
	display.EndpointURL = display.EndpointURL + "/health"
	display.LatencyMS = r.LatencyMS
	display.HTTPStatus = r.HTTPStatus
	display.RawError = r.Error
	display.Version = systemString(body["version"])
	display.ReadinessObserved = r.OK && r.JSONObserved && reported == "healthy"
	if display.ReadinessObserved {
		display.Status = "healthy"
		display.Deps = []systemProbeDependency{{Name: "postgres", Status: "healthy"}}
		display.Verdicts = []string{models.SystemVerdictHealthy}
		return display
	}
	if r.HTTPStatus != nil {
		display.Status = "degraded"
		if reported != "" {
			display.Deps = []systemProbeDependency{{Name: "postgres", Status: "unknown", Detail: reported}}
		}
		return display
	}
	display.Status = "unhealthy"
	display.Verdicts = []string{models.SystemVerdictServiceDown}
	return display
}

func checkSystemAggregation(ctx context.Context) systemProbeResult {
	display := systemProbeResult{Name: "aggregation", DisplayName: "Aggregation", EndpointURL: systemBaseURL("AGGREGATION_BASE_URL")}
	if display.EndpointURL == "" {
		return systemMissingProbe(display, "AGGREGATION_BASE_URL")
	}
	var health, ready systemHTTPProbeResult
	var queues []autopilotQueueStat
	var queueErr error
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); health = systemHTTPProbe(ctx, display.EndpointURL+"/health", false) }()
	go func() { defer wg.Done(); ready = systemHTTPProbe(ctx, display.EndpointURL+"/ready", false) }()
	go func() { defer wg.Done(); queues, queueErr = fetchSystemAggregationQueueStats(ctx) }()
	wg.Wait()
	display.LatencyMS = firstLatency(health.LatencyMS, ready.LatencyMS)
	display.HTTPStatus = firstHTTPStatus(health.HTTPStatus, ready.HTTPStatus)
	display.RawError = firstNonEmpty(health.Error, ready.Error)
	readyBody := asSystemRecord(ready.Body)
	display.Deps = mapSystemDependencies(asSystemRecord(readyBody["dependencies"]))
	healthObserved := health.JSONObserved && systemHealthStatusObserved(asSystemRecord(health.Body), "healthy")
	display.ReadinessObserved = ready.JSONObserved && readinessAggregationBodyObserved(readyBody)
	if queueErr == nil {
		display.Queues = queues
	} else if display.RawError == "" {
		display.RawError = queueErr.Error()
	}
	reachable := health.HTTPStatus != nil || ready.HTTPStatus != nil
	switch {
	case !reachable:
		display.Status = "unhealthy"
		display.Verdicts = []string{models.SystemVerdictServiceDown}
	case hasUnhealthySystemDeps(display.Deps):
		display.Status = "degraded"
		display.Verdicts = []string{models.SystemVerdictDependencyDown}
	case !healthObserved || !display.ReadinessObserved:
		display.Status = "degraded"
	case queueErr != nil:
		display.Status = "degraded"
	case hasBackloggedQueues(display.Queues):
		display.Status = "degraded"
		display.Verdicts = []string{models.SystemVerdictQueueBacklog}
	default:
		display.Status = "healthy"
		display.Verdicts = []string{models.SystemVerdictHealthy}
	}
	return display
}

func checkSystemEnrichment(ctx context.Context) systemProbeResult {
	return checkSystemMLService(ctx, "enrichment", "Enrichment", "ENRICHMENT_BASE_URL", false)
}

func checkSystemMedia(ctx context.Context) systemProbeResult {
	return checkSystemMLService(ctx, "media", "Media", "MEDIA_BASE_URL", true)
}

func checkSystemMLService(ctx context.Context, name, displayName, envKey string, includeWorker bool) systemProbeResult {
	base := systemBaseURL(envKey)
	display := systemProbeResult{Name: name, DisplayName: displayName, EndpointURL: base}
	if base == "" {
		return systemMissingProbe(display, envKey)
	}
	var health, ready, queue systemHTTPProbeResult
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); health = systemHTTPProbe(ctx, base+"/health", false) }()
	go func() { defer wg.Done(); ready = systemHTTPProbe(ctx, base+"/ready", false) }()
	if includeWorker {
		wg.Add(1)
		go func() { defer wg.Done(); queue = systemHTTPProbe(ctx, base+"/health/queue", false) }()
	}
	wg.Wait()
	display.LatencyMS = firstLatency(health.LatencyMS, ready.LatencyMS)
	display.HTTPStatus = firstHTTPStatus(health.HTTPStatus, ready.HTTPStatus)
	display.RawError = firstNonEmpty(health.Error, ready.Error)
	healthBody := asSystemRecord(health.Body)
	readyBody := asSystemRecord(ready.Body)
	display.Version = systemString(healthBody["version"])
	display.Models = mapSystemModels(readyBody)
	display.Deps = mapSystemDependencies(asSystemRecord(readyBody["dependencies"]))
	healthObserved := health.JSONObserved && systemHealthStatusObserved(healthBody, "ok")
	display.ReadinessObserved = ready.JSONObserved && readinessMLBodyObserved(readyBody)
	if includeWorker {
		if worker := mapSystemWorker(asSystemRecord(queue.Body)); queue.JSONObserved && worker != nil {
			display.Worker = worker
			// A stalled worker is degraded execution evidence, not a hard
			// service dependency eligible for sibling containment.
		}
	}
	reachable := health.HTTPStatus != nil || ready.HTTPStatus != nil
	switch {
	case !reachable:
		display.Status = "unhealthy"
		display.Verdicts = []string{models.SystemVerdictServiceDown}
	case hasUnhealthySystemDeps(display.Deps):
		display.Status = "degraded"
		display.Verdicts = []string{models.SystemVerdictDependencyDown}
	case !healthObserved || !display.ReadinessObserved:
		display.Status = "degraded"
	case hasUnloadedModels(display.Models):
		display.Status = "degraded"
		display.Verdicts = []string{models.SystemVerdictModelUnloaded}
	case display.Worker != nil && display.Worker.Configured && !display.Worker.Alive && display.Worker.Queued > 0:
		display.Status = "degraded"
		display.Verdicts = []string{models.SystemVerdictWorkerStalled}
	default:
		display.Status = "healthy"
		display.Verdicts = []string{models.SystemVerdictHealthy}
	}
	return display
}

func checkSystemPlatform(ctx context.Context) systemProbeResult {
	base := systemBaseURL("PLATFORM_BASE_URL")
	display := systemProbeResult{Name: "platform", DisplayName: "Wahb-Platform", EndpointURL: base}
	if base == "" {
		return systemMissingProbe(display, "PLATFORM_BASE_URL")
	}
	r := systemHTTPProbe(ctx, base, true)
	display.LatencyMS = r.LatencyMS
	display.HTTPStatus = r.HTTPStatus
	display.RawError = r.Error
	if r.OK {
		display.Status = "healthy"
		display.Verdicts = []string{models.SystemVerdictHealthy}
	} else {
		display.Status = "unhealthy"
		display.Verdicts = []string{models.SystemVerdictServiceDown}
	}
	return display
}

type systemHTTPProbeResult struct {
	OK           bool
	JSONObserved bool
	HTTPStatus   *int
	LatencyMS    *int64
	Body         interface{}
	Error        string
}

func systemHTTPProbe(parent context.Context, url string, allowText bool) systemHTTPProbeResult {
	start := time.Now()
	ctx, cancel := context.WithTimeout(parent, systemProbeTimeout)
	defer cancel()
	client := &http.Client{}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		latency := time.Since(start).Milliseconds()
		return systemHTTPProbeResult{LatencyMS: &latency, Error: err.Error()}
	}
	req.Close = true
	resp, err := client.Do(req)
	latency := time.Since(start).Milliseconds()
	result := systemHTTPProbeResult{LatencyMS: &latency}
	if err != nil {
		result.Error = err.Error()
		return result
	}
	defer resp.Body.Close()
	status := resp.StatusCode
	result.HTTPStatus = &status
	result.OK = resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices
	raw, err := io.ReadAll(io.LimitReader(resp.Body, systemProbeBodyLimit+1))
	if err != nil {
		result.Error = err.Error()
		return result
	}
	if len(raw) > systemProbeBodyLimit {
		result.Error = "probe response exceeds 256 KiB limit"
		return result
	}
	if allowText {
		return result
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		result.Error = "probe response is missing JSON"
		return result
	}
	var body interface{}
	if err := json.Unmarshal(raw, &body); err != nil {
		result.Error = "probe response is not valid JSON"
		return result
	}
	result.Body = body
	result.JSONObserved = true
	return result
}

// fetchSystemAggregationQueueStats is intentionally separate from the generic
// Aggregation helper: System Health must obey its phase context and cannot let
// a queue read consume the generic helper's 30-second, unbounded budget.
func fetchSystemAggregationQueueStats(parent context.Context) ([]autopilotQueueStat, error) {
	base := systemBaseURL("AGGREGATION_BASE_URL")
	if base == "" {
		return nil, fmt.Errorf("aggregation service URL is not configured")
	}
	token := aggregationInternalServiceToken()
	if token == "" {
		return nil, fmt.Errorf("aggregation service token is not configured")
	}
	ctx, cancel := context.WithTimeout(parent, systemProbeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/internal/queues", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("aggregation queues responded with status %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, systemQueueProbeBodyLimit+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > systemQueueProbeBodyLimit {
		return nil, fmt.Errorf("aggregation queue response exceeds 1 MiB limit")
	}
	var wrapped struct {
		Data []autopilotQueueStat `json:"data"`
	}
	if err := json.Unmarshal(raw, &wrapped); err != nil {
		return nil, fmt.Errorf("decode aggregation queues: %w", err)
	}
	return wrapped.Data, nil
}

func readinessAggregationBodyObserved(body map[string]interface{}) bool {
	status := systemString(body["status"])
	_, hasDeps := body["dependencies"]
	return (status == "ready" || status == "not_ready") && hasDeps
}

func readinessMLBodyObserved(body map[string]interface{}) bool {
	status := systemString(body["status"])
	_, hasModels := body["models"]
	_, hasDeps := body["dependencies"]
	return (status == "ok" || status == "not_ready") && hasModels && hasDeps
}

func systemHealthStatusObserved(body map[string]interface{}, expected string) bool {
	return systemString(body["status"]) == expected
}

func systemBaseURL(key string) string {
	return strings.TrimRight(strings.TrimSpace(os.Getenv(key)), "/")
}

func systemMissingProbe(result systemProbeResult, envKey string) systemProbeResult {
	result.Status = "unknown"
	result.RawError = envKey + " not configured"
	return result
}

func asSystemRecord(value interface{}) map[string]interface{} {
	if value == nil {
		return map[string]interface{}{}
	}
	if record, ok := value.(map[string]interface{}); ok {
		return record
	}
	return map[string]interface{}{}
}

func systemString(value interface{}) string {
	if s, ok := value.(string); ok {
		return s
	}
	return ""
}

func firstLatency(values ...*int64) *int64 {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

func firstHTTPStatus(values ...*int) *int {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func mapSystemDependencies(input map[string]interface{}) []systemProbeDependency {
	deps := []systemProbeDependency{}
	for name, value := range input {
		status := "unknown"
		detail := ""
		switch v := value.(type) {
		case bool:
			if v {
				status = "healthy"
			} else {
				status = "unhealthy"
			}
		case string:
			detail = v
			status = systemDependencyStatus(v)
		default:
			detail = fmt.Sprintf("%v", value)
		}
		deps = append(deps, systemProbeDependency{Name: name, Status: status, Detail: detail})
	}
	sort.Slice(deps, func(i, j int) bool { return deps[i].Name < deps[j].Name })
	return deps
}

func systemDependencyHealthyString(value string) bool {
	return systemDependencyStatus(value) == "healthy"
}

func systemDependencyStatus(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "connected", "reachable", "configured", "ready", "ok", "true":
		return "healthy"
	case "disconnected", "unreachable", "circuit_open", "not_ready", "false":
		return "unhealthy"
	default:
		return "unknown"
	}
}

func mapSystemModels(readyBody map[string]interface{}) []systemProbeModel {
	modelsOut := []systemProbeModel{}
	if detail, ok := readyBody["models_detail"].([]interface{}); ok {
		for _, raw := range detail {
			record := asSystemRecord(raw)
			role := systemString(record["type"])
			if role == "" {
				role = systemString(record["name"])
			}
			name := systemString(record["name"])
			detailText := name
			if dims, ok := record["dimensions"].(float64); ok && dims > 0 {
				detailText = fmt.Sprintf("%s · %dd", name, int(dims))
			}
			modelsOut = append(modelsOut, systemProbeModel{Name: role, Loaded: record["loaded"] == true, Detail: detailText})
		}
		return modelsOut
	}
	modelMap := asSystemRecord(readyBody["models"])
	for name, raw := range modelMap {
		loaded := raw == true
		if record := asSystemRecord(raw); len(record) > 0 {
			loaded = record["loaded"] == true
		}
		modelsOut = append(modelsOut, systemProbeModel{Name: name, Loaded: loaded})
	}
	sort.Slice(modelsOut, func(i, j int) bool { return modelsOut[i].Name < modelsOut[j].Name })
	return modelsOut
}

func mapSystemWorker(body map[string]interface{}) *systemProbeWorker {
	if len(body) == 0 {
		return nil
	}
	if _, ok := body["configured"]; !ok {
		return nil
	}
	return &systemProbeWorker{
		Configured: body["configured"] == true,
		Alive:      body["worker_alive"] == true,
		Queued:     systemInt(body["queued"]),
		Ongoing:    systemInt(body["jobs_ongoing"]),
		Complete:   systemInt(body["jobs_complete"]),
		Failed:     systemInt(body["jobs_failed"]),
		Retried:    systemInt(body["jobs_retried"]),
	}
}

func systemInt(value interface{}) int {
	switch v := value.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	default:
		return 0
	}
}

func hasUnhealthySystemDeps(deps []systemProbeDependency) bool {
	for _, dep := range deps {
		if dep.Status == "unhealthy" {
			return true
		}
	}
	return false
}

func hasUnloadedModels(items []systemProbeModel) bool {
	for _, item := range items {
		if !item.Loaded {
			return true
		}
	}
	return false
}

func hasBackloggedQueues(stats []autopilotQueueStat) bool {
	for _, q := range stats {
		if q.Waiting > systemQueueWaitingWarn || q.Failed > 0 {
			return true
		}
	}
	return false
}

func systemIssuesFromServices(services []systemProbeResult) []systemHealthIssue {
	issues := []systemHealthIssue{}
	for _, svc := range services {
		if svc.Status == "unhealthy" {
			msg := fmt.Sprintf("%s unhealthy", svc.DisplayName)
			if svc.RawError != "" {
				msg = fmt.Sprintf("%s unreachable: %s", svc.DisplayName, svc.RawError)
			}
			issues = append(issues, systemHealthIssue{Severity: "critical", Service: svc.Name, Message: msg})
		}
		for _, dep := range svc.Deps {
			if dep.Status == "unhealthy" {
				issues = append(issues, systemHealthIssue{Severity: "critical", Service: svc.Name, Message: fmt.Sprintf("%s dependency %q is unhealthy", svc.DisplayName, dep.Name)})
			}
		}
		for _, q := range svc.Queues {
			if q.Failed > 0 {
				issues = append(issues, systemHealthIssue{Severity: "warning", Service: svc.Name, Message: fmt.Sprintf("Queue %q has %d failed jobs", q.Queue, q.Failed)})
			}
			if q.Waiting > systemQueueWaitingWarn {
				issues = append(issues, systemHealthIssue{Severity: "warning", Service: svc.Name, Message: fmt.Sprintf("Queue %q is backed up (%d waiting)", q.Queue, q.Waiting)})
			}
		}
		for _, model := range svc.Models {
			if !model.Loaded {
				issues = append(issues, systemHealthIssue{Severity: "warning", Service: svc.Name, Message: fmt.Sprintf("Model %q is not loaded", model.Name)})
			}
		}
		if svc.Worker != nil && svc.Worker.Configured && !svc.Worker.Alive && svc.Worker.Queued > 0 {
			issues = append(issues, systemHealthIssue{Severity: "critical", Service: svc.Name, Message: fmt.Sprintf("Async worker is down with %d queued jobs", svc.Worker.Queued)})
		}
	}
	return issues
}

func systemAnomaliesFromServices(services []systemProbeResult) []systemAnomaly {
	anomalies := []systemAnomaly{}
	for _, svc := range services {
		for _, verdict := range svc.Verdicts {
			if verdict == models.SystemVerdictHealthy {
				continue
			}
			severity := "warning"
			if verdict == models.SystemVerdictServiceDown || verdict == models.SystemVerdictDependencyDown || verdict == models.SystemVerdictWorkerStalled {
				severity = "critical"
			}
			summary := systemAnomalySummary(svc, verdict)
			anomalies = append(anomalies, systemAnomaly{
				Key:      systemIncidentKey(svc.Name, verdict),
				Service:  svc.Name,
				Verdict:  verdict,
				Severity: severity,
				Summary:  summary,
				Evidence: map[string]interface{}{
					"service":      svc,
					"endpoint_url": svc.EndpointURL,
				},
			})
		}
	}
	return anomalies
}

func systemAnomalySummary(svc systemProbeResult, verdict string) string {
	switch verdict {
	case models.SystemVerdictServiceDown:
		return fmt.Sprintf("%s is unreachable or failing health probes", svc.DisplayName)
	case models.SystemVerdictDependencyDown:
		return fmt.Sprintf("%s has an unhealthy dependency", svc.DisplayName)
	case models.SystemVerdictQueueBacklog:
		return fmt.Sprintf("%s queues are backed up", svc.DisplayName)
	case models.SystemVerdictModelUnloaded:
		return fmt.Sprintf("%s has unloaded model(s)", svc.DisplayName)
	case models.SystemVerdictWorkerStalled:
		return fmt.Sprintf("%s async worker is stalled", svc.DisplayName)
	case models.SystemVerdictTransientProbeFailure:
		return fmt.Sprintf("%s probe is not configured or transiently unavailable", svc.DisplayName)
	default:
		return fmt.Sprintf("%s reported %s", svc.DisplayName, verdict)
	}
}

// systemEpisodeScopeChanged deliberately compares only the stable causal
// scope. Probe payloads contain latency and other live diagnostics that change
// on every run and must not grow an incident timeline indefinitely.
func systemEpisodeScopeChanged(ep models.SystemIncidentEpisode, anomaly systemAnomaly) bool {
	var stored struct {
		Evidence map[string]interface{} `json:"evidence"`
	}
	if err := json.Unmarshal(ep.Evidence, &stored); err != nil {
		return true
	}
	return systemEvidenceScope(stored.Evidence, ep.RootService) != systemEvidenceScope(anomaly.Evidence, anomaly.Service)
}

func systemEvidenceScope(evidence map[string]interface{}, fallback string) string {
	services := []string{}
	for _, value := range []interface{}{evidence["services"], evidence["members"]} {
		switch items := value.(type) {
		case []string:
			services = append(services, items...)
		case []interface{}:
			for _, item := range items {
				if service := systemString(asSystemRecord(item)["service"]); service != "" {
					services = append(services, service)
				} else if service, ok := item.(string); ok && service != "" {
					services = append(services, service)
				}
			}
		}
	}
	if len(services) == 0 {
		services = append(services, fallback)
	}
	sort.Strings(services)
	return strings.Join(services, ",")
}

func systemRootCauseHint(anomaly systemAnomaly) string {
	if anomaly.Verdict == models.SystemVerdictMultiServiceIncident {
		if raw, ok := anomaly.Evidence["hard_down_services"]; ok {
			return fmt.Sprintf("Multiple hard-down services in the same probe: %v", raw)
		}
		return "Multiple hard-down services in the same probe"
	}
	switch anomaly.Verdict {
	case models.SystemVerdictDependencyDown:
		return "Service is reachable, but one of its declared dependencies is unhealthy"
	case models.SystemVerdictServiceDown:
		return "Service health or readiness endpoint is unreachable"
	case models.SystemVerdictWorkerStalled:
		return "Worker is not alive while queue work is waiting"
	case models.SystemVerdictModelUnloaded:
		return "Service readiness reports at least one model unloaded"
	case models.SystemVerdictQueueBacklog:
		return "Queue backlog is sustained while service probes are otherwise alive"
	default:
		return ""
	}
}

func appendSystemEpisodeTimeline(existing datatypes.JSON, transition string, at time.Time, anomaly systemAnomaly, snapshot systemHealthSnapshot) datatypes.JSON {
	timeline := []map[string]interface{}{}
	if len(existing) > 0 {
		_ = json.Unmarshal(existing, &timeline)
	}
	timeline = append(timeline, map[string]interface{}{
		"transition": transition,
		"at":         at.Format(time.RFC3339),
		"service":    anomaly.Service,
		"verdict":    anomaly.Verdict,
		"severity":   anomaly.Severity,
		"summary":    anomaly.Summary,
		"overall":    snapshot.Overall,
		"issues":     snapshot.Issues,
	})
	// Episodes are long lived; retain enough state transitions for diagnosis
	// without allowing unchanged incidents to create unbounded JSONB rows.
	if len(timeline) > 200 {
		timeline = timeline[len(timeline)-200:]
	}
	return marshalAutopilotJSON(timeline)
}

func systemIncidentKey(service, verdict string) string {
	return service + ":" + verdict
}

func isSystemHardDownVerdict(verdict string) bool {
	return verdict == models.SystemVerdictServiceDown || verdict == models.SystemVerdictDependencyDown || verdict == models.SystemVerdictMultiServiceIncident
}

// systemCorrelationRoot is the approved static dependency graph. Only two or
// more symptoms of the same declared root are folded; unrelated failures stay
// independent and no broad platform incident bypasses ownership.
func systemCorrelationRoot(anomaly systemAnomaly) string {
	switch {
	case anomaly.Service == "aggregation" && anomaly.Verdict == models.SystemVerdictQueueBacklog:
		return "redis"
	case anomaly.Service == "media" && anomaly.Verdict == models.SystemVerdictWorkerStalled:
		return "redis"
	case (anomaly.Service == "cms" || anomaly.Service == "iam") && anomaly.Verdict == models.SystemVerdictDependencyDown:
		return "postgres"
	case (anomaly.Service == "aggregation" || anomaly.Service == "enrichment" || anomaly.Service == "media") && anomaly.Verdict == models.SystemVerdictServiceDown:
		return "cms"
	default:
		return ""
	}
}

func correlateSystemAnomalies(anomalies []systemAnomaly) []systemAnomaly {
	groups := map[string][]systemAnomaly{}
	out := []systemAnomaly{}
	for _, anomaly := range anomalies {
		if root := systemCorrelationRoot(anomaly); root != "" {
			groups[root] = append(groups[root], anomaly)
		} else {
			out = append(out, anomaly)
		}
	}
	roots := make([]string, 0, len(groups))
	for root := range groups {
		roots = append(roots, root)
	}
	sort.Strings(roots)
	for _, root := range roots {
		members := groups[root]
		if len(members) < 2 {
			out = append(out, members...)
			continue
		}
		services := make([]string, 0, len(members))
		for _, member := range members {
			services = append(services, member.Service)
		}
		sort.Strings(services)
		out = append(out, systemAnomaly{
			Key:       systemIncidentKey(root, models.SystemVerdictDependencyDown),
			Service:   root,
			Verdict:   models.SystemVerdictDependencyDown,
			Severity:  "critical",
			Summary:   fmt.Sprintf("%s is the shared dependency for %s", root, strings.Join(services, ", ")),
			Evidence:  map[string]interface{}{"root": root, "services": services, "members": members},
			Confirmed: true,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

func countHardDownServices(anomalies []systemAnomaly) int {
	seen := map[string]bool{}
	for _, anomaly := range anomalies {
		if isSystemHardDownVerdict(anomaly.Verdict) {
			seen[anomaly.Service] = true
		}
	}
	return len(seen)
}

func hardDownServiceNames(anomalies []systemAnomaly) []string {
	seen := map[string]bool{}
	for _, anomaly := range anomalies {
		if isSystemHardDownVerdict(anomaly.Verdict) {
			seen[anomaly.Service] = true
		}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func recentSystemRunSnapshots(db *gorm.DB, limit int) []systemRunSnapshot {
	var runs []models.SystemAutopilotRun
	if err := db.Where("status IN ?", []string{models.SystemAutopilotRunStatusCompleted, models.SystemAutopilotRunStatusPartial}).
		Order("started_at DESC").Limit(limit).Find(&runs).Error; err != nil {
		return nil
	}
	out := []systemRunSnapshot{}
	for _, run := range runs {
		var wrapper struct {
			RunSnapshot systemRunSnapshot `json:"run_snapshot"`
		}
		if err := json.Unmarshal(run.ProbeResults, &wrapper); err == nil && wrapper.RunSnapshot.Timestamp != "" {
			out = append(out, wrapper.RunSnapshot)
		}
	}
	return out
}

func confirmSystemAnomalies(current []systemAnomaly, prev []systemRunSnapshot, confirmProbes int) []systemAnomaly {
	if confirmProbes <= 1 {
		for i := range current {
			current[i].Confirmed = true
		}
		return current
	}
	confirmed := []systemAnomaly{}
	for _, anomaly := range current {
		count := 1
		for _, run := range prev {
			if runHasSystemAnomaly(run, anomaly.Key) {
				count++
				if count >= confirmProbes {
					break
				}
			} else {
				break
			}
		}
		if count >= confirmProbes {
			anomaly.Confirmed = true
			confirmed = append(confirmed, anomaly)
		}
	}
	return confirmed
}

func runHasSystemAnomaly(run systemRunSnapshot, key string) bool {
	for _, anomaly := range run.Anomalies {
		if anomaly.Key == key {
			return true
		}
	}
	return false
}

func systemAnomalyStreak(current systemAnomaly, prev []systemRunSnapshot) int {
	count := 1
	for _, run := range prev {
		if !runHasSystemAnomaly(run, current.Key) {
			break
		}
		count++
	}
	return count
}

func applySystemFlapGuard(db *gorm.DB, current []systemAnomaly, maxFlaps int, now time.Time, writeAction func(models.SystemAutopilotAction)) []systemAnomaly {
	if maxFlaps <= 0 {
		return current
	}
	out := []systemAnomaly{}
	for _, anomaly := range current {
		var count int64
		since := now.UTC().Add(-24 * time.Hour)
		_ = db.Model(&models.SystemIncidentEpisode{}).
			Where("root_service = ? AND verdict = ? AND status = ? AND resolved_at >= ?", anomaly.Service, anomaly.Verdict, models.SystemIncidentStatusResolved, since).
			Count(&count).Error
		if int(count) >= maxFlaps {
			writeAction(models.SystemAutopilotAction{
				Target:    anomaly.Service,
				Action:    models.SystemAutopilotActionSkipped,
				Verdict:   anomaly.Verdict,
				Status:    "attention",
				Guardrail: models.SystemAutopilotGuardFlapping,
				Reason:    "incident has flapped repeatedly in the last 24h",
				Output:    marshalAutopilotJSON(anomaly),
			})
			continue
		}
		out = append(out, anomaly)
	}
	return out
}

func openSystemIncidentEpisodes(db *gorm.DB) []models.SystemIncidentEpisode {
	var episodes []models.SystemIncidentEpisode
	_ = db.Where("status IN ?", []string{models.SystemIncidentStatusOpen, models.SystemIncidentStatusRecovering}).
		Order("last_seen_at DESC").Find(&episodes).Error
	return episodes
}

func resolveRecoveredSystemEpisodes(db *gorm.DB, openEpisodes []models.SystemIncidentEpisode, snapshot systemHealthSnapshot, prev []systemRunSnapshot, resolveProbes int, now time.Time, writeAction func(models.SystemAutopilotAction)) ([]models.SystemIncidentEpisode, int) {
	if resolveProbes < 1 {
		resolveProbes = 1
	}
	resolved := []models.SystemIncidentEpisode{}
	writeErrors := 0
	for _, ep := range openEpisodes {
		if !systemEpisodeObservablyHealthy(ep, snapshot) {
			continue
		}
		ok := 1
		for _, run := range prev {
			if systemEpisodeObservablyHealthyServices(ep, run.Services) {
				ok++
				if ok >= resolveProbes {
					break
				}
			} else {
				break
			}
		}
		if ok < resolveProbes {
			if ep.Status != models.SystemIncidentStatusRecovering {
				recoveringAt := now.UTC()
				ep.Status = models.SystemIncidentStatusRecovering
				ep.RecoveringSince = &recoveringAt
				if err := db.Save(&ep).Error; err != nil {
					writeErrors++
					writeAction(models.SystemAutopilotAction{
						EpisodeID: &ep.ID,
						Target:    ep.RootService,
						Action:    models.SystemAutopilotActionUpdateEpisode,
						Verdict:   ep.Verdict,
						Status:    "error",
						Reason:    "failed to mark incident recovering: " + err.Error(),
					})
				}
			}
			continue
		}
		resolvedAt := now.UTC()
		ep.Status = models.SystemIncidentStatusResolved
		ep.ResolvedAt = &resolvedAt
		ep.Timeline = appendSystemEpisodeTimeline(ep.Timeline, "resolved", resolvedAt, systemAnomaly{
			Key:      systemIncidentKey(ep.RootService, ep.Verdict),
			Service:  ep.RootService,
			Verdict:  ep.Verdict,
			Severity: ep.Severity,
			Summary:  ep.Summary,
		}, snapshot)
		if err := db.Save(&ep).Error; err != nil {
			writeErrors++
			writeAction(models.SystemAutopilotAction{
				EpisodeID: &ep.ID,
				Target:    ep.RootService,
				Action:    models.SystemAutopilotActionResolveEpisode,
				Verdict:   ep.Verdict,
				Status:    "error",
				Reason:    "failed to resolve incident episode: " + err.Error(),
			})
			continue
		}
		resolved = append(resolved, ep)
		writeAction(models.SystemAutopilotAction{
			EpisodeID: &ep.ID,
			Target:    ep.RootService,
			Action:    models.SystemAutopilotActionResolveEpisode,
			Verdict:   ep.Verdict,
			Reason:    "service stayed healthy for the configured resolve probes",
		})
	}
	return resolved, writeErrors
}

func systemEpisodeObservablyHealthy(ep models.SystemIncidentEpisode, snapshot systemHealthSnapshot) bool {
	return systemEpisodeObservablyHealthyServices(ep, snapshot.Services)
}

func systemEpisodeObservablyHealthyServices(ep models.SystemIncidentEpisode, services []systemProbeResult) bool {
	healthy := map[string]bool{}
	for _, svc := range services {
		healthy[svc.Name] = svc.Status == "healthy"
	}
	switch ep.RootService {
	case "redis":
		return healthy["aggregation"] && healthy["media"]
	case "postgres":
		return healthy["cms"] && healthy["iam"]
	case "cms":
		return healthy["aggregation"] && healthy["enrichment"] && healthy["media"]
	default:
		return healthy[ep.RootService]
	}
}

func containmentDisabledSet(policy models.SystemAutopilotPolicy) map[string]bool {
	disabled := map[string]bool{}
	var values []string
	if err := json.Unmarshal(policy.ContainmentDisabledFor, &values); err != nil {
		_ = json.Unmarshal(models.DefaultSystemAutopilotPolicy().ContainmentDisabledFor, &values)
	}
	for _, value := range values {
		disabled[strings.TrimSpace(value)] = true
	}
	return disabled
}

type systemSiblingPolicyRow struct {
	TenantID    string
	PausedUntil *time.Time
}

// Version 1 recorded one timestamp per sibling. It remains readable for
// historical display, but never authorizes an automatic resume because it did
// not identify the tenant policy row that System Health changed.
type systemContainmentLedger struct {
	Version  int                                                `json:"version"`
	Siblings map[string]map[string]systemContainmentLedgerEntry `json:"siblings"`
}

type systemContainmentLedgerEntry struct {
	WrittenUntil string `json:"written_until,omitempty"`
	Outcome      string `json:"outcome"`
	Reason       string `json:"reason,omitempty"`
}

type systemAutopilotActionStore func(*gorm.DB, models.SystemAutopilotAction) (models.SystemAutopilotAction, error)

func readSystemContainmentLedger(raw datatypes.JSON) (systemContainmentLedger, bool) {
	if len(raw) == 0 {
		return systemContainmentLedger{Version: 2, Siblings: map[string]map[string]systemContainmentLedgerEntry{}}, false
	}
	var ledger systemContainmentLedger
	if err := json.Unmarshal(raw, &ledger); err != nil || ledger.Version != 2 {
		return systemContainmentLedger{Version: 2, Siblings: map[string]map[string]systemContainmentLedgerEntry{}}, true
	}
	if ledger.Siblings == nil {
		ledger.Siblings = map[string]map[string]systemContainmentLedgerEntry{}
	}
	return ledger, false
}

func containmentEntry(ledger systemContainmentLedger, sibling, tenant string) (systemContainmentLedgerEntry, bool) {
	entry, ok := ledger.Siblings[sibling][tenant]
	return entry, ok
}

func storeSystemContainmentEntry(tx *gorm.DB, ep *models.SystemIncidentEpisode, ledger systemContainmentLedger, sibling, tenant string, entry systemContainmentLedgerEntry) (datatypes.JSON, error) {
	if ledger.Siblings[sibling] == nil {
		ledger.Siblings[sibling] = map[string]systemContainmentLedgerEntry{}
	}
	ledger.Siblings[sibling][tenant] = entry
	payload := marshalAutopilotJSON(ledger)
	if err := tx.Model(&models.SystemIncidentEpisode{}).Where("id = ?", ep.ID).Update("containment", payload).Error; err != nil {
		return nil, err
	}
	return payload, nil
}

func handleSystemContainment(db *gorm.DB, policy models.SystemAutopilotPolicy, anomaly systemAnomaly, ep *models.SystemIncidentEpisode, now time.Time, storeAction systemAutopilotActionStore, actionSink func(models.SystemAutopilotAction)) (bool, int) {
	disabled := containmentDisabledSet(policy)
	now = now.UTC()
	containmentPaused := policy.ContainmentPausedUntil != nil && policy.ContainmentPausedUntil.After(now)
	desiredUntil := now.Add(time.Duration(policy.ContainmentTTLMinutes) * time.Minute)
	applied, writeErrors := false, 0
	write := func(action models.SystemAutopilotAction) {
		if stored, err := storeAction(db, action); err != nil {
			writeErrors++
		} else {
			actionSink(stored)
		}
	}
	for _, sibling := range systemSiblingAutopilots {
		if !siblingDependsOnService(sibling, anomaly.Service) && anomaly.Verdict != models.SystemVerdictMultiServiceIncident {
			continue
		}
		base := models.SystemAutopilotAction{EpisodeID: &ep.ID, Target: sibling.Key, Verdict: anomaly.Verdict}
		if disabled[sibling.Key] {
			base.Action, base.Status, base.Guardrail = models.SystemAutopilotActionSkipped, "skipped", models.SystemAutopilotGuardOptedOut
			base.Reason = sibling.Label + " is registered but opted out of System Health containment"
			write(base)
			continue
		}
		if containmentPaused {
			base.Action, base.Status, base.Guardrail = models.SystemAutopilotActionWouldPause, "would_execute", models.SystemAutopilotGuardPaused
			base.Reason = "Containment is paused by a human; would pause " + sibling.Label
			write(base)
			continue
		}
		if policy.Mode != models.SystemAutopilotModeSafeAuto {
			base.Action, base.Status, base.Guardrail = models.SystemAutopilotActionWouldPause, "would_execute", models.SystemAutopilotGuardObserveMode
			base.Reason = "Observe mode would pause " + sibling.Label
			write(base)
			continue
		}
		rows, err := systemSiblingPolicyRows(db, sibling)
		if err != nil {
			base.Action, base.Status, base.Reason = models.SystemAutopilotActionSkipped, "error", "failed to list sibling policy rows: "+err.Error()
			writeErrors++
			write(base)
			continue
		}
		for _, row := range rows {
			action := base
			action.Output = marshalAutopilotJSON(gin.H{"tenant_id": row.TenantID, "paused_until": desiredUntil.Format(time.RFC3339Nano), "incident": anomaly.Key})
			ledger, _ := readSystemContainmentLedger(ep.Containment)
			owned, ownsPause := containmentEntry(ledger, sibling.Key, row.TenantID)
			var stored models.SystemAutopilotAction
			var updatedContainment datatypes.JSON
			mutated := false
			err := db.Transaction(func(tx *gorm.DB) error {
				entry := systemContainmentLedgerEntry{Outcome: "skipped"}
				if row.PausedUntil != nil && (!ownsPause || owned.WrittenUntil == "") {
					action.Action, action.Status, action.Guardrail = models.SystemAutopilotActionSkipped, "skipped", models.SystemAutopilotGuardHumanPause
					action.Reason, entry.Reason = "A human or another incident already owns this tenant pause", "human_pause"
				} else if row.PausedUntil != nil && !row.PausedUntil.Before(desiredUntil) {
					action.Action, action.Status, action.Guardrail = models.SystemAutopilotActionSkipped, "skipped", models.SystemAutopilotGuardContainmentTTL
					action.Reason, entry = "Existing System Health pause already covers the requested containment TTL", owned
				} else {
					where := "IS NULL"
					args := []interface{}{desiredUntil, now, row.TenantID}
					if row.PausedUntil != nil {
						where, args = "= ?", append(args, *row.PausedUntil)
					}
					query := systemPauseCompareAndSetSQL(sibling, where)
					var written time.Time
					result := tx.Raw(query, args...).Scan(&written)
					if result.Error != nil {
						return result.Error
					}
					if result.RowsAffected == 0 {
						action.Action, action.Status, action.Guardrail = models.SystemAutopilotActionSkipped, "skipped", models.SystemAutopilotGuardHumanPause
						action.Reason, entry.Reason = "Sibling pause changed before containment compare-and-set", "human_pause"
					} else {
						mutated = true
						entry = systemContainmentLedgerEntry{WrittenUntil: written.UTC().Format(time.RFC3339Nano), Outcome: "paused"}
						action.Action, action.Status, action.Reason = models.SystemAutopilotActionPauseSibling, "success", "Paused "+sibling.Label+" until dependency recovers"
					}
				}
				var err error
				updatedContainment, err = storeSystemContainmentEntry(tx, ep, ledger, sibling.Key, row.TenantID, entry)
				if err != nil {
					return err
				}
				stored, err = storeAction(tx, action)
				return err
			})
			if err != nil {
				writeErrors++
				continue
			}
			ep.Containment = updatedContainment
			actionSink(stored)
			applied = applied || mutated
		}
	}
	return applied, writeErrors
}

func siblingDependsOnService(sibling systemSiblingAutopilot, service string) bool {
	for _, dep := range sibling.Dependencies {
		if dep == service {
			return true
		}
	}
	return false
}

func systemSiblingPolicyRows(db *gorm.DB, sibling systemSiblingAutopilot) ([]systemSiblingPolicyRow, error) {
	if err := ensureSystemSiblingPolicyRow(db, sibling); err != nil {
		return nil, err
	}
	var rows []systemSiblingPolicyRow
	query := fmt.Sprintf("SELECT tenant_id, %s AS paused_until FROM %s", sibling.PauseColumn, sibling.Table)
	if err := db.Raw(query).Scan(&rows).Error; err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("%s has no policy rows to pause", sibling.Label)
	}
	return rows, nil
}

func ensureSystemSiblingPolicyRow(db *gorm.DB, sibling systemSiblingAutopilot) error {
	switch sibling.Key {
	case "pipeline":
		policy := models.DefaultPipelineAutopilotPolicy(defaultCirculationTenant)
		return db.Where("tenant_id = ?", defaultCirculationTenant).FirstOrCreate(&policy).Error
	case "enrichment":
		policy := models.DefaultEnrichmentAutopilotPolicy(defaultCirculationTenant)
		return db.Where("tenant_id = ?", defaultCirculationTenant).FirstOrCreate(&policy).Error
	case "embedding_lifecycle":
		_, err := getOrCreateEmbeddingPolicy(db)
		return err
	case "news_circulation":
		policy := models.DefaultNewsCirculationPolicy(defaultCirculationTenant)
		return db.Where("tenant_id = ?", defaultCirculationTenant).FirstOrCreate(&policy).Error
	case "media_circulation":
		policy := models.DefaultMediaCirculationPolicy(defaultCirculationTenant)
		return db.Where("tenant_id = ?", defaultCirculationTenant).FirstOrCreate(&policy).Error
	case "media_studio":
		policy := models.DefaultMediaStudioAutopilotPolicy(defaultCirculationTenant)
		return db.Where("tenant_id = ?", defaultCirculationTenant).FirstOrCreate(&policy).Error
	case "redundancy":
		policy := models.DefaultRedundancyPolicy(defaultCirculationTenant)
		return db.Where("tenant_id = ?", defaultCirculationTenant).FirstOrCreate(&policy).Error
	}
	return fmt.Errorf("unregistered sibling autopilot %q", sibling.Key)
}

func resumeRecoveredSystemContainment(db *gorm.DB, policy models.SystemAutopilotPolicy, episodes []models.SystemIncidentEpisode, now time.Time, storeAction systemAutopilotActionStore, actionSink func(models.SystemAutopilotAction)) int {
	if policy.Mode != models.SystemAutopilotModeSafeAuto {
		return 0
	}
	writeErrors := 0
	now = now.UTC()
	containmentPaused := policy.ContainmentPausedUntil != nil && policy.ContainmentPausedUntil.After(now)
	for _, episode := range episodes {
		ledger, legacy := readSystemContainmentLedger(episode.Containment)
		if legacy {
			// V1 had no tenant identity. Preserving the old JSON is intentional:
			// clearing it would risk removing a human pause.
			if stored, err := storeAction(db, models.SystemAutopilotAction{EpisodeID: &episode.ID, Target: episode.RootService, Action: models.SystemAutopilotActionSkipped, Status: "skipped", Guardrail: models.SystemAutopilotGuardHumanPause, Reason: "Legacy containment ownership lacks tenant identity; pause will expire naturally"}); err != nil {
				writeErrors++
			} else {
				actionSink(stored)
			}
			continue
		}
		for key, tenants := range ledger.Siblings {
			sibling, ok := systemSiblingByKey(key)
			if !ok {
				continue
			}
			for tenantID, ownership := range tenants {
				if ownership.WrittenUntil == "" {
					continue
				}
				until, err := time.Parse(time.RFC3339Nano, ownership.WrittenUntil)
				if err != nil {
					writeErrors++
					continue
				}
				action := models.SystemAutopilotAction{EpisodeID: &episode.ID, Target: sibling.Key, Output: marshalAutopilotJSON(gin.H{"tenant_id": tenantID, "episode_id": episode.PublicID.String(), "paused_until": ownership.WrittenUntil})}
				if containmentPaused {
					action.Action, action.Status, action.Guardrail = models.SystemAutopilotActionWouldResume, "would_execute", models.SystemAutopilotGuardPaused
					action.Reason = "Containment is paused by a human; would resume " + sibling.Label
					if stored, err := storeAction(db, action); err != nil {
						writeErrors++
					} else {
						actionSink(stored)
					}
					continue
				}
				if systemActiveIncidentOwnsSiblingTenant(db, episode.ID, sibling.Key, tenantID) {
					action.Action, action.Status, action.Guardrail = models.SystemAutopilotActionSkipped, "skipped", models.SystemAutopilotGuardContainmentTTL
					action.Reason = "Another active incident still owns this tenant containment pause"
					if stored, err := storeAction(db, action); err != nil {
						writeErrors++
					} else {
						actionSink(stored)
					}
					continue
				}
				var stored models.SystemAutopilotAction
				var updatedContainment datatypes.JSON
				err = db.Transaction(func(tx *gorm.DB) error {
					query := systemResumeCompareAndSetSQL(sibling)
					result := tx.Exec(query, now, tenantID, until)
					entry := ownership
					if result.Error != nil {
						return result.Error
					}
					if result.RowsAffected == 0 {
						action.Action, action.Status, action.Guardrail = models.SystemAutopilotActionSkipped, "skipped", models.SystemAutopilotGuardHumanPause
						action.Reason, entry.Outcome, entry.Reason = "Sibling pause no longer exactly matches System Health ownership", "skipped", "human_pause"
					} else {
						action.Action, action.Status, action.Reason = models.SystemAutopilotActionResumeSibling, "success", "Cleared resolved System Health containment pause"
						entry.Outcome, entry.Reason = "resumed", ""
					}
					var entryErr error
					updatedContainment, entryErr = storeSystemContainmentEntry(tx, &episode, ledger, sibling.Key, tenantID, entry)
					if entryErr != nil {
						return entryErr
					}
					stored, entryErr = storeAction(tx, action)
					return entryErr
				})
				if err != nil {
					writeErrors++
					continue
				}
				episode.Containment = updatedContainment
				actionSink(stored)
			}
		}
	}
	return writeErrors
}

func systemSiblingByKey(key string) (systemSiblingAutopilot, bool) {
	for _, sibling := range systemSiblingAutopilots {
		if sibling.Key == key {
			return sibling, true
		}
	}
	return systemSiblingAutopilot{}, false
}

func systemActiveIncidentOwnsSiblingTenant(db *gorm.DB, excludedEpisodeID uint, siblingKey, tenantID string) bool {
	var episodes []models.SystemIncidentEpisode
	if err := db.Where("id <> ? AND status IN ?", excludedEpisodeID, []string{models.SystemIncidentStatusOpen, models.SystemIncidentStatusRecovering}).Find(&episodes).Error; err != nil {
		return true // fail closed: a failed ownership lookup must not clear a pause.
	}
	for _, episode := range episodes {
		ledger, legacy := readSystemContainmentLedger(episode.Containment)
		if legacy {
			continue
		}
		if entry, ok := containmentEntry(ledger, siblingKey, tenantID); ok && entry.WrittenUntil != "" && entry.Outcome != "resumed" {
			return true
		}
	}
	return false
}

func systemPauseCompareAndSetSQL(sibling systemSiblingAutopilot, existingCondition string) string {
	return fmt.Sprintf("UPDATE %s SET %s = ?, updated_at = ? WHERE tenant_id = ? AND %s %s RETURNING %s", sibling.Table, sibling.PauseColumn, sibling.PauseColumn, existingCondition, sibling.PauseColumn)
}

func systemResumeCompareAndSetSQL(sibling systemSiblingAutopilot) string {
	return fmt.Sprintf("UPDATE %s SET %s = NULL, updated_at = ? WHERE tenant_id = ? AND %s = ?", sibling.Table, sibling.PauseColumn, sibling.PauseColumn)
}

func systemHeadline(snapshot systemHealthSnapshot, confirmed []systemAnomaly, contained bool, resolved int) string {
	if contained {
		return models.SystemAutopilotHeadlineContained
	}
	if len(confirmed) > 0 {
		return models.SystemAutopilotHeadlineIncidentOpen
	}
	if resolved > 0 {
		return models.SystemAutopilotHeadlineRecovering
	}
	if snapshot.Overall == "healthy" {
		return models.SystemAutopilotHeadlineAllClear
	}
	return models.SystemAutopilotHeadlineWatching
}

func systemRunSummary(snapshot systemHealthSnapshot, confirmed []systemAnomaly, resolved int, contained bool) string {
	if contained {
		return fmt.Sprintf("Confirmed %d incident signal(s), applied bounded containment", len(confirmed))
	}
	if len(confirmed) > 0 {
		return fmt.Sprintf("Confirmed %d incident signal(s), opened or updated episodes", len(confirmed))
	}
	if resolved > 0 {
		return fmt.Sprintf("Resolved %d recovered episode(s)", resolved)
	}
	if snapshot.Overall == "healthy" {
		return "All configured platform probes are healthy"
	}
	return "Watching unconfirmed or degraded platform signals"
}

func latestSystemRun(db *gorm.DB) *models.SystemAutopilotRun {
	var run models.SystemAutopilotRun
	if err := db.Order("started_at DESC").First(&run).Error; err != nil {
		return nil
	}
	return &run
}

func runDetailWithActions(db *gorm.DB, run models.SystemAutopilotRun) gin.H {
	var actions []models.SystemAutopilotAction
	_ = db.Where("run_id = ?", run.ID).Order("started_at ASC").Find(&actions).Error
	return gin.H{"run": run, "actions": actions}
}

func GetSystemAutopilotStatus(c *gin.Context) {
	if _, ok := requireAdminPrincipal(c); !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	policy := loadSystemAutopilotPolicy(db)
	var episodes []models.SystemIncidentEpisode
	_ = db.Where("status IN ?", []string{models.SystemIncidentStatusOpen, models.SystemIncidentStatusRecovering}).
		Order("last_seen_at DESC").Limit(10).Find(&episodes).Error
	var recentEpisodes []models.SystemIncidentEpisode
	_ = db.Order("last_seen_at DESC").Limit(5).Find(&recentEpisodes).Error
	latest := latestSystemRun(db)
	registry := make([]gin.H, 0, len(systemSiblingAutopilots))
	disabled := containmentDisabledSet(policy)
	for _, sibling := range systemSiblingAutopilots {
		registry = append(registry, gin.H{
			"id":                  sibling.Key,
			"key":                 sibling.Key,
			"label":               sibling.Label,
			"dependencies":        sibling.Dependencies,
			"containment_enabled": !disabled[sibling.Key],
		})
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{
		"policy":                policy,
		"state":                 systemAutopilotState(policy),
		"latest_run":            latest,
		"open_episodes":         episodes,
		"recent_episodes":       recentEpisodes,
		"registered_autopilots": registry,
	}})
}

func systemAutopilotState(policy models.SystemAutopilotPolicy) string {
	if !policy.Enabled {
		return "off"
	}
	if policy.ContainmentPausedUntil != nil && policy.ContainmentPausedUntil.After(time.Now().UTC()) {
		return "paused"
	}
	return policy.Mode
}

func GetSystemAutopilotPolicy(c *gin.Context) {
	if _, ok := requireAdminPrincipal(c); !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	c.JSON(http.StatusOK, gin.H{"data": loadSystemAutopilotPolicy(db)})
}

func UpdateSystemAutopilotPolicy(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	policy := loadSystemAutopilotPolicy(db)
	var patch struct {
		Enabled                *bool     `json:"enabled"`
		Mode                   *string   `json:"mode"`
		IntervalMinutes        *int      `json:"interval_minutes"`
		ConfirmProbes          *int      `json:"confirm_probes"`
		ResolveProbes          *int      `json:"resolve_probes"`
		FlapCycles24h          *int      `json:"flap_cycles_24h"`
		ContainmentTTLMinutes  *int      `json:"containment_ttl_minutes"`
		ContainmentDisabledFor *[]string `json:"containment_disabled_for"`
	}
	if err := c.ShouldBindJSON(&patch); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if patch.Enabled != nil {
		policy.Enabled = *patch.Enabled
	}
	if patch.Mode != nil {
		policy.Mode = *patch.Mode
	}
	if patch.IntervalMinutes != nil {
		policy.IntervalMinutes = *patch.IntervalMinutes
	}
	if patch.ConfirmProbes != nil {
		policy.ConfirmProbes = *patch.ConfirmProbes
	}
	if patch.ResolveProbes != nil {
		policy.ResolveProbes = *patch.ResolveProbes
	}
	if patch.FlapCycles24h != nil {
		policy.FlapCycles24h = *patch.FlapCycles24h
	}
	if patch.ContainmentTTLMinutes != nil {
		policy.ContainmentTTLMinutes = *patch.ContainmentTTLMinutes
	}
	if patch.ContainmentDisabledFor != nil {
		policy.ContainmentDisabledFor = marshalAutopilotJSON(*patch.ContainmentDisabledFor)
	}
	policy = sanitizeSystemAutopilotPolicy(policy)
	if err := db.Save(&policy).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	_ = db.Create(&models.AuditLog{
		TenantID:       defaultCirculationTenant,
		UserID:         principal.UserID,
		UserEmail:      principal.Email,
		TargetService:  "system_health_autopilot",
		Action:         "update_policy",
		TargetResource: systemAutopilotScope,
		Status:         "success",
		Payload:        marshalAutopilotJSON(policy),
	}).Error
	c.JSON(http.StatusOK, gin.H{"data": policy})
}

func RunSystemAutopilotNow(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	run, actions, err := runSystemHealthAutopilot(db, systemAutopilotRunOptions{
		Trigger:   "manual",
		CreatedBy: principal.Email,
	})
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"run": run, "actions": actions}})
}

func PauseSystemAutopilotContainment(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var body struct {
		Minutes int `json:"minutes"`
	}
	_ = c.ShouldBindJSON(&body)
	policy := loadSystemAutopilotPolicy(db)
	var until *time.Time
	if body.Minutes > 0 {
		t := time.Now().UTC().Add(time.Duration(body.Minutes) * time.Minute)
		until = &t
	}
	policy.ContainmentPausedUntil = until
	if err := db.Save(&policy).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	_ = db.Create(&models.AuditLog{
		TenantID:       defaultCirculationTenant,
		UserID:         principal.UserID,
		UserEmail:      principal.Email,
		TargetService:  "system_health_autopilot",
		Action:         "pause_containment",
		TargetResource: systemAutopilotScope,
		Status:         "success",
		Payload:        marshalAutopilotJSON(gin.H{"until": until}),
	}).Error
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"containment_paused_until": until}})
}

type systemRecommendedAction struct {
	Label  string `json:"label"`
	Kind   string `json:"kind"`
	Target string `json:"target"`
	Href   string `json:"href"`
}

type systemIncidentEpisodeListItem struct {
	ID                uuid.UUID               `json:"id"`
	RootService       string                  `json:"root_service"`
	Verdict           string                  `json:"verdict"`
	Status            string                  `json:"status"`
	Severity          string                  `json:"severity"`
	Shadow            bool                    `json:"shadow"`
	Summary           string                  `json:"summary"`
	RootCauseHint     string                  `json:"root_cause_hint,omitempty"`
	FirstDetectedAt   time.Time               `json:"first_detected_at"`
	LastSeenAt        time.Time               `json:"last_seen_at"`
	RecoveringSince   *time.Time              `json:"recovering_since,omitempty"`
	ResolvedAt        *time.Time              `json:"resolved_at,omitempty"`
	RecommendedAction systemRecommendedAction `json:"recommended_action"`
}

func recommendedSystemAction(ep models.SystemIncidentEpisode) systemRecommendedAction {
	return systemRecommendedAction{
		Label:  "Inspect System Health incident",
		Kind:   "system_health.inspect",
		Target: ep.PublicID.String(),
		Href:   "/platform/system-health",
	}
}

func systemIncidentEpisodeListProjection(ep models.SystemIncidentEpisode) systemIncidentEpisodeListItem {
	return systemIncidentEpisodeListItem{
		ID: ep.PublicID, RootService: ep.RootService, Verdict: ep.Verdict, Status: ep.Status, Severity: ep.Severity,
		Shadow: ep.Shadow, Summary: ep.Summary, RootCauseHint: ep.RootCauseHint, FirstDetectedAt: ep.FirstDetectedAt,
		LastSeenAt: ep.LastSeenAt, RecoveringSince: ep.RecoveringSince, ResolvedAt: ep.ResolvedAt,
		RecommendedAction: recommendedSystemAction(ep),
	}
}

func ListSystemIncidentEpisodes(c *gin.Context) {
	if _, ok := requireAdminPrincipal(c); !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	limit := clampQueryInt(c, "limit", 50, 1, 200)
	var episodes []models.SystemIncidentEpisode
	_ = db.Order("last_seen_at DESC").Limit(limit).Find(&episodes).Error
	items := make([]systemIncidentEpisodeListItem, 0, len(episodes))
	for _, episode := range episodes {
		items = append(items, systemIncidentEpisodeListProjection(episode))
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"items": items}})
}

func GetSystemIncidentEpisode(c *gin.Context) {
	if _, ok := requireAdminPrincipal(c); !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid episode id"})
		return
	}
	var ep models.SystemIncidentEpisode
	if err := db.Where("public_id = ?", id).First(&ep).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "episode not found"})
		return
	}
	var actions []models.SystemAutopilotAction
	_ = db.Where("episode_id = ?", ep.ID).Order("started_at ASC").Find(&actions).Error
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"episode": ep, "actions": actions, "recommended_action": recommendedSystemAction(ep)}})
}

func CloseSystemIncidentEpisode(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid episode id"})
		return
	}
	var body struct {
		Reason string `json:"reason"`
	}
	_ = c.ShouldBindJSON(&body)
	body.Reason = strings.TrimSpace(body.Reason)
	if body.Reason == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "close reason is required"})
		return
	}
	var ep models.SystemIncidentEpisode
	if err := db.Where("public_id = ?", id).First(&ep).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "episode not found"})
		return
	}
	now := time.Now().UTC()
	ep.Status = models.SystemIncidentStatusClosedByHuman
	ep.ResolvedAt = &now
	ep.ClosedBy = principal.Email
	ep.CloseReason = body.Reason
	ep.Timeline = appendSystemEpisodeTimeline(ep.Timeline, "closed_by_human", now, systemAnomaly{
		Key:      systemIncidentKey(ep.RootService, ep.Verdict),
		Service:  ep.RootService,
		Verdict:  ep.Verdict,
		Severity: ep.Severity,
		Summary:  body.Reason,
	}, systemHealthSnapshot{Overall: "human_override"})
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Save(&ep).Error; err != nil {
			return err
		}
		return tx.Create(&models.SystemAutopilotAction{
			RunID:      0,
			EpisodeID:  &ep.ID,
			Target:     ep.RootService,
			Action:     models.SystemAutopilotActionCloseEpisode,
			Verdict:    ep.Verdict,
			Status:     "success",
			Reason:     body.Reason,
			StartedAt:  now,
			FinishedAt: &now,
		}).Error
	}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	_ = db.Create(&models.AuditLog{
		TenantID:       defaultCirculationTenant,
		UserID:         principal.UserID,
		UserEmail:      principal.Email,
		TargetService:  "system_health_autopilot",
		Action:         "close_episode",
		TargetResource: ep.PublicID.String(),
		Status:         "success",
		Payload:        marshalAutopilotJSON(gin.H{"reason": body.Reason, "status": ep.Status}),
	}).Error
	c.JSON(http.StatusOK, gin.H{"data": ep})
}

func ListSystemAutopilotRuns(c *gin.Context) {
	if _, ok := requireAdminPrincipal(c); !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	limit := clampQueryInt(c, "limit", 20, 1, 100)
	var runs []models.SystemAutopilotRun
	_ = db.Order("started_at DESC").Limit(limit).Find(&runs).Error
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"items": runs}})
}

func GetSystemAutopilotRun(c *gin.Context) {
	if _, ok := requireAdminPrincipal(c); !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid run id"})
		return
	}
	var run models.SystemAutopilotRun
	if err := db.Where("public_id = ?", id).First(&run).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "run not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": runDetailWithActions(db, run)})
}

func clampQueryInt(c *gin.Context, name string, fallback, minValue, maxValue int) int {
	raw := strings.TrimSpace(c.Query(name))
	if raw == "" {
		return fallback
	}
	var value int
	if _, err := fmt.Sscanf(raw, "%d", &value); err != nil {
		return fallback
	}
	return int(math.Max(float64(minValue), math.Min(float64(maxValue), float64(value))))
}

func StartSystemHealthAutopilotHeartbeat(db *gorm.DB) {
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			runSystemHealthAutopilotDue(db)
		}
	}()
}

func runSystemHealthAutopilotDue(db *gorm.DB) {
	policy := loadSystemAutopilotPolicy(db)
	if !policy.Enabled {
		return
	}
	now := time.Now().UTC()
	if policy.LastRunAt != nil && now.Sub(*policy.LastRunAt) < time.Duration(policy.IntervalMinutes)*time.Minute {
		return
	}
	if _, _, err := runSystemHealthAutopilot(db, systemAutopilotRunOptions{Trigger: "scheduled"}); err != nil {
		log.Printf("system health autopilot scheduled run failed: %v", err)
	}
}
