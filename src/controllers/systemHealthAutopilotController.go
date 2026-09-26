package controllers

import (
	"bytes"
	"content-management-system/src/models"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
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
	"gorm.io/gorm/clause"
)

const (
	systemAutopilotScope           = "platform"
	systemProbeTimeout             = 3 * time.Second
	systemProbePhaseTimeout        = 10 * time.Second
	systemProbeBodyLimit           = 256 * 1024
	systemQueueProbeBodyLimit      = 1024 * 1024
	systemQueueWaitingWarn         = 100
	systemAutopilotHistoryRuns     = 12
	systemAutopilotAdvisoryKey     = 7_070_000_001
	systemAutopilotTargetCap       = 100
	systemAutopilotMutationTimeout = 30 * time.Second
)

var (
	systemAutopilotMu       sync.Mutex
	systemAutopilotRunning  bool
	errSystemAutopilotBusy  = errors.New("system health autopilot already running")
	errSystemIncidentClosed = errors.New("system incident episode is already closed")
)

type systemAutopilotRunOptions struct {
	Trigger       string
	CreatedBy     string
	CorrelationID *uuid.UUID
	TriggerRef    string
	// ObservationOnly is used by manual diagnostics and downstream recovery
	// checks. They may record evidence and episodes, but cannot mutate sibling
	// policy rows even when the persisted policy is Safe Auto.
	ObservationOnly bool
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
	TooClose  bool                `json:"-"`
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
	Capabilities []string
}

var systemSiblingAutopilots = []systemSiblingAutopilot{
	{Key: "pipeline", Label: "Pipeline Repair", Table: "pipeline_autopilot_policies", PauseColumn: "paused_until", Dependencies: []string{"aggregation"}, Capabilities: []string{"aggregation_pipeline"}},
	{Key: "enrichment", Label: "Enrichment Coverage", Table: "enrichment_autopilot_policies", PauseColumn: "paused_until", Dependencies: []string{"aggregation", "enrichment", "media"}, Capabilities: []string{"aggregation_pipeline", "enrichment", "media"}},
	{Key: "embedding_lifecycle", Label: "Embedding Lifecycle", Table: "embedding_lifecycle_policies", PauseColumn: "campaigns_paused_until", Dependencies: []string{"cms", "enrichment", "media"}, Capabilities: []string{"enrichment", "media"}},
	{Key: "news_circulation", Label: "News Circulation", Table: "news_circulation_policies", PauseColumn: "autopilot_paused_until", Dependencies: []string{"aggregation"}, Capabilities: []string{"aggregation_dispatcher", "news_processing"}},
	{Key: "media_circulation", Label: "Media Circulation", Table: "media_circulation_policies", PauseColumn: "autopilot_paused_until", Dependencies: []string{"aggregation"}, Capabilities: []string{"aggregation_dispatcher", "media"}},
	{Key: "media_studio", Label: "Media Studio", Table: "media_studio_autopilot_policies", PauseColumn: "paused_until", Dependencies: []string{"cms", "media", "enrichment"}, Capabilities: []string{"media", "enrichment"}},
	{Key: "redundancy", Label: "Redundancy Hygiene", Table: "redundancy_policies", PauseColumn: "paused_until", Dependencies: []string{"cms", "aggregation"}, Capabilities: []string{"aggregation_receipt"}},
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

func loadSystemAutopilotPolicyWithError(db *gorm.DB) (models.SystemAutopilotPolicy, error) {
	var policy models.SystemAutopilotPolicy
	if err := db.Where("scope = ?", systemAutopilotScope).First(&policy).Error; err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return policy, err
		}
		policy = models.DefaultSystemAutopilotPolicy()
		if err := db.Where("scope = ?", systemAutopilotScope).FirstOrCreate(&policy).Error; err != nil {
			return policy, err
		}
	}
	return sanitizeSystemAutopilotPolicy(policy), nil
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
	allowContainment := opts.Trigger == "scheduled" && policy.Enabled && policy.Mode == models.SystemAutopilotModeSafeAuto && !opts.ObservationOnly
	run := models.SystemAutopilotRun{
		Trigger:       opts.Trigger,
		Mode:          policy.Mode,
		Status:        models.SystemAutopilotRunStatusRunning,
		Headline:      models.SystemAutopilotHeadlineWatching,
		StartedAt:     now,
		CreatedBy:     opts.CreatedBy,
		ErrorClass:    models.SystemAutopilotErrorClassNone,
		CorrelationID: opts.CorrelationID,
		TriggerRef:    opts.TriggerRef,
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
	prev := recentSystemRunSnapshots(db, systemAutopilotHistoryRuns, now, policy.IntervalMinutes)
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
	for _, anomaly := range observed {
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
	mutationContext, cancelMutations := context.WithTimeout(context.Background(), systemAutopilotMutationTimeout)
	defer cancelMutations()
	mutationDB := db.WithContext(mutationContext)
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
			if err := saveSystemEpisodeObservation(db, &ep); err != nil {
				if errors.Is(err, errSystemIncidentClosed) {
					continue
				}
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
			applied, containmentWriteErrors := handleSystemContainment(mutationDB, policy, anomaly, &ep, now, storeAction, func(action models.SystemAutopilotAction) {
				actions = append(actions, action)
			}, allowContainment)
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
	if allowContainment {
		allContainmentEpisodes := allSystemIncidentEpisodesWithContainment(mutationDB)
		episodeWriteErrors += resumeRecoveredSystemContainment(mutationDB, policy, allContainmentEpisodes, now, systemRecoveryEvidence{Snapshot: snapshot, Previous: prev}, storeAction, func(action models.SystemAutopilotAction) {
			actions = append(actions, action)
		}, true)
	} else if opts.Trigger != "scheduled" {
		// Manual and recovery callers still expose what would be released; they
		// never acquire a mutation path through the diagnostic endpoint.
		allContainmentEpisodes := allSystemIncidentEpisodesWithContainment(mutationDB)
		episodeWriteErrors += resumeRecoveredSystemContainment(mutationDB, policy, allContainmentEpisodes, now, systemRecoveryEvidence{Snapshot: snapshot, Previous: prev}, storeAction, func(action models.SystemAutopilotAction) {
			actions = append(actions, action)
		}, false)
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
	run.Headline = systemHeadlineForState(db, snapshot, handledConfirmed, contained, len(resolvedEpisodes))
	run.Summary = systemRunSummaryForState(db, snapshot, handledConfirmed, len(resolvedEpisodes), contained, opts)
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
	go func() {
		defer wg.Done()
		ready = systemHTTPProbeWithToken(ctx, display.EndpointURL+"/internal/readiness/topology", false, aggregationInternalServiceToken())
	}()
	go func() { defer wg.Done(); queues, queueErr = fetchSystemAggregationQueueStats(ctx) }()
	wg.Wait()
	display.LatencyMS = firstLatency(health.LatencyMS, ready.LatencyMS)
	display.HTTPStatus = firstHTTPStatus(health.HTTPStatus, ready.HTTPStatus)
	display.RawError = firstNonEmpty(health.Error, ready.Error)
	readyBody := asSystemRecord(ready.Body)
	healthObserved := health.JSONObserved && systemHealthStatusObserved(asSystemRecord(health.Body), "healthy")
	display.ReadinessObserved = ready.JSONObserved && readinessAggregationBodyObserved(readyBody)
	// A response that fails the topology contract is observation-unknown. Do
	// not turn its partial role payload into a durable dependency-down verdict.
	if display.ReadinessObserved {
		display.Deps = mapAggregationTopologyDependencies(readyBody)
	}
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
	return systemHTTPProbeWithToken(parent, url, allowText, "")
}

func systemHTTPProbeWithToken(parent context.Context, url string, allowText bool, token string) systemHTTPProbeResult {
	start := time.Now()
	ctx, cancel := context.WithTimeout(parent, systemProbeTimeout)
	defer cancel()
	client := &http.Client{}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		latency := time.Since(start).Milliseconds()
		return systemHTTPProbeResult{LatencyMS: &latency, Error: err.Error()}
	}
	if strings.TrimSpace(token) != "" {
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(token))
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
	capturedAt, capturedErr := time.Parse(time.RFC3339, systemString(body["captured_at"]))
	_, hasRoles := body["roles"]
	_, hasCapabilities := body["capabilities"]
	digest := systemString(body["topology_digest"])
	return systemString(body["schema_version"]) == "aggregation-topology-readiness/v1" &&
		(status == "healthy" || status == "degraded") && hasRoles && hasCapabilities &&
		validSystemTopologyDigest(digest) && capturedErr == nil && time.Since(capturedAt.UTC()) <= 45*time.Second &&
		capturedAt.UTC().Before(time.Now().UTC().Add(5*time.Second))
}

func validSystemTopologyDigest(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func mapAggregationTopologyDependencies(body map[string]interface{}) []systemProbeDependency {
	deps := []systemProbeDependency{}
	for role, raw := range asSystemRecord(body["roles"]) {
		state := asSystemRecord(raw)
		status := "unhealthy"
		ready, _ := state["ready"].(bool)
		required, _ := state["required"].(bool)
		if ready {
			status = "healthy"
		} else if !required {
			status = "unknown"
		}
		reasons := []string{}
		if values, ok := state["reasons"].([]interface{}); ok {
			for _, value := range values {
				if reason := systemString(value); reason != "" {
					reasons = append(reasons, reason)
				}
			}
		}
		detail := strings.Join(reasons, "; ")
		if !required && !ready {
			detail = strings.TrimSpace("optional role is not currently required; " + detail)
		}
		deps = append(deps, systemProbeDependency{Name: "role:" + role, Status: status, Detail: detail})
	}
	for capability, raw := range asSystemRecord(body["capabilities"]) {
		state := asSystemRecord(raw)
		status := "unhealthy"
		if ready, _ := state["ready"].(bool); ready {
			status = "healthy"
		}
		deps = append(deps, systemProbeDependency{Name: "capability:" + capability, Status: status})
	}
	sort.Slice(deps, func(i, j int) bool { return deps[i].Name < deps[j].Name })
	return deps
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

func recentSystemRunSnapshots(db *gorm.DB, limit int, now time.Time, intervalMinutes int) []systemRunSnapshot {
	var runs []models.SystemAutopilotRun
	if err := db.Where("started_at < ?", now).
		Order("started_at DESC").Limit(limit * 16).Find(&runs).Error; err != nil {
		return nil
	}
	if intervalMinutes < 2 {
		intervalMinutes = 10
	}
	maxGap := time.Duration(intervalMinutes*2+1) * time.Minute
	out := []systemRunSnapshot{}
	lastAcceptedAt, lastObservedAt := now, now
	acceptedCount := 0
	for _, run := range runs {
		var wrapper struct {
			RunSnapshot systemRunSnapshot `json:"run_snapshot"`
		}
		if run.Status != models.SystemAutopilotRunStatusCompleted || json.Unmarshal(run.ProbeResults, &wrapper) != nil || wrapper.RunSnapshot.Timestamp == "" {
			out = append(out, systemRunSnapshot{}) // Failed or interrupted evidence breaks the streak.
			break
		}
		{
			observedAt, parseErr := time.Parse(time.RFC3339, wrapper.RunSnapshot.Timestamp)
			if parseErr != nil {
				observedAt, parseErr = time.Parse(time.RFC3339Nano, wrapper.RunSnapshot.Timestamp)
			}
			if parseErr != nil || observedAt.After(lastObservedAt) || lastObservedAt.Sub(observedAt) > maxGap {
				out = append(out, systemRunSnapshot{})
				break
			}
			// A burst of manual clicks or duplicate scheduler delivery must not
			// manufacture a confirmation/recovery streak. Count at most one
			// observation per configured scheduler interval.
			lastObservedAt = observedAt
			wrapper.RunSnapshot.TooClose = lastAcceptedAt.Sub(observedAt) < time.Duration(intervalMinutes)*time.Minute
			out = append(out, wrapper.RunSnapshot)
			if !wrapper.RunSnapshot.TooClose {
				lastAcceptedAt = observedAt
				acceptedCount++
				if acceptedCount >= limit {
					break
				}
			}
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
				if run.TooClose {
					continue
				}
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
	// Historical runs persist raw symptoms. Rebuild the deterministic
	// correlation group when a later run asks about its shared root.
	for _, anomaly := range correlateSystemAnomalies(run.Anomalies) {
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
		if run.TooClose {
			continue
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
		ok := systemRecoverySamples(ep, snapshot, prev, resolveProbes)
		if ok < resolveProbes {
			if ep.Status != models.SystemIncidentStatusRecovering {
				recoveringAt := now.UTC()
				ep.Status = models.SystemIncidentStatusRecovering
				ep.RecoveringSince = &recoveringAt
				if err := saveSystemEpisodeObservation(db, &ep); err != nil {
					if errors.Is(err, errSystemIncidentClosed) {
						continue
					}
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
		if err := saveSystemEpisodeObservation(db, &ep); err != nil {
			if errors.Is(err, errSystemIncidentClosed) {
				continue
			}
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
	if !systemEpisodeCoverageComplete(ep, snapshot.Services) {
		return false
	}
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

func systemEpisodeCoverageComplete(ep models.SystemIncidentEpisode, services []systemProbeResult) bool {
	seen := map[string]bool{}
	for _, svc := range services {
		if svc.Name != "" {
			seen[svc.Name] = true
		}
	}
	required := []string{ep.RootService}
	switch ep.RootService {
	case "redis":
		required = []string{"aggregation", "media"}
	case "postgres":
		required = []string{"cms", "iam"}
	case "cms":
		required = []string{"aggregation", "enrichment", "media"}
	}
	for _, name := range required {
		if !seen[name] {
			return false
		}
	}
	return true
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
	HumanOwned   bool   `json:"human_owned,omitempty"`
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

func handleSystemContainment(db *gorm.DB, policy models.SystemAutopilotPolicy, anomaly systemAnomaly, ep *models.SystemIncidentEpisode, now time.Time, storeAction systemAutopilotActionStore, actionSink func(models.SystemAutopilotAction), containmentOverride ...bool) (bool, int) {
	disabled := containmentDisabledSet(policy)
	now = now.UTC()
	containmentPaused := policy.ContainmentPausedUntil != nil && policy.ContainmentPausedUntil.After(now)
	allowContainment := true
	if len(containmentOverride) > 0 {
		allowContainment = containmentOverride[0]
	}
	desiredUntil := now.Add(time.Duration(policy.ContainmentTTLMinutes) * time.Minute)
	applied, writeErrors := false, 0
	write := func(action models.SystemAutopilotAction) {
		if stored, err := storeAction(db, action); err != nil {
			writeErrors++
		} else {
			actionSink(stored)
		}
	}
	// Bound mutation fanout before the first sibling write. The cap is a
	// deterministic safety boundary; an oversized registry is surfaced as an
	// attention action rather than silently pausing only the first N rows.
	preloadedRows := map[string][]systemSiblingPolicyRow{}
	preloadErrors := map[string]error{}
	preloaded := allowContainment && policy.Enabled && policy.Mode == models.SystemAutopilotModeSafeAuto && !containmentPaused
	preloadedTargetCount := 0
	if preloaded {
		for _, sibling := range systemSiblingAutopilots {
			if !siblingDependsOnAnomaly(sibling, anomaly) || disabled[sibling.Key] {
				continue
			}
			rows, err := systemSiblingPolicyRows(db, sibling)
			if err != nil {
				preloadErrors[sibling.Key] = err
				continue
			}
			preloadedRows[sibling.Key] = rows
			preloadedTargetCount += len(rows)
		}
		if preloadedTargetCount > systemAutopilotTargetCap {
			for _, sibling := range systemSiblingAutopilots {
				if _, ok := preloadedRows[sibling.Key]; !ok {
					continue
				}
				write(models.SystemAutopilotAction{
					EpisodeID: &ep.ID,
					Target:    sibling.Key,
					Action:    models.SystemAutopilotActionSkipped,
					Status:    "attention",
					Verdict:   anomaly.Verdict,
					Guardrail: models.SystemAutopilotGuardScopeCap,
					Reason:    fmt.Sprintf("Containment target fanout (%d) exceeds the per-run cap of %d; no sibling pause was written", preloadedTargetCount, systemAutopilotTargetCap),
				})
			}
			return false, writeErrors
		}
	}
	for _, sibling := range systemSiblingAutopilots {
		if !siblingDependsOnAnomaly(sibling, anomaly) {
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
		if !allowContainment {
			base.Action, base.Status, base.Guardrail = models.SystemAutopilotActionWouldPause, "would_execute", models.SystemAutopilotGuardManualObservation
			base.Reason = "This run records evidence only; containment is reserved for an enabled scheduled Safe Auto run"
			if !policy.Enabled {
				base.Guardrail = models.SystemAutopilotGuardDisabled
				base.Reason = "System Health Autopilot is disabled; this observation cannot change sibling policy"
			}
			write(base)
			continue
		}
		if policy.Mode != models.SystemAutopilotModeSafeAuto {
			base.Action, base.Status, base.Guardrail = models.SystemAutopilotActionWouldPause, "would_execute", models.SystemAutopilotGuardObserveMode
			base.Reason = "Observe mode would pause " + sibling.Label
			write(base)
			continue
		}
		rows, err := preloadedRows[sibling.Key], preloadErrors[sibling.Key]
		if preloaded && rows == nil && err == nil {
			rows, err = systemSiblingPolicyRows(db, sibling)
		}
		if !preloaded {
			rows, err = systemSiblingPolicyRows(db, sibling)
		}
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
				entry := owned
				if !ownsPause {
					entry.Outcome = "skipped"
				}
				// Re-read the platform policy under the same transaction as the
				// sibling CAS. A mode/disable/pause change made after the probe
				// cannot be bypassed by a stale run snapshot.
				currentPolicy := policy
				var persistedPolicy models.SystemAutopilotPolicy
				if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("scope = ?", systemAutopilotScope).First(&persistedPolicy).Error; err != nil {
					if !errors.Is(err, gorm.ErrRecordNotFound) {
						return err
					}
					// The public runner always creates the policy before entering
					// containment. Keep the helper usable in isolated fixtures by
					// treating a missing row as the caller's already-sanitized policy.
					currentPolicy = sanitizeSystemAutopilotPolicy(policy)
				} else {
					currentPolicy = sanitizeSystemAutopilotPolicy(persistedPolicy)
				}
				var currentEpisode models.SystemIncidentEpisode
				if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&currentEpisode, ep.ID).Error; err != nil {
					return err
				}
				if currentEpisode.Status != models.SystemIncidentStatusOpen && currentEpisode.Status != models.SystemIncidentStatusRecovering {
					return errSystemIncidentClosed
				}
				if !currentPolicy.Enabled {
					action.Action, action.Status, action.Guardrail = models.SystemAutopilotActionSkipped, "skipped", models.SystemAutopilotGuardDisabled
					action.Reason, entry.Reason = "System Health Autopilot was disabled before the containment write", "autopilot_disabled"
				} else if currentPolicy.Mode != models.SystemAutopilotModeSafeAuto {
					action.Action, action.Status, action.Guardrail = models.SystemAutopilotActionSkipped, "skipped", models.SystemAutopilotGuardObserveMode
					action.Reason, entry.Reason = "System Health policy changed to Observe before the containment write", "observe_mode"
				} else if containmentDisabledSet(currentPolicy)[sibling.Key] {
					action.Action, action.Status, action.Guardrail = models.SystemAutopilotActionSkipped, "skipped", models.SystemAutopilotGuardOptedOut
					action.Reason, entry.Reason = "Sibling containment was opted out before the containment write", "opted_out"
				} else if currentPolicy.ContainmentPausedUntil != nil && currentPolicy.ContainmentPausedUntil.After(now) {
					action.Action, action.Status, action.Guardrail = models.SystemAutopilotActionSkipped, "skipped", models.SystemAutopilotGuardPaused
					action.Reason, entry.Reason = "Containment was paused before the containment write", "paused"
				} else if ownsPause && owned.HumanOwned {
					action.Action, action.Status, action.Guardrail = models.SystemAutopilotActionSkipped, "skipped", models.SystemAutopilotGuardHumanPause
					action.Reason, entry.Reason, entry.HumanOwned = "A human takeover is recorded for this tenant; System Health will not reacquire it", "human_pause", true
				} else if row.PausedUntil != nil && (!ownsPause || owned.WrittenUntil == "") {
					action.Action, action.Status, action.Guardrail = models.SystemAutopilotActionSkipped, "skipped", models.SystemAutopilotGuardHumanPause
					action.Reason, entry.Reason = "A human or another incident already owns this tenant pause", "human_pause"
				} else if ownsPause && owned.WrittenUntil != "" && row.PausedUntil == nil {
					owned.HumanOwned = true
					action.Action, action.Status, action.Guardrail = models.SystemAutopilotActionSkipped, "skipped", models.SystemAutopilotGuardHumanPause
					action.Reason, entry = "The System Health pause was cleared or replaced before renewal", owned
					entry.Outcome, entry.Reason, entry.HumanOwned = "skipped", "human_pause", true
				} else if ownsPause && owned.WrittenUntil != "" && row.PausedUntil != nil {
					expected, parseErr := time.Parse(time.RFC3339Nano, owned.WrittenUntil)
					if parseErr != nil || !row.PausedUntil.UTC().Equal(expected.UTC()) {
						action.Action, action.Status, action.Guardrail = models.SystemAutopilotActionSkipped, "skipped", models.SystemAutopilotGuardHumanPause
						action.Reason, entry = "The sibling pause no longer matches the exact value System Health owns", owned
						entry.Outcome, entry.Reason, entry.HumanOwned = "skipped", "human_pause", true
					} else if !row.PausedUntil.Before(desiredUntil) {
						action.Action, action.Status, action.Guardrail = models.SystemAutopilotActionSkipped, "skipped", models.SystemAutopilotGuardContainmentTTL
						action.Reason, entry = "Existing System Health pause already covers the requested containment TTL", owned
					} else {
						where := "= ?"
						args := []interface{}{desiredUntil, now, row.TenantID, expected}
						query := systemPauseCompareAndSetSQL(sibling, where)
						var written time.Time
						result := tx.Raw(query, args...).Scan(&written)
						if result.Error != nil {
							return result.Error
						}
						if result.RowsAffected == 0 {
							action.Action, action.Status, action.Guardrail = models.SystemAutopilotActionSkipped, "skipped", models.SystemAutopilotGuardHumanPause
							action.Reason, entry.Reason, entry.HumanOwned = "Sibling pause changed before containment compare-and-set", "human_pause", true
						} else {
							mutated = true
							entry = systemContainmentLedgerEntry{WrittenUntil: written.UTC().Format(time.RFC3339Nano), Outcome: "paused"}
							action.Action, action.Status, action.Reason = models.SystemAutopilotActionPauseSibling, "success", "Extended "+sibling.Label+" until dependency recovers"
						}
					}
				} else if row.PausedUntil != nil {
					action.Action, action.Status, action.Guardrail = models.SystemAutopilotActionSkipped, "skipped", models.SystemAutopilotGuardContainmentTTL
					action.Reason, entry.Reason = "A foreign or human pause already exists on this tenant", "human_pause"
				} else {
					where := "IS NULL"
					args := []interface{}{desiredUntil, now, row.TenantID}
					query := systemPauseCompareAndSetSQL(sibling, where)
					var written time.Time
					result := tx.Raw(query, args...).Scan(&written)
					if result.Error != nil {
						return result.Error
					}
					if result.RowsAffected == 0 {
						action.Action, action.Status, action.Guardrail = models.SystemAutopilotActionSkipped, "skipped", models.SystemAutopilotGuardHumanPause
						action.Reason, entry.Reason, entry.HumanOwned = "Sibling pause changed before containment compare-and-set", "human_pause", true
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

func siblingDependsOnAnomaly(sibling systemSiblingAutopilot, anomaly systemAnomaly) bool {
	if anomaly.Verdict == models.SystemVerdictMultiServiceIncident {
		return true
	}
	if anomaly.Service != "aggregation" {
		return siblingDependsOnService(sibling, anomaly.Service)
	}
	// Aggregation's topology response contains capability-level evidence. A
	// sibling is scoped to the capability it consumes when that evidence is
	// available; a plain service-down response remains a service-wide signal.
	if unhealthyCapabilities := systemUnhealthyCapabilitiesFromEvidence(anomaly.Evidence["service"]); len(unhealthyCapabilities) > 0 {
		for _, capability := range sibling.Capabilities {
			if unhealthyCapabilities[capability] {
				return true
			}
		}
		return false
	}
	return siblingDependsOnService(sibling, anomaly.Service)
}

// systemUnhealthyCapabilitiesFromEvidence accepts both the in-memory probe
// value and its JSON-decoded representation. Episodes and run snapshots are
// persisted as JSON, so a type assertion against systemProbeResult alone would
// silently widen a capability-scoped signal back to all Aggregation siblings
// after the first process restart.
func systemUnhealthyCapabilitiesFromEvidence(value interface{}) map[string]bool {
	capabilities := map[string]bool{}
	add := func(name, status string) {
		if strings.HasPrefix(name, "capability:") && status == "unhealthy" {
			capabilities[strings.TrimPrefix(name, "capability:")] = true
		}
	}
	switch probe := value.(type) {
	case systemProbeResult:
		for _, dependency := range probe.Deps {
			add(dependency.Name, dependency.Status)
		}
	case *systemProbeResult:
		if probe != nil {
			for _, dependency := range probe.Deps {
				add(dependency.Name, dependency.Status)
			}
		}
	case map[string]interface{}:
		for _, raw := range []interface{}{probe["deps"], probe["dependencies"]} {
			items, ok := raw.([]interface{})
			if !ok {
				continue
			}
			for _, item := range items {
				record := asSystemRecord(item)
				add(systemString(record["name"]), systemString(record["status"]))
			}
		}
	}
	return capabilities
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

func allSystemIncidentEpisodesWithContainment(db *gorm.DB) []models.SystemIncidentEpisode {
	episodes, _ := allSystemIncidentEpisodesWithContainmentErr(db)
	return episodes
}

func allSystemIncidentEpisodesWithContainmentErr(db *gorm.DB) ([]models.SystemIncidentEpisode, error) {
	var episodes []models.SystemIncidentEpisode
	// Keep the query deliberately broad: resolved and human-closed episodes can
	// still own a pause that needs a later release or expiry reconciliation.
	err := db.Where("containment IS NOT NULL").Order("updated_at ASC").Find(&episodes).Error
	return episodes, err
}

func resumeRecoveredSystemContainment(db *gorm.DB, policy models.SystemAutopilotPolicy, episodes []models.SystemIncidentEpisode, now time.Time, evidence systemRecoveryEvidence, storeAction systemAutopilotActionStore, actionSink func(models.SystemAutopilotAction), containmentOverride ...bool) int {
	if policy.Mode != models.SystemAutopilotModeSafeAuto {
		return 0
	}
	allowContainment := true
	if len(containmentOverride) > 0 {
		allowContainment = containmentOverride[0]
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
				if ownership.WrittenUntil == "" || ownership.Outcome == "resumed" || ownership.Outcome == "expired" {
					continue
				}
				until, err := time.Parse(time.RFC3339Nano, ownership.WrittenUntil)
				if err != nil {
					writeErrors++
					continue
				}
				action := models.SystemAutopilotAction{EpisodeID: &episode.ID, Target: sibling.Key, Output: marshalAutopilotJSON(gin.H{"tenant_id": tenantID, "episode_id": episode.PublicID.String(), "paused_until": ownership.WrittenUntil})}

				persistOutcome := func(entry systemContainmentLedgerEntry) {
					var stored models.SystemAutopilotAction
					var updated datatypes.JSON
					err := db.Transaction(func(tx *gorm.DB) error {
						var entryErr error
						updated, entryErr = storeSystemContainmentEntry(tx, &episode, ledger, sibling.Key, tenantID, entry)
						if entryErr != nil {
							return entryErr
						}
						stored, entryErr = storeAction(tx, action)
						return entryErr
					})
					if err != nil {
						writeErrors++
						return
					}
					episode.Containment = updated
					ledger, _ = readSystemContainmentLedger(updated)
					actionSink(stored)
				}

				if ownership.HumanOwned {
					action.Action, action.Status, action.Guardrail = models.SystemAutopilotActionSkipped, "skipped", models.SystemAutopilotGuardHumanPause
					action.Reason = "A human takeover owns this sibling pause; System Health will not release it"
					persistOutcome(ownership)
					continue
				}
				if now.After(until) {
					ownership.Outcome, ownership.Reason = "expired", "System Health pause expired naturally before a verified release"
					action.Action, action.Status, action.Guardrail = models.SystemAutopilotActionSkipped, "skipped", models.SystemAutopilotGuardContainmentTTL
					action.Reason = ownership.Reason
					persistOutcome(ownership)
					continue
				}
				// Accounting for a lease is distinct from permission to release it.
				// Human close and an active/recovering episode never prove recovery.
				if episode.Status != models.SystemIncidentStatusResolved {
					continue
				}
				if !systemReleaseEvidenceHealthy(episode, evidence, policy, now) {
					action.Action, action.Status, action.Guardrail = models.SystemAutopilotActionWouldResume, "attention", models.SystemAutopilotGuardEvidenceStale
					action.Reason = "Release needs a fresh, complete recovery window; the owned pause is retained"
					if stored, err := storeAction(db, action); err != nil {
						writeErrors++
					} else {
						actionSink(stored)
					}
					continue
				}
				if containmentDisabledSet(policy)[sibling.Key] {
					action.Action, action.Status, action.Guardrail = models.SystemAutopilotActionWouldResume, "would_execute", models.SystemAutopilotGuardOptedOut
					action.Reason = "Sibling containment is opted out; release remains pending until an explicit cleanup decision"
					if stored, storeErr := storeAction(db, action); storeErr != nil {
						writeErrors++
					} else {
						actionSink(stored)
					}
					continue
				}
				if !allowContainment {
					action.Action, action.Status, action.Guardrail = models.SystemAutopilotActionWouldResume, "would_execute", models.SystemAutopilotGuardManualObservation
					action.Reason = "This run records evidence only; release remains pending"
					if !policy.Enabled {
						action.Guardrail = models.SystemAutopilotGuardDisabled
						action.Reason = "System Health Autopilot is disabled; release remains pending"
					}
					persistActionOnly := func() {
						if stored, storeErr := storeAction(db, action); storeErr != nil {
							writeErrors++
						} else {
							actionSink(stored)
						}
					}
					persistActionOnly()
					continue
				}
				if containmentPaused {
					action.Action, action.Status, action.Guardrail = models.SystemAutopilotActionWouldResume, "would_execute", models.SystemAutopilotGuardPaused
					action.Reason = "Containment is paused by a human; release remains pending"
					if stored, storeErr := storeAction(db, action); storeErr != nil {
						writeErrors++
					} else {
						actionSink(stored)
					}
					continue
				}
				if systemActiveIncidentBlocksSibling(db, episode.ID, sibling, tenantID) {
					action.Action, action.Status, action.Guardrail = models.SystemAutopilotActionSkipped, "skipped", models.SystemAutopilotGuardContainmentTTL
					action.Reason = "Another active incident still blocks this sibling tenant"
					if stored, storeErr := storeAction(db, action); storeErr != nil {
						writeErrors++
					} else {
						actionSink(stored)
					}
					continue
				}
				var stored models.SystemAutopilotAction
				var updated datatypes.JSON
				err = db.Transaction(func(tx *gorm.DB) error {
					var current models.SystemAutopilotPolicy
					if policyErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("scope = ?", systemAutopilotScope).First(&current).Error; policyErr != nil {
						if !errors.Is(policyErr, gorm.ErrRecordNotFound) {
							return policyErr
						}
						current = sanitizeSystemAutopilotPolicy(policy)
					} else {
						current = sanitizeSystemAutopilotPolicy(current)
					}
					if !current.Enabled {
						action.Action, action.Status, action.Guardrail = models.SystemAutopilotActionWouldResume, "would_execute", models.SystemAutopilotGuardDisabled
						action.Reason = "System Health Autopilot was disabled before the release"
						var storeErr error
						stored, storeErr = storeAction(tx, action)
						if storeErr != nil {
							return storeErr
						}
						updated = episode.Containment
						return nil
					}
					if current.Mode != models.SystemAutopilotModeSafeAuto {
						action.Action, action.Status, action.Guardrail = models.SystemAutopilotActionWouldResume, "would_execute", models.SystemAutopilotGuardObserveMode
						action.Reason = "System Health policy is in Observe mode; release remains pending"
						var storeErr error
						stored, storeErr = storeAction(tx, action)
						if storeErr != nil {
							return storeErr
						}
						updated = episode.Containment
						return nil
					}
					if containmentDisabledSet(current)[sibling.Key] {
						action.Action, action.Status, action.Guardrail = models.SystemAutopilotActionWouldResume, "would_execute", models.SystemAutopilotGuardOptedOut
						action.Reason = "Sibling containment was opted out before the release; cleanup remains pending"
						var storeErr error
						stored, storeErr = storeAction(tx, action)
						if storeErr != nil {
							return storeErr
						}
						updated = episode.Containment
						return nil
					}
					if current.ContainmentPausedUntil != nil && current.ContainmentPausedUntil.After(now) {
						action.Action, action.Status, action.Guardrail = models.SystemAutopilotActionWouldResume, "would_execute", models.SystemAutopilotGuardPaused
						action.Reason = "Containment is paused by a human; release remains pending"
						var storeErr error
						stored, storeErr = storeAction(tx, action)
						if storeErr != nil {
							return storeErr
						}
						updated = episode.Containment
						return nil
					}
					var currentEpisode models.SystemIncidentEpisode
					if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&currentEpisode, episode.ID).Error; err != nil {
						return err
					}
					if currentEpisode.Status != models.SystemIncidentStatusResolved || !systemReleaseEvidenceHealthy(currentEpisode, evidence, current, now) {
						return errSystemIncidentClosed
					}
					result := tx.Exec(systemResumeCompareAndSetSQL(sibling), now, tenantID, until)
					entry := ownership
					if result.Error != nil {
						return result.Error
					}
					if result.RowsAffected == 0 {
						entry.HumanOwned = true
						entry.Outcome, entry.Reason = "skipped", "human_pause"
						action.Action, action.Status, action.Guardrail = models.SystemAutopilotActionSkipped, "skipped", models.SystemAutopilotGuardHumanPause
						action.Reason = "Sibling pause no longer exactly matches System Health ownership"
					} else {
						entry.Outcome, entry.Reason = "resumed", ""
						action.Action, action.Status, action.Reason = models.SystemAutopilotActionResumeSibling, "success", "Cleared resolved System Health containment pause"
					}
					var entryErr error
					updated, entryErr = storeSystemContainmentEntry(tx, &episode, ledger, sibling.Key, tenantID, entry)
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
				episode.Containment = updated
				ledger, _ = readSystemContainmentLedger(updated)
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

func systemActiveIncidentBlocksSibling(db *gorm.DB, excludedEpisodeID uint, sibling systemSiblingAutopilot, tenantID string) bool {
	var episodes []models.SystemIncidentEpisode
	if err := db.Where("id <> ? AND status IN ?", excludedEpisodeID, []string{models.SystemIncidentStatusOpen, models.SystemIncidentStatusRecovering}).Find(&episodes).Error; err != nil {
		return true
	}
	for _, episode := range episodes {
		if !isSystemHardDownVerdict(episode.Verdict) || !systemEpisodeAffectsSibling(episode, sibling) {
			continue
		}
		return true // An active dependency failure blocks release regardless of who owns its pause.
	}
	return false
}

// systemEpisodeAffectsSibling preserves capability-level Aggregation scope
// while evaluating blockers from persisted episodes. The episode evidence is
// JSON-decoded, so this deliberately handles both direct probe evidence and
// correlated member evidence. If an older episode has no usable scope, fail
// closed to its service-level dependency graph.
func systemEpisodeAffectsSibling(episode models.SystemIncidentEpisode, sibling systemSiblingAutopilot) bool {
	if episode.RootService != "aggregation" {
		return siblingDependsOnService(sibling, episode.RootService)
	}
	var evidence map[string]interface{}
	if err := json.Unmarshal(episode.Evidence, &evidence); err != nil {
		return siblingDependsOnService(sibling, episode.RootService)
	}
	if capabilities := systemUnhealthyCapabilitiesFromEvidence(evidence["service"]); len(capabilities) > 0 {
		for _, capability := range sibling.Capabilities {
			if capabilities[capability] {
				return true
			}
		}
		return false
	}
	if members, ok := evidence["members"].([]interface{}); ok {
		foundCapabilityEvidence := false
		for _, member := range members {
			memberRecord := asSystemRecord(member)
			if systemString(memberRecord["service"]) != "aggregation" {
				continue
			}
			foundCapabilityEvidence = true
			if capabilities := systemUnhealthyCapabilitiesFromEvidence(memberRecord["evidence"]); len(capabilities) > 0 {
				for _, capability := range sibling.Capabilities {
					if capabilities[capability] {
						return true
					}
				}
			}
		}
		if foundCapabilityEvidence {
			return false
		}
	}
	return siblingDependsOnService(sibling, episode.RootService)
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

func systemHasOutstandingContainment(db *gorm.DB) bool {
	outstanding, err := systemHasOutstandingContainmentErr(db)
	return err == nil && outstanding
}

func systemHasOutstandingContainmentErr(db *gorm.DB) (bool, error) {
	projection, err := systemContainmentStatusWithError(db)
	return projection.Active+projection.Pending+projection.HumanOwned > 0, err
}

func systemHeadlineForState(db *gorm.DB, snapshot systemHealthSnapshot, confirmed []systemAnomaly, contained bool, resolved int) string {
	var active int64
	_ = db.Model(&models.SystemIncidentEpisode{}).Where("status IN ?", []string{models.SystemIncidentStatusOpen, models.SystemIncidentStatusRecovering}).Count(&active).Error
	outstanding, containmentErr := systemHasOutstandingContainmentErr(db)
	if active > 0 {
		if contained || containmentErr != nil || outstanding {
			return models.SystemAutopilotHeadlineContained
		}
		return models.SystemAutopilotHeadlineIncidentOpen
	}
	if containmentErr != nil || outstanding {
		return models.SystemAutopilotHeadlineRecovering
	}
	if resolved > 0 {
		return models.SystemAutopilotHeadlineRecovering
	}
	if snapshot.Overall == "healthy" && len(confirmed) == 0 {
		return models.SystemAutopilotHeadlineAllClear
	}
	return models.SystemAutopilotHeadlineWatching
}

func systemRunSummaryForState(db *gorm.DB, snapshot systemHealthSnapshot, confirmed []systemAnomaly, resolved int, contained bool, opts systemAutopilotRunOptions) string {
	if opts.Trigger != "scheduled" || opts.ObservationOnly {
		if len(confirmed) > 0 {
			return "Diagnostic observation recorded; no sibling automation was changed"
		}
		return "Diagnostic observation recorded; no containment effects were requested"
	}
	if contained {
		return fmt.Sprintf("Confirmed %d incident signal(s), applied bounded containment", len(confirmed))
	}
	if len(confirmed) > 0 {
		return fmt.Sprintf("Confirmed %d incident signal(s), opened or updated episodes", len(confirmed))
	}
	if resolved > 0 {
		return fmt.Sprintf("Resolved %d recovered episode(s); containment reconciliation is recorded", resolved)
	}
	if outstanding, err := systemHasOutstandingContainmentErr(db); err == nil && snapshot.Overall == "healthy" && !outstanding {
		return "All configured platform probes are healthy and containment is reconciled"
	}
	return "Watching unconfirmed, degraded, or incomplete platform signals"
}

func latestSystemRun(db *gorm.DB) *models.SystemAutopilotRun {
	var run models.SystemAutopilotRun
	if err := db.Order("started_at DESC").First(&run).Error; err != nil {
		return nil
	}
	return &run
}

func latestSystemRunWithError(db *gorm.DB) (*models.SystemAutopilotRun, error) {
	var run models.SystemAutopilotRun
	if err := db.Order("started_at DESC").First(&run).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &run, nil
}

func runDetailWithActions(db *gorm.DB, run models.SystemAutopilotRun) gin.H {
	var actions []models.SystemAutopilotAction
	_ = db.Where("run_id = ?", run.ID).Order("started_at ASC").Find(&actions).Error
	return gin.H{"run": run, "actions": actions}
}

type systemMonitorProjection struct {
	State           string     `json:"state"`
	Fresh           bool       `json:"fresh"`
	LastObservedAt  *time.Time `json:"last_observed_at,omitempty"`
	LastCompletedAt *time.Time `json:"last_completed_at,omitempty"`
	NextDueAt       *time.Time `json:"next_due_at,omitempty"`
	EvidenceAgeSecs *int64     `json:"evidence_age_seconds,omitempty"`
	Reason          string     `json:"reason,omitempty"`
}

type systemContainmentTargetProjection struct {
	EpisodeID  string `json:"episode_id"`
	Sibling    string `json:"sibling"`
	TenantID   string `json:"tenant_id"`
	Outcome    string `json:"outcome"`
	Until      string `json:"until,omitempty"`
	HumanOwned bool   `json:"human_owned,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

type systemContainmentProjection struct {
	Active     int                                 `json:"active"`
	Pending    int                                 `json:"pending"`
	HumanOwned int                                 `json:"human_owned"`
	Expired    int                                 `json:"expired"`
	Targets    []systemContainmentTargetProjection `json:"targets"`
}

type systemAttentionProjection struct {
	EpisodeID string `json:"episode_id,omitempty"`
	TenantID  string `json:"tenant_id,omitempty"`
	Target    string `json:"target"`
	Guardrail string `json:"guardrail,omitempty"`
	Status    string `json:"status"`
	Reason    string `json:"reason,omitempty"`
	At        string `json:"at"`
}

func systemRunObservationAt(run *models.SystemAutopilotRun) *time.Time {
	if run == nil {
		return nil
	}
	var wrapper struct {
		RunSnapshot systemRunSnapshot `json:"run_snapshot"`
	}
	if err := json.Unmarshal(run.ProbeResults, &wrapper); err != nil || wrapper.RunSnapshot.Timestamp == "" {
		return nil
	}
	observedAt, err := time.Parse(time.RFC3339Nano, wrapper.RunSnapshot.Timestamp)
	if err != nil {
		return nil
	}
	return &observedAt
}

func systemMonitorStatus(policy models.SystemAutopilotPolicy, latest *models.SystemAutopilotRun, now time.Time) systemMonitorProjection {
	disabled := !policy.Enabled
	projection := systemMonitorProjection{State: "never_observed", Reason: "No System Health probe has completed"}
	if disabled {
		projection.State = "disabled"
		projection.Reason = "Scheduled System Health observation is disabled"
	}
	if latest == nil {
		return projection
	}
	observedAt := systemRunObservationAt(latest)
	projection.LastCompletedAt = latest.FinishedAt
	projection.LastObservedAt = observedAt
	if latest.FinishedAt != nil && !disabled {
		next := latest.FinishedAt.Add(time.Duration(policy.IntervalMinutes) * time.Minute)
		projection.NextDueAt = &next
	}
	// Keep the control state distinct from evidence freshness. A previously
	// successful probe remains useful context after an administrator disables
	// the schedule, but it must not make the monitor look actively fresh.
	if disabled {
		return projection
	}
	if observedAt != nil {
		age := now.Sub(observedAt.UTC()).Seconds()
		if age < 0 {
			age = 0
		}
		ageValue := int64(age)
		projection.EvidenceAgeSecs = &ageValue
	}
	if latest.Status == models.SystemAutopilotRunStatusRunning {
		projection.State = "running"
		projection.Reason = "A System Health probe is still running"
		if now.Sub(latest.StartedAt) > time.Duration(policy.IntervalMinutes*2+1)*time.Minute {
			projection.State = "overdue"
			projection.Reason = "The last System Health run has exceeded its observation window"
		}
		return projection
	}
	if latest.Status == models.SystemAutopilotRunStatusFailed || latest.Status == models.SystemAutopilotRunStatusPartial {
		projection.State = "unavailable"
		projection.Reason = firstNonEmpty(latest.Error, "The last System Health run did not complete cleanly")
		return projection
	}
	if observedAt == nil {
		projection.State = "unavailable"
		projection.Reason = "The last run has no valid observation timestamp"
		return projection
	}
	maxAge := time.Duration(policy.IntervalMinutes*2+1) * time.Minute
	if now.Sub(observedAt.UTC()) > maxAge {
		projection.State = "overdue"
		projection.Reason = "The last System Health evidence is older than the allowed observation window"
		return projection
	}
	projection.State = "fresh"
	projection.Fresh = true
	projection.Reason = "Fresh System Health evidence is available"
	return projection
}

func systemContainmentStatus(db *gorm.DB) systemContainmentProjection {
	projection, _ := systemContainmentStatusWithError(db)
	return projection
}

func systemContainmentStatusWithError(db *gorm.DB) (systemContainmentProjection, error) {
	episodes, err := allSystemIncidentEpisodesWithContainmentErr(db)
	if err != nil {
		return systemContainmentProjection{}, err
	}
	return projectSystemContainment(db, episodes, time.Now().UTC())
}

func systemAttentionStatus(db *gorm.DB) ([]systemAttentionProjection, error) {
	return projectSystemAttention(db, time.Now().UTC())
}

func GetSystemAutopilotStatus(c *gin.Context) {
	if _, ok := requireAdminPrincipal(c); !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	policy, policyErr := loadSystemAutopilotPolicyWithError(db)
	if policyErr != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "System Health policy is unavailable", "code": "POLICY_READ_FAILED"})
		return
	}
	var episodes []models.SystemIncidentEpisode
	if err := db.Where("status IN ?", []string{models.SystemIncidentStatusOpen, models.SystemIncidentStatusRecovering}).
		Order("last_seen_at DESC").Limit(10).Find(&episodes).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "System Health incident state is unavailable", "code": "STATUS_READ_FAILED"})
		return
	}
	var recentEpisodes []models.SystemIncidentEpisode
	if err := db.Order("last_seen_at DESC").Limit(5).Find(&recentEpisodes).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "System Health incident history is unavailable", "code": "HISTORY_READ_FAILED"})
		return
	}
	latest, latestErr := latestSystemRunWithError(db)
	if latestErr != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "System Health run history is unavailable", "code": "RUN_READ_FAILED"})
		return
	}
	containment, containmentErr := systemContainmentStatusWithError(db)
	if containmentErr != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "System Health containment state is unavailable", "code": "CONTAINMENT_READ_FAILED"})
		return
	}
	attention, attentionErr := systemAttentionStatus(db)
	if attentionErr != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "System Health action history is unavailable", "code": "ACTION_READ_FAILED"})
		return
	}
	registry := make([]gin.H, 0, len(systemSiblingAutopilots))
	disabled := containmentDisabledSet(policy)
	for _, sibling := range systemSiblingAutopilots {
		registry = append(registry, gin.H{
			"id":                  sibling.Key,
			"key":                 sibling.Key,
			"label":               sibling.Label,
			"dependencies":        sibling.Dependencies,
			"capabilities":        sibling.Capabilities,
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
		"monitor":               systemMonitorStatus(policy, latest, time.Now().UTC()),
		"containment":           containment,
		"attention":             attention,
		"status_version":        "system-health-autopilot/v2",
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
		Trigger:         "manual",
		CreatedBy:       principal.Email,
		ObservationOnly: true,
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
	action := systemRecommendedAction{
		Label:  "Inspect System Health incident",
		Kind:   "system_health.inspect",
		Target: ep.PublicID.String(),
		Href:   "/platform/system-health",
	}
	switch ep.RootService {
	case "aggregation":
		action.Label = "Inspect Pipeline operations"
		action.Kind = "pipeline.inspect"
		action.Href = "/platform/pipeline"
	case "enrichment":
		action.Label = "Inspect Enrichment operations"
		action.Kind = "enrichment.inspect"
		action.Href = "/platform/enrichment"
	case "media":
		action.Label = "Inspect Media operations"
		action.Kind = "media.inspect"
		action.Href = "/platform/media"
	case "iam", "postgres", "redis", "cms", "platform":
		action.Href = "/platform/system-health?tab=configuration"
	}
	return action
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
	if err := db.Where("episode_id = ?", ep.ID).Order("started_at ASC").Find(&actions).Error; err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Incident actions are unavailable"})
		return
	}
	policy, err := loadSystemAutopilotPolicyWithError(db)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Incident policy is unavailable"})
		return
	}
	latest, err := latestSystemRunWithError(db)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Recovery evidence is unavailable"})
		return
	}
	now := time.Now().UTC()
	previous := []systemRunSnapshot{}
	if latest != nil {
		previous = recentSystemRunSnapshots(db, systemAutopilotHistoryRuns, latest.StartedAt, policy.IntervalMinutes)
	}
	containment, err := projectSystemContainment(db, []models.SystemIncidentEpisode{ep}, now)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Containment policy is unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"episode": ep, "actions": actions, "recommended_action": recommendedSystemAction(ep), "containment": containment, "recovery": systemEpisodeRecoveryStatus(ep, policy, latest, previous, now)}})
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
	if err := db.Transaction(func(tx *gorm.DB) error {
		var current models.SystemIncidentEpisode
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("public_id = ?", id).First(&current).Error; err != nil {
			return err
		}
		if current.Status != models.SystemIncidentStatusOpen && current.Status != models.SystemIncidentStatusRecovering {
			return errSystemIncidentClosed
		}
		current.Status = models.SystemIncidentStatusClosedByHuman
		current.ResolvedAt = &now
		current.ClosedBy = principal.Email
		current.CloseReason = body.Reason
		current.Timeline = appendSystemEpisodeTimeline(current.Timeline, "closed_by_human", now, systemAnomaly{
			Key:      systemIncidentKey(current.RootService, current.Verdict),
			Service:  current.RootService,
			Verdict:  current.Verdict,
			Severity: current.Severity,
			Summary:  body.Reason,
		}, systemHealthSnapshot{Overall: "human_override"})
		var actionRun models.SystemAutopilotRun
		if err := tx.Order("started_at DESC").First(&actionRun).Error; errors.Is(err, gorm.ErrRecordNotFound) {
			finished := now
			actionRun = models.SystemAutopilotRun{
				Trigger: "human_override", Mode: models.SystemAutopilotModeObserve,
				Status: models.SystemAutopilotRunStatusCompleted, Headline: models.SystemAutopilotHeadlineWatching,
				StartedAt: now, FinishedAt: &finished, Summary: "Human incident action recorded",
				ErrorClass: models.SystemAutopilotErrorClassNone, CreatedBy: principal.Email,
			}
			if err := tx.Create(&actionRun).Error; err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		if err := tx.Save(&current).Error; err != nil {
			return err
		}
		if err := tx.Create(&models.SystemAutopilotAction{
			RunID:      actionRun.ID,
			EpisodeID:  &current.ID,
			Target:     current.RootService,
			Action:     models.SystemAutopilotActionCloseEpisode,
			Verdict:    current.Verdict,
			Status:     "success",
			Reason:     body.Reason,
			StartedAt:  now,
			FinishedAt: &now,
		}).Error; err != nil {
			return err
		}
		ep = current
		return nil
	}); err != nil {
		if errors.Is(err, errSystemIncidentClosed) {
			c.JSON(http.StatusConflict, gin.H{"error": err.Error(), "code": "INCIDENT_ALREADY_CLOSED"})
			return
		}
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
		// A heartbeat can overlap a long-running manual or prior scheduled run.
		// This is an expected coalescing outcome, not a failed health run. Keep
		// real probe/persistence errors visible at error level.
		if errors.Is(err, errSystemAutopilotBusy) {
			log.Printf("system health autopilot scheduled run deferred: already running")
			return
		}
		log.Printf("system health autopilot scheduled run failed: %v", err)
	}
}
