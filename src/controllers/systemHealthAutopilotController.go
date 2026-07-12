package controllers

import (
	"bytes"
	"content-management-system/src/models"
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
	systemQueueWaitingWarn     = 100
	systemAutopilotHistoryRuns = 12
)

var (
	systemAutopilotMu      sync.Mutex
	systemAutopilotRunning bool
)

type systemAutopilotRunOptions struct {
	Trigger   string
	CreatedBy string
}

type systemProbeDependency struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Detail string `json:"detail,omitempty"`
}

type systemProbeModel struct {
	Name   string `json:"name"`
	Loaded bool   `json:"loaded"`
	Detail string `json:"detail,omitempty"`
}

type systemProbeWorker struct {
	Configured bool `json:"configured"`
	Alive      bool `json:"alive"`
	Queued     int  `json:"queued"`
	Ongoing    int  `json:"ongoing"`
	Complete   int  `json:"complete"`
	Failed     int  `json:"failed"`
	Retried    int  `json:"retried"`
}

type systemProbeResult struct {
	Name        string                  `json:"name"`
	DisplayName string                  `json:"display_name"`
	EndpointURL string                  `json:"endpoint_url"`
	Status      string                  `json:"status"`
	LatencyMS   *int64                  `json:"latency_ms,omitempty"`
	HTTPStatus  *int                    `json:"http_status,omitempty"`
	Version     string                  `json:"version,omitempty"`
	Deps        []systemProbeDependency `json:"deps,omitempty"`
	Queues      []autopilotQueueStat    `json:"queues,omitempty"`
	Models      []systemProbeModel      `json:"models,omitempty"`
	Worker      *systemProbeWorker      `json:"worker,omitempty"`
	RawError    string                  `json:"raw_error,omitempty"`
	Verdicts    []string                `json:"verdicts,omitempty"`
}

type systemHealthSnapshot struct {
	Timestamp string              `json:"timestamp"`
	Overall   string              `json:"overall"`
	Services  []systemProbeResult `json:"services"`
	Issues    []systemHealthIssue `json:"issues"`
}

type systemHealthIssue struct {
	Severity string `json:"severity"`
	Service  string `json:"service,omitempty"`
	Message  string `json:"message"`
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
	Timestamp string          `json:"timestamp"`
	Overall   string          `json:"overall"`
	Anomalies []systemAnomaly `json:"anomalies"`
}

type systemSiblingAutopilot struct {
	Key             string
	Label           string
	Table           string
	PauseColumn     string
	Dependencies    []string
	DefaultDisabled bool
}

var systemSiblingAutopilots = []systemSiblingAutopilot{
	{Key: "pipeline", Label: "Pipeline Repair", Table: "pipeline_autopilot_policies", PauseColumn: "paused_until", Dependencies: []string{"aggregation"}},
	{Key: "enrichment", Label: "Enrichment Coverage", Table: "enrichment_autopilot_policies", PauseColumn: "paused_until", Dependencies: []string{"aggregation", "enrichment", "media"}},
	{Key: "embedding_lifecycle", Label: "Embedding Lifecycle", Table: "embedding_lifecycle_policies", PauseColumn: "campaigns_paused_until", Dependencies: []string{"cms", "enrichment", "media"}},
	{Key: "news_circulation", Label: "News Circulation", Table: "news_circulation_policies", PauseColumn: "autopilot_paused_until", Dependencies: []string{"aggregation"}, DefaultDisabled: true},
	{Key: "media_circulation", Label: "Media Circulation", Table: "media_circulation_policies", PauseColumn: "autopilot_paused_until", Dependencies: []string{"aggregation"}, DefaultDisabled: true},
	{Key: "media_studio", Label: "Media Studio", Table: "media_studio_autopilot_policies", PauseColumn: "paused_until", Dependencies: []string{"cms", "media", "enrichment"}, DefaultDisabled: true},
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
	if opts.Trigger == "" {
		opts.Trigger = "manual"
	}
	if !tryStartSystemAutopilotRun() {
		return models.SystemAutopilotRun{}, nil, fmt.Errorf("system health autopilot already running")
	}
	defer finishSystemAutopilotRun()

	now := time.Now().UTC()
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
	writeAction := func(a models.SystemAutopilotAction) {
		t := time.Now().UTC()
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
		if err := db.Create(&a).Error; err == nil {
			actions = append(actions, a)
		}
	}

	snapshot, anomalies := collectSystemHealthSnapshot(db)
	prev := recentSystemRunSnapshots(db, systemAutopilotHistoryRuns)
	confirmed := confirmSystemAnomalies(anomalies, prev, policy.ConfirmProbes)
	confirmed = applySystemFlapGuard(db, confirmed, policy.FlapCycles24h, writeAction)

	for _, anomaly := range anomalies {
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
			ep.LastSeenAt = now
			ep.Status = models.SystemIncidentStatusOpen
			ep.Shadow = policy.Mode != models.SystemAutopilotModeSafeAuto
			ep.Summary = anomaly.Summary
			ep.RootCauseHint = systemRootCauseHint(anomaly)
			ep.Evidence = marshalAutopilotJSON(anomaly.Evidence)
			ep.Timeline = appendSystemEpisodeTimeline(ep.Timeline, "updated", now, anomaly, snapshot)
			ep.RecoveringSince = nil
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
			writeAction(models.SystemAutopilotAction{
				EpisodeID: &ep.ID,
				Target:    anomaly.Service,
				Action:    models.SystemAutopilotActionUpdateEpisode,
				Verdict:   anomaly.Verdict,
				Reason:    anomaly.Summary,
				Output:    marshalAutopilotJSON(anomaly),
			})
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
			if handleSystemContainment(db, policy, anomaly, &ep, writeAction) {
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

	resolvedEpisodes, resolutionWriteErrors := resolveRecoveredSystemEpisodes(db, openEpisodes, snapshot, prev, policy.ResolveProbes, writeAction)
	episodeWriteErrors += resolutionWriteErrors
	if len(resolvedEpisodes) > 0 {
		resumeRecoveredSystemContainment(db, policy, resolvedEpisodes, writeAction)
	}

	runSnapshot := systemRunSnapshot{Timestamp: snapshot.Timestamp, Overall: snapshot.Overall, Anomalies: anomalies}
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
	finished := time.Now().UTC()
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

func collectSystemHealthSnapshot(db *gorm.DB) (systemHealthSnapshot, []systemAnomaly) {
	now := time.Now().UTC()
	services := []systemProbeResult{
		checkSystemCMS(db),
		checkSystemIAM(),
		checkSystemAggregation(),
		checkSystemEnrichment(),
		checkSystemMedia(),
		checkSystemPlatform(),
	}
	overall := "healthy"
	for _, svc := range services {
		if svc.Status == "unhealthy" {
			overall = "unhealthy"
			break
		}
		if svc.Status == "degraded" || svc.Status == "unknown" {
			overall = "degraded"
		}
	}
	issues := systemIssuesFromServices(services)
	anomalies := systemAnomaliesFromServices(services)
	if countHardDownServices(anomalies) > 1 {
		anomalies = append(anomalies, systemAnomaly{
			Key:      "platform:" + models.SystemVerdictMultiServiceIncident,
			Service:  "platform",
			Verdict:  models.SystemVerdictMultiServiceIncident,
			Severity: "critical",
			Summary:  "Multiple services are down in the same probe cycle",
			Evidence: map[string]interface{}{"hard_down_services": hardDownServiceNames(anomalies)},
		})
	}
	return systemHealthSnapshot{
		Timestamp: now.Format(time.RFC3339),
		Overall:   overall,
		Services:  services,
		Issues:    issues,
	}, anomalies
}

func checkSystemCMS(db *gorm.DB) systemProbeResult {
	start := time.Now()
	result := systemProbeResult{Name: "cms", DisplayName: "CMS", EndpointURL: "local", Status: "healthy"}
	sqlDB, err := db.DB()
	if err != nil {
		result.Status = "unhealthy"
		result.RawError = err.Error()
		result.Verdicts = []string{models.SystemVerdictServiceDown}
		return result
	}
	err = sqlDB.Ping()
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

func checkSystemIAM() systemProbeResult {
	display := systemProbeResult{Name: "iam", DisplayName: "IAM", EndpointURL: systemBaseURL("IAM_BASE_URL")}
	if display.EndpointURL == "" {
		return systemMissingProbe(display, "IAM_BASE_URL")
	}
	r := systemHTTPProbe(display.EndpointURL+"/health", false)
	body := asSystemRecord(r.Body)
	reported := systemString(body["status"])
	display.EndpointURL = display.EndpointURL + "/health"
	display.LatencyMS = r.LatencyMS
	display.HTTPStatus = r.HTTPStatus
	display.RawError = r.Error
	display.Version = systemString(body["version"])
	if r.OK && reported == "healthy" {
		display.Status = "healthy"
		display.Deps = []systemProbeDependency{{Name: "postgres", Status: "healthy"}}
		display.Verdicts = []string{models.SystemVerdictHealthy}
		return display
	}
	if r.OK {
		display.Status = "degraded"
		display.Deps = []systemProbeDependency{{Name: "postgres", Status: "unhealthy", Detail: reported}}
		display.Verdicts = []string{models.SystemVerdictDependencyDown}
		return display
	}
	display.Status = "unhealthy"
	display.Verdicts = []string{models.SystemVerdictServiceDown}
	return display
}

func checkSystemAggregation() systemProbeResult {
	display := systemProbeResult{Name: "aggregation", DisplayName: "Aggregation", EndpointURL: systemBaseURL("AGGREGATION_BASE_URL")}
	if display.EndpointURL == "" {
		return systemMissingProbe(display, "AGGREGATION_BASE_URL")
	}
	health := systemHTTPProbe(display.EndpointURL+"/health", false)
	ready := systemHTTPProbe(display.EndpointURL+"/ready", false)
	display.LatencyMS = firstLatency(health.LatencyMS, ready.LatencyMS)
	display.HTTPStatus = firstHTTPStatus(health.HTTPStatus, ready.HTTPStatus)
	display.RawError = firstNonEmpty(health.Error, ready.Error)
	readyBody := asSystemRecord(ready.Body)
	display.Deps = mapSystemDependencies(asSystemRecord(readyBody["dependencies"]))
	if stats, err := fetchAggregationQueueStats(); err == nil {
		display.Queues = stats
	} else if display.RawError == "" {
		display.RawError = err.Error()
	}
	reachable := health.OK || ready.OK
	switch {
	case !reachable:
		display.Status = "unhealthy"
		display.Verdicts = []string{models.SystemVerdictServiceDown}
	case hasUnhealthySystemDeps(display.Deps):
		display.Status = "degraded"
		display.Verdicts = []string{models.SystemVerdictDependencyDown}
	case hasBackloggedQueues(display.Queues):
		display.Status = "degraded"
		display.Verdicts = []string{models.SystemVerdictQueueBacklog}
	default:
		display.Status = "healthy"
		display.Verdicts = []string{models.SystemVerdictHealthy}
	}
	return display
}

func checkSystemEnrichment() systemProbeResult {
	return checkSystemMLService("enrichment", "Enrichment", "ENRICHMENT_BASE_URL", false)
}

func checkSystemMedia() systemProbeResult {
	return checkSystemMLService("media", "Media", "MEDIA_BASE_URL", true)
}

func checkSystemMLService(name, displayName, envKey string, includeWorker bool) systemProbeResult {
	base := systemBaseURL(envKey)
	display := systemProbeResult{Name: name, DisplayName: displayName, EndpointURL: base}
	if base == "" {
		return systemMissingProbe(display, envKey)
	}
	health := systemHTTPProbe(base+"/health", false)
	ready := systemHTTPProbe(base+"/ready", false)
	display.LatencyMS = firstLatency(health.LatencyMS, ready.LatencyMS)
	display.HTTPStatus = firstHTTPStatus(health.HTTPStatus, ready.HTTPStatus)
	display.RawError = firstNonEmpty(health.Error, ready.Error)
	healthBody := asSystemRecord(health.Body)
	readyBody := asSystemRecord(ready.Body)
	display.Version = systemString(healthBody["version"])
	display.Models = mapSystemModels(readyBody)
	display.Deps = mapSystemDependencies(asSystemRecord(readyBody["dependencies"]))
	if includeWorker {
		queue := systemHTTPProbe(base+"/health/queue", false)
		if worker := mapSystemWorker(asSystemRecord(queue.Body)); worker != nil {
			display.Worker = worker
			status := "unknown"
			switch {
			case !worker.Configured:
				status = "unknown"
			case worker.Alive:
				status = "healthy"
			case worker.Queued > 0:
				status = "unhealthy"
			}
			display.Deps = append(display.Deps, systemProbeDependency{
				Name:   "arq-worker",
				Status: status,
				Detail: fmt.Sprintf("%s · %d queued", map[bool]string{true: "alive", false: "down"}[worker.Alive], worker.Queued),
			})
		}
	}
	reachable := health.OK || ready.OK
	switch {
	case !reachable:
		display.Status = "unhealthy"
		display.Verdicts = []string{models.SystemVerdictServiceDown}
	case hasUnhealthySystemDeps(display.Deps):
		display.Status = "degraded"
		display.Verdicts = []string{models.SystemVerdictDependencyDown}
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

func checkSystemPlatform() systemProbeResult {
	base := systemBaseURL("PLATFORM_BASE_URL")
	display := systemProbeResult{Name: "platform", DisplayName: "Wahb-Platform", EndpointURL: base}
	if base == "" {
		return systemMissingProbe(display, "PLATFORM_BASE_URL")
	}
	r := systemHTTPProbe(base, true)
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
	OK         bool
	HTTPStatus *int
	LatencyMS  *int64
	Body       interface{}
	Error      string
}

func systemHTTPProbe(url string, allowText bool) systemHTTPProbeResult {
	start := time.Now()
	client := &http.Client{Timeout: systemProbeTimeout}
	req, err := http.NewRequest(http.MethodGet, url, nil)
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
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	if allowText {
		return result
	}
	var body interface{}
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &body); err != nil {
			result.Body = string(raw)
			return result
		}
	}
	result.Body = body
	return result
}

func systemBaseURL(key string) string {
	return strings.TrimRight(strings.TrimSpace(os.Getenv(key)), "/")
}

func systemMissingProbe(result systemProbeResult, envKey string) systemProbeResult {
	result.Status = "unknown"
	result.RawError = envKey + " not configured"
	result.Verdicts = []string{models.SystemVerdictTransientProbeFailure}
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
		status := "unhealthy"
		detail := ""
		switch v := value.(type) {
		case bool:
			if v {
				status = "healthy"
			}
		case string:
			detail = v
			if systemDependencyHealthyString(v) {
				status = "healthy"
			}
		default:
			detail = fmt.Sprintf("%v", value)
		}
		deps = append(deps, systemProbeDependency{Name: name, Status: status, Detail: detail})
	}
	sort.Slice(deps, func(i, j int) bool { return deps[i].Name < deps[j].Name })
	return deps
}

func systemDependencyHealthyString(value string) bool {
	v := strings.ToLower(value)
	return strings.Contains(v, "connected") || strings.Contains(v, "reachable") || strings.Contains(v, "configured") || strings.Contains(v, "ready") || v == "ok" || v == "true"
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
	return marshalAutopilotJSON(timeline)
}

func systemIncidentKey(service, verdict string) string {
	return service + ":" + verdict
}

func isSystemHardDownVerdict(verdict string) bool {
	return verdict == models.SystemVerdictServiceDown || verdict == models.SystemVerdictDependencyDown || verdict == models.SystemVerdictMultiServiceIncident
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

func applySystemFlapGuard(db *gorm.DB, current []systemAnomaly, maxFlaps int, writeAction func(models.SystemAutopilotAction)) []systemAnomaly {
	if maxFlaps <= 0 {
		return current
	}
	out := []systemAnomaly{}
	for _, anomaly := range current {
		var count int64
		since := time.Now().UTC().Add(-24 * time.Hour)
		_ = db.Model(&models.SystemIncidentEpisode{}).
			Where("root_service = ? AND verdict = ? AND created_at >= ?", anomaly.Service, anomaly.Verdict, since).
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

func resolveRecoveredSystemEpisodes(db *gorm.DB, openEpisodes []models.SystemIncidentEpisode, snapshot systemHealthSnapshot, prev []systemRunSnapshot, resolveProbes int, writeAction func(models.SystemAutopilotAction)) ([]models.SystemIncidentEpisode, int) {
	if resolveProbes < 1 {
		resolveProbes = 1
	}
	resolved := []models.SystemIncidentEpisode{}
	writeErrors := 0
	healthy := map[string]bool{}
	for _, svc := range snapshot.Services {
		healthy[svc.Name] = svc.Status == "healthy"
	}
	for _, ep := range openEpisodes {
		if !healthy[ep.RootService] {
			continue
		}
		ok := 1
		for _, run := range prev {
			if !runHasSystemAnomaly(run, systemIncidentKey(ep.RootService, ep.Verdict)) {
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
				now := time.Now().UTC()
				ep.Status = models.SystemIncidentStatusRecovering
				ep.RecoveringSince = &now
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
		now := time.Now().UTC()
		ep.Status = models.SystemIncidentStatusResolved
		ep.ResolvedAt = &now
		ep.Timeline = appendSystemEpisodeTimeline(ep.Timeline, "resolved", now, systemAnomaly{
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

func containmentDisabledSet(policy models.SystemAutopilotPolicy) map[string]bool {
	disabled := map[string]bool{}
	var values []string
	if err := json.Unmarshal(policy.ContainmentDisabledFor, &values); err != nil {
		values = []string{"news_circulation", "media_circulation", "media_studio"}
	}
	for _, value := range values {
		disabled[strings.TrimSpace(value)] = true
	}
	for _, sibling := range systemSiblingAutopilots {
		if sibling.DefaultDisabled {
			if _, exists := disabled[sibling.Key]; !exists {
				disabled[sibling.Key] = true
			}
		}
	}
	return disabled
}

func handleSystemContainment(db *gorm.DB, policy models.SystemAutopilotPolicy, anomaly systemAnomaly, ep *models.SystemIncidentEpisode, writeAction func(models.SystemAutopilotAction)) bool {
	disabled := containmentDisabledSet(policy)
	now := time.Now().UTC()
	containmentPaused := policy.ContainmentPausedUntil != nil && policy.ContainmentPausedUntil.After(now)
	desiredUntil := now.Add(time.Duration(policy.ContainmentTTLMinutes) * time.Minute)
	containment := map[string]string{}
	applied := false
	for _, sibling := range systemSiblingAutopilots {
		if !siblingDependsOnService(sibling, anomaly.Service) && anomaly.Verdict != models.SystemVerdictMultiServiceIncident {
			continue
		}
		action := models.SystemAutopilotAction{
			EpisodeID: &ep.ID,
			Target:    sibling.Key,
			Verdict:   anomaly.Verdict,
			Output:    marshalAutopilotJSON(gin.H{"paused_until": desiredUntil.Format(time.RFC3339), "incident": anomaly.Key}),
		}
		if disabled[sibling.Key] {
			action.Action = models.SystemAutopilotActionSkipped
			action.Status = "skipped"
			action.Guardrail = models.SystemAutopilotGuardOptedOut
			action.Reason = sibling.Label + " is registered but opted out of System Health containment"
			writeAction(action)
			continue
		}
		if containmentPaused {
			action.Action = models.SystemAutopilotActionWouldPause
			action.Status = "would_execute"
			action.Guardrail = models.SystemAutopilotGuardPaused
			action.Reason = "Containment is paused by a human; would pause " + sibling.Label
			writeAction(action)
			continue
		}
		if policy.Mode != models.SystemAutopilotModeSafeAuto {
			action.Action = models.SystemAutopilotActionWouldPause
			action.Status = "would_execute"
			action.Guardrail = models.SystemAutopilotGuardObserveMode
			action.Reason = "Observe mode would pause " + sibling.Label
			writeAction(action)
			continue
		}
		if err := pauseSiblingAutopilot(db, sibling, desiredUntil); err != nil {
			action.Action = models.SystemAutopilotActionSkipped
			action.Status = "skipped"
			action.Guardrail = models.SystemAutopilotGuardHumanPause
			action.Reason = err.Error()
			writeAction(action)
			continue
		}
		action.Action = models.SystemAutopilotActionPauseSibling
		action.Status = "success"
		action.Reason = "Paused " + sibling.Label + " until dependency recovers"
		writeAction(action)
		containment[sibling.Key] = desiredUntil.Format(time.RFC3339)
		applied = true
	}
	if len(containment) > 0 {
		ep.Containment = marshalAutopilotJSON(containment)
		_ = db.Save(ep).Error
	}
	return applied
}

func siblingDependsOnService(sibling systemSiblingAutopilot, service string) bool {
	for _, dep := range sibling.Dependencies {
		if dep == service {
			return true
		}
	}
	return false
}

func pauseSiblingAutopilot(db *gorm.DB, sibling systemSiblingAutopilot, until time.Time) error {
	ensureSystemSiblingPolicyRow(db, sibling)
	var rows []struct {
		TenantID    string
		PausedUntil *time.Time
	}
	query := fmt.Sprintf("SELECT tenant_id, %s AS paused_until FROM %s", sibling.PauseColumn, sibling.Table)
	if err := db.Raw(query).Scan(&rows).Error; err != nil {
		return err
	}
	if len(rows) == 0 {
		return fmt.Errorf("%s has no policy rows to pause", sibling.Label)
	}
	for _, row := range rows {
		if row.PausedUntil != nil && row.PausedUntil.After(until) {
			return fmt.Errorf("%s already has a longer human pause", sibling.Label)
		}
	}
	update := fmt.Sprintf("UPDATE %s SET %s = ?, updated_at = ? WHERE tenant_id <> ''", sibling.Table, sibling.PauseColumn)
	return db.Exec(update, until, time.Now().UTC()).Error
}

func ensureSystemSiblingPolicyRow(db *gorm.DB, sibling systemSiblingAutopilot) {
	switch sibling.Key {
	case "pipeline":
		policy := models.DefaultPipelineAutopilotPolicy(defaultCirculationTenant)
		_ = db.Where("tenant_id = ?", defaultCirculationTenant).FirstOrCreate(&policy).Error
	case "enrichment":
		policy := models.DefaultEnrichmentAutopilotPolicy(defaultCirculationTenant)
		_ = db.Where("tenant_id = ?", defaultCirculationTenant).FirstOrCreate(&policy).Error
	case "embedding_lifecycle":
		_, _ = getOrCreateEmbeddingPolicy(db)
	case "news_circulation":
		policy := models.DefaultNewsCirculationPolicy(defaultCirculationTenant)
		_ = db.Where("tenant_id = ?", defaultCirculationTenant).FirstOrCreate(&policy).Error
	case "media_circulation":
		policy := models.DefaultMediaCirculationPolicy(defaultCirculationTenant)
		_ = db.Where("tenant_id = ?", defaultCirculationTenant).FirstOrCreate(&policy).Error
	case "media_studio":
		policy := models.DefaultMediaStudioAutopilotPolicy(defaultCirculationTenant)
		_ = db.Where("tenant_id = ?", defaultCirculationTenant).FirstOrCreate(&policy).Error
	}
}

func resumeRecoveredSystemContainment(db *gorm.DB, policy models.SystemAutopilotPolicy, episodes []models.SystemIncidentEpisode, writeAction func(models.SystemAutopilotAction)) {
	if policy.Mode != models.SystemAutopilotModeSafeAuto {
		return
	}
	disabled := containmentDisabledSet(policy)
	seen := map[string]bool{}
	for _, episode := range episodes {
		var containment map[string]string
		if err := json.Unmarshal(episode.Containment, &containment); err != nil {
			continue
		}
		for key, rawUntil := range containment {
			if disabled[key] || seen[key] {
				continue
			}
			sibling, ok := systemSiblingByKey(key)
			if !ok {
				continue
			}
			until, err := time.Parse(time.RFC3339, rawUntil)
			if err != nil {
				continue
			}
			seen[key] = true
			if resumeSiblingAutopilot(db, sibling, until) {
				writeAction(models.SystemAutopilotAction{
					Target: sibling.Key,
					Action: models.SystemAutopilotActionResumeSibling,
					Status: "success",
					Reason: "Cleared resolved System Health containment pause",
					Output: marshalAutopilotJSON(gin.H{
						"episode_id":   episode.PublicID.String(),
						"paused_until": until.Format(time.RFC3339),
					}),
				})
			}
		}
	}
}

func systemSiblingByKey(key string) (systemSiblingAutopilot, bool) {
	for _, sibling := range systemSiblingAutopilots {
		if sibling.Key == key {
			return sibling, true
		}
	}
	return systemSiblingAutopilot{}, false
}

func resumeSiblingAutopilot(db *gorm.DB, sibling systemSiblingAutopilot, until time.Time) bool {
	query := fmt.Sprintf(
		"UPDATE %s SET %s = NULL, updated_at = ? WHERE %s IS NOT NULL AND %s <= ?",
		sibling.Table,
		sibling.PauseColumn,
		sibling.PauseColumn,
		sibling.PauseColumn,
	)
	result := db.Exec(query, time.Now().UTC(), until.Add(5*time.Second))
	return result.Error == nil && result.RowsAffected > 0
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

func ListSystemIncidentEpisodes(c *gin.Context) {
	if _, ok := requireAdminPrincipal(c); !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	limit := clampQueryInt(c, "limit", 50, 1, 200)
	var episodes []models.SystemIncidentEpisode
	_ = db.Order("last_seen_at DESC").Limit(limit).Find(&episodes).Error
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"items": episodes}})
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
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"episode": ep, "actions": actions}})
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
	if err := db.Save(&ep).Error; err != nil {
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
