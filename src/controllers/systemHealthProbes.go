package controllers

import (
	"context"
	"time"

	"gorm.io/gorm"
)

// These types and the coordinator are intentionally kept outside the System
// Health controller. The controller owns scheduling, persistence, episodes,
// and containment; this probe engine owns bounded observation only.
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
	Name              string                  `json:"name"`
	DisplayName       string                  `json:"display_name"`
	EndpointURL       string                  `json:"endpoint_url"`
	Status            string                  `json:"status"`
	ReadinessObserved bool                    `json:"readiness_observed"`
	LatencyMS         *int64                  `json:"latency_ms,omitempty"`
	HTTPStatus        *int                    `json:"http_status,omitempty"`
	Version           string                  `json:"version,omitempty"`
	Deps              []systemProbeDependency `json:"deps,omitempty"`
	Queues            []autopilotQueueStat    `json:"queues,omitempty"`
	Models            []systemProbeModel      `json:"models,omitempty"`
	Worker            *systemProbeWorker      `json:"worker,omitempty"`
	RawError          string                  `json:"raw_error,omitempty"`
	Verdicts          []string                `json:"verdicts,omitempty"`
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

func collectSystemHealthSnapshot(db *gorm.DB) (systemHealthSnapshot, []systemAnomaly) {
	now := time.Now().UTC()
	phase, cancel := context.WithTimeout(context.Background(), systemProbePhaseTimeout)
	defer cancel()
	services := collectSystemProbeResults(phase, []systemProbeCheck{
		{name: "cms", check: func(ctx context.Context) systemProbeResult { return checkSystemCMS(ctx, db) }},
		{name: "iam", check: checkSystemIAM},
		{name: "aggregation", check: checkSystemAggregation},
		{name: "enrichment", check: checkSystemEnrichment},
		{name: "media", check: checkSystemMedia},
		{name: "platform", check: checkSystemPlatform},
	})
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
	return systemHealthSnapshot{
		Timestamp: now.Format(time.RFC3339),
		Overall:   overall,
		Services:  services,
		Issues:    issues,
	}, anomalies
}

type systemProbeCheck struct {
	name  string
	check func(context.Context) systemProbeResult
}

// collectSystemProbeResults preserves the caller-provided order while letting
// every service use the same bounded phase. A timed-out probe stays unknown;
// it cannot block observations of its siblings.
func collectSystemProbeResults(ctx context.Context, checks []systemProbeCheck) []systemProbeResult {
	services := make([]systemProbeResult, len(checks))
	type result struct {
		index int
		probe systemProbeResult
	}
	results := make(chan result, len(checks))
	for index, check := range checks {
		go func(index int, check systemProbeCheck) {
			results <- result{index: index, probe: check.check(ctx)}
		}(index, check)
	}
	received := make([]bool, len(checks))
	for range checks {
		select {
		case result := <-results:
			services[result.index], received[result.index] = result.probe, true
		case <-ctx.Done():
			for index := range services {
				if !received[index] {
					services[index] = systemProbeResult{Name: checks[index].name, Status: "unknown", RawError: "system health probe phase timed out"}
				}
			}
			return services
		}
	}
	return services
}
