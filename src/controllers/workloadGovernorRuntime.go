package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/utils"
	"content-management-system/src/workload"
	"gorm.io/gorm"
)

// The permit path consumes this bounded cache. Remote probes never run while a
// native claim or resource-allocation transaction holds database locks.
var workloadHealthCache = struct {
	sync.RWMutex
	at       time.Time
	services map[string]systemProbeResult
}{services: map[string]systemProbeResult{}}

func refreshWorkloadHealth() {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	services := collectSystemProbeResults(ctx, []systemProbeCheck{
		{name: "aggregation", check: checkSystemAggregation},
		{name: "enrichment", check: checkSystemEnrichment},
		{name: "media", check: checkSystemMedia},
	})
	byName := make(map[string]systemProbeResult, len(services))
	for _, service := range services {
		byName[service.Name] = service
	}
	workloadHealthCache.Lock()
	workloadHealthCache.at, workloadHealthCache.services = time.Now().UTC(), byName
	workloadHealthCache.Unlock()
}

func workloadServiceSample(service systemProbeResult, at time.Time) workload.Sample {
	available := !at.IsZero() && service.ReadinessObserved
	return workload.Sample{Available: available, Healthy: available && service.Status == "healthy", ObservedAt: at,
		Reason:   "Existing System Health readiness contract: " + service.Status,
		Evidence: map[string]any{"service": service.Name, "contract": service.ContractIdentity, "readiness_observed": service.ReadinessObserved}}
}

// Snapshot rows repeat an installation-wide lane observation into each tenant's
// view. Select one authoritative Aggregation observation; never sum its copies.
func workloadLaneSample(row models.PipelineLaneHealthSnapshot, queue string) workload.Sample {
	var stages map[string]int
	if json.Unmarshal(row.StageCounts, &stages) != nil || row.CapturedAt.IsZero() {
		return workload.Sample{Reason: "Canonical lane queue evidence is unavailable"}
	}
	keys := []string{"news-enrichment-queue", "news-optional-queue"}
	if row.Lane == "pods" {
		keys = []string{"pods-media-queue", "pods-completion-queue", "pods-atomization-queue", "pods-optional-queue"}
	}
	if queue != "" {
		keys = []string{queue}
	}
	depth := 0
	for _, key := range keys {
		n, exists := stages[key]
		if !exists || n < 0 {
			return workload.Sample{ObservedAt: row.CapturedAt, Reason: "Canonical lane queue coverage is incomplete"}
		}
		depth += n
	}
	return workload.Sample{Available: true, Healthy: true, Pressure: float64(depth), ObservedAt: row.CapturedAt,
		Reason:   "Canonical lane snapshot includes waiting, active, delayed and prioritized jobs",
		Evidence: map[string]any{"lane": row.Lane, "queues": stages, "required_oldest_age_seconds": row.RequiredOldestAgeSeconds, "optional_oldest_age_seconds": row.OptionalOldestAgeSeconds}}
}

func workloadFetchSample(queue autopilotQueueStat, at time.Time) workload.Sample {
	if queue.Prioritized == nil || queue.Waiting < 0 || queue.Active < 0 || queue.Delayed < 0 || *queue.Prioritized < 0 {
		return workload.Sample{ObservedAt: at, Reason: "Intake queue coverage is incomplete or invalid"}
	}
	return workload.Sample{Available: true, Healthy: true, ObservedAt: at, Pressure: float64(queue.Waiting) + float64(queue.Active) + float64(queue.Delayed) + float64(*queue.Prioritized), Reason: "Intake fetch queue observation including prioritized jobs"}
}

func workloadSamples(db *gorm.DB) workload.SampleProvider {
	return func(ctx context.Context, keys []string) (map[string]workload.Sample, error) {
		out := make(map[string]workload.Sample, len(keys))
		requested := make(map[string]bool, len(keys))
		for _, key := range keys {
			requested[key] = true
		}
		workloadHealthCache.RLock()
		at := workloadHealthCache.at
		services := make(map[string]systemProbeResult, len(workloadHealthCache.services))
		for key, value := range workloadHealthCache.services {
			services[key] = value
		}
		workloadHealthCache.RUnlock()
		for _, key := range keys {
			out[key] = workload.Sample{Reason: "Capacity evidence has not been observed"}
		}
		for resource, service := range map[string]string{"aggregation_svc": "aggregation", "enrichment_svc": "enrichment", "media_svc": "media", "stt_slots": "media", "llm_slots": "enrichment"} {
			out[resource] = workloadServiceSample(services[service], at)
		}
		// The fetch queue belongs to intake-control. It is the only queue read
		// from that role; News and Pods use their independent canonical snapshots.
		for _, queue := range services["aggregation"].Queues {
			if queue.Queue == "fetch-queue" && requested["agg_fetch_queue"] {
				out["agg_fetch_queue"] = workloadFetchSample(queue, at)
			}
		}
		for _, lane := range []string{"news", "pods"} {
			if !requested["agg_"+lane+"_queue"] && !(lane == "pods" && requested["agg_media_queue"]) {
				continue
			}
			var row models.PipelineLaneHealthSnapshot
			err := db.WithContext(ctx).Where("lane=? AND owner_principal=?", lane, string(utils.MachinePrincipalAggregation)).Order("captured_at DESC").First(&row).Error
			if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return out, err
			}
			if err == nil {
				out["agg_"+lane+"_queue"] = workloadLaneSample(row, "")
				if lane == "pods" {
					out["agg_media_queue"] = workloadLaneSample(row, "pods-media-queue")
				}
			}
		}
		if requested["cms_db"] {
			started := time.Now()
			var one int
			if err := db.WithContext(ctx).Raw("SELECT 1").Scan(&one).Error; err != nil {
				return out, err
			}
			out["cms_db"] = workload.Sample{Available: one == 1, Healthy: one == 1, ObservedAt: time.Now().UTC(), Reason: "Database reachability and separately reserved sweep slots", Evidence: map[string]any{"ping_ms": time.Since(started).Milliseconds()}}
		}
		var episodes []models.SystemIncidentEpisode
		if err := db.WithContext(ctx).Where("status IN ?", []string{models.SystemIncidentStatusOpen, models.SystemIncidentStatusRecovering}).Limit(100).Find(&episodes).Error; err != nil {
			return out, err
		}
		for _, episode := range episodes {
			for _, resource := range workloadIncidentResources(episode.RootService) {
				sample := out[resource]
				sample.Incident = true
				sample.Available = true
				sample.Healthy = false
				sample.Reason = "Confirmed System Health incident affects this resource"
				// Incident evidence is current durable state, even when its original
				// detection time predates the current observation window.
				sample.ObservedAt = time.Now().UTC()
				out[resource] = sample
			}
		}
		return out, nil
	}
}

func workloadIncidentResources(service string) []string {
	switch service {
	case "aggregation":
		return []string{"aggregation_svc", "agg_fetch_queue", "agg_news_queue", "agg_pods_queue", "agg_media_queue"}
	case "enrichment":
		return []string{"enrichment_svc", "llm_slots"}
	case "media":
		return []string{"media_svc", "stt_slots"}
	case "cms", "database":
		return []string{"cms_db"}
	default:
		return nil
	}
}

func workloadLLMSpendSignal(value map[string]any, now time.Time) workload.SpendSignal {
	health, _ := value["ledger_health"].(string)
	reason, _ := value["reason"].(string)
	if reason == "governor_unavailable" || health == "unavailable" || (reason != "governor_disabled" && health != "healthy" && health != "no_evidence") {
		return workload.SpendSignal{Reason: "Tenant LLM allowance is unavailable", ObservedAt: now}
	}
	state := "normal"
	if value["allowed"] == false {
		state = "capped"
	} else if value["verdict"] == "warning" {
		state = "warning"
	}
	return workload.SpendSignal{Available: true, State: state, Reason: reason, ObservedAt: now}
}

func workloadSpendSignal(ctx context.Context, db *gorm.DB, request workload.Request) (workload.SpendSignal, error) {
	now := time.Now().UTC()
	if request.Class == "llm_batch" {
		// No fallback to the default-tenant compatibility wrapper.
		value := CheckTenantSpendAllowance(db.WithContext(ctx), request.TenantID, "llm", 0, request.System, "")
		return workloadLLMSpendSignal(value, now), nil
	}
	if request.Class == "stt_batch" {
		var config models.TranscriptionConfig
		err := db.WithContext(ctx).Where("tenant_id=?", request.TenantID).First(&config).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return workload.SpendSignal{Available: true, State: "normal", Reason: "Transcription owner uses its default uncapped budget; native admission remains required", ObservedAt: now}, nil
		}
		if err != nil {
			return workload.SpendSignal{Reason: "Transcription owner budget unavailable", ObservedAt: now}, err
		}
		state := "normal"
		// Read-only: window rollover and per-job cost/reservation remain entirely
		// authoritative in transcription owner admission, never in this adapter.
		if now.Before(config.MonthlyWindowStart.Add(30*24*time.Hour)) && config.MonthlyBudgetCapUsd > 0 {
			ratio := (config.MonthlySpendUsd + config.MonthlyReservedUsd) / config.MonthlyBudgetCapUsd
			if ratio >= 1 {
				state = "capped"
			} else if ratio >= .8 {
				state = "warning"
			}
		}
		return workload.SpendSignal{Available: true, State: state, Reason: "Tenant transcription-owner budget projection; each native job still prices and reserves its cost", ObservedAt: now}, nil
	}
	return workload.SpendSignal{Available: true, State: "normal", Reason: "No financial resource is consumed by this class", ObservedAt: now}, nil
}

func StartWorkloadGovernorHeartbeat(db *gorm.DB) {
	workload.InstallOffPolicyCache(db)
	workload.SetSampleProvider(workloadSamples(db))
	workload.SetSpendProvider(workloadSpendSignal)
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			func() {
				defer func() {
					if recover() != nil {
						log.Print("workload governor observation unavailable; native owner admission continues")
					}
				}()
				refreshWorkloadHealth()
				if err := workload.RefreshSnapshots(db); err != nil {
					log.Print("workload capacity snapshot unavailable; native owner admission continues")
				}
				if err := workload.Maintain(db); err != nil {
					log.Print("workload governor maintenance unavailable")
				}
			}()
			<-ticker.C
		}
	}()
}
