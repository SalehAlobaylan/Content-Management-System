package supply

import (
	"fmt"
	"time"

	"content-management-system/src/models"
	"gorm.io/gorm"
)

const SupplyOperationalMetricsSchema = "media-supply-operational-metrics/v1"

type SupplyMetricSample struct {
	Name    string  `json:"name"`
	Owner   string  `json:"owner"`
	Action  string  `json:"action"`
	Stage   string  `json:"stage"`
	Verdict string  `json:"verdict"`
	Value   float64 `json:"value"`
	Unit    string  `json:"unit"`
}

type SupplyOperationalMetrics struct {
	SchemaVersion string               `json:"schema_version"`
	GeneratedAt   time.Time            `json:"generated_at"`
	Samples       []SupplyMetricSample `json:"samples"`
	Truncated     bool                 `json:"truncated"`
	Unknowns      []string             `json:"unknowns"`
}

const maxSupplyMetricSamples = 32

// BuildSupplyOperationalMetrics exposes only fixed dimensions. No item,
// request, source, tenant, job, or arbitrary failure value is ever a label.
// Every scan is tenant scoped and bounded to a fixed state vocabulary.
func BuildSupplyOperationalMetrics(db *gorm.DB, tenant string, now time.Time) SupplyOperationalMetrics {
	out := SupplyOperationalMetrics{SchemaVersion: SupplyOperationalMetricsSchema, GeneratedAt: now.UTC(), Samples: []SupplyMetricSample{}, Unknowns: []string{}}
	if db == nil || tenant == "" {
		out.Unknowns = append(out.Unknowns, "tenant-scoped operational metrics are unavailable")
		return out
	}
	appendCount := func(name, owner, action, stage, verdict, table, predicate string, args ...any) {
		if len(out.Samples) >= maxSupplyMetricSamples {
			out.Truncated = true
			return
		}
		var count int64
		query := db.Table(table).Where("tenant_id=?", tenant)
		if predicate != "" {
			query = query.Where(predicate, args...)
		}
		if err := query.Count(&count).Error; err != nil {
			out.Unknowns = append(out.Unknowns, fmt.Sprintf("%s is unavailable", name))
			return
		}
		out.Samples = append(out.Samples, SupplyMetricSample{Name: name, Owner: owner, Action: action, Stage: stage, Verdict: verdict, Value: float64(count), Unit: "count"})
	}
	appendAge := func(name, owner, action, stage, verdict, table, timeColumn, predicate string, args ...any) {
		if len(out.Samples) >= maxSupplyMetricSamples {
			out.Truncated = true
			return
		}
		var oldest *time.Time
		query := db.Table(table).Where("tenant_id=?", tenant)
		if predicate != "" {
			query = query.Where(predicate, args...)
		}
		if err := query.Select("MIN(" + timeColumn + ")").Scan(&oldest).Error; err != nil {
			out.Unknowns = append(out.Unknowns, fmt.Sprintf("%s is unavailable", name))
			return
		}
		seconds := float64(0)
		if oldest != nil && !oldest.IsZero() && now.After(*oldest) {
			seconds = now.Sub(oldest.UTC()).Seconds()
		}
		out.Samples = append(out.Samples, SupplyMetricSample{Name: name, Owner: owner, Action: action, Stage: stage, Verdict: verdict, Value: seconds, Unit: "seconds"})
	}
	appendFreshness := func(name, owner, action, stage, verdict, table, timeColumn, predicate string, args ...any) {
		if len(out.Samples) >= maxSupplyMetricSamples {
			out.Truncated = true
			return
		}
		var newest *time.Time
		query := db.Table(table).Where("tenant_id=?", tenant)
		if predicate != "" {
			query = query.Where(predicate, args...)
		}
		if err := query.Select("MAX(" + timeColumn + ")").Scan(&newest).Error; err != nil {
			out.Unknowns = append(out.Unknowns, fmt.Sprintf("%s is unavailable", name))
			return
		}
		seconds := float64(0)
		if newest != nil && !newest.IsZero() && now.After(*newest) {
			seconds = now.Sub(newest.UTC()).Seconds()
		}
		out.Samples = append(out.Samples, SupplyMetricSample{Name: name, Owner: owner, Action: action, Stage: stage, Verdict: verdict, Value: seconds, Unit: "seconds"})
	}

	openSourceStates := []string{"requested", "accepted", "claimed", "running", "verifying", "uncertain"}
	appendCount("source_requests_open", "cms", "source_run", "admission", "open", models.SourceRunRequest{}.TableName(), "state IN ?", openSourceStates)
	appendAge("source_request_oldest_age", "cms", "source_run", "admission", "open", models.SourceRunRequest{}.TableName(), "requested_at", "state IN ?", openSourceStates)
	appendCount("source_leases_expired", "aggregation", "source_run", "dispatch", "expired", models.SourceRunAttempt{}.TableName(), "state IN ? AND dispatcher_lease_expires_at<?", []string{"claimed", "running"}, now)
	appendCount("receipts_retained", "aggregation", "source_run", "receipt", "retained", models.SourceRunRetainedReceipt{}.TableName(), "state=?", "retained")
	appendAge("receipt_oldest_lag", "aggregation", "source_run", "receipt", "retained", models.SourceRunRetainedReceipt{}.TableName(), "created_at", "state=?", "retained")
	appendCount("actions_in_flight", "cms", "supply_action", "execution", "open", models.MediaSupplyActionRequest{}.TableName(), "state IN ?", []string{"queued", "claimed", "running", "verifying", "uncertain"})
	appendCount("actions_verification_pending", "cms", "supply_action", "verification", "pending", models.MediaSupplyActionRequest{}.TableName(), "state IN ?", []string{"verifying", "uncertain"})
	appendCount("actions_cancelled", "cms", "supply_action", "execution", "cancelled", models.MediaSupplyActionRequest{}.TableName(), "state=?", models.MediaSupplyActionRequestCancelled)
	appendCount("action_controls_disabled", "cms", "supply_action", "control", "blocked", models.MediaSupplyControl{}.TableName(), "control_key LIKE ?", "supply_action:%")
	appendCount("source_provider_failures", "aggregation", "source_run", "provider", "failed", models.SourceRunAttempt{}.TableName(), "state IN ?", []string{models.SourceRunAttemptFailed, models.SourceRunAttemptBlocked})
	appendCount("pipeline_repairs_open", "aggregation", "pipeline_repair", "pipeline", "open", models.PipelineRepairRequest{}.TableName(), "state IN ?", []string{models.PipelineRepairQueued, models.PipelineRepairClaimed, models.PipelineRepairRunning, models.PipelineRepairVerifying, models.PipelineRepairUncertain})
	appendCount("pipeline_repairs_verified", "aggregation", "pipeline_repair", "verification", "present", models.PipelineRepairRequest{}.TableName(), "state=?", models.PipelineRepairSucceeded)
	appendCount("artifact_verification_pending", "media_enrichment", "artifact_coverage", "verification", "pending", models.ArtifactCoverageRequest{}.TableName(), "state IN ?", []string{"verifying", "uncertain"})
	appendCount("atomization_verification_pending", "aggregation", "atomization", "verification", "pending", models.AtomizationWorkRequest{}.TableName(), "state IN ?", []string{"verifying", "uncertain"})
	appendFreshness("feed_return_freshness", "cms", "consumer_boundary", "return", "present", models.PodsBoundaryObservation{}.TableName(), "observed_at", "boundary=? AND verdict=?", "feed_return", string(VerdictPresent))
	appendFreshness("page_render_freshness", "cms", "consumer_boundary", "render", "present", models.PodsBoundaryObservation{}.TableName(), "observed_at", "boundary=? AND verdict=?", "page_render", string(VerdictPresent))
	appendFreshness("exact_view_freshness", "cms", "consumer_boundary", "view", "present", models.PodsBoundaryObservation{}.TableName(), "observed_at", "boundary=? AND verdict=?", "exact_view", string(VerdictPresent))
	appendCount("episodes_open", "cms", "supply_episode", "evaluation", "open", models.MediaSupplyEpisode{}.TableName(), "state IN ?", []string{models.MediaSupplyEpisodeOpen, models.MediaSupplyEpisodeRecovering})
	return out
}

func SupplyMetricDimensionsAreBounded(sample SupplyMetricSample) bool {
	allowedOwner := map[string]bool{"cms": true, "aggregation": true, "media_enrichment": true}
	allowedAction := map[string]bool{"source_run": true, "supply_action": true, "pipeline_repair": true, "artifact_coverage": true, "atomization": true, "consumer_boundary": true, "supply_episode": true}
	allowedStage := map[string]bool{"admission": true, "dispatch": true, "receipt": true, "provider": true, "pipeline": true, "execution": true, "control": true, "verification": true, "return": true, "render": true, "view": true, "evaluation": true}
	allowedVerdict := map[string]bool{"open": true, "expired": true, "retained": true, "pending": true, "cancelled": true, "blocked": true, "failed": true, "present": true}
	return allowedOwner[sample.Owner] && allowedAction[sample.Action] && allowedStage[sample.Stage] && allowedVerdict[sample.Verdict]
}
