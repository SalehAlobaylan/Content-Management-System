package controllers

import (
	"content-management-system/src/models"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
)

type experienceReleaseMetric struct {
	Surface             string `json:"surface"`
	Metric              string `json:"metric"`
	Windows             int64  `json:"comparable_windows"`
	CurrentNumerator    int64  `json:"current_numerator"`
	CurrentDenominator  int64  `json:"current_denominator"`
	PreviousNumerator   int64  `json:"previous_numerator"`
	PreviousDenominator int64  `json:"previous_denominator"`
	CurrentP75          int    `json:"current_p75_ms,omitempty"`
	PreviousP75         int    `json:"previous_p75_ms,omitempty"`
	LatencyQualified    bool   `json:"latency_qualified"`
}

type experienceReleaseComparison struct {
	Current   string                    `json:"current_release,omitempty"`
	Previous  string                    `json:"previous_release,omitempty"`
	Qualified bool                      `json:"qualified"`
	Reason    string                    `json:"reason"`
	Metrics   []experienceReleaseMetric `json:"metrics"`
}

// Compare only completed matching surface/metric/hour populations. Query output
// is bounded by the fixed registry, independently of evidence-list pagination.
// Current/previous means the last two observed non-development build identities actually seen
// at the server; retained aggregates alone cannot establish deployment ordering.
func compareExperienceReleases(db *gorm.DB, tenant string, policy models.ExperiencePolicy, start, end time.Time, clients ...string) (experienceReleaseComparison, error) {
	result := experienceReleaseComparison{Reason: "Two observed releases with sufficient overlapping hours are required.", Metrics: []experienceReleaseMetric{}}
	var releases []string
	if err := db.Model(&models.ExperienceEvent{}).Select("release").
		Where("tenant_id=? AND received_at>=? AND received_at<? AND schema_version IN (2,3) AND terminal_status='accepted' AND visible=true AND release<>'' AND release<>'dev'", tenant, start, end).
		Where("client_platform=?", experienceClient(clients)).Group("release").Order("MAX(received_at) DESC,release ASC").Limit(2).Pluck("release", &releases).Error; err != nil {
		return result, err
	}
	if len(releases) < 2 {
		return result, nil
	}
	result.Current, result.Previous = releases[0], releases[1]
	fields := []string{
		"a.surface", "a.metric_key", "COUNT(*) AS windows",
		"SUM(a.numerator)::bigint AS current_numerator", "SUM(a.denominator)::bigint AS current_denominator",
		"SUM(b.numerator)::bigint AS previous_numerator", "SUM(b.denominator)::bigint AS previous_denominator",
		"SUM(a.sample_count)::bigint AS current_samples", "SUM(b.sample_count)::bigint AS previous_samples",
	}
	for i := 0; i < 8; i++ {
		fields = append(fields,
			fmt.Sprintf("SUM(COALESCE((a.latency_buckets->>%d)::bigint,0))::bigint AS current_bucket_%d", i, i),
			fmt.Sprintf("SUM(COALESCE((b.latency_buckets->>%d)::bigint,0))::bigint AS previous_bucket_%d", i, i))
	}
	query := "SELECT " + strings.Join(fields, ",") +
		" FROM experience_metric_rollups a JOIN experience_metric_rollups b ON a.tenant_id=b.tenant_id AND a.client_platform=b.client_platform AND a.bucket_start=b.bucket_start AND a.surface=b.surface AND a.metric_key=b.metric_key AND a.resolution=b.resolution" +
		" WHERE a.tenant_id=? AND a.client_platform=? AND a.resolution='hour' AND a.completed=true AND b.completed=true AND a.histogram_version=2 AND b.histogram_version=2" +
		" AND a.cohort_dim='release' AND b.cohort_dim='release' AND a.cohort_val=? AND b.cohort_val=? AND a.bucket_start>=? AND a.bucket_start<?" +
		" AND a.denominator>=? AND b.denominator>=? AND (a.metric_key<>'rebuffer_ratio' OR (a.outcome_count>=? AND b.outcome_count>=?))" +
		" GROUP BY a.surface,a.metric_key ORDER BY a.surface,a.metric_key"
	var groups []map[string]interface{}
	if err := db.Raw(query, tenant, experienceClient(clients), result.Current, result.Previous, start, end, policy.MinSampleFloor, policy.MinSampleFloor, policy.MinSampleFloor, policy.MinSampleFloor).Scan(&groups).Error; err != nil {
		return result, err
	}
	for _, group := range groups {
		metric := fmt.Sprint(group["metric_key"])
		if !knownExperienceMetric(metric) {
			continue
		}
		row := experienceReleaseMetric{
			Surface: fmt.Sprint(group["surface"]), Metric: metric, Windows: asExperienceInt(group["windows"]),
			CurrentNumerator: asExperienceInt(group["current_numerator"]), CurrentDenominator: asExperienceInt(group["current_denominator"]),
			PreviousNumerator: asExperienceInt(group["previous_numerator"]), PreviousDenominator: asExperienceInt(group["previous_denominator"]),
		}
		row.LatencyQualified = latencyMetrics[metric] && asExperienceInt(group["current_samples"]) >= int64(policy.LatencySampleFloor) && asExperienceInt(group["previous_samples"]) >= int64(policy.LatencySampleFloor)
		if row.LatencyQualified {
			var current, previous [8]int64
			for i := range current {
				current[i] = asExperienceInt(group[fmt.Sprintf("current_bucket_%d", i)])
				previous[i] = asExperienceInt(group[fmt.Sprintf("previous_bucket_%d", i)])
			}
			row.CurrentP75, row.PreviousP75 = p75FromBuckets(current), p75FromBuckets(previous)
		}
		result.Metrics = append(result.Metrics, row)
	}
	result.Qualified = len(result.Metrics) > 0
	if result.Qualified {
		result.Reason = "Matched hours with sufficient volume; cohort composition and deployment causality are not established."
	}
	return result, nil
}
