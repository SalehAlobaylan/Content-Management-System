package controllers

// Read-only structural advice; Retention never creates or rewrites tables.
import (
	"content-management-system/src/models"
	"gorm.io/gorm"
)

type retentionSatelliteStatus struct {
	State           string `json:"state"`
	CompletedCycles int64  `json:"completed_cycles"`
	RequiredCycles  int    `json:"required_cycles"`
	Recommendation  string `json:"recommendation"`
	Reason          string `json:"reason"`
}

// The satellite is a structural escape hatch, not a migration performed by
// Retention. After two completed monthly cycles, expose a deterministic
// recommendation from measured allocation/trend so an operator can decide.
func retentionSatelliteEvaluation(db *gorm.DB, tenant string, policy models.RetentionPolicy, sample *models.RetentionDBSample, forecast retentionForecast) retentionSatelliteStatus {
	result := retentionSatelliteStatus{State: "not_ready", RequiredCycles: 2, Recommendation: "wait_for_two_monthly_cycles", Reason: "Two completed monthly cycles are required before structural evaluation"}
	// A completed cycle is a finalized immutable Month in Review revision. Do
	// not count the mutable retention-month bookkeeping row as an archive.
	db.Model(&models.NewsMonthArchive{}).
		Where("tenant_id=? AND state=?", tenant, "finalized").
		Distinct("month_start").
		Count(&result.CompletedCycles)
	if result.CompletedCycles < int64(result.RequiredCycles) {
		return result
	}
	result.State = "evaluated"
	if sample == nil {
		result.Recommendation = "collect_measurements"
		result.Reason = "Two cycles exist, but no current allocation measurement is available"
		return result
	}
	if sample.DatabaseBytes > policy.DatabaseTargetBytes || forecast.GrowthBytesPerDay > 0 {
		result.Recommendation = "evaluate_heavy_news_satellite"
		result.Reason = "Measured allocation remains above target or continues to grow after two cycles"
		return result
	}
	result.Recommendation = "retain_v1_anchor_design"
	result.Reason = "Allocation is at or below target with no positive measured growth"
	return result
}
