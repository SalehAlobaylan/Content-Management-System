package controllers

// Retention Autopilot is a deterministic, ledgered supervisor. Destructive
// canonical-content and physical-storage actions remain human-only; Slice 10
// can promote only the verified derived-state snapshot refresh action.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/utils"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const retentionV1Tenant = "default"

var (
	retentionRunMu      sync.Mutex
	retentionRunTenants = map[string]bool{}
	errRetentionBusy    = errors.New("a retention run is already active for this tenant")
)

type retentionForecast struct {
	GrowthBytesPerDay    int64    `json:"growth_bytes_per_day"`
	RunwayToTargetDays   *float64 `json:"runway_to_target_days,omitempty"`
	RunwayToActionDays   *float64 `json:"runway_to_action_days,omitempty"`
	RunwayToCriticalDays *float64 `json:"runway_to_critical_days,omitempty"`
	SampleCount          int      `json:"sample_count"`
	WindowHours          float64  `json:"window_hours"`
}

type retentionPreview struct {
	EligibleStories int   `json:"eligible_stories"`
	CandidateRows   int   `json:"candidate_rows"`
	EstimatedBytes  int64 `json:"estimated_bytes"`
	ProtectedRows   int   `json:"protected_rows"`
	BlockedStories  int   `json:"blocked_stories"`
}

type retentionStatus struct {
	Policy       models.RetentionPolicy             `json:"policy"`
	LatestSample *models.RetentionDBSample          `json:"latest_sample,omitempty"`
	LatestRun    *models.RetentionRun               `json:"latest_run,omitempty"`
	Forecast     retentionForecast                  `json:"forecast"`
	Verdict      string                             `json:"verdict"`
	Preview      retentionPreview                   `json:"preview"`
	Historical   map[string]interface{}             `json:"historical"`
	Maintenance  *models.RetentionMaintenanceReport `json:"latest_maintenance,omitempty"`
	Execution    models.RetentionExecutionControl   `json:"execution_controls"`
	Paused       bool                               `json:"paused"`
	ObserveOnly  bool                               `json:"observe_only"`
	Promotion    retentionPromotionStatus           `json:"promotion"`
	Trust        []retentionTrustStat               `json:"trust"`
	Satellite    retentionSatelliteStatus           `json:"satellite_evaluation"`
	Guarantees   map[string]interface{}             `json:"guarantees"`
}

const (
	retentionCapabilityCanonicalCompaction = "canonical_compaction"
	retentionCapabilityHistorical          = "historical_retirement"
	retentionCapabilityOwnerRuns           = "owner_runs"
	retentionCapabilityRecoveryRotate      = "feed_recovery_rotate"
	retentionCapabilityRecoveryPurge       = "feed_recovery_purge_reseed"
)

func retentionCapabilityEnabled(control models.RetentionExecutionControl, capability string) bool {
	switch capability {
	case retentionCapabilityCanonicalCompaction:
		return control.CanonicalCompactionEnabled
	case retentionCapabilityHistorical:
		return control.HistoricalEnabled
	case retentionCapabilityOwnerRuns:
		return control.OwnerRunsEnabled
	case retentionCapabilityRecoveryRotate:
		return control.FeedRecoveryRotateEnabled
	case retentionCapabilityRecoveryPurge:
		return control.FeedRecoveryPurgeEnabled
	default:
		return false
	}
}

func requireRetentionCapability(db *gorm.DB, tenant, capability string) error {
	var control models.RetentionExecutionControl
	if err := db.Where("tenant_id = ?", tenant).First(&control).Error; err != nil {
		return fmt.Errorf("safety_remediation_required: execution control is unavailable")
	}
	if !retentionCapabilityEnabled(control, capability) {
		return fmt.Errorf("safety_remediation_required: %s execution is disabled until rollout validation completes", capability)
	}
	return nil
}

func retentionExecutionControlFor(db *gorm.DB, tenant string) models.RetentionExecutionControl {
	var control models.RetentionExecutionControl
	_ = db.Where("tenant_id = ?", tenant).First(&control).Error
	return control
}

func loadRetentionPolicy(db *gorm.DB, tenant string) models.RetentionPolicy {
	var policy models.RetentionPolicy
	if err := db.Where("tenant_id = ?", tenant).First(&policy).Error; err != nil {
		return models.DefaultRetentionPolicy(tenant)
	}
	return normalizeRetentionPolicy(policy)
}

func normalizeRetentionPolicy(policy models.RetentionPolicy) models.RetentionPolicy {
	defaults := models.DefaultRetentionPolicy(policy.TenantID)
	if policy.Mode != models.RetentionModeObserve &&
		policy.Mode != models.RetentionModeAssist &&
		policy.Mode != models.RetentionModeSafeAuto {
		policy.Mode = defaults.Mode
	}
	if policy.ScheduleIntervalMinutes < 15 {
		policy.ScheduleIntervalMinutes = defaults.ScheduleIntervalMinutes
	}
	if policy.DatabaseTargetBytes <= 0 {
		policy.DatabaseTargetBytes = defaults.DatabaseTargetBytes
	}
	if policy.DatabaseWarningBytes <= 0 {
		policy.DatabaseWarningBytes = defaults.DatabaseWarningBytes
	}
	if policy.DatabaseActionBytes <= 0 {
		policy.DatabaseActionBytes = defaults.DatabaseActionBytes
	}
	if policy.DatabaseCriticalBytes <= 0 {
		policy.DatabaseCriticalBytes = defaults.DatabaseCriticalBytes
	}
	if policy.WarningForecastDays < 1 {
		policy.WarningForecastDays = defaults.WarningForecastDays
	}
	if policy.ActionForecastDays < 1 {
		policy.ActionForecastDays = defaults.ActionForecastDays
	}
	if policy.CriticalForecastHours < 1 {
		policy.CriticalForecastHours = defaults.CriticalForecastHours
	}
	if policy.MaxRowsPerRun < 1 {
		policy.MaxRowsPerRun = defaults.MaxRowsPerRun
	}
	if policy.MaxBytesPerRun < 1 {
		policy.MaxBytesPerRun = defaults.MaxBytesPerRun
	}
	if policy.MaxActionsPerRun < 1 {
		policy.MaxActionsPerRun = defaults.MaxActionsPerRun
	}
	if policy.NewsTimezone == "" {
		policy.NewsTimezone = defaults.NewsTimezone
	}
	if policy.TrustMinDecisions < 1 {
		policy.TrustMinDecisions = defaults.TrustMinDecisions
	}
	if policy.TrustMinAgreementPct < 50 || policy.TrustMinAgreementPct > 100 {
		policy.TrustMinAgreementPct = defaults.TrustMinAgreementPct
	}
	return policy
}

func retentionPolicyValid(policy models.RetentionPolicy) error {
	if policy.Mode != models.RetentionModeObserve &&
		policy.Mode != models.RetentionModeAssist &&
		policy.Mode != models.RetentionModeSafeAuto {
		return errors.New("mode must be observe, assist, or safe_auto")
	}
	if policy.ScheduleIntervalMinutes < 15 || policy.ScheduleIntervalMinutes > 10080 {
		return errors.New("schedule_interval_minutes must be between 15 and 10080")
	}
	if !(policy.DatabaseTargetBytes <= policy.DatabaseWarningBytes &&
		policy.DatabaseWarningBytes < policy.DatabaseActionBytes &&
		policy.DatabaseActionBytes < policy.DatabaseCriticalBytes) {
		return errors.New("database thresholds must be ordered target <= warning < action < critical")
	}
	if policy.WarningForecastDays < 1 || policy.WarningForecastDays > 90 ||
		policy.ActionForecastDays < 1 || policy.ActionForecastDays > 90 ||
		policy.CriticalForecastHours < 1 || policy.CriticalForecastHours > 720 {
		return errors.New("forecast horizons are outside supported bounds")
	}
	if policy.MaxRowsPerRun < 1 || policy.MaxRowsPerRun > 10000 ||
		policy.MaxBytesPerRun < 1 || policy.MaxActionsPerRun < 1 || policy.MaxActionsPerRun > 50 {
		return errors.New("run caps are outside supported bounds")
	}
	if policy.TrustMinDecisions < 1 || policy.TrustMinDecisions > 10000 || policy.TrustMinAgreementPct < 50 || policy.TrustMinAgreementPct > 100 {
		return errors.New("trust thresholds are outside supported bounds")
	}
	if len(policy.ActionModes) > 0 {
		var modes map[string]string
		if err := json.Unmarshal(policy.ActionModes, &modes); err != nil {
			return errors.New("action_modes must be valid JSON")
		}
		for actionClass, mode := range modes {
			if mode != models.RetentionModeObserve && mode != models.RetentionModeAssist && mode != models.RetentionModeSafeAuto {
				return fmt.Errorf("invalid mode for action class %s", actionClass)
			}
			if mode == models.RetentionModeSafeAuto && !retentionActionAutoEligible(actionClass) {
				return fmt.Errorf("action class %s is human-only", actionClass)
			}
		}
	}
	return nil
}

func retentionVerdict(policy models.RetentionPolicy, sample *models.RetentionDBSample, forecast retentionForecast) string {
	if sample == nil {
		return models.RetentionVerdictInconclusive
	}
	criticalDays := float64(policy.CriticalForecastHours) / 24
	if sample.DatabaseBytes > policy.DatabaseCriticalBytes ||
		runwayWithin(forecast.RunwayToCriticalDays, criticalDays) {
		return models.RetentionVerdictCritical
	}
	if sample.DatabaseBytes > policy.DatabaseActionBytes ||
		runwayWithin(forecast.RunwayToActionDays, float64(policy.ActionForecastDays)) {
		return models.RetentionVerdictActionRequired
	}
	if sample.DatabaseBytes > policy.DatabaseWarningBytes ||
		runwayWithin(forecast.RunwayToTargetDays, float64(policy.WarningForecastDays)) {
		return models.RetentionVerdictWarning
	}
	return models.RetentionVerdictHealthy
}

func retentionOperationalVerdict(policy models.RetentionPolicy, sample *models.RetentionDBSample, forecast retentionForecast, preview retentionPreview, recoveryInProgress bool) string {
	if recoveryInProgress {
		return models.RetentionVerdictRecoveryInProgress
	}
	base := retentionVerdict(policy, sample, forecast)
	if sample == nil {
		return models.RetentionVerdictInconclusive
	}
	if base == models.RetentionVerdictCritical || base == models.RetentionVerdictActionRequired {
		if preview.CandidateRows > 0 {
			return models.RetentionVerdictCompactionDue
		}
		return models.RetentionVerdictBlocked
	}
	if base == models.RetentionVerdictWarning && preview.CandidateRows > 0 {
		return models.RetentionVerdictCompactionDue
	}
	return base
}

func retentionHistoricalSignal(db *gorm.DB, tenant, timezone string, policy models.RetentionPolicy) (string, map[string]interface{}) {
	_, err := time.LoadLocation(timezone)
	if err != nil {
		return models.RetentionVerdictInconclusive, map[string]interface{}{"error": "invalid_news_timezone"}
	}
	monthStart := monthlyStart(time.Now(), timezone)
	var oldRows int64
	if err := db.Model(&models.ContentItem{}).Where("tenant_id=? AND type=? AND published_at < ? AND status IN ?", tenant, models.ContentTypeNews, monthStart.UTC(), []models.ContentStatus{models.ContentStatusReady, models.ContentStatusArchived, models.ContentStatusFailed}).Count(&oldRows).Error; err != nil {
		return models.RetentionVerdictInconclusive, map[string]interface{}{"error": "historical_inventory_unavailable"}
	}
	var finalized int64
	if err := db.Model(&models.NewsMonthArchive{}).Where("tenant_id=? AND month_start < ? AND finalized_at IS NOT NULL", tenant, monthStart.UTC()).Count(&finalized).Error; err != nil {
		return models.RetentionVerdictInconclusive, map[string]interface{}{"error": "archive_inventory_unavailable"}
	}
	evidence := map[string]interface{}{"month_start": monthStart.Format("2006-01-02"), "old_news_rows": oldRows, "finalized_archives": finalized}
	if oldRows == 0 {
		return "", evidence
	}
	if finalized == 0 {
		return models.RetentionVerdictArchiveBlocked, evidence
	}
	_ = policy // policy caps are applied by historical preparation, not status reads.
	return models.RetentionVerdictCleanupDue, evidence
}

func runwayWithin(runway *float64, horizon float64) bool {
	return runway != nil && *runway >= 0 && *runway <= horizon
}

func calculateRetentionForecast(samples []models.RetentionDBSample, policy models.RetentionPolicy) retentionForecast {
	result := retentionForecast{SampleCount: len(samples)}
	if len(samples) < 2 {
		return result
	}
	first, last := samples[0], samples[len(samples)-1]
	window := last.MeasuredAt.Sub(first.MeasuredAt)
	result.WindowHours = math.Round(window.Hours()*100) / 100
	if window < time.Hour || last.DatabaseBytes <= first.DatabaseBytes {
		return result
	}
	result.GrowthBytesPerDay = int64(float64(last.DatabaseBytes-first.DatabaseBytes) / window.Hours() * 24)
	result.RunwayToTargetDays = retentionRunway(last.DatabaseBytes, policy.DatabaseTargetBytes, result.GrowthBytesPerDay)
	result.RunwayToActionDays = retentionRunway(last.DatabaseBytes, policy.DatabaseActionBytes, result.GrowthBytesPerDay)
	result.RunwayToCriticalDays = retentionRunway(last.DatabaseBytes, policy.DatabaseCriticalBytes, result.GrowthBytesPerDay)
	return result
}

func retentionRunway(current, threshold, growthPerDay int64) *float64 {
	if growthPerDay <= 0 {
		return nil
	}
	days := float64(threshold-current) / float64(growthPerDay)
	return &days
}

func retentionSamples(db *gorm.DB, tenant string, limit int) []models.RetentionDBSample {
	var samples []models.RetentionDBSample
	db.Where("tenant_id = ? AND measured_at >= ?", tenant, time.Now().UTC().AddDate(0, 0, -8)).
		Order("measured_at ASC").Limit(limit).Find(&samples)
	return samples
}

type retentionRelationSample struct {
	SchemaName string `json:"schema_name"`
	TableName  string `json:"table_name"`
	TotalBytes int64  `json:"total_bytes"`
	IndexBytes int64  `json:"index_bytes"`
	ToastBytes int64  `json:"toast_bytes"`
	LiveTuples int64  `json:"live_tuples"`
	DeadTuples int64  `json:"dead_tuples"`
}

func collectRetentionDBSample(db *gorm.DB, tenant string) (models.RetentionDBSample, error) {
	now := time.Now().UTC()
	sample := models.RetentionDBSample{
		TenantID: tenant, ProviderSource: "unavailable", MeasuredAt: now,
		RelationBreakdown: datatypes.JSON([]byte(`[]`)),
		ForecastInputs:    datatypes.JSON([]byte(`{"window_days":7}`)),
	}
	if err := db.Raw("SELECT pg_database_size(current_database())").Scan(&sample.DatabaseBytes).Error; err != nil {
		return sample, fmt.Errorf("database size: %w", err)
	}

	var relations []retentionRelationSample
	err := db.Raw(`
		SELECT schemaname AS schema_name,
		       relname AS table_name,
		       pg_total_relation_size(relid) AS total_bytes,
		       pg_indexes_size(relid) AS index_bytes,
		       CASE WHEN reltoastrelid = 0 THEN 0 ELSE pg_total_relation_size(reltoastrelid) END AS toast_bytes,
		       n_live_tup::bigint AS live_tuples,
		       n_dead_tup::bigint AS dead_tuples
		FROM pg_stat_user_tables
		JOIN pg_class ON pg_class.oid = relid
		ORDER BY pg_total_relation_size(relid) DESC
		LIMIT 50`).Scan(&relations).Error
	if err != nil {
		return sample, fmt.Errorf("relation attribution: %w", err)
	}
	for _, relation := range relations {
		sample.AllocatedBytes += relation.TotalBytes
		sample.IndexBytes += relation.IndexBytes
		sample.ToastBytes += relation.ToastBytes
		sample.LiveTuples += relation.LiveTuples
		sample.DeadTuples += relation.DeadTuples
	}
	sample.RelationBytes = sample.AllocatedBytes - sample.IndexBytes
	if sample.RelationBytes < 0 {
		sample.RelationBytes = 0
	}
	if sample.LiveTuples+sample.DeadTuples > 0 {
		sample.ReusableBytes = int64(float64(sample.AllocatedBytes) *
			(float64(sample.DeadTuples) / float64(sample.LiveTuples+sample.DeadTuples)))
	}
	raw, _ := json.Marshal(relations)
	sample.RelationBreakdown = datatypes.JSON(raw)
	if err := db.Create(&sample).Error; err != nil {
		return sample, fmt.Errorf("persist sample: %w", err)
	}
	return sample, nil
}

func retentionNewsTimezone(db *gorm.DB, tenant string) string {
	var policy models.NewsCirculationPolicy
	if err := db.Select("timezone").Where("tenant_id = ?", tenant).First(&policy).Error; err == nil &&
		strings.TrimSpace(policy.Timezone) != "" {
		return policy.Timezone
	}
	return "Asia/Riyadh"
}

func previewRetentionNews(db *gorm.DB, tenant, timezone string) (retentionPreview, error) {
	location, err := time.LoadLocation(timezone)
	if err != nil {
		location = time.FixedZone("Asia/Riyadh", 3*60*60)
	}
	now := time.Now().In(location)
	dormantCutoff := now.AddDate(0, 0, -7).UTC()
	weekStartLocal := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, location).
		AddDate(0, 0, -int(now.Weekday()))
	weekStart := weekStartLocal.UTC()
	monthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, location).UTC()

	var result struct {
		EligibleStories int
		CandidateRows   int
		EstimatedBytes  int64
	}
	err = db.Raw(`
		WITH eligible AS (
			SELECT s.id, s.public_id
			FROM stories s
			WHERE s.tenant_id = ?
			  AND COALESCE(s.news_retention_state, 'full') = 'full'
			  AND s.last_member_at >= ?
			  AND s.last_member_at <= ?
			  AND s.last_member_at < ?
			  AND NOT EXISTS (
				SELECT 1 FROM content_items pending
				WHERE pending.tenant_id = s.tenant_id
				  AND pending.story_id = s.public_id
				  AND pending.status IN ('PENDING', 'PROCESSING')
			  )
			  AND NOT EXISTS (
				SELECT 1 FROM retention_holds hold
				WHERE hold.tenant_id = s.tenant_id
				  AND hold.target_type = 'story'
				  AND hold.target_id = s.public_id
				  AND hold.released_at IS NULL
				  AND (hold.expires_at IS NULL OR hold.expires_at > NOW())
			  )
		)
		SELECT COUNT(DISTINCT eligible.id)::int AS eligible_stories,
		       COUNT(items.id)::int AS candidate_rows,
		       COALESCE(SUM(pg_column_size(items)), 0)::bigint AS estimated_bytes
		FROM eligible
		JOIN content_items items
		  ON items.tenant_id = ? AND items.story_id = eligible.public_id
		WHERE items.type = 'NEWS'
		  AND COALESCE(items.news_retention_state, 'full') = 'full'`,
		tenant, monthStart, dormantCutoff, weekStart, tenant).Scan(&result).Error
	if err != nil {
		return retentionPreview{}, err
	}
	return retentionPreview{
		EligibleStories: result.EligibleStories,
		CandidateRows:   result.CandidateRows,
		EstimatedBytes:  result.EstimatedBytes,
	}, nil
}

func retentionLatestState(db *gorm.DB, tenant string) (*models.RetentionDBSample, *models.RetentionRun) {
	var sample models.RetentionDBSample
	var run models.RetentionRun
	var samplePtr *models.RetentionDBSample
	var runPtr *models.RetentionRun
	if db.Where("tenant_id = ?", tenant).Order("measured_at DESC").First(&sample).Error == nil {
		samplePtr = &sample
	}
	if db.Where("tenant_id = ?", tenant).Order("started_at DESC").First(&run).Error == nil {
		runPtr = &run
	}
	return samplePtr, runPtr
}

func retentionStatusFor(db *gorm.DB, tenant string) retentionStatus {
	policy := loadRetentionPolicy(db, tenant)
	policy.NewsTimezone = retentionNewsTimezone(db, tenant)
	sample, run := retentionLatestState(db, tenant)
	forecast := calculateRetentionForecast(retentionSamples(db, tenant, 200), policy)
	preview, previewErr := previewRetentionNews(db, tenant, policy.NewsTimezone)
	var activeRecovery int64
	activeRecoveryErr := db.Table("feed_availability_states").Where("tenant_id = ? AND state <> 'normal'", tenant).Count(&activeRecovery).Error
	now := time.Now().UTC()
	promotion := retentionPromotionFor(db, tenant, policy)
	satellite := retentionSatelliteEvaluation(db, tenant, policy, sample, forecast)
	verdict := retentionOperationalVerdict(policy, sample, forecast, preview, activeRecovery > 0)
	historicalVerdict, historicalEvidence := retentionHistoricalSignal(db, tenant, policy.NewsTimezone, policy)
	statusReadErrors := []string{}
	if previewErr != nil {
		statusReadErrors = append(statusReadErrors, "news_preview_unavailable")
	}
	if activeRecoveryErr != nil {
		statusReadErrors = append(statusReadErrors, "recovery_availability_unavailable")
	}
	if len(statusReadErrors) > 0 {
		historicalEvidence["status_read_errors"] = statusReadErrors
	}
	var maintenance *models.RetentionMaintenanceReport
	var latestMaintenance models.RetentionMaintenanceReport
	if db.Where("tenant_id=?", tenant).Order("created_at DESC").First(&latestMaintenance).Error == nil {
		maintenance = &latestMaintenance
	}
	if activeRecovery == 0 && verdict != models.RetentionVerdictCritical && verdict != models.RetentionVerdictActionRequired && verdict != models.RetentionVerdictCompactionDue {
		if historicalVerdict != "" {
			verdict = historicalVerdict
		}
	}
	if activeRecovery == 0 && maintenance != nil && maintenance.PostgresReady && !maintenance.ProviderReady && verdict == models.RetentionVerdictHealthy {
		verdict = models.RetentionVerdictMaintenanceRequired
	}
	if len(statusReadErrors) > 0 {
		verdict = models.RetentionVerdictInconclusive
	}
	var latestIntegrity models.FeedIntegrityRun
	if activeRecovery == 0 && db.Where("tenant_id=? AND started_at >= ?", tenant, time.Now().UTC().Add(-24*time.Hour)).Order("started_at DESC").First(&latestIntegrity).Error == nil && (latestIntegrity.Status != models.FeedIntegrityRunCompleted || latestIntegrity.Headline != "all_clear") {
		verdict = models.RetentionVerdictRecoveryRequired
	}
	return retentionStatus{
		Policy: policy, LatestSample: sample, LatestRun: run, Forecast: forecast,
		Verdict: verdict, Preview: preview, Historical: historicalEvidence, Maintenance: maintenance,
		Execution:   retentionExecutionControlFor(db, tenant),
		Paused:      policy.PausedUntil != nil && policy.PausedUntil.After(now),
		ObserveOnly: policy.Mode == models.RetentionModeObserve,
		Promotion:   promotion,
		Trust:       promotion.Trust,
		Satellite:   satellite,
		Guarantees: map[string]interface{}{
			"full_fidelity_days":     7,
			"history_retention_days": 90,
			"canonical_row_deletion": "human_approval_required",
			"sources_preserved":      true,
			"physical_rewrites":      "operator_only",
		},
	}
}

func requireRetentionTenant(c *gin.Context) (utils.AdminPrincipal, bool) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return principal, false
	}
	if principal.TenantID != retentionV1Tenant {
		c.JSON(http.StatusConflict, gin.H{"error": "Retention V1 is limited to the default public tenant"})
		return principal, false
	}
	return principal, true
}

func GetRetentionStatus(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	c.JSON(http.StatusOK, gin.H{"data": retentionStatusFor(db, principal.TenantID)})
}

func GetRetentionPolicy(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	policy := loadRetentionPolicy(db, principal.TenantID)
	policy.NewsTimezone = retentionNewsTimezone(db, principal.TenantID)
	c.JSON(http.StatusOK, gin.H{"data": policy})
}

func GetRetentionExecutionControl(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	c.JSON(http.StatusOK, gin.H{"data": retentionExecutionControlFor(db, principal.TenantID)})
}

func UpdateRetentionExecutionControl(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	if !principal.HasRole("admin") {
		c.JSON(http.StatusForbidden, gin.H{"error": "administrator role required to change execution controls"})
		return
	}
	var patch struct {
		CanonicalCompactionEnabled *bool  `json:"canonical_compaction_enabled"`
		HistoricalEnabled          *bool  `json:"historical_enabled"`
		OwnerRunsEnabled           *bool  `json:"owner_runs_enabled"`
		FeedRecoveryRotateEnabled  *bool  `json:"feed_recovery_rotate_enabled"`
		FeedRecoveryPurgeEnabled   *bool  `json:"feed_recovery_purge_enabled"`
		Reason                     string `json:"reason"`
	}
	if err := c.ShouldBindJSON(&patch); err != nil || len(strings.TrimSpace(patch.Reason)) < 10 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "a reason of at least 10 characters is required"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var control models.RetentionExecutionControl
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=?", principal.TenantID).First(&control).Error; err != nil {
			if !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
			control = models.RetentionExecutionControl{TenantID: principal.TenantID}
			if err := tx.Create(&control).Error; err != nil {
				return err
			}
		}
		if patch.CanonicalCompactionEnabled != nil {
			control.CanonicalCompactionEnabled = *patch.CanonicalCompactionEnabled
		}
		if patch.HistoricalEnabled != nil {
			control.HistoricalEnabled = *patch.HistoricalEnabled
		}
		if patch.OwnerRunsEnabled != nil {
			control.OwnerRunsEnabled = *patch.OwnerRunsEnabled
		}
		if patch.FeedRecoveryRotateEnabled != nil {
			control.FeedRecoveryRotateEnabled = *patch.FeedRecoveryRotateEnabled
		}
		if patch.FeedRecoveryPurgeEnabled != nil {
			control.FeedRecoveryPurgeEnabled = *patch.FeedRecoveryPurgeEnabled
		}
		control.UpdatedBy = principal.Email
		control.UpdatedAt = time.Now().UTC()
		return tx.Model(&control).Updates(map[string]interface{}{
			"canonical_compaction_enabled": control.CanonicalCompactionEnabled,
			"historical_enabled":           control.HistoricalEnabled,
			"owner_runs_enabled":           control.OwnerRunsEnabled,
			"feed_recovery_rotate_enabled": control.FeedRecoveryRotateEnabled,
			"feed_recovery_purge_enabled":  control.FeedRecoveryPurgeEnabled,
			"updated_by":                   control.UpdatedBy,
			"updated_at":                   control.UpdatedAt,
		}).Error
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not persist execution control"})
		return
	}
	retentionAudit(db, principal, "retention.execution_control.update", principal.TenantID, "success", map[string]interface{}{"reason": patch.Reason, "control": control})
	c.JSON(http.StatusOK, gin.H{"data": control})
}

func UpdateRetentionPolicy(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	policy := loadRetentionPolicy(db, principal.TenantID)
	var patch struct {
		Enabled                 *bool             `json:"enabled"`
		Mode                    *string           `json:"mode"`
		ScheduleIntervalMinutes *int              `json:"schedule_interval_minutes"`
		DatabaseTargetBytes     *int64            `json:"database_target_bytes"`
		DatabaseWarningBytes    *int64            `json:"database_warning_bytes"`
		DatabaseActionBytes     *int64            `json:"database_action_bytes"`
		DatabaseCriticalBytes   *int64            `json:"database_critical_bytes"`
		WarningForecastDays     *int              `json:"warning_forecast_days"`
		ActionForecastDays      *int              `json:"action_forecast_days"`
		CriticalForecastHours   *int              `json:"critical_forecast_hours"`
		MaxRowsPerRun           *int              `json:"max_rows_per_run"`
		MaxBytesPerRun          *int64            `json:"max_bytes_per_run"`
		MaxActionsPerRun        *int              `json:"max_actions_per_run"`
		ActionModes             map[string]string `json:"action_modes"`
		TrustMinDecisions       *int              `json:"trust_min_decisions"`
		TrustMinAgreementPct    *int              `json:"trust_min_agreement_pct"`
	}
	if err := c.ShouldBindJSON(&patch); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid policy payload"})
		return
	}
	if patch.Enabled != nil {
		policy.Enabled = *patch.Enabled
	}
	if patch.Mode != nil {
		policy.Mode = *patch.Mode
	}
	if patch.ScheduleIntervalMinutes != nil {
		policy.ScheduleIntervalMinutes = *patch.ScheduleIntervalMinutes
	}
	if patch.DatabaseTargetBytes != nil {
		policy.DatabaseTargetBytes = *patch.DatabaseTargetBytes
	}
	if patch.DatabaseWarningBytes != nil {
		policy.DatabaseWarningBytes = *patch.DatabaseWarningBytes
	}
	if patch.DatabaseActionBytes != nil {
		policy.DatabaseActionBytes = *patch.DatabaseActionBytes
	}
	if patch.DatabaseCriticalBytes != nil {
		policy.DatabaseCriticalBytes = *patch.DatabaseCriticalBytes
	}
	if patch.WarningForecastDays != nil {
		policy.WarningForecastDays = *patch.WarningForecastDays
	}
	if patch.ActionForecastDays != nil {
		policy.ActionForecastDays = *patch.ActionForecastDays
	}
	if patch.CriticalForecastHours != nil {
		policy.CriticalForecastHours = *patch.CriticalForecastHours
	}
	if patch.MaxRowsPerRun != nil {
		policy.MaxRowsPerRun = *patch.MaxRowsPerRun
	}
	if patch.MaxBytesPerRun != nil {
		policy.MaxBytesPerRun = *patch.MaxBytesPerRun
	}
	if patch.MaxActionsPerRun != nil {
		policy.MaxActionsPerRun = *patch.MaxActionsPerRun
	}
	if patch.ActionModes != nil {
		raw, _ := json.Marshal(patch.ActionModes)
		policy.ActionModes = datatypes.JSON(raw)
	}
	if patch.TrustMinDecisions != nil {
		policy.TrustMinDecisions = *patch.TrustMinDecisions
	}
	if patch.TrustMinAgreementPct != nil {
		policy.TrustMinAgreementPct = *patch.TrustMinAgreementPct
	}
	policy.NewsTimezone = retentionNewsTimezone(db, principal.TenantID)
	policy.PolicyVersion++
	policy.UpdatedBy = principal.Email
	if err := retentionPolicyValid(policy); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if policy.Mode == models.RetentionModeSafeAuto && !retentionSafeAutoPromotionReady(db, principal.TenantID, policy) {
		c.JSON(http.StatusConflict, gin.H{"error": "Safe Auto promotion requires trusted, breaker-closed Assist agreement for every eligible action class"})
		return
	}
	if err := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "tenant_id"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"enabled", "mode", "schedule_interval_minutes", "policy_version",
			"news_timezone", "database_target_bytes", "database_warning_bytes",
			"database_action_bytes", "database_critical_bytes", "warning_forecast_days",
			"action_forecast_days", "critical_forecast_hours", "max_rows_per_run",
			"max_bytes_per_run", "max_actions_per_run", "action_modes", "trust_min_decisions",
			"trust_min_agreement_pct", "updated_by", "updated_at",
		}),
	}).Create(&policy).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not save retention policy"})
		return
	}
	retentionAudit(db, principal, "retention.policy.update", policy.PublicID.String(), "success", nil)
	c.JSON(http.StatusOK, gin.H{"data": policy})
}

func runRetention(db *gorm.DB, tenant, trigger, createdBy string) (models.RetentionRun, error) {
	retentionRunMu.Lock()
	if retentionRunTenants[tenant] {
		retentionRunMu.Unlock()
		return models.RetentionRun{}, errRetentionBusy
	}
	retentionRunTenants[tenant] = true
	retentionRunMu.Unlock()
	defer func() {
		retentionRunMu.Lock()
		delete(retentionRunTenants, tenant)
		retentionRunMu.Unlock()
	}()

	policy := loadRetentionPolicy(db, tenant)
	policy.NewsTimezone = retentionNewsTimezone(db, tenant)
	now := time.Now().UTC()
	// A crashed replica must not leave the partial active-run lease wedged
	// forever. No content mutation can occur in this slice, so marking an
	// abandoned observation failed is the complete recovery action.
	staleBefore := now.Add(-15 * time.Minute)
	_ = db.Model(&models.RetentionRun{}).
		Where("tenant_id = ? AND lane = ? AND status = ? AND heartbeat_at < ?",
			tenant, "database", models.RetentionRunRunning, staleBefore).
		Updates(map[string]interface{}{
			"status": models.RetentionRunFailed, "verdict": models.RetentionVerdictInconclusive,
			"error_class": "stale_run_recovered", "error": "observation heartbeat expired",
			"finished_at": now,
		}).Error
	run := models.RetentionRun{
		TenantID: tenant, Lane: "database", Trigger: trigger, Mode: policy.Mode,
		Status: models.RetentionRunRunning, Verdict: models.RetentionVerdictInconclusive,
		PolicyVersion: policy.PolicyVersion, CorrelationID: uuid.New(),
		CreatedBy: createdBy, StartedAt: now, HeartbeatAt: now,
		BeforeEvidence:   datatypes.JSON([]byte(`{}`)),
		ForecastEvidence: datatypes.JSON([]byte(`{}`)),
		AfterEvidence:    datatypes.JSON([]byte(`{}`)),
		Counts:           datatypes.JSON([]byte(`{}`)), ErrorClass: "none",
	}
	if err := db.Create(&run).Error; err != nil {
		return run, errRetentionBusy
	}
	_ = ensureCurrentRetentionMonth(db, tenant, policy.NewsTimezone, now)

	finishFailure := func(class string, err error) (models.RetentionRun, error) {
		finished := time.Now().UTC()
		run.Status, run.ErrorClass, run.Error, run.FinishedAt = models.RetentionRunFailed, class, err.Error(), &finished
		_ = db.Model(&run).Updates(map[string]interface{}{
			"status": run.Status, "error_class": class, "error": run.Error,
			"finished_at": finished, "heartbeat_at": finished,
		}).Error
		return run, err
	}

	sample, err := collectRetentionDBSample(db, tenant)
	if err != nil {
		return finishFailure("sample_failed", err)
	}
	samples := retentionSamples(db, tenant, 200)
	forecast := calculateRetentionForecast(samples, policy)
	preview, err := previewRetentionNews(db, tenant, policy.NewsTimezone)
	if err != nil {
		return finishFailure("preview_failed", err)
	}
	run.Verdict = retentionVerdict(policy, &sample, forecast)
	beforeRaw, _ := json.Marshal(map[string]interface{}{
		"sample_id": sample.PublicID, "database_bytes": sample.DatabaseBytes,
		"provider_bytes": sample.ProviderBytes, "provider_source": sample.ProviderSource,
	})
	forecastRaw, _ := json.Marshal(forecast)
	countsRaw, _ := json.Marshal(preview)
	run.BeforeEvidence = datatypes.JSON(beforeRaw)
	run.ForecastEvidence = datatypes.JSON(forecastRaw)
	run.Counts = datatypes.JSON(countsRaw)
	var safeAutoAction *models.RetentionAction

	if preview.EligibleStories > 0 {
		evidence := map[string]interface{}{
			"preview": preview, "timezone": policy.NewsTimezone,
			"dormancy_days": 7, "destructive_execution_enabled": false,
			"required_preflight": []string{
				"protected-reference scan", "dependency scan", "snapshot invalidation",
				"manifest readback", "post-action feed integrity", "post-action system health",
			},
		}
		evidenceRaw, _ := json.Marshal(evidence)
		sum := sha256.Sum256(evidenceRaw)
		fingerprint := hex.EncodeToString(sum[:])
		action := models.RetentionAction{
			RunID: run.ID, TenantID: tenant, ActionClass: models.RetentionActionPreviewNewsCompaction,
			OwnerSystem: "news_database", TargetScope: "dormant_current_month_stories",
			Mode: policy.Mode, Decision: "shadow_preview", Outcome: models.RetentionActionWouldExecute,
			IdempotencyKey:      run.CorrelationID.String() + ":news-preview",
			EvidenceFingerprint: fingerprint, TargetCount: preview.CandidateRows,
			ProtectedCount: preview.ProtectedRows, EstimatedBytes: preview.EstimatedBytes,
			Guardrail: "observe_only_no_content_mutation", Evidence: datatypes.JSON(evidenceRaw),
			BeforeBytes: &sample.DatabaseBytes,
		}
		forecastAfter := sample.DatabaseBytes - minInt64(preview.EstimatedBytes, policy.MaxBytesPerRun)
		if forecastAfter < 0 {
			forecastAfter = 0
		}
		action.ForecastAfterBytes = &forecastAfter
		if err := db.Create(&action).Error; err != nil {
			return finishFailure("action_ledger_failed", err)
		}
	}

	// Slice 10's only promotable action is a bounded derived-state News
	// snapshot refresh. Observe writes the shadow decision, Assist asks for a
	// human decision, and Safe Auto can create a ready action only after the
	// same class has earned trust with a closed breaker. No canonical row,
	// source, object, or physical rewrite is eligible here.
	refreshWindows := retentionSnapshotRefreshWindows(db, tenant)
	if len(refreshWindows) > 0 {
		trusted := retentionSafeAutoPromotionReady(db, tenant, policy)
		action := buildRetentionSnapshotRefreshAction(run, policy, preview, refreshWindows, trusted)
		if err := db.Create(&action).Error; err != nil {
			return finishFailure("action_ledger_failed", err)
		}
		if action.Outcome == models.RetentionActionReady {
			safeAutoAction = &action
		}
	}

	finished := time.Now().UTC()
	run.Status, run.FinishedAt, run.HeartbeatAt = models.RetentionRunCompleted, &finished, finished
	if err := db.Model(&run).Updates(map[string]interface{}{
		"status": run.Status, "verdict": run.Verdict, "before_evidence": run.BeforeEvidence,
		"forecast_evidence": run.ForecastEvidence, "counts": run.Counts,
		"finished_at": finished, "heartbeat_at": finished,
	}).Error; err != nil {
		return run, err
	}
	if safeAutoAction != nil {
		if _, refreshErr := executeRetentionSnapshotRefresh(db, *safeAutoAction, "retention-autopilot"); refreshErr != nil {
			run.Status = models.RetentionRunPartial
			run.ErrorClass = "safe_auto_action_failed"
			run.Error = refreshErr.Error()
			_ = db.Model(&run).Updates(map[string]interface{}{"status": run.Status, "error_class": run.ErrorClass, "error": run.Error, "updated_at": time.Now().UTC()}).Error
		}
	}
	_ = db.Model(&models.RetentionPolicy{}).Where("tenant_id = ?", tenant).
		Update("last_run_at", finished).Error
	return run, nil
}

func ensureCurrentRetentionMonth(db *gorm.DB, tenant, timezone string, now time.Time) error {
	location, err := time.LoadLocation(timezone)
	if err != nil {
		location = time.FixedZone("Asia/Riyadh", 3*60*60)
	}
	local := now.In(location)
	// Store the local calendar label as a UTC-midnight DATE value. Passing
	// local midnight (+03) to a DATE column can otherwise cast to the previous
	// UTC day depending on the connection timezone.
	monthStart := time.Date(local.Year(), local.Month(), 1, 0, 0, 0, 0, time.UTC)
	month := models.RetentionMonth{
		TenantID: tenant, MonthStart: monthStart, State: "open",
		StateReason: "current calendar month",
	}
	return db.Where("tenant_id = ? AND month_start = ?", tenant, monthStart).
		FirstOrCreate(&month).Error
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func RunRetentionNow(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	run, err := runRetention(db, principal.TenantID, "manual", principal.Email)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, errRetentionBusy) {
			status = http.StatusConflict
		}
		c.JSON(status, gin.H{"error": err.Error()})
		return
	}
	retentionAudit(db, principal, "retention.run", run.PublicID.String(), "success",
		map[string]interface{}{"correlation_id": run.CorrelationID, "verdict": run.Verdict})
	c.JSON(http.StatusCreated, gin.H{"data": run})
}

func PauseRetention(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	var request struct {
		Minutes int `json:"minutes"`
	}
	if c.ShouldBindJSON(&request) != nil || request.Minutes < 1 || request.Minutes > 10080 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "minutes must be between 1 and 10080"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	policy := loadRetentionPolicy(db, principal.TenantID)
	until := time.Now().UTC().Add(time.Duration(request.Minutes) * time.Minute)
	policy.PausedUntil, policy.UpdatedBy = &until, principal.Email
	if err := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "tenant_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"paused_until", "updated_by", "updated_at"}),
	}).Create(&policy).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not pause retention"})
		return
	}
	retentionAudit(db, principal, "retention.pause", policy.PublicID.String(), "success",
		map[string]interface{}{"paused_until": until})
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"paused_until": until}})
}

func ListRetentionRuns(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var runs []models.RetentionRun
	if err := db.Where("tenant_id = ?", principal.TenantID).Order("started_at DESC").Limit(100).Find(&runs).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not list retention runs"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"items": runs}})
}

func GetRetentionRun(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid run id"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var run models.RetentionRun
	if db.Where("tenant_id = ? AND public_id = ?", principal.TenantID, id).First(&run).Error != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "retention run not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": run})
}

func ListRetentionRunActions(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	runID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid run id"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var run models.RetentionRun
	if db.Select("id").Where("tenant_id = ? AND public_id = ?", principal.TenantID, runID).First(&run).Error != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "retention run not found"})
		return
	}
	var actions []models.RetentionAction
	if err := db.Where("tenant_id = ? AND run_id = ?", principal.TenantID, run.ID).
		Order("created_at ASC").Find(&actions).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not list retention actions"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"items": actions}})
}

func ApproveRetentionAction(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid action id"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var action models.RetentionAction
	if db.Where("tenant_id = ? AND public_id = ?", principal.TenantID, id).First(&action).Error != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "retention action not found"})
		return
	}
	// The harness intentionally has no canonical-row executor. Approval cannot
	// turn an observe-only preview into an implicit mutation.
	if action.Outcome == models.RetentionActionWouldExecute || action.ActionClass == models.RetentionActionPreviewNewsCompaction {
		c.JSON(http.StatusConflict, gin.H{"error": "shadow previews cannot be approved; destructive execution is not installed"})
		return
	}
	now := time.Now().UTC()
	err = db.Transaction(func(tx *gorm.DB) error {
		var locked models.RetentionAction
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("tenant_id = ? AND public_id = ?", principal.TenantID, id).First(&locked).Error; err != nil {
			return err
		}
		if locked.Outcome != models.RetentionActionApprovalRequired {
			return fmt.Errorf("action is no longer awaiting approval")
		}
		result := tx.Model(&locked).Where("id = ? AND tenant_id = ? AND outcome = ?", locked.ID, principal.TenantID, models.RetentionActionApprovalRequired).
			Updates(map[string]interface{}{"outcome": models.RetentionActionApproved, "decision": "approved", "approved_at": now, "approved_by": principal.Email})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("action approval compare-and-set lost a race")
		}
		manifestHash := locked.ManifestHash
		decision := models.RetentionActionDecision{
			ActionID: locked.ID, TenantID: locked.TenantID, ActionClass: locked.ActionClass,
			Mode: locked.Mode, Decision: "approved", Actor: principal.Email,
			ManifestHash: manifestHash, EvidenceFingerprint: locked.EvidenceFingerprint,
			DecidedAt: now, Evidence: locked.Evidence,
		}
		if err := tx.Create(&decision).Error; err != nil {
			return err
		}
		if result := tx.Model(&models.RetentionCompactionManifest{}).Where("action_id = ? AND state = 'prepared'", locked.ID).
			Updates(map[string]interface{}{"state": "approved", "approved_at": now, "approved_by": principal.Email}); result.Error != nil {
			return result.Error
		}
		if result := tx.Model(&models.RetentionHistoricalManifest{}).Where("action_id = ? AND state = 'prepared'", locked.ID).
			Updates(map[string]interface{}{"state": "approved", "approved_at": now, "approved_by": principal.Email}); result.Error != nil {
			return result.Error
		}
		if result := tx.Model(&models.RetentionOwnerRequest{}).Where("action_id = ? AND status = ?", locked.ID, "approval_required").
			Update("status", "approved"); result.Error != nil {
			return result.Error
		}
		action = locked
		action.Outcome = models.RetentionActionApproved
		action.Decision = "approved"
		action.ApprovedAt = &now
		action.ApprovedBy = principal.Email
		return nil
	})
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	retentionAudit(db, principal, "retention.action.approve", action.PublicID.String(), "success", nil)
	c.JSON(http.StatusOK, gin.H{"data": action})
}

func RejectRetentionAction(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	var request struct {
		Reason string `json:"reason"`
	}
	if c.ShouldBindJSON(&request) != nil || strings.TrimSpace(request.Reason) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "reason is required"})
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid action id"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	now := time.Now().UTC()
	err = db.Transaction(func(tx *gorm.DB) error {
		var action models.RetentionAction
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("tenant_id = ? AND public_id = ?", principal.TenantID, id).First(&action).Error; err != nil {
			return err
		}
		if action.Outcome != models.RetentionActionApprovalRequired {
			return fmt.Errorf("action is not rejectable")
		}
		result := tx.Model(&action).Where("id = ? AND tenant_id = ? AND outcome = ?", action.ID, principal.TenantID, models.RetentionActionApprovalRequired).
			Updates(map[string]interface{}{"outcome": models.RetentionActionRejected, "decision": "rejected",
				"rejected_at": now, "rejected_by": principal.Email, "rejection_reason": request.Reason})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("action rejection compare-and-set lost a race")
		}
		manifestHash := action.ManifestHash
		return tx.Create(&models.RetentionActionDecision{
			ActionID: action.ID, TenantID: action.TenantID, ActionClass: action.ActionClass,
			Mode: action.Mode, Decision: "rejected", Actor: principal.Email,
			Reason: request.Reason, ManifestHash: manifestHash,
			EvidenceFingerprint: action.EvidenceFingerprint, DecidedAt: now,
			Evidence: action.Evidence,
		}).Error
	})
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"rejected_at": now}})
}

func ListRetentionMonths(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var months []models.RetentionMonth
	if err := db.Where("tenant_id = ?", principal.TenantID).Order("month_start DESC").Limit(36).Find(&months).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not list retention months"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"items": months}})
}

func ListRetentionHolds(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var holds []models.RetentionHold
	if err := db.Where("tenant_id = ? AND released_at IS NULL", principal.TenantID).
		Order("created_at DESC").Limit(200).Find(&holds).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not list retention holds"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"items": holds}})
}

func CreateRetentionHold(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	var request struct {
		TargetType string     `json:"target_type"`
		TargetID   uuid.UUID  `json:"target_id"`
		HoldClass  string     `json:"hold_class"`
		Reason     string     `json:"reason"`
		ExpiresAt  *time.Time `json:"expires_at"`
	}
	if c.ShouldBindJSON(&request) != nil || request.TargetID == uuid.Nil || strings.TrimSpace(request.Reason) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "target_type, target_id, hold_class, and reason are required"})
		return
	}
	if !containsString([]string{"content", "story", "month"}, request.TargetType) ||
		!containsString([]string{"manual", "editorial", "legal", "moderation", "recovery"}, request.HoldClass) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "unsupported hold target or class"})
		return
	}
	hold := models.RetentionHold{
		TenantID: principal.TenantID, TargetType: request.TargetType, TargetID: request.TargetID,
		HoldClass: request.HoldClass, Reason: strings.TrimSpace(request.Reason),
		CreatedBy: principal.Email, ExpiresAt: request.ExpiresAt,
	}
	db := c.MustGet("db").(*gorm.DB)
	if err := db.Create(&hold).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not create retention hold"})
		return
	}
	retentionAudit(db, principal, "retention.hold.create", hold.PublicID.String(), "success", nil)
	c.JSON(http.StatusCreated, gin.H{"data": hold})
}

func DeleteRetentionHold(c *gin.Context) {
	principal, ok := requireRetentionTenant(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid hold id"})
		return
	}
	var request struct {
		Reason string `json:"reason"`
	}
	_ = c.ShouldBindJSON(&request)
	if strings.TrimSpace(request.Reason) == "" {
		request.Reason = "released by administrator"
	}
	now := time.Now().UTC()
	db := c.MustGet("db").(*gorm.DB)
	result := db.Model(&models.RetentionHold{}).
		Where("tenant_id = ? AND public_id = ? AND released_at IS NULL", principal.TenantID, id).
		Updates(map[string]interface{}{"released_at": now, "released_by": principal.Email, "release_reason": request.Reason})
	if result.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not release retention hold"})
		return
	}
	if result.RowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "active retention hold not found"})
		return
	}
	retentionAudit(db, principal, "retention.hold.release", id.String(), "success", nil)
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"released_at": now}})
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func retentionAudit(db *gorm.DB, principal utils.AdminPrincipal, action, resource, status string, payload map[string]interface{}) {
	raw, _ := json.Marshal(payload)
	_ = db.Create(&models.AuditLog{
		TenantID: principal.TenantID, UserID: principal.UserID, UserEmail: principal.Email,
		Action: action, TargetService: "cms", TargetResource: resource, Status: status,
		Payload: datatypes.JSON(raw),
	}).Error
}
