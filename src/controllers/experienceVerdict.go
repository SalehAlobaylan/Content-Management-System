package controllers

import (
	"encoding/json"
	"fmt"
	"math"
	"time"

	"content-management-system/src/models"

	"gorm.io/gorm"
)

// Real User Experience — deterministic verdict + incident engine (plan §10).
// No opaque composite score decides health; verdicts are tied to named SLIs with
// absolute thresholds, a minimum sample floor, and confirm/resolve streaks.

type sliSpec struct {
	metric      string
	direction   string  // "min": value must be >= target; "max": value <= target
	target      float64 // ratio in [0,1]
	critical    bool    // a breach with sufficient volume can drive `critical`
	latencyP75  int     // optional p75 budget in ms (0 = none)
	label       string
	likelyOwner string
}

// V1 SLI registry — code defaults (Config Discipline). Tunable later via
// experience_policies.thresholds.
var experienceSLIs = []sliSpec{
	{mFeedRenderSuccess, "min", 0.995, true, 2500, "First usable feed", "feed_integrity"},
	{mPlaybackStartSuccess, "min", 0.985, true, 2000, "Playback start", "storage"},
	{mPlaybackFatalRate, "max", 0.01, false, 0, "Fatal playback", "storage"},
	{mPaginationSuccess, "min", 0.995, false, 0, "Pagination", "wahb_platform"},
	{mArticleReadySuccess, "min", 0.99, false, 0, "Article ready", "news"},
	{mHandoffSuccess, "min", 0.98, false, 0, "Playback handoff", "wahb_platform"},
	{mErrorFreeSessions, "min", 0.995, true, 0, "Error-free sessions", "wahb_platform"},
	{mFeedEmptyRate, "max", 0.02, false, 0, "Empty feed", "circulation"},
}

// sliStatus is the per-SLI outcome for one bucket/cohort.
type sliStatus struct {
	Metric      string  `json:"metric"`
	Label       string  `json:"label"`
	Value       float64 `json:"value"`
	Target      float64 `json:"target"`
	Denominator int64   `json:"denominator"`
	P75         int     `json:"p75_ms,omitempty"`
	P75Budget   int     `json:"p75_budget_ms,omitempty"`
	Status      string  `json:"status"` // ok | breach | latency_breach | insufficient
	Critical    bool    `json:"critical"`
	LikelyOwner string  `json:"likely_owner,omitempty"`
}

// evaluateSLI is a pure decision over one metric's counts. It never opens
// incidents; it only classifies.
func evaluateSLI(spec sliSpec, num, denom, samples, latencySum int64, latBuckets [7]int64, minSampleFloor int64) sliStatus {
	st := sliStatus{
		Metric: spec.metric, Label: spec.label, Target: spec.target,
		Denominator: denom, Critical: spec.critical, LikelyOwner: spec.likelyOwner, P75Budget: spec.latencyP75,
	}
	if denom < minSampleFloor {
		st.Status = "insufficient"
		return st
	}
	value := float64(num) / float64(denom)
	st.Value = math.Round(value*10000) / 10000

	breach := false
	if spec.direction == "min" && value < spec.target {
		breach = true
	} else if spec.direction == "max" && value > spec.target {
		breach = true
	}
	if breach {
		st.Status = "breach"
		return st
	}
	// Latency companion check (uses success-terminal samples).
	if spec.latencyP75 > 0 && samples > 0 {
		st.P75 = p75FromBuckets(latBuckets)
		if st.P75 > spec.latencyP75 {
			st.Status = "latency_breach"
			return st
		}
	}
	st.Status = "ok"
	return st
}

// rollupValues loads the global-cohort rollup counts for a metric/surface/bucket.
func loadRollup(db *gorm.DB, tenantID, metric, surface, dim, val string, bucket time.Time) (num, denom, samples, latSum int64, lat [7]int64, found bool) {
	var row models.ExperienceMetricRollup
	err := db.Where("tenant_id = ? AND bucket_start = ? AND resolution = ? AND metric_key = ? AND surface = ? AND cohort_dim = ? AND cohort_val = ?",
		tenantID, bucket, rollupResolution, metric, surface, dim, val).First(&row).Error
	if err != nil {
		return 0, 0, 0, 0, lat, false
	}
	return row.Numerator, row.Denominator, row.SampleCount, row.LatencySum, parseLatBuckets(row.LatencyBuckets), true
}

// surfaceVerdict folds the global SLI statuses into one surface verdict.
func surfaceVerdict(statuses []sliStatus, telemetryFresh bool) string {
	if !telemetryFresh {
		return models.RuxVerdictTelemetryDegrade
	}
	anyData := false
	worst := models.RuxVerdictHealthy
	rank := map[string]int{
		models.RuxVerdictHealthy: 0, models.RuxVerdictWatching: 1,
		models.RuxVerdictDegraded: 2, models.RuxVerdictCritical: 3,
	}
	for _, s := range statuses {
		if s.Status == "insufficient" {
			continue
		}
		anyData = true
		var v string
		switch s.Status {
		case "ok":
			v = models.RuxVerdictHealthy
		case "latency_breach":
			v = models.RuxVerdictWatching
		case "breach":
			if s.Critical {
				v = models.RuxVerdictCritical
			} else {
				v = models.RuxVerdictDegraded
			}
		}
		if rank[v] > rank[worst] {
			worst = v
		}
	}
	if !anyData {
		return models.RuxVerdictInsufficient
	}
	return worst
}

// RunExperienceEvaluation is the top-level pass: roll up closed buckets since the
// checkpoint, evaluate the newly-closed buckets in order (advancing incident
// streaks exactly once per bucket), and write the run + ledger. Single-flight is
// enforced by the caller (scheduler mutex / manual 409).
func RunExperienceEvaluation(db *gorm.DB, tenantID, trigger string) (*models.ExperienceEvaluationRun, error) {
	policy := getOrCreateExperiencePolicy(db, tenantID)
	now := time.Now().UTC()

	run := models.ExperienceEvaluationRun{
		TenantID: tenantID, Trigger: trigger, Status: models.RuxRunRunning, StartedAt: now,
	}
	if err := db.Create(&run).Error; err != nil {
		return nil, err
	}

	// Determine the closed-bucket range to process.
	currentBucket := floorBucket(now) // in-progress hour, excluded
	var startBucket time.Time
	if policy.LastEvaluatedBucket != nil {
		startBucket = floorBucket(*policy.LastEvaluatedBucket).Add(rollupBucketDuration)
	} else {
		startBucket = currentBucket.Add(-rollupBucketDuration) // first run: last closed hour only
	}

	keptReleases := keptReleaseSet(db, tenantID, policy.MaxReleaseCohorts, now)
	const scanCap = 200000

	bucketsProcessed := 0
	partial := false
	var lastBucketVerdicts map[string]any
	telemetryFresh := isTelemetryFresh(db, tenantID, policy, now)

	for b := startBucket; b.Before(currentBucket); b = b.Add(rollupBucketDuration) {
		if bucketsProcessed >= policy.RollupMaxBucketsPerPass {
			partial = true
			break
		}
		if _, err := rollupBucket(db, tenantID, b, keptReleases, scanCap); err != nil {
			finishExperienceRun(db, &run, models.RuxRunFailed, bucketsProcessed, nil, telemetryFresh, "rollup_error", err.Error())
			return &run, err
		}
		verdicts := evaluateBucketAndUpdateIncidents(db, tenantID, policy, &run, b, telemetryFresh)
		lastBucketVerdicts = verdicts
		bucketsProcessed++
		// Advance the checkpoint after each fully-processed bucket.
		policy.LastEvaluatedBucket = &b
		db.Model(&models.ExperiencePolicy{}).Where("tenant_id = ?", tenantID).Update("last_evaluated_bucket", b)
	}

	status := models.RuxRunCompleted
	if partial {
		status = models.RuxRunPartial
	}
	if bucketsProcessed == 0 {
		// Nothing new to close; still recompute a display verdict for the last
		// closed bucket so the cockpit reflects current state.
		lastClosed := currentBucket.Add(-rollupBucketDuration)
		lastBucketVerdicts = computeDisplayVerdicts(db, tenantID, policy, lastClosed, telemetryFresh)
	}
	finishExperienceRun(db, &run, status, bucketsProcessed, lastBucketVerdicts, telemetryFresh, "none", "")
	return &run, nil
}

func isTelemetryFresh(db *gorm.DB, tenantID string, policy models.ExperiencePolicy, now time.Time) bool {
	var latest models.ExperienceEvent
	if err := db.Where("tenant_id = ?", tenantID).Order("received_at DESC").First(&latest).Error; err != nil {
		return false // no events at all → not fresh (insufficient/telemetry_degraded)
	}
	return now.Sub(latest.ReceivedAt.UTC()) <= time.Duration(policy.TelemetryFreshnessMinutes)*time.Minute
}

// computeDisplayVerdicts recomputes surface verdicts for a bucket WITHOUT
// advancing incident streaks (used when no new bucket closed this run).
func computeDisplayVerdicts(db *gorm.DB, tenantID string, policy models.ExperiencePolicy, bucket time.Time, telemetryFresh bool) map[string]any {
	out := map[string]any{}
	for _, surface := range enabledSurfaces(policy) {
		statuses := globalSLIStatuses(db, tenantID, policy, surface, bucket)
		out[surface] = map[string]any{
			"verdict": surfaceVerdict(statuses, telemetryFresh),
			"slis":    statuses,
		}
	}
	return out
}

func globalSLIStatuses(db *gorm.DB, tenantID string, policy models.ExperiencePolicy, surface string, bucket time.Time) []sliStatus {
	floor := int64(policy.MinSampleFloor)
	var out []sliStatus
	for _, spec := range experienceSLIs {
		num, denom, samples, latSum, lat, _ := loadRollup(db, tenantID, spec.metric, surface, "global", "all", bucket)
		out = append(out, evaluateSLI(spec, num, denom, samples, latSum, lat, floor))
	}
	return out
}

// criticalCohortStatuses evaluates only the fixed, low-cardinality cohorts
// that can explain a broad critical regression. Global verdicts remain the
// cockpit headline; these statuses create correctly scoped incidents.
func criticalCohortStatuses(db *gorm.DB, tenantID string, policy models.ExperiencePolicy, surface string, bucket time.Time) []struct {
	dim string
	val string
	st  sliStatus
} {
	critical := map[string]bool{mFeedRenderSuccess: true, mPlaybackStartSuccess: true, mErrorFreeSessions: true}
	var rows []struct {
		MetricKey string
		CohortDim string
		CohortVal string
	}
	db.Model(&models.ExperienceMetricRollup{}).
		Select("DISTINCT metric_key, cohort_dim, cohort_val").
		Where("tenant_id = ? AND bucket_start = ? AND resolution = ? AND surface = ? AND cohort_dim IN ?", tenantID, bucket, rollupResolution, surface, []string{"release", "playback_type", "browser"}).
		Scan(&rows)

	floor := int64(policy.MinSampleFloor)
	out := make([]struct {
		dim string
		val string
		st  sliStatus
	}, 0, len(rows))
	for _, row := range rows {
		if !critical[row.MetricKey] {
			continue
		}
		for _, spec := range experienceSLIs {
			if spec.metric != row.MetricKey {
				continue
			}
			n, d, s, sum, lat, _ := loadRollup(db, tenantID, spec.metric, surface, row.CohortDim, row.CohortVal, bucket)
			out = append(out, struct {
				dim string
				val string
				st  sliStatus
			}{row.CohortDim, row.CohortVal, evaluateSLI(spec, n, d, s, sum, lat, floor)})
		}
	}
	return out
}

// evaluateBucketAndUpdateIncidents evaluates the global SLIs (and critical-SLI
// cohorts) for one newly-closed bucket, advancing incident streaks once.
func evaluateBucketAndUpdateIncidents(db *gorm.DB, tenantID string, policy models.ExperiencePolicy, run *models.ExperienceEvaluationRun, bucket time.Time, telemetryFresh bool) map[string]any {
	out := map[string]any{}
	for _, surface := range enabledSurfaces(policy) {
		statuses := globalSLIStatuses(db, tenantID, policy, surface, bucket)
		out[surface] = map[string]any{
			"verdict": surfaceVerdict(statuses, telemetryFresh),
			"slis":    statuses,
		}
		if !telemetryFresh {
			continue // withhold incident changes on degraded telemetry
		}
		for _, st := range statuses {
			breached := st.Status == "breach" || st.Status == "latency_breach"
			if st.Status == "insufficient" {
				continue
			}
			updateIncident(db, tenantID, policy, run, surface, "global", "all", st, breached, bucket)
		}
		for _, cohort := range criticalCohortStatuses(db, tenantID, policy, surface, bucket) {
			if cohort.st.Status == "insufficient" {
				continue
			}
			breached := cohort.st.Status == "breach" || cohort.st.Status == "latency_breach"
			updateIncident(db, tenantID, policy, run, surface, cohort.dim, cohort.val, cohort.st, breached, bucket)
		}
	}
	return out
}

// updateIncident advances the confirm/resolve streaks for one SLI fingerprint
// and opens/recovers/resolves the incident, honoring active suppressions.
func updateIncident(db *gorm.DB, tenantID string, policy models.ExperiencePolicy, run *models.ExperienceEvaluationRun, surface, dim, val string, st sliStatus, breached bool, bucket time.Time) {
	fingerprint := fmt.Sprintf("%s:%s:%s:%s", st.Metric, surface, dim, val)
	severity := models.RuxSeverityDegraded
	if st.Status == "latency_breach" {
		severity = models.RuxSeverityWatching
	} else if st.Critical {
		severity = models.RuxSeverityCritical
	}

	var inc models.ExperienceIncident
	err := db.Where("tenant_id = ? AND fingerprint = ? AND status IN ?", tenantID, fingerprint, []string{models.RuxIncidentOpen, models.RuxIncidentRecovering}).First(&inc).Error
	open := err == nil

	if breached {
		if suppressed(db, tenantID, st.Metric, surface, dim, val) {
			writeExperienceAction(db, tenantID, run, nil, "finding_suppressed", fmt.Sprintf("%s breach suppressed", st.Label), st, "suppressed", bucket)
			return
		}
		if open {
			inc.ViolationStreak++
			inc.CleanStreak = 0
			inc.LastSeenAt = bucket
			inc.Severity = severity
			inc.Status = models.RuxIncidentOpen
			inc.RecoveringSince = nil
			inc.Evidence = incidentEvidence(st, inc.ViolationStreak)
			db.Save(&inc)
			writeExperienceAction(db, tenantID, run, &inc.ID, "incident_updated", fmt.Sprintf("%s still breached (streak %d)", st.Label, inc.ViolationStreak), st, "", bucket)
		} else {
			inc = models.ExperienceIncident{
				TenantID: tenantID, Fingerprint: fingerprint, MetricKey: st.Metric, Surface: surface,
				CohortDim: dim, CohortVal: val, Severity: severity, Status: models.RuxIncidentOpen,
				Summary:  fmt.Sprintf("%s %s on %s", st.Label, st.Status, surface),
				Evidence: incidentEvidence(st, 1), Recommendation: recommendationFor(st, surface),
				LikelyOwner: st.LikelyOwner, ViolationStreak: 1, CleanStreak: 0,
				FirstSeenAt: bucket, LastSeenAt: bucket,
			}
			db.Create(&inc)
			// The confirm window gates whether this is alert-worthy yet.
			if inc.ViolationStreak >= policy.ConfirmWindows || st.Critical {
				writeExperienceAction(db, tenantID, run, &inc.ID, "incident_opened", fmt.Sprintf("%s breached on %s", st.Label, surface), st, "", bucket)
			} else {
				writeExperienceAction(db, tenantID, run, &inc.ID, "incident_watching", fmt.Sprintf("%s regressing on %s (confirming)", st.Label, surface), st, "", bucket)
			}
		}
	} else if open {
		inc.CleanStreak++
		inc.ViolationStreak = 0
		inc.LastSeenAt = bucket
		if inc.RecoveringSince == nil {
			t := bucket
			inc.RecoveringSince = &t
			inc.Status = models.RuxIncidentRecovering
			writeExperienceAction(db, tenantID, run, &inc.ID, "incident_recovering", fmt.Sprintf("%s recovering on %s", st.Label, surface), st, "", bucket)
		}
		if inc.CleanStreak >= policy.ResolveWindows {
			t := bucket
			inc.Status = models.RuxIncidentResolved
			inc.ResolvedAt = &t
			writeExperienceAction(db, tenantID, run, &inc.ID, "incident_resolved", fmt.Sprintf("%s resolved on %s after %d clean windows", st.Label, surface, inc.CleanStreak), st, "", bucket)
		}
		db.Save(&inc)
	}
}

func incidentEvidence(st sliStatus, streak int) []byte {
	b, _ := json.Marshal(map[string]any{
		"value": st.Value, "target": st.Target, "denominator": st.Denominator,
		"p75_ms": st.P75, "p75_budget_ms": st.P75Budget, "status": st.Status, "violation_streak": streak,
	})
	return b
}

func recommendationFor(st sliStatus, surface string) string {
	switch st.LikelyOwner {
	case "feed_integrity":
		return "Check Feed Integrity for a serving-path or contract regression."
	case "storage":
		return "Check Storage/Quality and Feed Integrity playback probes for dead or malformed media."
	case "news":
		return "Check News circulation / snapshot freshness for this surface."
	case "circulation":
		return "Check Media Circulation intake — inventory may be thin."
	default:
		return "Inspect the Wahb-Platform client for a release/browser regression."
	}
}

func suppressed(db *gorm.DB, tenantID, metric, surface, dim, val string) bool {
	now := time.Now()
	var count int64
	db.Model(&models.ExperienceSuppression{}).
		Where("tenant_id = ? AND revoked_at IS NULL AND starts_at <= ? AND expires_at > ?", tenantID, now, now).
		Where("(metric_key = ? OR metric_key IS NULL OR metric_key = '')", metric).
		Where("(surface = ? OR surface IS NULL OR surface = '')", surface).
		Where("(cohort_dim = ? OR cohort_dim IS NULL OR cohort_dim = '')", dim).
		Where("(cohort_val = ? OR cohort_val IS NULL OR cohort_val = '')", val).
		Count(&count)
	return count > 0
}

func writeExperienceAction(db *gorm.DB, tenantID string, run *models.ExperienceEvaluationRun, incidentID *uint, class, label string, st sliStatus, guardrail string, bucket time.Time) {
	ev, _ := json.Marshal(map[string]any{
		"value": st.Value, "target": st.Target, "denominator": st.Denominator, "bucket": bucketLabel(bucket),
	})
	var runID *uint
	if run != nil {
		runID = &run.ID
	}
	db.Create(&models.ExperienceAction{
		TenantID: tenantID, RunID: runID, IncidentID: incidentID, ActionClass: class, Label: label,
		MetricKey: st.Metric, Guardrail: guardrail, Evidence: ev,
	})
}

func finishExperienceRun(db *gorm.DB, run *models.ExperienceEvaluationRun, status string, buckets int, verdicts map[string]any, telemetryFresh bool, errClass, errMsg string) {
	now := time.Now()
	run.Status = status
	run.FinishedAt = &now
	run.BucketsProcessed = buckets
	run.TelemetryFresh = telemetryFresh
	run.ErrorClass = errClass
	run.Error = errMsg
	if verdicts != nil {
		vb, _ := json.Marshal(verdicts)
		run.SurfaceVerdicts = vb
		run.Summary = summarizeVerdicts(verdicts)
	}
	db.Save(run)
}

func summarizeVerdicts(verdicts map[string]any) string {
	parts := ""
	for surface, v := range verdicts {
		if m, ok := v.(map[string]any); ok {
			parts += fmt.Sprintf("%s=%v ", surface, m["verdict"])
		}
	}
	return parts
}

func enabledSurfaces(policy models.ExperiencePolicy) []string {
	// V1 fixed set; enabled_surfaces string could narrow it later.
	return []string{"foryou", "news"}
}
