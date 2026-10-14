package controllers

import (
	"content-management-system/src/models"
	"encoding/json"
	"errors"
	"fmt"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"math"
	"strings"
	"time"
)

type sliSpec struct {
	metric, direction  string
	target             float64
	critical           bool
	latencyP75         int
	label, likelyOwner string
}

var experienceSLIs = []sliSpec{
	{mFeedRenderSuccess, "min", .995, false, 2500, "First usable feed", "feed_integrity"},
	{mPlaybackStartSuccess, "min", .985, false, 2000, "Playback start", "storage"},
	{mPlaybackFatalRate, "max", .01, false, 0, "Fatal startup", "storage"},
	{mAutoplayBlockedRate, "max", 1, false, 0, "Automatic play blocked", "wahb_platform"},
	{mContinuationFatalRate, "max", .01, false, 0, "Listening failures", "storage"},
	{mStallIncidence, "max", .05, false, 0, "Listening with stalls", "storage"},
	{mRebufferRatio, "max", .02, false, 0, "Rebuffer time", "storage"},
	{mFallbackUse, "max", .1, false, 0, "Fallback use", "storage"},
	{mContinuationFallbackUse, "max", .1, false, 0, "Listening fallback use", "storage"},
	{mFallbackSuccess, "min", .98, false, 0, "Fallback recovery", "storage"},
	{mPaginationSuccess, "min", .995, false, 0, "Pagination delivery", "wahb_platform"},
	{mPaginationStarvation, "max", .005, false, 0, "Loaded boundary starvation", "wahb_platform"},
	{mArticleReadySuccess, "min", .99, false, 2500, "Article ready", "news"},
	{mHandoffSuccess, "min", .98, false, 0, "Playback handoff", "wahb_platform"},
	{mHandoffDrift, "max", .01, false, 0, "Handoff position drift", "wahb_platform"},
	{mErrorFreeSessions, "min", .995, false, 0, "Error-free sessions", "wahb_platform"},
	{mFeedEmptyRate, "max", .02, false, 0, "Empty feed", "circulation"},
}

type sliStatus struct {
	ClientPlatform   string  `json:"client_platform"`
	Surface          string  `json:"surface,omitempty"`
	CohortDim        string  `json:"cohort_dim,omitempty"`
	CohortVal        string  `json:"cohort_val,omitempty"`
	Metric           string  `json:"metric"`
	Label            string  `json:"label"`
	Value            float64 `json:"value"`
	Target           float64 `json:"target"`
	Numerator        int64   `json:"numerator"`
	Denominator      int64   `json:"denominator"`
	Samples          int64   `json:"latency_samples"`
	ActiveSessions   int64   `json:"active_sessions"`
	AffectedSessions int64   `json:"affected_sessions"`
	Population       string  `json:"population"`
	P75              int     `json:"p75_ms,omitempty"`
	P75Budget        int     `json:"p75_budget_ms,omitempty"`
	EvidenceReason   string  `json:"evidence_reason,omitempty"`
	Status           string  `json:"status"`
	Critical         bool    `json:"critical"`
	Required         bool    `json:"required"`
	Confirmed        bool    `json:"confirmed"`
	LikelyOwner      string  `json:"likely_owner,omitempty"`
}

func metricPopulation(metric string) string {
	if metric == mErrorFreeSessions {
		return "active tabs"
	}
	if metric == mRebufferRatio {
		return "observed listening milliseconds"
	}
	return "eligible journey outcomes"
}
func evaluateSLI(spec sliSpec, num, denom, samples, latencySum int64, latBuckets [8]int64, minSampleFloor int64, latencyFloor ...int64) sliStatus {
	st := sliStatus{Metric: spec.metric, Label: spec.label, Target: spec.target, Numerator: num, Denominator: denom, Samples: samples, LikelyOwner: spec.likelyOwner, P75Budget: spec.latencyP75, Population: metricPopulation(spec.metric)}
	if denom < minSampleFloor || denom <= 0 || num < 0 || num > denom {
		st.Status = "insufficient"
		return st
	}
	value := float64(num) / float64(denom)
	st.Value = math.Round(value*10000) / 10000
	if spec.direction == "min" && value < spec.target || spec.direction == "max" && value > spec.target {
		st.Status = "breach"
		return st
	}
	if spec.latencyP75 > 0 {
		sampleFloor := minSampleFloor
		if len(latencyFloor) > 0 {
			sampleFloor = latencyFloor[0]
		}
		if samples < sampleFloor {
			st.Status = "insufficient"
			return st
		}
		var histogramSamples int64
		for _, count := range latBuckets {
			if count < 0 {
				st.Status, st.EvidenceReason = "insufficient", "latency_histogram_invalid"
				return st
			}
			histogramSamples += count
		}
		if histogramSamples != samples {
			st.Status, st.EvidenceReason = "insufficient", "latency_histogram_incomplete"
			return st
		}
		st.P75 = p75FromBuckets(latBuckets)
		if st.P75 > spec.latencyP75 {
			// Histograms report an upper bound. If the configured budget lies
			// inside the percentile's bin, it cannot establish a breach.
			lower := 0
			for _, bound := range latencyBounds {
				if int(bound) >= st.P75 {
					break
				}
				lower = int(bound)
			}
			if spec.latencyP75 > lower {
				st.Status, st.EvidenceReason = "insufficient", "latency_histogram_resolution"
				return st
			}
			st.Status = "latency_breach"
			return st
		}
	}
	st.Status = "ok"
	return st
}
func requiredMetric(surface, metric string) bool {
	return metric == mFeedRenderSuccess || metric == mErrorFreeSessions || surface == "pods" && (metric == mPlaybackStartSuccess || metric == mPlaybackFatalRate) || surface == "news" && metric == mArticleReadySuccess
}
func appliesMetric(surface, metric string) bool {
	if surface == "news" {
		return metric == mFeedRenderSuccess || metric == mFeedEmptyRate || metric == mErrorFreeSessions || metric == mArticleReadySuccess || metric == mPaginationSuccess || metric == mPaginationStarvation
	}
	return metric != mArticleReadySuccess
}
func policySpec(policy models.ExperiencePolicy, spec sliSpec) sliSpec {
	var overrides map[string]struct {
		Target     *float64 `json:"target"`
		LatencyP75 *int     `json:"latency_p75_ms"`
	}
	if json.Unmarshal(policy.Thresholds, &overrides) == nil {
		if v, ok := overrides[spec.metric]; ok {
			if v.Target != nil {
				spec.target = *v.Target
			}
			if v.LatencyP75 != nil {
				spec.latencyP75 = *v.LatencyP75
			}
		}
	}
	return spec
}
func statusesFromRows(db *gorm.DB, tenant string, policy models.ExperiencePolicy, surface, dim, val string, bucket time.Time, clients ...string) ([]sliStatus, error) {
	var rows []models.ExperienceMetricRollup
	if err := db.Where("tenant_id=? AND bucket_start=? AND resolution='hour' AND completed=true AND surface=? AND cohort_dim=? AND cohort_val=?", tenant, bucket, surface, dim, val).Where("client_platform=?", experienceClient(clients)).Find(&rows).Error; err != nil {
		return nil, err
	}
	byMetric := map[string]models.ExperienceMetricRollup{}
	for _, r := range rows {
		byMetric[r.MetricKey] = r
	}
	var out []sliStatus
	for _, base := range experienceSLIs {
		if !appliesMetric(surface, base.metric) {
			continue
		}
		spec := policySpec(policy, base)
		row := byMetric[spec.metric]
		samples := row.SampleCount
		if row.HistogramVersion != 2 {
			samples = 0
		}
		st := evaluateSLI(spec, row.Numerator, row.Denominator, samples, row.LatencySum, parseLatBuckets(row.LatencyBuckets), int64(policy.MinSampleFloor), int64(policy.LatencySampleFloor))
		if spec.metric == mRebufferRatio && row.OutcomeCount < int64(policy.MinSampleFloor) {
			st.Status = "insufficient"
		}
		st.ClientPlatform = experienceClient(clients)
		if st.ClientPlatform != "web" && st.LikelyOwner == "wahb_platform" {
			st.LikelyOwner = "wahb_mobile"
		}
		if st.ClientPlatform != "web" && st.Metric == mErrorFreeSessions {
			st.Population = "active app sessions"
		}
		st.Required = requiredMetric(surface, spec.metric)
		st.ActiveSessions = row.ActiveSessions
		st.AffectedSessions = row.AffectedSessions
		if row.Denominator == 0 && !st.Required {
			st.Status = "not_exercised"
		}
		if spec.latencyP75 > 0 && (st.Status == "ok" || st.Status == "latency_breach") && samples < int64(policy.LatencySampleFloor) {
			st.Status = "insufficient"
		}
		out = append(out, st)
	}
	return out, nil
}
func surfaceVerdict(statuses []sliStatus, fresh bool) string {
	if !fresh {
		return models.RuxVerdictTelemetryDegrade
	}
	insufficient := false
	worst := models.RuxVerdictHealthy
	any := false
	for _, s := range statuses {
		if s.Status == "insufficient" && (s.Required || s.Metric == "") {
			insufficient = true
		}
		if s.Status == "ok" {
			any = true
		}
		if s.Status == "breach" || s.Status == "latency_breach" {
			worst = models.RuxVerdictWatching
			if s.Confirmed {
				return models.RuxVerdictDegraded
			}
		}
	}
	if insufficient || !any && worst == models.RuxVerdictHealthy {
		return models.RuxVerdictInsufficient
	}
	return worst
}
func enabledSurfaces(policy models.ExperiencePolicy) []string {
	var out []string
	for _, s := range strings.Split(policy.EnabledSurfaces, ",") {
		if models.RuxSurfaces[s] {
			out = append(out, s)
		}
	}
	return out
}
func surfaceFresh(db *gorm.DB, tenant, surface string, policy models.ExperiencePolicy, now time.Time, clients ...string) (bool, error) {
	var latest models.ExperienceEvent
	err := db.Where("tenant_id=? AND surface=? AND schema_version IN (2,3)", tenant, surface).Where("client_platform=?", experienceClient(clients)).Order("received_at DESC,id DESC").First(&latest).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return now.Sub(latest.ReceivedAt.UTC()) <= time.Duration(policy.TelemetryFreshnessMinutes)*time.Minute, nil
}
func isTelemetryFresh(db *gorm.DB, tenant string, policy models.ExperiencePolicy, now time.Time, clients ...string) (bool, error) {
	allFresh := len(enabledSurfaces(policy)) > 0
	for _, s := range enabledSurfaces(policy) {
		fresh, err := surfaceFresh(db, tenant, s, policy, now, experienceClient(clients))
		if err != nil {
			return false, err
		}
		allFresh = allFresh && fresh
	}
	return allFresh, nil
}

// Active intent coverage is correlated by phase identity, never wall-clock
// latency. A late terminal may arrive next window; until then withhold health.
func bucketIntegrity(db *gorm.DB, tenant, surface string, bucket time.Time, clients ...string) (bool, error) {
	var conflicts int64
	if err := db.Model(&models.ExperienceEvent{}).Where("tenant_id=? AND surface=? AND received_at>=? AND received_at<? AND terminal_status='conflict'", tenant, surface, bucket, bucket.Add(time.Hour)).Where("client_platform=?", experienceClient(clients)).Count(&conflicts).Error; err != nil {
		return false, err
	}
	if conflicts > 0 {
		return false, nil
	}
	var missing int64
	err := db.Raw(`SELECT COUNT(*) FROM experience_events s WHERE s.tenant_id=? AND s.surface=? AND s.schema_version IN (2,3) AND s.client_platform=? AND s.received_at>=? AND s.received_at<? AND s.event_type IN ('feed_requested','pagination_requested','playback_attempted','handoff_started','article_opened','playback_fallback') AND s.visible=true AND NOT EXISTS (SELECT 1 FROM experience_events t WHERE t.tenant_id=s.tenant_id AND t.client_platform=s.client_platform AND t.session_id=s.session_id AND t.page_load_id=s.page_load_id AND t.journey_id=s.journey_id AND t.phase=CASE s.event_type WHEN 'feed_requested' THEN 'feed' WHEN 'pagination_requested' THEN 'pagination' WHEN 'playback_attempted' THEN 'startup' WHEN 'handoff_started' THEN 'handoff' WHEN 'article_opened' THEN 'reader' WHEN 'playback_fallback' THEN 'fallback' END AND t.terminal_status='accepted')`, tenant, surface, experienceClient(clients), bucket, bucket.Add(time.Hour)).Scan(&missing).Error
	return missing == 0, err
}
func computeDisplayVerdicts(db *gorm.DB, tenant string, policy models.ExperiencePolicy, bucket time.Time, fresh bool, clients ...string) (map[string]any, error) {
	out := map[string]any{}
	for _, surface := range enabledSurfaces(policy) {
		statuses, err := statusesFromRows(db, tenant, policy, surface, "global", "all", bucket, experienceClient(clients))
		if err != nil {
			return nil, err
		}
		sf, err := surfaceFresh(db, tenant, surface, policy, time.Now().UTC(), experienceClient(clients))
		if err != nil {
			return nil, err
		}
		integrity, err := bucketIntegrity(db, tenant, surface, bucket, experienceClient(clients))
		if err != nil {
			return nil, err
		}
		var incidents []models.ExperienceIncident
		if err := db.Where("tenant_id=? AND surface=? AND status IN ('open','recovering') AND confirmed_at IS NOT NULL", tenant, surface).Where("client_platform=?", experienceClient(clients)).Find(&incidents).Error; err != nil {
			return nil, err
		}
		for i := range statuses {
			for _, inc := range incidents {
				if inc.MetricKey == statuses[i].Metric {
					statuses[i].Confirmed = true
				}
			}
		}
		v := surfaceVerdict(statuses, fresh && sf && integrity)
		if !experienceClientEnabled(policy, experienceClient(clients)) {
			v = models.RuxVerdictInsufficient
		}
		for _, inc := range incidents {
			if v == "healthy" || v == "watching" {
				v = models.RuxVerdictDegraded
			}
			_ = inc
		}
		out[surface] = map[string]any{"verdict": v, "slis": statuses, "window_start": bucket, "window_end": bucket.Add(time.Hour), "telemetry_fresh": sf, "terminal_coverage_complete": integrity}
	}
	return out, nil
}

// Every page is bounded. Policy row locking serializes durable cursor work
// across processes; the final page, incident transitions, ledger and completed
// checkpoint commit together. A replay or another runner cannot advance twice.
func RunExperienceEvaluation(db *gorm.DB, tenant, trigger string) (*models.ExperienceEvaluationRun, error) {
	policy, err := loadExperiencePolicy(db, tenant)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	run := models.ExperienceEvaluationRun{TenantID: tenant, Trigger: trigger, Status: models.RuxRunRunning, StartedAt: now}
	if err := db.Create(&run).Error; err != nil {
		return nil, err
	}
	pages := 0
	buckets := 0
	partial := false
	// Minute trends are subordinate to hourly decisions and have their own cursor.
	for _, resolution := range []string{"hour", "minute"} {
		step := time.Hour
		if resolution == "minute" {
			step = time.Minute
		}
		end := now.Add(-time.Minute).Truncate(step)
		for buckets < policy.RollupMaxBucketsPerPass && pages < experiencePagesPerPass {
			completed := false
			var committedStart *time.Time
			noWork := false
			err = db.Transaction(func(tx *gorm.DB) error {
				var locked models.ExperiencePolicy
				if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=?", tenant).First(&locked).Error; err != nil {
					return err
				}
				checkpoint := locked.LastEvaluatedBucket
				if resolution == "minute" {
					checkpoint = locked.LastMinuteBucket
				}
				start := end.Add(-step)
				if checkpoint != nil {
					start = checkpoint.UTC().Truncate(step).Add(step)
				}
				if !start.Before(end) {
					noWork = true
					return nil
				}
				// Expired unevaluated windows are durable gaps. Never pretend the swept
				// horizon was healthy; interruption applies to every active fingerprint.
				cutoff := now.AddDate(0, 0, -locked.RawRetentionDays).Truncate(step)
				if start.Before(cutoff) {
					if err := tx.Create(&models.ExperienceAction{TenantID: tenant, RunID: &run.ID, ActionClass: "evidence_gap", Label: "Unevaluated evidence exceeded raw retention; health withheld", Guardrail: "raw_retention_horizon", Evidence: jsonBytes(map[string]any{"window_start": start, "window_end": cutoff})}).Error; err != nil {
						return err
					}
					if resolution == "hour" {
						if err := interruptIncidents(tx, tenant, "", start); err != nil {
							return err
						}
					}
					start = cutoff
				}
				kept, err := keptReleaseSet(tx, tenant, locked.MaxReleaseCohorts, start.Add(step))
				if err != nil {
					return err
				}
				progress, err := beginRollupProgress(tx, tenant, resolution, start)
				if err != nil {
					return err
				}
				done, err := accumulatePage(tx, tenant, resolution, start, start.Add(step), kept, &progress)
				if err != nil {
					return err
				}
				pages++
				if !done {
					return nil
				}

				if err := completeAffectedSessions(tx, tenant, start, start.Add(step), resolution, kept, locked); err != nil {
					return err
				}
				if err := tx.Model(&models.ExperienceMetricRollup{}).Where("tenant_id=? AND resolution=? AND bucket_start=?", tenant, resolution, start).Update("completed", true).Error; err != nil {
					return err
				}
				field := "last_minute_bucket"
				if resolution == "hour" {
					field = "last_evaluated_bucket"
					if err := evaluateBucketAndUpdateIncidents(tx, tenant, locked, &run, start); err != nil {
						return err
					}
					t := start
					committedStart = &t
				}
				if err := tx.Model(&models.ExperiencePolicy{}).Where("tenant_id=?", tenant).Update(field, start).Error; err != nil {
					return err
				}
				completed = true
				return nil
			})
			if err != nil {
				finalErr := finishExperienceRun(db, &run, models.RuxRunFailed, buckets, nil, false, "persistence_error", "Experience evaluation failed; inspect CMS diagnostics")
				return &run, errors.Join(err, finalErr)
			}
			if completed && resolution == "hour" {
				buckets++
				if run.WindowStart == nil {
					run.WindowStart = committedStart
				}
				t := committedStart.Add(time.Hour)
				run.WindowEnd = &t
			}
			if noWork {
				break
			}
			if !completed && pages >= experiencePagesPerPass {
				partial = true
				break
			}
		}
		if pages >= experiencePagesPerPass || buckets >= policy.RollupMaxBucketsPerPass {
			partial = true
			break
		}
	}
	policy, err = loadExperiencePolicy(db, tenant)
	if err != nil {
		finalErr := finishExperienceRun(db, &run, models.RuxRunFailed, buckets, nil, false, "read_error", "Experience policy unavailable")
		return &run, errors.Join(err, finalErr)
	}
	verdicts, fresh, _, err := allClientExperienceSnapshot(db, tenant, policy, now)
	if err != nil {
		finalErr := finishExperienceRun(db, &run, models.RuxRunFailed, buckets, nil, false, "read_error", "Experience evidence unavailable")
		return &run, errors.Join(err, finalErr)
	}
	status := models.RuxRunCompleted
	if partial {
		status = models.RuxRunPartial
	}
	if err := finishExperienceRun(db, &run, status, buckets, verdicts, fresh, "none", ""); err != nil {
		return &run, err
	}
	return &run, nil
}

// Preserve the legacy Web keys while adding distinct native surface keys. The
// fleet summary includes every enabled client; healthy Web cannot mask native.
func allClientExperienceSnapshot(db *gorm.DB, tenant string, policy models.ExperiencePolicy, now time.Time) (map[string]any, bool, map[string]bool, error) {
	out := map[string]any{}
	freshness := map[string]bool{}
	clients := enabledExperienceClients(policy)
	allFresh := len(clients) > 0
	bucket := floorBucket(now).Add(-time.Hour)
	complete := policy.LastEvaluatedBucket != nil && !policy.LastEvaluatedBucket.Before(bucket)
	for _, client := range clients {
		fresh, err := isTelemetryFresh(db, tenant, policy, now, client)
		if err != nil {
			return nil, false, nil, err
		}
		freshness[client] = fresh && complete
		allFresh = allFresh && freshness[client]
		verdicts, err := computeDisplayVerdicts(db, tenant, policy, bucket, complete, client)
		if err != nil {
			return nil, false, nil, err
		}
		for surface, verdict := range verdicts {
			key := surface
			if client != "web" {
				key = client + ":" + surface
			}
			out[key] = verdict
		}
	}
	return out, allFresh, freshness, nil
}

func interruptIncidents(db *gorm.DB, tenant, surface string, bucket time.Time, clients ...string) error {
	q := db.Model(&models.ExperienceIncident{}).Where("tenant_id=? AND status IN ('open','recovering','confirming')", tenant)
	if surface != "" {
		q = q.Where("surface=?", surface)
	}
	if len(clients) > 0 {
		q = q.Where("client_platform=?", experienceClient(clients))
	}
	return q.Updates(map[string]any{"violation_streak": 0, "clean_streak": 0, "recovering_since": nil, "status": gorm.Expr("CASE WHEN confirmed_at IS NOT NULL THEN 'open' ELSE 'confirming' END"), "last_evaluated_bucket": bucket}).Error
}
func evaluateBucketAndUpdateIncidents(db *gorm.DB, tenant string, policy models.ExperiencePolicy, run *models.ExperienceEvaluationRun, bucket time.Time) error {
	// Mark unseen existing cohort fingerprints insufficient at the start; current
	// eligible rows explicitly restore their adjacent prior state in updateIncident.
	for _, client := range enabledExperienceClients(policy) {
		for _, surface := range enabledSurfaces(policy) {
			integrity, err := bucketIntegrity(db, tenant, surface, bucket, client)
			if err != nil {
				return err
			}
			if !integrity {
				if err := interruptIncidents(db, tenant, surface, bucket, client); err != nil {
					return err
				}
				if err := writeExperienceAction(db, tenant, run, nil, "verdict_withheld", "Terminal coverage or integrity incomplete", sliStatus{Surface: surface, ClientPlatform: client}, "telemetry_degraded", bucket); err != nil {
					return err
				}
				continue
			}
			var rows []models.ExperienceMetricRollup
			if err := db.Where("tenant_id=? AND resolution='hour' AND completed=true AND bucket_start=? AND surface=?", tenant, bucket, surface).Where("client_platform=?", client).Find(&rows).Error; err != nil {
				return err
			}
			scopes := map[[2]string]bool{{"global", "all"}: true}
			for _, r := range rows {
				scopes[[2]string{r.CohortDim, r.CohortVal}] = true
			}
			seen := map[string]bool{}
			for ch := range scopes {
				statuses, err := statusesFromRows(db, tenant, policy, surface, ch[0], ch[1], bucket, client)
				if err != nil {
					return err
				}
				for _, st := range statuses {
					fp := fmt.Sprintf("%s:%s:%s:%s", st.Metric, surface, ch[0], ch[1])
					if client != "web" {
						fp = client + ":" + fp
					}
					seen[fp] = true
					if ch[0] != "global" && (st.ActiveSessions < 5 || (st.Status == "breach" || st.Status == "latency_breach") && st.AffectedSessions < 5) {
						st.Status = "insufficient"
					}
					if err := updateIncident(db, tenant, policy, run, surface, ch[0], ch[1], st, st.Status == "breach" || st.Status == "latency_breach", bucket); err != nil {
						return err
					}
				}
			}
			var active []models.ExperienceIncident
			if err := db.Where("tenant_id=? AND surface=? AND status IN ('open','recovering','confirming')", tenant, surface).Where("client_platform=?", client).Find(&active).Error; err != nil {
				return err
			}
			for _, inc := range active {
				if !seen[inc.Fingerprint] {
					inc.ViolationStreak = 0
					inc.CleanStreak = 0
					inc.LastEvaluatedBucket = &bucket
					inc.RecoveringSince = nil
					if inc.ConfirmedAt != nil {
						inc.Status = "open"
					} else {
						inc.Status = "confirming"
					}
					if err := db.Save(&inc).Error; err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

// Pure state transition, shared by deterministic adjacency/failure fixtures.
func advanceExperienceIncident(inc *models.ExperienceIncident, policy models.ExperiencePolicy, status string, bucket time.Time) string {
	if inc.LastEvaluatedBucket != nil && !bucket.After(*inc.LastEvaluatedBucket) {
		return ""
	}
	adjacent := inc.LastEvaluatedBucket != nil && bucket.Equal(inc.LastEvaluatedBucket.Add(time.Hour))
	if !adjacent {
		inc.ViolationStreak = 0
		inc.CleanStreak = 0
	}
	inc.LastEvaluatedBucket = &bucket
	if status == "insufficient" || status == "not_exercised" || status == "suppressed" {
		inc.ViolationStreak = 0
		inc.CleanStreak = 0
		inc.RecoveringSince = nil
		if inc.ConfirmedAt != nil {
			inc.Status = "open"
		} else {
			inc.Status = "confirming"
		}
		return "streak_interrupted"
	}
	if status == "breach" || status == "latency_breach" {
		inc.ViolationStreak++
		inc.CleanStreak = 0
		inc.LastSeenAt = bucket
		inc.RecoveringSince = nil
		if inc.ConfirmedAt == nil && inc.ViolationStreak < policy.ConfirmWindows {
			inc.Status = "confirming"
			inc.Severity = "watching"
			return "incident_watching"
		}
		if inc.ConfirmedAt == nil {
			inc.ConfirmedAt = &bucket
			inc.Status = "open"
			inc.Severity = "degraded"
			return "incident_opened"
		}
		inc.Status = "open"
		inc.Severity = "degraded"
		return "incident_updated"
	}
	if inc.ConfirmedAt == nil {
		inc.Status = "resolved"
		inc.ResolvedAt = &bucket
		inc.ViolationStreak = 0
		return "watch_cleared"
	}
	inc.ViolationStreak = 0
	inc.CleanStreak++
	inc.Status = "recovering"
	if inc.RecoveringSince == nil {
		inc.RecoveringSince = &bucket
	}
	if inc.CleanStreak >= policy.ResolveWindows {
		inc.Status = "resolved"
		inc.ResolvedAt = &bucket
		return "incident_resolved"
	}
	return "incident_recovering"
}
func updateIncident(db *gorm.DB, tenant string, policy models.ExperiencePolicy, run *models.ExperienceEvaluationRun, surface, dim, val string, st sliStatus, breached bool, bucket time.Time) error {
	fingerprint := fmt.Sprintf("%s:%s:%s:%s", st.Metric, surface, dim, val)
	if models.RuxPlatform(st.ClientPlatform) != "web" {
		fingerprint = models.RuxPlatform(st.ClientPlatform) + ":" + fingerprint
	}
	var inc models.ExperienceIncident
	err := db.Where("tenant_id=? AND fingerprint=? AND status IN ('open','recovering','confirming')", tenant, fingerprint).Where("client_platform=?", models.RuxPlatform(st.ClientPlatform)).First(&inc).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	exists := err == nil
	muted, err := suppressed(db, tenant, st.Metric, surface, dim, val, bucket.Add(time.Hour), models.RuxPlatform(st.ClientPlatform))
	if err != nil {
		return err
	}
	if muted {
		st.Status = "suppressed"
		breached = false
	}
	if !exists && !breached {
		if dim == "global" && (st.Required && st.Status == "insufficient" || muted) {
			return writeExperienceAction(db, tenant, run, nil, "verdict_withheld", st.Label+" evidence withheld", st, st.Status, bucket)
		}
		return nil
	}
	if !exists {
		inc = models.ExperienceIncident{TenantID: tenant, ClientPlatform: models.RuxPlatform(st.ClientPlatform), Fingerprint: fingerprint, MetricKey: st.Metric, Surface: surface, CohortDim: dim, CohortVal: val, FirstSeenAt: bucket, Summary: st.Label + " on " + surface}
	}
	if st.LikelyOwner == "wahb_platform" && models.RuxPlatform(st.ClientPlatform) != "web" {
		st.LikelyOwner = "wahb_mobile"
	}
	if dim == "browser" || dim == "release" || dim == "os" {
		st.LikelyOwner = "wahb_platform"
		if models.RuxPlatform(st.ClientPlatform) != "web" {
			st.LikelyOwner = "wahb_mobile"
		}
	}
	inc.LikelyOwner = st.LikelyOwner
	inc.Recommendation = recommendationFor(st, surface)
	inc.InvestigationPath = investigationPath(st.LikelyOwner)
	class := advanceExperienceIncident(&inc, policy, st.Status, bucket)
	if class == "" {
		return nil
	}
	inc.Evidence = jsonBytes(map[string]any{"client_platform": models.RuxPlatform(st.ClientPlatform), "value": st.Value, "target": st.Target, "numerator": st.Numerator, "denominator": st.Denominator, "population": st.Population, "active_sessions": st.ActiveSessions, "affected_sessions": st.AffectedSessions, "p75_ms": st.P75, "p75_budget_ms": st.P75Budget, "latency_samples": st.Samples, "window_start": bucket, "window_end": bucket.Add(time.Hour), "cohort_dim": dim, "cohort_val": val, "confirm_windows": policy.ConfirmWindows, "resolve_windows": policy.ResolveWindows})
	if err := db.Save(&inc).Error; err != nil {
		return err
	}
	st.Surface = surface
	st.CohortDim = dim
	st.CohortVal = val
	if err := writeExperienceAction(db, tenant, run, &inc.ID, class, st.Label+" — "+strings.ReplaceAll(class, "_", " "), st, st.Status, bucket); err != nil {
		return err
	}
	if class == "incident_opened" {
		if err := writeExperienceAction(db, tenant, run, &inc.ID, "diagnostic_recommended", inc.Recommendation, st, "observe_only", bucket); err != nil {
			return err
		}
		return writeExperienceAction(db, tenant, run, &inc.ID, "would_request_deep_check", "A targeted Feed Integrity check may help; no check executed", st, "safe_auto_deferred", bucket)
	}
	return nil
}
func investigationPath(owner string) string {
	switch owner {
	case "feed_integrity", "storage":
		return "/platform/feed-integrity"
	case "circulation":
		return "/platform/media"
	case "news":
		return "/platform/news"
	default:
		return "/platform/real-experience"
	}
}
func recommendationFor(st sliStatus, _ string) string {
	switch st.LikelyOwner {
	case "feed_integrity":
		return "Inspect Feed Integrity serving evidence, then System Health."
	case "storage":
		return "Inspect Feed Integrity playback probes and Storage/Quality."
	case "news":
		return "Inspect News snapshot delivery and article content."
	case "circulation":
		return "Inspect Media Circulation inventory."
	case "wahb_mobile":
		return "Compare Wahb-Mobile build and OS evidence; inspect native playback and lifecycle."
	default:
		return "Compare Wahb-Platform release and browser evidence."
	}
}
func suppressed(db *gorm.DB, tenant, metric, surface, dim, val string, at time.Time, clients ...string) (bool, error) {
	var count int64
	err := db.Model(&models.ExperienceSuppression{}).Where("tenant_id=? AND revoked_at IS NULL AND starts_at<=? AND expires_at>?", tenant, at, at).Where("(metric_key=? OR metric_key IS NULL OR metric_key='')", metric).Where("(surface=? OR surface IS NULL OR surface='')", surface).Where("(cohort_dim=? OR cohort_dim IS NULL OR cohort_dim='')", dim).Where("(cohort_val=? OR cohort_val IS NULL OR cohort_val='')", val).Where("client_platform=?", experienceClient(clients)).Count(&count).Error
	return count > 0, err
}
func jsonBytes(v any) []byte { b, _ := json.Marshal(v); return b }
func writeExperienceAction(db *gorm.DB, tenant string, run *models.ExperienceEvaluationRun, incidentID *uint, class, label string, st sliStatus, guardrail string, bucket time.Time) error {
	var runID *uint
	if run != nil {
		runID = &run.ID
	}
	return db.Create(&models.ExperienceAction{TenantID: tenant, RunID: runID, IncidentID: incidentID, ActionClass: class, Label: label, MetricKey: st.Metric, Surface: st.Surface, CohortDim: st.CohortDim, CohortVal: st.CohortVal, Guardrail: guardrail, Evidence: jsonBytes(map[string]any{"client_platform": models.RuxPlatform(st.ClientPlatform), "value": st.Value, "target": st.Target, "numerator": st.Numerator, "denominator": st.Denominator, "active_sessions": st.ActiveSessions, "affected_sessions": st.AffectedSessions, "window_start": bucket, "window_end": bucket.Add(time.Hour), "surface": st.Surface, "cohort_dim": st.CohortDim, "cohort_val": st.CohortVal, "release": func() string {
		if st.CohortDim == "release" {
			return st.CohortVal
		}
		return ""
	}(), "latency_samples": st.Samples, "p75_ms": st.P75, "p75_budget_ms": st.P75Budget})}).Error
}
func finishExperienceRun(db *gorm.DB, run *models.ExperienceEvaluationRun, status string, buckets int, verdicts map[string]any, fresh bool, errClass, errMsg string) error {
	now := time.Now().UTC()
	run.Status = status
	run.FinishedAt = &now
	run.BucketsProcessed = buckets
	run.TelemetryFresh = fresh
	run.ErrorClass = errClass
	run.Error = errMsg
	if verdicts != nil {
		run.SurfaceVerdicts = jsonBytes(verdicts)
		run.Summary = summarizeVerdicts(verdicts)
	}
	return db.Save(run).Error
}
func summarizeVerdicts(verdicts map[string]any) string {
	parts := []string{}
	for _, surface := range []string{"pods", "news"} {
		if m, ok := verdicts[surface].(map[string]any); ok {
			parts = append(parts, fmt.Sprintf("%s=%v", surface, m["verdict"]))
		}
	}
	return strings.Join(parts, " ")
}
