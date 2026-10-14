package controllers

import (
	"content-management-system/src/models"
	"encoding/json"
	"errors"
	"fmt"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	mFeedRenderSuccess       = "feed_render_success"
	mFeedEmptyRate           = "feed_empty_rate"
	mPaginationSuccess       = "pagination_success"
	mPaginationStarvation    = "pagination_starvation"
	mPlaybackStartSuccess    = "playback_start_success"
	mPlaybackFatalRate       = "playback_fatal_rate"
	mAutoplayBlockedRate     = "autoplay_blocked_rate"
	mContinuationFatalRate   = "continuation_fatal_rate"
	mStallIncidence          = "stall_incidence"
	mRebufferRatio           = "rebuffer_ratio"
	mFallbackUse             = "fallback_use"
	mContinuationFallbackUse = "continuation_fallback_use"
	mFallbackSuccess         = "fallback_success"
	mHandoffSuccess          = "handoff_success"
	mHandoffDrift            = "handoff_drift"
	mArticleReadySuccess     = "article_ready_success"
	mErrorFreeSessions       = "error_free_sessions"
)

var latencyMetrics = map[string]bool{mFeedRenderSuccess: true, mPlaybackStartSuccess: true, mHandoffSuccess: true, mArticleReadySuccess: true}

// Version 2 adds the exact inclusive feed SLO boundary. Version 1 retained
// histograms are diagnostic only; they cannot satisfy latency evidence floors.
var latencyBounds = []int64{250, 500, 1000, 2000, 2500, 4000, 8000}

const rollupBucketDuration = time.Hour
const rollupResolution = "hour"
const experiencePageSize = 5000
const experiencePagesPerPass = 40

var errExperiencePartial = errors.New("bounded rollup pass incomplete")

type rollupKey struct{ metric, surface, cohortDim, cohortVal, clientPlatform string }
type rollupAgg struct {
	num, denom, samples, latencyMS, outcomes int64
	latBuckets                               [8]int64
}
type contribution struct {
	metric     string
	num, denom int64
	latency    *int
}

func boolCount(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
func isEligibleExperience(ev *models.ExperienceEvent) bool {
	return (ev.SchemaVersion == 2 || ev.SchemaVersion == 3) && ev.Visible != nil && *ev.Visible && (ev.EventType == "client_failure" || ev.EventType == "journey_terminal" && ev.TerminalStatus == "accepted" && ev.Outcome != "backgrounded" && ev.Outcome != "cancelled" && ev.Outcome != "end_of_feed")
}
func contributionsFor(ev *models.ExperienceEvent) []contribution {
	if !isEligibleExperience(ev) || ev.EventType != "journey_terminal" {
		return nil
	}
	success := boolCount(ev.Outcome == "success")
	switch ev.Phase {
	case "feed":
		out := []contribution{{mFeedRenderSuccess, success, 1, ev.DurationMS}}
		if ev.Outcome == "success" || ev.Outcome == "empty" {
			out = append(out, contribution{mFeedEmptyRate, boolCount(ev.Outcome == "empty"), 1, nil})
		}
		return out
	case "pagination":
		return []contribution{{mPaginationSuccess, success, 1, nil}, {mPaginationStarvation, boolCount(ev.BoundaryStarved != nil && *ev.BoundaryStarved), 1, nil}}
	case "startup":
		if ev.Outcome == "autoplay_blocked" {
			return []contribution{{mAutoplayBlockedRate, 1, 1, nil}}
		}
		return []contribution{{mPlaybackStartSuccess, success, 1, ev.DurationMS}, {mPlaybackFatalRate, 1 - success, 1, nil}, {mAutoplayBlockedRate, 0, 1, nil}, {mFallbackUse, boolCount(ev.FallbackUsed != nil && *ev.FallbackUsed), 1, nil}}
	case "continuation":
		out := []contribution{{mContinuationFatalRate, 1 - success, 1, nil}}
		if ev.FallbackUsed != nil {
			out = append(out, contribution{mContinuationFallbackUse, boolCount(*ev.FallbackUsed), 1, nil})
		}
		if ev.ObservedPlaybackMS != nil && *ev.ObservedPlaybackMS > 0 && ev.StallDurationMS != nil && ev.StallCount != nil {
			out = append(out, contribution{mStallIncidence, boolCount(*ev.StallCount > 0), 1, nil}, contribution{mRebufferRatio, int64(*ev.StallDurationMS), int64(*ev.ObservedPlaybackMS), nil})
		}
		return out
	case "fallback":
		return []contribution{{mFallbackSuccess, success, 1, nil}}
	case "handoff":
		out := []contribution{{mHandoffSuccess, success, 1, ev.DurationMS}}
		if ev.PositionDriftMS != nil {
			out = append(out, contribution{mHandoffDrift, boolCount(*ev.PositionDriftMS > 2000), 1, nil})
		}
		return out
	case "reader":
		return []contribution{{mArticleReadySuccess, success, 1, ev.DurationMS}}
	}
	return nil
}
func cohortsFor(ev *models.ExperienceEvent, kept map[string]bool) [][2]string {
	out := [][2]string{{"global", "all"}}
	rel := ev.Release
	if !kept[models.RuxPlatform(ev.ClientPlatform)+":"+rel] && !kept[rel] {
		rel = "other"
	}
	out = append(out, [2]string{"release", rel})
	if ev.PlaybackType != nil {
		out = append(out, [2]string{"playback_type", *ev.PlaybackType})
	}
	if models.RuxPlatform(ev.ClientPlatform) != "web" {
		out = append(out, [2]string{"os", strconv.Itoa(ev.OSMajor)})
	}
	browser := ev.BrowserFamily + "-" + strconv.Itoa(ev.BrowserMajor)
	if !kept["browser:"+browser] {
		browser = ev.BrowserFamily + "-other"
	}
	if models.RuxPlatform(ev.ClientPlatform) == "web" {
		out = append(out, [2]string{"browser", browser})
	}
	out = append(out, [2]string{"device", ev.DeviceClass}, [2]string{"network", ev.NetworkClass})
	if ev.Locale != nil {
		out = append(out, [2]string{"locale", *ev.Locale})
	}
	return out
}
func latencyBucketIndex(ms int) int {
	for i, b := range latencyBounds {
		if int64(ms) <= b {
			return i
		}
	}
	return len(latencyBounds)
}
func keptReleaseSet(db *gorm.DB, tenant string, max int, before time.Time) (map[string]bool, error) {
	var rows []struct{ Release, ClientPlatform string }
	if err := db.Raw(`SELECT release,client_platform FROM (
 SELECT release,client_platform,ROW_NUMBER() OVER (PARTITION BY client_platform ORDER BY MAX(received_at) DESC,release ASC) AS rank
 FROM experience_events WHERE tenant_id=? AND received_at>=? AND received_at<? AND schema_version IN (2,3)
 GROUP BY release,client_platform) r WHERE rank<=?`, tenant, before.Add(-14*24*time.Hour), before, max).Scan(&rows).Error; err != nil {
		return nil, err
	}
	kept := map[string]bool{}
	for _, r := range rows {
		kept[r.ClientPlatform+":"+r.Release] = true
	}
	var browsers []struct {
		BrowserFamily string
		BrowserMajor  int
	}
	if err := db.Model(&models.ExperienceEvent{}).Select("browser_family,browser_major").Where("client_platform='web'").Where("tenant_id=? AND received_at>=? AND received_at<? AND schema_version IN (2,3)", tenant, before.Add(-14*24*time.Hour), before).Group("browser_family,browser_major").Order("COUNT(*) DESC, browser_family ASC, browser_major ASC").Limit(24).Scan(&browsers).Error; err != nil {
		return nil, err
	}
	for _, b := range browsers {
		kept["browser:"+b.BrowserFamily+"-"+strconv.Itoa(b.BrowserMajor)] = true
	}
	return kept, nil
}

// accumulatePage commits an ordered bounded page and its cursor in the same
// transaction. The completed policy checkpoint alone permits serving evidence.
func accumulatePage(db *gorm.DB, tenant, resolution string, start, end time.Time, kept map[string]bool, progress *models.ExperienceRollupProgress) (bool, error) {
	var events []models.ExperienceEvent
	if err := db.Where("tenant_id=? AND received_at>=? AND received_at<? AND id>?", tenant, start, end, progress.Cursor).Order("id ASC").Limit(experiencePageSize).Find(&events).Error; err != nil {
		return false, err
	}
	aggs := map[rollupKey]*rollupAgg{}
	for i := range events {
		ev := &events[i]
		for _, c := range contributionsFor(ev) {
			for _, cohort := range cohortsFor(ev, kept) {
				k := rollupKey{c.metric, ev.Surface, cohort[0], cohort[1], models.RuxPlatform(ev.ClientPlatform)}
				a := aggs[k]
				if a == nil {
					a = &rollupAgg{}
					aggs[k] = a
				}
				a.outcomes++
				a.num += c.num
				a.denom += c.denom
				if c.num > 0 && c.latency != nil && latencyMetrics[c.metric] {
					a.samples++
					a.latencyMS += int64(*c.latency)
					a.latBuckets[latencyBucketIndex(*c.latency)]++
				}
			}
		}
	}
	if err := upsertExperienceRollupDeltas(db, tenant, resolution, start.UTC(), aggs); err != nil {
		return false, err
	}

	if len(events) > 0 {
		progress.Cursor = events[len(events)-1].ID
		progress.EventsProcessed += int64(len(events))
	}
	if err := db.Save(progress).Error; err != nil {
		return false, err
	}
	return len(events) < experiencePageSize, nil
}
func beginRollupProgress(db *gorm.DB, tenant, resolution string, start time.Time) (models.ExperienceRollupProgress, error) {
	var p models.ExperienceRollupProgress
	err := db.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=? AND resolution=?", tenant, resolution).First(&p).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return p, err
	}
	if errors.Is(err, gorm.ErrRecordNotFound) || !p.BucketStart.Equal(start) {
		if err := db.Where("tenant_id=? AND resolution=? AND bucket_start=?", tenant, resolution, start).Delete(&models.ExperienceMetricRollup{}).Error; err != nil {
			return p, err
		}
		p = models.ExperienceRollupProgress{TenantID: tenant, Resolution: resolution, BucketStart: start}
		if err := db.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "tenant_id"}, {Name: "resolution"}}, DoUpdates: clause.AssignmentColumns([]string{"bucket_start", "cursor", "events_processed", "updated_at"})}).Create(&p).Error; err != nil {
			return p, err
		}
	}
	return p, nil
}
func eligiblePopulation(db *gorm.DB, tenant, surface string, start, end time.Time) *gorm.DB {
	return db.Model(&models.ExperienceEvent{}).Where("tenant_id=? AND surface=? AND received_at>=? AND received_at<? AND schema_version IN (2,3) AND visible=true AND (event_type='client_failure' OR (event_type='journey_terminal' AND terminal_status='accepted' AND outcome NOT IN ('backgrounded','cancelled','end_of_feed')))", tenant, surface, start, end)
}

// Unit-test compatibility entry point: a full page scan, replayable from scratch.
func rollupBucket(db *gorm.DB, tenant string, start time.Time, kept map[string]bool, _ int) (int, error) {
	count := 0
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("tenant_id=? AND resolution=?", tenant, "hour").Delete(&models.ExperienceRollupProgress{}).Error; err != nil {
			return err
		}
		p, err := beginRollupProgress(tx, tenant, "hour", start)
		if err != nil {
			return err
		}
		for {
			done, err := accumulatePage(tx, tenant, "hour", start, start.Add(time.Hour), kept, &p)
			if err != nil {
				return err
			}
			if done {
				break
			}
		}
		count = int(p.EventsProcessed)
		if err := completeAffectedSessions(tx, tenant, start, start.Add(time.Hour), "hour", kept); err != nil {
			return err
		}
		return tx.Model(&models.ExperienceMetricRollup{}).Where("tenant_id=? AND bucket_start=? AND resolution='hour'", tenant, start).Update("completed", true).Error
	})
	return count, err
}
func p75FromBuckets(buckets [8]int64) int {
	var total int64
	for _, c := range buckets {
		total += c
	}
	if total == 0 {
		return 0
	}
	target := (total*75 + 99) / 100
	var cumulative int64
	for i, c := range buckets {
		cumulative += c
		if cumulative >= target {
			if i < len(latencyBounds) {
				return int(latencyBounds[i])
			}
			return models.RuxMaxDurationMS
		}
	}
	return 0
}
func parseLatBuckets(raw []byte) [8]int64 {
	var out [8]int64
	_ = json.Unmarshal(raw, &out)
	return out
}
func floorBucket(t time.Time) time.Time { return t.UTC().Truncate(time.Hour) }
func bucketLabel(t time.Time) string    { return fmt.Sprintf("%s", floorBucket(t).Format(time.RFC3339)) }

func rateFailurePredicate(metric string) string {
	switch metric {
	case mFeedRenderSuccess:
		return "phase='feed' AND outcome<>'success'"
	case mFeedEmptyRate:
		return "phase='feed' AND outcome='empty'"
	case mPlaybackStartSuccess, mPlaybackFatalRate:
		return "phase='startup' AND outcome IN ('failed','timeout')"
	case mAutoplayBlockedRate:
		return "phase='startup' AND outcome='autoplay_blocked'"
	case mPaginationSuccess:
		return "phase='pagination' AND outcome<>'success'"
	case mPaginationStarvation:
		return "phase='pagination' AND boundary_starved=true"
	case mArticleReadySuccess:
		return "phase='reader' AND outcome<>'success'"
	case mHandoffSuccess:
		return "phase='handoff' AND outcome<>'success'"
	case mHandoffDrift:
		return "phase='handoff' AND position_drift_ms>2000"
	case mFallbackSuccess:
		return "phase='fallback' AND outcome<>'success'"
	case mFallbackUse:
		return "phase='startup' AND fallback_used=true"
	case mContinuationFallbackUse:
		return "phase='continuation' AND fallback_used=true"
	case mContinuationFatalRate:
		return "phase='continuation' AND outcome<>'success'"
	case mStallIncidence, mRebufferRatio:
		return "phase='continuation' AND stall_count>0"
	case mErrorFreeSessions:
		return "event_type='client_failure'"
	}
	return "false"
}

// One finite SQL cohort projection computes exact distinct session populations.
// No per-session slice or per-metric full scan enters CMS memory.
func completeAffectedSessions(db *gorm.DB, tenant string, start, end time.Time, resolution string, kept map[string]bool, policies ...models.ExperiencePolicy) error {
	policy := models.DefaultExperiencePolicy(tenant)
	if len(policies) > 0 {
		policy = policies[0]
	}
	releases := []string{"__none__"}
	browsers := []string{"__none__"}
	for k := range kept {
		if strings.HasPrefix(k, "browser:") {
			browsers = append(browsers, strings.TrimPrefix(k, "browser:"))
		} else {
			releases = append(releases, k)
		}
	}
	fields := []string{"e.client_platform", "e.surface", "ch.dim AS cohort_dim", "ch.val AS cohort_val", "COUNT(DISTINCT e.session_id) AS active_sessions"}
	metrics := append([]sliSpec{}, experienceSLIs...)
	for _, metric := range metrics {
		fields = append(fields, "COUNT(DISTINCT e.session_id) FILTER (WHERE "+metricFailurePredicate(metric.metric, policy)+") AS "+metric.metric)
	}
	sql := "SELECT " + strings.Join(fields, ",") + ` FROM experience_events e CROSS JOIN LATERAL (VALUES
 ('global','all'), ('release',CASE WHEN (e.client_platform||':'||e.release) IN ? OR e.release IN ? THEN e.release ELSE 'other' END),
 ('os',CASE WHEN e.client_platform<>'web' THEN e.os_major::text ELSE NULL END),
 ('browser',CASE WHEN e.client_platform<>'web' THEN NULL WHEN (e.browser_family || '-' || e.browser_major::text) IN ? THEN (e.browser_family || '-' || e.browser_major::text) ELSE (e.browser_family || '-other') END),
 ('device',e.device_class), ('network',e.network_class), ('locale',COALESCE(e.locale,'unknown')), ('playback_type',e.playback_type)
 ) ch(dim,val)
 WHERE e.tenant_id=? AND e.received_at>=? AND e.received_at<? AND e.schema_version IN (2,3) AND e.visible=true
 AND (e.event_type='client_failure' OR (e.event_type='journey_terminal' AND e.terminal_status='accepted' AND e.outcome NOT IN ('backgrounded','cancelled','end_of_feed')))
 AND ch.val IS NOT NULL GROUP BY e.client_platform,e.surface,ch.dim,ch.val`
	var groups []map[string]interface{}
	if err := db.Raw(sql, releases, releases, browsers, tenant, start, end).Scan(&groups).Error; err != nil {
		return err
	}
	sessionRows := make([]models.ExperienceMetricRollup, 0, len(groups))
	counts := make([]experienceCohortCount, 0, len(groups)*len(metrics))
	now := time.Now().UTC()
	for _, group := range groups {
		client := models.RuxPlatform(fmt.Sprint(group["client_platform"]))
		surface := fmt.Sprint(group["surface"])
		dimension := fmt.Sprint(group["cohort_dim"])
		value := fmt.Sprint(group["cohort_val"])
		active := asExperienceInt(group["active_sessions"])
		failures := asExperienceInt(group[mErrorFreeSessions])
		sessionRows = append(sessionRows, models.ExperienceMetricRollup{
			TenantID: tenant, ClientPlatform: client, BucketStart: start.UTC(), Resolution: resolution, MetricKey: mErrorFreeSessions,
			Surface: surface, CohortDim: dimension, CohortVal: value, Numerator: active - failures,
			Denominator: active, ActiveSessions: active, AffectedSessions: failures, HistogramVersion: 2,
			CreatedAt: now, UpdatedAt: now,
		})
		for _, metric := range metrics {
			counts = append(counts, experienceCohortCount{
				ClientPlatform: client, Surface: surface, CohortDim: dimension, CohortVal: value, MetricKey: metric.metric,
				ActiveSessions: active, AffectedSessions: asExperienceInt(group[metric.metric]),
			})
		}
	}
	if len(sessionRows) == 0 {
		return nil
	}
	// Exact error-free populations and every metric's impact commit in two
	// bounded statements, rather than one network round trip per cohort/SLI.
	if err := db.Clauses(clause.OnConflict{
		Columns:   experienceRollupIdentityColumns(),
		DoUpdates: clause.AssignmentColumns([]string{"numerator", "denominator", "active_sessions", "affected_sessions", "histogram_version", "updated_at"}),
	}).CreateInBatches(&sessionRows, 500).Error; err != nil {
		return err
	}
	payload, err := json.Marshal(counts)
	if err != nil {
		return err
	}
	return db.Exec(`UPDATE experience_metric_rollups r SET
        active_sessions=counts.active_sessions, affected_sessions=counts.affected_sessions, updated_at=CURRENT_TIMESTAMP
        FROM jsonb_to_recordset(?::jsonb) AS counts(client_platform varchar(8),surface varchar(16),cohort_dim varchar(24),cohort_val varchar(80),metric_key varchar(64),active_sessions bigint,affected_sessions bigint)
        WHERE r.tenant_id=? AND r.bucket_start=? AND r.resolution=? AND r.surface=counts.surface
        AND r.client_platform=counts.client_platform AND r.cohort_dim=counts.cohort_dim AND r.cohort_val=counts.cohort_val AND r.metric_key=counts.metric_key`,
		string(payload), tenant, start.UTC(), resolution).Error
}
func metricFailurePredicate(metric string, policy models.ExperiencePolicy) string {
	predicate := rateFailurePredicate(metric)
	phase := map[string]string{mFeedRenderSuccess: "feed", mPlaybackStartSuccess: "startup", mArticleReadySuccess: "reader"}[metric]
	if phase == "" {
		return predicate
	}
	for _, base := range experienceSLIs {
		if base.metric == metric {
			budget := policySpec(policy, base).latencyP75
			if budget > 0 {
				return fmt.Sprintf("(%s OR (phase='%s' AND outcome='success' AND duration_ms>%d))", predicate, phase, budget)
			}
		}
	}
	return predicate
}
func asExperienceInt(v interface{}) int64 {
	switch value := v.(type) {
	case int64:
		return value
	case int:
		return int64(value)
	case []byte:
		n, _ := strconv.ParseInt(string(value), 10, 64)
		return n
	case string:
		n, _ := strconv.ParseInt(value, 10, 64)
		return n
	}
	return 0
}

// A bounded page carries additive deltas only. Histogram addition happens in
// PostgreSQL inside the same transaction as the page cursor, never through a
// read/modify/write sequence that needs one remote round trip per aggregate.
func upsertExperienceRollupDeltas(db *gorm.DB, tenant, resolution string, start time.Time, aggs map[rollupKey]*rollupAgg) error {
	if len(aggs) == 0 {
		return nil
	}
	rows := make([]models.ExperienceMetricRollup, 0, len(aggs))
	for key, aggregate := range aggs {
		buckets, err := json.Marshal(aggregate.latBuckets)
		if err != nil {
			return err
		}
		rows = append(rows, models.ExperienceMetricRollup{
			TenantID: tenant, ClientPlatform: models.RuxPlatform(key.clientPlatform), BucketStart: start.UTC(), Resolution: resolution, MetricKey: key.metric, Surface: key.surface,
			CohortDim: key.cohortDim, CohortVal: key.cohortVal, Numerator: aggregate.num, Denominator: aggregate.denom,
			SampleCount: aggregate.samples, LatencySum: aggregate.latencyMS, LatencyBuckets: buckets, HistogramVersion: 2,
			OutcomeCount: aggregate.outcomes,
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		left, right := rows[i], rows[j]
		return strings.Join([]string{left.ClientPlatform, left.MetricKey, left.Surface, left.CohortDim, left.CohortVal}, "\n") < strings.Join([]string{right.ClientPlatform, right.MetricKey, right.Surface, right.CohortDim, right.CohortVal}, "\n")
	})
	payload, err := json.Marshal(rows)
	if err != nil {
		return err
	}
	histogramEntries := make([]string, len(latencyBounds)+1)
	for index := range histogramEntries {
		histogramEntries[index] = fmt.Sprintf("COALESCE((experience_metric_rollups.latency_buckets->>%d)::bigint,0)+COALESCE((EXCLUDED.latency_buckets->>%d)::bigint,0)", index, index)
	}
	const columns = `client_platform,tenant_id,bucket_start,resolution,metric_key,surface,cohort_dim,cohort_val,numerator,denominator,sample_count,latency_sum,latency_buckets,histogram_version,outcome_count`
	statement := "INSERT INTO experience_metric_rollups (" + columns + ",created_at,updated_at) SELECT " + columns + ",CURRENT_TIMESTAMP,CURRENT_TIMESTAMP FROM jsonb_populate_recordset(NULL::experience_metric_rollups,?::jsonb) ORDER BY metric_key,surface,cohort_dim,cohort_val " +
		"ON CONFLICT (tenant_id,bucket_start,resolution,metric_key,surface,cohort_dim,cohort_val,client_platform) DO UPDATE SET " +
		"numerator=experience_metric_rollups.numerator+EXCLUDED.numerator," +
		"denominator=experience_metric_rollups.denominator+EXCLUDED.denominator," +
		"sample_count=experience_metric_rollups.sample_count+EXCLUDED.sample_count," +
		"latency_sum=experience_metric_rollups.latency_sum+EXCLUDED.latency_sum," +
		"outcome_count=experience_metric_rollups.outcome_count+EXCLUDED.outcome_count," +
		"latency_buckets=jsonb_build_array(" + strings.Join(histogramEntries, ",") + "),updated_at=CURRENT_TIMESTAMP " +
		"WHERE experience_metric_rollups.histogram_version=2 AND experience_metric_rollups.completed=false"
	result := db.Exec(statement, string(payload))
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != int64(len(rows)) {
		return errors.New("experience aggregate delta rejected completed or incompatible evidence")
	}
	return nil
}
func experienceRollupIdentityColumns() []clause.Column {
	return []clause.Column{{Name: "tenant_id"}, {Name: "bucket_start"}, {Name: "resolution"}, {Name: "metric_key"}, {Name: "surface"}, {Name: "cohort_dim"}, {Name: "cohort_val"}, {Name: "client_platform"}}
}

type experienceCohortCount struct {
	ClientPlatform   string `json:"client_platform"`
	Surface          string `json:"surface"`
	CohortDim        string `json:"cohort_dim"`
	CohortVal        string `json:"cohort_val"`
	MetricKey        string `json:"metric_key"`
	ActiveSessions   int64  `json:"active_sessions"`
	AffectedSessions int64  `json:"affected_sessions"`
}
