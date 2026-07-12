package controllers

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"content-management-system/src/models"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Real User Experience — rollup engine.
//
// Terminal-outcome counting (plan §8): SLI ratios are never computed by dividing
// two event types across a window. Each measured journey emits one terminal
// outcome event; the rollup counts terminal outcomes within the bucket the
// terminal landed in. Rollups bucket by received_at (plan §14) so late beacons
// never amend an already-closed bucket.

// Metric keys (the cockpit's named SLIs).
const (
	mFeedRenderSuccess    = "feed_render_success"
	mFeedEmptyRate        = "feed_empty_rate"
	mPaginationSuccess    = "pagination_success"
	mPlaybackStartSuccess = "playback_start_success"
	mPlaybackFatalRate    = "playback_fatal_rate"
	mAutoplayBlockedRate  = "autoplay_blocked_rate"
	mHandoffSuccess       = "handoff_success"
	mArticleReadySuccess  = "article_ready_success"
	mErrorFreeSessions    = "error_free_sessions"
)

// Metrics that carry a client-measured latency on their success terminal.
var latencyMetrics = map[string]bool{
	mFeedRenderSuccess: true, mPlaybackStartSuccess: true, mHandoffSuccess: true, mArticleReadySuccess: true,
}

// Latency histogram upper bounds (ms). Seven buckets: the last is the overflow.
var latencyBounds = []int64{250, 500, 1000, 2000, 4000, 8000}

const rollupBucketDuration = time.Hour
const rollupResolution = "hour"

// rollupKey identifies one rollup row.
type rollupKey struct {
	metric    string
	surface   string
	cohortDim string
	cohortVal string
}

type rollupAgg struct {
	num        int64
	denom      int64
	samples    int64
	latencyMS  int64
	latBuckets [7]int64
}

// contribution is one metric delta from a single event.
type contribution struct {
	metric  string
	num     int64
	denom   int64
	latency *int
}

// contributionsFor maps an event to its metric contributions. Backgrounded
// playback and diagnostic (attempt/waiting/resumed/requested/started-session)
// events contribute nothing to SLI counts.
func contributionsFor(ev *models.ExperienceEvent) []contribution {
	dur := ev.DurationMS
	switch ev.EventType {
	case "feed_rendered":
		return []contribution{{mFeedRenderSuccess, 1, 1, dur}}
	case "feed_empty":
		return []contribution{{mFeedRenderSuccess, 0, 1, nil}, {mFeedEmptyRate, 1, 1, nil}}
	case "feed_failed":
		return []contribution{{mFeedRenderSuccess, 0, 1, nil}, {mFeedEmptyRate, 0, 1, nil}}
	case "pagination_received":
		return []contribution{{mPaginationSuccess, 1, 1, nil}}
	case "pagination_starved":
		return []contribution{{mPaginationSuccess, 0, 1, nil}}
	case "playback_started":
		return []contribution{
			{mPlaybackStartSuccess, 1, 1, dur},
			{mPlaybackFatalRate, 0, 1, nil},
			{mAutoplayBlockedRate, 0, 1, nil},
		}
	case "playback_failed":
		if ev.FailureClass != nil && *ev.FailureClass == "autoplay_blocked" {
			// Autoplay is expected browser behavior — its own rate only, never a
			// fatal failure or a start-success denominator (plan §8).
			return []contribution{{mAutoplayBlockedRate, 1, 1, nil}}
		}
		return []contribution{
			{mPlaybackStartSuccess, 0, 1, nil},
			{mPlaybackFatalRate, 1, 1, nil},
			{mAutoplayBlockedRate, 0, 1, nil},
		}
	case "handoff_completed":
		return []contribution{{mHandoffSuccess, 1, 1, dur}}
	case "handoff_failed":
		return []contribution{{mHandoffSuccess, 0, 1, nil}}
	case "article_opened":
		return nil
	case "article_ready":
		return []contribution{{mArticleReadySuccess, 1, 1, dur}}
	case "article_failed":
		return []contribution{{mArticleReadySuccess, 0, 1, nil}}
	}
	return nil
}

// cohortsFor lists the (dim, val) pairs an event contributes to, always
// including the global row. Release is folded to 'other' outside the kept set.
func cohortsFor(ev *models.ExperienceEvent, keptReleases map[string]bool) [][2]string {
	out := [][2]string{{"global", "all"}}
	rel := ev.Release
	if rel != "" {
		if !keptReleases[rel] {
			rel = "other"
		}
		out = append(out, [2]string{"release", rel})
	}
	if ev.PlaybackType != nil && *ev.PlaybackType != "" {
		out = append(out, [2]string{"playback_type", *ev.PlaybackType})
	}
	out = append(out, [2]string{"browser", ev.BrowserFamily + "-" + strconv.Itoa(ev.BrowserMajor)})
	out = append(out, [2]string{"device", ev.DeviceClass})
	out = append(out, [2]string{"network", ev.NetworkClass})
	if ev.Locale != nil && *ev.Locale != "" {
		out = append(out, [2]string{"locale", *ev.Locale})
	}
	return out
}

func latencyBucketIndex(ms int) int {
	for i, b := range latencyBounds {
		if int64(ms) < b {
			return i
		}
	}
	return len(latencyBounds) // overflow bucket
}

// keptReleaseSet returns the newest N distinct releases by max(received_at) over
// a recent lookback, so per-release rollup cardinality stays bounded.
func keptReleaseSet(db *gorm.DB, tenantID string, max int, before time.Time) map[string]bool {
	type row struct {
		Release string
	}
	var rows []row
	db.Model(&models.ExperienceEvent{}).
		Select("release").
		Where("tenant_id = ? AND received_at >= ? AND received_at < ?", tenantID, before.Add(-14*24*time.Hour), before).
		Group("release").
		Order("MAX(received_at) DESC").
		Limit(max).
		Scan(&rows)
	kept := make(map[string]bool, len(rows))
	for _, r := range rows {
		kept[r.Release] = true
	}
	return kept
}

// rollupBucket aggregates one closed hour bucket into experience_metric_rollups.
// Returns the number of events processed. Idempotent: re-running a bucket
// overwrites its rows (ON CONFLICT), so checkpoint replay is safe.
func rollupBucket(db *gorm.DB, tenantID string, bucketStart time.Time, keptReleases map[string]bool, scanCap int) (int, error) {
	bucketEnd := bucketStart.Add(rollupBucketDuration)
	aggs := map[rollupKey]*rollupAgg{}

	var events []models.ExperienceEvent
	if err := db.Where("tenant_id = ? AND received_at >= ? AND received_at < ?", tenantID, bucketStart, bucketEnd).
		Limit(scanCap).Find(&events).Error; err != nil {
		return 0, err
	}

	for i := range events {
		ev := &events[i]
		contribs := contributionsFor(ev)
		if len(contribs) == 0 {
			continue
		}
		cohorts := cohortsFor(ev, keptReleases)
		for _, con := range contribs {
			for _, ch := range cohorts {
				k := rollupKey{con.metric, ev.Surface, ch[0], ch[1]}
				a := aggs[k]
				if a == nil {
					a = &rollupAgg{}
					aggs[k] = a
				}
				a.num += con.num
				a.denom += con.denom
				if con.latency != nil && latencyMetrics[con.metric] {
					a.samples++
					a.latencyMS += int64(*con.latency)
					a.latBuckets[latencyBucketIndex(*con.latency)]++
				}
			}
		}
	}

	// error_free_sessions: distinct sessions vs distinct sessions with a
	// client_failure, per surface (global cohort only in V1).
	addErrorFreeSessions(db, tenantID, bucketStart, bucketEnd, aggs)

	// Upsert every rollup row idempotently.
	for k, a := range aggs {
		latJSON, _ := json.Marshal(a.latBuckets)
		row := models.ExperienceMetricRollup{
			TenantID: tenantID, BucketStart: bucketStart, Resolution: rollupResolution,
			MetricKey: k.metric, Surface: k.surface, CohortDim: k.cohortDim, CohortVal: k.cohortVal,
			Numerator: a.num, Denominator: a.denom, SampleCount: a.samples, LatencySum: a.latencyMS,
			LatencyBuckets: latJSON,
		}
		if err := db.Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "tenant_id"}, {Name: "bucket_start"}, {Name: "resolution"},
				{Name: "metric_key"}, {Name: "surface"}, {Name: "cohort_dim"}, {Name: "cohort_val"},
			},
			DoUpdates: clause.AssignmentColumns([]string{"numerator", "denominator", "sample_count", "latency_sum", "latency_buckets", "updated_at"}),
		}).Create(&row).Error; err != nil {
			return len(events), err
		}
	}

	return len(events), nil
}

func addErrorFreeSessions(db *gorm.DB, tenantID string, start, end time.Time, aggs map[rollupKey]*rollupAgg) {
	for _, surface := range []string{"foryou", "news"} {
		var total int64
		db.Model(&models.ExperienceEvent{}).
			Where("tenant_id = ? AND received_at >= ? AND received_at < ? AND surface = ? AND event_type = ?",
				tenantID, start, end, surface, "session_started").
			Distinct("session_id").Count(&total)
		if total == 0 {
			continue
		}
		var withErr int64
		db.Model(&models.ExperienceEvent{}).
			Where("tenant_id = ? AND received_at >= ? AND received_at < ? AND surface = ? AND event_type = ?",
				tenantID, start, end, surface, "client_failure").
			Distinct("session_id").Count(&withErr)
		aggs[rollupKey{mErrorFreeSessions, surface, "global", "all"}] = &rollupAgg{
			num: total - withErr, denom: total,
		}
	}
}

// p75FromBuckets estimates the 75th percentile latency (ms) from a histogram.
func p75FromBuckets(buckets [7]int64) int {
	var total int64
	for _, c := range buckets {
		total += c
	}
	if total == 0 {
		return 0
	}
	target := (total*75 + 99) / 100 // ceil(0.75*total)
	var cum int64
	for i, c := range buckets {
		cum += c
		if cum >= target {
			if i < len(latencyBounds) {
				return int(latencyBounds[i])
			}
			return int(latencyBounds[len(latencyBounds)-1]) * 2 // overflow estimate
		}
	}
	return 0
}

func parseLatBuckets(raw []byte) [7]int64 {
	var out [7]int64
	if len(raw) == 0 {
		return out
	}
	_ = json.Unmarshal(raw, &out)
	return out
}

// floorBucket truncates a time to the start of its hour bucket (UTC).
func floorBucket(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, time.UTC)
}

func bucketLabel(t time.Time) string {
	return fmt.Sprintf("%s", floorBucket(t).Format(time.RFC3339))
}
