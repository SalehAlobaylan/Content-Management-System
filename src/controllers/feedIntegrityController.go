package controllers

// Feed Integrity is deliberately a deterministic, read-only supervisor. It owns
// its ledger and attention objects, but never changes feed/content state.

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/utils"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

const (
	feedIntegrityTenant       = "default"
	feedIntegritySyntheticHdr = "X-Wahb-Synthetic"
	feedIntegrityMaxRun       = 10 * time.Minute
)

var (
	feedIntegrityMu         sync.Mutex
	feedIntegrityRunning    = map[string]bool{}
	feedIntegrityCapability = newFeedIntegrityCapability()
)

func newFeedIntegrityCapability() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return uuid.NewString() + uuid.NewString()
	}
	return hex.EncodeToString(b)
}

// isFeedIntegritySynthetic is intentionally stricter than a header check. A
// public caller must never be able to suppress ranking telemetry by spoofing it.
func isFeedIntegritySynthetic(c *gin.Context) bool {
	remote, _, err := net.SplitHostPort(c.Request.RemoteAddr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(remote)
	if ip == nil || !ip.IsLoopback() {
		return false
	}
	v := c.GetHeader(feedIntegritySyntheticHdr)
	return len(v) == len(feedIntegrityCapability) && subtle.ConstantTimeCompare([]byte(v), []byte(feedIntegrityCapability)) == 1
}

type feedIntegrityRunOptions struct{ Trigger, CreatedBy, Tier string }

type feedIntegrityCheck struct {
	Key, Label, Lane, Feed, Axis, OwnerSurface string
	Severity                                   string
}

var feedIntegrityChecks = []feedIntegrityCheck{
	{"inv_fy_status_contract", "Archived item matches the For You serving predicate", "inventory", "foryou", models.FeedIntegrityAxisReadiness, "content", "major"},
	{"inv_fy_bounds", "Visible media duration outside 270-2400 seconds", "inventory", "foryou", models.FeedIntegrityAxisReadiness, "media-studio", "major"},
	{"inv_fy_parent_leak", "Long parent remains a visible raw feed unit", "inventory", "foryou", models.FeedIntegrityAxisReadiness, "media-studio", "major"},
	{"inv_fy_playback_missing", "Visible feed unit has no legal playback URL", "inventory", "foryou", models.FeedIntegrityAxisReadiness, "storage", "major"},
	{"inv_fy_thumb_missing", "Otherwise-servable feed unit has no thumbnail", "inventory", "foryou", models.FeedIntegrityAxisReadiness, "media-circulation", "info"},
	{"inv_fy_bucket_mismatch", "Duration bucket does not match duration", "inventory", "foryou", models.FeedIntegrityAxisReadiness, "media-studio", "minor"},
	{"inv_fy_renditions_malformed", "Media renditions are malformed", "inventory", "foryou", models.FeedIntegrityAxisReadiness, "storage", "minor"},
	{"inv_news_related_dangling", "Story references a missing related story", "inventory", "news", models.FeedIntegrityAxisReadiness, "news", "minor"},
	{"inv_news_empty_story", "Active story has no ready members", "inventory", "news", models.FeedIntegrityAxisReadiness, "news", "major"},
	{"inv_news_unlabeled_stale", "Feed-eligible story remains unlabeled", "inventory", "news", models.FeedIntegrityAxisReadiness, "enrichment", "info"},
	{"inv_news_cache_rebuild_debt", "News snapshot remains stale beyond cache ceiling", "inventory", "news", models.FeedIntegrityAxisReadiness, "news", "minor"},
	{"edge_fy_http", "For You CMS endpoint is unavailable", "edge", "foryou", models.FeedIntegrityAxisConsumer, "content", "critical"},
	{"edge_fy_latency", "For You page exceeded its latency budget", "edge", "foryou", models.FeedIntegrityAxisConsumer, "content", "major"},
	{"edge_fy_empty", "For You page is empty despite eligible inventory", "edge", "foryou", models.FeedIntegrityAxisConsumer, "media-circulation", "critical"},
	{"edge_fy_required_metadata", "Served For You item lacks required metadata", "edge", "foryou", models.FeedIntegrityAxisConsumer, "content", "major"},
	{"edge_fy_bounds_served", "Served For You item has invalid duration", "edge", "foryou", models.FeedIntegrityAxisConsumer, "content", "major"},
	{"edge_fy_playback_fields", "Served For You item has invalid playback fields", "edge", "foryou", models.FeedIntegrityAxisConsumer, "storage", "major"},
	{"edge_fy_status_served", "Served For You item is archived", "edge", "foryou", models.FeedIntegrityAxisConsumer, "content", "major"},
	{"edge_fy_dup", "For You cursor walk repeats an item", "edge", "foryou", models.FeedIntegrityAxisConsumer, "content", "major"},
	{"edge_news_http", "News CMS endpoint is unavailable", "edge", "news", models.FeedIntegrityAxisConsumer, "content", "critical"},
	{"edge_news_latency", "News page exceeded its latency budget", "edge", "news", models.FeedIntegrityAxisConsumer, "content", "major"},
	{"edge_news_empty", "News page is empty despite active stories", "edge", "news", models.FeedIntegrityAxisConsumer, "news", "critical"},
	{"edge_news_shape", "News slide has an invalid shape", "edge", "news", models.FeedIntegrityAxisConsumer, "news", "major"},
	{"edge_news_dup", "News cursor walk repeats a story", "edge", "news", models.FeedIntegrityAxisConsumer, "news", "major"},
	{"edge_news_cache_stale", "News served a snapshot beyond its stale ceiling", "edge", "news", models.FeedIntegrityAxisConsumer, "news-circulation", "major"},
	{"probe_url_dead", "Primary playback URL is unavailable", "probe", "foryou", models.FeedIntegrityAxisConsumer, "storage", "major"},
	{"probe_hls_manifest", "HLS manifest is invalid", "probe", "foryou", models.FeedIntegrityAxisConsumer, "storage", "major"},
	{"checker_unhealthy", "Feed Integrity checker repeatedly failed", "checker", "platform", models.FeedIntegrityAxisReadiness, "system-health", "major"},
}

func feedIntegrityCheckCatalog() []feedIntegrityCheck {
	return append([]feedIntegrityCheck(nil), feedIntegrityChecks...)
}

func loadFeedIntegrityPolicy(db *gorm.DB, tenant string) models.FeedIntegrityPolicy {
	var policy models.FeedIntegrityPolicy
	if err := db.Where("tenant_id = ?", tenant).First(&policy).Error; err != nil {
		policy = models.DefaultFeedIntegrityPolicy(tenant)
		_ = db.Where("tenant_id = ?", tenant).FirstOrCreate(&policy).Error
	}
	if policy.EdgePagesPerFeed < 1 {
		policy.EdgePagesPerFeed = 1
	}
	if policy.EdgePagesPerFeed > 5 {
		policy.EdgePagesPerFeed = 5
	}
	if policy.ProbeURLBudget < 0 {
		policy.ProbeURLBudget = 0
	}
	if policy.ProbeURLBudget > 100 {
		policy.ProbeURLBudget = 100
	}
	if policy.ProbeTimeoutMS < 500 {
		policy.ProbeTimeoutMS = 500
	}
	if policy.ProbeTimeoutMS > 10000 {
		policy.ProbeTimeoutMS = 10000
	}
	if policy.ConfirmRuns < 1 {
		policy.ConfirmRuns = 1
	}
	if policy.ResolveRuns < 1 {
		policy.ResolveRuns = 1
	}
	return policy
}

func feedIntegrityTryStart(tenant string) bool {
	feedIntegrityMu.Lock()
	defer feedIntegrityMu.Unlock()
	if feedIntegrityRunning[tenant] {
		return false
	}
	feedIntegrityRunning[tenant] = true
	return true
}
func feedIntegrityFinish(tenant string) {
	feedIntegrityMu.Lock()
	delete(feedIntegrityRunning, tenant)
	feedIntegrityMu.Unlock()
}

func withFeedIntegrityLock(ctx context.Context, db *gorm.DB, tenant string) (func(), bool) {
	sqlDB, err := db.DB()
	if err != nil {
		return func() {}, false
	}
	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		return func() {}, false
	}
	key := int64(0)
	for _, b := range []byte("wahb:feed-integrity:" + tenant) {
		key = key*31 + int64(b)
	}
	var acquired bool
	if err := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&acquired); err != nil || !acquired {
		conn.Close()
		return func() {}, false
	}
	return func() {
		_, _ = conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", key)
		_ = conn.Close()
	}, true
}

type integrityResult struct {
	Feed, Variant, ConsumerVerdict, ReadinessVerdict string
	ConsumerScore, ReadinessScore                    float64
	Violations                                       int
	Checked                                          int
}

func runFeedIntegrity(db *gorm.DB, tenant string, opts feedIntegrityRunOptions) (models.FeedIntegrityRun, error) {
	if !feedIntegrityTryStart(tenant) {
		return models.FeedIntegrityRun{}, fmt.Errorf("feed integrity run already running")
	}
	defer feedIntegrityFinish(tenant)
	ctx, cancel := context.WithTimeout(context.Background(), feedIntegrityMaxRun)
	defer cancel()
	unlock, ok := withFeedIntegrityLock(ctx, db, tenant)
	if !ok {
		return models.FeedIntegrityRun{}, fmt.Errorf("feed integrity run already running")
	}
	defer unlock()

	now := time.Now().UTC()
	_ = db.Model(&models.FeedIntegrityRun{}).Where("tenant_id = ? AND status = ? AND started_at < ?", tenant, models.FeedIntegrityRunRunning, now.Add(-feedIntegrityMaxRun)).Updates(map[string]interface{}{"status": models.FeedIntegrityRunFailed, "error_class": "abandoned", "error": "run exceeded hard deadline", "finished_at": now}).Error
	policy := loadFeedIntegrityPolicy(db, tenant)
	tier := opts.Tier
	if tier != models.FeedIntegrityTierDeep {
		tier = models.FeedIntegrityTierLight
	}
	run := models.FeedIntegrityRun{TenantID: tenant, Trigger: feedIntegrityOr(opts.Trigger, "manual"), Tier: tier, Status: models.FeedIntegrityRunRunning, Headline: "running", StartedAt: now, CreatedBy: feedIntegrityOr(opts.CreatedBy, "automation"), ErrorClass: "none"}
	if err := db.Create(&run).Error; err != nil {
		return run, err
	}

	findings := make([]models.FeedIntegrityFinding, 0)
	var findingsMu sync.Mutex
	results := map[string]*integrityResult{"foryou": {Feed: "foryou", Variant: "all", ConsumerVerdict: models.FeedIntegrityVerdictHealthy, ReadinessVerdict: models.FeedIntegrityVerdictHealthy, ConsumerScore: 100, ReadinessScore: 100}, "news": {Feed: "news", Variant: "all", ConsumerVerdict: models.FeedIntegrityVerdictHealthy, ReadinessVerdict: models.FeedIntegrityVerdictHealthy, ConsumerScore: 100, ReadinessScore: 100}}
	laneResults := map[string]map[string]int{"inventory": {"executed": 1}, "edge": {"executed": 1}, "probe": {"executed": 0}}
	add := func(key, lane, feed, variant, axis, severity, status, targetType, target string, candidates int, evidence interface{}) {
		findingsMu.Lock()
		defer findingsMu.Unlock()
		raw, _ := json.Marshal(evidence)
		findings = append(findings, models.FeedIntegrityFinding{RunID: run.ID, TenantID: tenant, Lane: lane, CheckKey: key, Feed: feed, Variant: variant, Axis: axis, Severity: severity, Status: status, TargetType: targetType, TargetRef: target, CandidateCount: candidates, AffectedCount: 1, SampleCount: candidates, Evidence: datatypes.JSON(raw)})
		variantKey := feed + ":" + variant
		if results[variantKey] == nil {
			results[variantKey] = &integrityResult{Feed: feed, Variant: variant, ConsumerVerdict: models.FeedIntegrityVerdictHealthy, ReadinessVerdict: models.FeedIntegrityVerdictHealthy, ConsumerScore: 100, ReadinessScore: 100}
		}
		for _, res := range []*integrityResult{results[feed], results[variantKey]} {
			res.Checked += candidates
			if status == "violation" {
				res.Violations++
				if axis == models.FeedIntegrityAxisConsumer {
					res.ConsumerScore -= scorePenalty(severity)
					res.ConsumerVerdict = worsenIntegrityVerdict(res.ConsumerVerdict, severity)
				} else {
					res.ReadinessScore -= scorePenalty(severity)
					res.ReadinessVerdict = worsenIntegrityVerdict(res.ReadinessVerdict, severity)
				}
			}
		}
	}

	runFeedIntegrityInventory(db, tenant, add)
	probeURLs := runFeedIntegrityEdge(ctx, db, policy, tier, add)
	if tier == models.FeedIntegrityTierDeep {
		laneResults["probe"]["executed"] = 1
		runFeedIntegrityProbes(ctx, policy, probeURLs, add)
	}
	for _, r := range results {
		if r.ConsumerScore < 0 {
			r.ConsumerScore = 0
		}
		if r.ReadinessScore < 0 {
			r.ReadinessScore = 0
		}
	}
	if len(findings) > 0 {
		if err := db.Create(&findings).Error; err != nil {
			run.Error = err.Error()
		}
	}
	updateFeedIntegrityEpisodes(db, tenant, policy, run, findings)
	updateFeedIntegrityCheckerEpisode(db, tenant, run, findings)
	feedJSON, _ := json.Marshal(results)
	laneJSON, _ := json.Marshal(laneResults)
	counts := map[string]int{"findings": len(findings), "violations": 0, "check_errors": 0}
	for _, f := range findings {
		if f.Status == "violation" {
			counts["violations"]++
		} else if f.Status == "check_error" {
			counts["check_errors"]++
		}
	}
	countsJSON, _ := json.Marshal(counts)
	headline := "all_clear"
	if counts["violations"] > 0 {
		headline = "watching"
	}
	status := models.FeedIntegrityRunCompleted
	if run.Error != "" || counts["check_errors"] > 0 {
		status = models.FeedIntegrityRunPartial
	}
	finished := time.Now().UTC()
	run.Status, run.Headline, run.FinishedAt, run.FeedResults, run.Counts, run.LaneResults = status, headline, &finished, datatypes.JSON(feedJSON), datatypes.JSON(countsJSON), datatypes.JSON(laneJSON)
	run.Summary = fmt.Sprintf("%s CMS edge checks: %d violation(s) across %d finding(s)", tier, counts["violations"], len(findings))
	_ = db.Model(&models.FeedIntegrityRun{}).Where("id = ?", run.ID).Updates(map[string]interface{}{"status": run.Status, "headline": run.Headline, "finished_at": finished, "feed_results": run.FeedResults, "counts": run.Counts, "lane_results": run.LaneResults, "summary": run.Summary, "error": run.Error, "updated_at": finished}).Error
	if tier == models.FeedIntegrityTierDeep {
		_ = db.Model(&models.FeedIntegrityPolicy{}).Where("id = ?", policy.ID).Updates(map[string]interface{}{"last_deep_run_at": finished, "last_light_run_at": finished, "updated_at": finished}).Error
	} else {
		_ = db.Model(&models.FeedIntegrityPolicy{}).Where("id = ?", policy.ID).Updates(map[string]interface{}{"last_light_run_at": finished, "updated_at": finished}).Error
	}
	_ = evaluateFeedIntegrityAutopilot(db, run.ID)
	return run, nil
}

func updateFeedIntegrityCheckerEpisode(db *gorm.DB, tenant string, run models.FeedIntegrityRun, findings []models.FeedIntegrityFinding) {
	hasError := false
	for _, finding := range findings {
		if finding.Status == "check_error" {
			hasError = true
			break
		}
	}
	if !hasError {
		now := time.Now().UTC()
		var ep models.FeedIntegrityEpisode
		if db.Where("tenant_id=? AND check_key=? AND scope=? AND status IN ?", tenant, "checker_unhealthy", "checker", []string{models.FeedIntegrityEpisodeOpen, models.FeedIntegrityEpisodeRecovering}).First(&ep).Error == nil {
			if ep.Status == models.FeedIntegrityEpisodeOpen {
				_ = db.Model(&ep).Updates(map[string]interface{}{"status": models.FeedIntegrityEpisodeRecovering, "recovering_since": now, "clean_streak": 1, "violation_streak": 0}).Error
			} else if ep.CleanStreak+1 >= loadFeedIntegrityPolicy(db, tenant).ResolveRuns {
				_ = db.Model(&ep).Updates(map[string]interface{}{"status": models.FeedIntegrityEpisodeResolved, "resolved_at": now, "clean_streak": ep.CleanStreak + 1}).Error
			} else {
				_ = db.Model(&ep).Update("clean_streak", ep.CleanStreak+1).Error
			}
		}
		return
	}
	var previous models.FeedIntegrityRun
	if eligibleRunQuery(db, tenant, "edge").Where("started_at < ?", run.StartedAt).Order("started_at DESC").First(&previous).Error != nil {
		return
	}
	var previousErrors int64
	db.Model(&models.FeedIntegrityFinding{}).Where("run_id=? AND status='check_error'", previous.ID).Count(&previousErrors)
	if previousErrors == 0 {
		return
	}
	now := time.Now().UTC()
	var ep models.FeedIntegrityEpisode
	err := db.Where("tenant_id=? AND check_key=? AND scope=? AND status IN ?", tenant, "checker_unhealthy", "checker", []string{models.FeedIntegrityEpisodeOpen, models.FeedIntegrityEpisodeRecovering}).First(&ep).Error
	if err == nil {
		_ = db.Model(&ep).Updates(map[string]interface{}{"status": models.FeedIntegrityEpisodeOpen, "last_seen_at": now, "violation_streak": ep.ViolationStreak + 1, "clean_streak": 0, "recovering_since": nil}).Error
		return
	}
	ep = models.FeedIntegrityEpisode{TenantID: tenant, CheckKey: "checker_unhealthy", Axis: models.FeedIntegrityAxisReadiness, Feed: "platform", Variant: "cms-edge", Scope: "checker", Status: models.FeedIntegrityEpisodeOpen, Severity: "major", Summary: feedIntegritySummary("checker_unhealthy"), FirstDetectedAt: now, LastSeenAt: now, ViolationStreak: 2}
	_ = db.Create(&ep).Error
}

func scorePenalty(severity string) float64 {
	switch severity {
	case "critical":
		return 40
	case "major":
		return 15
	case "minor":
		return 4
	default:
		return 0
	}
}

func feedIntegrityOr(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
func worsenIntegrityVerdict(current, severity string) string {
	if severity == "critical" {
		return models.FeedIntegrityVerdictBroken
	}
	if severity == "major" && current != models.FeedIntegrityVerdictBroken {
		return models.FeedIntegrityVerdictDegradedMajor
	}
	if severity == "minor" && current == models.FeedIntegrityVerdictHealthy {
		return models.FeedIntegrityVerdictDegradedMinor
	}
	return current
}

type integrityAdd func(key, lane, feed, variant, axis, severity, status, targetType, target string, candidates int, evidence interface{})

func runFeedIntegrityInventory(db *gorm.DB, tenant string, add integrityAdd) {
	var items []models.ContentItem
	base := db.Where("tenant_id = ? AND type IN ? AND is_feed_unit = TRUE AND feed_visibility = ?", tenant, []models.ContentType{models.ContentTypeVideo, models.ContentTypePodcast}, feedVisibilityVisible)
	base.Where("duration_sec IS NOT NULL").Find(&items)
	for _, item := range items {
		id := item.PublicID.String()
		if item.Status == models.ContentStatusArchived && item.DurationSec != nil && *item.DurationSec >= forYouMinDurationSec && *item.DurationSec <= forYouHardMaxDurationSec && (item.PlaybackURL != nil || item.MediaURL != nil) && item.ThumbnailURL != nil {
			add("inv_fy_status_contract", "inventory", "foryou", "default", models.FeedIntegrityAxisReadiness, "major", "violation", "content_item", id, 1, map[string]interface{}{"status": item.Status})
		}
		if item.DurationSec == nil || *item.DurationSec < forYouMinDurationSec || *item.DurationSec > forYouHardMaxDurationSec {
			add("inv_fy_bounds", "inventory", "foryou", "default", models.FeedIntegrityAxisReadiness, "major", "violation", "content_item", id, 1, map[string]interface{}{"duration_sec": item.DurationSec})
		}
		if item.ParentContentItemID == nil && item.DurationSec != nil && *item.DurationSec > forYouHardMaxDurationSec && item.Status == models.ContentStatusReady {
			add("inv_fy_parent_leak", "inventory", "foryou", "default", models.FeedIntegrityAxisReadiness, "major", "violation", "content_item", id, 1, nil)
		}
		if item.DurationSec != nil && *item.DurationSec >= forYouMinDurationSec && *item.DurationSec <= forYouHardMaxDurationSec && (item.PlaybackURL == nil || strings.TrimSpace(*item.PlaybackURL) == "") && (item.MediaURL == nil || strings.TrimSpace(*item.MediaURL) == "") {
			add("inv_fy_playback_missing", "inventory", "foryou", "default", models.FeedIntegrityAxisReadiness, "major", "violation", "content_item", id, 1, nil)
		}
		if item.DurationSec != nil && *item.DurationSec >= forYouMinDurationSec && *item.DurationSec <= forYouHardMaxDurationSec && (item.PlaybackURL != nil || item.MediaURL != nil) && (item.ThumbnailURL == nil || strings.TrimSpace(*item.ThumbnailURL) == "") {
			add("inv_fy_thumb_missing", "inventory", "foryou", "default", models.FeedIntegrityAxisReadiness, "info", "violation", "content_item", id, 1, nil)
		}
		if item.DurationSec != nil && item.DurationBucket != nil && *item.DurationBucket != durationBucketLabel(*item.DurationSec*1000) {
			add("inv_fy_bucket_mismatch", "inventory", "foryou", "default", models.FeedIntegrityAxisReadiness, "minor", "violation", "content_item", id, 1, map[string]interface{}{"actual": *item.DurationBucket, "expected": durationBucketLabel(*item.DurationSec * 1000)})
		}
		if len(item.MediaRenditions) > 0 {
			var v []map[string]interface{}
			if json.Unmarshal(item.MediaRenditions, &v) != nil {
				add("inv_fy_renditions_malformed", "inventory", "foryou", "default", models.FeedIntegrityAxisReadiness, "minor", "violation", "content_item", id, 1, nil)
			}
		}
	}
	var stories []models.Story
	db.Where("tenant_id = ?", tenant).Find(&stories)
	storySet := map[uuid.UUID]bool{}
	for _, s := range stories {
		storySet[s.PublicID] = true
	}
	for _, story := range stories {
		if story.LastMemberAt != nil && story.LastMemberAt.After(time.Now().Add(-7*24*time.Hour)) {
			var readyMembers int64
			db.Model(&models.ContentItem{}).Where("tenant_id = ? AND story_id = ? AND status = ?", tenant, story.PublicID, models.ContentStatusReady).Count(&readyMembers)
			if readyMembers == 0 {
				add("inv_news_empty_story", "inventory", "news", "today", models.FeedIntegrityAxisReadiness, "major", "violation", "story", story.PublicID.String(), 1, nil)
			}
			if !story.Labeled && time.Since(*story.LastMemberAt) > time.Hour {
				add("inv_news_unlabeled_stale", "inventory", "news", "today", models.FeedIntegrityAxisReadiness, "info", "violation", "story", story.PublicID.String(), 1, nil)
			}
		}
		if len(story.RelatedIDs) == 0 {
			continue
		}
		var refs []string
		if json.Unmarshal(story.RelatedIDs, &refs) != nil {
			continue
		}
		for _, ref := range refs {
			id, err := uuid.Parse(ref)
			if err == nil && !storySet[id] {
				add("inv_news_related_dangling", "inventory", "news", "today", models.FeedIntegrityAxisReadiness, "minor", "violation", "story", story.PublicID.String(), 1, map[string]interface{}{"related_id": ref})
			}
		}
	}
	var snaps []models.NewsSnapshot
	db.Where("tenant_id = ?", tenant).Find(&snaps)
	for _, snap := range snaps {
		// Rebuild debt = the cached row is dirty (new content landed, awaiting a
		// rebuild) OR it has aged past the max-stale ceiling. Dirty must count:
		// a dirty snapshot is exactly what the edge lane can observe served to a
		// consumer, and the Safe Auto refresh gate requires BOTH signals to
		// agree on the same window. Keying inventory only on age would leave the
		// dual-evidence gate permanently unsatisfiable for the common
		// dirty-but-recent case.
		if snap.Dirty || time.Since(snap.BuiltAt) > newsSnapshotMaxStale {
			add("inv_news_cache_rebuild_debt", "inventory", "news", snap.Window, models.FeedIntegrityAxisReadiness, "minor", "violation", "snapshot", snap.Window, 1, map[string]interface{}{"age_seconds": int(time.Since(snap.BuiltAt).Seconds()), "dirty": snap.Dirty})
		}
	}
}

func feedIntegritySelfURL() string {
	port := strings.TrimSpace(os.Getenv("PORT"))
	if port == "" {
		port = "8080"
	}
	return "http://127.0.0.1:" + port
}

func runFeedIntegrityEdge(ctx context.Context, db *gorm.DB, policy models.FeedIntegrityPolicy, tier string, add integrityAdd) []string {
	client := &http.Client{Timeout: time.Duration(policy.ForYouLatencyBudgetMS+1000) * time.Millisecond}
	urls := []string{}
	variants := []string{"", "5", "10", "15", "20", "30", "40"}
	for _, duration := range variants {
		variant := "default"
		path := "/api/v1/feed/foryou?limit=20"
		if duration != "" {
			variant = "duration:" + duration + "m"
			path += "&duration=" + duration
		}
		started := time.Now()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, feedIntegritySelfURL()+path, nil)
		req.Header.Set(feedIntegritySyntheticHdr, feedIntegrityCapability)
		resp, err := client.Do(req)
		elapsed := time.Since(started)
		if err != nil || resp.StatusCode != http.StatusOK {
			add("edge_fy_http", "edge", "foryou", variant, models.FeedIntegrityAxisConsumer, "critical", "violation", "page", variant, 1, map[string]interface{}{"error": safeIntegrityError(err), "status": httpStatus(resp)})
			if resp != nil {
				resp.Body.Close()
			}
			continue
		}
		var payload struct {
			Cursor *string `json:"cursor"`
			Items  []struct {
				ID           string `json:"id"`
				Type         string `json:"type"`
				Title        string `json:"title"`
				ThumbnailURL string `json:"thumbnail_url"`
				DurationSec  int    `json:"duration_sec"`
				PlaybackURL  string `json:"playback_url"`
				PlaybackType string `json:"playback_type"`
				IsArchived   bool   `json:"is_archived"`
			} `json:"items"`
		}
		err = json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(&payload)
		resp.Body.Close()
		if err != nil {
			add("edge_fy_http", "edge", "foryou", variant, models.FeedIntegrityAxisConsumer, "critical", "check_error", "page", variant, 1, map[string]interface{}{"error": safeIntegrityError(err)})
			continue
		}
		if policy.ForYouLatencyBudgetMS > 0 && elapsed > time.Duration(policy.ForYouLatencyBudgetMS)*time.Millisecond {
			add("edge_fy_latency", "edge", "foryou", variant, models.FeedIntegrityAxisConsumer, "major", "violation", "page", variant, 1, map[string]interface{}{"latency_ms": elapsed.Milliseconds(), "budget_ms": policy.ForYouLatencyBudgetMS})
		}
		if len(payload.Items) == 0 {
			var count int64
			forYouEligibleMediaQuery(db, supportsAtomizedForYouSchema(db)).Count(&count)
			sev := "critical"
			if count < int64(policy.ExpectedMinForYouUnits) {
				sev = "info"
			}
			add("edge_fy_empty", "edge", "foryou", variant, models.FeedIntegrityAxisConsumer, sev, "violation", "page", variant, int(count), nil)
		}
		seen := map[string]bool{}
		for _, item := range payload.Items {
			if seen[item.ID] {
				add("edge_fy_dup", "edge", "foryou", variant, models.FeedIntegrityAxisConsumer, "major", "violation", "content_item", item.ID, 1, nil)
			}
			seen[item.ID] = true
			// A present playback URL is the contract; playback_type is advisory
			// and legitimately empty for legacy MP4-fallback units (media_url
			// only, no explicit type). Do not require playback_type here — that
			// flagged every valid MP4 compatibility item as a major violation.
			if item.ID == "" || (item.Type != "VIDEO" && item.Type != "PODCAST") || item.Title == "" || item.ThumbnailURL == "" || item.DurationSec <= 0 || item.PlaybackURL == "" {
				add("edge_fy_required_metadata", "edge", "foryou", variant, models.FeedIntegrityAxisConsumer, "major", "violation", "content_item", item.ID, 1, nil)
			}
			if item.DurationSec < forYouMinDurationSec || item.DurationSec > forYouHardMaxDurationSec {
				add("edge_fy_bounds_served", "edge", "foryou", variant, models.FeedIntegrityAxisConsumer, "major", "violation", "content_item", item.ID, 1, map[string]interface{}{"duration_sec": item.DurationSec})
			}
			// Only a NON-EMPTY, unrecognized playback_type is a contract
			// violation. Empty type with a valid URL is the legacy fallback.
			if item.PlaybackType != "" && item.PlaybackType != "hls" && item.PlaybackType != "mp4" && item.PlaybackType != "audio" {
				add("edge_fy_playback_fields", "edge", "foryou", variant, models.FeedIntegrityAxisConsumer, "major", "violation", "content_item", item.ID, 1, map[string]interface{}{"playback_type": item.PlaybackType})
			}
			if item.IsArchived {
				add("edge_fy_status_served", "edge", "foryou", variant, models.FeedIntegrityAxisConsumer, "major", "violation", "content_item", item.ID, 1, nil)
			}
			if item.PlaybackURL != "" {
				urls = append(urls, item.PlaybackURL)
			}
		}
		cursor := payload.Cursor
		for page := 1; page < policy.EdgePagesPerFeed && cursor != nil && *cursor != ""; page++ {
			nextReq, _ := http.NewRequestWithContext(ctx, http.MethodGet, feedIntegritySelfURL()+path+"&cursor="+url.QueryEscape(*cursor), nil)
			nextReq.Header.Set(feedIntegritySyntheticHdr, feedIntegrityCapability)
			nextResp, nextErr := client.Do(nextReq)
			if nextErr != nil || nextResp.StatusCode != http.StatusOK {
				add("edge_fy_http", "edge", "foryou", variant, models.FeedIntegrityAxisConsumer, "critical", "check_error", "page", variant, 1, map[string]interface{}{"page": page + 1, "error": safeIntegrityError(nextErr), "status": httpStatus(nextResp)})
				if nextResp != nil {
					nextResp.Body.Close()
				}
				break
			}
			var next struct {
				Cursor *string `json:"cursor"`
				Items  []struct {
					ID          string `json:"id"`
					DurationSec int    `json:"duration_sec"`
					PlaybackURL string `json:"playback_url"`
				} `json:"items"`
			}
			nextErr = json.NewDecoder(io.LimitReader(nextResp.Body, 2<<20)).Decode(&next)
			nextResp.Body.Close()
			if nextErr != nil {
				add("edge_fy_http", "edge", "foryou", variant, models.FeedIntegrityAxisConsumer, "critical", "check_error", "page", variant, 1, map[string]interface{}{"page": page + 1, "error": safeIntegrityError(nextErr)})
				break
			}
			for _, item := range next.Items {
				if seen[item.ID] {
					add("edge_fy_dup", "edge", "foryou", variant, models.FeedIntegrityAxisConsumer, "major", "violation", "content_item", item.ID, 1, map[string]interface{}{"page": page + 1})
				}
				seen[item.ID] = true
				if item.DurationSec < forYouMinDurationSec || item.DurationSec > forYouHardMaxDurationSec {
					add("edge_fy_bounds_served", "edge", "foryou", variant, models.FeedIntegrityAxisConsumer, "major", "violation", "content_item", item.ID, 1, map[string]interface{}{"page": page + 1, "duration_sec": item.DurationSec})
				}
				if item.PlaybackURL != "" {
					urls = append(urls, item.PlaybackURL)
				}
			}
			cursor = next.Cursor
		}
	}
	newsVariants := []string{"today"}
	if tier == models.FeedIntegrityTierDeep {
		newsVariants = append(newsVariants, "week", "month")
	}
	for _, window := range newsVariants {
		started := time.Now()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, feedIntegritySelfURL()+"/api/v1/feed/news?limit=10&window="+window, nil)
		req.Header.Set(feedIntegritySyntheticHdr, feedIntegrityCapability)
		resp, err := client.Do(req)
		elapsed := time.Since(started)
		variant := "window:" + window
		if err != nil || resp.StatusCode != http.StatusOK {
			add("edge_news_http", "edge", "news", variant, models.FeedIntegrityAxisConsumer, "critical", "violation", "page", variant, 1, map[string]interface{}{"error": safeIntegrityError(err), "status": httpStatus(resp)})
			if resp != nil {
				resp.Body.Close()
			}
			continue
		}
		serveSource := resp.Header.Get("X-Wahb-Feed-Source")
		snapshotAgeMS, _ := strconv.ParseInt(resp.Header.Get("X-Wahb-Snapshot-Age-Ms"), 10, 64)
		snapshotWindow := resp.Header.Get("X-Wahb-Snapshot-Window")
		snapshotBuiltAt := resp.Header.Get("X-Wahb-Snapshot-Built-At")
		snapshotDirty := resp.Header.Get("X-Wahb-Snapshot-Dirty") == "true"
		var payload struct {
			Cursor *string `json:"cursor"`
			Slides []struct {
				Featured struct {
					StoryID string            `json:"story_id"`
					Title   string            `json:"title"`
					Label   string            `json:"label"`
					Members []json.RawMessage `json:"members"`
				} `json:"featured"`
				Related []json.RawMessage `json:"related"`
			} `json:"slides"`
		}
		err = json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(&payload)
		resp.Body.Close()
		if err != nil {
			add("edge_news_http", "edge", "news", variant, models.FeedIntegrityAxisConsumer, "critical", "check_error", "page", variant, 1, map[string]interface{}{"error": safeIntegrityError(err)})
			continue
		}
		if policy.NewsLatencyBudgetMS > 0 && elapsed > time.Duration(policy.NewsLatencyBudgetMS)*time.Millisecond {
			add("edge_news_latency", "edge", "news", variant, models.FeedIntegrityAxisConsumer, "major", "violation", "page", variant, 1, map[string]interface{}{"latency_ms": elapsed.Milliseconds(), "budget_ms": policy.NewsLatencyBudgetMS})
		}
		// Consumer-facing News staleness. The serving path only returns
		// Source="cache" when age <= newsSnapshotMaxStale (past that it
		// assembles live), so a pure `age > maxStale` test on a cache response
		// is unreachable. The real consumer damage is a DIRTY snapshot still
		// being served from cache — new content exists but the stale page is
		// shipped while a rebuild is pending — which IS served within the
		// max-stale window. Keep the age branch as a belt-and-suspenders guard
		// for any future serving-path change.
		if serveSource == "cache" && (snapshotDirty || snapshotAgeMS > newsSnapshotMaxStale.Milliseconds()) {
			add("edge_news_cache_stale", "edge", "news", variant, models.FeedIntegrityAxisConsumer, "major", "violation", "snapshot", window, 1, map[string]interface{}{"source": serveSource, "age_ms": snapshotAgeMS, "window": snapshotWindow, "built_at": snapshotBuiltAt, "dirty": snapshotDirty})
		}
		if len(payload.Slides) == 0 {
			var active int64
			db.Model(&models.Story{}).Where("tenant_id = ? AND last_member_at IS NOT NULL AND last_member_at > ?", policy.TenantID, time.Now().Add(-7*24*time.Hour)).Count(&active)
			sev := "critical"
			if active < int64(policy.ExpectedMinNewsSlides) {
				sev = "info"
			}
			add("edge_news_empty", "edge", "news", variant, models.FeedIntegrityAxisConsumer, sev, "violation", "page", variant, int(active), nil)
		}
		seen := map[string]bool{}
		for _, slide := range payload.Slides {
			if seen[slide.Featured.StoryID] {
				add("edge_news_dup", "edge", "news", variant, models.FeedIntegrityAxisConsumer, "major", "violation", "story", slide.Featured.StoryID, 1, nil)
			}
			seen[slide.Featured.StoryID] = true
			if slide.Featured.StoryID == "" || (slide.Featured.Title == "" && slide.Featured.Label == "") || len(slide.Featured.Members) == 0 || len(slide.Related) > 3 {
				add("edge_news_shape", "edge", "news", variant, models.FeedIntegrityAxisConsumer, "major", "violation", "story", slide.Featured.StoryID, 1, nil)
			}
		}
		cursor := payload.Cursor
		for page := 1; page < policy.EdgePagesPerFeed && cursor != nil && *cursor != ""; page++ {
			nextReq, _ := http.NewRequestWithContext(ctx, http.MethodGet, feedIntegritySelfURL()+"/api/v1/feed/news?limit=10&window="+window+"&cursor="+url.QueryEscape(*cursor), nil)
			nextReq.Header.Set(feedIntegritySyntheticHdr, feedIntegrityCapability)
			nextResp, nextErr := client.Do(nextReq)
			if nextErr != nil || nextResp.StatusCode != http.StatusOK {
				add("edge_news_http", "edge", "news", variant, models.FeedIntegrityAxisConsumer, "critical", "check_error", "page", variant, 1, map[string]interface{}{"page": page + 1, "error": safeIntegrityError(nextErr), "status": httpStatus(nextResp)})
				if nextResp != nil {
					nextResp.Body.Close()
				}
				break
			}
			var next struct {
				Cursor *string `json:"cursor"`
				Slides []struct {
					Featured struct {
						StoryID string            `json:"story_id"`
						Title   string            `json:"title"`
						Label   string            `json:"label"`
						Members []json.RawMessage `json:"members"`
					} `json:"featured"`
					Related []json.RawMessage `json:"related"`
				} `json:"slides"`
			}
			nextErr = json.NewDecoder(io.LimitReader(nextResp.Body, 2<<20)).Decode(&next)
			nextResp.Body.Close()
			if nextErr != nil {
				add("edge_news_http", "edge", "news", variant, models.FeedIntegrityAxisConsumer, "critical", "check_error", "page", variant, 1, map[string]interface{}{"page": page + 1, "error": safeIntegrityError(nextErr)})
				break
			}
			for _, slide := range next.Slides {
				if seen[slide.Featured.StoryID] {
					add("edge_news_dup", "edge", "news", variant, models.FeedIntegrityAxisConsumer, "major", "violation", "story", slide.Featured.StoryID, 1, map[string]interface{}{"page": page + 1})
				}
				seen[slide.Featured.StoryID] = true
				if slide.Featured.StoryID == "" || (slide.Featured.Title == "" && slide.Featured.Label == "") || len(slide.Featured.Members) == 0 || len(slide.Related) > 3 {
					add("edge_news_shape", "edge", "news", variant, models.FeedIntegrityAxisConsumer, "major", "violation", "story", slide.Featured.StoryID, 1, map[string]interface{}{"page": page + 1})
				}
			}
			cursor = next.Cursor
		}
	}
	return urls
}

func httpStatus(resp *http.Response) int {
	if resp == nil {
		return 0
	}
	return resp.StatusCode
}
func safeIntegrityError(err error) string {
	if err == nil {
		return ""
	}
	return strings.ReplaceAll(strings.Split(err.Error(), "?")[0], "\n", " ")
}

func runFeedIntegrityProbes(ctx context.Context, policy models.FeedIntegrityPolicy, rawURLs []string, add integrityAdd) {
	seen := map[string]bool{}
	urls := make([]string, 0)
	for _, raw := range rawURLs {
		if !seen[raw] {
			seen[raw] = true
			urls = append(urls, raw)
		}
		if len(urls) >= policy.ProbeURLBudget {
			break
		}
	}
	sem := make(chan struct{}, minIntegrity(policy.ProbeConcurrency, 4))
	var wg sync.WaitGroup
	for _, raw := range urls {
		raw := raw
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if err := probeIntegrityURL(ctx, raw, time.Duration(policy.ProbeTimeoutMS)*time.Millisecond); err != nil {
				add("probe_url_dead", "probe", "foryou", "default", models.FeedIntegrityAxisConsumer, "major", "violation", "url", redactIntegrityURL(raw), 1, map[string]interface{}{"reason": safeIntegrityError(err)})
			}
			if strings.Contains(strings.ToLower(strings.Split(raw, "?")[0]), ".m3u8") {
				if err := probeIntegrityHLS(ctx, raw, time.Duration(policy.ProbeTimeoutMS)*time.Millisecond); err != nil {
					add("probe_hls_manifest", "probe", "foryou", "default", models.FeedIntegrityAxisConsumer, "major", "violation", "url", redactIntegrityURL(raw), 1, map[string]interface{}{"reason": safeIntegrityError(err)})
				}
			}
		}()
	}
	wg.Wait()
}
func minIntegrity(a, b int) int {
	if a < 1 {
		return 1
	}
	if a < b {
		return a
	}
	return b
}

func probeIntegrityURL(ctx context.Context, raw string, timeout time.Duration) error {
	if err := validateIntegrityURL(ctx, raw); err != nil {
		return err
	}
	client := newIntegrityHTTPClient(timeout)
	req, _ := http.NewRequestWithContext(ctx, http.MethodHead, raw, nil)
	resp, err := client.Do(req)
	if err == nil && resp != nil {
		resp.Body.Close()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		if resp.StatusCode != http.StatusMethodNotAllowed && resp.StatusCode != http.StatusForbidden {
			return fmt.Errorf("http_%d", resp.StatusCode)
		}
	}
	req, _ = http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	req.Header.Set("Range", "bytes=0-1023")
	resp, err = client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("http_%d", resp.StatusCode)
	}
	return nil
}

func validateIntegrityURL(ctx context.Context, raw string) error {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
		return fmt.Errorf("unsafe_url")
	}
	host := u.Hostname()
	if host == "" {
		return fmt.Errorf("unsafe_url")
	}
	ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return fmt.Errorf("dns_failed")
	}
	for _, ip := range ips {
		if integrityPrivateIP(ip) {
			return fmt.Errorf("private_target")
		}
	}
	return nil
}

func newIntegrityHTTPClient(timeout time.Duration) *http.Client {
	dialer := &net.Dialer{Timeout: timeout, KeepAlive: 15 * time.Second}
	transport := &http.Transport{
		Proxy:                 nil,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          4,
		MaxIdleConnsPerHost:   2,
		IdleConnTimeout:       15 * time.Second,
		ResponseHeaderTimeout: timeout,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, fmt.Errorf("invalid_target")
			}
			ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
			if err != nil {
				return nil, fmt.Errorf("dns_failed")
			}
			for _, ip := range ips {
				if !integrityPrivateIP(ip) {
					return dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
				}
			}
			return nil, fmt.Errorf("private_target")
		},
	}
	return &http.Client{
		Timeout:   timeout,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 3 {
				return fmt.Errorf("too_many_redirects")
			}
			return validateIntegrityURL(req.Context(), req.URL.String())
		},
	}
}

func probeIntegrityHLS(ctx context.Context, raw string, timeout time.Duration) error {
	if err := validateIntegrityURL(ctx, raw); err != nil {
		return err
	}
	client := newIntegrityHTTPClient(timeout)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("http_%d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 256<<10))
	if err != nil {
		return err
	}
	text := string(body)
	if !strings.Contains(text, "#EXTM3U") || (!strings.Contains(text, "#EXT-X-STREAM-INF") && !strings.Contains(text, "#EXTINF")) {
		return fmt.Errorf("invalid_hls_manifest")
	}
	base, _ := url.Parse(raw)
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		child, err := url.Parse(line)
		if err != nil {
			return fmt.Errorf("invalid_hls_child")
		}
		childURL := base.ResolveReference(child).String()
		if strings.Contains(strings.ToLower(child.Path), ".m3u8") {
			return probeIntegrityHLSChild(ctx, childURL, timeout)
		}
		return probeIntegrityURL(ctx, childURL, timeout)
	}
	return fmt.Errorf("hls_has_no_child")
}

func probeIntegrityHLSChild(ctx context.Context, raw string, timeout time.Duration) error {
	if err := validateIntegrityURL(ctx, raw); err != nil {
		return err
	}
	client := newIntegrityHTTPClient(timeout)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("http_%d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 256<<10))
	if err != nil {
		return err
	}
	text := string(body)
	if !strings.Contains(text, "#EXTM3U") || !strings.Contains(text, "#EXTINF") {
		return fmt.Errorf("invalid_hls_child_manifest")
	}
	return nil
}
func integrityPrivateIP(ip netip.Addr) bool {
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified()
}
func redactIntegrityURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "invalid"
	}
	return u.Scheme + "://" + u.Host + u.Path
}

func updateFeedIntegrityEpisodes(db *gorm.DB, tenant string, policy models.FeedIntegrityPolicy, run models.FeedIntegrityRun, findings []models.FeedIntegrityFinding) {
	now := time.Now().UTC()
	active := map[string]models.FeedIntegrityFinding{}
	for _, f := range findings {
		if f.Status != "violation" {
			continue
		}
		active[f.CheckKey+"|"+f.Feed+"|"+f.Variant+"|"+feedIntegrityScope(f)] = f
	}
	openedByClass := map[string]int{}
	overflowByIdentity := map[string][]models.FeedIntegrityFinding{}
	aggregateActive := map[string]bool{}
	for _, f := range active {
		if isFeedIntegritySuppressed(db, tenant, f, now) {
			continue
		}
		var ep models.FeedIntegrityEpisode
		scope := feedIntegrityScope(f)
		err := db.Where("tenant_id=? AND check_key=? AND feed=? AND variant=? AND scope=? AND status IN ?", tenant, f.CheckKey, f.Feed, f.Variant, scope, []string{models.FeedIntegrityEpisodeOpen, models.FeedIntegrityEpisodeRecovering}).First(&ep).Error
		if err == nil {
			flaps := ep.FlapCount24h
			if ep.Status == models.FeedIntegrityEpisodeRecovering && ep.RecoveringSince != nil && now.Sub(*ep.RecoveringSince) <= 24*time.Hour {
				flaps++
			}
			_ = db.Model(&ep).Updates(map[string]interface{}{"status": models.FeedIntegrityEpisodeOpen, "last_seen_at": now, "severity": f.Severity, "recovering_since": nil, "violation_streak": ep.ViolationStreak + 1, "clean_streak": 0, "flap_count_24h": flaps}).Error
			continue
		}
		// Confirm across CONSECUTIVE eligible runs, not any-two-runs-ever: each
		// of the previous confirm_runs-1 eligible runs must also carry this
		// violation. A flake seen once last month + once today must not confirm.
		if !feedIntegrityConfirmed(db, tenant, policy, run, f) {
			continue
		}
		var recentClosed int64
		db.Model(&models.FeedIntegrityEpisode{}).Where("tenant_id=? AND check_key=? AND feed=? AND variant=? AND scope=? AND status=? AND updated_at > ?", tenant, f.CheckKey, f.Feed, f.Variant, scope, models.FeedIntegrityEpisodeClosed, now.Add(-time.Duration(policy.AutopilotCooldownMinutes)*time.Minute)).Count(&recentClosed)
		if recentClosed > 0 {
			continue
		}
		if openedByClass[f.CheckKey] >= 20 {
			overflowKey := f.CheckKey + "|" + f.Feed + "|" + f.Variant
			overflowByIdentity[overflowKey] = append(overflowByIdentity[overflowKey], f)
			continue
		}
		ep = models.FeedIntegrityEpisode{TenantID: tenant, CheckKey: f.CheckKey, Axis: f.Axis, Feed: f.Feed, Variant: f.Variant, Scope: scope, Status: models.FeedIntegrityEpisodeOpen, Severity: f.Severity, Summary: feedIntegritySummary(f.CheckKey), Evidence: f.Evidence, FirstDetectedAt: now, LastSeenAt: now, ViolationStreak: policy.ConfirmRuns}
		_ = db.Create(&ep).Error
		openedByClass[f.CheckKey]++
	}
	for _, overflow := range overflowByIdentity {
		if len(overflow) == 0 {
			continue
		}
		f := overflow[0]
		scope := "aggregate:" + f.CheckKey + ":" + f.Feed + ":" + f.Variant
		key := f.CheckKey + "|" + f.Feed + "|" + f.Variant + "|" + scope
		aggregateActive[key] = true
		examples := make([]string, 0, minIntegrity(len(overflow), 5))
		for _, row := range overflow {
			if len(examples) == 5 {
				break
			}
			if row.TargetRef != "" {
				examples = append(examples, row.TargetRef)
			}
		}
		evidence, _ := json.Marshal(gin.H{"affected_count": len(overflow), "sample_count": len(overflow), "examples": examples, "overflow": true})
		var aggregate models.FeedIntegrityEpisode
		if db.Where("tenant_id=? AND check_key=? AND feed=? AND variant=? AND scope=? AND status IN ?", tenant, f.CheckKey, f.Feed, f.Variant, scope, []string{models.FeedIntegrityEpisodeOpen, models.FeedIntegrityEpisodeRecovering}).First(&aggregate).Error == nil {
			_ = db.Model(&aggregate).Updates(map[string]interface{}{"status": models.FeedIntegrityEpisodeOpen, "severity": f.Severity, "last_seen_at": now, "evidence": datatypes.JSON(evidence), "affected_trend": datatypes.JSON(evidence), "recovering_since": nil, "clean_streak": 0, "updated_at": now}).Error
			continue
		}
		aggregate = models.FeedIntegrityEpisode{TenantID: tenant, CheckKey: f.CheckKey, Axis: f.Axis, Feed: f.Feed, Variant: f.Variant, Scope: scope, Status: models.FeedIntegrityEpisodeOpen, Severity: f.Severity, Summary: fmt.Sprintf("%d additional %s violations beyond the per-run episode cap", len(overflow), feedIntegritySummary(f.CheckKey)), Evidence: datatypes.JSON(evidence), AffectedTrend: datatypes.JSON(evidence), FirstDetectedAt: now, LastSeenAt: now, ViolationStreak: policy.ConfirmRuns}
		_ = db.Create(&aggregate).Error
	}
	var open []models.FeedIntegrityEpisode
	db.Where("tenant_id=? AND status IN ?", tenant, []string{models.FeedIntegrityEpisodeOpen, models.FeedIntegrityEpisodeRecovering}).Find(&open)
	for _, ep := range open {
		identity := ep.CheckKey + "|" + ep.Feed + "|" + ep.Variant + "|" + ep.Scope
		if _, ok := active[identity]; ok {
			continue
		}
		if aggregateActive[identity] {
			continue
		}
		if ep.Status == models.FeedIntegrityEpisodeOpen {
			_ = db.Model(&ep).Updates(map[string]interface{}{"status": models.FeedIntegrityEpisodeRecovering, "recovering_since": now, "clean_streak": 1, "violation_streak": 0}).Error
		} else if ep.RecoveringSince != nil && feedIntegrityCleanRunsSince(db, tenant, ep, *ep.RecoveringSince) >= int64(policy.ResolveRuns) {
			// resolve_runs is a count of clean eligible runs, not wall-clock
			// minutes — the previous `ResolveRuns * time.Minute` resolved an
			// episode on the very next run regardless of how many clean runs.
			_ = db.Model(&ep).Updates(map[string]interface{}{"status": models.FeedIntegrityEpisodeResolved, "resolved_at": now, "clean_streak": policy.ResolveRuns}).Error
		} else if ep.Status == models.FeedIntegrityEpisodeRecovering {
			_ = db.Model(&ep).Update("clean_streak", feedIntegrityCleanRunsSince(db, tenant, ep, *ep.RecoveringSince)).Error
		}
	}
}
func feedIntegritySummary(key string) string {
	for _, check := range feedIntegrityChecks {
		if check.Key == key {
			return check.Label
		}
	}
	return key
}

// feedIntegrityCheckLane returns a check's lane from the registry so episode
// eligibility knows whether the check only executes on deep runs.
func feedIntegrityCheckLane(key string) string {
	for _, check := range feedIntegrityChecks {
		if check.Key == key {
			return check.Lane
		}
	}
	return ""
}

// eligibleRunQuery scopes the completed/partial runs that count as history for a
// given lane. Probe-lane checks only execute on deep runs, so light runs must
// not count as clean/confirming evidence for them.
func eligibleRunQuery(db *gorm.DB, tenant, lane string) *gorm.DB {
	q := db.Model(&models.FeedIntegrityRun{}).
		Where("tenant_id=? AND status IN ?", tenant, []string{models.FeedIntegrityRunCompleted, models.FeedIntegrityRunPartial})
	if lane == "probe" {
		q = q.Where("tier = ?", models.FeedIntegrityTierDeep)
	}
	return q
}

// feedIntegrityConfirmed reports whether the previous confirm_runs-1 eligible
// runs each carry this exact violation (consecutive confirmation).
func feedIntegrityConfirmed(db *gorm.DB, tenant string, policy models.FeedIntegrityPolicy, run models.FeedIntegrityRun, f models.FeedIntegrityFinding) bool {
	need := policy.ConfirmRuns - 1
	if need <= 0 {
		return true
	}
	var priorRuns []models.FeedIntegrityRun
	eligibleRunQuery(db, tenant, f.Lane).
		Where("started_at < ?", run.StartedAt).
		Order("started_at DESC").Limit(need).Find(&priorRuns)
	if len(priorRuns) < need {
		return false // not enough eligible history to confirm yet
	}
	for _, pr := range priorRuns {
		var c int64
		db.Model(&models.FeedIntegrityFinding{}).
			Where("run_id=? AND check_key=? AND feed=? AND variant=? AND status='violation'", pr.ID, f.CheckKey, f.Feed, f.Variant).
			Count(&c)
		if c == 0 {
			return false
		}
	}
	return true
}

// feedIntegrityCleanRunsSince counts eligible runs after the episode entered
// recovering. The episode reverts to open the moment it re-violates, so these
// are consecutive clean runs by construction.
func feedIntegrityCleanRunsSince(db *gorm.DB, tenant string, ep models.FeedIntegrityEpisode, since time.Time) int64 {
	var n int64
	eligibleRunQuery(db, tenant, feedIntegrityCheckLane(ep.CheckKey)).
		Where("started_at > ?", since).Count(&n)
	return n
}
func isFeedIntegritySuppressed(db *gorm.DB, tenant string, f models.FeedIntegrityFinding, now time.Time) bool {
	var n int64
	db.Model(&models.FeedIntegritySuppression{}).Where("tenant_id=? AND check_key=? AND starts_at <= ? AND expires_at > ? AND revoked_at IS NULL", tenant, f.CheckKey, now, now).Where("feed='' OR feed IS NULL OR feed=?", f.Feed).Where("variant='' OR variant IS NULL OR variant=?", f.Variant).Where("scope='' OR scope IS NULL OR scope=?", feedIntegrityScope(f)).Count(&n)
	return n > 0
}

func feedIntegrityStatus(db *gorm.DB, tenant string) gin.H {
	policy := loadFeedIntegrityPolicy(db, tenant)
	var latestRun interface{}
	var run models.FeedIntegrityRun
	if err := db.Where("tenant_id=?", tenant).Order("started_at DESC").First(&run).Error; err == nil {
		latestRun = run
	}
	var eps []models.FeedIntegrityEpisode
	db.Where("tenant_id=? AND status IN ?", tenant, []string{models.FeedIntegrityEpisodeOpen, models.FeedIntegrityEpisodeRecovering}).Order("last_seen_at DESC").Find(&eps)
	var suppressions []models.FeedIntegritySuppression
	db.Where("tenant_id=? AND revoked_at IS NULL AND expires_at > ?", tenant, time.Now().UTC()).Find(&suppressions)
	return gin.H{"policy": policy, "latest_run": latestRun, "open_episodes": eps, "active_suppressions": suppressions, "checks": feedIntegrityCheckCatalog()}
}

func GetFeedIntegrityStatus(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	c.JSON(http.StatusOK, gin.H{"data": feedIntegrityStatus(db, principal.TenantID)})
}
func GetFeedIntegrityPolicy(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	c.JSON(http.StatusOK, gin.H{"data": loadFeedIntegrityPolicy(db, principal.TenantID)})
}
func UpdateFeedIntegrityPolicy(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	policy := loadFeedIntegrityPolicy(db, principal.TenantID)
	var patch map[string]interface{}
	if c.ShouldBindJSON(&patch) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid policy"})
		return
	}
	allowed := map[string]bool{"scheduled_enabled": true, "light_interval_minutes": true, "deep_interval_hours": true, "confirm_runs": true, "resolve_runs": true, "edge_pages_per_feed": true, "probe_url_budget": true, "probe_concurrency": true, "probe_timeout_ms": true, "foryou_latency_budget_ms": true, "news_latency_budget_ms": true, "expected_min_foryou_units": true, "expected_min_news_slides": true}
	updates := map[string]interface{}{}
	for k, v := range patch {
		if allowed[k] {
			updates[k] = v
		}
	}
	if len(updates) > 0 {
		updates["updated_at"] = time.Now().UTC()
		_ = db.Model(&policy).Updates(updates).Error
	}
	_ = db.Where("id=?", policy.ID).First(&policy).Error
	feedIntegrityAudit(db, principal, "feed_integrity.policy.update", principal.TenantID, "success", updates)
	c.JSON(http.StatusOK, gin.H{"data": policy})
}
func RunFeedIntegrityNow(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	var req struct {
		Tier string `json:"tier"`
	}
	_ = c.ShouldBindJSON(&req)
	if req.Tier != "deep" {
		req.Tier = "light"
	}
	db := c.MustGet("db").(*gorm.DB)
	run, err := runFeedIntegrity(db, principal.TenantID, feedIntegrityRunOptions{Trigger: "manual", CreatedBy: principal.Email, Tier: req.Tier})
	if err != nil {
		feedIntegrityAudit(db, principal, "feed_integrity.run", principal.TenantID, "failure", map[string]interface{}{"error": err.Error()})
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	feedIntegrityAudit(db, principal, "feed_integrity.run", run.PublicID.String(), "success", map[string]interface{}{"tier": req.Tier})
	c.JSON(http.StatusOK, gin.H{"data": run})
}
func PauseFeedIntegritySchedule(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	var req struct {
		Minutes int `json:"minutes"`
	}
	_ = c.ShouldBindJSON(&req)
	db := c.MustGet("db").(*gorm.DB)
	policy := loadFeedIntegrityPolicy(db, principal.TenantID)
	var until *time.Time
	if req.Minutes > 0 {
		t := time.Now().UTC().Add(time.Duration(req.Minutes) * time.Minute)
		until = &t
	}
	_ = db.Model(&policy).Updates(map[string]interface{}{"paused_until": until, "updated_at": time.Now().UTC()}).Error
	feedIntegrityAudit(db, principal, "feed_integrity.schedule.pause", principal.TenantID, "success", map[string]interface{}{"minutes": req.Minutes})
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"paused_until": until}})
}
func ListFeedIntegrityRuns(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var rows []models.FeedIntegrityRun
	db.Where("tenant_id=?", principal.TenantID).Order("started_at DESC").Limit(50).Find(&rows)
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"items": rows}})
}
func GetFeedIntegrityRun(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid run id"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var run models.FeedIntegrityRun
	if db.Where("tenant_id=? AND public_id=?", principal.TenantID, id).First(&run).Error != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "run not found"})
		return
	}
	var findings []models.FeedIntegrityFinding
	db.Where("run_id=?", run.ID).Order("status DESC,created_at DESC").Find(&findings)
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"run": run, "findings": findings}})
}
func ListFeedIntegrityFindings(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	q := db.Where("tenant_id=?", principal.TenantID)
	if v := strings.TrimSpace(c.Query("check")); v != "" {
		q = q.Where("check_key=?", v)
	}
	if v := strings.TrimSpace(c.Query("feed")); v != "" {
		q = q.Where("feed=?", v)
	}
	if v := strings.TrimSpace(c.Query("status")); v != "" {
		q = q.Where("status=?", v)
	}
	var rows []models.FeedIntegrityFinding
	q.Order("created_at DESC").Limit(200).Find(&rows)
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"items": rows}})
}
func ListFeedIntegrityEpisodes(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var rows []models.FeedIntegrityEpisode
	db.Where("tenant_id=?", principal.TenantID).Order("last_seen_at DESC").Limit(100).Find(&rows)
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"items": rows}})
}
func GetFeedIntegrityEpisode(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid episode id"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var episode models.FeedIntegrityEpisode
	if db.Where("tenant_id=? AND public_id=?", principal.TenantID, id).First(&episode).Error != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "episode not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": episode})
}
func CloseFeedIntegrityEpisode(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid episode id"})
		return
	}
	var req struct {
		ReasonClass string `json:"reason_class"`
		Notes       string `json:"notes"`
	}
	_ = c.ShouldBindJSON(&req)
	allowedReasons := map[string]bool{"resolved": true, "false_positive": true, "expected": true, "duplicate": true}
	if !allowedReasons[req.ReasonClass] {
		c.JSON(http.StatusBadRequest, gin.H{"error": "reason_class must be resolved, false_positive, expected, or duplicate"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var ep models.FeedIntegrityEpisode
	if db.Where("tenant_id=? AND public_id=?", principal.TenantID, id).First(&ep).Error != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "episode not found"})
		return
	}
	now := time.Now().UTC()
	_ = db.Model(&ep).Updates(map[string]interface{}{"status": models.FeedIntegrityEpisodeClosed, "closed_by": principal.Email, "close_reason_class": req.ReasonClass, "close_notes": req.Notes, "resolved_at": now}).Error
	_ = db.Where("id=?", ep.ID).First(&ep).Error
	feedIntegrityAudit(db, principal, "feed_integrity.episode.close", ep.PublicID.String(), "success", map[string]interface{}{"reason_class": req.ReasonClass})
	c.JSON(http.StatusOK, gin.H{"data": ep})
}
func CreateFeedIntegritySuppression(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	var req struct {
		TTLMinutes int    `json:"ttl_minutes"`
		Reason     string `json:"reason"`
		Feed       string `json:"feed"`
		Variant    string `json:"variant"`
		Scope      string `json:"scope"`
	}
	if c.ShouldBindJSON(&req) != nil || strings.TrimSpace(req.Reason) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "reason is required"})
		return
	}
	if req.TTLMinutes < 1 {
		req.TTLMinutes = 60
	}
	if req.TTLMinutes > 10080 {
		req.TTLMinutes = 10080
	}
	check := c.Param("key")
	if !feedIntegrityKnownCheck(check) {
		c.JSON(http.StatusNotFound, gin.H{"error": "check not found"})
		return
	}
	now := time.Now().UTC()
	row := models.FeedIntegritySuppression{TenantID: principal.TenantID, CheckKey: check, Feed: req.Feed, Variant: req.Variant, Scope: req.Scope, Reason: req.Reason, StartsAt: now, ExpiresAt: now.Add(time.Duration(req.TTLMinutes) * time.Minute), CreatedBy: principal.Email}
	db := c.MustGet("db").(*gorm.DB)
	if db.Create(&row).Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not create suppression"})
		return
	}
	feedIntegrityAudit(db, principal, "feed_integrity.suppression.create", row.PublicID.String(), "success", map[string]interface{}{"check": check})
	c.JSON(http.StatusCreated, gin.H{"data": row})
}
func DeleteFeedIntegritySuppression(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid suppression id"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var row models.FeedIntegritySuppression
	if db.Where("tenant_id=? AND public_id=?", principal.TenantID, id).First(&row).Error != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "suppression not found"})
		return
	}
	now := time.Now().UTC()
	_ = db.Model(&row).Updates(map[string]interface{}{"revoked_at": now, "revoked_by": principal.Email}).Error
	feedIntegrityAudit(db, principal, "feed_integrity.suppression.revoke", row.PublicID.String(), "success", nil)
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"revoked_at": now}})
}
func GetFeedIntegrityChecks(c *gin.Context) {
	if _, ok := requireAdminPrincipal(c); !ok {
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"items": feedIntegrityCheckCatalog()}})
}
func feedIntegrityKnownCheck(key string) bool {
	for _, check := range feedIntegrityChecks {
		if check.Key == key {
			return true
		}
	}
	return false
}
func feedIntegrityAudit(db *gorm.DB, p utils.AdminPrincipal, action, resource, status string, payload map[string]interface{}) {
	raw, _ := json.Marshal(payload)
	_ = db.Create(&models.AuditLog{TenantID: p.TenantID, UserID: p.UserID, UserEmail: p.Email, Action: action, TargetService: "cms", TargetResource: resource, Status: status, Payload: datatypes.JSON(raw)}).Error
}

func StartFeedIntegrityHeartbeat(db *gorm.DB) {
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		runFeedIntegrityDue(db)
		for range ticker.C {
			runFeedIntegrityDue(db)
		}
	}()
}
func runFeedIntegrityDue(db *gorm.DB) {
	evaluatePendingFeedIntegrityRuns(db)
	processFeedIntegrityActions(db)
	sweepFeedIntegrityRetention(db)
	var policies []models.FeedIntegrityPolicy
	if err := db.Where("scheduled_enabled = TRUE").Order("tenant_id ASC").Limit(100).Find(&policies).Error; err != nil {
		return
	}
	sem := make(chan struct{}, 2)
	var wg sync.WaitGroup
	for _, policy := range policies {
		policy := policy
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			runFeedIntegrityTenantDue(db, policy)
		}()
	}
	wg.Wait()
}

func runFeedIntegrityTenantDue(db *gorm.DB, policy models.FeedIntegrityPolicy) {
	now := time.Now().UTC()
	if !policy.ScheduledEnabled || (policy.PausedUntil != nil && policy.PausedUntil.After(now)) {
		return
	}
	tier := ""
	if policy.LastDeepRunAt == nil || now.Sub(*policy.LastDeepRunAt) >= time.Duration(policy.DeepIntervalHours)*time.Hour {
		tier = models.FeedIntegrityTierDeep
	} else if policy.LastLightRunAt == nil || now.Sub(*policy.LastLightRunAt) >= time.Duration(policy.LightIntervalMinutes)*time.Minute {
		tier = models.FeedIntegrityTierLight
	}
	if tier == "" {
		return
	}
	_, _ = runFeedIntegrity(db, policy.TenantID, feedIntegrityRunOptions{Trigger: "scheduled", CreatedBy: "automation", Tier: tier})
}
