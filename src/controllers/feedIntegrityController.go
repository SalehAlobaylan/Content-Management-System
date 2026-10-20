package controllers

// Feed Integrity is deliberately a deterministic, read-only supervisor. It owns
// its ledger and attention objects, but never changes feed/content state.

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
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

const (
	feedIntegrityTenant       = "default"
	feedIntegritySyntheticHdr = "X-Wahb-Synthetic"
	feedIntegrityMaxRun       = 10 * time.Minute
)

var (
	feedIntegrityMu                   sync.Mutex
	feedIntegrityRunning              = map[string]bool{}
	feedIntegrityCapability           = newFeedIntegrityCapability()
	errFeedIntegrityTenantUnavailable = errors.New("feed integrity is currently available only for the default public tenant")
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

type feedIntegrityRunOptions struct {
	Trigger, CreatedBy, Tier string
	CorrelationID            *uuid.UUID
	TriggerRef               string
}

type feedIntegrityCheck struct {
	Key, Label, Lane, Feed, Axis, OwnerSurface string
	Severity                                   string
}

var feedIntegrityChecks = []feedIntegrityCheck{
	{"inv_pods_status_contract", "Archived item matches the Pods serving predicate", "inventory", "pods", models.FeedIntegrityAxisReadiness, "content", "major"},
	{"inv_pods_bounds", "Visible media duration outside 270-2400 seconds", "inventory", "pods", models.FeedIntegrityAxisReadiness, "media-studio", "major"},
	{"inv_pods_parent_leak", "Long parent remains a visible raw feed unit", "inventory", "pods", models.FeedIntegrityAxisReadiness, "media-studio", "major"},
	{"inv_pods_playback_missing", "Visible feed unit has no legal playback URL", "inventory", "pods", models.FeedIntegrityAxisReadiness, "storage", "major"},
	{"inv_pods_thumb_missing", "Otherwise-servable feed unit has no thumbnail", "inventory", "pods", models.FeedIntegrityAxisReadiness, "media-circulation", "info"},
	{"inv_pods_bucket_mismatch", "Duration bucket does not match duration", "inventory", "pods", models.FeedIntegrityAxisReadiness, "media-studio", "minor"},
	{"inv_pods_renditions_malformed", "Media renditions are malformed", "inventory", "pods", models.FeedIntegrityAxisReadiness, "storage", "minor"},
	{"inv_news_related_dangling", "Story references a missing related story", "inventory", "news", models.FeedIntegrityAxisReadiness, "news", "minor"},
	{"inv_news_empty_story", "Active story has no ready members", "inventory", "news", models.FeedIntegrityAxisReadiness, "news", "major"},
	{"inv_news_unlabeled_stale", "Feed-eligible story remains unlabeled", "inventory", "news", models.FeedIntegrityAxisReadiness, "enrichment", "info"},
	{"inv_news_cache_rebuild_debt", "News snapshot remains stale beyond cache ceiling", "inventory", "news", models.FeedIntegrityAxisReadiness, "news", "minor"},
	{"inv_news_generation_membership_drift", "Eligible News stories are absent from the active serving generation", "inventory", "news", models.FeedIntegrityAxisReadiness, "news", "major"},
	{"edge_pods_http", "Pods CMS endpoint is unavailable", "edge", "pods", models.FeedIntegrityAxisConsumer, "content", "critical"},
	{"edge_pods_latency", "Pods page exceeded its latency budget", "edge", "pods", models.FeedIntegrityAxisConsumer, "content", "major"},
	{"edge_pods_empty", "Pods page is empty despite eligible inventory", "edge", "pods", models.FeedIntegrityAxisConsumer, "media-circulation", "critical"},
	{"edge_pods_required_metadata", "Served Pods item lacks required metadata", "edge", "pods", models.FeedIntegrityAxisConsumer, "content", "major"},
	{"edge_pods_bounds_served", "Served Pods item has invalid duration", "edge", "pods", models.FeedIntegrityAxisConsumer, "content", "major"},
	{"edge_pods_playback_fields", "Served Pods item has invalid playback fields", "edge", "pods", models.FeedIntegrityAxisConsumer, "storage", "major"},
	{"edge_pods_status_served", "Served Pods item is archived", "edge", "pods", models.FeedIntegrityAxisConsumer, "content", "major"},
	{"edge_pods_dup", "Pods cursor walk repeats an item", "edge", "pods", models.FeedIntegrityAxisConsumer, "content", "major"},
	{"edge_pods_family_dup", "Pods cursor walk serves two members of one redundancy family", "edge", "pods", models.FeedIntegrityAxisConsumer, "content", "major"},
	{"edge_news_http", "News CMS endpoint is unavailable", "edge", "news", models.FeedIntegrityAxisConsumer, "content", "critical"},
	{"edge_news_latency", "News page exceeded its latency budget", "edge", "news", models.FeedIntegrityAxisConsumer, "content", "major"},
	{"edge_news_empty", "News page is empty despite active stories", "edge", "news", models.FeedIntegrityAxisConsumer, "news", "critical"},
	{"edge_news_thin", "Singleton News story has no related coverage", "edge", "news", models.FeedIntegrityAxisReadiness, "news-circulation", "info"},
	{"edge_news_source_monopoly", "News first page is monopolized by one source", "edge", "news", models.FeedIntegrityAxisReadiness, "news-circulation", "minor"},
	{"edge_news_shape", "News slide has an invalid shape", "edge", "news", models.FeedIntegrityAxisConsumer, "news", "major"},
	{"edge_news_dup", "News cursor walk repeats a story", "edge", "news", models.FeedIntegrityAxisConsumer, "news", "major"},
	{"edge_news_cache_stale", "News served a snapshot beyond its stale ceiling", "edge", "news", models.FeedIntegrityAxisConsumer, "news-circulation", "major"},
	{"probe_url_dead", "Primary playback URL is unavailable", "probe", "pods", models.FeedIntegrityAxisConsumer, "storage", "major"},
	{"probe_hls_manifest", "HLS manifest is invalid", "probe", "pods", models.FeedIntegrityAxisConsumer, "storage", "major"},
	{"edge_pods_fill", "Pods page is below available supply", "edge", "pods", models.FeedIntegrityAxisConsumer, "media-circulation", "minor"},
	{"edge_pods_cursor", "Pods cursor does not progress", "edge", "pods", models.FeedIntegrityAxisConsumer, "content", "major"},
	{"edge_pods_duration_match", "Pods item misses requested duration tolerance", "edge", "pods", models.FeedIntegrityAxisConsumer, "content", "major"},
	{"edge_news_cursor", "News cursor does not progress", "edge", "news", models.FeedIntegrityAxisConsumer, "news-circulation", "major"},
	{"probe_fallback", "Declared fallback is unavailable", "probe", "pods", models.FeedIntegrityAxisConsumer, "storage", "minor"},
	{"probe_thumbnail", "Optional artwork is unavailable", "probe", "pods", models.FeedIntegrityAxisReadiness, "storage", "info"},
	{"probe_content_type", "Media response contradicts declared type", "probe", "pods", models.FeedIntegrityAxisConsumer, "storage", "major"},

	{"checker_unhealthy", "Feed Integrity check coverage is incomplete", "checker", "platform", models.FeedIntegrityAxisReadiness, "system-health", "major"},
}

func feedIntegrityCheckCatalog() []feedIntegrityCheck {
	return append([]feedIntegrityCheck(nil), feedIntegrityChecks...)
}

func readFeedIntegrityPolicy(db *gorm.DB, tenant string) (models.FeedIntegrityPolicy, error) {
	var policy models.FeedIntegrityPolicy
	err := db.Where("tenant_id=?", tenant).First(&policy).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		policy = models.DefaultFeedIntegrityPolicy(tenant)
		err = db.Clauses(clause.OnConflict{DoNothing: true}).Create(&policy).Error
		if err == nil {
			err = db.Where("tenant_id=?", tenant).First(&policy).Error
		}
	}
	if err == nil {
		sanitizeFeedIntegrityAutopilotPolicy(&policy)
		policy.EdgePagesPerFeed = max(1, min(5, policy.EdgePagesPerFeed))
		policy.ProbeURLBudget = max(0, min(100, policy.ProbeURLBudget))
		policy.ProbeConcurrency = max(1, min(4, policy.ProbeConcurrency))
		policy.ProbeTimeoutMS = max(500, min(10000, policy.ProbeTimeoutMS))
	}
	return policy, err
}
func loadFeedIntegrityPolicy(db *gorm.DB, tenant string) models.FeedIntegrityPolicy {
	policy, _ := readFeedIntegrityPolicy(db, tenant)
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

func withFeedIntegrityLock(ctx context.Context, db *gorm.DB, tenant string) (func(), bool, error) {
	sqlDB, err := db.DB()
	if err != nil {
		return func() {}, false, err
	}
	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		return func() {}, false, err
	}
	// Keep the guard transaction alive until the entire owner run finishes.
	// Transaction pooling cannot safely retain a session advisory lock.
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		conn.Close()
		return func() {}, false, err
	}
	key := int64(0)
	for _, b := range []byte("wahb:feed-integrity:" + tenant) {
		key = key*31 + int64(b)
	}
	var acquired bool
	err = tx.QueryRowContext(ctx, "SELECT pg_try_advisory_xact_lock($1)", key).Scan(&acquired)
	if err != nil || !acquired {
		_ = tx.Rollback()
		conn.Close()
		return func() {}, false, err
	}
	return func() {
		_ = tx.Rollback()
		_ = conn.Close()
	}, true, nil
}

type integrityResult struct {
	Feed             string  `json:"feed"`
	Variant          string  `json:"variant"`
	ConsumerVerdict  string  `json:"consumer_verdict"`
	ReadinessVerdict string  `json:"readiness_verdict"`
	ConsumerScore    float64 `json:"consumer_score"`
	ReadinessScore   float64 `json:"readiness_score"`
	Violations       int     `json:"violations"`
	Checked          int     `json:"checked"`
}

func runFeedIntegrity(db *gorm.DB, tenant string, opts feedIntegrityRunOptions) (models.FeedIntegrityRun, error) {
	// Edge probes exercise V1 public routes, whose tenant contract is default
	// only. Refuse another tenant rather than mixing its inventory with default
	// consumer evidence.
	if publicTenant, err := utils.GetConfiguredPublicTenantID(); err != nil || tenant != publicTenant {
		return models.FeedIntegrityRun{}, errFeedIntegrityTenantUnavailable
	}
	if !feedIntegrityTryStart(tenant) {
		return models.FeedIntegrityRun{}, fmt.Errorf("feed integrity run already running")
	}
	defer feedIntegrityFinish(tenant)
	ctx, cancel := context.WithTimeout(context.Background(), feedIntegrityMaxRun)
	defer cancel()
	unlock, ok, lockErr := withFeedIntegrityLock(ctx, db, tenant)
	if lockErr != nil {
		return models.FeedIntegrityRun{}, fmt.Errorf("feed integrity owner guard unavailable: %w", lockErr)
	}
	if !ok {
		return models.FeedIntegrityRun{}, fmt.Errorf("feed integrity run already running")
	}
	defer unlock()

	now := time.Now().UTC()
	if err := db.WithContext(ctx).Model(&models.FeedIntegrityRun{}).Where("tenant_id = ? AND status = ? AND started_at < ?", tenant, models.FeedIntegrityRunRunning, now.Add(-feedIntegrityMaxRun)).Updates(map[string]interface{}{"status": models.FeedIntegrityRunFailed, "error_class": "abandoned", "error": "run exceeded hard deadline", "finished_at": now}).Error; err != nil {
		return models.FeedIntegrityRun{}, err
	}
	policy, err := readFeedIntegrityPolicy(db, tenant)
	if err != nil {
		return models.FeedIntegrityRun{}, err
	}
	tier := opts.Tier
	if tier != models.FeedIntegrityTierDeep {
		tier = models.FeedIntegrityTierLight
	}
	if tier == models.FeedIntegrityTierDeep {
		capacityDone, capacityErr := beginWorkloadSweep(db, tenant, "feed_integrity_deep", feedIntegrityOr(opts.Trigger, "manual"))
		if capacityErr != nil {
			return models.FeedIntegrityRun{}, capacityErr
		}
		defer capacityDone()
	}
	run := models.FeedIntegrityRun{TenantID: tenant, Trigger: feedIntegrityOr(opts.Trigger, "manual"), Tier: tier, Status: models.FeedIntegrityRunRunning, Headline: "running", StartedAt: now, CreatedBy: feedIntegrityOr(opts.CreatedBy, "automation"), ErrorClass: "none", CorrelationID: opts.CorrelationID, TriggerRef: opts.TriggerRef}
	if err := db.Create(&run).Error; err != nil {
		return run, err
	}
	failRead := func(err error) (models.FeedIntegrityRun, error) {
		finished := time.Now().UTC()
		run.Status, run.Headline, run.ErrorClass, run.Error, run.FinishedAt = "failed", "inconclusive", "evidence_read_failed", "initial evidence unavailable", &finished
		_ = db.Model(&run).Updates(map[string]interface{}{"status": run.Status, "headline": run.Headline, "error_class": run.ErrorClass, "error": run.Error, "finished_at": finished}).Error
		return run, err
	}

	var suppressions []models.FeedIntegritySuppression
	var episodes []models.FeedIntegrityEpisode
	if err := db.WithContext(ctx).Where("tenant_id=?", tenant).Find(&suppressions).Error; err != nil {
		return failRead(err)
	}
	if err := db.WithContext(ctx).Where("tenant_id=? AND status IN ?", tenant, []string{"open", "recovering"}).Find(&episodes).Error; err != nil {
		return failRead(err)
	}
	collector := newIntegrityCollector(run, suppressions, episodes)
	guarded := func(lane string, fn func()) {
		defer func() {
			if recover() != nil {
				for _, o := range collector.observations {
					if feedIntegrityCheckLane(o.CheckKey) == lane {
						collector.add(o.CheckKey, lane, o.Feed, o.Variant, o.Axis, o.Severity, "check_error", "checker", lane, 0, map[string]interface{}{"reason": "evaluator_panic"})
					}
				}
			}
		}()
		fn()
	}
	guarded("inventory", func() { collectFeedIntegrityInventory(ctx, db, tenant, collector) })
	var targets []integrityProbeTarget
	guarded("edge", func() { targets = collectFeedIntegrityEdge(ctx, db, policy, tier, collector) })
	if tier == models.FeedIntegrityTierDeep {
		guarded("probe", func() {
			targets = append(integrityOpenProbeTargets(db.WithContext(ctx), tenant, episodes, collector), targets...)
			collectFeedIntegrityProbes(ctx, policy, targets, collector)
		})
	}
	collector.checkerObservations()
	collector.aggregateFindings()
	lanes := collector.lanes()
	observations := []integrityObservation{}
	for _, lane := range lanes {
		observations = append(observations, lane.Observations...)
	}
	results := integrityResults(observations)
	counts := map[string]int{"findings": len(collector.findings), "violations": 0, "check_errors": 0, "checked": 0}
	headline := "all_clear"
	for _, o := range observations {
		counts["checked"] += o.SampleCount
		if o.Severity != "info" {
			counts["violations"] += o.AffectedCount
		} else {
			counts["informational_debt"] += o.AffectedCount
		}
		if o.Outcome == "check_error" || (strings.HasPrefix(o.Outcome, "skipped:") && o.Outcome != "skipped:not_applicable") {
			counts["check_errors"]++
			headline = "inconclusive"
		}
		if o.Outcome == "not_ready" && headline == "all_clear" {
			headline = "not_ready"
		}
	}
	if counts["violations"] > 0 && headline == "all_clear" {
		headline = "watching"
	}
	status := models.FeedIntegrityRunCompleted
	if counts["check_errors"] > 0 || ctx.Err() != nil {
		status = models.FeedIntegrityRunPartial
	}
	finished := time.Now().UTC()
	run.Status, run.Headline, run.FinishedAt = status, headline, &finished
	run.FeedResults = integrityJSON(results)
	run.Counts = integrityJSON(counts)
	run.LaneResults = integrityJSON(lanes)
	run.Summary = fmt.Sprintf("%s CMS edge checks: %d affected, %d coverage gaps", tier, counts["violations"], counts["check_errors"])
	if err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Serialize finalization with human changes and action admission.
		var locked models.FeedIntegrityPolicy
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("tenant_id=?", tenant).First(&locked).Error; err != nil {
			return err
		}
		if len(collector.findings) > 0 {
			if err := tx.CreateInBatches(&collector.findings, 500).Error; err != nil {
				return err
			}
		}
		if err := updateFeedIntegrityEpisodes(tx, tenant, locked, run, collector.findings); err != nil {
			return err
		}
		var active int64
		if err := tx.Model(&models.FeedIntegrityEpisode{}).Where("tenant_id=? AND status IN ?", tenant, []string{"open", "recovering"}).Count(&active).Error; err != nil {
			return err
		}
		if active > 0 {
			run.Headline = "attention"
		}
		if err := tx.Model(&models.FeedIntegrityRun{}).Where("id=?", run.ID).Updates(map[string]interface{}{"status": run.Status, "headline": run.Headline, "finished_at": finished, "feed_results": run.FeedResults, "counts": run.Counts, "lane_results": run.LaneResults, "summary": run.Summary, "updated_at": finished}).Error; err != nil {
			return err
		}
		update := map[string]interface{}{"last_light_run_at": finished, "updated_at": finished}
		if tier == "deep" {
			update["last_deep_run_at"] = finished
		}
		return tx.Model(&locked).Updates(update).Error
	}); err != nil {
		run.Status, run.Headline, run.ErrorClass, run.Error = "failed", "inconclusive", "finalization_failed", "evidence finalization failed"
		_ = db.Model(&run).Updates(map[string]interface{}{"status": "failed", "headline": "inconclusive", "error_class": "finalization_failed", "error": "evidence finalization failed", "finished_at": finished}).Error
		return run, fmt.Errorf("feed integrity finalization failed: %w", err)
	}
	if run.Status == models.FeedIntegrityRunCompleted {
		_ = evaluateFeedIntegrityAutopilot(db, run.ID)
	}
	return run, nil
}
func integrityJSON(v interface{}) datatypes.JSON {
	raw, _ := json.Marshal(v)
	return datatypes.JSON(raw)
}

func scorePenalty(severity string) float64 {
	switch severity {
	case "critical":
		return 3
	case "major":
		return 2
	case "minor":
		return 1
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

func feedIntegritySelfURL() string {
	port := strings.TrimSpace(os.Getenv("PORT"))
	if port == "" {
		port = "8080"
	}
	return "http://127.0.0.1:" + port
}

func httpStatus(resp *http.Response) int {
	if resp == nil {
		return 0
	}
	return resp.StatusCode
}

func activeRedundancyFamilyForContent(db *gorm.DB, contentID string) uint {
	var familyID uint
	if contentID == "" {
		return 0
	}
	db.Table("redundancy_family_members m").Joins("JOIN redundancy_families f ON f.id = m.family_id").Where("m.content_item_id = ? AND m.ended_at IS NULL AND f.status = ?", contentID, "active").Limit(1).Pluck("m.family_id", &familyID)
	return familyID
}
func safeIntegrityError(err error) string {
	if err == nil {
		return ""
	}
	var requestError *url.Error
	if errors.As(err, &requestError) {
		return requestError.Op + " " + redactIntegrityURL(requestError.URL) + ": " + safeIntegrityError(requestError.Err)
	}
	return strings.ReplaceAll(strings.Split(err.Error(), "?")[0], "\n", " ")
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
	client := newIntegrityHTTPClient(timeout)
	err, _ := probeIntegrityTarget(ctx, client, integrityProbeTarget{URL: raw, Role: "primary"})
	return err
}

func validateIntegrityURL(ctx context.Context, raw string) error {
	return validateIntegrityURLWithResolver(ctx, raw, net.DefaultResolver.LookupNetIP)
}
func validateIntegrityURLWithResolver(ctx context.Context, raw string, lookup func(context.Context, string, string) ([]netip.Addr, error)) error {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
		return fmt.Errorf("unsafe_url")
	}
	host := u.Hostname()
	if host == "" {
		return fmt.Errorf("unsafe_url")
	}
	ips, err := lookup(ctx, "ip", host)
	if err != nil || len(ips) == 0 {
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
				if integrityPrivateIP(ip) {
					return nil, fmt.Errorf("private_target")
				}
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
	return probeIntegrityManifest(ctx, newIntegrityHTTPClient(timeout), raw, false)
}
func probeIntegrityHLSChild(ctx context.Context, raw string, timeout time.Duration) error {
	return probeIntegrityManifest(ctx, newIntegrityHTTPClient(timeout), raw, true)
}
func integrityPrivateIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified()
}
func redactIntegrityURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "invalid"
	}
	return u.Scheme + "://" + u.Host + u.Path
}

func updateFeedIntegrityEpisodes(db *gorm.DB, tenant string, policy models.FeedIntegrityPolicy, run models.FeedIntegrityRun, findings []models.FeedIntegrityFinding) error {
	now := time.Now().UTC()
	var suppressed []models.FeedIntegritySuppression
	if err := db.Where("tenant_id=?", tenant).Find(&suppressed).Error; err != nil {
		return err
	}
	var existing []models.FeedIntegrityEpisode
	if err := db.Where("tenant_id=? AND status IN ?", tenant, []string{"open", "recovering"}).Find(&existing).Error; err != nil {
		return err
	}
	existingByScope := map[string]models.FeedIntegrityEpisode{}
	for _, ep := range existing {
		existingByScope[ep.CheckKey+"|"+ep.Feed+"|"+ep.Variant+"|"+ep.Scope] = ep
	}
	active := map[string]models.FeedIntegrityFinding{}
	for _, f := range findings {
		if f.Status != "violation" || f.Severity == "info" || (feedIntegrityCheckLane(f.CheckKey) == "probe" && run.Tier != "deep") {
			continue
		}
		active[f.CheckKey+"|"+f.Feed+"|"+f.Variant+"|"+feedIntegrityScope(f)] = f
	}
	// Bounded examples may revisit many existing identities. Load human
	// boundaries and the at-most-confirm_runs history once, rather than issuing
	// per-target queries against a remote database.
	closedBoundaries := map[string]time.Time{}
	var closed []models.FeedIntegrityEpisode
	keys, scopes := []string{}, []string{}
	for _, f := range active {
		keys = append(keys, f.CheckKey)
		scopes = append(scopes, feedIntegrityScope(f))
	}
	if len(active) > 0 {
		if err := db.Select("DISTINCT ON (check_key,feed,variant,scope) check_key,feed,variant,scope,updated_at").Where("tenant_id=? AND status=? AND check_key IN ? AND scope IN ?", tenant, models.FeedIntegrityEpisodeClosed, keys, scopes).Order("check_key,feed,variant,scope,updated_at DESC").Find(&closed).Error; err != nil {
			return err
		}
	}
	for _, ep := range closed {
		closedBoundaries[ep.CheckKey+"|"+ep.Feed+"|"+ep.Variant+"|"+ep.Scope] = ep.UpdatedAt
	}
	boundaryFor := func(f models.FeedIntegrityFinding) time.Time {
		scope := feedIntegrityScope(f)
		boundary := closedBoundaries[f.CheckKey+"|"+f.Feed+"|"+f.Variant+"|"+scope]
		for _, s := range suppressed {
			if s.CheckKey != f.CheckKey || (s.Feed != "" && s.Feed != f.Feed) || (s.Variant != "" && s.Variant != f.Variant) || (s.Scope != "" && s.Scope != scope) {
				continue
			}
			end := s.ExpiresAt
			if s.RevokedAt != nil {
				end = *s.RevokedAt
			}
			if end.After(boundary) {
				boundary = end
			}
		}
		return boundary
	}
	history := map[bool][]models.FeedIntegrityRun{}
	confirm := func(f models.FeedIntegrityFinding) bool {
		o, ok := integrityObservationFor(run, f.CheckKey, f.Feed, f.Variant)
		if !ok || integrityScopeOutcome(o, feedIntegrityScope(f)) != "violation" {
			return false
		}
		boundary := boundaryFor(f)
		if !run.StartedAt.After(boundary) {
			return false
		}
		need := policy.ConfirmRuns - 1
		if need <= 0 {
			return true
		}
		deep := integrityFindingRequiresDeep(f)
		rows, loaded := history[deep]
		if !loaded {
			q := db.Where("tenant_id=? AND started_at<? AND status IN ?", tenant, run.StartedAt, []string{"completed", "partial", "failed"})
			if deep {
				q = q.Where("tier='deep'")
			}
			if err := q.Order("started_at DESC").Limit(need).Find(&rows).Error; err != nil {
				return false
			}
			history[deep] = rows
		}
		if len(rows) < need {
			return false
		}
		for _, pr := range rows {
			if pr.Status == "failed" || !pr.StartedAt.After(boundary) {
				return false
			}
			o, ok := integrityObservationFor(pr, f.CheckKey, f.Feed, f.Variant)
			if !ok || integrityScopeOutcome(o, feedIntegrityScope(f)) != "violation" {
				return false
			}
		}
		return true
	}
	openedByClass := map[string]int{}
	overflowByIdentity := map[string][]models.FeedIntegrityFinding{}
	aggregateActive := map[string]bool{}
	for _, f := range active {
		if integritySuppressed(suppressed, f, feedIntegrityScope(f), now) {
			continue
		}
		scope := feedIntegrityScope(f)
		ep, found := existingByScope[f.CheckKey+"|"+f.Feed+"|"+f.Variant+"|"+scope]
		var err error = gorm.ErrRecordNotFound
		if found {
			err = nil
		}
		if err == nil {
			if integrityEpisodeLastRun(ep) == run.ID {
				continue
			}
			boundary := boundaryFor(f)
			if boundary.After(ep.LastSeenAt) && !confirm(f) {
				continue
			}
			if f.CheckKey == "edge_news_thin" && !integrityEpisodeVersioned(ep) {
				continue
			}
			evidence, flaps := integrityEpisodeRunEvidence(ep, f, run.ID, now, ep.Status == models.FeedIntegrityEpisodeRecovering)

			if err := db.Model(&ep).Updates(map[string]interface{}{"status": models.FeedIntegrityEpisodeOpen, "last_seen_at": now, "severity": f.Severity, "recovering_since": nil, "violation_streak": ep.ViolationStreak + 1, "clean_streak": 0, "flap_count_24h": flaps, "evidence": evidence, "affected_trend": integrityTrend(ep, now, f.AffectedCount), "updated_at": now}).Error; err != nil {
				return err
			}
			continue
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		// Confirm across CONSECUTIVE eligible runs, not any-two-runs-ever: each
		// of the previous confirm_runs-1 eligible runs must also carry this
		// violation. A flake seen once last month + once today must not confirm.
		if !confirm(f) {
			continue
		}
		if openedByClass[f.CheckKey] >= 20 {
			overflowKey := f.CheckKey + "|" + f.Feed + "|" + f.Variant
			overflowByIdentity[overflowKey] = append(overflowByIdentity[overflowKey], f)
			continue
		}
		onset := run.StartedAt
		if prior := history[integrityFindingRequiresDeep(f)]; len(prior) > 0 {
			onset = prior[len(prior)-1].StartedAt
		}
		ep = models.FeedIntegrityEpisode{TenantID: tenant, CheckKey: f.CheckKey, Axis: f.Axis, Feed: f.Feed, Variant: f.Variant, Scope: scope, Status: models.FeedIntegrityEpisodeOpen, Severity: f.Severity, Summary: feedIntegritySummary(f.CheckKey), Evidence: integrityEpisodeEvidence(f), AffectedTrend: integrityTrend(models.FeedIntegrityEpisode{}, now, f.AffectedCount), Attribution: integrityAttribution(db, tenant, f, onset), FirstDetectedAt: onset, LastSeenAt: now, ViolationStreak: policy.ConfirmRuns}
		if err := db.Create(&ep).Error; err != nil {
			return err
		}
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
		affected := len(overflow)
		if o, ok := integrityObservationFor(run, f.CheckKey, f.Feed, f.Variant); ok {
			affected = max(affected, o.AffectedCount-20)
		}
		evidence, _ := json.Marshal(gin.H{"observation_version": 1, "affected_count": affected, "sample_count": len(overflow), "examples": examples, "overflow": true, "last_observed_run_id": run.ID})
		var aggregate models.FeedIntegrityEpisode
		if db.Where("tenant_id=? AND check_key=? AND feed=? AND variant=? AND scope=? AND status IN ?", tenant, f.CheckKey, f.Feed, f.Variant, scope, []string{models.FeedIntegrityEpisodeOpen, models.FeedIntegrityEpisodeRecovering}).First(&aggregate).Error == nil {
			if err := db.Model(&aggregate).Updates(map[string]interface{}{"status": models.FeedIntegrityEpisodeOpen, "severity": f.Severity, "last_seen_at": now, "evidence": datatypes.JSON(evidence), "affected_trend": integrityTrend(aggregate, now, len(overflow)), "recovering_since": nil, "clean_streak": 0, "updated_at": now}).Error; err != nil {
				return err
			}
			continue
		}
		aggregate = models.FeedIntegrityEpisode{TenantID: tenant, CheckKey: f.CheckKey, Axis: f.Axis, Feed: f.Feed, Variant: f.Variant, Scope: scope, Status: models.FeedIntegrityEpisodeOpen, Severity: f.Severity, Summary: fmt.Sprintf("%d additional %s violations beyond the per-run episode cap", len(overflow), feedIntegritySummary(f.CheckKey)), Evidence: datatypes.JSON(evidence), AffectedTrend: integrityTrend(models.FeedIntegrityEpisode{}, now, len(overflow)), FirstDetectedAt: now, LastSeenAt: now, ViolationStreak: policy.ConfirmRuns}
		if err := db.Create(&aggregate).Error; err != nil {
			return err
		}
	}
	var open []models.FeedIntegrityEpisode
	if err := db.Where("tenant_id=? AND status IN ?", tenant, []string{models.FeedIntegrityEpisodeOpen, models.FeedIntegrityEpisodeRecovering}).Find(&open).Error; err != nil {
		return err
	}
	for _, ep := range open {
		if feedIntegrityCheckLane(ep.CheckKey) == "probe" && run.Tier != "deep" {
			continue
		}
		identity := ep.CheckKey + "|" + ep.Feed + "|" + ep.Variant + "|" + ep.Scope
		if _, ok := active[identity]; ok {
			continue
		}
		if aggregateActive[identity] {
			continue
		}
		if integrityEpisodeLastRun(ep) == run.ID {
			continue
		}
		observation, observed := integrityObservationFor(run, ep.CheckKey, ep.Feed, ep.Variant)
		if !observed || !integrityEpisodeVersioned(ep) || integrityScopeOutcome(observation, ep.Scope) != "pass" {
			continue
		}
		f := models.FeedIntegrityFinding{CheckKey: ep.CheckKey, Feed: ep.Feed, Variant: ep.Variant}
		if integritySuppressed(suppressed, models.FeedIntegrityFinding{CheckKey: ep.CheckKey, Feed: ep.Feed, Variant: ep.Variant}, ep.Scope, now) {
			continue
		}
		_ = f
		clean := ep.CleanStreak + 1
		updates := map[string]interface{}{"status": "recovering", "clean_streak": clean, "violation_streak": 0, "updated_at": now}
		if ep.RecoveringSince == nil {
			updates["recovering_since"] = now
		}
		if clean >= policy.ResolveRuns {
			updates["status"] = "resolved"
			updates["resolved_at"] = now
		}
		evidence, flaps := integrityEpisodeRunEvidence(ep, models.FeedIntegrityFinding{Evidence: ep.Evidence}, run.ID, now, false)
		updates["evidence"] = evidence
		updates["flap_count_24h"] = flaps
		updates["affected_trend"] = integrityTrend(ep, now, 0)
		if err := db.Model(&ep).Updates(updates).Error; err != nil {
			return err
		}

	}
	return nil
}
func integrityFindingRequiresDeep(f models.FeedIntegrityFinding) bool {
	return feedIntegrityCheckLane(f.CheckKey) == "probe" || f.Variant == "window:week" || f.Variant == "window:month" || (f.CheckKey == "checker_unhealthy" && (strings.HasPrefix(f.TargetRef, "check:probe_") || strings.HasSuffix(f.TargetRef, ":window:week") || strings.HasSuffix(f.TargetRef, ":window:month")))
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
		Where("tenant_id=? AND status IN ?", tenant, []string{models.FeedIntegrityRunCompleted, models.FeedIntegrityRunPartial, models.FeedIntegrityRunFailed})
	if lane == "probe" {
		q = q.Where("tier = ?", models.FeedIntegrityTierDeep)
	}
	return q
}

// feedIntegrityConfirmed reports whether the previous confirm_runs-1 eligible
// runs each carry this exact violation (consecutive confirmation).
func feedIntegrityConfirmed(db *gorm.DB, tenant string, policy models.FeedIntegrityPolicy, run models.FeedIntegrityRun, f models.FeedIntegrityFinding) bool {
	current, ok := integrityObservationFor(run, f.CheckKey, f.Feed, f.Variant)
	if !ok || integrityScopeOutcome(current, feedIntegrityScope(f)) != "violation" {
		return false
	}
	need := policy.ConfirmRuns - 1
	if need <= 0 {
		return true
	}
	boundary, err := integrityConfirmationBoundary(db, tenant, f)
	if err != nil {
		return false
	}
	var prior []models.FeedIntegrityRun
	query := eligibleRunQuery(db, tenant, f.Lane)
	if integrityFindingRequiresDeep(f) {
		query = query.Where("tier='deep'")
	}
	if err := query.Where("started_at < ? AND started_at > ?", run.StartedAt, boundary).Order("started_at DESC").Limit(need).Find(&prior).Error; err != nil || len(prior) < need {
		return false
	}
	for _, pr := range prior {
		if pr.Status == "failed" {
			return false
		}
		o, ok := integrityObservationFor(pr, f.CheckKey, f.Feed, f.Variant)
		if !ok || integrityScopeOutcome(o, feedIntegrityScope(f)) != "violation" {
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
	status, err := readIntegrityStatus(db, tenant)
	if err != nil {
		return gin.H{"error": "unavailable"}
	}
	return status
}

func GetFeedIntegrityStatus(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	status, err := readIntegrityStatus(db, principal.TenantID)
	if err != nil {
		integrityReadError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": status})
}
func GetFeedIntegrityPolicy(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	policy, err := readFeedIntegrityPolicy(db, principal.TenantID)
	if err != nil {
		integrityReadError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": policy})
}

type feedIntegrityPolicyPatch struct {
	ScheduledEnabled     *bool `json:"scheduled_enabled"`
	LightIntervalMinutes *int  `json:"light_interval_minutes"`
	DeepIntervalHours    *int  `json:"deep_interval_hours"`
	ConfirmRuns          *int  `json:"confirm_runs"`
	ResolveRuns          *int  `json:"resolve_runs"`
	EdgePagesPerFeed     *int  `json:"edge_pages_per_feed"`
	ProbeURLBudget       *int  `json:"probe_url_budget"`
	ProbeConcurrency     *int  `json:"probe_concurrency"`
	ProbeTimeoutMS       *int  `json:"probe_timeout_ms"`
	PodsLatencyMS        *int  `json:"pods_latency_budget_ms"`
	NewsLatencyMS        *int  `json:"news_latency_budget_ms"`
	ExpectedPods         *int  `json:"expected_min_pods_units"`
	ExpectedNews         *int  `json:"expected_min_news_slides"`
}

func (p feedIntegrityPolicyPatch) updates() (map[string]interface{}, error) {
	updates := map[string]interface{}{}
	bounded := func(name string, value *int, min, max int) error {
		if value == nil {
			return nil
		}
		if *value < min || *value > max {
			return fmt.Errorf("%s must be between %d and %d", name, min, max)
		}
		updates[name] = *value
		return nil
	}
	if p.ScheduledEnabled != nil {
		updates["scheduled_enabled"] = *p.ScheduledEnabled
	}
	for _, rule := range []struct {
		name     string
		value    *int
		min, max int
	}{
		{"light_interval_minutes", p.LightIntervalMinutes, 1, 1440}, {"deep_interval_hours", p.DeepIntervalHours, 1, 720},
		{"confirm_runs", p.ConfirmRuns, 1, 20}, {"resolve_runs", p.ResolveRuns, 1, 20}, {"edge_pages_per_feed", p.EdgePagesPerFeed, 1, 5},
		{"probe_url_budget", p.ProbeURLBudget, 0, 100}, {"probe_concurrency", p.ProbeConcurrency, 1, 4}, {"probe_timeout_ms", p.ProbeTimeoutMS, 500, 10000},
		{"pods_latency_budget_ms", p.PodsLatencyMS, 1, 60000}, {"news_latency_budget_ms", p.NewsLatencyMS, 1, 60000},
		{"expected_min_pods_units", p.ExpectedPods, 0, 1000}, {"expected_min_news_slides", p.ExpectedNews, 0, 1000},
	} {
		if err := bounded(rule.name, rule.value, rule.min, rule.max); err != nil {
			return nil, err
		}
	}
	return updates, nil
}

func decodeFeedIntegrityPolicyPatch(r io.Reader, patch *feedIntegrityPolicyPatch) error {
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(patch); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("policy body must contain exactly one JSON object")
		}
		return err
	}
	return nil
}

func UpdateFeedIntegrityPolicy(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	policy, policyErr := readFeedIntegrityPolicy(db, principal.TenantID)
	if policyErr != nil {
		integrityReadError(c, policyErr)
		return
	}
	var patch feedIntegrityPolicyPatch
	if err := decodeFeedIntegrityPolicyPatch(c.Request.Body, &patch); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid policy"})
		return
	}
	updates, err := patch.updates()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if len(updates) > 0 {
		updates["updated_at"] = time.Now().UTC()
		if err := updateIntegrityPolicyAudited(db, principal, "feed_integrity.policy.update", updates); err != nil {
			feedIntegrityAudit(db, principal, "feed_integrity.policy.update", principal.TenantID, "failure", map[string]interface{}{"error": "save_failed"})
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to save policy"})
			return
		}
	}
	policy, err = readFeedIntegrityPolicy(db, principal.TenantID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to reload policy"})
		return
	}
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
	if c.ShouldBindJSON(&req) != nil || (req.Tier != "light" && req.Tier != "deep") {
		c.JSON(400, gin.H{"error": "tier must be light or deep"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	run, err := runFeedIntegrity(db, principal.TenantID, feedIntegrityRunOptions{Trigger: "manual", CreatedBy: principal.Email, Tier: req.Tier})
	if err != nil {
		feedIntegrityAudit(db, principal, "feed_integrity.run", principal.TenantID, "failure", map[string]interface{}{"error": err.Error()})
		status := http.StatusInternalServerError
		if errors.Is(err, errFeedIntegrityTenantUnavailable) || strings.Contains(err.Error(), "already running") {
			status = http.StatusConflict
		}
		c.JSON(status, gin.H{"error": safeIntegrityError(err)})
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
	if c.ShouldBindJSON(&req) != nil || req.Minutes < 0 || req.Minutes > 10080 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "minutes must be between 0 and 10080"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	_, policyErr := readFeedIntegrityPolicy(db, principal.TenantID)
	if policyErr != nil {
		integrityReadError(c, policyErr)
		return
	}
	var until *time.Time
	if req.Minutes > 0 {
		t := time.Now().UTC().Add(time.Duration(req.Minutes) * time.Minute)
		until = &t
	}
	if err := updateIntegrityPolicyAudited(db, principal, "feed_integrity.schedule.pause", map[string]interface{}{"paused_until": until, "updated_at": time.Now().UTC()}); err != nil {
		integrityReadError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"paused_until": until}})
}
func ListFeedIntegrityRuns(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var rows []models.FeedIntegrityRun
	if err := db.Where("tenant_id=?", principal.TenantID).Order("started_at DESC").Limit(50).Find(&rows).Error; err != nil {
		integrityReadError(c, err)
		return
	}
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
	if err := db.Where("run_id=?", run.ID).Order("status DESC,created_at DESC").Find(&findings).Error; err != nil {
		integrityReadError(c, err)
		return
	}
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
	if err := q.Order("created_at DESC").Limit(200).Find(&rows).Error; err != nil {
		integrityReadError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": gin.H{"items": rows}})
}
func ListFeedIntegrityEpisodes(c *gin.Context) {
	principal, ok := requireAdminPrincipal(c)
	if !ok {
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	var rows []models.FeedIntegrityEpisode
	if err := db.Where("tenant_id=?", principal.TenantID).Order("last_seen_at DESC").Limit(100).Find(&rows).Error; err != nil {
		integrityReadError(c, err)
		return
	}
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
	if c.ShouldBindJSON(&req) != nil || strings.TrimSpace(req.Notes) == "" || len(req.Notes) > 2000 {
		c.JSON(400, gin.H{"error": "reason and notes are required"})
		return
	}
	aliases := map[string]string{"resolved": "resolved_manually", "expected": "accepted_debt", "duplicate": "superseded"}
	if canonical, ok := aliases[req.ReasonClass]; ok {
		req.ReasonClass = canonical
	}
	allowed := map[string]bool{"resolved_manually": true, "accepted_debt": true, "false_positive": true, "superseded": true}
	if !allowed[req.ReasonClass] {
		c.JSON(400, gin.H{"error": "invalid closure reason"})
		return
	}
	db := c.MustGet("db").(*gorm.DB)
	ep, err := closeIntegrityEpisode(db, principal, id, req.ReasonClass, req.Notes)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(404, gin.H{"error": "episode not found"})
		} else {
			c.JSON(500, gin.H{"error": "could not close episode"})
		}
		return
	}
	c.JSON(200, gin.H{"data": ep})
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
	if createIntegritySuppression(db, principal, &row) != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not create suppression"})
		return
	}
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
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := lockIntegrityPolicy(tx, principal.TenantID); err != nil {
			return err
		}
		if err := tx.Model(&row).Updates(map[string]interface{}{"revoked_at": now, "revoked_by": principal.Email}).Error; err != nil {
			return err
		}
		return tx.Create(&models.AuditLog{TenantID: principal.TenantID, UserID: principal.UserID, UserEmail: principal.Email, Action: "feed_integrity.suppression.revoke", TargetService: "cms", TargetResource: row.PublicID.String(), Status: "success", Payload: integrityJSON(map[string]interface{}{"revoked_at": now})}).Error
	}); err != nil {
		c.JSON(500, gin.H{"error": "could not revoke suppression"})
		return
	}
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
	defer func() { _ = recover() }()
	publicTenant, err := utils.GetConfiguredPublicTenantID()
	if err != nil {
		return
	}
	evaluatePendingFeedIntegrityRuns(db)
	processFeedIntegrityActions(db)
	sweepFeedIntegrityRetention(db)
	var policies []models.FeedIntegrityPolicy
	if err := db.Where("scheduled_enabled = TRUE AND tenant_id = ?", publicTenant).Limit(1).Find(&policies).Error; err != nil {
		return
	}
	sem := make(chan struct{}, 2)
	var wg sync.WaitGroup
	for _, policy := range policies {
		policy := policy
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { _ = recover() }()
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
