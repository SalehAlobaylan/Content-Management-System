package controllers

import (
	"bytes"
	"content-management-system/src/feedcontract"
	"content-management-system/src/feedstate"
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"context"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/pgvector/pgvector-go"
	"gorm.io/gorm"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestFeedIntegrityDBEpisodeExactConfirmationRecoveryAndHumanBoundary(t *testing.T) {
	db := feedIntegrityTestDB(t)
	policy := models.DefaultFeedIntegrityPolicy("default")
	if err := db.Create(&policy).Error; err != nil {
		t.Fatal(err)
	}
	attempt := func(scope, status, tier string) models.FeedIntegrityRun {
		at := time.Now().UTC()
		run := models.FeedIntegrityRun{TenantID: "default", Trigger: "test", Tier: tier, Status: "completed", StartedAt: at, FinishedAt: &at}
		if err := db.Create(&run).Error; err != nil {
			t.Fatal(err)
		}
		c := newIntegrityCollector(run, nil, nil)
		c.add("probe_url_dead", "probe", "pods", "default", "consumer", "major", status, "probe", scope, 1, nil)
		run.LaneResults = integrityJSON(c.lanes())
		if err := db.Model(&run).Update("lane_results", run.LaneResults).Error; err != nil {
			t.Fatal(err)
		}
		if err := updateFeedIntegrityEpisodes(db, "default", policy, run, c.findings); err != nil {
			t.Fatal(err)
		}
		return run
	}
	a := "content:" + uuid.NewString() + ":primary"
	b := "content:" + uuid.NewString() + ":primary"
	attempt(a, "violation", "deep")
	attempt(b, "violation", "deep")
	var n int64
	db.Model(&models.FeedIntegrityEpisode{}).Count(&n)
	if n != 0 {
		t.Fatal("different targets confirmed")
	}
	failed := attempt(a, "violation", "deep")
	if err := db.Model(&failed).Update("status", "failed").Error; err != nil {
		t.Fatal(err)
	}
	attempt(a, "violation", "deep")
	db.Model(&models.FeedIntegrityEpisode{}).Where("status='open'").Count(&n)
	if n != 0 {
		t.Fatal("failed run did not break confirmation")
	}
	attempt(a, "violation", "deep")
	var ep models.FeedIntegrityEpisode
	if err := db.First(&ep).Error; err != nil {
		t.Fatal(err)
	}
	attempt(b, "ok", "deep")
	attempt(a, "ok", "light")
	attempt(a, "check_error", "deep")
	db.First(&ep, ep.ID)
	if ep.CleanStreak != 0 {
		t.Fatal("unknown or light observation recovered probe", ep)
	}
	clean := attempt(a, "ok", "deep")
	if err := updateFeedIntegrityEpisodes(db, "default", policy, clean, nil); err != nil {
		t.Fatal(err)
	}
	db.First(&ep, ep.ID)
	if ep.CleanStreak != 1 {
		t.Fatalf("same run double counted: %d", ep.CleanStreak)
	}
	attempt(a, "ok", "deep")
	attempt(a, "ok", "deep")
	db.First(&ep, ep.ID)
	if ep.Status != "resolved" || ep.CleanStreak != 3 {
		t.Fatal(ep)
	}
	attempt(a, "violation", "deep")
	attempt(a, "violation", "deep")
	ep = models.FeedIntegrityEpisode{}
	if err := db.Where("status='open'").First(&ep).Error; err != nil {
		t.Fatal(err)
	}
	principal := utils.AdminPrincipal{TenantID: "default", Email: "reviewer@example.test"}
	if _, err := closeIntegrityEpisode(db, principal, ep.PublicID, "accepted_debt", "Accepted pending owner repair"); err != nil {
		t.Fatal(err)
	}
	// Pre-closure evidence cannot reopen the identity.
	attempt(a, "violation", "deep")
	db.Model(&models.FeedIntegrityEpisode{}).Where("status='open'").Count(&n)
	if n != 0 {
		t.Fatal("closed evidence reused")
	}
}

func TestFeedIntegrityDBHumanMutationsRequireCommittedAudit(t *testing.T) {
	db := feedIntegrityTestDB(t)
	_, ep, action := seedFeedIntegrityAction(t, db, "default", "approved")
	var policy models.FeedIntegrityPolicy
	if err := db.Where("tenant_id=?", "default").First(&policy).Error; err != nil {
		t.Fatal(err)
	}
	principal := utils.AdminPrincipal{TenantID: "default", Email: "reviewer@example.test"}
	if err := db.Exec(`CREATE FUNCTION fi_reject_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture audit unavailable'; END $$;
		CREATE TRIGGER fi_reject_audit BEFORE INSERT ON audit_logs FOR EACH ROW EXECUTE FUNCTION fi_reject_audit()`).Error; err != nil {
		t.Fatal(err)
	}
	if err := updateIntegrityPolicyAudited(db, principal, "feed_integrity.policy.update", map[string]interface{}{"probe_url_budget": 0}); err == nil {
		t.Fatal("unaudited policy effect committed")
	}
	var stored models.FeedIntegrityPolicy
	if err := db.First(&stored, policy.ID).Error; err != nil || stored.ProbeURLBudget != policy.ProbeURLBudget {
		t.Fatal("policy effect did not roll back", err)
	}
	now := time.Now().UTC()
	row := models.FeedIntegritySuppression{TenantID: "default", CheckKey: ep.CheckKey, Feed: ep.Feed, Variant: ep.Variant, Scope: ep.Scope, StartsAt: now, ExpiresAt: now.Add(time.Hour), CreatedBy: principal.Email, Reason: "Fixture human stop"}
	if err := createIntegritySuppression(db, principal, &row); err == nil {
		t.Fatal("unaudited suppression committed")
	}
	if err := db.First(&action, action.ID).Error; err != nil || action.Outcome != "approved" {
		t.Fatal("action cancellation did not roll back", err)
	}
	var count int64
	if err := db.Model(&models.FeedIntegritySuppression{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatal("suppression did not roll back", count, err)
	}
	if err := db.Exec("DROP TRIGGER fi_reject_audit ON audit_logs").Error; err != nil {
		t.Fatal(err)
	}
	row.ID, row.PublicID = 0, uuid.Nil
	if err := createIntegritySuppression(db, principal, &row); err != nil {
		t.Fatal(err)
	}
	if err := db.First(&action, action.ID).Error; err != nil || action.Outcome != "skipped" {
		t.Fatal("committed human stop did not cancel unstarted action", err)
	}
}
func TestFeedIntegrityDBActionCapsCrashAndAuditedUnknownReview(t *testing.T) {
	db := feedIntegrityTestDB(t)
	_, ep, action := seedFeedIntegrityAction(t, db, "default", "approved")
	claimed, err := claimFeedIntegrityAction(db, "default", action.ID)
	if err != nil {
		t.Fatal(err)
	}
	running, err := admitIntegrityAction(db, "default", claimed.ID, claimed.ClaimToken, true)
	if err != nil {
		t.Fatal(err)
	}
	expired := time.Now().Add(-time.Minute)
	db.Model(&running).Update("claim_expires_at", expired)
	recoverExpiredFeedIntegrityClaims(db, time.Now())
	db.First(&running, running.ID)
	if running.ErrorClass != "effect_unknown" || running.Outcome != "tool_failed" {
		t.Fatal(running)
	}
	// A new proposal cannot replay or bypass the uncertain effect, even with approval.
	fresh := action
	fresh.ID = 0
	fresh.PublicID = uuid.Nil
	fresh.IdempotencyKey = "fresh"
	fresh.Outcome = "approved"
	if err := db.Create(&fresh).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := claimFeedIntegrityAction(db, "default", fresh.ID); err == nil {
		t.Fatal("unknown effect was replayed")
	}
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("db", db)
		c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: "default", Email: "reviewer@example.test"})
	})
	router.POST("/:id", ReviewFeedIntegrityUnknownAction)
	body := `{"reason":"Checked owner snapshot and logs","disposition":"allow_fresh_approval"}`
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest("POST", "/"+running.PublicID.String(), strings.NewReader(body)))
	if w.Code != 200 {
		t.Fatalf("review: %d %s", w.Code, w.Body.String())
	}
	db.First(&running, running.ID)
	if running.Outcome != "tool_failed" || running.ErrorClass != "effect_unknown" {
		t.Fatal("review erased uncertainty")
	}
	if _, err := claimFeedIntegrityAction(db, "default", fresh.ID); err == nil {
		t.Fatal("review reset admission caps")
	}
	var audits int64
	db.Model(&models.AuditLog{}).Where("action='feed_integrity.autopilot.action.review_unknown'").Count(&audits)
	if audits != 1 {
		t.Fatal("review unaudited")
	}
	// A human close cancels the unstarted new proposal without refunding the old attempt.
	if _, err := closeIntegrityEpisode(db, utils.AdminPrincipal{TenantID: "default", Email: "reviewer@example.test"}, ep.PublicID, "resolved_manually", "Investigated and stopped further repairs"); err != nil {
		t.Fatal(err)
	}
	db.First(&fresh, fresh.ID)
	if fresh.Outcome != "skipped" {
		t.Fatal(fresh)
	}
}
func integrityBusinessChecksum(t *testing.T, db *gorm.DB) string {
	t.Helper()
	result := ""
	for _, table := range []string{"content_items", "stories", "feed_generation_heads", "feed_generation_memberships", "consumer_feed_sessions", "interactions", "preference_stats", "ranking_configs", "media_demand_stats", "media_intelligence_scores", "news_story_overrides", "sources"} {
		if !db.Migrator().HasTable(table) {
			continue
		}
		var sum string
		if err := db.Raw("SELECT md5(COALESCE(string_agg(to_jsonb(t)::text, '' ORDER BY to_jsonb(t)::text),'')) FROM " + table + " t").Scan(&sum).Error; err != nil {
			t.Fatal(err)
		}
		result += table + ":" + sum
	}
	return result
}
func integritySeedPublicFixture(t *testing.T, db *gorm.DB) []uuid.UUID {
	t.Helper()
	ids := []uuid.UUID{}
	for _, lane := range []string{"media", "news"} {
		g := models.FeedGeneration{TenantID: "default", Lane: lane, State: "active", BuildWatermark: time.Now(), Verification: integrityJSON(map[string]interface{}{})}
		if err := db.Create(&g).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Exec("INSERT INTO feed_generation_heads(tenant_id,lane,active_generation_id,generation,updated_at) VALUES('default',?,?,1,now()) ON CONFLICT(tenant_id,lane) DO UPDATE SET active_generation_id=excluded.active_generation_id,generation=1", lane, g.PublicID).Error; err != nil {
			t.Fatal(err)
		}
		if lane == "media" {
			for i, duration := range []int{300, 600, 1800} {
				id := uuid.New()
				ids = append(ids, id)
				if err := db.Exec(`INSERT INTO content_items(public_id,tenant_id,type,source,status,title,original_url,media_url,playback_url,playback_type,duration_sec,is_feed_unit,feed_visibility,published_at,idempotency_key) VALUES(?,'default','PODCAST','RSS','READY',?,'https://fixture.test/source','https://fixture.test/audio.mp3','https://fixture.test/audio.mp3','audio',?,true,'visible',now(),?)`, id, fmt.Sprintf("Fixture %d", i), duration, fmt.Sprintf("fixture-%d", i)).Error; err != nil {
					t.Fatal(err)
				}
				if err := db.Create(&models.FeedGenerationMembership{GenerationID: g.PublicID, MemberType: "feed_unit", MemberID: id}).Error; err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	loadTenantConfig(db, "default")
	loadCirculationPolicy(db, "default")
	return ids
}
func TestFeedIntegrityDBSessionHandlersAndReadOnlyFullRun(t *testing.T) {
	db := feedIntegrityTestDB(t)
	t.Setenv("DEFAULT_TENANT_ID", "default")
	ids := integritySeedPublicFixture(t, db)
	policy := models.DefaultFeedIntegrityPolicy("default")
	policy.ProbeURLBudget = 0
	if err := db.Create(&policy).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&policy).Update("probe_url_budget", 0).Error; err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	router.Use(func(c *gin.Context) { c.Set("db", db) })
	router.GET("/api/v1/feed/pods", GetPodsFeed)
	router.GET("/api/v1/feed/news", GetNewsFeed)
	router.POST("/sessions", CreatePodsFeedSession)
	router.GET("/sessions/:id", GetPodsFeedSessionPage)
	router.GET("/sessions/:id/freshness", GetPodsFeedSessionFreshness)
	server := httptest.NewServer(router)
	defer server.Close()
	u, _ := url.Parse(server.URL)
	t.Setenv("PORT", u.Port())
	call := func(method, path string, synthetic bool) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(method, path, nil)
		r.RemoteAddr = "127.0.0.1:99"
		if synthetic {
			r.Header.Set(feedIntegritySyntheticHdr, feedIntegrityCapability)
		}
		router.ServeHTTP(w, r)
		return w
	}
	created := call("POST", "/sessions?session_id=fixture-consumer&limit=1&duration=10", false)
	if created.Code != 201 {
		t.Fatalf("create session: %d %s", created.Code, created.Body.String())
	}
	var frozen frozenPodsSessionResponse
	json.Unmarshal(created.Body.Bytes(), &frozen)
	if len(frozen.Items) != 1 || frozen.Items[0].ID != ids[1] {
		t.Fatalf("duration filtered session: %s", created.Body.String())
	}
	path := "/sessions/" + frozen.SessionID + "?session_id=fixture-consumer"
	page := call("GET", path, false)
	if page.Code != 200 || !bytes.Contains(page.Body.Bytes(), []byte(ids[1].String())) {
		t.Fatal(page.Body.String())
	}
	before := integrityBusinessChecksum(t, db)
	run, err := runFeedIntegrity(db, "default", feedIntegrityRunOptions{Tier: "deep", Trigger: "test"})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	after := integrityBusinessChecksum(t, db)
	if before != after {
		t.Fatal("checker changed business/telemetry/session rows")
	}
	for _, o := range integrityRunObservations(run) {
		if o.Outcome == "check_error" && feedIntegrityCheckLane(o.CheckKey) == "inventory" {
			t.Fatalf("inventory fixture checker failed: %s; findings %v", o.CheckKey, run.Summary)
		}
	}
	if len(integrityRunObservations(run)) == 0 {
		t.Fatal("full run lacks observations")
	}
	// A spoofed capability behaves as ordinary traffic and therefore records demand.
	spoof := httptest.NewRequest("GET", "/api/v1/feed/pods", nil)
	spoof.RemoteAddr = "203.0.113.9:123"
	spoof.Header.Set(feedIntegritySyntheticHdr, feedIntegrityCapability)
	router.ServeHTTP(httptest.NewRecorder(), spoof)
	deadline := time.Now().Add(2 * time.Second)
	for before == integrityBusinessChecksum(t, db) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if before == integrityBusinessChecksum(t, db) {
		t.Fatal("spoof suppressed real telemetry")
	}
	db.Model(&models.ContentItem{}).Where("public_id=?", ids[1]).Update("feed_visibility", "hidden")
	hidden := call("GET", path, false)
	if hidden.Code != 200 || bytes.Contains(hidden.Body.Bytes(), []byte(ids[1].String())) {
		t.Fatal("session leaked hidden unit")
	}
	db.Model(&models.FeedGenerationHead{}).Where("tenant_id='default' AND lane='media'").Update("generation", 2)
	if stale := call("GET", path, false); stale.Code != 409 {
		t.Fatal("cutover did not invalidate session", stale.Code)
	}
	if fresh := call("GET", "/sessions/"+frozen.SessionID+"/freshness?session_id=fixture-consumer", false); fresh.Code != 200 || !bytes.Contains(fresh.Body.Bytes(), []byte("true")) {
		t.Fatal(fresh.Body.String())
	}
	db.Model(&models.ConsumerFeedSession{}).Where("id=?", frozen.SessionID).Update("expires_at", time.Now().Add(-time.Minute))
	if expired := call("GET", path, false); expired.Code != http.StatusGone {
		t.Fatal("expiry", expired.Code)
	}
	// A late ledger failure must roll back findings, episodes and schedule progress.
	if err := db.Exec(`INSERT INTO content_items(tenant_id,type,source,status,title,original_url,duration_sec,is_feed_unit,feed_visibility,idempotency_key) VALUES('default','VIDEO','RSS','READY','invalid fixture','https://fixture.test/finalization',100,true,'visible','finalization-fixture')`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&policy).Update("confirm_runs", 1).Error; err != nil {
		t.Fatal(err)
	}
	var beforePolicy models.FeedIntegrityPolicy
	db.First(&beforePolicy, policy.ID)
	counts := func() []int64 {
		out := []int64{}
		for _, table := range []string{"feed_integrity_findings", "feed_integrity_episodes", "feed_integrity_actions"} {
			var n int64
			if err := db.Table(table).Count(&n).Error; err != nil {
				t.Fatal(err)
			}
			out = append(out, n)
		}
		return out
	}
	ledgerBefore := fmt.Sprint(counts())
	if err := db.Exec(`CREATE FUNCTION fi_fail_terminal() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.status IN ('completed','partial') THEN RAISE EXCEPTION 'fixture finalization failure'; END IF; RETURN NEW; END $$;
		CREATE TRIGGER fi_fail_terminal BEFORE UPDATE ON feed_integrity_runs FOR EACH ROW EXECUTE FUNCTION fi_fail_terminal()`).Error; err != nil {
		t.Fatal(err)
	}
	failed, err := runFeedIntegrity(db, "default", feedIntegrityRunOptions{Tier: "light", Trigger: "test"})
	if err == nil || failed.Status != "failed" || failed.Headline != "inconclusive" {
		t.Fatal("failed finalization reported success", failed, err)
	}
	if ledgerBefore != fmt.Sprint(counts()) {
		t.Fatal("failed finalization committed unsupported ledger changes")
	}
	var afterPolicy models.FeedIntegrityPolicy
	db.First(&afterPolicy, policy.ID)
	if !beforePolicy.LastLightRunAt.Equal(*afterPolicy.LastLightRunAt) {
		t.Fatal("failed finalization advanced schedule")
	}
	var stored models.FeedIntegrityRun
	if err := db.First(&stored, failed.ID).Error; err != nil || stored.Status != "failed" || stored.AutopilotEvaluatedAt != nil {
		t.Fatal("failed finalization authorized autopilot or stayed running", err)
	}
}

func TestFeedIntegrityDBStatusDeepEvidenceAndRetention(t *testing.T) {
	db := feedIntegrityTestDB(t)
	policy := models.DefaultFeedIntegrityPolicy("default")
	if err := db.Create(&policy).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	deep := models.FeedIntegrityRun{TenantID: "default", Trigger: "test", Tier: "deep", Status: "completed", StartedAt: now.Add(-time.Hour), FinishedAt: &now}
	if err := db.Create(&deep).Error; err != nil {
		t.Fatal(err)
	}
	c := newIntegrityCollector(deep, nil, nil)
	scope := "content:" + uuid.NewString() + ":primary"
	c.add("probe_url_dead", "probe", "pods", "default", "consumer", "major", "violation", "probe", scope, 1, nil)
	deep.LaneResults = integrityJSON(c.lanes())
	db.Model(&deep).Update("lane_results", deep.LaneResults)
	// More light rows than the bounded light history cannot crowd out deep proof.
	for i := 0; i < 45; i++ {
		run := models.FeedIntegrityRun{TenantID: "default", Trigger: "test", Tier: "light", Status: "completed", StartedAt: now.Add(-time.Duration(45-i) * time.Second), FinishedAt: &now}
		if err := db.Create(&run).Error; err != nil {
			t.Fatal(err)
		}
	}
	status, err := readIntegrityStatus(db, "default")
	if err != nil {
		t.Fatal(err)
	}
	find := func(status map[string]interface{}) integrityCurrentObservation {
		for _, o := range status["current_observations"].([]integrityCurrentObservation) {
			if o.CheckKey == "probe_url_dead" {
				return o
			}
		}
		t.Fatal("probe observation missing")
		return integrityCurrentObservation{}
	}
	if got := find(status); got.Outcome != "violation" {
		t.Fatal("light erased deep evidence", got)
	}
	failed := models.FeedIntegrityRun{TenantID: "default", Trigger: "test", Tier: "deep", Status: "failed", StartedAt: now, FinishedAt: &now}
	// Even well-formed observations on an unsuccessful finalization are unusable.
	failedCollector := newIntegrityCollector(failed, nil, nil)
	failedCollector.add("probe_url_dead", "probe", "pods", "default", "consumer", "major", "ok", "probe", scope, 1, nil)
	failed.LaneResults = integrityJSON(failedCollector.lanes())
	db.Create(&failed)
	status, err = readIntegrityStatus(db, "default")
	if err != nil {
		t.Fatal(err)
	}
	got := find(status)
	if got.Outcome != "check_error" || got.LastKnown == nil || got.LastKnown.Outcome != "violation" {
		t.Fatal("failed attempt erased last known", got)
	}
	ep := models.FeedIntegrityEpisode{TenantID: "default", CheckKey: "probe_url_dead", Feed: "pods", Variant: "default", Scope: scope, Status: "open", Axis: "consumer", Severity: "major", Summary: "dead playback", FirstDetectedAt: now, LastSeenAt: now, Evidence: integrityEpisodeEvidence(c.findings[0])}
	if err := db.Create(&ep).Error; err != nil {
		t.Fatal(err)
	}
	// A newer successful sample of another target must preserve only committed
	// defect evidence, never the observations from an unsuccessful finalization.
	failedCollector = newIntegrityCollector(failed, nil, nil)
	failedCollector.add("probe_url_dead", "probe", "pods", "default", "consumer", "major", "violation", "probe", scope, 17, nil)
	if err := db.Model(&failed).Update("lane_results", integrityJSON(failedCollector.lanes())).Error; err != nil {
		t.Fatal(err)
	}
	other := models.FeedIntegrityRun{TenantID: "default", Trigger: "test", Tier: "deep", Status: "completed", StartedAt: now.Add(time.Second), FinishedAt: &now}
	otherCollector := newIntegrityCollector(other, nil, nil)
	otherCollector.add("probe_url_dead", "probe", "pods", "default", "consumer", "major", "ok", "probe", "content:"+uuid.NewString()+":primary", 1, nil)
	other.LaneResults = integrityJSON(otherCollector.lanes())
	if err := db.Create(&other).Error; err != nil {
		t.Fatal(err)
	}
	status, err = readIntegrityStatus(db, "default")
	if err != nil {
		t.Fatal(err)
	}
	if got := find(status); got.Outcome != "violation" || got.AffectedCount != 1 {
		t.Fatal("uncommitted failed-run evidence replaced confirmed defect", got)
	}
	old := now.Add(-200 * 24 * time.Hour)
	violation := c.findings[0]
	violation.CreatedAt = old
	if err := db.Create(&violation).Error; err != nil {
		t.Fatal(err)
	}
	pass := violation
	pass.ID = 0
	pass.PublicID = uuid.Nil
	pass.Status = "ok"
	if err := db.Create(&pass).Error; err != nil {
		t.Fatal(err)
	}
	feedIntegrityRetention.Lock()
	feedIntegrityRetention.Last = time.Time{}
	feedIntegrityRetention.Unlock()
	sweepFeedIntegrityRetention(db)
	var count int64
	db.Model(&models.FeedIntegrityFinding{}).Where("id=?", violation.ID).Count(&count)
	if count != 1 {
		t.Fatal("unresolved evidence swept")
	}
	db.Model(&models.FeedIntegrityFinding{}).Where("id=?", pass.ID).Count(&count)
	if count != 0 {
		t.Fatal("old pass evidence retained")
	}
}

func TestFeedIntegrityDBClaimRechecksEveryStopAndFailedRunningWrite(t *testing.T) {
	db := feedIntegrityTestDB(t)
	run, ep, action := seedFeedIntegrityAction(t, db, "default", "approved")
	cases := []struct {
		name string
		stop func(*gorm.DB) error
	}{
		{"disabled", func(tx *gorm.DB) error {
			return tx.Model(&models.FeedIntegrityPolicy{}).Where("tenant_id='default'").Update("autopilot_enabled", false).Error
		}},
		{"observe", func(tx *gorm.DB) error {
			return tx.Model(&models.FeedIntegrityPolicy{}).Where("tenant_id='default'").Update("autopilot_mode", "observe").Error
		}},
		{"legacy_safe_auto", func(tx *gorm.DB) error {
			return tx.Model(&models.FeedIntegrityPolicy{}).Where("tenant_id='default'").Update("autopilot_mode", "safe_auto").Error
		}},
		{"paused", func(tx *gorm.DB) error {
			return tx.Model(&models.FeedIntegrityPolicy{}).Where("tenant_id='default'").Update("autopilot_paused_until", time.Now().Add(time.Hour)).Error
		}},
		{"closed", func(tx *gorm.DB) error {
			return tx.Model(&models.FeedIntegrityEpisode{}).Where("id=?", ep.ID).Update("status", "closed_by_human").Error
		}},
		{"snapshot_changed", func(tx *gorm.DB) error {
			return tx.Model(&models.NewsSnapshot{}).Where("tenant_id='default'").Update("generation", 2).Error
		}},
		{"evidence_failed", func(tx *gorm.DB) error {
			return tx.Model(&models.FeedIntegrityRun{}).Where("id=?", run.ID).Update("status", "failed").Error
		}},
		{"suppressed", func(tx *gorm.DB) error {
			return tx.Create(&models.FeedIntegritySuppression{TenantID: "default", CheckKey: ep.CheckKey, Feed: ep.Feed, Variant: ep.Variant, Scope: ep.Scope, StartsAt: time.Now().Add(-time.Minute), ExpiresAt: time.Now().Add(time.Hour), Reason: "operator stop"}).Error
		}},
		{"health_unknown", func(tx *gorm.DB) error {
			return tx.Model(&models.SystemAutopilotRun{}).Where("1=1").Update("status", "failed").Error
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tx := db.Begin()
			defer tx.Rollback()
			if err := tc.stop(tx); err != nil {
				t.Fatal(err)
			}
			if _, err := claimFeedIntegrityAction(tx, "default", action.ID); err == nil {
				t.Fatal("stop bypassed")
			}
		})
	}
	// Manual Assist remains possible while scheduled checks are disabled.
	db.Model(&models.FeedIntegrityPolicy{}).Where("tenant_id='default'").Update("scheduled_enabled", false)
	claimed, err := claimFeedIntegrityAction(db, "default", action.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`CREATE FUNCTION fail_integrity_running() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.outcome='running' THEN RAISE EXCEPTION 'injected running persistence failure'; END IF; RETURN NEW; END $$`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`CREATE TRIGGER fail_integrity_running BEFORE UPDATE ON feed_integrity_actions FOR EACH ROW EXECUTE FUNCTION fail_integrity_running()`).Error; err != nil {
		t.Fatal(err)
	}
	var before models.NewsSnapshot
	db.Where("tenant_id='default'").First(&before)
	if err := executeFeedIntegrityAction(db, claimed); err == nil {
		t.Fatal("effect ran despite failed running persistence")
	}
	var after models.NewsSnapshot
	db.Where("tenant_id='default'").First(&after)
	if !after.BuiltAt.Equal(before.BuiltAt) || after.Generation != before.Generation {
		t.Fatal("builder ran before durable admission")
	}
}

func TestFeedIntegrityDBAllPagesShapeCursorAndDeclaredInventory(t *testing.T) {
	db := feedIntegrityTestDB(t)
	t.Setenv("DEFAULT_TENANT_ID", "default")
	ids := integritySeedPublicFixture(t, db)
	if err := db.Exec(`INSERT INTO content_items(tenant_id,type,source,status,title,original_url,duration_sec,is_feed_unit,feed_visibility,idempotency_key) VALUES('default','VIDEO','RSS','READY','invalid','https://fixture.test/invalid',100,true,'visible','invalid')`).Error; err != nil {
		t.Fatal(err)
	}
	run := models.FeedIntegrityRun{TenantID: "default", Tier: "deep", StartedAt: time.Now().UTC()}
	collector := newIntegrityCollector(run, nil, nil)
	collectFeedIntegrityInventory(context.Background(), db, "default", collector)
	if collector.observations["inv_pods_bounds|pods|default"].AffectedCount != 1 {
		t.Fatal("declared invalid unit was invisible to checker")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "pods") {
			item := PodsItem{ID: ids[0], Type: "PODCAST", Title: "legal audio", DurationSec: 300, MediaURL: "https://fixture.test/a.mp3"}
			cursor := "repeat"
			if r.URL.Query().Get("cursor") != "" {
				item.DurationSec = 100
				item.Title = ""
				item.IsArchived = true
			}
			if raw := r.URL.Query().Get("duration"); raw != "" {
				n, _ := strconv.Atoi(raw)
				item.DurationSec = n * 60
				_ = json.NewEncoder(w).Encode(integrityPodsPage{Items: []PodsItem{item}})
				return
			}
			_ = json.NewEncoder(w).Encode(integrityPodsPage{Items: []PodsItem{item}, Cursor: &cursor})
			return
		}
		window := r.URL.Query().Get("window")
		w.Header().Set("X-Wahb-Feed-Source", "cache")
		w.Header().Set("X-Wahb-Snapshot-Window", window)
		w.Header().Set("X-Wahb-Snapshot-Age-Ms", "120000")
		w.Header().Set("X-Wahb-Snapshot-Built-At", time.Now().Add(-2*time.Minute).UTC().Format(time.RFC3339Nano))
		w.Header().Set("X-Wahb-Snapshot-Dirty", "true")
		story := integrityNewsStory{StoryID: ids[0].String(), Title: "News", Members: []StoryMember{{ID: ids[1], Type: "NEWS"}}}
		cursor := "repeat"
		_ = json.NewEncoder(w).Encode(integrityNewsPage{Cursor: &cursor, Slides: []integrityNewsSlide{{Featured: story, Related: []integrityNewsStory{story}}}})
	}))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	t.Setenv("PORT", u.Port())
	policy := models.DefaultFeedIntegrityPolicy("default")
	policy.EdgePagesPerFeed = 2
	collectFeedIntegrityEdge(context.Background(), db, policy, "deep", collector)
	for _, key := range []string{"edge_pods_bounds_served", "edge_pods_dup", "edge_pods_required_metadata", "edge_pods_status_served", "edge_pods_cursor"} {
		if got := collector.observations[key+"|pods|default"]; got.Outcome != "violation" {
			t.Fatalf("later-page %s: %+v", key, got)
		}
	}
	for _, key := range []string{"edge_news_shape", "edge_news_dup", "edge_news_cursor", "edge_news_cache_stale"} {
		if got := collector.observations[key+"|news|window:today"]; got.Outcome != "violation" {
			t.Fatalf("news %s: %+v", key, got)
		}
	}
}

func TestFeedIntegrityDBFeedReadsPreserveProjectionsAndBatchRelatedStories(t *testing.T) {
	db := feedIntegrityTestDB(t)
	integritySeedPublicFixture(t, db)
	var generation models.FeedGenerationHead
	if err := db.Where("tenant_id='default' AND lane='news'").First(&generation).Error; err != nil {
		t.Fatal(err)
	}
	anchor, neighbor, unrelated, inactive := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	vector := make([]float32, 1024)
	vector[0] = 1
	other := make([]float32, 1024)
	other[1] = 1
	now := time.Now().UTC()
	space := strings.Repeat("a", 64)
	for i, id := range []uuid.UUID{anchor, neighbor, unrelated, inactive} {
		emb := pgvector.NewVector(vector)
		if id == unrelated {
			emb = pgvector.NewVector(other)
		}
		story := models.Story{TenantID: "default", PublicID: id, Label: fmt.Sprintf("Read fixture %d", i), Embedding: &emb, EmbeddingSpaceID: &space, ArticleCount: 1, LastMemberAt: &now}
		if err := db.Create(&story).Error; err != nil {
			t.Fatal(err)
		}
		if id != inactive {
			if err := db.Exec(`INSERT INTO content_items(public_id,tenant_id,type,source,status,story_id,title,original_url,idempotency_key) VALUES(?,'default','NEWS','RSS','READY',?,'Related fixture',?,?)`, uuid.New(), id, fmt.Sprintf("https://fixture.test/related/%d", i), fmt.Sprintf("related-fixture-%d", i)).Error; err != nil {
				t.Fatal(err)
			}
			if err := db.Create(&models.FeedGenerationMembership{GenerationID: *generation.ActiveGenerationID, MemberType: "story", MemberID: id}).Error; err != nil {
				t.Fatal(err)
			}
		}
	}
	// Canonical records retain their native fields; projection patches can
	// override fields (including explicit NULL) without serializing embeddings.
	if err := db.Exec(`INSERT INTO news_feed_story_projections(tenant_id,generation_id,story_id,projection,member_hash,updated_at) VALUES('default',?,?,?::jsonb,?,NOW())`, *generation.ActiveGenerationID, neighbor, string(integrityJSON(map[string]interface{}{"tenant_id": "default", "public_id": neighbor, "label": "Projected headline", "article_count": 3, "summary": nil})), strings.Repeat("b", 64)).Error; err != nil {
		t.Fatal(err)
	}
	scoped := feedcontract.WithReadSchema(db)
	var stories []models.Story
	if err := feedstate.ActiveNewsStoryQuery(scoped, "default").Select("public_id,label,article_count,embedding,summary").Where("public_id IN ?", []uuid.UUID{anchor, neighbor, inactive}).Find(&stories).Error; err != nil {
		t.Fatal(err)
	}
	byID := map[uuid.UUID]models.Story{}
	for _, story := range stories {
		byID[story.PublicID] = story
	}
	if len(stories) != 2 || byID[neighbor].Label != "Projected headline" || byID[neighbor].ArticleCount != 3 || byID[neighbor].Summary != nil || byID[anchor].Embedding == nil || byID[neighbor].Embedding == nil {
		t.Fatalf("lost canonical/projection contract: %+v", stories)
	}
	candidates, err := relatedCandidatesForPage(scoped, feedstate.ActiveNewsStoryQuery(scoped, "default"), []uuid.UUID{anchor}, byID)
	if err != nil {
		t.Fatal(err)
	}
	if got := candidates[anchor]; len(got) != 1 || got[0] != neighbor {
		t.Fatalf("cosine/active-view fallback: %v", got)
	}
	byID[anchor] = models.Story{RelatedIDs: integrityJSON([]string{neighbor.String(), unrelated.String()})}
	byID[neighbor] = models.Story{RelatedIDs: integrityJSON([]string{})}
	stored, err := relatedCandidatesForPage(scoped, feedstate.ActiveNewsStoryQuery(scoped, "default"), []uuid.UUID{anchor, neighbor}, byID)
	if err != nil || len(stored[anchor]) != 2 || stored[anchor][0] != neighbor || stored[anchor][1] != unrelated || len(stored[neighbor]) != 0 {
		t.Fatalf("stored order/explicit empty changed: %v %v", stored, err)
	}
	// Schema facts are request-local; the active head is still read live.
	if err := db.Model(&models.FeedGenerationHead{}).Where("tenant_id='default' AND lane='news'").Update("active_generation_id", nil).Error; err != nil {
		t.Fatal(err)
	}
	if _, supported, active := feedcontract.ActiveGeneration(scoped, "default", "news"); !supported || active {
		t.Fatal("request cached serving authority")
	}
	if err := db.Model(&models.FeedGenerationHead{}).Where("tenant_id='default' AND lane='news'").Update("active_generation_id", generation.ActiveGenerationID).Error; err != nil {
		t.Fatal(err)
	}
	// PostgreSQL must infer generation IDs as UUIDs in INSERT ... SELECT.
	itemID := uuid.New()
	if err := db.Exec(`INSERT INTO content_items(public_id,tenant_id,type,source,status,story_id,title,original_url,idempotency_key) VALUES(?,'default','NEWS','RSS','READY',?,'Reconcile fixture','https://fixture.test/reconcile','reconcile-fixture')`, itemID, anchor).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := feedstate.ReconcileNewsMembership(db, "default"); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := db.Model(&models.FeedGenerationMembership{}).Where("generation_id=? AND member_type='news_item' AND member_id=?", generation.ActiveGenerationID, itemID).Count(&count).Error; err != nil || count != 1 {
		t.Fatal("missing exact reconciliation", count, err)
	}
	if n, err := feedstate.ReconcileNewsMembership(db, "default"); err != nil || n != 0 {
		t.Fatal("reconciliation was not idempotent", n, err)
	}
}
