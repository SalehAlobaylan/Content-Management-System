package controllers

import (
	"content-management-system/src/models"
	operatorpkg "content-management-system/src/operator"
	"context"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm/clause"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

// Exercise the actual bounded adapter and public handler, including the
// recorded effect version. Claim-only fixtures cannot establish this behavior.
func TestFeedIntegrityDBRefreshEffectAndPublicVerification(t *testing.T) {
	db := feedIntegrityTestDB(t)
	t.Setenv("DEFAULT_TENANT_ID", "default")
	integritySeedPublicFixture(t, db)
	_, ep, action := seedFeedIntegrityAction(t, db, "default", "approved")
	now := time.Now().UTC()
	story := models.Story{TenantID: "default", PublicID: uuid.New(), Label: "Qualified News fixture", ArticleCount: 1, LastMemberAt: &now}
	if err := db.Create(&story).Error; err != nil {
		t.Fatal(err)
	}
	itemID := uuid.New()
	if err := db.Exec(`INSERT INTO content_items(public_id,tenant_id,type,source,status,story_id,title,original_url,published_at,idempotency_key) VALUES(?,'default','NEWS','RSS','READY',?,'Qualified fixture','https://fixture.test/news',now(),?)`, itemID, story.PublicID, itemID.String()).Error; err != nil {
		t.Fatal(err)
	}
	var head models.FeedGenerationHead
	if err := db.Where("tenant_id='default' AND lane='news'").First(&head).Error; err != nil {
		t.Fatal(err)
	}
	for kind, id := range map[string]uuid.UUID{"story": story.PublicID, "news_item": itemID} {
		if err := db.Clauses(clause.OnConflict{DoNothing: true}).Create(&models.FeedGenerationMembership{GenerationID: *head.ActiveGenerationID, MemberType: kind, MemberID: id}).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, window := range []string{"week", "month"} {
		if err := db.Create(&models.NewsSnapshot{TenantID: "default", Window: window, Slides: integrityJSON([]string{"untouched"}), Generation: 7, Dirty: true, BuiltAt: now.Add(-time.Hour)}).Error; err != nil {
			t.Fatal(err)
		}
	}
	checksum := integrityBusinessChecksum(t, db)
	var otherBefore, otherAfter []models.NewsSnapshot
	if err := db.Where("tenant_id='default' AND \"window\" <> 'today'").Order("\"window\"").Find(&otherBefore).Error; err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	router.Use(func(c *gin.Context) { c.Set("db", db) })
	router.GET("/api/v1/feed/news", GetNewsFeed)
	server := httptest.NewServer(router)
	defer server.Close()
	u, _ := url.Parse(server.URL)
	t.Setenv("PORT", u.Port())
	claimed, err := claimFeedIntegrityAction(db, "default", action.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := executeFeedIntegrityAction(db, claimed); err != nil {
		t.Fatal(err)
	}
	if err := db.First(&action, action.ID).Error; err != nil {
		t.Fatal(err)
	}
	if action.Outcome != "verifying" {
		t.Fatal("adapter did not persist its effect", action.Outcome)
	}
	verifyFeedIntegrityAction(db, action)
	if err := db.First(&action, action.ID).Error; err != nil {
		t.Fatal(err)
	}
	if action.Outcome != "verification_passed" {
		t.Fatalf("actual delivery not verified: %s %s", action.Outcome, action.Verification)
	}
	if err := db.Where("tenant_id='default' AND \"window\" <> 'today'").Order("\"window\"").Find(&otherAfter).Error; err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(otherBefore)
	after, _ := json.Marshal(otherAfter)
	if string(before) != string(after) || checksum != integrityBusinessChecksum(t, db) {
		t.Fatal("single-window repair changed another window or business/telemetry rows")
	}
	if err := executeFeedIntegrityAction(db, claimed); err == nil {
		t.Fatal("completed effect was replayed")
	}
	if err := db.First(&ep, ep.ID).Error; err != nil || ep.CleanStreak != 0 || ep.Status != "open" {
		t.Fatal("repair bypassed checker-owned recovery", err, ep)
	}
}

func TestFeedIntegrityDBOperatorSuppressionCompatibility(t *testing.T) {
	db := feedIntegrityTestDB(t)
	_, ep, action := seedFeedIntegrityAction(t, db, "default", "approved")
	canonical := operatorpkg.CanonicalPlan{ActorID: "fixture-reviewer", NormalizedArguments: map[string]any{"episode_id": ep.PublicID.String(), "ttl_minutes": 60}}
	// Existing signed Operator callers retain their exact tenant/scope contract.
	if ok, _, _, _ := executeFeedIntegrityEpisodeSuppression(context.Background(), db, "other-tenant", models.OperatorActionPlan{ActorID: canonical.ActorID}, canonical); ok {
		t.Fatal("cross-tenant suppression accepted")
	}
	ok, _, output, _ := executeFeedIntegrityEpisodeSuppression(context.Background(), db, "default", models.OperatorActionPlan{ActorID: canonical.ActorID}, canonical)
	if !ok {
		t.Fatal(output)
	}
	if _, err := claimFeedIntegrityAction(db, "default", action.ID); err == nil {
		t.Fatal("Operator suppression allowed approved repair")
	}
	canonical.NormalizedArguments = map[string]any{"suppression_id": output["suppression_id"]}
	if ok, _, _, _ := executeFeedIntegritySuppressionRevoke(context.Background(), db, "other-tenant", canonical); ok {
		t.Fatal("cross-tenant revocation accepted")
	}
	if ok, _, output, _ := executeFeedIntegritySuppressionRevoke(context.Background(), db, "default", canonical); !ok {
		t.Fatal(output)
	}
	if _, err := claimFeedIntegrityAction(db, "default", action.ID); err == nil {
		t.Fatal("revocation resurrected pre-suppression approval")
	}
	if integrityEpisodeSuppressed(db, "default", ep, time.Now()) {
		t.Fatal("revoked suppression remained active")
	}
}

func TestFeedIntegrityDBVerificationRejectsHealthyUnrelatedDelivery(t *testing.T) {
	for _, scenario := range []string{"obsolete_generation", "missing_generation", "live_fallback", "different_cached_version", "changed_during_probe"} {
		t.Run(scenario, func(t *testing.T) {
			db := feedIntegrityTestDB(t)
			_, _, action := seedFeedIntegrityAction(t, db, "default", "approved")
			builtAt := time.Now().UTC().Truncate(time.Microsecond)
			if err := db.Model(&models.NewsSnapshot{}).Where("tenant_id='default' AND \"window\"='today'").Updates(map[string]interface{}{"built_at": builtAt, "generation": 1, "dirty": false, "slide_count": 1}).Error; err != nil {
				t.Fatal(err)
			}
			if scenario != "missing_generation" {
				generation := int64(1)
				if scenario == "obsolete_generation" {
					generation = 2
				}
				if err := db.Create(&models.NewsSnapshotGeneration{TenantID: "default", Window: "today", Generation: generation}).Error; err != nil {
					t.Fatal(err)
				}
			}
			action.Outcome, action.ClaimToken = "verifying", "qualification-claim"
			action.Output = integrityJSON(map[string]interface{}{"effect_known": true, "built_at": builtAt, "generation": 1})
			if err := db.Save(&action).Error; err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				servedAt := builtAt
				source := "cache"
				if scenario == "live_fallback" {
					source = "live"
				}
				if scenario == "different_cached_version" {
					servedAt = servedAt.Add(time.Millisecond)
				}
				if scenario == "changed_during_probe" {
					if err := db.Model(&models.NewsSnapshot{}).Where("tenant_id='default' AND \"window\"='today'").Update("dirty", true).Error; err != nil {
						t.Error(err)
					}
				}
				w.Header().Set("X-Wahb-Feed-Source", source)
				w.Header().Set("X-Wahb-Snapshot-Window", "today")
				w.Header().Set("X-Wahb-Snapshot-Age-Ms", "1")
				w.Header().Set("X-Wahb-Snapshot-Built-At", servedAt.Format(time.RFC3339Nano))
				w.Header().Set("X-Wahb-Snapshot-Dirty", "false")
				_ = json.NewEncoder(w).Encode(map[string]interface{}{"slides": []interface{}{map[string]interface{}{"featured": map[string]interface{}{"story_id": uuid.NewString(), "members": []interface{}{map[string]interface{}{}}}, "related": []interface{}{}}}})
			}))
			defer server.Close()
			u, _ := url.Parse(server.URL)
			t.Setenv("PORT", u.Port())
			verifyFeedIntegrityAction(db, action)
			if err := db.First(&action, action.ID).Error; err != nil {
				t.Fatal(err)
			}
			if action.Outcome != "verification_failed" {
				t.Fatalf("unrelated healthy delivery verified obsolete repair: %s %s", action.Outcome, action.Verification)
			}
		})
	}
}

func TestFeedIntegrityDBRecoveryCallerRejectsUnknownAndLegacyEvidence(t *testing.T) {
	db := feedIntegrityTestDB(t)
	now := time.Now().UTC()
	run := models.FeedIntegrityRun{TenantID: "default", Tier: "deep", Status: "completed", StartedAt: now, FinishedAt: &now}
	if err := db.Create(&run).Error; err != nil {
		t.Fatal(err)
	}
	c := newIntegrityCollector(run, nil, nil)
	for _, feed := range []string{"pods", "news"} {
		variant := "default"
		if feed == "news" {
			variant = "window:today"
		}
		c.edgeResult("edge_"+feed+"_http", feed, variant, "ok", variant, 2, nil)
		c.edgeResult("edge_"+feed+"_latency", feed, variant, "ok", variant, 2, nil)
		c.add("edge_"+feed+"_page_success", "edge", feed, variant, "consumer", "minor", "ok", "page", variant, 2, nil)
	}
	c.add("probe_url_dead", "probe", "pods", "default", "consumer", "major", "ok", "probe", "content:fixture:primary", 1, nil)
	c.add("probe_url_success", "probe", "pods", "default", "consumer", "minor", "ok", "probe", "content:fixture:primary", 1, map[string]interface{}{"role": "primary", "complete": true})
	run.LaneResults = integrityJSON(c.lanes())
	if err := db.Create(&c.findings).Error; err != nil {
		t.Fatal(err)
	}
	if !feedIntegrityLatencyClean(db, run) {
		t.Fatal("qualified latency rejected")
	}
	if n, err := feedIntegrityPageCount(db, run, "edge_pods_page_success", "pods", "default"); err != nil || n != 2 {
		t.Fatal(n, err)
	}
	if n, err := feedIntegritySuccessfulPlaybackProbes(db, run); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	for _, lanes := range [][]byte{integrityJSON(map[string]interface{}{"edge": map[string]interface{}{"executed": 1}}), nil} {
		run.LaneResults = lanes
		if feedIntegrityLatencyClean(db, run) {
			t.Fatal("legacy/missing latency accepted")
		}
		if _, err := feedIntegrityPageCount(db, run, "edge_pods_page_success", "pods", "default"); err == nil {
			t.Fatal("legacy/missing page evidence accepted")
		}
		if _, err := feedIntegritySuccessfulPlaybackProbes(db, run); err == nil {
			t.Fatal("legacy/missing probe evidence accepted")
		}
	}
	run.LaneResults = integrityJSON(c.lanes())
	run.Status = "partial"
	if feedIntegrityLatencyClean(db, run) {
		t.Fatal("partial run accepted")
	}
	if _, err := feedIntegritySuccessfulPlaybackProbes(db, run); err == nil {
		t.Fatal("partial run probe proof accepted")
	}
}
