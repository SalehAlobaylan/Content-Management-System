package controllers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/utils"
	"github.com/gin-gonic/gin"
	"github.com/lib/pq"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

func opsOwnerChecksum(t *testing.T, db *gorm.DB) string {
	t.Helper()
	var names []string
	if err := db.Raw("SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename NOT LIKE 'ops_%' ORDER BY tablename").Scan(&names).Error; err != nil {
		t.Fatal(err)
	}
	parts := []string{}
	for _, name := range names {
		parts = append(parts, fmt.Sprintf("SELECT %s AS owner_table,md5(COALESCE(jsonb_agg(to_jsonb(r) ORDER BY to_jsonb(r)::text)::text,'[]')) AS digest FROM %s r", "'"+strings.ReplaceAll(name, "'", "''")+"'", pq.QuoteIdentifier(name)))
	}
	var rows []struct{ OwnerTable, Digest string }
	if err := db.Raw("SELECT * FROM (" + strings.Join(parts, " UNION ALL ") + ") owners ORDER BY owner_table").Scan(&rows).Error; err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(rows)
	return string(raw)
}
func opsReadRequest(db *gorm.DB, path string, handler gin.HandlerFunc) *httptest.ResponseRecorder {
	return opsFixtureRequest(db, http.MethodGet, path, nil, handler)
}
func opsFixtureRequest(db *gorm.DB, method, path string, body []byte, handler gin.HandlerFunc) *httptest.ResponseRecorder {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("db", db)
		c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{UserID: "ops-reader", Email: "ops-reader@example.test", TenantID: "ops-fixture", Role: "admin"})
		c.Next()
	})
	router.Handle(method, path, handler)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(method, path, bytes.NewReader(body)))
	return rec
}

// One canonical fixture exercises real owner schema, populated adapters, every
// read endpoint, and a checksum of ALL non-ops tables around those reads.
func TestOpsReadCanonicalAcceptanceAndForeignReadOnlyProof(t *testing.T) {
	db := opsTestDB(t)
	t.Setenv("JWT_SECRET", "operations-fixture-signing-secret-not-a-credential")
	now := time.Now().UTC().Truncate(time.Microsecond)
	tenant := "ops-fixture"
	old := now.Add(-48 * time.Hour)
	p := models.DefaultPipelineAutopilotPolicy(tenant)
	p.Enabled = true
	p.LastRunAt = &now
	if err := db.Create(&p).Error; err != nil {
		t.Fatal(err)
	}
	second := models.DefaultPipelineAutopilotPolicy("ops-second-tenant")
	second.Enabled = true
	second.LastRunAt = &old
	if err := db.Create(&second).Error; err != nil {
		t.Fatal(err)
	}
	finished := now.Add(-5 * time.Minute)
	run := models.PipelineAutopilotRun{TenantID: tenant, Trigger: "manual", Mode: "observe", Status: "completed", Headline: "observed", StartedAt: now.Add(-10 * time.Minute), FinishedAt: &finished}
	if err := db.Create(&run).Error; err != nil {
		t.Fatal(err)
	}
	incident := models.ExperienceIncident{TenantID: tenant, ClientPlatform: "ios", Fingerprint: "native-playback", MetricKey: "playback_start_success", Surface: "pods", CohortDim: "global", CohortVal: "all", Severity: "degraded", Status: "open", Summary: "iOS startup failures", ConfirmedAt: &finished, FirstSeenAt: now.Add(-time.Hour), LastSeenAt: now, Evidence: datatypes.JSON([]byte(`{}`))}
	if err := db.Create(&incident).Error; err != nil {
		t.Fatal(err)
	}
	title := "Fixture recording"
	item := models.ContentItem{TenantID: tenant, Type: models.ContentTypePodcast, Source: models.SourceTypePodcast, Status: models.ContentStatusPending, Title: &title}
	if err := db.Create(&item).Error; err != nil {
		t.Fatal(err)
	}
	transcript := models.Transcript{ContentItemID: item.PublicID, FullText: "fixture transcript"}
	if err := db.Create(&transcript).Error; err != nil {
		t.Fatal(err)
	}
	published := models.Chapter{TenantID: tenant, TranscriptID: transcript.PublicID, Title: "Already resolved", Source: "derived", Status: "published"}
	if err := db.Create(&published).Error; err != nil {
		t.Fatal(err)
	}
	review := models.Chapter{TenantID: tenant, TranscriptID: transcript.PublicID, Title: "Current review", Source: "derived", Status: "needs_review"}
	if err := db.Create(&review).Error; err != nil {
		t.Fatal(err)
	}
	studioRun := models.MediaStudioRun{TenantID: tenant, Trigger: "manual", Mode: "observe", Status: "completed", StartedAt: now.Add(-5 * time.Minute), FinishedAt: &finished}
	if err := db.Create(&studioRun).Error; err != nil {
		t.Fatal(err)
	}
	stale := models.MediaStudioAction{TenantID: tenant, RunID: studioRun.ID, ChapterID: &published.PublicID, UnitType: "chapter_review", Verdict: "review", ToolName: "proposals.generate", Status: "approval_required", StartedAt: now.Add(-time.Hour), Proposal: datatypes.JSON([]byte(`{"proposal":"publish"}`))}
	if err := db.Create(&stale).Error; err != nil {
		t.Fatal(err)
	}
	supplyEpisode := models.MediaSupplyEpisode{TenantID: tenant, Fingerprint: "supply-fixture", FirstFailedBoundary: "source_delivery", Verdict: "blocked", Severity: "major", Owner: "aggregation", State: "open", Summary: "Supply requires owner review", AffectedSubjects: datatypes.JSON([]byte(`[]`)), EvidenceDigest: strings.Repeat("a", 64), EvidenceCompleteness: "complete", Evidence: datatypes.JSON([]byte(`{}`)), FirstSeenAt: now.Add(-time.Hour), LastSeenAt: now}
	if err := db.Create(&supplyEpisode).Error; err != nil {
		t.Fatal(err)
	}
	proposal := models.TopicProposal{TenantID: tenant, SuggestedSlug: "resolved-fixture", Status: "approved", CreatedAt: old, ResolvedAt: &finished}
	if err := db.Create(&proposal).Error; err != nil {
		t.Fatal(err)
	}
	before := opsOwnerChecksum(t, db)
	ctx, cancel := context.WithTimeout(context.Background(), opsReadDeadline)
	s := collectOpsSnapshot(db.WithContext(ctx), now, true)
	cancel()
	if !s.Complete {
		t.Fatalf("seeded snapshot incomplete: %v", s.Errors)
	}
	native, openReview, supply, secondStalled, runPaired := false, false, false, false, false
	for _, attention := range s.Attention {
		switch attention.System {
		case "experience":
			native = attention.Severity == "major" && strings.Contains(attention.Href, "client=ios")
		case "media_studio":
			if attention.Kind == "approval" {
				if strings.Contains(attention.Key, published.PublicID.String()) {
					t.Fatal("resolved historical Studio action resurfaced")
				}
				openReview = strings.Contains(attention.Key, review.PublicID.String())
			}
		case "supply_continuity":
			supply = strings.Contains(attention.Href, "/platform/media/circulation")
		}
	}
	for _, status := range s.Fleet {
		if status.MemberKey == "pipeline" && status.TenantID == tenant {
			runPaired = status.LastRunAt != nil && status.LastRunAt.Equal(run.StartedAt) && status.LastRunStatus == run.Status
			if status.NextDueAt == nil || !status.NextDueAt.Equal(now.Add(time.Duration(p.IntervalMinutes)*time.Minute)) {
				t.Fatal("displayed run evidence changed the scheduler watermark")
			}
		}
		if status.MemberKey == "pipeline" && status.TenantID == "ops-second-tenant" {
			secondStalled = status.State == "stalled"
		}
	}
	if !native || !openReview || !supply || !secondStalled || !runPaired {
		t.Fatalf("missing owner semantics native=%t review=%t supply=%t second=%t run=%t", native, openReview, supply, secondStalled, runPaired)
	}
	for _, route := range []struct {
		path    string
		handler gin.HandlerFunc
	}{{"/status", GetOpsStatus}, {"/attention", ListOpsAttention}, {"/fleet", GetOpsFleet}, {"/briefing", GetOpsBriefing}, {"/calendar", GetOpsCalendar}} {
		t.Run(route.path, func(t *testing.T) {
			start := time.Now()
			res := opsReadRequest(db, route.path, route.handler)
			if res.Code != 200 {
				t.Fatalf("read failed %d: %s", res.Code, res.Body.String())
			}
			var body struct {
				Data struct {
					Complete      bool     `json:"complete"`
					AdapterErrors []string `json:"adapter_errors"`
				}
			}
			if err := json.Unmarshal(res.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if !body.Data.Complete {
				t.Fatalf("read was partial: %v", body.Data.AdapterErrors)
			}
			t.Logf("%s completed in %s", route.path, time.Since(start).Round(time.Millisecond))
		})
	}
	if after := opsOwnerChecksum(t, db); after != before {
		t.Fatal("Operations observation mutated an owning-system table")
	}
	// Exercise cross-table history semantics without a timestamp-only unit mock.
	items, errs := collectOpsBriefing(db, now.Add(-24*time.Hour), now.Add(time.Second))
	if len(errs) > 0 {
		t.Fatalf("history adapter contract: %v", errs)
	}
	foundRun, foundEpisode, foundTopicReview := false, false, false
	for _, row := range items {
		foundRun = foundRun || row.System == "pipeline" && row.Kind == "run"
		foundEpisode = foundEpisode || row.System == "experience" && row.Kind == "episode"
		foundTopicReview = foundTopicReview || row.System == "preferences" && row.Kind == "topic_review" && row.Count == 1 && row.LastSeen.Equal(finished)
	}
	if !foundRun || !foundEpisode || !foundTopicReview {
		t.Fatal("briefing omitted runs, episodes, or a recent resolution of an older topic proposal")
	}
	runs, _, errs, _ := collectOpsCalendar(db, now)
	if len(errs) > 0 {
		t.Fatalf("calendar adapter contract: %v", errs)
	}
	found := false
	for _, r := range runs {
		if r.Key == "pipeline_autopilot_runs:"+fmt.Sprint(run.ID) {
			found = r.DurationMS == 5*60*1000
		}
	}
	if !found {
		t.Fatal("calendar lost recorded duration/status")
	}
	t.Run("briefing_high_water_race", func(t *testing.T) {
		response := opsReadRequest(db, "/briefing", GetOpsBriefing)
		var briefing struct {
			Data struct {
				Since     time.Time `json:"since"`
				Through   time.Time `json:"through"`
				ReadToken string    `json:"read_token"`
				Complete  bool      `json:"complete"`
			}
		}
		if err := json.Unmarshal(response.Body.Bytes(), &briefing); err != nil || !briefing.Data.Complete {
			t.Fatalf("complete briefing required: %s", response.Body.String())
		}
		// This event commits after GET captured through and before mark-as-read.
		later := models.OpsFleetCommand{TenantID: tenant, Command: "pause_member", Scope: "pipeline", Reason: "fixture after digest", IdempotencyKey: "briefing-after-get", ActorID: "fixture", ActorEmail: "fixture@example.test", Status: "succeeded", Counts: datatypes.JSON([]byte(`{}`))}
		if err := db.Create(&later).Error; err != nil {
			t.Fatal(err)
		}
		body, _ := json.Marshal(gin.H{"since": briefing.Data.Since, "through": briefing.Data.Through, "read_token": briefing.Data.ReadToken})
		seen := opsFixtureRequest(db, http.MethodPost, "/seen", body, MarkOpsBriefingSeen)
		if seen.Code != 200 {
			t.Fatalf("mark read failed: %s", seen.Body.String())
		}
		next := opsReadRequest(db, "/briefing", GetOpsBriefing)
		var digest struct {
			Data struct {
				Commands []models.OpsFleetCommand `json:"commands"`
			}
		}
		if err := json.Unmarshal(next.Body.Bytes(), &digest); err != nil {
			t.Fatal(err)
		}
		found := false
		for _, row := range digest.Data.Commands {
			found = found || row.PublicID == later.PublicID
		}
		if !found {
			t.Fatal("event between GET and mark-read was lost")
		}
		tampered, _ := json.Marshal(gin.H{"since": briefing.Data.Since, "through": briefing.Data.Through.Add(time.Hour), "read_token": briefing.Data.ReadToken})
		if res := opsFixtureRequest(db, http.MethodPost, "/seen", tampered, MarkOpsBriefingSeen); res.Code != 409 {
			t.Fatal("unsigned high-water advance accepted")
		}
	})
	if after := opsOwnerChecksum(t, db); after != before {
		t.Fatal("briefing cursor writes changed an owner table")
	}
}
