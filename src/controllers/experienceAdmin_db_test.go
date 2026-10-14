package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

func experienceAdminRequest(db *gorm.DB, tenant, method, pattern, path, body string, handler gin.HandlerFunc) *httptest.ResponseRecorder {
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("db", db)
		c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: tenant, Email: "operator@example.test"})
		c.Next()
	})
	router.Handle(method, pattern, handler)
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	return response
}

func TestExperienceDatabaseLostFallbackTerminalWithholdsCoverage(t *testing.T) {
	db := experienceTestDB(t)
	start := floorBucket(time.Now()).Add(-time.Hour)
	ev := terminalFixture("fallback", "success")
	ev.EventType, ev.TerminalStatus, ev.Phase, ev.Outcome = "playback_fallback", "", "", ""
	ev.ReceivedAt, ev.OccurredAt = start.Add(time.Minute), start.Add(time.Minute)
	if err := db.Create(&ev).Error; err != nil {
		t.Fatal(err)
	}
	complete, err := bucketIntegrity(db, ev.TenantID, "pods", start)
	if err != nil || complete {
		t.Fatalf("lost fallback outcome claimed coverage: %v %v", complete, err)
	}
	terminal := ev
	terminal.ID, terminal.EventID = 0, uuid.New()
	terminal.EventType, terminal.Phase, terminal.Outcome, terminal.TerminalStatus = "journey_terminal", "fallback", "success", "accepted"
	if err := db.Create(&terminal).Error; err != nil {
		t.Fatal(err)
	}
	complete, err = bucketIntegrity(db, ev.TenantID, "pods", start)
	if err != nil || !complete {
		t.Fatalf("correlated recovery was lost: %v %v", complete, err)
	}
}

func TestExperienceDatabaseAdminPolicyPreservesCheckpoints(t *testing.T) {
	db := experienceTestDB(t)
	tenant := "rux-" + uuid.NewString()
	policy := models.DefaultExperiencePolicy(tenant)
	checkpoint := floorBucket(time.Now()).Add(-time.Hour)
	policy.LastEvaluatedBucket = &checkpoint
	if err := db.Create(&policy).Error; err != nil {
		t.Fatal(err)
	}
	response := experienceAdminRequest(db, tenant, http.MethodPut, "/policy", "/policy",
		`{"min_sample_floor":75,"last_evaluated_bucket":null,"last_minute_bucket":null}`, UpdateExperiencePolicy)
	if response.Code != 200 {
		t.Fatal(response.Body.String())
	}
	if err := db.Where("tenant_id=?", tenant).First(&policy).Error; err != nil {
		t.Fatal(err)
	}
	if policy.MinSampleFloor != 75 || policy.LastEvaluatedBucket == nil || !policy.LastEvaluatedBucket.Equal(checkpoint) {
		t.Fatal("admin patch changed durable checkpoint")
	}
	var audit models.ExperienceAction
	if err := db.Where("tenant_id=? AND action_class='policy_updated'", tenant).First(&audit).Error; err != nil || !strings.Contains(string(audit.Evidence), `"before"`) || !strings.Contains(string(audit.Evidence), `"after"`) {
		t.Fatal("policy change lacks persisted before/after audit")
	}
}

func TestExperienceDatabaseAdminUnavailableAndExactExceptions(t *testing.T) {
	db := experienceTestDB(t)
	tenant := "rux-" + uuid.NewString()
	policy := models.DefaultExperiencePolicy(tenant)
	if err := db.Create(&policy).Error; err != nil {
		t.Fatal(err)
	}
	callback := "rux_read_failure_" + uuid.NewString()
	db.Callback().Query().Before("gorm:query").Register(callback, func(tx *gorm.DB) {
		if tx.Statement.Table == "experience_events" {
			tx.AddError(errors.New("injected unavailable evidence"))
		}
	})
	response := experienceAdminRequest(db, tenant, http.MethodGet, "/status", "/status", "", GetExperienceStatus)
	db.Callback().Query().Remove(callback)
	if response.Code != 503 {
		t.Fatalf("read failure presented as success: %s", response.Body.String())
	}
	response = experienceAdminRequest(db, tenant, http.MethodPost, "/suppressions", "/suppressions",
		`{"metric_key":"feed_render_success","surface":"pods","cohort_dim":"browser","cohort_val":"safari-18","reason":"fixture","ttl_minutes":0}`, CreateExperienceSuppression)
	if response.Code != 400 {
		t.Fatal("zero-TTL suppression accepted")
	}
	response = experienceAdminRequest(db, tenant, http.MethodPost, "/suppressions", "/suppressions",
		`{"metric_key":"feed_render_success","surface":"pods","cohort_dim":"browser","cohort_val":"safari-18","reason":"fixture","ttl_minutes":60}`, CreateExperienceSuppression)
	if response.Code != 200 {
		t.Fatal(response.Body.String())
	}
	var suppression models.ExperienceSuppression
	if err := db.Where("tenant_id=?", tenant).First(&suppression).Error; err != nil {
		t.Fatal(err)
	}
	if suppression.CohortDim != "browser" || suppression.CohortVal != "safari-18" || suppression.Surface != "pods" {
		t.Fatal("suppression scope silently broadened")
	}
}

func TestExperienceDatabaseReleaseComparisonUsesMatchingVolume(t *testing.T) {
	db := experienceTestDB(t)
	tenant := "rux-" + uuid.NewString()
	bucket := floorBucket(time.Now()).Add(-time.Hour)
	for _, release := range []string{"r2", "r3"} {
		for i := 0; i < 60; i++ {
			ev := terminalFixture("feed", "success")
			ev.TenantID, ev.Release, ev.SessionID = tenant, release, uuid.NewString()
			ev.ReceivedAt = bucket.Add(time.Minute)
			if release == "r3" {
				ev.ReceivedAt = ev.ReceivedAt.Add(time.Minute)
			}
			if _, err := persistExperienceEvent(db, &ev); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := rollupBucket(db, tenant, bucket, map[string]bool{"r2": true, "r3": true}, 1); err != nil {
		t.Fatal(err)
	}
	policy := models.DefaultExperiencePolicy(tenant)
	comparison, err := compareExperienceReleases(db, tenant, policy, bucket, bucket.Add(time.Hour))
	if err != nil || !comparison.Qualified || comparison.Current != "r3" || comparison.Previous != "r2" {
		t.Fatalf("comparison %+v: %v", comparison, err)
	}
	found := false
	for _, metric := range comparison.Metrics {
		if metric.Metric == mFeedRenderSuccess {
			found = metric.CurrentDenominator == 60 && metric.PreviousDenominator == 60 && metric.Windows == 1 && metric.LatencyQualified
		}
	}
	if !found {
		t.Fatalf("matching complete feed windows absent: %+v", comparison.Metrics)
	}
	if err := db.Model(&models.ExperienceMetricRollup{}).Where("tenant_id=? AND cohort_dim='release' AND cohort_val='r2'", tenant).Update("denominator", 49).Error; err != nil {
		t.Fatal(err)
	}
	comparison, err = compareExperienceReleases(db, tenant, policy, bucket, bucket.Add(time.Hour))
	if err != nil || comparison.Qualified {
		t.Fatal("sparse previous release claimed comparable")
	}
}

func TestExperienceDatabaseLatencyAffectedSessionsHonorPolicy(t *testing.T) {
	db := experienceTestDB(t)
	tenant := "rux-" + uuid.NewString()
	bucket := floorBucket(time.Now()).Add(-time.Hour)
	ev := terminalFixture("feed", "success")
	ev.TenantID, ev.ReceivedAt = tenant, bucket.Add(time.Minute)
	duration := 3000
	ev.DurationMS = &duration
	if _, err := persistExperienceEvent(db, &ev); err != nil {
		t.Fatal(err)
	}
	policy := models.DefaultExperiencePolicy(tenant)
	if err := completeAffectedSessions(db, tenant, bucket, bucket.Add(time.Hour), "hour", map[string]bool{"r2": true}, policy); err != nil {
		t.Fatal(err)
	}
	// Produce the metric row before attaching its exact affected population.
	if _, err := rollupBucket(db, tenant, bucket, map[string]bool{"r2": true}, 1); err != nil {
		t.Fatal(err)
	}
	var row models.ExperienceMetricRollup
	if err := db.Where("tenant_id=? AND metric_key=? AND cohort_dim='global'", tenant, mFeedRenderSuccess).First(&row).Error; err != nil || row.AffectedSessions != 1 {
		t.Fatalf("slow successful session omitted: %v", err)
	}
	policy.Thresholds = jsonBytes(map[string]any{mFeedRenderSuccess: map[string]int{"latency_p75_ms": 3500}})
	if err := completeAffectedSessions(db, tenant, bucket, bucket.Add(time.Hour), "hour", map[string]bool{"r2": true}, policy); err != nil {
		t.Fatal(err)
	}
	if err := db.First(&row, row.ID).Error; err != nil || row.AffectedSessions != 0 {
		t.Fatal("affected sessions ignored configured latency budget")
	}
}

func TestExperienceDatabaseEmptyEvidenceIsAnArray(t *testing.T) {
	db := experienceTestDB(t)
	tenant := "rux-" + uuid.NewString()
	policy := models.DefaultExperiencePolicy(tenant)
	checkpoint := floorBucket(time.Now()).Add(-time.Hour)
	policy.LastEvaluatedBucket = &checkpoint
	if err := db.Create(&policy).Error; err != nil {
		t.Fatal(err)
	}
	response := experienceAdminRequest(db, tenant, http.MethodGet, "/evidence/:kind", "/evidence/cohorts", "", GetExperienceEvidence)
	if response.Code != 200 || !strings.Contains(response.Body.String(), `"rows":[]`) {
		t.Fatalf("empty aggregate evidence is not renderable: %s", response.Body.String())
	}
}
