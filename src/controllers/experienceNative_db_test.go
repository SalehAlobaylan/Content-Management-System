package controllers

import (
	"bytes"
	"content-management-system/src/models"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

func TestExperienceNativeDatabasePopulationsFreshnessAndSuppressionStaySeparate(t *testing.T) {
	db := experienceTestDB(t)
	nativeDatabasePopulationCases(t, db)
}

func nativeDatabasePopulationCases(t *testing.T, db *gorm.DB) {
	t.Helper()
	tenant := "rux-native-" + uuid.NewString()
	start := floorBucket(time.Now()).Add(-time.Hour)
	policy := models.DefaultExperiencePolicy(tenant)
	policy.EnabledClients = "web,ios,android"
	policy.MinSampleFloor = 1
	policy.LatencySampleFloor = 1
	if err := db.Create(&policy).Error; err != nil {
		t.Fatal(err)
	}
	for _, platform := range []string{"web", "ios", "android"} {
		ev := terminalFixture("startup", "success")
		ev.ClientPlatform = platform
		ev.TenantID = tenant
		ev.SchemaVersion = 2
		if platform != "web" {
			ev.SchemaVersion = 3
		}
		ev.ReceivedAt = start.Add(time.Minute)
		ev.OccurredAt = ev.ReceivedAt
		ev.Release = "same-build"
		ev.SessionID = "same-process"
		ev.OSMajor = 26
		if platform == "android" {
			ev.Outcome = "failed"
		}
		if _, err := persistExperienceEvent(db, &ev); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		_, err := rollupBucket(tx, tenant, start, map[string]bool{"same-build": true}, 0)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for _, platform := range []string{"web", "ios", "android"} {
		var row models.ExperienceMetricRollup
		if err := db.Where("tenant_id=? AND client_platform=? AND metric_key=? AND cohort_dim='global'", tenant, platform, mPlaybackStartSuccess).First(&row).Error; err != nil {
			t.Fatal(err)
		}
		if row.Denominator != 1 || row.ActiveSessions != 1 {
			t.Fatalf("platforms pooled: %+v", row)
		}
		if platform == "android" && row.Numerator != 0 {
			t.Fatal("Web success made Android healthy")
		}
	}
	fresh := terminalFixture("feed", "success")
	fresh.ClientPlatform = "ios"
	fresh.SchemaVersion = 3
	fresh.TenantID = tenant
	fresh.ReceivedAt = time.Now().UTC()
	fresh.OccurredAt = fresh.ReceivedAt
	if err := db.Create(&fresh).Error; err != nil {
		t.Fatal(err)
	}
	for _, platform := range []string{"web", "ios", "android"} {
		ok, err := surfaceFresh(db, tenant, "pods", policy, time.Now(), platform)
		if err != nil || ok != (platform == "ios") {
			t.Fatalf("freshness crossed clients: %s %v %v", platform, ok, err)
		}
	}
	mute := models.ExperienceSuppression{TenantID: tenant, ClientPlatform: "ios", StartsAt: start, ExpiresAt: start.Add(3 * time.Hour)}
	if err := db.Create(&mute).Error; err != nil {
		t.Fatal(err)
	}
	for _, platform := range []string{"web", "ios", "android"} {
		ok, err := suppressed(db, tenant, mPlaybackStartSuccess, "pods", "global", "all", start.Add(time.Hour), platform)
		if err != nil || ok != (platform == "ios") {
			t.Fatalf("suppression crossed clients: %s %v %v", platform, ok, err)
		}
	}
	response := experienceAdminRequest(db, tenant, "GET", "/metrics", "/metrics?client_platform=android", "", GetExperienceMetrics)
	var body map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || response.Code != 200 {
		t.Fatal(response.Body.String())
	}
}

func TestExperienceNativeHTTPAdmissionBindsClaimsAndCannotUseWebTrust(t *testing.T) {
	db := experienceTestDB(t)
	t.Setenv("RUX_INGEST_TOKEN", "private-fixture-key")
	t.Setenv("WAHB_RUNTIME_ID", "native-fixture-launch")
	policy := models.DefaultExperiencePolicy(ruxDefaultTenant)
	policy.EnabledClients = "web,ios,android"
	policy.IngestEnabled = true
	if err := db.Create(&policy).Error; err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	router.Use(func(c *gin.Context) { c.Set("db", db); c.Next() })
	router.POST("/admission", CreateNativeExperienceAdmission)
	router.POST("/native", NativeExperienceAdmissionMiddleware(), IngestExperienceEvents)
	router.POST("/web", RuxIngestAuthMiddleware(), IngestExperienceEvents)
	request := func(path string, body any, ticket string) *httptest.ResponseRecorder {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-RUX-Native-Ticket", ticket)
		// Caller rate/forwarding headers must not bypass the native process budget.
		req.Header.Set(ruxRateKeyHeader, uuid.NewString())
		res := httptest.NewRecorder()
		router.ServeHTTP(res, req)
		return res
	}
	claims := nativeExperienceTicket{Platform: "ios", Session: uuid.NewString(), Page: uuid.NewString(), Release: "native-build-1"}
	res := request("/admission", claims, "")
	var admission struct {
		Ticket string `json:"ticket"`
	}
	if res.Code != 200 || json.Unmarshal(res.Body.Bytes(), &admission) != nil || admission.Ticket == "" {
		t.Fatal(res.Body.String())
	}
	journey := uuid.NewString()
	event := map[string]any{"event_id": uuid.NewString(), "schema_version": 3, "event_type": "journey_terminal", "surface": "pods", "phase": "startup", "outcome": "success", "occurred_at": time.Now().UTC().Format(time.RFC3339), "journey_id": journey, "session_id": claims.Session, "page_load_id": claims.Page, "release": claims.Release, "client": map[string]any{"platform": "ios", "os_major": 26, "browser_family": "other", "device_class": "mobile", "network_class": "unknown"}, "measurements": map[string]any{"visible": true, "duration_ms": 250}}
	batch := func() map[string]any { return map[string]any{"schema_version": 3, "events": []any{event}} }
	res = request("/native", batch(), admission.Ticket)
	var counts struct{ Accepted, Duplicate, Rejected int }
	if res.Code != 200 || json.Unmarshal(res.Body.Bytes(), &counts) != nil || counts.Accepted != 1 {
		t.Fatal(res.Body.String())
	}
	for _, key := range []string{"session_id", "page_load_id", "release", "client"} {
		before := event[key]
		event["event_id"] = uuid.NewString()
		if key == "client" {
			event[key] = map[string]any{"platform": "android", "os_major": 16}
		} else {
			event[key] = uuid.NewString()
		}
		res = request("/native", batch(), admission.Ticket)
		if res.Code != 200 || json.Unmarshal(res.Body.Bytes(), &counts) != nil || counts.Accepted != 0 || counts.Rejected != 1 {
			t.Fatalf("unbound%s: %s", key, res.Body.String())
		}
		event[key] = before
	}
	t.Setenv("WAHB_RUNTIME_ID", "different-launch")
	if res = request("/native", batch(), admission.Ticket); res.Code != 409 {
		t.Fatal("old launch ticket admitted into a new environment")
	}
	t.Setenv("WAHB_RUNTIME_ID", "native-fixture-launch")
	if res = request("/native", batch(), admission.Ticket+"x"); res.Code != 401 {
		t.Fatal("forged ticket admitted")
	}
	if res = request("/web", batch(), admission.Ticket); res.Code != 401 {
		t.Fatal("native ticket authorized Web/BFF trust")
	}
	var persisted int64
	if err := db.Model(&models.ExperienceEvent{}).Count(&persisted).Error; err != nil || persisted != 1 {
		t.Fatalf("invalid claim wrote evidence: %d %v", persisted, err)
	}
	if err := db.Model(&policy).Update("enabled_clients", "web").Error; err != nil {
		t.Fatal(err)
	}
	res = request("/admission", claims, "")
	var disabled struct{ Disabled bool }
	if json.Unmarshal(res.Body.Bytes(), &disabled) != nil || !disabled.Disabled {
		t.Fatal("native disable not enforced")
	}
	res = request("/native", batch(), admission.Ticket)
	if json.Unmarshal(res.Body.Bytes(), &disabled) != nil || !disabled.Disabled {
		t.Fatal("old admission bypassed current client policy")
	}
}
func TestExperienceNativeFleetSnapshotAndPolicyStaySeparate(t *testing.T) {
	db := experienceTestDB(t)
	tenant := "rux-native-" + uuid.NewString()
	policy := models.DefaultExperiencePolicy(tenant)
	checkpoint := floorBucket(time.Now()).Add(-time.Hour)
	policy.LastEvaluatedBucket = &checkpoint
	if err := db.Create(&policy).Error; err != nil {
		t.Fatal(err)
	}
	res := experienceAdminRequest(db, tenant, "PUT", "/policy", "/policy", `{"enabled_clients":"web,ios,android","last_evaluated_bucket":null}`, UpdateExperiencePolicy)
	if res.Code != 200 {
		t.Fatal(res.Body.String())
	}
	if err := db.Where("tenant_id=?", tenant).First(&policy).Error; err != nil || policy.LastEvaluatedBucket == nil || !policy.LastEvaluatedBucket.Equal(checkpoint) {
		t.Fatal("native policy reset checkpoint", err)
	}
	verdicts, fresh, clients, err := allClientExperienceSnapshot(db, tenant, policy, time.Now())
	if err != nil || fresh || len(verdicts) != 6 || len(clients) != 3 {
		t.Fatalf("fleet snapshot omitted clients: %#v %v %v", verdicts, fresh, err)
	}
	for _, value := range verdicts {
		if value.(map[string]any)["verdict"] == "healthy" {
			t.Fatal("missing native evidence declared healthy")
		}
	}
	res = experienceAdminRequest(db, tenant, "PUT", "/policy", "/policy", `{"enabled_clients":"web,ios,unknown"}`, UpdateExperiencePolicy)
	if res.Code != 400 {
		t.Fatal("invalid platform admitted")
	}
}

func TestExperienceNativeConfirmationRecoveryAndDetailDoNotCrossClients(t *testing.T) {
	db := experienceTestDB(t)
	tenant := "rux-native-" + uuid.NewString()
	policy := models.DefaultExperiencePolicy(tenant)
	policy.EnabledClients = "web,ios,android"
	if err := db.Create(&policy).Error; err != nil {
		t.Fatal(err)
	}
	start := floorBucket(time.Now()).Add(-6 * time.Hour)
	observe := func(platform, status string, hour int) {
		st := sliStatus{ClientPlatform: platform, Metric: mPlaybackStartSuccess, Label: "Startup", Status: status, LikelyOwner: "wahb_platform"}
		if err := updateIncident(db, tenant, policy, nil, "pods", "global", "all", st, status == "breach", start.Add(time.Duration(hour)*time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	observe("ios", "breach", 0)
	observe("android", "breach", 0)
	observe("ios", "breach", 1)
	observe("android", "ok", 1)
	observe("web", "ok", 1)
	var ios models.ExperienceIncident
	if err := db.Where("tenant_id=? AND client_platform='ios'", tenant).First(&ios).Error; err != nil || ios.Status != "open" || ios.LikelyOwner != "wahb_mobile" {
		t.Fatalf("native confirmation/owner lost: %+v %v", ios, err)
	}
	for hour := 2; hour <= 4; hour++ {
		observe("ios", "ok", hour)
	}
	observe("android", "breach", 2)
	observe("android", "breach", 3)
	if err := db.Where("id=?", ios.ID).First(&ios).Error; err != nil || ios.Status != "resolved" {
		t.Fatal("Android failure blocked iOS recovery", err)
	}
	var android models.ExperienceIncident
	if err := db.Where("tenant_id=? AND client_platform='android' AND status='open'", tenant).First(&android).Error; err != nil {
		t.Fatal("iOS recovery cleared Android", err)
	}
	response := experienceAdminRequest(db, tenant, "GET", "/evidence/:kind/:id", "/evidence/incident/"+android.PublicID.String()+"?client_platform=ios", "", GetExperienceEvidence)
	if response.Code == 200 {
		t.Fatal("Android incident leaked into selected iOS detail")
	}
	response = experienceAdminRequest(db, tenant, "GET", "/evidence/:kind/:id", "/evidence/incident/"+android.PublicID.String()+"?client_platform=android", "", GetExperienceEvidence)
	if response.Code != 200 {
		t.Fatal(response.Body.String())
	}
	var webCount int64
	if err := db.Model(&models.ExperienceIncident{}).Where("tenant_id=? AND client_platform='web'", tenant).Count(&webCount).Error; err != nil || webCount != 0 {
		t.Fatal("native incident created Web degradation", err)
	}
}

func TestExperienceNativeOSSuppressionAndBrowserBudget(t *testing.T) {
	db := experienceTestDB(t)
	tenant := "rux-native-" + uuid.NewString()
	p := models.DefaultExperiencePolicy(tenant)
	if err := db.Create(&p).Error; err != nil {
		t.Fatal(err)
	}
	for _, platform := range []string{"ios", "android", "web"} {
		body := `{"client_platform":"` + platform + `","metric_key":"playback_start_success","surface":"pods","cohort_dim":"os","cohort_val":"26","reason":"exact build issue","ttl_minutes":60}`
		res := experienceAdminRequest(db, tenant, "POST", "/suppression", "/suppression", body, CreateExperienceSuppression)
		if (res.Code == 200) != (platform != "web") {
			t.Fatalf("OS suppression scope%s: %d %s", platform, res.Code, res.Body.String())
		}
	}
	for _, value := range []string{"-1", "1000", "026", "unknown"} {
		if validExperienceCohort("os", value) {
			t.Fatalf("invalid OS cohort%s", value)
		}
	}
	now := time.Now().UTC()
	rows := []models.ExperienceEvent{}
	for major := 0; major < 24; major++ {
		ev := terminalFixture("feed", "success")
		ev.TenantID = tenant
		ev.ClientPlatform = "web"
		ev.BrowserMajor = major
		ev.ReceivedAt = now.Add(-time.Minute)
		rows = append(rows, ev)
	}
	for n := 0; n < 100; n++ {
		ev := terminalFixture("feed", "success")
		ev.TenantID = tenant
		ev.SchemaVersion = 3
		ev.ClientPlatform = "ios"
		ev.BrowserFamily = "other"
		ev.BrowserMajor = 0
		ev.ReceivedAt = now.Add(-time.Minute)
		rows = append(rows, ev)
	}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	kept, err := keptReleaseSet(db, tenant, 8, now)
	if err != nil {
		t.Fatal(err)
	}
	for major := 0; major < 24; major++ {
		if !kept["browser:safari-"+strconv.Itoa(major)] {
			t.Fatal("native traffic displaced Web browser cohort")
		}
	}
	if kept["browser:other-0"] {
		t.Fatal("native browser placeholder entered Web budget")
	}
}
