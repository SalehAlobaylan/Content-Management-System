package controllers

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/utils"
	"content-management-system/src/workload"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestWorkloadAuditActorComesFromVerifiedPrincipal(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Set("admin_email", "unverified@example.test")
	if workloadActor(c) != "" {
		t.Fatal("unverified legacy context became audit authority")
	}
	c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{UserID: "verified-actor", Role: "admin"})
	if workloadActor(c) != "verified-actor" {
		t.Fatal("policy mutation lost verified actor")
	}
}

func TestWorkloadPermitHistoryRejectsUnboundedAndMalformedCursors(t *testing.T) {
	for _, cursor := range []string{strings.Repeat("a", 201), "not-a-cursor", "YWJjfDE"} {
		response := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(response)
		c.Request = httptest.NewRequest(http.MethodGet, "/admin/workload/permits?before="+cursor, nil)
		if _, ok := workloadFilter(c, true); ok || response.Code != 400 {
			t.Fatal("invalid cursor accepted")
		}
	}
}

func TestWorkloadLaneUsesCanonicalQueuesAndRejectsPartialCoverage(t *testing.T) {
	now := time.Now().UTC()
	row := models.PipelineLaneHealthSnapshot{Lane: "pods", CapturedAt: now, StageCounts: datatypes.JSON([]byte(`{"pods-media-queue":4,"pods-completion-queue":3,"pods-atomization-queue":2,"pods-optional-queue":1}`))}
	all := workloadLaneSample(row, "")
	media := workloadLaneSample(row, "pods-media-queue")
	if !all.Available || all.Pressure != 10 || media.Pressure != 4 || !all.ObservedAt.Equal(now) {
		t.Fatalf("incorrect canonical lane accounting: %#v / %#v", all, media)
	}
	row.StageCounts = datatypes.JSON([]byte(`{"ai-queue":0,"pods-media-queue":4}`))
	if sample := workloadLaneSample(row, ""); sample.Available || sample.Healthy {
		t.Fatalf("partial legacy queue evidence became healthy: %#v", sample)
	}
	row.StageCounts = datatypes.JSON([]byte(`{"pods-media-queue":-1}`))
	if workloadLaneSample(row, "pods-media-queue").Available {
		t.Fatal("negative depth accepted")
	}
}

func TestWorkloadIntakeRequiresPrioritizedCoverageAndNonnegativeCounters(t *testing.T) {
	prioritized := 2
	q := autopilotQueueStat{Waiting: 4, Active: 3, Delayed: 1, Prioritized: &prioritized}
	if sample := workloadFetchSample(q, time.Now()); !sample.Available || sample.Pressure != 10 {
		t.Fatalf("wrong intake pressure: %+v", sample)
	}
	q.Prioritized = nil
	if workloadFetchSample(q, time.Now()).Available {
		t.Fatal("old intake response claimed complete coverage")
	}
	q.Prioritized = &prioritized
	q.Delayed = -1
	if workloadFetchSample(q, time.Now()).Available {
		t.Fatal("negative queue counter cancelled valid pressure")
	}
}

func TestWorkloadSpendAdapterMatchesCurrentOwnerContract(t *testing.T) {
	now := time.Now().UTC()
	for _, test := range []struct {
		name      string
		value     map[string]any
		available bool
		state     string
	}{
		{"normal", map[string]any{"allowed": true, "verdict": "within", "ledger_health": "healthy", "reason": "within_budget"}, true, "normal"},
		{"warning", map[string]any{"allowed": true, "verdict": "warning", "ledger_health": "healthy"}, true, "warning"},
		{"bounded_stop", map[string]any{"allowed": false, "verdict": "bounded_stop", "ledger_health": "healthy"}, true, "capped"},
		{"paused", map[string]any{"allowed": false, "verdict": "paused", "ledger_health": "healthy"}, true, "capped"},
		{"unavailable", map[string]any{"allowed": true, "reason": "governor_unavailable", "ledger_health": "unavailable"}, false, ""},
		{"incomplete", map[string]any{"allowed": true, "verdict": "within", "ledger_health": "untrusted"}, false, ""},
		{"disabled", map[string]any{"allowed": true, "reason": "governor_disabled", "ledger_health": "no_evidence"}, true, "normal"},
	} {
		t.Run(test.name, func(t *testing.T) {
			signal := workloadLLMSpendSignal(test.value, now)
			if signal.Available != test.available || signal.State != test.state {
				t.Fatalf("wrong tenant allowance mapping: %#v", signal)
			}
		})
	}
}

func TestWorkloadUnknownReadinessCannotBecomeHealthy(t *testing.T) {
	sample := workloadServiceSample(systemProbeResult{Name: "media", Status: "healthy", ReadinessObserved: false}, time.Now())
	if sample.Available || sample.Healthy {
		t.Fatal("HTTP health without readiness is not capacity evidence")
	}
	for _, service := range []string{"aggregation", "enrichment", "media", "cms"} {
		if len(workloadIncidentResources(service)) == 0 {
			t.Fatalf("known incident service %s is unaccounted for", service)
		}
	}
	if len(workloadIncidentResources("unrelated")) != 0 {
		t.Fatal("unrelated incident should not saturate every resource")
	}
}

func TestWorkloadBeginOnlyStopsForAuthoritativeCapacityChange(t *testing.T) {
	for _, err := range []error{workload.ErrNotAdmitted, workload.ErrUnsafeRevoke, workload.ErrConflict} {
		if !workloadKnownStop(err) {
			t.Fatalf("known stop ignored: %v", err)
		}
	}
	if workloadKnownStop(errors.New("database unavailable")) {
		t.Fatal("optional Governor SQL failure must preserve native fail-open behavior")
	}
}

func TestWorkloadRequestRejectsExtraFieldsAndTrailingBodies(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, body := range []string{`{"revision":1,"arbitrary_origin":"human"}`, `{"revision":1} {"revision":2}`, strings.Repeat("x", 33<<10)} {
		response := httptest.NewRecorder()
		context, _ := gin.CreateTestContext(response)
		context.Request = httptest.NewRequest(http.MethodPut, "/admin/workload/policy", strings.NewReader(body))
		var value struct {
			Revision int64 `json:"revision"`
		}
		if decodeWorkloadBody(context, &value) || response.Code != 400 {
			t.Fatalf("unbounded/ambiguous request accepted: %d", response.Code)
		}
	}
}

func TestWorkloadHTTPFailureDoesNotExposeSQLOrCredentials(t *testing.T) {
	response := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(response)
	workloadFailure(context, errors.New("internal database details must remain private"))
	var value map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &value); err != nil {
		t.Fatal(err)
	}
	if response.Code != 503 || strings.Contains(response.Body.String(), "private") || value["code"] != "WORKLOAD_UNAVAILABLE" {
		t.Fatal("internal failure leaked or hid unavailability")
	}
}

func TestWorkloadNativeOverloadIsDeferralAndOtherResponsesArePreserved(t *testing.T) {
	for _, test := range []struct {
		body     string
		deferred bool
	}{
		{`{"error_code":"WORKLOAD_OVERLOADED","error":"temporarily full"}`, true},
		{`{"error_code":"PROVIDER_RATE_LIMIT"}`, false},
		{strings.Repeat("x", 9000), false},
	} {
		response := &http.Response{StatusCode: 429, Body: io.NopCloser(strings.NewReader(test.body))}
		if actual := modelRejectedForCapacity(response); actual != test.deferred {
			t.Fatalf("wrong overload classification: %v", actual)
		}
		if !test.deferred {
			body, err := io.ReadAll(response.Body)
			if err != nil || string(body) != test.body {
				t.Fatal("ordinary owner error response was changed")
			}
			_ = response.Body.Close()
		}
	}
}

func TestWorkloadSingleHumanAndBulkCampaignHaveDifferentOrigins(t *testing.T) {
	if enrichmentWorkloadContext(nil, "tenant", "admin_enrichment", "run").workloadOrigin != workload.OriginHuman {
		t.Fatal("single human action lost bypass provenance")
	}
	if enrichmentWorkloadContext(nil, "tenant", "enrichment_campaign", "run").workloadOrigin != workload.OriginCampaign {
		t.Fatal("bulk campaign bypassed governance")
	}
	if enrichmentNativeTranscriptionTrigger("enrichment_campaign") != models.TranscriptionTriggerManual {
		t.Fatal("capacity provenance altered authoritative STT admission")
	}
	for trigger, origin := range map[string]string{models.TranscriptionTriggerManual: workload.OriginHuman, models.TranscriptionTriggerBulkManual: workload.OriginCampaign, models.TranscriptionTriggerStudioAutopilot: workload.OriginAutomation} {
		if r := transcriptionWorkloadRequest("tenant", uuid.New(), trigger); r.Origin != origin || r.OwnerKind != legacySTTWorkloadOwner {
			t.Fatalf("wrong native transcription provenance: %+v", r)
		}
	}
	if r := transcriptionWorkloadRequest("tenant", uuid.New(), models.TranscriptionTriggerManual, workload.OriginCampaign); r.Origin != workload.OriginCampaign {
		t.Fatal("native STT financial trigger turned a campaign into a human bypass")
	}
}

func TestWorkloadCapacityDeferralSurvivesCampaignAdapterErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(429)
		_, _ = w.Write([]byte(`{"error_code":"WORKLOAD_OVERLOADED"}`))
	}))
	defer server.Close()
	t.Setenv("ENRICHMENT_BASE_URL", server.URL)
	t.Setenv("ENRICHMENT_SERVICE_TOKEN", "test-token")
	for _, test := range []struct {
		table   string
		columns []string
		values  []any
		call    func(*gorm.DB) error
	}{
		{"topics", []string{"label_en", "label_ar"}, []any{"Capacity", "السعة"}, func(db *gorm.DB) error { _, err := refreshTopicCentroid(db, "target", expectedSpace{}); return err }},
		{"topic_proposals", []string{"suggested_slug", "suggested_label_en"}, []any{"capacity", "Capacity"}, func(db *gorm.DB) error { return refreshTopicProposalVector(db, "1", expectedSpace{}) }},
		{"discovery_profiles", []string{"name", "description"}, []any{"Capacity", "bounded input"}, func(db *gorm.DB) error { return refreshDiscoveryProfileVector(db, "target", expectedSpace{}) }},
	} {
		t.Run(test.table, func(t *testing.T) {
			sqlDB, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer sqlDB.Close()
			db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{DisableAutomaticPing: true})
			if err != nil {
				t.Fatal(err)
			}
			rows := sqlmock.NewRows(test.columns)
			// database/sql driver values are deliberately narrow in this fixture.
			if test.table == "topics" {
				rows.AddRow("Capacity", "السعة")
			} else if test.table == "topic_proposals" {
				rows.AddRow("capacity", "Capacity")
			} else {
				rows.AddRow("Capacity", "bounded input")
			}
			mock.ExpectQuery(`SELECT .*FROM "` + test.table + `"`).WillReturnRows(rows)
			if err := test.call(db); !isWorkloadDeferred(err) {
				t.Fatalf("capacity was turned into a consumed native failure: %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWorkloadDeferredProposalCannotEarnApproval(t *testing.T) {
	score := preferenceCapacityDeferredScore(proposalScore{ProposalID: 9, Slug: "pending"})
	if !score.WorkloadDeferred || score.Verdict != models.PreferenceVerdictReview || !score.CategoryUnknown || !score.NeedsLabel || score.EmbeddingReady {
		t.Fatalf("capacity deferral became editorial evidence: %#v", score)
	}
}
