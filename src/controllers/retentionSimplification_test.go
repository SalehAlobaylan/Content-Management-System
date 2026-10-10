package controllers

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"content-management-system/src/models"
	"content-management-system/src/utils"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type retentionDenyHTTP struct{ calls int }

const retentionTestMiB = int64(1024 * 1024)

func (transport *retentionDenyHTTP) RoundTrip(*http.Request) (*http.Response, error) {
	transport.calls++
	return nil, errors.New("unexpected owner HTTP call")
}

// Unexpected SQL must fail even where a legacy best-effort write ignores its
// error. The mock admits only the operation's own maintenance records.
func retentionIsolatedDB(t *testing.T) (*gorm.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock := newMockGorm(t)
	db.Config.SkipDefaultTransaction = true
	var logs bytes.Buffer
	db.Config.Logger = logger.New(log.New(&logs, "", 0), logger.Config{LogLevel: logger.Error})
	transport, previous := &retentionDenyHTTP{}, http.DefaultTransport
	http.DefaultTransport = transport
	t.Cleanup(func() {
		http.DefaultTransport = previous
		if transport.calls != 0 || logs.Len() != 0 {
			t.Errorf("unexpected side effect: HTTP=%d SQL=%s", transport.calls, logs.String())
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})
	return db, mock
}

func retentionExpectPolicy(mock sqlmock.Sqlmock, modes ...string) {
	mode := models.RetentionModeAssist
	if len(modes) > 0 {
		mode = modes[0]
	}
	mock.ExpectQuery(`SELECT .*FROM "retention_policies"`).WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "mode", "policy_version", "max_rows_per_run", "max_bytes_per_run", "action_modes"}).AddRow("default", mode, 3, 500, 33554432, []byte(`{"news_database.refresh_snapshots":"safe_auto"}`)))
	mock.ExpectQuery(`SELECT .*FROM "news_circulation_policies"`).WillReturnRows(sqlmock.NewRows([]string{"timezone"}).AddRow("Asia/Riyadh"))
}

func retentionExpectPreparationRun(mock sqlmock.Sqlmock, id uuid.UUID, modes ...string) {
	retentionExpectPolicy(mock, modes...)
	mock.ExpectExec(`UPDATE "retention_runs"`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`INSERT INTO "retention_runs"`).WillReturnRows(sqlmock.NewRows([]string{"id", "public_id"}).AddRow(11, id))
	mock.ExpectQuery(`SELECT .*FROM "retention_months"`).WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "state"}).AddRow(12, "default", "open"))
	mock.ExpectQuery(`SELECT pg_database_size`).WillReturnRows(sqlmock.NewRows([]string{"pg_database_size"}).AddRow(100 * retentionTestMiB))
	mock.ExpectQuery(`FROM pg_stat_user_tables`).WillReturnRows(sqlmock.NewRows([]string{"table_name", "total_bytes"}))
	mock.ExpectQuery(`INSERT INTO "retention_db_samples"`).WillReturnRows(sqlmock.NewRows([]string{"id", "public_id"}).AddRow(13, uuid.New()))
	mock.ExpectQuery(`SELECT .*FROM "retention_db_samples"`).WillReturnRows(sqlmock.NewRows([]string{"database_bytes", "measured_at"}).AddRow(100*retentionTestMiB, time.Now().UTC()))
}

func retentionExpectUnprotected(mock sqlmock.Sqlmock) {
	for _, query := range []string{`FROM "user_interactions"`, `FROM "retention_holds"`, `JOIN retention_holds`, `FROM "moderation_reports"`, `FROM "content_flags"`} {
		mock.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"public_id"}))
	}
}

func retentionExpectNoDependencies(mock sqlmock.Sqlmock) {
	for _, table := range []string{"content_items", "transcripts", "media_atomization_runs", "media_storage_artifact_events", "redundancy_family_members", "redundancy_pairs"} {
		mock.ExpectQuery(`SELECT count\(\*\) FROM "` + table + `"`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	}
}

func retentionTestRequest(db *gorm.DB, handler gin.HandlerFunc, id uuid.UUID, bodies ...string) *httptest.ResponseRecorder {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	body := `{}`
	if len(bodies) > 0 {
		body = bodies[0]
	}
	c.Request = httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(body))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Set("db", db)
	c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: "default", Email: "reviewer@example.test"})
	if id != uuid.Nil {
		c.Params = gin.Params{{Key: "id", Value: id.String()}}
	}
	handler(c)
	return w
}

func TestRetentionPreparationCreatesOnlyItsExactWork(t *testing.T) {
	for _, historical := range []bool{false, true} {
		name := "compaction"
		if historical {
			name = "historical"
		}
		t.Run(name, func(t *testing.T) {
			db, mock := retentionIsolatedDB(t)
			runID, manifestID, actionID, storyID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
			var action models.RetentionAction
			created := map[string]int{}
			if err := db.Callback().Create().After("gorm:create").Register("test:own_records", func(tx *gorm.DB) {
				created[tx.Statement.Table]++
				if value, ok := tx.Statement.Dest.(*models.RetentionAction); ok {
					action = *value
				}
			}); err != nil {
				t.Fatal(err)
			}
			if historical {
				retentionExpectPolicy(mock)
			}
			retentionExpectPreparationRun(mock, runID)
			ids := []uuid.UUID{uuid.MustParse("11111111-1111-4111-8111-111111111111")}
			manifestTable := "retention_historical_manifests"
			handler := PrepareHistoricalRetention
			if historical {
				stamp := time.Now().UTC().AddDate(0, -1, 0)
				month := time.Date(stamp.Year(), stamp.Month(), 1, 0, 0, 0, 0, time.UTC)
				mock.ExpectQuery(`SELECT .*FROM "news_month_archives"`).WillReturnRows(sqlmock.NewRows([]string{"month_start", "state"}).AddRow(month, "finalized"))
				mock.ExpectQuery(`SELECT .*FROM "content_items"`).WillReturnRows(sqlmock.NewRows([]string{"id", "public_id", "tenant_id", "type", "status", "story_id", "published_at", "created_at"}).AddRow(1, ids[0], "default", "NEWS", "READY", storyID, stamp, stamp))
				retentionExpectUnprotected(mock)
				mock.ExpectQuery(`SELECT public_id, pg_column_size`).WillReturnRows(sqlmock.NewRows([]string{"public_id", "bytes"}).AddRow(ids[0], 200))
				mock.ExpectQuery(`SELECT COALESCE\(SUM\(pg_column_size`).WillReturnRows(sqlmock.NewRows([]string{"bytes"}).AddRow(200))
				retentionExpectNoDependencies(mock)
				mock.ExpectBegin()
			} else {
				manifestTable, handler = "retention_compaction_manifests", PrepareRetentionCompaction
				retentionExpectPolicy(mock)
				stamp := time.Now().UTC().AddDate(0, 0, -8)
				mock.ExpectQuery(`SELECT s.public_id AS story_id`).WillReturnRows(sqlmock.NewRows([]string{"story_id", "last_member_at", "ready_member_count"}).AddRow(uuid.New(), stamp.Add(-time.Hour), 4).AddRow(storyID, stamp, 5))
				mock.ExpectBegin()
				mock.ExpectQuery(`SELECT .*FROM "stories".*FOR UPDATE`).WillReturnRows(sqlmock.NewRows([]string{"id", "public_id", "tenant_id"}).AddRow(1, storyID, "default"))
				mock.ExpectQuery(`SELECT story_id, COUNT`).WillReturnRows(sqlmock.NewRows([]string{"story_id", "rows", "bytes"}).AddRow(storyID, 5, 1000))
				rows := sqlmock.NewRows([]string{"public_id", "tenant_id", "type", "status", "source_name", "like_count", "published_at", "story_id"})
				for i := 1; i <= 5; i++ {
					id := uuid.MustParse("11111111-1111-4111-8111-11111111111" + string(rune('0'+i)))
					rows.AddRow(id, "default", "NEWS", "READY", "source", 6-i, stamp.Add(-time.Duration(i)*time.Hour), storyID)
					if i == 5 {
						ids = []uuid.UUID{id}
					}
				}
				mock.ExpectQuery(`SELECT .*FROM "content_items"`).WillReturnRows(rows)
				retentionExpectUnprotected(mock)
				retentionExpectNoDependencies(mock)
				mock.ExpectQuery(`SELECT COALESCE\(SUM\(pg_column_size`).WillReturnRows(sqlmock.NewRows([]string{"bytes"}).AddRow(200))
			}
			mock.ExpectQuery(`INSERT INTO "` + manifestTable + `"`).WillReturnRows(sqlmock.NewRows([]string{"id", "public_id"}).AddRow(21, manifestID))
			mock.ExpectQuery(`INSERT INTO "retention_actions"`).WillReturnRows(sqlmock.NewRows([]string{"id", "public_id"}).AddRow(31, actionID))
			mock.ExpectExec(`UPDATE "`+manifestTable+`"`).WithArgs(31, sqlmock.AnyArg(), 21).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectCommit()
			mock.ExpectExec(`UPDATE "retention_runs"`).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectQuery(`INSERT INTO "audit_logs"`).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(41))
			response := retentionTestRequest(db, handler, uuid.Nil)
			if response.Code != http.StatusCreated {
				t.Fatalf("preparation: %d %s", response.Code, response.Body)
			}
			var envelope struct {
				Data json.RawMessage `json:"data"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
				t.Fatal(err)
			}
			var links struct {
				RunID    uuid.UUID `json:"run_id"`
				ActionID uuid.UUID `json:"action_id"`
			}
			if err := json.Unmarshal(envelope.Data, &links); err != nil {
				t.Fatal(err)
			}
			if links.RunID != runID || links.ActionID != actionID || action.RunID != 11 || action.Outcome != models.RetentionActionApprovalRequired || action.TargetCount != 1 {
				t.Fatalf("preparation did not link its own approval-bound work: %s", envelope.Data)
			}
			if historical {
				var manifest models.RetentionHistoricalManifest
				_ = json.Unmarshal(envelope.Data, &manifest)
				if err := validateHistoricalManifest(manifest); err != nil || string(manifest.ContentIDs) != string(idsJSON(ids)) {
					t.Fatalf("unexpected historical selection: %s, %v", envelope.Data, err)
				}
				if action.ActionClass != models.RetentionActionPrepareHistoricalPurge {
					t.Fatal("unexpected action class")
				}
			} else {
				var manifest models.RetentionCompactionManifest
				_ = json.Unmarshal(envelope.Data, &manifest)
				if _, err := decodeRetentionManifest(manifest); err != nil || string(manifest.RetireContentIDs) != string(idsJSON(ids)) || manifest.AnchorCount != 4 {
					t.Fatalf("unexpected compact selection: %s, %v", envelope.Data, err)
				}
				if action.ActionClass != retentionActionPrepareNewsCompaction {
					t.Fatal("unexpected action class")
				}
			}
			for _, table := range []string{"retention_runs", "retention_db_samples", manifestTable, "retention_actions", "audit_logs"} {
				if created[table] != 1 {
					t.Fatalf("%s: created %d records", table, created[table])
				}
				delete(created, table)
			}
			if len(created) != 0 {
				t.Fatalf("unrelated records created: %v", created)
			}
		})
	}
}

func TestRetentionHistoricalPreparationSavesArchiveBlockReason(t *testing.T) {
	db, mock := retentionIsolatedDB(t)
	retentionExpectPolicy(mock)
	retentionExpectPreparationRun(mock, uuid.New())
	mock.ExpectQuery(`SELECT .*FROM "news_month_archives"`).WillReturnRows(sqlmock.NewRows([]string{"month_start", "state"}))
	var saved map[string]interface{}
	if err := db.Callback().Update().Before("gorm:update").Register("test:saved_failure", func(tx *gorm.DB) {
		if tx.Statement.Table == "retention_runs" {
			if values, ok := tx.Statement.Dest.(map[string]interface{}); ok && values["error_class"] == "run_failed" {
				saved = values
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	mock.ExpectExec(`UPDATE "retention_runs"`).WillReturnResult(sqlmock.NewResult(0, 1))
	response := retentionTestRequest(db, PrepareHistoricalRetention, uuid.Nil)
	if response.Code != http.StatusConflict || saved["error"] != "no archive-gated, unprotected historical News is eligible" || !strings.Contains(string(saved["after_evidence"].(datatypes.JSON)), "Historical preparation") {
		t.Fatalf("archive block lost its saved reason: HTTP=%d saved=%v", response.Code, saved)
	}
}

func TestRetentionHistoricalSignalUsesLocalCalendarLabel(t *testing.T) {
	db, mock := retentionIsolatedDB(t)
	mock.ExpectQuery(`SELECT count\(\*\) FROM "content_items"`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(10))
	mock.ExpectQuery(`SELECT count\(\*\) FROM "news_month_archives"`).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	verdict, evidence := retentionHistoricalSignal(db, "default", "Asia/Riyadh", models.DefaultRetentionPolicy("default"))
	location, _ := time.LoadLocation("Asia/Riyadh")
	if verdict != models.RetentionVerdictArchiveBlocked || evidence["month_start"] != time.Now().In(location).Format("2006-01")+"-01" {
		t.Fatalf("historical month label is not the configured local calendar: %v", evidence)
	}
}

func TestRetentionLegacyObservationHasNoEffects(t *testing.T) {
	for _, trigger := range []string{"manual", "scheduled"} {
		t.Run(trigger, func(t *testing.T) {
			db, mock := retentionIsolatedDB(t)
			var created []string
			if err := db.Callback().Create().After("gorm:create").Register("test:observation_records", func(tx *gorm.DB) {
				created = append(created, tx.Statement.Table)
			}); err != nil {
				t.Fatal(err)
			}
			if err := db.Callback().Update().Before("gorm:update").Register("test:observation_policy", func(tx *gorm.DB) {
				if tx.Statement.Table == "retention_policies" {
					values := tx.Statement.Dest.(map[string]interface{})
					if len(values) != 1 || values["last_run_at"] == nil {
						t.Fatal("observation rewrote saved policy authority")
					}
				}
			}); err != nil {
				t.Fatal(err)
			}
			retentionExpectPreparationRun(mock, uuid.New(), models.RetentionModeSafeAuto)
			mock.ExpectQuery(`SELECT .*FROM "news_circulation_policies"`).WillReturnRows(sqlmock.NewRows([]string{"timezone"}).AddRow("Asia/Riyadh"))
			mock.ExpectQuery(`WITH eligible AS`).WillReturnRows(sqlmock.NewRows([]string{"eligible_stories", "candidate_rows", "estimated_bytes"}).AddRow(100, 500, 10000))
			mock.ExpectExec(`UPDATE "retention_runs"`).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec(`UPDATE "retention_policies"`).WillReturnResult(sqlmock.NewResult(0, 1))
			run, err := runRetention(db, "default", trigger, "reviewer@example.test")
			if err != nil || run.Status != models.RetentionRunCompleted || run.Mode != models.RetentionModeObserve || run.Trigger != trigger || !bytes.Contains(run.Counts, []byte(`"candidate_rows":500`)) {
				t.Fatalf("observation: %+v, %v", run, err)
			}
			if strings.Join(created, ",") != "retention_runs,retention_db_samples" {
				t.Fatalf("observation created unrelated work: %v", created)
			}
		})
	}
}

func TestRetentionRetiredRoutePreservesAuthentication(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		tenant string
		status int
	}{{"", 401}, {"foreign", 409}, {"default", 410}} {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		if tc.tenant != "" {
			c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: tc.tenant})
		}
		RetentionCapabilityRetired(c)
		if w.Code != tc.status {
			t.Fatalf("tenant %q: %d", tc.tenant, w.Code)
		}
	}
}

func TestRetentionReviewRejectsMalformedIdentityBeforeDatabase(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: "default"})
	c.Params = gin.Params{{Key: "id", Value: "invalid"}}
	GetRetentionActionReview(c)
	if w.Code != 400 {
		t.Fatalf("malformed ID: %d", w.Code)
	}
}

func TestRetentionHistoricalVerificationRequiresOriginalEffect(t *testing.T) {
	ids := []uuid.UUID{uuid.MustParse("11111111-1111-4111-8111-111111111111")}
	stories := []uuid.UUID{uuid.MustParse("22222222-2222-4222-8222-222222222222")}
	now := time.Now().UTC()
	actionID := uint(7)
	hash := historicalManifestHash("default", "Asia/Riyadh", ids, stories)
	action := models.RetentionAction{ID: actionID, RunID: 8, TenantID: "default", ActionClass: models.RetentionActionExecuteHistoricalPurge, Outcome: models.RetentionActionVerifyFailed, ManifestHash: &hash, RecoveryRef: "original-key"}
	manifest := models.RetentionHistoricalManifest{ID: 9, ActionID: &actionID, RunID: 8, TenantID: "default", Timezone: "Asia/Riyadh", ManifestHash: hash, State: "executed", ContentIDs: idsJSON(ids), StoryIDs: idsJSON(stories), ContentCount: 1, StoryCount: 1, ExpiresAt: now.Add(-time.Hour)}
	artifact := models.RetentionHistoricalRecoveryArtifact{ActionID: actionID, ManifestID: 9, TenantID: "default", ArtifactKey: "original-key", SHA256: strings.Repeat("a", 64), VerifiedAt: &now, State: "deleted", DeletedAt: &now, ExpiresAt: now.Add(-time.Hour)}
	if err := historicalVerificationProof(action, manifest, artifact); err != nil {
		t.Fatalf("expired original proof must remain usable: %v", err)
	}
	for _, mutate := range []func(*models.RetentionAction, *models.RetentionHistoricalManifest, *models.RetentionHistoricalRecoveryArtifact){
		func(a *models.RetentionAction, m *models.RetentionHistoricalManifest, r *models.RetentionHistoricalRecoveryArtifact) {
			a.Outcome = models.RetentionActionApproved
		},
		func(a *models.RetentionAction, m *models.RetentionHistoricalManifest, r *models.RetentionHistoricalRecoveryArtifact) {
			m.TenantID = "foreign"
		},
		func(a *models.RetentionAction, m *models.RetentionHistoricalManifest, r *models.RetentionHistoricalRecoveryArtifact) {
			m.State = "approved"
		},
		func(a *models.RetentionAction, m *models.RetentionHistoricalManifest, r *models.RetentionHistoricalRecoveryArtifact) {
			m.ContentCount = 2
		},
		func(a *models.RetentionAction, m *models.RetentionHistoricalManifest, r *models.RetentionHistoricalRecoveryArtifact) {
			m.ContentIDs = idsJSON(stories)
		},
		func(a *models.RetentionAction, m *models.RetentionHistoricalManifest, r *models.RetentionHistoricalRecoveryArtifact) {
			r.VerifiedAt = nil
		},
		func(a *models.RetentionAction, m *models.RetentionHistoricalManifest, r *models.RetentionHistoricalRecoveryArtifact) {
			r.ActionID++
		},
		func(a *models.RetentionAction, m *models.RetentionHistoricalManifest, r *models.RetentionHistoricalRecoveryArtifact) {
			r.SHA256 = "bad"
		},
		func(a *models.RetentionAction, m *models.RetentionHistoricalManifest, r *models.RetentionHistoricalRecoveryArtifact) {
			a.RecoveryRef = "replacement-key"
		},
	} {
		a, m, r := action, manifest, artifact
		mutate(&a, &m, &r)
		if historicalVerificationProof(a, m, r) == nil {
			t.Fatal("uncertain or foreign proof accepted")
		}
	}
}

func TestRetentionExactReviewIsTenantScopedAndReadOnly(t *testing.T) {
	for _, present := range []bool{false, true} {
		t.Run(fmtBool(present), func(t *testing.T) {
			db, mock := newMockGorm(t)
			actionID, runID := uuid.New(), uuid.New()
			internalActionID := uint(7)
			actionRows := sqlmock.NewRows([]string{"id", "public_id", "tenant_id", "run_id", "action_class", "manifest_hash"})
			ids, stories := []uuid.UUID{uuid.New()}, []uuid.UUID{uuid.New()}
			hash := historicalManifestHash("default", "Asia/Riyadh", ids, stories)
			if present {
				actionRows.AddRow(internalActionID, actionID, "default", 8, models.RetentionActionPrepareHistoricalPurge, hash)
			}
			mock.ExpectQuery(`SELECT .*FROM "retention_actions"`).WithArgs("default", actionID, 1).WillReturnRows(actionRows)
			if present {
				mock.ExpectQuery(`SELECT .*FROM "retention_runs"`).WithArgs("default", 8, 1).WillReturnRows(sqlmock.NewRows([]string{"id", "public_id", "tenant_id"}).AddRow(8, runID, "default"))
				mock.ExpectQuery(`SELECT .*FROM "retention_historical_manifests"`).WithArgs("default", 7, 1).WillReturnRows(sqlmock.NewRows([]string{"id", "public_id", "tenant_id", "run_id", "action_id", "timezone", "manifest_hash", "content_ids", "story_ids", "content_count", "story_count"}).AddRow(9, uuid.New(), "default", 8, 7, "Asia/Riyadh", hash, []byte(idsJSON(ids)), []byte(idsJSON(stories)), 1, 1))
			}
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Set("db", db)
			c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: "default"})
			c.Params = gin.Params{{Key: "id", Value: actionID.String()}}
			GetRetentionActionReview(c)
			want := 404
			if present {
				want = 200
				if !bytes.Contains(w.Body.Bytes(), []byte(ids[0].String())) {
					t.Fatal("exact frozen IDs missing")
				}
			}
			if w.Code != want {
				t.Fatalf("review: %d %s", w.Code, w.Body)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func fmtBool(value bool) string {
	if value {
		return "saved_manifest"
	}
	return "foreign_or_missing"
}

func TestRetentionRetiredApprovalAndExecutionHaveNoWrites(t *testing.T) {
	for _, class := range []string{models.RetentionActionRefreshNewsSnapshots, models.RetentionActionRequestStorageRun, models.RetentionActionRequestMediaRun} {
		for index, handler := range []gin.HandlerFunc{ApproveRetentionAction, ExecuteRetentionCompaction} {
			db, mock := newMockGorm(t)
			id := uuid.New()
			if index == 1 {
				mock.ExpectQuery(`SELECT .*FROM "retention_policies"`).WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "mode"}).AddRow("default", "safe_auto"))
			}
			mock.ExpectQuery(`SELECT .*FROM "retention_actions"`).WithArgs("default", id, 1).WillReturnRows(sqlmock.NewRows([]string{"id", "public_id", "tenant_id", "action_class", "outcome"}).AddRow(7, id, "default", class, models.RetentionActionApproved))
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest(http.MethodPost, "/", nil)
			c.Set("db", db)
			c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: "default"})
			c.Params = gin.Params{{Key: "id", Value: id.String()}}
			handler(c)
			if w.Code != 410 {
				t.Fatalf("retired %s: %d %s", class, w.Code, w.Body)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestRetentionHistoricalContinuationCannotReplayMutation(t *testing.T) {
	for _, verified := range []bool{false, true} {
		t.Run(fmtBool(verified), func(t *testing.T) {
			db, mock := newMockGorm(t)
			id, now := uuid.New(), time.Now().UTC()
			ids, stories := []uuid.UUID{uuid.New()}, []uuid.UUID{uuid.New()}
			hash := historicalManifestHash("default", "Asia/Riyadh", ids, stories)
			outcome := models.RetentionActionVerifyFailed
			var verifiedAt interface{}
			if verified {
				outcome = models.RetentionActionVerified
				verifiedAt = now
			}
			mock.ExpectQuery(`SELECT .*FROM "retention_actions"`).WithArgs("default", id, 1).WillReturnRows(sqlmock.NewRows([]string{"id", "public_id", "tenant_id", "run_id", "action_class", "outcome", "manifest_hash", "recovery_ref"}).AddRow(7, id, "default", 8, models.RetentionActionExecuteHistoricalPurge, outcome, hash, "original-key"))
			mock.ExpectQuery(`SELECT .*FROM "retention_execution_controls"`).WithArgs("default", 1).WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "historical_enabled"}).AddRow("default", true))
			mock.ExpectQuery(`SELECT .*FROM "retention_historical_manifests"`).WithArgs("default", 7, 1).WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "run_id", "action_id", "state", "timezone", "manifest_hash", "content_ids", "story_ids", "content_count", "story_count", "expires_at"}).AddRow(9, "default", 8, 7, "executed", "Asia/Riyadh", hash, []byte(idsJSON(ids)), []byte(idsJSON(stories)), 1, 1, now.Add(-time.Hour)))
			mock.ExpectQuery(`SELECT .*FROM "retention_historical_recovery_artifacts"`).WithArgs("default", 7, 1).WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "action_id", "manifest_id", "artifact_key", "sha256", "verified_at", "state", "expires_at"}).AddRow("default", 7, 9, "original-key", strings.Repeat("a", 64), verifiedAt, "deleted", now.Add(-time.Hour)))
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Set("db", db)
			c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: "default"})
			c.Params = gin.Params{{Key: "id", Value: id.String()}}
			ExecuteHistoricalRetention(c)
			want := 409
			if verified {
				want = 200
			} else if !bytes.Contains(w.Body.Bytes(), []byte("manual reconciliation required")) {
				t.Fatal("missing proof must require reconciliation")
			}
			if w.Code != want {
				t.Fatalf("continuation: %d %s", w.Code, w.Body)
			}
			// Only original-proof reads are expected: no policy, insert, delete,
			// recovery creation, candidate selection or approval transaction.
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRetentionHistoricalVerificationRetryCompletesOriginalAction(t *testing.T) {
	db, mock := retentionIsolatedDB(t)
	previous := historicalReadbacks
	t.Cleanup(func() { historicalReadbacks = previous })
	id, integrityID, now := uuid.New(), uuid.New(), time.Now().UTC()
	ids, stories := []uuid.UUID{uuid.New()}, []uuid.UUID{uuid.New()}
	hash := historicalManifestHash("default", "Asia/Riyadh", ids, stories)
	windows := []string{}
	failSnapshot, integrityCalls, healthCalls, sampleCalls := true, 0, 0, 0
	historicalReadbacks.snapshot = func(ownerDB *gorm.DB, tenant, window string) (int, error) {
		if ownerDB != db || tenant != "default" {
			t.Fatal("snapshot owner used a foreign database or tenant")
		}
		windows = append(windows, window)
		if failSnapshot {
			return 0, errors.New("snapshot temporarily unavailable")
		}
		return 0, nil
	}
	historicalReadbacks.integrity = func(ownerDB *gorm.DB, tenant string, opts feedIntegrityRunOptions) (models.FeedIntegrityRun, error) {
		integrityCalls++
		if ownerDB != db || tenant != "default" || opts.Tier != models.FeedIntegrityTierDeep || opts.Trigger != "retention_historical" || opts.CreatedBy != "reviewer@example.test" {
			t.Fatal("incorrect native integrity request")
		}
		return models.FeedIntegrityRun{PublicID: integrityID, Status: models.FeedIntegrityRunCompleted, Headline: "all_clear"}, nil
	}
	historicalReadbacks.health = func(ownerDB *gorm.DB) (systemHealthSnapshot, []systemAnomaly) {
		healthCalls++
		if ownerDB != db {
			t.Fatal("health readback used another database")
		}
		return systemHealthSnapshot{Overall: "healthy"}, nil
	}
	historicalReadbacks.sample = func(ownerDB *gorm.DB, tenant string) (models.RetentionDBSample, error) {
		sampleCalls++
		if ownerDB != db || tenant != "default" {
			t.Fatal("measurement used another database or tenant")
		}
		return models.RetentionDBSample{DatabaseBytes: 80 * retentionTestMiB}, nil
	}
	var receipts []map[string]interface{}
	if err := db.Callback().Update().Before("gorm:update").Register("test:verification_receipts", func(tx *gorm.DB) {
		if tx.Statement.Table == "retention_actions" {
			receipts = append(receipts, tx.Statement.Dest.(map[string]interface{}))
		}
	}); err != nil {
		t.Fatal(err)
	}
	expectOriginalProof := func() {
		mock.ExpectQuery(`SELECT .*FROM "retention_actions"`).WithArgs("default", id, 1).WillReturnRows(sqlmock.NewRows([]string{"id", "public_id", "tenant_id", "run_id", "action_class", "outcome", "manifest_hash", "recovery_ref"}).AddRow(7, id, "default", 8, models.RetentionActionExecuteHistoricalPurge, models.RetentionActionVerifyFailed, hash, "original-key"))
		mock.ExpectQuery(`SELECT .*FROM "retention_execution_controls"`).WithArgs("default", 1).WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "historical_enabled"}).AddRow("default", true))
		mock.ExpectQuery(`SELECT .*FROM "retention_historical_manifests"`).WithArgs("default", 7, 1).WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "run_id", "action_id", "state", "timezone", "manifest_hash", "content_ids", "story_ids", "content_count", "story_count", "expires_at"}).AddRow(9, "default", 8, 7, "executed", "Asia/Riyadh", hash, []byte(idsJSON(ids)), []byte(idsJSON(stories)), 1, 1, now.Add(-time.Hour)))
		mock.ExpectQuery(`SELECT .*FROM "retention_historical_recovery_artifacts"`).WithArgs("default", 7, 1).WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "action_id", "manifest_id", "artifact_key", "sha256", "verified_at", "state", "expires_at"}).AddRow("default", 7, 9, "original-key", strings.Repeat("a", 64), now.Add(-2*time.Hour), "deleted", now.Add(-time.Hour)))
	}
	expectOriginalProof()
	mock.ExpectExec(`UPDATE "retention_actions"`).WillReturnResult(sqlmock.NewResult(0, 1))
	failed := retentionTestRequest(db, ExecuteHistoricalRetention, id)
	if failed.Code != http.StatusConflict || len(receipts) != 1 || receipts[0]["outcome"] != models.RetentionActionVerifyFailed {
		t.Fatalf("failed readback: %d %s", failed.Code, failed.Body)
	}
	if integrityCalls+healthCalls+sampleCalls != 0 {
		t.Fatal("failed snapshot must stop subsequent verification")
	}
	// Retry reloads the original expired manifest and deleted artifact's durable
	// verified receipt. No policy, approval, preparation, batch, artifact write,
	// canonical DELETE or mutation transaction is admitted by this mock.
	failSnapshot = false
	windows = nil
	expectOriginalProof()
	mock.ExpectQuery(`SELECT count\(\*\) FROM "content_items"`).WithArgs("default", ids[0]).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec(`VACUUM \(ANALYZE\) content_items`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`UPDATE "retention_actions"`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT .*FROM "retention_actions"`).WithArgs("default", 7, 7, 1).WillReturnRows(sqlmock.NewRows([]string{"id", "public_id", "tenant_id", "run_id", "action_class", "outcome", "manifest_hash", "recovery_ref"}).AddRow(7, id, "default", 8, models.RetentionActionExecuteHistoricalPurge, models.RetentionActionVerified, hash, "original-key"))
	mock.ExpectQuery(`INSERT INTO "audit_logs"`).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(41))
	verified := retentionTestRequest(db, ExecuteHistoricalRetention, id)
	if verified.Code != http.StatusOK || !bytes.Contains(verified.Body.Bytes(), []byte(id.String())) || !bytes.Contains(verified.Body.Bytes(), []byte(models.RetentionActionVerified)) {
		t.Fatalf("retry: %d %s", verified.Code, verified.Body)
	}
	if strings.Join(windows, ",") != strings.Join([]string{models.NewsWindowToday, models.NewsWindowWeek, models.NewsWindowMonth}, ",") || integrityCalls != 1 || healthCalls != 1 || sampleCalls != 1 || len(receipts) != 2 {
		t.Fatalf("incomplete readbacks: windows=%v integrity=%d health=%d samples=%d receipts=%d", windows, integrityCalls, healthCalls, sampleCalls, len(receipts))
	}
	receipt := receipts[1]
	var evidence map[string]interface{}
	raw, ok := receipt["verification"].(interface{ MarshalJSON() ([]byte, error) })
	if !ok {
		t.Fatal("verification receipt is not JSON")
	}
	encoded, err := raw.MarshalJSON()
	if err != nil || json.Unmarshal(encoded, &evidence) != nil || receipt["outcome"] != models.RetentionActionVerified || receipt["after_bytes"] != int64(80*retentionTestMiB) || evidence["manifest_hash"] != hash || evidence["recovery_artifact"] != "original-key" || evidence["feed_integrity_run_id"] != integrityID.String() {
		t.Fatalf("original effect proof not preserved: %v", receipt)
	}
}

func TestRetentionExactCompactionContinuationUsesOnlySavedSelection(t *testing.T) {
	db, mock := retentionIsolatedDB(t)
	previous := compactionReadback
	t.Cleanup(func() { compactionReadback = previous })
	id, storyID, leadID, retireID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	payload := retentionManifestPayload{TenantID: "default", PolicyVersion: 3, Timezone: "Asia/Riyadh", Stories: []retentionManifestStory{{StoryID: storyID, LeadID: leadID, RetireIDs: []uuid.UUID{retireID}}}}
	raw := retentionActionEvidence(payload)
	hash := sha256Hex(string(raw))
	readbacks := 0
	compactionReadback = func(ownerDB *gorm.DB, tenant string, manifest models.RetentionCompactionManifest, saved retentionManifestPayload) (datatypes.JSON, error) {
		readbacks++
		_, anchors, _, retirees := compactManifestIDs(saved)
		if ownerDB != db || tenant != "default" || manifest.ManifestHash != hash || manifest.State != "executed" || manifest.ExpiresAt.After(time.Now()) || len(anchors) != 1 || anchors[0] != leadID || len(retirees) != 1 || retirees[0] != retireID {
			t.Fatal("verification did not use the original expired manifest")
		}
		return retentionActionEvidence(map[string]interface{}{"manifest_hash": hash, "retire_count": 1}), nil
	}
	// Observe/disabled policy cannot prevent readback of an already committed
	// effect. The independent canonical execution control remains required.
	mock.ExpectQuery(`SELECT .*FROM "retention_policies"`).WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "mode", "enabled"}).AddRow("default", "observe", false))
	mock.ExpectQuery(`SELECT .*FROM "retention_actions"`).WithArgs("default", id, 1).WillReturnRows(sqlmock.NewRows([]string{"id", "public_id", "tenant_id", "run_id", "action_class", "outcome", "manifest_hash"}).AddRow(7, id, "default", 8, retentionActionExecuteNewsCompaction, models.RetentionActionVerifyFailed, hash))
	mock.ExpectQuery(`SELECT .*FROM "retention_execution_controls"`).WithArgs("default", 1).WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "canonical_compaction_enabled"}).AddRow("default", true))
	mock.ExpectQuery(`SELECT .*FROM "retention_compaction_manifests"`).WithArgs("default", 7, 1).WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "run_id", "action_id", "state", "timezone", "policy_version", "manifest_hash", "evidence", "expires_at"}).AddRow(9, "default", 8, 7, "executed", "Asia/Riyadh", 3, hash, []byte(raw), time.Now().Add(-time.Hour)))
	mock.ExpectExec(`UPDATE "retention_actions"`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE "retention_compaction_batches"`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`VACUUM \(ANALYZE\) content_items`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT pg_database_size`).WillReturnRows(sqlmock.NewRows([]string{"pg_database_size"}).AddRow(80 * retentionTestMiB))
	mock.ExpectQuery(`FROM pg_stat_user_tables`).WillReturnRows(sqlmock.NewRows([]string{"table_name", "total_bytes"}))
	mock.ExpectQuery(`INSERT INTO "retention_db_samples"`).WillReturnRows(sqlmock.NewRows([]string{"id", "public_id"}).AddRow(13, uuid.New()))
	mock.ExpectExec(`UPDATE "retention_actions"`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE "retention_compaction_batches"`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT .*FROM "retention_actions"`).WithArgs(7, 7, 1).WillReturnRows(sqlmock.NewRows([]string{"id", "public_id", "tenant_id", "run_id", "action_class", "outcome", "manifest_hash"}).AddRow(7, id, "default", 8, retentionActionExecuteNewsCompaction, models.RetentionActionVerified, hash))
	mock.ExpectQuery(`INSERT INTO "audit_logs"`).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(41))
	response := retentionTestRequest(db, ExecuteRetentionCompaction, id)
	if response.Code != http.StatusOK || readbacks != 1 || !bytes.Contains(response.Body.Bytes(), []byte(id.String())) || !bytes.Contains(response.Body.Bytes(), []byte(models.RetentionActionVerified)) {
		t.Fatalf("compaction continuation: %d %s", response.Code, response.Body)
	}
	// No candidate queries, canonical writes, recovery creation or new batches
	// are admitted; only readback, measurement and original receipt updates.
}
