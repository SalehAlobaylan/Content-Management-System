package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"content-management-system/src/models"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type experienceStatementCounter struct{ statements atomic.Int64 }

func (*experienceStatementCounter) LogMode(logger.LogLevel) logger.Interface {
	return &experienceStatementCounter{}
}
func (*experienceStatementCounter) Info(context.Context, string, ...interface{})  {}
func (*experienceStatementCounter) Warn(context.Context, string, ...interface{})  {}
func (*experienceStatementCounter) Error(context.Context, string, ...interface{}) {}
func (counter *experienceStatementCounter) Trace(_ context.Context, _ time.Time, _ func() (string, int64), _ error) {
	counter.statements.Add(1)
}

func batchTerminal(tenant string) models.ExperienceEvent {
	event := terminalFixture("startup", "success")
	event.TenantID = tenant
	event.OccurredAt = time.Now().UTC()
	event.ReceivedAt = event.OccurredAt
	event.Measurements = jsonBytes(map[string]interface{}{"duration_ms": *event.DurationMS, "visible": true})
	return event
}
func TestExperienceBatchAdmission_ExactDispositionsAndBoundedStatements(t *testing.T) {
	db := experienceTestDB(t)
	tenant := "rux-batch-" + uuid.NewString()
	first := batchTerminal(tenant)
	retry := first
	retry.EventID = uuid.New()
	conflicting := first
	conflicting.EventID = uuid.New()
	conflicting.Outcome = "failed"
	rows := []models.ExperienceEvent{first, retry, conflicting, first}
	counter := &experienceStatementCounter{}
	measured := db.Session(&gorm.Session{Logger: counter})
	dispositions, err := persistExperienceEvents(measured, rows)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(dispositions, ",") != "accepted,duplicate,conflict,duplicate" {
		t.Fatal(dispositions)
	}
	if got := counter.statements.Load(); got != 4 {
		t.Fatalf("bounded batch wanted four SQL statements, got%d", got)
	}
	var events, receipts, eligible int64
	if err := db.Model(&models.ExperienceEvent{}).Where("tenant_id=?", tenant).Count(&events).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&models.ExperienceTerminalReceipt{}).Count(&receipts).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&models.ExperienceEvent{}).Where("tenant_id=? AND terminal_status='accepted'", tenant).Count(&eligible).Error; err != nil {
		t.Fatal(err)
	}
	if events != 3 || receipts != 1 || eligible != 1 {
		t.Fatalf("events%d receipts%d eligible%d", events, receipts, eligible)
	}
}
func TestExperienceBatchAdmission_ConcurrentSemanticRetries(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(fmt.Sprint(conflict), func(t *testing.T) {
			db := experienceTestDB(t)
			tenant := "rux-batch-" + uuid.NewString()
			first := batchTerminal(tenant)
			second := first
			second.EventID = uuid.New()
			if conflict {
				second.Outcome = "failed"
			}
			var wait sync.WaitGroup
			results := make(chan string, 2)
			failures := make(chan error, 2)
			for _, event := range []models.ExperienceEvent{first, second} {
				wait.Add(1)
				go func(event models.ExperienceEvent) {
					defer wait.Done()
					dispositions, err := persistExperienceEvents(db, []models.ExperienceEvent{event})
					if err != nil {
						failures <- err
						return
					}
					results <- dispositions[0]
				}(event)
			}
			wait.Wait()
			close(failures)
			close(results)
			for err := range failures {
				t.Fatal(err)
			}
			counts := map[string]int{}
			for disposition := range results {
				counts[disposition]++
			}
			expectedOther := "duplicate"
			if conflict {
				expectedOther = "conflict"
			}
			if counts["accepted"] != 1 || counts[expectedOther] != 1 {
				t.Fatalf("concurrent terminal dispositions %+v", counts)
			}
			var receipts int64
			if err := db.Model(&models.ExperienceTerminalReceipt{}).Count(&receipts).Error; err != nil || receipts != 1 {
				t.Fatalf("receipts%d error%v", receipts, err)
			}
		})
	}
}
func TestExperienceBatchAdmission_TransportRetryCannotReserveAnotherIdentity(t *testing.T) {
	db := experienceTestDB(t)
	first := batchTerminal("rux-" + uuid.NewString())
	if _, err := persistExperienceEvents(db, []models.ExperienceEvent{first}); err != nil {
		t.Fatal(err)
	}
	changed := first
	journey := uuid.New()
	changed.JourneyID = &journey
	changed.Outcome = "failed"
	dispositions, err := persistExperienceEvents(db, []models.ExperienceEvent{changed})
	if err != nil || dispositions[0] != "duplicate" {
		t.Fatalf("retry%v error%v", dispositions, err)
	}
	var receipts int64
	if err := db.Model(&models.ExperienceTerminalReceipt{}).Count(&receipts).Error; err != nil || receipts != 1 {
		t.Fatalf("orphan receipt count%d %v", receipts, err)
	}
}
func TestExperienceBatchAdmission_RollsBackWholeBatch(t *testing.T) {
	db := experienceTestDB(t)
	tenant := "rux-" + uuid.NewString()
	rows := []models.ExperienceEvent{batchTerminal(tenant), batchTerminal(tenant)}
	callback := "rux-batch-failure-" + uuid.NewString()
	if err := db.Callback().Raw().Before("gorm:raw").Register(callback, func(tx *gorm.DB) {
		if strings.HasPrefix(tx.Statement.SQL.String(), "UPDATE experience_events") {
			tx.AddError(errors.New("injected terminal disposition failure"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	_, err := persistExperienceEvents(db, rows)
	db.Callback().Raw().Remove(callback)
	if err == nil {
		t.Fatal("failure injection was not reached")
	}
	var count int64
	for _, model := range []interface{}{&models.ExperienceEvent{}, &models.ExperienceTerminalReceipt{}} {
		if err := db.Model(model).Count(&count).Error; err != nil || count != 0 {
			t.Fatalf("batch leaked %d records: %v", count, err)
		}
	}
	dispositions, err := persistExperienceEvents(db, rows)
	if err != nil || strings.Join(dispositions, ",") != "accepted,accepted" {
		t.Fatalf("replay %+v %v", dispositions, err)
	}
}
func TestExperienceBatchAdmission_FiftyEventsUseConstantRoundTrips(t *testing.T) {
	db := experienceTestDB(t)
	counter := &experienceStatementCounter{}
	rows := make([]models.ExperienceEvent, 50)
	tenant := "rux-" + uuid.NewString()
	for index := range rows {
		rows[index] = batchTerminal(tenant)
	}
	dispositions, err := persistExperienceEvents(db.Session(&gorm.Session{Logger: counter}), rows)
	if err != nil {
		t.Fatal(err)
	}
	for _, disposition := range dispositions {
		if disposition != "accepted" {
			t.Fatal(dispositions)
		}
	}
	if counter.statements.Load() != 4 {
		t.Fatalf("50 events incurred%d SQL statements", counter.statements.Load())
	}
}

func rawExperienceTerminal(t *testing.T, session string) json.RawMessage {
	t.Helper()
	event := batchTerminal("default")
	event.SessionID = session
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}
func ingestExperienceFixture(t *testing.T, db *gorm.DB, events []json.RawMessage) *httptest.ResponseRecorder {
	t.Helper()
	router := gin.New()
	router.Use(func(c *gin.Context) { c.Set("db", db); c.Next() })
	router.POST("/events", IngestExperienceEvents)
	payload, err := json.Marshal(incomingRuxBatch{SchemaVersion: 2, Events: events})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(string(payload)))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(ruxRateKeyHeader, uuid.NewString())
	result := httptest.NewRecorder()
	router.ServeHTTP(result, request)
	return result
}
func TestExperienceBatchIngest_MalformedMeasurementIsolated(t *testing.T) {
	db := experienceTestDB(t)
	session := uuid.NewString()
	good := rawExperienceTerminal(t, session)
	var malformed map[string]interface{}
	if err := json.Unmarshal(good, &malformed); err != nil {
		t.Fatal(err)
	}
	malformed["event_id"] = uuid.NewString()
	malformed["measurements"] = map[string]interface{}{"duration_ms": "wrong type", "visible": true}
	bad, err := json.Marshal(malformed)
	if err != nil {
		t.Fatal(err)
	}
	result := ingestExperienceFixture(t, db, []json.RawMessage{bad, good})
	if result.Code != http.StatusOK {
		t.Fatalf("response%d %s", result.Code, result.Body.String())
	}
	var counts struct{ Accepted, Duplicate, Rejected int }
	if err := json.Unmarshal(result.Body.Bytes(), &counts); err != nil {
		t.Fatal(err)
	}
	if counts.Accepted != 1 || counts.Rejected != 1 || counts.Duplicate != 0 {
		t.Fatalf("mixed validity counts%+v", counts)
	}
}
func TestExperienceBatchIngest_MixedSessionGuardStillAppliesToMistypedEvent(t *testing.T) {
	db := experienceTestDB(t)
	good := rawExperienceTerminal(t, uuid.NewString())
	var bad map[string]interface{}
	json.Unmarshal(good, &bad)
	bad["event_id"] = uuid.NewString()
	bad["session_id"] = uuid.NewString()
	bad["measurements"] = map[string]interface{}{"duration_ms": "wrong type"}
	malformed, _ := json.Marshal(bad)
	result := ingestExperienceFixture(t, db, []json.RawMessage{good, malformed})
	if result.Code != http.StatusBadRequest {
		t.Fatalf("mixed sessions accepted: %d", result.Code)
	}
	var count int64
	db.Model(&models.ExperienceEvent{}).Count(&count)
	if count != 0 {
		t.Fatal("mixed batch persisted")
	}
}
func TestExperienceBatchIngest_DefaultTenantScopeSingleLookup(t *testing.T) {
	db := experienceTestDB(t)
	if err := db.Exec(`CREATE TABLE content_items(public_id uuid PRIMARY KEY,tenant_id varchar(64),source_run_request_id uuid)`).Error; err != nil {
		t.Fatal(err)
	}
	public, foreign := uuid.New(), uuid.New()
	if err := db.Exec(`INSERT INTO content_items(public_id,tenant_id) VALUES (?,'default'),(?,'another-tenant')`, public, foreign).Error; err != nil {
		t.Fatal(err)
	}
	first := batchTerminal("default")
	first.ContentID = &public
	second := batchTerminal("default")
	second.ContentID = &foreign
	counter := &experienceStatementCounter{}
	items, err := experienceContentScope(db.Session(&gorm.Session{Logger: counter}), []models.ExperienceEvent{first, second, first})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[public].TenantID != "default" || counter.statements.Load() != 1 {
		t.Fatalf("scope items%d queries%d", len(items), counter.statements.Load())
	}
}
func TestExperienceBatchIngest_CanonicalTimestampColumnsStayUTC(t *testing.T) {
	db := experienceTestDB(t)
	if err := db.Exec(`ALTER TABLE experience_events ALTER COLUMN occurred_at TYPE timestamp without time zone USING occurred_at AT TIME ZONE 'UTC', ALTER COLUMN received_at TYPE timestamp without time zone USING received_at AT TIME ZONE 'UTC'`).Error; err != nil {
		t.Fatal(err)
	}
	event := validRuxEvent()
	event.OccurredAt = "2026-10-13T07:00:00+03:00"
	normalized, valid := validateAndNormalizeRuxEvent(&event, time.Now())
	if !valid {
		t.Fatal("valid timezone event rejected")
	}
	normalized.ReceivedAt = time.Date(2026, 10, 13, 9, 0, 0, 0, time.FixedZone("client", 3*3600))
	normalized.TenantID = "default"
	rows := []models.ExperienceEvent{normalized}
	if _, err := persistExperienceEvents(db, rows); err != nil {
		t.Fatal(err)
	}
	var actual struct{ Occurred, Received string }
	if err := db.Raw(`SELECT to_char(occurred_at,'YYYY-MM-DD HH24:MI:SS') AS occurred,to_char(received_at,'YYYY-MM-DD HH24:MI:SS') AS received FROM experience_events WHERE event_id=?`, normalized.EventID).Scan(&actual).Error; err != nil {
		t.Fatal(err)
	}
	if actual.Occurred != "2026-10-13 04:00:00" || actual.Received != "2026-10-13 06:00:00" {
		t.Fatalf("UTC timestamp evidence %+v", actual)
	}
}
