package controllers

import (
	"bytes"
	"content-management-system/src/models"
	operatorpkg "content-management-system/src/operator"
	"content-management-system/src/tests/testdb"
	"content-management-system/src/utils"
	"context"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func aiSpendTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	if os.Getenv("CMS_TEST_ADMIN_URL") == "" {
		t.Skip("explicit disposable CMS_TEST_ADMIN_URL required")
	}
	db := testdb.Open(t)
	for _, name := range []string{"20260712010000_ai_spend_governor.sql", "20261012010000_ai_spend_completion.sql", "20261012020000_ai_spend_human_provenance.sql", "20261012030000_ai_spend_review_completion.sql", "20261017030000_ai_provider_cache_pricing.sql"} {
		sql, err := os.ReadFile("../../migrations/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if err = db.Exec(string(sql)).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.AutoMigrate(&models.TranscriptionJob{}, &models.TranscriptionConfig{}); err != nil {
		t.Fatal(err)
	}
	return db
}
func aiSpendRequest(t *testing.T, db *gorm.DB, fn gin.HandlerFunc, tenant, body string) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/", bytes.NewBufferString(body))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Set("db", db)
	c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: tenant, UserID: "human", Role: "admin"})
	fn(c)
	return w
}
func aiSpendIngestFixture(t *testing.T, db *gorm.DB, tenant string, at time.Time, id uuid.UUID) {
	t.Helper()
	payload := map[string]any{"events": []map[string]any{{"event_id": id.String(), "occurred_at": at, "tenant_id": tenant, "spend_class": "llm", "operation": "translate", "provider": "deepseek", "model": "deepseek-chat", "units": map[string]any{"input_tokens": 1000000, "output_tokens": 0}, "trigger_source": "test"}}}
	b, _ := json.Marshal(payload)
	w := aiSpendRequest(t, db, InternalIngestAISpendEvents, tenant, string(b))
	if w.Code != 202 {
		t.Fatalf("ingest: %d %s", w.Code, w.Body)
	}
}
func TestAISpendDurableAcknowledgementAndRetentionIdempotency(t *testing.T) {
	db := aiSpendTestDB(t)
	id := uuid.New()
	at := time.Now().UTC().AddDate(0, 0, -100)
	aiSpendIngestFixture(t, db, "a", at, id)
	aiSpendIngestFixture(t, db, "a", at, id)
	run, err := executeAISpendRun(db, "a", "manual")
	if err != nil {
		t.Fatal(err)
	}
	if run.EventsFolded != 1 {
		t.Fatalf("folded=%d", run.EventsFolded)
	}
	var count int64
	db.Model(&models.AISpendEvent{}).Where("tenant_id='a'").Count(&count)
	if count != 0 {
		t.Fatal("old event not swept")
	}
	aiSpendIngestFixture(t, db, "a", at, id)
	if _, err = executeAISpendRun(db, "a", "manual"); err != nil {
		t.Fatal(err)
	}
	var rollup models.AISpendRollup
	db.Where("tenant_id='a'").First(&rollup)
	if rollup.Events != 1 || rollup.CostUSD != .27 {
		t.Fatalf("redelivery changed rollup: %+v", rollup)
	}
	aiSpendIngestFixture(t, db, "a", at, uuid.New())
	if _, err = executeAISpendRun(db, "a", "manual"); err != nil {
		t.Fatal(err)
	}
	db.Where("tenant_id='a'").First(&rollup)
	if rollup.Events != 2 || rollup.CostUSD != .54 {
		t.Fatalf("late event overwrote history: %+v", rollup)
	}
}
func TestAISpendTenantExactWindowAndBudgetEdits(t *testing.T) {
	db := aiSpendTestDB(t)
	now := time.Now().UTC()
	start := now.Add(-time.Hour).Truncate(time.Microsecond)
	cap := .3
	b := models.AISpendBudget{TenantID: "a", Scope: "class:llm", CapUSD: &cap, WarnPct: 70, HardPct: 100, WindowStartedAt: start}
	db.Create(&b)
	db.Create(&models.AISpendPolicy{TenantID: "a", Enabled: true, AggregationIntervalMinutes: 5, RetentionDays: 90, ForecastHorizonDays: 30, SpikeMultiplier: 4})
	aiSpendIngestFixture(t, db, "a", start.Add(-time.Second), uuid.New())
	aiSpendIngestFixture(t, db, "b", now, uuid.New())
	answer := CheckTenantSpendAllowance(db, "a", "llm", 0, "test", "")
	if answer["allowed"] != true || answer["spend_usd"] != float64(0) {
		t.Fatalf("boundary or tenant leak: %+v", answer)
	}
	aiSpendIngestFixture(t, db, "a", start, uuid.New())
	answer = CheckTenantSpendAllowance(db, "a", "llm", .05, "test", "")
	if answer["allowed"] != false {
		t.Fatalf("expected bounded stop: %+v", answer)
	}
	w := aiSpendRequest(t, db, UpsertAISpendBudget, "a", `{"scope":"class:llm","cap_usd":2}`)
	if w.Code != 200 {
		t.Fatal(w.Body)
	}
	var edited models.AISpendBudget
	db.Where("id=?", b.ID).First(&edited)
	if !edited.WindowStartedAt.Equal(start) {
		t.Fatal("budget edit reset window")
	}
	if got := aiSpendActiveWindow(start.Add(-3*aiSpendWindow), now); !got.Equal(start) {
		t.Fatalf("rollover drift: %s != %s", got, start)
	}
}
func TestAISpendSpecificPriceAndMissingPrice(t *testing.T) {
	db := aiSpendTestDB(t)
	now := time.Now().UTC()
	db.Create(&models.AIPriceBook{SpendClass: "llm", Provider: "deepseek", ModelPattern: "*", InputUSDPer1M: 99, EffectiveFrom: now.Add(-time.Hour)})
	in := aiSpendEventInput{SpendClass: "llm", Provider: "deepseek", Model: "deepseek-chat", OccurredAt: now, Units: map[string]any{"input_tokens": float64(1000000)}}
	_, cost, u, err := resolveAISpendPriceChecked(db, in)
	if err != nil || u || cost != .27 {
		t.Fatalf("prefix precedence %v %f %v", err, cost, u)
	}
	in.Provider = "unconfigured"
	_, _, u, err = resolveAISpendPriceChecked(db, in)
	if err != nil || !u {
		t.Fatal("missing price not marked")
	}
}
func TestAISpendRunnerConflictAndSTTSettlementAmendments(t *testing.T) {
	db := aiSpendTestDB(t)
	lockTx := db.Begin()
	var ok bool
	lockTx.Raw("SELECT pg_try_advisory_xact_lock(hashtextextended(?,0))", "wahb:ai-spend/xact/v1/a").Scan(&ok)
	if !ok {
		t.Fatal("lock")
	}
	if _, err := executeAISpendRun(db, "a", "manual"); err != errAISpendBusy {
		t.Fatalf("conflict: %v", err)
	}
	lockTx.Rollback()
	now := time.Now().UTC()
	job := models.TranscriptionJob{PublicID: uuid.New(), TenantID: "a", ContentItemID: uuid.New(), TriggerSource: "manual", Status: "succeeded", Provider: "deepgram", ActualCostUsd: .2, CompletedAt: &now}
	if err := db.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := executeAISpendRun(db, "a", "manual"); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Model(&job).Update("actual_cost_usd", .3).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := executeAISpendRun(db, "a", "manual"); err != nil {
		t.Fatal(err)
	}
	var cost float64
	db.Model(&models.AISpendRollup{}).Where("tenant_id='a' AND spend_class='stt_api'").Select("sum(cost_usd)").Scan(&cost)
	if cost != .3 {
		t.Fatalf("owner settlement cost=%f", cost)
	}
}

func TestAISpendPartialFailureCannotAcknowledgeOrLoseReceipt(t *testing.T) {
	db := aiSpendTestDB(t)
	if err := db.Exec(`CREATE FUNCTION fail_spend_fixture() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.operation='fixture_fail' THEN RAISE EXCEPTION 'fixture insert failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER spend_fixture BEFORE INSERT ON ai_spend_events FOR EACH ROW EXECUTE FUNCTION fail_spend_fixture()`).Error; err != nil {
		t.Fatal(err)
	}
	ids := []uuid.UUID{uuid.New(), uuid.New()}
	events := []map[string]any{}
	for i, id := range ids {
		op := "translate"
		if i == 1 {
			op = "fixture_fail"
		}
		events = append(events, map[string]any{"event_id": id, "occurred_at": time.Now().UTC(), "tenant_id": "a", "spend_class": "llm", "operation": op, "provider": "deepseek", "model": "deepseek-chat", "units": map[string]int{"input_tokens": 100}, "trigger_source": "test"})
	}
	payload, _ := json.Marshal(map[string]any{"events": events})
	w := aiSpendRequest(t, db, InternalIngestAISpendEvents, "a", string(payload))
	if w.Code != 503 {
		t.Fatalf("expected failure: %d %s", w.Code, w.Body)
	}
	var result struct {
		Acks []struct {
			Status string `json:"status"`
		} `json:"acknowledgements"`
	}
	json.Unmarshal(w.Body.Bytes(), &result)
	if len(result.Acks) != 2 || result.Acks[0].Status != "inserted" || result.Acks[1].Status != "failed" {
		t.Fatal(w.Body)
	}
	var receipts int64
	db.Model(&models.AISpendReceipt{}).Count(&receipts)
	if receipts != 1 {
		t.Fatalf("failed insert left receipt: %d", receipts)
	}
	db.Exec("DROP TRIGGER spend_fixture ON ai_spend_events")
	w = aiSpendRequest(t, db, InternalIngestAISpendEvents, "a", string(payload))
	if w.Code != 202 {
		t.Fatal(w.Body)
	}
	var count int64
	db.Model(&models.AISpendEvent{}).Count(&count)
	if count != 2 {
		t.Fatalf("replay count %d", count)
	}
}

func TestAISpendPolicyPauseResumeAndAuthenticatedOverride(t *testing.T) {
	db := aiSpendTestDB(t)
	cap := .01
	now := time.Now().UTC()
	pause := now.Add(time.Hour)
	db.Create(&models.AISpendBudget{TenantID: "a", Scope: "class:llm", CapUSD: &cap, WarnPct: 70, HardPct: 100, WindowStartedAt: now.Add(-time.Hour)})
	p, _ := getTenantAISpendPolicy(db, "a")
	p.Enabled = true
	db.Save(p)
	if a := CheckTenantSpendAllowance(db, "a", "llm", 0, "manual", ""); a["allowed"] != false {
		t.Fatal(a)
	}
	g := models.AISpendOverride{ID: uuid.New(), TenantID: "a", CreatedBy: "human", TriggerSource: "manual", ExpiresAt: now.Add(time.Minute)}
	db.Create(&g)
	if a := CheckTenantSpendAllowance(db, "a", "llm", 0, "manual", g.ID.String()); a["allowed"] != true || a["over_cap_human"] != true {
		t.Fatal(a)
	}
	if a := CheckTenantSpendAllowance(db, "a", "llm", 0, "forged", g.ID.String()); a["allowed"] != false {
		t.Fatal(a)
	}
	p.PausedUntil = &pause
	db.Save(p)
	if a := CheckTenantSpendAllowance(db, "a", "llm", 0, "manual", g.ID.String()); a["reason"] != "human_confirmed_override" || a["allowed"] != true {
		t.Fatal(a)
	}
	w := aiSpendRequest(t, db, UpdateAISpendPolicy, "a", `{"enabled":false}`)
	if w.Code != 200 {
		t.Fatal(w.Body)
	}
	w = aiSpendRequest(t, db, UpdateAISpendPolicy, "a", `{"paused_until":null}`)
	if w.Code != 200 {
		t.Fatal(w.Body)
	}
	p = &models.AISpendPolicy{}
	db.Where("tenant_id='a'").First(p)
	if p.Enabled || p.PausedUntil != nil || p.RetentionDays != 90 {
		t.Fatalf("resume altered policy %+v", p)
	}
}

func TestAISpendAggregateCountUTCAndExactTotalOver500Cells(t *testing.T) {
	db := aiSpendTestDB(t)
	db.Exec("SET TIME ZONE 'Asia/Riyadh'")
	at := time.Now().UTC().Truncate(24 * time.Hour).Add(time.Minute)
	for i := 0; i < 510; i++ {
		e := models.AISpendEvent{EventID: uuid.New(), TenantID: "a", OccurredAt: at, EventCount: 3, SpendClass: "embedding", Operation: "embedding", Provider: "local", Model: "qwen", Units: aiSpendJSON(map[string]any{"items": 3, "compute_ms": 1}), CostUSD: .1, TriggerSource: "test", SystemRunID: fmt.Sprintf("run-%d", i), SourceService: "fixture"}
		if err := db.Create(&e).Error; err != nil {
			t.Fatal(err)
		}
	}
	if _, err := executeAISpendRun(db, "a", "manual"); err != nil {
		t.Fatal(err)
	}
	w := aiSpendRequest(t, db, GetAISpendStatus, "a", `{}`)
	if w.Code != 200 {
		t.Fatal(w.Body)
	}
	var status struct {
		Totals aiSpendTotals `json:"totals"`
	}
	json.Unmarshal(w.Body.Bytes(), &status)
	if status.Totals.Events != 1530 || math.Abs(status.Totals.CostUSD-51) > 1e-8 {
		t.Fatalf("total truncated: %+v", status.Totals)
	}
	var day time.Time
	db.Model(&models.AISpendRollup{}).Where("tenant_id='a'").Select("min(day)").Scan(&day)
	if day.Format("2006-01-02") != at.Format("2006-01-02") {
		t.Fatal("day used database timezone")
	}
}

func TestAISpendFailedRunRollsBackAndPreservesOwnerRows(t *testing.T) {
	db := aiSpendTestDB(t)
	aiSpendIngestFixture(t, db, "a", time.Now().UTC(), uuid.New())
	now := time.Now().UTC()
	job := models.TranscriptionJob{PublicID: uuid.New(), TenantID: "a", ContentItemID: uuid.New(), Status: "succeeded", Provider: "deepgram", ActualCostUsd: .1, CompletedAt: &now}
	db.Create(&job)
	hash := func() string {
		var s string
		db.Raw("SELECT md5(COALESCE(jsonb_agg(to_jsonb(t) ORDER BY id)::text,'')) FROM transcription_jobs t").Scan(&s)
		return s
	}
	before := hash()
	db.Exec(`CREATE FUNCTION fail_fold_fixture() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture fold failure'; END $$; CREATE TRIGGER fold_fixture BEFORE INSERT ON ai_spend_rollups FOR EACH ROW EXECUTE FUNCTION fail_fold_fixture()`)
	run, err := executeAISpendRun(db, "a", "manual")
	if err == nil || run.Status != "failed" || run.ErrorClass == "" {
		t.Fatalf("failed run not durable: %+v %v", run, err)
	}
	var count int64
	db.Model(&models.AISpendEvent{}).Where("tenant_id='a' AND folded_at IS NOT NULL").Count(&count)
	if count != 0 {
		t.Fatal("failed fold marked events durable")
	}
	db.Exec("DROP TRIGGER fold_fixture ON ai_spend_rollups")
	if _, err := executeAISpendRun(db, "a", "manual"); err != nil {
		t.Fatal(err)
	}
	if hash() != before {
		t.Fatal("accounting pass changed owner rows")
	}
}

func TestAISpendEpisodeConfirmationRecoveryAndEmitterSequence(t *testing.T) {
	db := aiSpendTestDB(t)
	p, _ := getTenantAISpendPolicy(db, "a")
	p.Enabled = true
	db.Save(p)
	now := time.Now().UTC()
	id := uuid.New()
	health := models.AISpendEmitter{ProcessID: id.String(), TenantID: "a", SourceService: "enrichment", Sequence: 2, Emitted: 2, Accepted: 1, Dropped: 1, StartedAt: now}
	payload, _ := json.Marshal(map[string]any{"health": health})
	w := aiSpendRequest(t, db, InternalIngestAISpendEvents, "a", string(payload))
	if w.Code != 202 {
		t.Fatal(w.Body)
	}
	health.Sequence = 1
	health.Dropped = 0
	payload, _ = json.Marshal(map[string]any{"health": health})
	aiSpendRequest(t, db, InternalIngestAISpendEvents, "a", string(payload))
	var saved models.AISpendEmitter
	db.Where("process_id=?", id.String()).First(&saved)
	if saved.Sequence != 2 || saved.Dropped != 1 {
		t.Fatal("old health overwrote loss")
	}
	for i := 0; i < 2; i++ {
		if _, err := executeAISpendRun(db, "a", "manual"); err != nil {
			t.Fatal(err)
		}
	}
	var ep models.AISpendEpisode
	db.Where("tenant_id='a' AND kind='untrusted_ledger'").First(&ep)
	if ep.Status != "open" {
		t.Fatalf("not confirmed %+v", ep)
	}
	db.Model(&saved).Update("last_loss_at", now.Add(-25*time.Hour))
	for i := 0; i < 3; i++ {
		if _, err := executeAISpendRun(db, "a", "manual"); err != nil {
			t.Fatal(err)
		}
	}
	db.First(&ep, ep.ID)
	if ep.Status != "resolved" {
		t.Fatalf("not resolved %+v", ep)
	}
}

func TestAISpendForecastRequiresHistoryAndUsesCompleteDays(t *testing.T) {
	db := aiSpendTestDB(t)
	now := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	policy := models.AISpendPolicy{ForecastHorizonDays: 10}
	cap := 100.0
	budget := models.AISpendBudget{TenantID: "a", Scope: "class:llm", CapUSD: &cap, SpendUSD: 20, WindowStartedAt: now.AddDate(0, 0, -10)}
	f, err := forecastAISpendBudget(db, "a", budget, &policy, now)
	if err != nil || f["status"] != "insufficient_history" {
		t.Fatalf("empty history: %v %v", f, err)
	}
	for i := 1; i <= 7; i++ {
		if err := db.Create(&models.AISpendRollup{TenantID: "a", Day: now.Truncate(24*time.Hour).AddDate(0, 0, -i), SpendClass: "llm", Operation: "translate", Provider: "test", Model: "test", TriggerSource: "test", Events: 1, CostUSD: 2, Units: aiSpendJSON(map[string]any{})}).Error; err != nil {
			t.Fatal(err)
		}
	}
	// Neither today's partial day nor another tenant should distort the forecast.
	db.Create(&models.AISpendRollup{TenantID: "a", Day: now.Truncate(24 * time.Hour), SpendClass: "llm", Operation: "today", CostUSD: 500, Units: aiSpendJSON(map[string]any{})})
	db.Create(&models.AISpendRollup{TenantID: "b", Day: now.AddDate(0, 0, -1).Truncate(24 * time.Hour), SpendClass: "llm", CostUSD: 500, Units: aiSpendJSON(map[string]any{})})
	f, err = forecastAISpendBudget(db, "a", budget, &policy, now)
	if err != nil || f["daily_pace_usd"] != 2.0 || f["horizon_spend_usd"] != 20.0 || f["projected_window_usd"] != 60.0 {
		t.Fatalf("forecast %v %v", f, err)
	}
	if got, ok := f["cap_reached_at"].(time.Time); !ok || !got.Equal(now.AddDate(0, 0, 40)) {
		t.Fatalf("cap date: %v", f)
	}
	cap = 1e9
	f, err = forecastAISpendBudget(db, "a", budget, &policy, now)
	if _, ok := f["cap_reached_at"]; err != nil || ok {
		t.Fatalf("unrepresentable date must stay unknown: %v %v", f, err)
	}
}

func TestAISpendHumanProvenanceSurvivesDelayedDeliveryAndSweep(t *testing.T) {
	db := aiSpendTestDB(t)
	at := time.Now().UTC().AddDate(0, 0, -100)
	grant := models.AISpendOverride{ID: uuid.New(), TenantID: "a", TriggerSource: "manual", CreatedBy: "human", CreatedAt: at.Add(-time.Minute), ExpiresAt: at.Add(time.Minute)}
	if err := db.Create(&grant).Error; err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	event := map[string]any{"event_id": id, "tenant_id": "a", "occurred_at": at, "spend_class": "llm", "operation": "translate", "provider": "deepseek", "model": "deepseek-chat", "units": map[string]any{"input_tokens": 1000000}, "trigger_source": "manual", "system_run_id": "old-run", "override_id": grant.ID}
	payload, _ := json.Marshal(map[string]any{"events": []any{event}})
	w := aiSpendRequest(t, db, InternalIngestAISpendEvents, "a", string(payload))
	if w.Code != 202 {
		t.Fatalf("ingest: %d %s", w.Code, w.Body)
	}
	var stored models.AISpendEvent
	db.Where("event_id=?", id).First(&stored)
	if !stored.OverCapHuman || stored.HumanActorID != "human" {
		t.Fatalf("missing actor: %+v", stored)
	}
	if _, err := executeAISpendRun(db, "a", "manual"); err != nil {
		t.Fatal(err)
	}
	var retained models.AISpendOverrideUse
	if err := db.Where("event_id=?", id).First(&retained).Error; err != nil || retained.HumanActorID != "human" {
		t.Fatal("permanent grant provenance lost", err)
	}
	var roll models.AISpendRollup
	db.Where("tenant_id='a'").First(&roll)
	if roll.OverCapHumanEvents != 1 {
		t.Fatal("human count lost")
	}
	// Caller flags and malformed grant IDs cannot create human attribution.
	event["event_id"] = uuid.New()
	event["occurred_at"] = time.Now().UTC()
	event["over_cap_human"] = true
	event["override_id"] = "invalid"
	payload, _ = json.Marshal(map[string]any{"events": []any{event}})
	w = aiSpendRequest(t, db, InternalIngestAISpendEvents, "a", string(payload))
	if w.Code != 202 {
		t.Fatal(w.Body)
	}
	stored = models.AISpendEvent{}
	db.Where("event_id=?", event["event_id"]).First(&stored)
	if stored.OverCapHuman {
		t.Fatal("forged human flag accepted")
	}
}

func TestAISpendLossFreshnessDoesNotAdvanceOnUnchangedCounters(t *testing.T) {
	db := aiSpendTestDB(t)
	started := time.Now().UTC().Add(-72 * time.Hour)
	health := map[string]any{"process_id": "process", "tenant_id": "a", "started_at": started, "sequence": 1, "dropped": 1, "emitted": 1}
	payload, _ := json.Marshal(map[string]any{"events": []any{}, "health": health})
	if w := aiSpendRequest(t, db, InternalIngestAISpendEvents, "a", string(payload)); w.Code != 202 {
		t.Fatal(w.Body)
	}
	old := time.Now().UTC().Add(-48 * time.Hour)
	db.Model(&models.AISpendEmitter{}).Where("tenant_id='a'").Update("last_loss_at", old)
	health["sequence"] = 2
	payload, _ = json.Marshal(map[string]any{"events": []any{}, "health": health})
	aiSpendRequest(t, db, InternalIngestAISpendEvents, "a", string(payload))
	var emitter models.AISpendEmitter
	db.Where("tenant_id='a'").First(&emitter)
	if emitter.LastLossAt == nil || math.Abs(emitter.LastLossAt.Sub(old).Seconds()) > .01 {
		t.Fatal("unchanged counter falsely refreshed loss")
	}
	if _, err := executeAISpendRun(db, "a", "manual"); err != nil {
		t.Fatal(err)
	}
	p, _ := getTenantAISpendPolicy(db, "a")
	if health, err := aiSpendLedgerHealth(db, "a", p, time.Now().UTC()); err != nil || health != "healthy" {
		t.Fatalf("expired loss: %s %v", health, err)
	}
}

func aiSpendCappedOwnerDB(t *testing.T) *gorm.DB {
	db := aiSpendTestDB(t)
	p, _ := getTenantAISpendPolicy(db, "a")
	p.Enabled = true
	if err := db.Save(p).Error; err != nil {
		t.Fatal(err)
	}
	cap := .01
	if err := db.Create(&models.AISpendBudget{TenantID: "a", Scope: "class:llm", CapUSD: &cap, WarnPct: 70, HardPct: 100, WindowStartedAt: time.Now().UTC()}).Error; err != nil {
		t.Fatal(err)
	}
	return db
}
func aiSpendCappedEnrichment(t *testing.T, expectedTrigger string) *int {
	calls := new(int)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{}`))
			return
		}
		var body struct {
			Context AISpendContext `json:"spend_context"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		*calls++
		if body.Context.TenantID != "a" || body.Context.TriggerSource != expectedTrigger || body.Context.SystemRunID == "" || body.Context.Admission["allowed"] != false {
			t.Errorf("missing authoritative context: %+v", body.Context)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(409)
		_, _ = w.Write([]byte(`{"error_code":"AI_BUDGET_CAPPED","error":"cap"}`))
	}))
	t.Cleanup(server.Close)
	t.Setenv("ENRICHMENT_BASE_URL", server.URL)
	t.Setenv("ENRICHMENT_SERVICE_TOKEN", "fixture")
	return calls
}
func TestAISpendPreferencesOwnerPersistsSkipWithoutCompletingProposal(t *testing.T) {
	db := aiSpendCappedOwnerDB(t)
	if err := db.AutoMigrate(&models.Topic{}, &models.TopicProposal{}, &models.PreferenceAutopilotRun{}, &models.PreferenceAutopilotAction{}); err != nil {
		t.Fatal(err)
	}
	proposal := models.TopicProposal{TenantID: "a", SuggestedSlug: "space", SuggestedLabelEN: "Space exploration", Status: "pending"}
	db.Create(&proposal)
	run := models.PreferenceAutopilotRun{TenantID: "a", PublicID: uuid.New(), Trigger: "manual"}
	db.Create(&run)
	calls := aiSpendCappedEnrichment(t, "preferences_autopilot")
	runner := preferenceAutopilotRunner{db: db, run: &run, policy: models.PreferenceAutopilotPolicy{MaxProposalsEnriched: 1, MaxTranslationCalls: 1}}
	scores := runner.enrichProposals()
	var action models.PreferenceAutopilotAction
	db.Where("run_id=?", run.ID).First(&action)
	if *calls != 1 || action.Guardrail != "budget_capped" || action.Status != models.PreferenceActionStatusSkipped || runner.ledgerErr != nil || len(scores) != 1 || !scores[0].BudgetCapped {
		t.Fatalf("missing skip: %d %+v %v", *calls, action, runner.ledgerErr)
	}
	db.First(&proposal, proposal.ID)
	if proposal.Status != "pending" || proposal.EnrichedAt != nil {
		t.Fatal("unfinished proposal falsely completed")
	}
}
func TestAISpendStudioOwnerPersistsSkipAndKeepsReview(t *testing.T) {
	db := aiSpendCappedOwnerDB(t)
	if err := db.AutoMigrate(&models.MediaStudioRun{}, &models.MediaStudioAction{}); err != nil {
		t.Fatal(err)
	}
	run := models.MediaStudioRun{TenantID: "a", PublicID: uuid.New(), Trigger: "manual", Mode: "assist", Status: "running", StartedAt: time.Now()}
	db.Create(&run)
	action := models.MediaStudioAction{TenantID: "a", RunID: run.ID, Status: "approval_required"}
	db.Create(&action)
	calls := aiSpendCappedEnrichment(t, "studio_autopilot")
	runner := studioRunner{db: db, run: &run, tenantID: "a", policy: models.MediaStudioAutopilotPolicy{MaxProposalsPerRun: 1}, proposalQueue: []studioProposalCase{{ActionID: action.ID, Chapter: models.Chapter{PublicID: uuid.New()}}}}
	runner.runProposalPhase()
	db.First(&action, action.ID)
	var skipped models.MediaStudioAction
	db.Where("run_id=? AND guardrail='budget_capped' AND status=?", run.ID, models.StudioActionStatusSkipped).First(&skipped)
	if *calls != 1 || runner.fatalErr != nil || action.Status != "approval_required" || action.Guardrail != "budget_capped" || skipped.ID == 0 {
		t.Fatalf("missing Studio skip %+v %v", action, runner.fatalErr)
	}
}

type aiSpendCountingReasoner struct{ calls int }

func (r *aiSpendCountingReasoner) Reason(context.Context, operatorpkg.ReasonRequest) (operatorpkg.ReasonResponse, error) {
	r.calls++
	return operatorpkg.ReasonResponse{}, nil
}
func TestAISpendOperatorOwnerPersistsSkipBeforeReasoner(t *testing.T) {
	db := aiSpendCappedOwnerDB(t)
	if err := db.AutoMigrate(&models.OperatorInvestigation{}, &models.OperatorInvestigationEvent{}); err != nil {
		t.Fatal(err)
	}
	investigation := models.OperatorInvestigation{PublicID: uuid.New(), TenantID: "a", ActorID: "human", State: "running"}
	db.Create(&investigation)
	next := &aiSpendCountingReasoner{}
	guarded := spendGuardedReasoner{db: db, next: next}
	_, err := guarded.Reason(context.Background(), operatorpkg.ReasonRequest{InvestigationID: investigation.PublicID.String(), Packet: operatorpkg.DecisionPacket{TenantID: "a"}})
	var event models.OperatorInvestigationEvent
	db.Where("investigation_id=? AND event_type='budget_capped'", investigation.ID).First(&event)
	if !isAISpendCapped(err) || next.calls != 0 || event.ID == 0 {
		t.Fatalf("reasoner executed or skip missing %v %+v", err, event)
	}
}
func TestAISpendEpisodeRepeatedRecoveryBecomesFlapping(t *testing.T) {
	db := aiSpendTestDB(t)
	p, _ := getTenantAISpendPolicy(db, "a")
	p.Enabled = true
	db.Save(p)
	now := time.Now().UTC()
	emitter := models.AISpendEmitter{ProcessID: uuid.New().String(), TenantID: "a", SourceService: "enrichment", Sequence: 1, Emitted: 1, Dropped: 1, StartedAt: now, LastLossAt: &now, UpdatedAt: now}
	db.Create(&emitter)
	run := func(n int) {
		for i := 0; i < n; i++ {
			if _, err := executeAISpendRun(db, "a", "manual"); err != nil {
				t.Fatal(err)
			}
		}
	}
	run(2)
	for cycle := 0; cycle < 3; cycle++ {
		db.Model(&models.AISpendEmitter{}).Where("process_id=?", emitter.ProcessID).Update("last_loss_at", time.Now().UTC().Add(-25*time.Hour))
		run(3)
		db.Model(&models.AISpendEmitter{}).Where("process_id=?", emitter.ProcessID).Update("last_loss_at", time.Now().UTC())
		run(2)
	}
	var ep models.AISpendEpisode
	db.Where("tenant_id='a' AND kind='untrusted_ledger'").First(&ep)
	if ep.Status != "flapping" || ep.Reopens != 3 {
		t.Fatalf("flapping lifecycle %+v", ep)
	}
}

func TestAISpendProviderCachePriceUsesBoundedSubsetAndHistoricalFallback(t *testing.T) {
	db := aiSpendTestDB(t)
	cached := 0.003
	now := time.Now().UTC()
	old := models.AIPriceBook{SpendClass: "llm", Provider: "test-cache", ModelPattern: "model", InputUSDPer1M: 0.15, OutputUSDPer1M: 0.6, EffectiveFrom: now.Add(-time.Hour)}
	if e := db.Create(&old).Error; e != nil {
		t.Fatal(e)
	}
	current := old
	current.ID = 0
	current.EffectiveFrom = now
	current.CachedInputUSDPer1M = &cached
	if e := db.Create(&current).Error; e != nil {
		t.Fatal(e)
	}
	for _, c := range []struct {
		name string
		at   time.Time
		hits float64
		want float64
	}{
		{"discount", now, 400000, 0.0912}, {"clamp oversized", now, 2000000, 0.003}, {"negative subset", now, -1, 0.15}, {"historical fallback", now.Add(-time.Minute), 400000, 0.15},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, cost, _, e := resolveAISpendPriceChecked(db, aiSpendEventInput{SpendClass: "llm", Provider: "test-cache", Model: "model", OccurredAt: c.at, Units: map[string]any{"input_tokens": 1000000, "input_cache_hit_tokens": c.hits}})
			if e != nil || math.Abs(cost-c.want) > 1e-12 {
				t.Fatalf("cost=%v want=%v err=%v", cost, c.want, e)
			}
		})
	}
}

func TestAIPriceRejectsNegativeProviderCacheRate(t *testing.T) {
	db := aiSpendTestDB(t)
	w := aiSpendRequest(t, db, CreateAIPriceBook, "a", `{"spend_class":"llm","provider":"deepseek","cached_input_usd_per_1m":-0.1}`)
	if w.Code != 400 {
		t.Fatalf("negative cached rate accepted: %d %s", w.Code, w.Body)
	}
}
