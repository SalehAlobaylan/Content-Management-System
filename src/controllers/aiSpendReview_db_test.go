package controllers

import (
	"bytes"
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"encoding/json"
	"fmt"
	"math"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

func TestAISpendZeroCapNullCapAndPauseGrantValidation(t *testing.T) {
	db := aiSpendTestDB(t)
	p, _ := getTenantAISpendPolicy(db, "a")
	p.Enabled = true
	db.Save(p)
	zero := 0.0
	now := time.Now().UTC()
	b := models.AISpendBudget{TenantID: "a", Scope: "class:llm", CapUSD: &zero, WarnPct: 70, HardPct: 100, WindowStartedAt: now}
	db.Create(&b)
	if a := CheckTenantSpendAllowance(db, "a", "llm", 0, "manual", ""); a["allowed"] != false {
		t.Fatal(a)
	}
	b.CapUSD = nil
	db.Save(&b)
	if a := CheckTenantSpendAllowance(db, "a", "llm", 0, "manual", ""); a["reason"] != "uncapped" || a["allowed"] != true {
		t.Fatal(a)
	}
	pause := now.Add(time.Hour)
	b.PausedUntil = &pause
	db.Save(&b)
	grant := models.AISpendOverride{ID: uuid.New(), TenantID: "a", TriggerSource: "manual", CreatedBy: "human", CreatedAt: now, ExpiresAt: pause}
	db.Create(&grant)
	if a := CheckTenantSpendAllowance(db, "a", "llm", 0, "manual", grant.ID.String()); a["reason"] != "human_confirmed_override" {
		t.Fatal(a)
	}
	for _, mismatch := range []string{"expired", "future", "tenant", "trigger"} {
		g := grant
		g.ID = uuid.New()
		switch mismatch {
		case "expired":
			g.ExpiresAt = now.Add(-time.Second)
		case "future":
			g.CreatedAt = pause
		case "tenant":
			g.TenantID = "b"
		case "trigger":
			g.TriggerSource = "different"
		}
		db.Create(&g)
		if a := CheckTenantSpendAllowance(db, "a", "llm", 0, "manual", g.ID.String()); a["allowed"] != false {
			t.Fatalf("%s: %v", mismatch, a)
		}
	}
}

func TestAISpendReportingMarginAndAdvisoryCanonicalHeadline(t *testing.T) {
	db := aiSpendTestDB(t)
	p, _ := getTenantAISpendPolicy(db, "a")
	p.Enabled = true
	db.Save(p)
	now := time.Now().UTC()
	db.Create(&models.AISpendEvent{EventID: uuid.New(), TenantID: "a", OccurredAt: now.Add(-time.Minute), SpendClass: "llm", Operation: "translate", Provider: "fixture", Model: "fixture", CostUSD: 36, EventCount: 1, TriggerSource: "test", Units: aiSpendJSON(map[string]any{})})
	db.Create(&models.AISpendEvent{EventID: uuid.New(), TenantID: "b", OccurredAt: now.Add(-time.Minute), SpendClass: "llm", CostUSD: 10000, Units: aiSpendJSON(map[string]any{})})
	margin, err := aiSpendLagMargin(db, "a", 100, p, now)
	if err != nil || math.Abs(margin-3.65) > 1e-8 {
		t.Fatalf("margin=%f %v", margin, err)
	}
	zero := 0.0
	db.Create(&models.AISpendBudget{TenantID: "a", Scope: "platform", CapUSD: &zero, WarnPct: 70, HardPct: 100, WindowStartedAt: now.Add(-time.Hour)})
	run, err := executeAISpendRun(db, "a", "manual")
	if err != nil || run.Headline != "bounded_stop" {
		t.Fatalf("first completed run: %+v %v", run, err)
	}
	a := CheckTenantSpendAllowance(db, "a", "llm", 0, "test", "")
	if a["allowed"] != true || a["tolerance_usd"] != nil || a["tolerance_seconds"] != nil || a["tolerance_status"] != "unqualified_concurrency" {
		t.Fatal(a)
	}
	w := aiSpendRequest(t, db, GetAISpendStatus, "a", `{}`)
	var status map[string]any
	json.Unmarshal(w.Body.Bytes(), &status)
	if w.Code != 200 || status["headline"] != run.Headline {
		t.Fatalf("canonical mismatch: %s", w.Body)
	}
}

func TestAISpendPermanentActorGrainAndFilterAfterSweep(t *testing.T) {
	db := aiSpendTestDB(t)
	at := time.Now().UTC().AddDate(0, 0, -100)
	for _, actor := range []string{"one", "two"} {
		db.Create(&models.AISpendEvent{EventID: uuid.New(), TenantID: "a", OccurredAt: at, SpendClass: "llm", Operation: "translate", Provider: "fixture", Model: "fixture", TriggerSource: "manual", SystemRunID: "same-run", HumanActorID: actor, CostUSD: 2, EventCount: 1, Units: aiSpendJSON(map[string]any{})})
	}
	if _, err := executeAISpendRun(db, "a", "manual"); err != nil {
		t.Fatal(err)
	}
	var count int64
	db.Model(&models.AISpendEvent{}).Count(&count)
	if count != 0 {
		t.Fatal("raw events not swept")
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", fmt.Sprintf("/?from=%s&to=%s&human_actor_id=one", at.Format("2006-01-02"), at.AddDate(0, 0, 1).Format("2006-01-02")), nil)
	c.Set("db", db)
	c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: "a", UserID: "human", Role: "admin"})
	ListAISpendRollups(c)
	var result struct {
		Total   int64
		Rollups []models.AISpendRollup
	}
	json.Unmarshal(w.Body.Bytes(), &result)
	if w.Code != 200 || result.Total != 1 || len(result.Rollups) != 1 || result.Rollups[0].HumanActorID != "one" || result.Rollups[0].CostUSD != 2 {
		t.Fatal(w.Body)
	}
}

func TestAISpendCellSpikesRequireHistoryAndCannotBeMasked(t *testing.T) {
	db := aiSpendTestDB(t)
	now := time.Now().UTC()
	day := now.Truncate(24 * time.Hour)
	p := models.AISpendPolicy{SpikeMultiplier: 4}
	for i := 0; i <= 7; i++ {
		for _, cell := range []struct {
			trigger string
			cost    float64
		}{{"small", 1}, {"large", 1000}} {
			cost := cell.cost
			if i == 0 && cell.trigger == "small" {
				cost = 10
			}
			db.Create(&models.AISpendRollup{TenantID: "a", Day: day.AddDate(0, 0, -i), SpendClass: "llm", Operation: "translate", TriggerSource: cell.trigger, CostUSD: cost, Units: aiSpendJSON(map[string]any{})})
		}
	}
	db.Create(&models.AISpendRollup{TenantID: "a", Day: day, SpendClass: "llm", Operation: "translate", TriggerSource: "cold", CostUSD: 100, Units: aiSpendJSON(map[string]any{})})
	signals, err := aiSpendSpikeSignals(db, "a", &p, now)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]aiSpendSignal{}
	for _, s := range signals {
		seen[s.scope] = s
	}
	if !seen["llm/small"].active || seen["llm/large"].active || seen["llm/cold"].active || seen["llm/cold"].evidence["status"] != "insufficient_history" {
		t.Fatalf("%+v", signals)
	}
}

func TestAISpendComputeEpisodesRecoverFalsePositiveAndFlappingLock(t *testing.T) {
	db := aiSpendTestDB(t)
	p, _ := getTenantAISpendPolicy(db, "a")
	p.Enabled = true
	p.ComputeWatchLimits = aiSpendJSON(map[string]any{"embedding": 10})
	db.Save(p)
	now := time.Now().UTC()
	r := models.AISpendRollup{TenantID: "a", Day: now.Truncate(24 * time.Hour), SpendClass: "embedding", Operation: "embedding", Units: aiSpendJSON(map[string]any{"items": 20})}
	db.Create(&r)
	eval := func() {
		t.Helper()
		if _, err := evaluateAISpendEpisodes(db, "a", p, now); err != nil {
			t.Fatal(err)
		}
	}
	eval()
	eval()
	var ep models.AISpendEpisode
	db.Where("tenant_id='a' AND kind='compute_volume'").First(&ep)
	if ep.Status != "open" {
		t.Fatal(ep)
	}
	ep.FalsePositive = true
	ep.Status = "resolved"
	db.Save(&ep)
	eval()
	var count int64
	db.Model(&models.AISpendEpisode{}).Where("kind='compute_volume'").Count(&count)
	if count != 1 {
		t.Fatal("reopened persistent reviewed signal")
	}
	db.Model(&r).Update("units", aiSpendJSON(map[string]any{"items": 0}))
	eval()
	eval()
	eval()
	db.Model(&r).Update("units", aiSpendJSON(map[string]any{"items": 20}))
	eval()
	eval()
	var next models.AISpendEpisode
	db.Where("tenant_id='a' AND kind='compute_volume'").Order("id DESC").First(&next)
	if next.ID == ep.ID || next.FalsePositive || next.Status != "open" {
		t.Fatal(next)
	}
	next.Status = "resolved"
	next.ReopenTimes = aiSpendJSON([]time.Time{now.Add(-25 * time.Hour), now.Add(-time.Hour), now.Add(-time.Minute)})
	db.Save(&next)
	eval()
	db.First(&next, next.ID)
	if next.Status != "flapping" || next.Reopens != 3 {
		t.Fatal(next)
	}
	db.Model(&r).Update("units", aiSpendJSON(map[string]any{"items": 0}))
	eval()
	eval()
	eval()
	db.First(&next, next.ID)
	if next.Status != "flapping" {
		t.Fatal("flapping lock auto-cleared")
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/", bytes.NewBufferString(`{"reason":"reviewed"}`))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Params = gin.Params{{Key: "id", Value: fmt.Sprint(next.ID)}}
	c.Set("db", db)
	c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: "a", UserID: "reviewer", Role: "admin"})
	CloseAISpendEpisode(c)
	db.First(&next, next.ID)
	if w.Code != 200 || next.Reopens != 0 || next.ClosedBy != "reviewer" || next.ClosedAt == nil {
		t.Fatal(w.Body, next)
	}
}

func TestAISpendSTTReservationDriftAndOwnerWindowRetention(t *testing.T) {
	db := aiSpendTestDB(t)
	now := time.Now().UTC()
	start := now.AddDate(0, 0, -100)
	cfg := models.TranscriptionConfig{TenantID: "a", MonthlyWindowStart: start, MonthlySpendUsd: .2, MonthlyReservedUsd: .3}
	if err := db.Create(&cfg).Error; err != nil {
		t.Fatal(err)
	}
	db.Create(&models.AISpendEvent{EventID: uuid.New(), TenantID: "a", OccurredAt: start.Add(time.Hour), SpendClass: "stt_api", Operation: "stt_settlement", CostUSD: .2, Units: aiSpendJSON(map[string]any{})})
	db.Create(&models.TranscriptionJob{PublicID: uuid.New(), ContentItemID: uuid.New(), TenantID: "a", Status: "queued", ReservedCostUsd: .3, CreatedAt: start.Add(time.Hour)})
	evidence, active, err := aiSpendSTTDrift(db, "a", cfg)
	if err != nil || active {
		t.Fatal(evidence, err)
	}
	cfg.MonthlyReservedUsd = .4
	evidence, active, err = aiSpendSTTDrift(db, "a", cfg)
	if err != nil || !active {
		t.Fatal(evidence, err)
	}
	if _, err = executeAISpendRun(db, "a", "manual"); err != nil {
		t.Fatal(err)
	}
	var count int64
	db.Model(&models.AISpendEvent{}).Where("tenant_id='a'").Count(&count)
	if count != 1 {
		t.Fatal("swept active STT owner-window evidence")
	}
}

func TestAISpendRestartUncertaintyCleanCloseAndUnknownOperationHygiene(t *testing.T) {
	db := aiSpendTestDB(t)
	p, _ := getTenantAISpendPolicy(db, "a")
	now := time.Now().UTC()
	e := models.AISpendEmitter{ProcessID: uuid.New().String(), TenantID: "a", SourceService: "fixture", StartedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-3 * time.Minute)}
	db.Create(&e)
	if _, err := executeAISpendRun(db, "a", "manual"); err != nil {
		t.Fatal(err)
	}
	health, err := aiSpendLedgerHealth(db, "a", p, now)
	if err != nil || health != "untrusted" {
		t.Fatal(health, err)
	}
	db.Model(&e).Update("closed", true)
	health, err = aiSpendLedgerHealth(db, "a", p, now)
	if err != nil || health != "healthy" {
		t.Fatal(health, err)
	}
	db.Create(&models.AISpendRollup{TenantID: "a", Day: now.Truncate(24 * time.Hour), SpendClass: "llm", Operation: "new_uncatalogued_op", Events: 1, CostUSD: 3, Units: aiSpendJSON(map[string]any{})})
	h, err := aiSpendHygiene(db, "a", now)
	if err != nil || len(h["unknown_operations"].([]string)) != 1 || h["metering_dropped_cumulative"] != int64(0) {
		t.Fatal(h, err)
	}
}

func TestAISpendComputePolicyAndSharedPriceAuthority(t *testing.T) {
	db := aiSpendTestDB(t)
	for _, body := range []string{`{"compute_watch_limits":{"embedding":0}}`, `{"compute_watch_limits":{"unknown":1}}`} {
		if w := aiSpendRequest(t, db, UpdateAISpendPolicy, "a", body); w.Code != 400 {
			t.Fatal(w.Body)
		}
	}
	if w := aiSpendRequest(t, db, UpdateAISpendPolicy, "a", `{"compute_watch_limits":{"embedding":12}}`); w.Code != 200 {
		t.Fatal(w.Body)
	}
	p, _ := getTenantAISpendPolicy(db, "a")
	if aiSpendComputeLimits(p)["embedding"] != 12 {
		t.Fatal(p)
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/", bytes.NewBufferString(`{"spend_class":"llm","provider":"fixture","model_pattern":"*"}`))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Set("db", db)
	c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: "a", UserID: "editor", Role: "editor"})
	CreateAIPriceBook(c)
	if w.Code != 403 {
		t.Fatal(w.Body)
	}
	_, cost, unpriced, err := resolveAISpendPriceChecked(db, aiSpendEventInput{SpendClass: "llm", Provider: "unknown", Model: "unknown", OccurredAt: time.Now().UTC(), Units: map[string]any{"input_tokens": 1000000}})
	if err != nil || !unpriced || cost != 1.5 {
		t.Fatal(cost, unpriced, err)
	}
}

func TestAISpendRunHistoryFiltersAndOwnerEvidenceIsolation(t *testing.T) {
	db := aiSpendTestDB(t)
	now := time.Now().UTC()
	for _, r := range []models.AISpendRun{{TenantID: "a", StartedAt: now, Status: "failed", Trigger: "manual"}, {TenantID: "a", StartedAt: now, Status: "completed", Trigger: "manual"}, {TenantID: "b", StartedAt: now, Status: "failed", Trigger: "manual"}, {TenantID: "a", StartedAt: now.AddDate(0, 0, -2), Status: "failed", Trigger: "manual"}} {
		db.Create(&r)
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/?from="+now.Format("2006-01-02")+"&status=failed&trigger=manual&limit=1", nil)
	c.Set("db", db)
	c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: "a", UserID: "human", Role: "admin"})
	ListAISpendRuns(c)
	var result struct {
		Total int64
		Runs  []models.AISpendRun
	}
	json.Unmarshal(w.Body.Bytes(), &result)
	if w.Code != 200 || result.Total != 1 || len(result.Runs) != 1 || result.Runs[0].TenantID != "a" {
		t.Fatal(w.Body)
	}
	if err := db.AutoMigrate(&models.OperatorInvestigation{}); err != nil {
		t.Fatal(err)
	}
	owner := models.OperatorInvestigation{PublicID: uuid.New(), TenantID: "a", ActorID: "operator-human", State: "completed", Request: aiSpendJSON(map[string]any{"private_prompt": "must not appear"})}
	db.Create(&owner)
	db.Create(&models.AISpendRollup{TenantID: "a", Day: now.Truncate(24 * time.Hour), SystemRunID: owner.PublicID.String(), TriggerSource: "operator", SpendClass: "llm", Operation: "operator_reason", Units: aiSpendJSON(map[string]any{})})
	refs := aiSpendOwnerReferences(db, "a", owner.PublicID.String())
	if len(refs) != 1 || refs[0]["status"] != "matched" {
		t.Fatal(refs)
	}
	metadata := refs[0]["metadata"].(map[string]any)
	if metadata["actor_id"] != "operator-human" || metadata["private_prompt"] != nil || metadata["request"] != nil {
		t.Fatal(metadata)
	}
	if refs = aiSpendOwnerReferences(db, "b", owner.PublicID.String()); len(refs) != 0 {
		t.Fatal("owner evidence leaked", refs)
	}
}

func TestAISpendNamedItemEconomicsExcludeMissingHistoricalDenominators(t *testing.T) {
	db := aiSpendTestDB(t)
	now := time.Now().UTC().Truncate(24 * time.Hour)
	for _, r := range []models.AISpendRollup{{TenantID: "a", Day: now, SpendClass: "llm", Operation: "translate", Provider: "fixture", Model: "new", CostUSD: 2, Events: 1, Units: aiSpendJSON(map[string]any{"operation_items": 2})}, {TenantID: "a", Day: now, SpendClass: "llm", Operation: "translate", Provider: "fixture", Model: "legacy", CostUSD: 9, Events: 1, Units: aiSpendJSON(map[string]any{})}} {
		db.Create(&r)
	}
	w := aiSpendRequest(t, db, GetAISpendStatus, "a", `{}`)
	// Decode field names explicitly: underscores are part of the API contract.
	var data map[string]json.RawMessage
	json.Unmarshal(w.Body.Bytes(), &data)
	var items []struct {
		Items          float64 `json:"items"`
		CostUSD        float64 `json:"cost_usd"`
		CapturedEvents int64   `json:"captured_events"`
		TotalEvents    int64   `json:"total_events"`
	}
	json.Unmarshal(data["item_economics"], &items)
	if w.Code != 200 || len(items) != 1 || items[0].Items != 2 || items[0].CostUSD != 2 || items[0].CapturedEvents != 1 || items[0].TotalEvents != 2 {
		t.Fatal(w.Body)
	}
}

func TestAISpendConfirmationPersistsAdmissionSnapshot(t *testing.T) {
	db := aiSpendTestDB(t)
	w := aiSpendRequest(t, db, ConfirmAISpendOverride, "a", `{"trigger_source":"manual_story_digest","confirm":true}`)
	if w.Code != 201 {
		t.Fatal(w.Body)
	}
	var grant models.AISpendOverride
	db.Where("tenant_id='a'").First(&grant)
	var snapshot map[string]any
	json.Unmarshal(grant.AdmissionSnapshot, &snapshot)
	if grant.CreatedBy != "human" || snapshot["reason"] != "governor_disabled" || snapshot["ledger_health"] != "no_evidence" {
		t.Fatal(grant, snapshot)
	}
	id := uuid.New()
	if err := db.Create(&models.AISpendReceipt{EventID: id}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&models.AISpendOverrideUse{EventID: id, OverrideID: grant.ID, TenantID: "a", SystemRunID: "run", HumanActorID: "human", OccurredAt: time.Now().UTC()}).Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		db.Create(&models.AISpendRollup{TenantID: "a", Day: time.Now().UTC().Truncate(24 * time.Hour), SpendClass: "llm", Operation: "translate", Model: fmt.Sprint(i), SystemRunID: "run", Events: 1, CostUSD: 1, Units: aiSpendJSON(map[string]any{})})
	}
	w = httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/?limit=2", nil)
	c.Params = gin.Params{{Key: "id", Value: "run"}}
	c.Set("db", db)
	c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: "a", UserID: "human", Role: "admin"})
	GetAISpendByRun(c)
	var detail struct {
		RollupTotal int                    `json:"rollup_total"`
		HumanTotal  int                    `json:"human_total"`
		Rollups     []models.AISpendRollup `json:"rollups"`
		Humans      []struct {
			Snapshot map[string]any `json:"admission_snapshot"`
		} `json:"human_confirmations"`
		Totals aiSpendTotals `json:"totals"`
	}
	json.Unmarshal(w.Body.Bytes(), &detail)
	if w.Code != 200 || detail.RollupTotal != 3 || len(detail.Rollups) != 2 || detail.Totals.CostUSD != 3 || detail.HumanTotal != 1 || len(detail.Humans) != 1 || detail.Humans[0].Snapshot["reason"] != "governor_disabled" {
		t.Fatal(w.Body)
	}
}
