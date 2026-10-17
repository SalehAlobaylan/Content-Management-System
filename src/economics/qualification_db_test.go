package economics

import (
	"bytes"
	"content-management-system/src/models"
	"encoding/base64"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCollectorUsesVerifiedIdentityWithoutRequestPathIO(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c := NewCollector()
	c.SetPolicies([]Policy{{TenantID: "a", CollectionEnabled: true}})
	t.Setenv("DEFAULT_TENANT_ID", "a")
	id := uuid.NewString()
	engine := gin.New()
	engine.Use(c.Middleware())
	engine.GET("/api/v1/feed/pods", func(ctx *gin.Context) {
		if ctx.Query("signed") == "1" {
			ctx.Set("user_id", id)
			ctx.Set("tenant_id", "a")
		}
		if ctx.Query("signed") == "other" {
			ctx.Set("user_id", id)
			ctx.Set("tenant_id", "b")
		}
		ctx.JSON(200, gin.H{"ok": true})
	})
	for _, path := range []string{"/api/v1/feed/pods?signed=1", "/api/v1/feed/pods?user_id=" + id, "/api/v1/feed/pods?signed=other"} {
		w := httptest.NewRecorder()
		engine.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 200 {
			t.Fatal(w.Code)
		}
	}
	subjects := map[string]int64{}
	for key, n := range c.values {
		if key.Unit == "requests" {
			subjects[key.Subject] += n
		}
	}
	if subjects["public"] != 1 || subjects["account:"+id] != 1 || subjects["unknown"] != 1 {
		t.Fatal("unverified identity merged", subjects)
	}
}
func TestErasureRacingBatchCannotResurrectConsumer(t *testing.T) {
	db := economicsDB(t)
	id := uuid.NewString()
	now := time.Now().UTC()
	batch := Batch{Environment: EnvironmentName(), Version: 1, TenantID: "a", Epoch: "race", Sequence: 1, StartedAt: now.Truncate(24 * time.Hour), EndedAt: now, Rows: []Usage{{Day: now.Truncate(24 * time.Hour), Subject: "account:" + id, Operation: "search", Unit: "requests", Value: 9}}}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	wg.Add(2)
	go func() { defer wg.Done(); errs <- Ingest(db, "cms", batch) }()
	go func() { defer wg.Done(); errs <- db.Transaction(func(tx *gorm.DB) error { return Erase(tx, id) }) }()
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	if e := Ingest(db, "cms", batch); e != nil {
		t.Fatal(e)
	}
	var linked, total int64
	db.Model(&Usage{}).Where("subject=?", "account:"+id).Count(&linked)
	db.Model(&Usage{}).Select("sum(value)").Scan(&total)
	if linked != 0 || total != 9 {
		t.Fatal("race leaked or lost aggregate", linked, total)
	}
}
func TestRetentionFoldsOnceAndExpiresConsumerEvidence(t *testing.T) {
	db := economicsDB(t)
	now := time.Now().UTC()
	id := uuid.NewString()
	subject := "account:" + id
	old := now.AddDate(0, 0, -100).Truncate(24 * time.Hour)
	db.Create(&Usage{TenantID: "a", Day: old, Subject: subject, Operation: "search", Unit: "requests", Value: 7})
	db.Create(&Attribution{ID: "old-owner", TenantID: "a", Source: "cms", Subject: subject, OwnerSubject: subject, Feature: "pods", Benefit: "shared_content", ContentID: uuid.NewString(), CreatedAt: old})
	db.Exec(`INSERT INTO economics_diagnostic_examples(id,tenant_id,subject,created_at) VALUES('expired','a',?,?)`, subject, now.AddDate(0, 0, -8))
	for i := 0; i < 2; i++ {
		if e := db.Transaction(func(tx *gorm.DB) error { return Retention(tx, "a", now) }); e != nil {
			t.Fatal(e)
		}
	}
	var daily, monthly, examples int64
	db.Model(&Usage{}).Count(&daily)
	db.Table("economics_usage_monthly").Select("sum(value)").Scan(&monthly)
	db.Table("economics_diagnostic_examples").Count(&examples)
	var a Attribution
	db.First(&a, "id='old-owner'")
	if daily != 0 || monthly != 7 || examples != 0 || a.Subject != "expired" || a.OwnerSubject != "expired" {
		t.Fatal("retention duplicate or lingering consumer", daily, monthly, examples, a)
	}
}
func TestPublicNewsOpensAreNotUniquePeople(t *testing.T) {
	db := economicsDB(t)
	consumptionTables(t, db)
	article, story := uuid.New(), uuid.New()
	now := time.Now().UTC()
	db.Exec(`INSERT INTO content_items(public_id,tenant_id,type,story_id) VALUES(?,'a','NEWS',?)`, article, story)
	m := ConsumptionObservation{Version: 1, Client: "mobile", ObservedAt: now, Semantics: "story_open_v1", StoryID: story.String()}
	payload, _ := json.Marshal(m)
	for i := 0; i < 2; i++ {
		db.Create(&models.UserInteraction{PublicID: uuid.New(), ContentItemID: article, Type: models.InteractionTypeView, Metadata: payload, CreatedAt: now})
	}
	if e := Rebuild(db, "a"); e != nil {
		t.Fatal(e)
	}
	var weights, opens int64
	db.Model(&Weight{}).Count(&weights)
	db.Model(&Usage{}).Where("unit='news_open_events'").Select("sum(value)").Scan(&opens)
	if weights != 0 || opens != 2 {
		t.Fatal("public opens became distinct readers", weights, opens)
	}
}
func TestUnpricedAttemptsDoNotDonateInvoiceShareToKnownAccounts(t *testing.T) {
	now := time.Now().UTC().Truncate(24 * time.Hour)
	rows := reconcile([]Cost{
		{SourceID: "ai-owner", Provider: "fixture", Resource: "ai", PeriodStart: now, PeriodEnd: now.AddDate(0, 0, 1), Net: mustAmount(t, "3")},
		{SourceID: "ai-owner", Provider: "fixture", Resource: "ai", PeriodStart: now, PeriodEnd: now.AddDate(0, 0, 1), Net: Zero, Method: "incomplete_owner_evidence"},
		{SourceID: "ai-invoice", Provider: "fixture", Resource: "ai", PeriodStart: now, PeriodEnd: now.AddDate(0, 0, 1), Destination: "reconciliation", Net: mustAmount(t, "10"), Reconciled: true},
	})
	if rows[0].Net != mustAmount(t, "3") || rows[0].Reconciled || rows[2].Net != mustAmount(t, "7") || rows[2].Destination != "unallocated" {
		t.Fatal(rows)
	}
}
func TestAccountReadsAwaitRestoreLifecycleAuthority(t *testing.T) {
	db := economicsDB(t)
	markLifecycleReady(db, time.Time{})
	w := request(t, db, Accounts, "admin", "a", "/admin/economics/accounts", "")
	if w.Code != 503 {
		t.Fatal("restored identity exposed before IAM replay", w.Code, w.Body)
	}
}

func TestOwnerWorkAndFreeProviderUnitsDoNotCreateExtraExpense(t *testing.T) {
	db := economicsDB(t)
	ownerTables(t, db)
	now := time.Now().UTC()
	ref := uuid.NewString()
	subject := "account:" + uuid.NewString()
	if e := db.Transaction(func(tx *gorm.DB) error {
		return RegisterAttribution(tx, Attribution{ID: ref, TenantID: "a", Source: "cms", Subject: subject, Feature: "upload", Benefit: "shared_content", ContentID: uuid.NewString()})
	}); e != nil {
		t.Fatal(e)
	}
	event := models.AISpendEvent{EventID: uuid.New(), TenantID: "a", OccurredAt: now, SpendClass: "embedding", Operation: "embed", Provider: "local", Units: []byte(`{"compute_ms":125,"items":1}`), SourceService: "enrichment"}
	db.Create(&event)
	if e := LinkReceipt(db, event.EventID, "a", ref); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 2; i++ {
		if e := Rebuild(db, "a"); e != nil {
			t.Fatal(e)
		}
	}
	var work, expenses int64
	db.Model(&Usage{}).Where("unit='work_ms' AND subject=?", subject).Select("sum(value)").Scan(&work)
	db.Model(&Cost{}).Where("source_id='ai-owner'").Count(&expenses)
	if work != 125 || expenses != 0 {
		t.Fatal("local work lost, duplicated or priced twice", work, expenses)
	}
}
func TestCompletedProcessingAttemptsKeepElapsedWorkSeparateFromMoney(t *testing.T) {
	db := economicsDB(t)
	consumptionTables(t, db)
	if e := db.AutoMigrate(&models.ContentStageRequest{}, &models.ContentStageAttempt{}); e != nil {
		t.Fatal(e)
	}
	if e := db.Exec("ALTER TABLE content_stage_attempts ALTER COLUMN request_id TYPE uuid USING request_id::uuid").Error; e != nil {
		t.Fatal(e)
	}
	now := time.Now().UTC()
	started := now.Add(-2 * time.Second)
	account, content, requestID := uuid.New(), uuid.New(), uuid.New()
	db.Exec(`INSERT INTO content_items(public_id,tenant_id,author_id,type) VALUES(?,'a',?,'PODCAST')`, content, account)
	db.Create(&models.ContentStageRequest{PublicID: requestID, TenantID: "a", ContentItemID: content})
	db.Create(&models.ContentStageAttempt{PublicID: uuid.New(), TenantID: "a", RequestID: requestID, Stage: "pods_media_artifacts", EffectStartedAt: &started, FinishedAt: &now, FailureClass: "provider_unavailable"})
	for i := 0; i < 2; i++ {
		if e := Rebuild(db, "a"); e != nil {
			t.Fatal(e)
		}
	}
	var work, attempts, failures, expenses int64
	db.Model(&Usage{}).Where("unit='job_elapsed_ms'").Select("sum(value)").Scan(&work)
	db.Model(&Usage{}).Where("unit='job_attempts'").Select("sum(value)").Scan(&attempts)
	db.Model(&Usage{}).Where("unit='job_failures'").Select("sum(value)").Scan(&failures)
	db.Model(&Cost{}).Count(&expenses)
	if work != 2000 || attempts != 1 || failures != 1 || expenses != 0 {
		t.Fatal(work, attempts, failures, expenses)
	}
}

func TestWarningsRequireClosedWindowConfirmationAndRecovery(t *testing.T) {
	db := economicsDB(t)
	now := time.Now().UTC()
	at := func(days int) time.Time { return now.AddDate(0, 0, days) }
	for _, step := range []struct {
		day    int
		active bool
		status string
		count  int
	}{{0, true, "candidate", 1}, {0, true, "candidate", 1}, {1, true, "open", 2}, {2, false, "recovering", -1}, {3, false, "resolved", -2}} {
		if e := ObserveIssue(db, "a", "efficiency_regression", "pods", "Measured regression", step.active, map[string]any{"basis": "fixture"}, at(step.day)); e != nil {
			t.Fatal(e)
		}
		var issue Issue
		db.Where("tenant_id='a' AND kind='efficiency_regression'").Take(&issue)
		if issue.Status != step.status || issue.Confirmations != step.count {
			t.Fatal("confirmation advanced without new day or failed recovery", issue)
		}
	}
}
func TestUnknownPaidAttemptIsUnavailableRatherThanFree(t *testing.T) {
	db := economicsDB(t)
	ownerTables(t, db)
	id := uuid.NewString()
	ref := uuid.NewString()
	now := time.Now().UTC()
	if e := RegisterAttribution(db, Attribution{ID: ref, TenantID: "a", Source: "cms", Subject: "account:" + id, Feature: "transcription", Benefit: "private"}); e != nil {
		t.Fatal(e)
	}
	event := models.AISpendEvent{EventID: uuid.New(), TenantID: "a", OccurredAt: now, SpendClass: "llm", Operation: "translate", Provider: "fixture", Units: []byte(`{}`), Unpriced: true, UsageUnavailable: true, SourceService: "enrichment"}
	db.Create(&event)
	if e := LinkReceipt(db, event.EventID, "a", ref); e != nil {
		t.Fatal(e)
	}
	if e := Rebuild(db, "a"); e != nil {
		t.Fatal(e)
	}
	w := request(t, db, Accounts, "admin", "a", "/admin/economics/accounts", "")
	var body struct{ Accounts []AccountRow }
	if e := json.Unmarshal(w.Body.Bytes(), &body); e != nil {
		t.Fatal(e)
	}
	if w.Code != 200 || len(body.Accounts) != 1 || body.Accounts[0].Total != nil {
		t.Fatal("unpriced attempt reported as free", w.Code, w.Body)
	}
}

func TestUsageBatchCoalescesKeysAndRejectsAnotherEnvironment(t *testing.T) {
	db := economicsDB(t)
	now := time.Now().UTC()
	day := now.Truncate(24 * time.Hour)
	b := Batch{Version: 1, Environment: EnvironmentName(), TenantID: "a", Epoch: "coalesce", Sequence: 1, StartedAt: day, EndedAt: now, Rows: []Usage{{Day: day, Subject: "public", Operation: "search", Unit: "requests", Value: 2}, {Day: day, Subject: "public", Operation: "search", Unit: "requests", Value: 3}}}
	if e := Ingest(db, "cms", b); e != nil {
		t.Fatal(e)
	}
	var total int64
	db.Model(&Usage{}).Select("sum(value)").Scan(&total)
	if total != 5 {
		t.Fatal(total)
	}
	b.Epoch = "other-scope"
	b.Environment = "another-environment"
	if e := Ingest(db, "cms", b); e == nil {
		t.Fatal("environment mismatch accepted")
	}
}
func TestLifecycleInvalidatesAnExistingAccountPageCursor(t *testing.T) {
	db := economicsDB(t)
	now := time.Now().UTC()
	id := uuid.NewString()
	db.Create(&Usage{TenantID: "a", Day: now.Truncate(24 * time.Hour), Subject: "account:" + id, Operation: "search", Unit: "requests", Value: 1})
	if e := Rebuild(db, "a"); e != nil {
		t.Fatal(e)
	}
	var old string
	db.Table("economics_publications").Where("tenant_id='a'").Pluck("revision", &old)
	if e := db.Transaction(func(tx *gorm.DB) error { return Erase(tx, id) }); e != nil {
		t.Fatal(e)
	}
	cursor := base64.RawURLEncoding.EncodeToString([]byte(old + ":1"))
	w := request(t, db, Accounts, "admin", "a", "/admin/economics/accounts?cursor="+cursor, "")
	if w.Code != 409 {
		t.Fatal("old identity page cursor remained usable", w.Code, w.Body)
	}
}
func TestCollectorHighCardinalityShedsWithinItsMemoryBudget(t *testing.T) {
	c := NewCollector()
	c.SetPolicies([]Policy{{TenantID: "a", CollectionEnabled: true}})
	for i := 0; i < MaxKeys+1000; i++ {
		c.Observe("a", "account:"+uuid.NewString(), "search", "requests", "ok", 1, time.Now())
	}
	if len(c.values) > MaxKeys || c.bytes > 16*1024*1024 || c.lost < 1000 {
		t.Fatal("unbounded optional collection", len(c.values), c.bytes, c.lost)
	}
}

func TestCanonicalInvoiceUpgradeAllowsCreditsAndRejectsNonfiniteValues(t *testing.T) {
	db := economicsDB(t)
	if e := db.Exec(`CREATE TABLE ai_spend_invoices(amount_usd double precision NOT NULL CHECK(amount_usd>=0));INSERT INTO ai_spend_invoices VALUES(2)`).Error; e != nil {
		t.Fatal(e)
	}
	migration, e := os.ReadFile("../../migrations/20261017010000_economics_signed_invoices.sql")
	if e != nil {
		t.Fatal(e)
	}
	if e := db.Exec(string(migration)).Error; e != nil {
		t.Fatal(e)
	}
	if e := db.Exec(`INSERT INTO ai_spend_invoices VALUES(-1)`).Error; e != nil {
		t.Fatal(e)
	}
	if e := db.Exec(`INSERT INTO ai_spend_invoices VALUES('NaN'::float8)`).Error; e == nil {
		t.Fatal("nonfinite financial input accepted")
	}
	var total float64
	db.Table("ai_spend_invoices").Select("sum(amount_usd)").Scan(&total)
	if total != 1 {
		t.Fatal("existing invoice history changed", total)
	}
}
func TestSharedPreparationWorkCannotBeChargedToItsInitiatorAsServing(t *testing.T) {
	c := Cost{Resource: "app_compute", Metric: "work_ms", PeriodStart: testTime, PeriodEnd: testTime.AddDate(0, 0, 1)}
	_, qualified := infrastructureWeights(c, []Usage{{Day: testTime, Subject: "account:" + uuid.NewString(), Operation: "processing", Unit: "work_ms", Variant: "preparation_owner_receipt", Value: 100}}, []Profile{{Resource: "app_compute", Operation: "processing", EffectiveFrom: testTime, Weight: 1, Evidence: "fixture"}})
	if qualified {
		t.Fatal("shared preparation became uploader serving expense")
	}
}

func TestEfficiencyPeersMatchFeatureClientAndUsageBand(t *testing.T) {
	db := economicsDB(t)
	day := time.Now().UTC().Truncate(24 * time.Hour)
	first := ""
	for i := 0; i < 32; i++ {
		subject := "account:" + uuid.NewString()
		if i == 0 {
			first = subject
		}
		units, variant := int64(5), "observed:mobile"
		if i == 30 {
			units = 10000
		}
		if i == 31 {
			variant = "observed:web_completion"
		}
		db.Create(&Usage{TenantID: "a", Day: day, Subject: subject, Operation: "news_open", Unit: "news_opens", Variant: variant, Value: units})
		db.Create(&Summary{TenantID: "a", Day: day, Subject: subject, Resource: "database", Feature: "news", Direct: Zero, Shared: mustAmount(t, "1"), Method: "calibrated_resource_profile"})
	}
	rows, e := AccountEfficiency(db, "a", first, day, day.AddDate(0, 0, 1))
	if e != nil {
		t.Fatal(e)
	}
	for _, r := range rows {
		if r.Feature == "news" && (r.PeerCount != 30 || r.Amount == nil || *r.Amount != mustAmount(t, "0.2") || r.PeerMedian == nil) {
			t.Fatal("unlike populations were compared", r)
		}
	}
}

func TestCounterRetriesAreBoundedAndNewEpochCanRecover(t *testing.T) {
	db := economicsDB(t)
	collector := NewCollector()
	collector.SetPolicies([]Policy{{TenantID: "a", CollectionEnabled: true}})
	collector.Observe("a", "public", "search", "requests", "ok", 1, time.Now())
	batch := collector.seal("a")
	old := batch.Epoch
	batch.Version = 2
	for i := 0; i < 10; i++ {
		if e := collector.Flush(db); e == nil {
			t.Fatal("invalid sink acknowledgement accepted")
		}
	}
	if collector.pending["a"] != nil || collector.currentEpoch("a") == old {
		t.Fatal("failed envelope retained indefinitely")
	}
	collector.Observe("a", "public", "search", "requests", "ok", 1, time.Now())
	if e := collector.Flush(db); e != nil {
		t.Fatal("new epoch could not recover", e)
	}
	var total int64
	db.Model(&Usage{}).Select("sum(value)").Scan(&total)
	if total != 1 {
		t.Fatal("discarded data was replayed", total)
	}
}

func TestGracefulCollectorClosePreservesUsageAndClosesEpoch(t *testing.T) {
	db := economicsDB(t)
	c := NewCollector()
	c.SetPolicies([]Policy{{TenantID: "a", CollectionEnabled: true}})
	c.Observe("a", "public", "search", "requests", "ok", 3, time.Now())
	if e := c.Close(db); e != nil {
		t.Fatal(e)
	}
	var state struct {
		Sequence int64
		Closed   bool
	}
	if e := db.Table("economics_collection_state").Where("tenant_id=? AND source=? AND epoch=?", "a", "cms", c.currentEpoch("a")).Take(&state).Error; e != nil {
		t.Fatal(e)
	}
	if !state.Closed || state.Sequence < 2 {
		t.Fatal("graceful epoch left open", state)
	}
	var total int64
	db.Model(&Usage{}).Select("sum(value)").Scan(&total)
	if total != 3 {
		t.Fatal("shutdown changed observed usage", total)
	}
}

func TestConsumerIngestionDoesNotCopyIdentityIntoSQLLogs(t *testing.T) {
	db := economicsDB(t)
	var output bytes.Buffer
	recording := db.Session(&gorm.Session{Logger: logger.New(log.New(&output, "", 0), logger.Config{LogLevel: logger.Info})})
	subject := "account:" + uuid.NewString()
	now := time.Now().UTC()
	b := Batch{Version: 1, TenantID: "a", Environment: EnvironmentName(), Epoch: uuid.NewString(), Sequence: 1, StartedAt: now, EndedAt: now, Rows: []Usage{{Day: now.Truncate(24 * time.Hour), Subject: subject, Operation: "search", Unit: "requests", Value: 1}}}
	if e := Ingest(recording, "cms", b); e != nil {
		t.Fatal(e)
	}
	if output.Len() != 0 {
		t.Fatal("private consumer SQL escaped to application logger")
	}
	var count int64
	db.Model(&Usage{}).Where("subject=?", subject).Count(&count)
	if count != 1 {
		t.Fatal("privacy logging policy discarded the actual measurement")
	}
}

func TestBackgroundContentReferenceKeepsOwnerSeparateFromRequester(t *testing.T) {
	db := economicsDB(t)
	if e := db.Exec(`CREATE TABLE content_items(id bigserial,public_id uuid,author_id uuid,created_at timestamptz,type text,story_id uuid,tenant_id text)`).Error; e != nil {
		t.Fatal(e)
	}
	content, author := uuid.NewString(), uuid.NewString()
	if e := db.Exec(`INSERT INTO content_items(public_id,author_id,created_at,type,tenant_id) VALUES(?,?,?,'PODCAST','a')`, content, author, time.Now()).Error; e != nil {
		t.Fatal(e)
	}
	if e := ProjectContentAttributions(db, "a", true); e != nil {
		t.Fatal(e)
	}
	var ref Attribution
	if e := db.Where("id=?", "content:"+content).Take(&ref).Error; e != nil {
		t.Fatal(e)
	}
	if ref.Subject != "system" || ref.OwnerSubject != "account:"+author || ref.Benefit != "shared_content" {
		t.Fatal("background work invented an account requester", ref)
	}
	ownerTables(t, db)
	now := time.Now().UTC()
	event := models.AISpendEvent{EventID: uuid.New(), TenantID: "a", OccurredAt: now, SpendClass: "embedding", Operation: "embed", Provider: "local", Units: []byte(`{"compute_ms":125}`), SourceService: "enrichment"}
	if e := db.Create(&event).Error; e != nil {
		t.Fatal(e)
	}
	if e := LinkReceipt(db, event.EventID, "a", "content:"+content); e != nil {
		t.Fatal(e)
	}
	if e := ProjectOwnerUsage(db, "a"); e != nil {
		t.Fatal(e)
	}
	var work int64
	db.Model(&Usage{}).Where("subject=? AND variant='preparation_owned_receipt' AND unit='work_ms'", "account:"+author).Select("sum(value)").Scan(&work)
	if work != 125 {
		t.Fatal("owned work disappeared when requester was unknown", work)
	}
	if e := db.Transaction(func(tx *gorm.DB) error { return Erase(tx, author) }); e != nil {
		t.Fatal(e)
	}
	if e := db.Where("id=?", "content:"+content).Take(&ref).Error; e != nil {
		t.Fatal(e)
	}
	if ref.OwnerSubject != "removed" || ref.Subject != "system" {
		t.Fatal("owner identity survived erasure or actor changed", ref)
	}
}

func TestRestoredLifecycleAuthorityRetriesAndReplaysConfirmedRequestsBeforeReads(t *testing.T) {
	db := economicsDB(t)
	pool, _ := db.DB()
	lifecycleProof.Lock()
	delete(lifecycleProof.at, pool)
	delete(lifecycleProof.begun, pool)
	lifecycleProof.Unlock()
	id := uuid.NewString()
	at := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	if e := db.Exec(`UPDATE economics_lifecycle_sync SET cursor_time=now(),cursor_user='ffffffff-ffff-ffff-ffff-ffffffffffff',synced_at=now()`).Error; e != nil {
		t.Fatal(e)
	}
	db.Create(&Usage{TenantID: "a", Subject: "account:" + id, Day: time.Now().UTC().Truncate(24 * time.Hour), Operation: "search", Unit: "requests", Value: 3})
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer lifecycle-fixture" {
			t.Error("missing owner credential")
			w.WriteHeader(403)
			return
		}
		call := calls.Add(1)
		if call == 1 {
			w.WriteHeader(503)
			return
		}
		if call == 2 {
			if r.URL.Query().Get("after") != "1970-01-01T00:00:00Z" {
				t.Error("restored cursor skipped IAM authority")
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"erasures": []any{map[string]any{"user_id": id, "tenant_id": "a", "erased_at": at}}, "has_more": false})
			return
		}
		if r.URL.Query().Get("user_id") != id {
			t.Error("incremental lifecycle cursor was reset after successful replay")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"erasures": []any{}, "has_more": false})
	}))
	defer server.Close()
	t.Setenv("IAM_BASE_URL", server.URL)
	t.Setenv("OPERATOR_IAM_ACCESS_SNAPSHOT_TOKEN", "lifecycle-fixture")
	if e := SyncLifecycle(db); e == nil || LifecycleReady(db) {
		t.Fatal("unavailable IAM authority accepted")
	}
	if e := SyncLifecycle(db); e != nil || !LifecycleReady(db) {
		t.Fatal("authority did not recover", e)
	}
	if e := SyncLifecycle(db); e != nil {
		t.Fatal(e)
	}
	var linked, total int64
	db.Model(&Usage{}).Where("subject=?", "account:"+id).Count(&linked)
	db.Model(&Usage{}).Where("subject='removed'").Select("sum(value)").Scan(&total)
	if linked != 0 || total != 3 {
		t.Fatal("confirmed request was not erased before reads", linked, total)
	}
}
