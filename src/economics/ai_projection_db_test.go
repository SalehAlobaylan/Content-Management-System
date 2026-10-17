package economics

import (
	"content-management-system/src/models"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"testing"
	"time"
)

func ownerTables(t *testing.T, db *gorm.DB) {
	t.Helper()
	if e := db.AutoMigrate(&models.TranscriptionJob{}, &models.AISpendEvent{}, &models.AISpendRollup{}, &models.AISpendSTTSettlement{}, &models.AISpendInvoice{}); e != nil {
		t.Fatal(e)
	}
}
func TestCanonicalAIReceiptLegacyTotalAndInvoiceAreCountedOnce(t *testing.T) {
	db := economicsDB(t)
	ownerTables(t, db)
	now := time.Now().UTC()
	day := now.Truncate(24 * time.Hour)
	subject := "account:" + uuid.NewString()
	ref := uuid.NewString()
	if e := db.Transaction(func(tx *gorm.DB) error {
		return RegisterAttribution(tx, Attribution{ID: ref, TenantID: "a", Source: "cms", Subject: subject, Feature: "transcription", Benefit: "private"})
	}); e != nil {
		t.Fatal(e)
	}
	event := models.AISpendEvent{EventID: uuid.New(), TenantID: "a", OccurredAt: now, SpendClass: "llm", Operation: "translate", Provider: "fixture", Units: []byte(`{"input_tokens":1000,"output_tokens":500}`), CostUSD: .3, FoldedAt: &now, SourceService: "enrichment"}
	if e := db.Create(&event).Error; e != nil {
		t.Fatal(e)
	}
	if e := LinkReceipt(db, event.EventID, "a", ref); e != nil {
		t.Fatal(e)
	}
	db.Create(&models.AISpendRollup{TenantID: "a", Day: day, SpendClass: "llm", Provider: "fixture", CostUSD: 1.3})
	month := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	db.Create(&models.AISpendInvoice{TenantID: "a", Provider: "fixture", Month: month, AmountUSD: .65, CreatedBy: "test", Note: "Actual invoice fixture with a known 50% benefit"})
	for i := 0; i < 2; i++ {
		if e := Rebuild(db, "a"); e != nil {
			t.Fatal(e)
		}
	}
	var all, account, gross string
	db.Model(&Summary{}).Select("sum(direct+shared)::text").Scan(&all)
	db.Model(&Summary{}).Where("subject=?", subject).Select("sum(direct+shared)::text").Scan(&account)
	db.Model(&Summary{}).Select("sum(before_benefits)::text").Scan(&gross)
	if mustAmount(t, all) != mustAmount(t, "0.65") || mustAmount(t, account) != mustAmount(t, "0.15") || mustAmount(t, gross) != mustAmount(t, "1.3") {
		t.Fatal("invoice, receipt or legacy total duplicated", all, account, gross)
	}
	var tokens int64
	db.Model(&Usage{}).Where("subject=? AND unit='tokens'", subject).Select("sum(value)").Scan(&tokens)
	if tokens != 1500 {
		t.Fatal("canonical token usage was replayed or lost", tokens)
	}
}
func TestSharedPreparationUsesQualifiedMonthAndPreservesDeletedWeight(t *testing.T) {
	db := economicsDB(t)
	now := time.Now().UTC()
	day := now.Truncate(24 * time.Hour)
	content := uuid.NewString()
	a, b := uuid.NewString(), uuid.NewString()
	rule := ContentRule{TenantID: "a", Kind: "pods", Version: 1, EffectiveFrom: day, AllocableBPS: 10000, Observation: "mobile", Evidence: "Measured private qualification fixture; no public consumption", Actor: "test"}
	db.Create(&rule)
	ref := uuid.NewString()
	if e := db.Transaction(func(tx *gorm.DB) error {
		return RegisterAttribution(tx, Attribution{ID: ref, TenantID: "a", Source: "cms", Subject: "system", Feature: "pods", Benefit: "shared_content", ContentID: content})
	}); e != nil {
		t.Fatal(e)
	}
	month := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	for _, subject := range []string{"account:" + a, "account:" + b} {
		db.Create(&Weight{TenantID: "a", Month: month, ContentID: content, Subject: subject, Segment: uuid.NewString(), Client: "mobile", Kind: "media_seconds", Value: 600, ObservedAt: now})
	}
	c := Cost{TenantID: "a", SourceID: "fixture", LogicalKey: "shared-preparation", Provider: "test", Resource: "ai", Feature: "pods", PeriodStart: day, PeriodEnd: day.AddDate(0, 0, 1), Net: mustAmount(t, "1"), Currency: "USD", Destination: "unallocated", AttributionRef: ref, Reason: "shared artifact fixture", Actor: "test"}
	if _, e := AppendCost(db, c); e != nil {
		t.Fatal(e)
	}
	if e := Rebuild(db, "a"); e != nil {
		t.Fatal(e)
	}
	if e := db.Transaction(func(tx *gorm.DB) error { return Erase(tx, a) }); e != nil {
		t.Fatal(e)
	}
	db.Model(&Summary{}).Where("tenant_id='a'").Update("published_at", now.AddDate(0, 0, -1))
	if e := Rebuild(db, "a"); e != nil {
		t.Fatal(e)
	}
	var cost, all string
	db.Model(&Summary{}).Where("subject=?", "account:"+b).Select("sum(direct+shared)::text").Scan(&cost)
	db.Model(&Summary{}).Select("sum(direct+shared)::text").Scan(&all)
	if mustAmount(t, cost) != mustAmount(t, "0.5") || mustAmount(t, all) != mustAmount(t, "1") {
		t.Fatal("erasure renormalized another account or lost expense", cost, all)
	}
}
func TestUnchangedReportsDoNotRewriteCostGroups(t *testing.T) {
	db := economicsDB(t)
	day := time.Now().UTC().Truncate(24 * time.Hour)
	c := Cost{TenantID: "a", SourceID: "fixture", LogicalKey: "static", Provider: "test", Resource: "database", Feature: "serving", PeriodStart: day, PeriodEnd: day.AddDate(0, 0, 1), Net: mustAmount(t, "1"), Currency: "USD", Destination: "overhead", Reason: "unchanged source fixture", Actor: "test"}
	if _, e := AppendCost(db, c); e != nil {
		t.Fatal(e)
	}
	if e := Rebuild(db, "a"); e != nil {
		t.Fatal(e)
	}
	if e := Rebuild(db, "a"); e != nil {
		t.Fatal(e)
	}
	var run Run
	db.Order("started_at DESC").First(&run)
	if run.Rows != 0 {
		t.Fatal("unchanged report rewrote cost history", run.Rows)
	}
}
