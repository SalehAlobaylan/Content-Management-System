package economics

import (
	"bytes"
	"content-management-system/src/utils"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func request(t *testing.T, db *gorm.DB, handler gin.HandlerFunc, role, tenant, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Set("db", db)
	c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: tenant, UserID: "operator", Role: role})
	c.Request = httptest.NewRequest("GET", path, bytes.NewBufferString(body))
	handler(c)
	return w
}
func TestAccountReadsAreAdminTenantScopedAndMissingCostsNull(t *testing.T) {
	db := economicsDB(t)
	day := time.Now().UTC().Truncate(24 * time.Hour)
	id := uuid.NewString()
	db.Create(&Usage{TenantID: "a", Day: day, Subject: "account:" + id, Operation: "search", Unit: "requests", Value: 4})
	w := request(t, db, Accounts, "reader", "a", "/", "")
	if w.Code != 403 {
		t.Fatal(w.Code, w.Body)
	}
	w = request(t, db, Accounts, "admin", "a", "/", "")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body)
	}
	var data struct {
		Accounts []struct {
			Total     *Amount `json:"total"`
			AccountID string  `json:"account_id"`
		}
	}
	if e := json.Unmarshal(w.Body.Bytes(), &data); e != nil {
		t.Fatal(e)
	}
	if len(data.Accounts) != 1 || data.Accounts[0].Total != nil {
		t.Fatal("missing cost became zero", w.Body)
	}
	w = request(t, db, Accounts, "admin", "b", "/", "")
	if w.Code != 200 || bytes.Contains(w.Body.Bytes(), []byte(id)) {
		t.Fatal("cross-tenant disclosure", w.Code, w.Body)
	}
}
func TestOverviewAndBreakdownConserveAtReadBoundary(t *testing.T) {
	db := economicsDB(t)
	day := time.Now().UTC().Truncate(24 * time.Hour)
	gross := mustAmount(t, "18")
	c := Cost{TenantID: "a", SourceID: "fixture", LogicalKey: "hosting", Provider: "test", Resource: "app_compute", Feature: "serving", PeriodStart: day, PeriodEnd: day.AddDate(0, 0, 1), Net: mustAmount(t, "10"), BeforeBenefits: &gross, Currency: "USD", Destination: "overhead", Reason: "provider fixture", Actor: "test"}
	if _, e := AppendCost(db, c); e != nil {
		t.Fatal(e)
	}
	if e := Rebuild(db, "a"); e != nil {
		t.Fatal(e)
	}
	w := request(t, db, Overview, "admin", "a", "/", "")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body)
	}
	var result struct {
		Total     ReportRow   `json:"total"`
		Resources []ReportRow `json:"resources"`
	}
	if e := json.Unmarshal(w.Body.Bytes(), &result); e != nil {
		t.Fatal(e)
	}
	if result.Total.Total == nil || *result.Total.Total != c.Net || result.Total.BeforeBenefits == nil || *result.Total.BeforeBenefits != gross {
		t.Fatal("money mismatch", w.Body)
	}
}
func TestHistoryClearDetachesExpenseJoinsKeepsResourceMoneyAndRevisions(t *testing.T) {
	db := economicsDB(t)
	day := time.Now().UTC().Truncate(24 * time.Hour)
	account := uuid.NewString()
	subject := "account:" + account
	c := Cost{TenantID: "a", SourceID: "fixture", LogicalKey: "private", Provider: "test", Resource: "ai", Feature: "transcription", PeriodStart: day, PeriodEnd: day.AddDate(0, 0, 1), Net: mustAmount(t, "5"), Currency: "USD", Destination: "direct", Subject: subject, ContentID: uuid.NewString(), Reason: "owner fixture", Actor: "test"}
	if _, e := AppendCost(db, c); e != nil {
		t.Fatal(e)
	}
	if e := Rebuild(db, "a"); e != nil {
		t.Fatal(e)
	}
	if e := db.Transaction(func(tx *gorm.DB) error { return Clear(tx, account, time.Now().UTC()) }); e != nil {
		t.Fatal(e)
	}
	var linked int64
	db.Model(&Summary{}).Where("subject=? AND expense_key<>''", subject).Count(&linked)
	if linked != 0 {
		t.Fatal("cleared account retains expense/content join")
	}
	if e := Rebuild(db, "a"); e != nil {
		t.Fatal(e)
	}
	var accountTotal, platformTotal string
	db.Model(&Summary{}).Where("subject=?", subject).Select("sum(direct+shared)::text").Scan(&accountTotal)
	db.Model(&Summary{}).Select("sum(direct+shared)::text").Scan(&platformTotal)
	if mustAmount(t, accountTotal) != c.Net || mustAmount(t, platformTotal) != c.Net {
		t.Fatal("clear changed resource or platform total", accountTotal, platformTotal)
	}
}
func TestLateCorrectionAfterDailyExpiryDoesNotInventAccountDetail(t *testing.T) {
	db := economicsDB(t)
	day := time.Now().UTC().AddDate(0, 0, -100).Truncate(24 * time.Hour)
	subject := "account:" + uuid.NewString()
	c := Cost{TenantID: "a", SourceID: "fixture", LogicalKey: "historical", Provider: "test", Resource: "ai", Feature: "private", PeriodStart: day, PeriodEnd: day.AddDate(0, 0, 1), Net: mustAmount(t, "9"), Currency: "USD", Destination: "direct", Subject: subject, Reason: "historical fixture", Actor: "test"}
	if _, e := AppendCost(db, c); e != nil {
		t.Fatal(e)
	}
	if e := Rebuild(db, "a"); e != nil {
		t.Fatal(e)
	}
	c.Net = mustAmount(t, "12")
	c.Reason = "corrected provider bill"
	if _, e := AppendCost(db, c); e != nil {
		t.Fatal(e)
	}
	if e := Rebuild(db, "a"); e != nil {
		t.Fatal(e)
	}
	var total, account string
	db.Model(&Summary{}).Select("sum(direct+shared)::text").Scan(&total)
	db.Model(&Summary{}).Where("subject=?", subject).Select("sum(direct+shared)::text").Scan(&account)
	if mustAmount(t, total) != c.Net || mustAmount(t, account) != mustAmount(t, "9") {
		t.Fatal("expired evidence silently reconstructed", total, account)
	}
}

func TestImportedProviderEvidenceIsVisibleScopedAndNeverBecomesAnInvoice(t *testing.T) {
	db := economicsDB(t)
	source := uuid.NewString()
	if e := db.Exec(`INSERT INTO economics_sources(id,tenant_id,provider,resource,scope,currency,capability,status) VALUES(?,'a','neon','database','fixture-project','USD','neon_consumption','usage_only')`, source).Error; e != nil {
		t.Fatal(e)
	}
	payload := `{"timeframe_start":"2026-10-15T00:00:00Z","timeframe_end":"2026-10-16T00:00:00Z","metrics":[{"metric_name":"compute_unit_seconds","value":123.125}]}`
	if e := db.Exec(`INSERT INTO economics_import_windows(tenant_id,source,window_key,checksum,payload,observed_at) VALUES('a','neon',?,'fixture',?::jsonb,now())`, source+":fixture", payload).Error; e != nil {
		t.Fatal(e)
	}
	w := request(t, db, Sources, "admin", "a", "/admin/economics/sources", "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"value":"123.125"`) || !strings.Contains(w.Body.String(), `"usage_evidence"`) {
		t.Fatal(w.Code, w.Body)
	}
	other := request(t, db, Sources, "admin", "b", "/admin/economics/sources", "")
	if other.Code != 200 || strings.Contains(other.Body.String(), "123.125") {
		t.Fatal("cross-tenant resource evidence", other.Code, other.Body)
	}
	var costs int64
	db.Model(&Cost{}).Count(&costs)
	if costs != 0 {
		t.Fatal("provider usage invented a charge")
	}
}
