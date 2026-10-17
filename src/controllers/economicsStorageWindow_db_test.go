package controllers

import (
	"bytes"
	"content-management-system/src/models"
	"content-management-system/src/tests/testdb"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestCloudflareWindowReplayAmendmentAndSDKOverlap(t *testing.T) {
	if os.Getenv("CMS_TEST_ADMIN_URL") == "" {
		t.Skip("explicit disposable local PostgreSQL required")
	}
	db := testdb.Open(t)
	if e := db.Exec("CREATE TABLE transcription_jobs(id bigserial PRIMARY KEY)").Error; e != nil {
		t.Fatal(e)
	}
	sql, e := os.ReadFile("../../migrations/20261016010000_platform_user_economics.sql")
	if e != nil {
		t.Fatal(e)
	}
	if e = db.Exec(string(sql)).Error; e != nil {
		t.Fatal(e)
	}
	if e = db.AutoMigrate(&models.StorageOpMetric{}); e != nil {
		t.Fatal(e)
	}
	gin.SetMode(gin.TestMode)
	start := time.Now().UTC().Truncate(time.Hour).Add(-2 * time.Hour)
	write := func(count int64) *httptest.ResponseRecorder {
		req := writeOpMetricsRequest{Source: "cloudflare", TenantID: "a", WindowKey: "account/media", WindowStart: start.Format(time.RFC3339), WindowEnd: start.Add(time.Hour).Format(time.RFC3339), Date: start.Format("2006-01-02"), Items: []opMetricItem{{Tier: "primary", OpClass: "B", OpType: "GET", Count: count}, {Tier: "primary", OpClass: "F", OpType: "DELETE", Count: 4}}}
		body, _ := json.Marshal(req)
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Set("db", db)
		c.Request = httptest.NewRequest("POST", "/internal/storage/op-metrics", bytes.NewReader(body))
		InternalWriteOpMetrics(c)
		return w
	}
	for _, n := range []int64{10, 10, 7} {
		w := write(n)
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body)
		}
	}
	var reads int64
	db.Model(&models.StorageOpMetric{}).Where("op_class='B'").Select("sum(count)").Scan(&reads)
	if reads != 7 {
		t.Fatal("window replay/amendment duplicated usage", reads)
	}
	db.Create(&models.StorageOpMetric{TenantID: "a", Date: start.Truncate(24 * time.Hour), Tier: "primary", OpClass: "B", OpType: "GET", Source: "internal", Count: 5})
	classA, classB := monthToDateOpCounts(db, "a")
	if classA != 0 || classB != 7 {
		t.Fatal("free deletes or SDK overlap were charged twice", classA, classB)
	}
}
