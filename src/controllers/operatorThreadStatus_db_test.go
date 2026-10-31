package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/tests/testdb"
	"content-management-system/src/utils"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestOperatorOpenChatSummariesAreOwnerScopedAndReadOnly(t *testing.T) {
	if os.Getenv("CMS_TEST_ADMIN_URL") == "" {
		t.Skip("requires guarded disposable Neon")
	}
	db := testdb.Open(t)
	if err := db.AutoMigrate(&models.OperatorThread{}, &models.OperatorInvestigation{}, &models.OperatorActionPlan{}, &models.OperatorPolicy{}, &models.OperatorCapabilityControl{}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	threads := []models.OperatorThread{
		{PublicID: uuid.New(), TenantID: "a", CreatorID: "alice", Title: "Open", Locale: "en", ExpiresAt: now.Add(time.Hour)},
		{PublicID: uuid.New(), TenantID: "a", CreatorID: "bob", Title: "Other owner", Locale: "en", ExpiresAt: now.Add(time.Hour)},
		{PublicID: uuid.New(), TenantID: "b", CreatorID: "alice", Title: "Other tenant", Locale: "en", ExpiresAt: now.Add(time.Hour)},
		{PublicID: uuid.New(), TenantID: "a", CreatorID: "alice", Title: "Expired", Locale: "en", ExpiresAt: now.Add(-time.Hour)},
		{PublicID: uuid.New(), TenantID: "a", CreatorID: "alice", Title: "Archived open chat", Locale: "en", ArchivedAt: &now, ExpiresAt: now.Add(time.Hour)},
	}
	if err := db.Create(&threads).Error; err != nil {
		t.Fatal(err)
	}
	investigations := []models.OperatorInvestigation{
		{TenantID: "a", ActorID: "alice", ThreadID: &threads[0].ID, State: "completed", ExpiresAt: now.Add(time.Hour)},
		{TenantID: "a", ActorID: "alice", ThreadID: &threads[0].ID, State: "backgrounded", ExpiresAt: now.Add(time.Hour)},
		{TenantID: "a", ActorID: "bob", ThreadID: &threads[0].ID, State: "running", ExpiresAt: now.Add(time.Hour)},
		{TenantID: "b", ActorID: "alice", ThreadID: &threads[0].ID, State: "running", ExpiresAt: now.Add(time.Hour)},
		{TenantID: "a", ActorID: "alice", ThreadID: &threads[4].ID, State: "completed", ReadAt: &now, ExpiresAt: now.Add(time.Hour)},
	}
	if err := db.Create(&investigations).Error; err != nil {
		t.Fatal(err)
	}
	plans := []models.OperatorActionPlan{
		{TenantID: "a", ActorID: "alice", InvestigationID: &investigations[0].ID, State: "awaiting_approval", ExpiresAt: now.Add(time.Hour)},
		{TenantID: "a", ActorID: "alice", InvestigationID: &investigations[4].ID, State: "awaiting_approval", ExpiresAt: now.Add(-time.Minute)},
		{TenantID: "a", ActorID: "bob", InvestigationID: &investigations[0].ID, State: "running", ExpiresAt: now.Add(time.Hour)},
	}
	if err := db.Create(&plans).Error; err != nil {
		t.Fatal(err)
	}
	selected := []string{}
	for _, thread := range threads {
		selected = append(selected, thread.PublicID.String())
	}
	call := func(tx *gorm.DB, ids string) (int, []byte) {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", "/admin/operator/threads?ids="+ids, nil)
		c.Set("db", tx)
		c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{UserID: "alice", TenantID: "a", TenantClaimed: true, Role: "admin"})
		ListOperatorThreads(c)
		return w.Code, w.Body.Bytes()
	}
	// The real handler must work in a transaction that rejects every write.
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("SET TRANSACTION READ ONLY").Error; err != nil {
			return err
		}
		code, body := call(tx, strings.Join(selected, ","))
		if code != 200 {
			t.Fatalf("status=%d %s", code, body)
		}
		var result struct {
			Items []struct {
				ID       string `json:"id"`
				Activity string `json:"activity_state"`
				Active   int    `json:"active_investigation_count"`
				Unread   int    `json:"unread_count"`
			} `json:"items"`
		}
		if err := json.Unmarshal(body, &result); err != nil {
			t.Fatal(err)
		}
		if len(result.Items) != 2 {
			t.Fatalf("owner/tenant/expiry filtering: %s", body)
		}
		for _, item := range result.Items {
			switch item.ID {
			case threads[0].PublicID.String():
				if item.Activity != "needs_approval" || item.Active != 1 || item.Unread != 1 {
					t.Fatalf("aggregate/owner scope: %+v", item)
				}
			case threads[4].PublicID.String():
				if item.Activity != "needs_review" || item.Active != 0 || item.Unread != 0 {
					t.Fatalf("expired preview: %+v", item)
				}
			default:
				t.Fatalf("leaked chat: %+v", item)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var after models.OperatorInvestigation
	if err := db.First(&after, investigations[0].ID).Error; err != nil || after.ReadAt != nil {
		t.Fatal("status polling acknowledged an unread answer")
	}
	if code, _ := call(db, "bad"); code != 400 {
		t.Fatalf("invalid selection status=%d", code)
	}
	// Missing status storage must never become an idle/healthy tab.
	if err := db.Migrator().DropTable(&models.OperatorActionPlan{}); err != nil {
		t.Fatal(err)
	}
	if code, _ := call(db, threads[0].PublicID.String()); code != 503 {
		t.Fatalf("failed status read=%d", code)
	}
}
