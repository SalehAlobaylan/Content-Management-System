package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/tests/testdb"
	"content-management-system/src/utils"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func operatorDetailsTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	if os.Getenv("CMS_TEST_ADMIN_URL") == "" {
		t.Skip("requires guarded disposable Neon")
	}
	db := testdb.Open(t)
	if err := db.AutoMigrate(&models.OperatorThread{}, &models.OperatorMessage{}, &models.OperatorInvestigation{}, &models.OperatorActionPlan{}, &models.OperatorSchedule{}, &models.OperatorScheduleRun{}, &models.OperatorPolicy{}, &models.OperatorCapabilityControl{}); err != nil {
		t.Fatal(err)
	}
	return db
}
func operatorDetailsTestRead(t *testing.T, db *gorm.DB, route string, handler gin.HandlerFunc, thread string) (int, []byte) {
	t.Helper()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", route, nil)
	c.Set("db", db)
	c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{UserID: "alice", TenantID: "a", TenantClaimed: true, Role: "admin"})
	if thread != "" {
		c.Params = gin.Params{{Key: "id", Value: thread}}
	}
	handler(c)
	return w.Code, w.Body.Bytes()
}
func TestOperatorDetailsHistoryPagesAndFocusAreOwnedReadOnly(t *testing.T) {
	db := operatorDetailsTestDB(t)
	now := time.Now().UTC()
	thread := models.OperatorThread{TenantID: "a", CreatorID: "alice", Title: "Long history", Locale: "en", ExpiresAt: now.Add(time.Hour)}
	if err := db.Create(&thread).Error; err != nil {
		t.Fatal(err)
	}
	inv := models.OperatorInvestigation{TenantID: "a", ActorID: "alice", ThreadID: &thread.ID, State: "completed", ExpiresAt: now.Add(time.Hour)}
	if err := db.Create(&inv).Error; err != nil {
		t.Fatal(err)
	}
	other := models.OperatorInvestigation{TenantID: "a", ActorID: "bob", ThreadID: &thread.ID, State: "completed", ExpiresAt: now.Add(time.Hour)}
	if err := db.Create(&other).Error; err != nil {
		t.Fatal(err)
	}
	rows := make([]models.OperatorMessage, 245)
	for i := range rows {
		rows[i] = models.OperatorMessage{ThreadID: thread.ID, TenantID: "a", ActorType: "admin", Content: datatypes.JSON(fmt.Sprintf(`{"text":"message %03d"}`, i)), CreatedAt: now.Add(time.Duration(i) * time.Second)}
		if i < 2 {
			rows[i].InvestigationID = &inv.ID
		}
	}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("SET TRANSACTION READ ONLY").Error; err != nil {
			return err
		}
		page, cursor, err := operatorThreadMessagePage(tx, thread, "")
		if err != nil {
			t.Fatal(err)
		}
		if len(page) != 100 || page[0].PublicID != rows[145].PublicID || page[99].PublicID != rows[244].PublicID || cursor == "" {
			t.Fatal("newest history was not returned")
		}
		page, cursor, err = operatorThreadMessagePage(tx, thread, cursor)
		if err != nil || len(page) != 100 || page[0].PublicID != rows[45].PublicID || page[99].PublicID != rows[144].PublicID {
			t.Fatalf("middle history overlap or gap: %v", err)
		}
		page, cursor, err = operatorThreadMessagePage(tx, thread, cursor)
		if err != nil || len(page) != 45 || page[0].PublicID != rows[0].PublicID || cursor != "" {
			t.Fatal("oldest history is not reachable")
		}
		code, body := operatorDetailsTestRead(t, tx, "/threads?focus="+inv.PublicID.String(), GetOperatorThread, thread.PublicID.String())
		if code != 200 {
			t.Fatalf("focused read %d: %s", code, body)
		}
		var result struct {
			Messages []struct {
				ID string `json:"id"`
			} `json:"messages"`
			Older string `json:"older_cursor"`
		}
		if err := json.Unmarshal(body, &result); err != nil {
			t.Fatal(err)
		}
		if len(result.Messages) != 102 || result.Messages[0].ID != rows[0].PublicID.String() || result.Messages[101].ID != rows[244].PublicID.String() || result.Older == "" {
			t.Fatal("selected answer was not retained with recent history")
		}
		if code, _ := operatorDetailsTestRead(t, tx, "/threads?focus="+other.PublicID.String(), GetOperatorThread, thread.PublicID.String()); code != 404 {
			t.Fatalf("cross-owner focus status=%d", code)
		}
		if code, _ := operatorDetailsTestRead(t, tx, "/threads?before=broken", GetOperatorThread, thread.PublicID.String()); code != 400 {
			t.Fatalf("bad history cursor=%d", code)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var after models.OperatorInvestigation
	if err := db.First(&after, inv.ID).Error; err != nil || after.ReadAt != nil {
		t.Fatal("history marked an answer read")
	}
}
func TestOperatorDetailsActivityFiltersBeforePagingAndFailsHonestly(t *testing.T) {
	db := operatorDetailsTestDB(t)
	now := time.Now().UTC()
	threads := []models.OperatorThread{{TenantID: "a", CreatorID: "alice", Title: "A", Locale: "en", ExpiresAt: now.Add(time.Hour)}, {TenantID: "a", CreatorID: "alice", Title: "B", Locale: "en", ExpiresAt: now.Add(time.Hour)}, {TenantID: "a", CreatorID: "bob", Title: "Other", Locale: "en", ExpiresAt: now.Add(time.Hour)}}
	if err := db.Create(&threads).Error; err != nil {
		t.Fatal(err)
	}
	rows := make([]models.OperatorInvestigation, 125)
	for i := range rows {
		state := "completed"
		if i < 5 {
			state = "failed"
		}
		rows[i] = models.OperatorInvestigation{TenantID: "a", ActorID: "alice", ThreadID: &threads[0].ID, State: state, VisibleContext: datatypes.JSON(`{"domain":"storage_quality"}`), ExpiresAt: now.Add(time.Hour), UpdatedAt: now}
	}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	others := []models.OperatorInvestigation{{TenantID: "a", ActorID: "alice", ThreadID: &threads[1].ID, State: "failed", ExpiresAt: now.Add(time.Hour)}, {TenantID: "a", ActorID: "bob", ThreadID: &threads[0].ID, State: "failed", ExpiresAt: now.Add(time.Hour)}, {TenantID: "b", ActorID: "alice", ThreadID: &threads[0].ID, State: "failed", ExpiresAt: now.Add(time.Hour)}}
	if err := db.Create(&others).Error; err != nil {
		t.Fatal(err)
	}
	plan := models.OperatorActionPlan{TenantID: "a", ActorID: "alice", InvestigationID: &rows[0].ID, State: "awaiting_approval", ToolKey: "storage.policy", ExpiresAt: now.Add(time.Hour)}
	if err := db.Create(&plan).Error; err != nil {
		t.Fatal(err)
	}
	read := func(tx *gorm.DB, query string) (map[string]any, int) {
		code, body := operatorDetailsTestRead(t, tx, "/tasks?"+query, ListOperatorTasks, "")
		var result map[string]any
		_ = json.Unmarshal(body, &result)
		return result, code
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("SET TRANSACTION READ ONLY").Error; err != nil {
			return err
		}
		cursor := ""
		seen := map[string]bool{}
		for page := 0; page < 4; page++ {
			result, code := read(tx, "kind=investigation&group=failed&limit=2&thread_id="+threads[0].PublicID.String()+"&cursor="+cursor)
			if code != 200 {
				t.Fatalf("activity status=%d %v", code, result)
			}
			for _, raw := range result["items"].([]any) {
				item := raw.(map[string]any)
				id := item["id"].(string)
				if seen[id] || item["state"] != "failed" || item["thread_id"] != threads[0].PublicID.String() {
					t.Fatal("cursor duplicate or owner/scope leak")
				}
				seen[id] = true
			}
			cursor = result["next_cursor"].(string)
			if cursor == "" {
				break
			}
		}
		if len(seen) != 5 {
			t.Fatalf("filter lost older matching work: %d", len(seen))
		}
		result, code := read(tx, "kind=plan&group=all&thread_id="+threads[0].PublicID.String())
		if code != 200 {
			t.Fatal(result)
		}
		items := result["items"].([]any)
		if len(items) != 1 {
			t.Fatal("plan filter")
		}
		item := items[0].(map[string]any)
		link := item["deep_link"].(string)
		if item["thread_id"] != threads[0].PublicID.String() || !strings.Contains(link, "thread="+threads[0].PublicID.String()) || !strings.Contains(link, "plan="+plan.PublicID.String()) {
			t.Fatal("plan lost its chat")
		}
		if _, code := read(tx, "thread_id="+threads[2].PublicID.String()); code != 404 {
			t.Fatalf("other owner chat status=%d", code)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Migrator().DropTable(&models.OperatorScheduleRun{}); err != nil {
		t.Fatal(err)
	}
	if _, code := read(db, "kind=all&group=all"); code != 503 {
		t.Fatalf("partial read silently returned an empty success: %d", code)
	}
}
