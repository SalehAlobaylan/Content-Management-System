package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"encoding/json"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestJourneyChapterPagesAreOrderedAndGenerationBound(t *testing.T) {
	for _, stale := range []bool{false, true} {
		t.Run(fmt.Sprint("stale=", stale), func(t *testing.T) {
			sqlDB, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer sqlDB.Close()
			db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
			if err != nil {
				t.Fatal(err)
			}
			parent, gen := uuid.New(), uuid.New()
			mock.ExpectQuery(`SELECT .*content_items.*tenant_id = .*public_id = `).WithArgs("tenant-a", parent, 1).WillReturnRows(sqlmock.NewRows([]string{"public_id", "tenant_id", "type", "processing_generation"}).AddRow(parent, "tenant-a", "PODCAST", 4))
			mock.ExpectQuery(`SELECT .*atomization_generations.*generation_number DESC`).WithArgs("tenant-a", parent, int64(4), 1).WillReturnRows(sqlmock.NewRows([]string{"public_id"}).AddRow(gen))
			url := "/journey/chapters"
			if stale {
				url += "?cursor=" + uuid.NewString() + ":24"
			} else {
				rows := sqlmock.NewRows([]string{"id", "unit_index", "state", "title", "start_ms", "end_ms"})
				for i := 0; i < 26; i++ {
					rows.AddRow(uuid.NewString(), i, "verified", fmt.Sprint("Chapter ", i), i*300000, (i+1)*300000)
				}
				mock.ExpectQuery(`SELECT .*atomization_chapter_units.*g.public_id=.*u.unit_index>.*ORDER BY u.unit_index ASC LIMIT`).WithArgs("tenant-a", parent, int64(4), gen, -1, 26).WillReturnRows(rows)
			}
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest("GET", url, nil)
			c.Params = gin.Params{{Key: "id", Value: parent.String()}}
			c.Set("db", db)
			c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: "tenant-a", Permissions: []string{"content:read"}})
			AdminGetMediaJourneyChapters(c)
			if stale {
				if recorder.Code != 409 {
					t.Fatalf("stale page: %d %s", recorder.Code, recorder.Body.String())
				}
			} else {
				if recorder.Code != 200 {
					t.Fatalf("page: %d %s", recorder.Code, recorder.Body.String())
				}
				var response struct {
					Data struct {
						Items []struct {
							UnitIndex int `json:"unit_index"`
						}
						NextCursor string `json:"next_cursor"`
					}
				}
				if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
					t.Fatal(err)
				}
				if len(response.Data.Items) != 25 || response.Data.NextCursor != gen.String()+":24" {
					t.Fatalf("wrong cursor: %s", recorder.Body.String())
				}
				for i, row := range response.Data.Items {
					if row.UnitIndex != i {
						t.Fatalf("chapter order: %+v", response.Data.Items)
					}
				}
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestJourneyMissingTenantPolicyDoesNotSeedOnRead(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery(`SELECT .*media_atomization_policies`).WithArgs("fresh-tenant", 1).WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}))
	policy, err := readOnlyEffectiveMediaPolicy(db, &models.ContentItem{TenantID: "fresh-tenant"})
	if err != nil {
		t.Fatal(err)
	}
	if policy.PolicySource != "code_default" {
		t.Fatalf("wrong source: %s", policy.PolicySource)
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestJourneyEventPageUsesOccurrenceTimestamp(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	parent := uuid.New()
	mock.ExpectQuery(`SELECT .*content_items.*tenant_id = .*public_id = `).WithArgs("tenant-a", parent, 1).WillReturnRows(sqlmock.NewRows([]string{"public_id", "tenant_id", "type"}).AddRow(parent, "tenant-a", "PODCAST"))
	mock.ExpectQuery(`SELECT e.sequence,r.stage,e.event_type,e.occurred_at AS created_at,r.processing_generation.*e.sequence<.*ORDER BY e.sequence DESC LIMIT`).WithArgs("tenant-a", parent, parent, int64(20), 26).WillReturnRows(sqlmock.NewRows([]string{"sequence", "stage", "event_type", "created_at", "processing_generation"}).AddRow(19, "pods_media_artifacts", "checkpoint:uploaded", time.Date(2026, 9, 10, 17, 0, 0, 0, time.UTC), 4))
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("GET", "/journey/events?cursor=20", nil)
	c.Params = gin.Params{{Key: "id", Value: parent.String()}}
	c.Set("db", db)
	c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: "tenant-a", Permissions: []string{"content:read"}})
	AdminGetMediaJourneyEvents(c)
	if recorder.Code != 200 || !strings.Contains(recorder.Body.String(), `"created_at":"2026-09-10T17:00:00Z"`) {
		t.Fatalf("event evidence unavailable: %d %s", recorder.Code, recorder.Body.String())
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestJourneyListReturnsBoundedPageAndSnapshotTotalsWithoutWrites(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	items := []map[string]any{}
	for i := 1; i <= 26; i++ {
		items = append(items, map[string]any{"id": fmt.Sprintf("11111111-1111-4111-8111-%012d", i), "media_stage_state": "awaiting_approval", "status": "PENDING", "title": "Preview", "updated_at": "2026-09-10T16:20:13.407613"})
	}
	payload, _ := json.Marshal(items)
	mock.ExpectQuery("WITH inventory AS").WithArgs("tenant-a", "awaiting_download", "awaiting_download", "", 26).WillReturnRows(sqlmock.NewRows([]string{"items", "counts", "total"}).AddRow(string(payload), `[{"key":"awaiting_download","count":40}]`, 40))
	mock.ExpectQuery(`SELECT r.content_item_id::text AS id,MAX\(e.occurred_at\) AS at`).WillReturnRows(sqlmock.NewRows([]string{"id", "at"}).AddRow("11111111-1111-4111-8111-000000000001", time.Date(2026, 9, 10, 17, 0, 0, 0, time.UTC)))
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("GET", "/pipeline?view=list&lane=awaiting_download", nil)
	c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: "tenant-a", Permissions: []string{"content:read"}})
	adminMediaPipelineList(c, db, "tenant-a")
	if recorder.Code != 200 {
		t.Fatalf("%d: %s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Data struct {
			Items      []mediaAtomizationPipelineItem
			Total      int
			NextCursor string `json:"next_cursor"`
		}
	}
	if err = json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Data.Items) != 25 || response.Data.Total != 40 || response.Data.NextCursor != "11111111-1111-4111-8111-000000000025" {
		t.Fatalf("bad bounded page: %s", recorder.Body.String())
	}
	if response.Data.Items[0].LastProgressAt == nil || response.Data.Items[0].LastProgressAt.Hour() != 17 {
		t.Fatal("checkpoint occurrence time was not projected")
	}
	for _, item := range response.Data.Items {
		if item.UpdatedAt.Format(time.RFC3339Nano) != "2026-09-10T16:20:13.407613Z" {
			t.Fatalf("legacy timestamp not normalized: %s", item.UpdatedAt)
		}
		for _, action := range item.Actions {
			if action.Code == "download" && action.Enabled {
				t.Fatal("read-only principal admitted download")
			}
		}
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
