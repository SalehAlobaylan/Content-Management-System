package controllers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

func TestGetTranscriptUsesVisibleContentOwnerAsBaseTable(t *testing.T) {
	for _, visible := range []bool{true, false} {
		name := "visible active transcript"
		if !visible {
			name = "hidden or inactive transcript"
		}
		t.Run(name, func(t *testing.T) {
			db, mock := newMockGorm(t)
			id, owner := uuid.New(), uuid.New()
			for _, table := range []string{"feed_generation_heads", "feed_generation_memberships"} {
				mock.ExpectQuery(`SELECT count\(\*\) FROM information_schema.tables`).
					WithArgs(table, "BASE TABLE").
					WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
			}
			mock.ExpectQuery(`SELECT EXISTS .*constraint_found.*exact_items`).
				WillReturnRows(sqlmock.NewRows([]string{"constraint_found", "exact_items"}).AddRow(true, true))
			rows := sqlmock.NewRows([]string{"public_id", "content_item_id", "full_text", "segments", "created_at"})
			if visible {
				rows.AddRow(id, owner, "نص الحلقة", `[{"start":0,"end":2,"text":"نص الحلقة"}]`, time.Now())
			}
			// Pin the content table despite scanning a Transcript destination, and
			// retain visibility plus the owner's active-transcript association.
			mock.ExpectQuery(`SELECT transcripts\.\* FROM "content_items" JOIN transcripts ON transcripts.content_item_id = content_items.public_id WHERE content_items.status = .*content_items.feed_visibility = .*content_items.duration_sec BETWEEN .*media_head.tenant_id = content_items.tenant_id.*media_membership.member_id = content_items.public_id.*transcripts.public_id = .*content_items.transcript_id = transcripts.public_id`).
				WillReturnRows(rows)
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Set("db", db)
			c.Params = gin.Params{{Key: "id", Value: id.String()}}
			c.Request = httptest.NewRequest(http.MethodGet, "/api/v1/transcripts/"+id.String(), nil)
			GetTranscript(c)
			if visible {
				if w.Code != http.StatusOK {
					t.Fatalf("expected playable transcript, got %d: %s", w.Code, w.Body.String())
				}
				var response struct {
					Data TranscriptResponse `json:"data"`
				}
				if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
					t.Fatal(err)
				}
				if response.Data.ID != id.String() || response.Data.ContentItemID != owner.String() || response.Data.Segments == nil {
					t.Fatalf("expected owned timestamped transcript, got %#v", response.Data)
				}
			} else if w.Code != http.StatusNotFound {
				t.Fatalf("nonpublic transcript returned %d", w.Code)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
