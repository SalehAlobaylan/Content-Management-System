package controllers

import (
	"content-management-system/src/models"
	"content-management-system/src/utils"
	"encoding/json"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"os"
	"strings"
	"testing"
)

func TestEmbeddingCanonicalInputSharedFixtures(t *testing.T) {
	raw, err := os.ReadFile("../../testdata/embedding-input.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct{ Title, Excerpt, Body, Transcript, Expected string }
	if err = json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		item := models.ContentItem{Title: &c.Title, Excerpt: &c.Excerpt, BodyText: &c.Body}
		if got := buildEmbeddingText(&item, c.Transcript); got != c.Expected {
			t.Fatalf("got %q want %q", got, c.Expected)
		}
	}
	title, body := "😀", strings.Repeat("ح", 10_000)
	input := buildEmbeddingText(&models.ContentItem{Title: &title, BodyText: &body}, strings.Repeat("ن", 10_000))
	if len([]rune(input)) > 8192 || !strings.HasPrefix(input, "😀 "+strings.Repeat("ن", 3000)+" ") {
		t.Fatal("canonical input violated source caps or UTF-8")
	}
}

func TestEmbeddingCampaignRejectsEditedInput(t *testing.T) {
	db, mock := ownerSafetyDB(t)
	title := "before"
	item := models.ContentItem{PublicID: uuid.New(), Title: &title}
	hash, err := campaignInputFingerprint(db, &item, "content_text")
	if err != nil {
		t.Fatal(err)
	}
	evidence, _ := json.Marshal(map[string]string{"input_fingerprint": hash})
	correlation := &embeddingCampaignCorrelation{ActionID: 1}
	for _, changed := range []bool{false, true} {
		mock.ExpectQuery(`SELECT .*embedding_campaign_actions`).WillReturnRows(sqlmock.NewRows([]string{"id", "evidence"}).AddRow(1, evidence))
		if changed {
			other := "after"
			item.Title = &other
		}
		err := validateCampaignEmbeddingInput(db, &item, "content_text", correlation)
		if (err != nil) != changed {
			t.Fatalf("changed=%v error=%v", changed, err)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestEmbeddingStatusDistinguishesAvailabilityFromCurrency(t *testing.T) {
	db, mock := ownerSafetyDB(t)
	s, _ := EmbeddingSurfaceByKey("content_text")
	mock.ExpectQuery(`SELECT COUNT\(\*\) AS eligible`).WithArgs("space").WillReturnRows(sqlmock.NewRows([]string{"eligible", "current"}).AddRow(100, 25))
	eligible, current, err := embeddingAvailability(db, s, "space")
	if err != nil || eligible != 100 || current != 25 {
		t.Fatalf("availability: %d/%d %v", current, eligible, err)
	}
	if availabilityPercent(eligible, current, false) != float64(25) || availabilityPercent(0, 0, false) != nil || availabilityPercent(100, 25, true) != nil {
		t.Fatal("unknown availability presented as healthy")
	}
	mock.ExpectQuery(`SELECT COUNT\(\*\) AS eligible`).WillReturnError(errors.New("offline"))
	if _, _, err := embeddingAvailability(db, s, "space"); err == nil {
		t.Fatal("availability read failure ignored")
	}
}

func TestEmbeddingAdminUsesAuthenticatedPrincipal(t *testing.T) {
	c, _ := gin.CreateTestContext(nil)
	c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{UserID: "operator-123", Email: "operator@example.test"})
	if adminPrincipalLabel(c) != "operator@example.test" {
		t.Fatal("human attribution lost")
	}
	db, mock := ownerSafetyDB(t)
	mock.ExpectQuery(`INSERT INTO "audit_logs"`).WillReturnRows(sqlmock.NewRows([]string{"id", "public_id"}).AddRow(1, uuid.NewString()))
	if err := writeEmbeddingAdminAudit(c, db, "campaign_start", "123", map[string]any{"reason": "reviewed"}); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
