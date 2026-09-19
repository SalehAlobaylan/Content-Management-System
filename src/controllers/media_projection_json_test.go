package controllers

import (
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestMediaProjectionLegacyTimestamp(t *testing.T) {
	var rows []mediaAtomizationPipelineItem
	err := decodeMediaProjectionJSON([]byte(`[{"updated_at":"2026-09-10T16:20:13.407613","manual_atomization_requested_at":"2026-09-10T19:20:13.407613+03:00","last_progress_at":null}]`), &rows)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 9, 10, 16, 20, 13, 407613000, time.UTC)
	if len(rows) != 1 || !rows[0].UpdatedAt.Equal(want) || !rows[0].ManualAtomizationRequestedAt.Equal(want) {
		t.Fatalf("timestamp changed: %+v", rows)
	}
	if err := decodeMediaProjectionJSON([]byte(`[{"updated_at":"not-a-date"}]`), &rows); err == nil {
		t.Fatal("malformed evidence must not be silently accepted")
	}
}

func TestMediaProjectionPublicationTimestamp(t *testing.T) {
	var rows []struct {
		PublicationAt *time.Time `json:"publication_at"`
	}
	err := decodeMediaProjectionJSON([]byte(`[{"publication_at":"2026-09-10T16:20:13"},{"publication_at":null},{"publication_at":"2026-09-10T16:20:13Z"}]`), &rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 || rows[0].PublicationAt == nil || rows[1].PublicationAt != nil || rows[2].PublicationAt == nil || !rows[0].PublicationAt.Equal(*rows[2].PublicationAt) {
		t.Fatalf("publication timestamps changed: %+v", rows)
	}
}

func TestMediaQueryFailureDoesNotPrescribeMigrations(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	mediaAtomizationQueryError(c, errors.New("invalid updated_at"))
	if recorder.Code != 500 || strings.Contains(recorder.Body.String(), "Apply the CMS migrations") || !strings.Contains(recorder.Body.String(), "invalid updated_at") {
		t.Fatalf("misclassified query failure: %s", recorder.Body.String())
	}
}
