package controllers

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"content-management-system/src/models"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

func TestEmbeddingConsoleReadFailuresStayUnavailable(t *testing.T) {
	cases := []struct {
		name    string
		prepare func(sqlmock.Sqlmock)
		handler gin.HandlerFunc
		message string
	}{
		{"audit row read", func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(`SELECT .*embedding_lifecycle_runs`).WillReturnError(errors.New("offline"))
		}, GetEmbeddingLifecycleRun, "audit history unavailable"},
		{"audit findings read", func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(`SELECT .*embedding_lifecycle_runs`).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(7))
			mock.ExpectQuery(`SELECT .*embedding_lifecycle_findings`).WillReturnError(errors.New("offline"))
		}, GetEmbeddingLifecycleRun, "audit evidence unavailable"},
		{"exception history read", func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(`SELECT .*embedding_campaign_exceptions`).WillReturnError(errors.New("offline"))
		}, ListEmbeddingCampaignExceptions, "campaign exceptions unavailable"},
		{"campaign row read", func(mock sqlmock.Sqlmock) {
			mock.ExpectQuery(`SELECT .*embedding_campaigns`).WillReturnError(errors.New("offline"))
		}, GetEmbeddingCampaign, "campaign history unavailable"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			db, mock := ownerSafetyDB(t)
			test.prepare(mock)
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Set("db", db)
			c.Params = gin.Params{{Key: "id", Value: "7"}}
			test.handler(c)
			if recorder.Code != http.StatusInternalServerError || !strings.Contains(recorder.Body.String(), test.message) {
				t.Fatalf("failed read returned %d: %s", recorder.Code, recorder.Body.String())
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestEmbeddingRecommendationPrioritizesCurrentWork(t *testing.T) {
	clear := &models.EmbeddingLifecycleRun{Headline: models.EmbeddingHeadlineAllClear}
	cases := []struct {
		name      string
		latest    *models.EmbeddingLifecycleRun
		campaigns []models.EmbeddingCampaign
		held      map[string]int64
		want      string
	}{
		{"blocked beats old all-clear and running", clear, []models.EmbeddingCampaign{{ID: 1, State: models.EmbeddingCampaignRunning}, {ID: 2, State: models.EmbeddingCampaignBlocked, BlockedReason: "owner unavailable"}}, nil, "Resolve the blocked campaign"},
		{"paused beats old all-clear", clear, []models.EmbeddingCampaign{{State: models.EmbeddingCampaignPaused}}, nil, "Review the paused campaign"},
		{"verifying beats old all-clear", clear, []models.EmbeddingCampaign{{State: models.EmbeddingCampaignVerifying}}, nil, "Monitor campaign verification"},
		{"held work beats old all-clear", clear, nil, map[string]int64{"relations": 3}, "Review held semantic work"},
		{"coherent idle", clear, nil, nil, "No action needed"},
		{"no evidence", nil, nil, nil, "Run an audit"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			got := embeddingRecommendation(test.latest, test.campaigns, test.held)
			if got["label"] != test.want {
				t.Fatalf("want %q, got %#v", test.want, got)
			}
		})
	}
}
