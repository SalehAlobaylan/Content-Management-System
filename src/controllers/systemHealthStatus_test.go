package controllers

import (
	"content-management-system/src/utils"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestSystemHistoryReadFailureIsUnavailable(t *testing.T) {
	for _, name := range []string{"episodes", "runs"} {
		t.Run(name, func(t *testing.T) {
			db, mock := newMockGorm(t)
			if name == "episodes" {
				mock.ExpectQuery(`SELECT .*system_incident_episodes`).WillReturnError(errors.New("read failed"))
			} else {
				mock.ExpectQuery(`SELECT .*system_autopilot_runs`).WillReturnError(errors.New("read failed"))
			}
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest(http.MethodGet, "/", nil)
			c.Set("db", db)
			c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{UserID: "admin", TenantID: "default"})
			if name == "episodes" {
				ListSystemIncidentEpisodes(c)
			} else {
				ListSystemAutopilotRuns(c)
			}
			if w.Code != http.StatusServiceUnavailable {
				t.Fatalf("read failure became empty history: %d %s", w.Code, w.Body.String())
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSystemDetailReadFailureIsUnavailable(t *testing.T) {
	for _, name := range []string{"episodes", "runs"} {
		t.Run(name, func(t *testing.T) {
			db, mock := newMockGorm(t)
			if name == "episodes" {
				mock.ExpectQuery(`SELECT .*system_incident_episodes`).WillReturnError(errors.New("read failed"))
			} else {
				mock.ExpectQuery(`SELECT .*system_autopilot_runs`).WillReturnError(errors.New("read failed"))
			}
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest(http.MethodGet, "/", nil)
			c.Params = gin.Params{{Key: "id", Value: "11111111-1111-4111-8111-111111111111"}}
			c.Set("db", db)
			c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{UserID: "admin", TenantID: "default"})
			if name == "episodes" {
				GetSystemIncidentEpisode(c)
			} else {
				GetSystemAutopilotRun(c)
			}
			if w.Code != http.StatusServiceUnavailable {
				t.Fatalf("storage outage became a missing record: %d %s", w.Code, w.Body.String())
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
