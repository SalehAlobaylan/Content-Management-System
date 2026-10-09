package controllers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"content-management-system/src/utils"
	"github.com/gin-gonic/gin"
)

func TestContentResetAdmissionClosedBeforeDatabaseAccess(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for name, handler := range map[string]gin.HandlerFunc{
		"create":           ContentResetDiscontinued,
		"advance-planning": ContentResetDiscontinued,
		"approve":          ContentResetDiscontinued,
	} {
		t.Run(name, func(t *testing.T) {
			for _, authenticated := range []bool{false, true} {
				router := gin.New()
				router.POST("/campaigns/:id", func(c *gin.Context) {
					if authenticated {
						c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{UserID: "admin", TenantID: "tenant-a"})
					}
					// No DB is supplied: admission must not read or mutate any ledger.
					handler(c)
				})
				response := httptest.NewRecorder()
				router.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/campaigns/old-or-invalid-id", strings.NewReader(`{"operation":"fresh_start"}`)))
				want := http.StatusUnauthorized
				if authenticated {
					want = http.StatusGone
					var body map[string]any
					if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || body["code"] != "content_reset_discontinued" {
						t.Fatalf("unexpected discontinuation response: %s", response.Body)
					}
				}
				if response.Code != want {
					t.Fatalf("authenticated=%v: got %d, want %d", authenticated, response.Code, want)
				}
			}
		})
	}
}

func TestContentResetCapabilitiesCannotReopenAdmission(t *testing.T) {
	response := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(response)
	c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{UserID: "admin", TenantID: "tenant-a"})
	GetContentResetCapabilities(c)
	var body struct {
		AdmissionClosed  bool `json:"admission_closed"`
		ExecutionEnabled bool `json:"execution_enabled"`
		Operations       map[string]struct {
			Preview bool
			Execute bool
		} `json:"operations"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || response.Code != http.StatusOK || !body.AdmissionClosed || body.ExecutionEnabled {
		t.Fatalf("unexpected capabilities: %s", response.Body)
	}
	for _, name := range []string{"clear", "fresh_start", "empty"} {
		operation, exists := body.Operations[name]
		if !exists || operation.Preview || operation.Execute {
			t.Fatalf("%s admission remains open", name)
		}
	}
}
