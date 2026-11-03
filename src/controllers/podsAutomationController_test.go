package controllers

import (
	"content-management-system/src/utils"
	"github.com/gin-gonic/gin"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPodsAutomationPermissionAndStrictRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		permission, body string
		want             int
	}{
		{"aggregation:read", `{"paused":true,"expected_version":"` + strings.Repeat("a", 64) + `"}`, 403},
		{"aggregation:manage", `{"paused":true}`, 400},
		{"aggregation:manage", `{"paused":false,"expected_version":"` + strings.Repeat("a", 64) + `","tenant_id":"other"}`, 400},
		{"aggregation:manage", `{"paused":false,"expected_version":"` + strings.Repeat("a", 64) + `"} {}`, 400},
	} {
		r := gin.New()
		r.Use(func(c *gin.Context) {
			c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: "tenant-a", UserID: "actor", Permissions: []string{tc.permission}})
		})
		r.PATCH("/automation", utils.RequireAdminPermission("aggregation", "manage"), UpdatePodsAutomation)
		w := httptest.NewRecorder()
		req := httptest.NewRequest("PATCH", "/automation", strings.NewReader(tc.body))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)
		if w.Code != tc.want {
			t.Fatalf("%s: got %d want %d", tc.body, w.Code, tc.want)
		}
	}
}
