package controllers

import (
	"content-management-system/src/utils"
	"github.com/gin-gonic/gin"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNewsCollectionPermissionAndStrictPreviewBoundary(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		permission, body string
		want             int
	}{
		{"source:read", `{"action":"enable","scope":"all"}`, 403},
		{"source:write", `{"action":"enable","scope":"all","tenant_id":"other"}`, 400},
		{"source:write", `{"action":"enable","scope":"selected","ids":["invalid"]}`, 400},
		{"source:write", `{"action":"enable","scope":"all"} {}`, 400},
	} {
		r := gin.New()
		r.Use(func(c *gin.Context) {
			c.Set(utils.AdminPrincipalContextKey, utils.AdminPrincipal{TenantID: "tenant-a", UserID: "actor", Permissions: []string{tc.permission}})
		})
		r.POST("/preview", utils.RequireAdminPermission("source", "write"), PreviewNewsSourceOperation)
		request := httptest.NewRequest("POST", "/preview", strings.NewReader(tc.body))
		request.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, request)
		if w.Code != tc.want {
			t.Fatalf("permission=%s body=%s got %d want %d", tc.permission, tc.body, w.Code, tc.want)
		}
	}
}
