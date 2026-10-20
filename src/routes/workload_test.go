package routes

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"content-management-system/src/utils"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

func TestWorkloadRoutesRequirePlatformAdminEvenWithWildcardPermissions(t *testing.T) {
	t.Setenv("JWT_SECRET", "workload-route-test-secret")
	t.Setenv("JWT_ALLOWED_ISSUERS", "iam-authorization-service")
	t.Setenv("JWT_ALLOWED_AUDIENCES", "platform-console")
	gin.SetMode(gin.TestMode)
	router := gin.New()
	SetupAdminAuthRoutes(router, nil)
	tokenFor := func(role string) string {
		claims := utils.JWTClaims{UserID: "actor-1", Email: "operator@example.test", TenantID: "tenant-a", Role: role, Permissions: []string{"*:*"}, RegisteredClaims: jwt.RegisteredClaims{Subject: "actor-1", Issuer: "iam-authorization-service", Audience: []string{"platform-console"}, ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))}}
		token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte("workload-route-test-secret"))
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	for _, endpoint := range []struct{ method, path string }{
		{"GET", "/admin/workload/status"}, {"GET", "/admin/workload/policy"}, {"GET", "/admin/workload/registry"}, {"GET", "/admin/workload/qualification"}, {"GET", "/admin/workload/permits"}, {"GET", "/admin/workload/decisions"}, {"GET", "/admin/workload/snapshots"}, {"PUT", "/admin/workload/policy"}, {"POST", "/admin/workload/pause"}, {"POST", "/admin/workload/permits/11111111-1111-4111-8111-111111111111/revoke"},
	} {
		for _, authorized := range []bool{false, true} {
			request := httptest.NewRequest(endpoint.method, endpoint.path, nil)
			want := http.StatusUnauthorized
			if authorized {
				request.Header.Set("Authorization", "Bearer "+tokenFor("editor"))
				want = http.StatusForbidden
			}
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)
			if response.Code != want {
				t.Fatalf("%s %s: expected %d, got %d", endpoint.method, endpoint.path, want, response.Code)
			}
		}
	}
	request := httptest.NewRequest(http.MethodGet, "/admin/workload/registry", nil)
	request.Header.Set("Authorization", "Bearer "+tokenFor("admin"))
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("verified admin registry denied: %d %s", response.Code, response.Body.String())
	}
}
