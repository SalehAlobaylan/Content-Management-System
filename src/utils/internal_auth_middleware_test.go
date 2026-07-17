package utils

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func internalPolicyTestRouter(policy InternalRoutePolicy) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(InternalAuthMiddleware())
	router.GET("/internal/test", RequireInternalRoutePolicy(policy), func(c *gin.Context) {
		principal, _ := GetMachinePrincipal(c)
		credentialID, _ := GetMachineCredentialID(c)
		c.JSON(http.StatusOK, gin.H{"principal": principal, "credential_id": credentialID})
	})
	return router
}

func performInternalRequest(router *gin.Engine, token string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/internal/test", nil)
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	router.ServeHTTP(recorder, request)
	return recorder
}

func TestInternalMachinePrincipalCapabilityMatrix(t *testing.T) {
	t.Setenv("ENV", "test")
	t.Setenv("CMS_AGGREGATION_SERVICE_TOKEN", "aggregation-current")
	t.Setenv("CMS_AGGREGATION_SERVICE_TOKEN_NEXT", "aggregation-next")
	t.Setenv("CMS_ENRICHMENT_SERVICE_TOKEN", "enrichment-current")
	t.Setenv("CMS_MEDIA_SERVICE_TOKEN", "media-current")
	t.Setenv("CMS_SERVICE_TOKEN", "legacy")

	policy := InternalRoutePolicy{
		Method: http.MethodGet, Path: "/test", Capability: "embedding.read",
		Principals: []MachinePrincipal{MachinePrincipalEnrichment}, LegacySharedAllowed: true,
	}
	router := internalPolicyTestRouter(policy)

	if got := performInternalRequest(router, "").Code; got != http.StatusUnauthorized {
		t.Fatalf("missing credential: got %d, want %d", got, http.StatusUnauthorized)
	}
	if got := performInternalRequest(router, "aggregation-current").Code; got != http.StatusForbidden {
		t.Fatalf("wrong principal: got %d, want %d", got, http.StatusForbidden)
	}
	if got := performInternalRequest(router, "enrichment-current").Code; got != http.StatusOK {
		t.Fatalf("correct principal: got %d, want %d", got, http.StatusOK)
	}
	if got := performInternalRequest(router, "aggregation-next").Code; got != http.StatusForbidden {
		t.Fatalf("rotated wrong principal: got %d, want %d", got, http.StatusForbidden)
	}
	if got := performInternalRequest(router, "legacy").Code; got != http.StatusOK {
		t.Fatalf("legacy bridge on existing route: got %d, want %d", got, http.StatusOK)
	}
}

func TestInternalLegacyCredentialIsTimeBoundInProduction(t *testing.T) {
	t.Setenv("ENV", "production")
	t.Setenv("CMS_SERVICE_TOKEN", "legacy")
	t.Setenv("CMS_LEGACY_SERVICE_TOKEN_UNTIL", time.Now().Add(-time.Minute).Format(time.RFC3339))
	if _, _, ok := authenticateMachineToken("Bearer legacy"); ok {
		t.Fatal("expired production legacy credential must be rejected")
	}
	t.Setenv("CMS_LEGACY_SERVICE_TOKEN_UNTIL", time.Now().Add(time.Minute).Format(time.RFC3339))
	principal, credentialID, ok := authenticateMachineToken("Bearer legacy")
	if !ok || principal != MachinePrincipalLegacy || credentialID != "legacy-shared/deprecated" {
		t.Fatalf("future production bridge not recognized: principal=%q credential=%q ok=%v", principal, credentialID, ok)
	}
}

func TestInternalPolicyTableHasUniqueExplicitRoutes(t *testing.T) {
	seen := make(map[string]bool)
	for _, policy := range InternalRoutePolicies() {
		if policy.Method == "" || policy.Path == "" || policy.Capability == "" || len(policy.Principals) == 0 {
			t.Fatalf("incomplete policy: %+v", policy)
		}
		key := policy.Method + " " + policy.Path
		if seen[key] {
			t.Fatalf("duplicate internal route policy %s", key)
		}
		seen[key] = true
		if resolved, ok := FindInternalRoutePolicy(policy.Method, policy.Path); !ok || resolved.Capability != policy.Capability {
			t.Fatalf("policy lookup failed for %s", key)
		}
	}
}
