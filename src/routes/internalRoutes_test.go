package routes

import (
	"content-management-system/src/utils"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestLiteralUnitTransitionRoutesPassTheirState(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, resource := range []string{"atomization-chapter-units", "transcription-segments"} {
		for _, state := range []string{"running", "verifying", "verified", "deferred", "uncertain", "failed"} {
			t.Run(resource+"/"+state, func(t *testing.T) {
				router := gin.New()
				router.POST("/"+resource+"/:id/"+state, withTransitionState(state, func(c *gin.Context) {
					if c.Param("state") != state || c.Param("id") != "unit-id" {
						t.Fatalf("wrong transition parameters: %v", c.Params)
					}
					c.Status(http.StatusNoContent)
				}))
				response := httptest.NewRecorder()
				// Caller-controlled query state must not replace route authority.
				router.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/"+resource+"/unit-id/"+state+"?state=other", nil))
				if response.Code != http.StatusNoContent {
					t.Fatalf("unexpected status: %d", response.Code)
				}
			})
		}
	}
}

func TestInternalRoutesExactlyMatchCapabilityMatrix(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	SetupInternalRoutes(router, nil)

	registered := make(map[string]bool)
	for _, route := range router.Routes() {
		if !strings.HasPrefix(route.Path, "/internal/") {
			continue
		}
		registered[route.Method+" "+strings.TrimPrefix(route.Path, "/internal")] = true
	}
	for _, policy := range utils.InternalRoutePolicies() {
		key := policy.Method + " " + policy.Path
		if !registered[key] {
			t.Fatalf("policy route was not registered: %s", key)
		}
		delete(registered, key)
	}
	for key := range registered {
		t.Fatalf("registered internal route lacks policy: %s", key)
	}
}

func TestSourceRunDispatchRoutesAreAggregationOnlyAndRejectLegacyBridge(t *testing.T) {
	for _, path := range []string{
		"/source-runs/claim",
		"/media-supply-actions/unit-adoptions/claim",
		"/media-supply-actions/unit-adoptions/:action/prepare",
		"/media-supply-actions/unit-adoptions/:action/acknowledge",
		"/media-supply-actions/receipt-redeliveries/claim",
		"/media-supply-actions/receipt-redeliveries/:action/prepare",
		"/media-supply-actions/receipt-redeliveries/:action/complete",
		"/source-runs/:request/attempts/:attempt/units",
		"/source-runs/:request/attempts/:attempt/units/:unit/begin",
		"/source-runs/:request/attempts/:attempt/units/:unit/upstream-observations",
		"/source-runs/:request/attempts/:attempt/units/:unit/upstream-observations/:observation/disposition",
		"/source-run-verification-tasks/:task/terminal",
	} {
		policy, ok := utils.FindInternalRoutePolicy(http.MethodPost, path)
		if !ok || policy.LegacySharedAllowed || !policy.Allows(utils.MachinePrincipalAggregation) || policy.Allows(utils.MachinePrincipalMedia) {
			t.Fatalf("source-run route %s must be aggregation-only without the legacy bridge", path)
		}
	}
}
