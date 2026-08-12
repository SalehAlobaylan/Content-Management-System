package routes

import (
	"content-management-system/src/utils"
	"net/http"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

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
