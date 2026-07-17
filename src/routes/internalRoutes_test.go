package routes

import (
	"content-management-system/src/utils"
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
