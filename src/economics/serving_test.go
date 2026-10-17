package economics

import (
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestServingCountersCanonicalizeIdentityAndCountFailedResponsesConcurrently(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Setenv("DEFAULT_TENANT_ID", "a")
	c := NewCollector()
	c.SetPolicies([]Policy{{TenantID: "a", CollectionEnabled: true}})
	app := gin.New()
	app.Use(c.Middleware())
	id := "AABBCCDD-0011-4233-8455-66778899AABB"
	app.GET("/api/v1/feed/pods", func(ctx *gin.Context) {
		ctx.Set("tenant_id", "a")
		ctx.Set("user_id", id)
		ctx.String(503, "retry")
	})
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			app.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/api/v1/feed/pods", nil))
		}()
	}
	wg.Wait()
	totals := map[string]int64{}
	for key, value := range c.values {
		if key.Subject != "account:"+strings.ToLower(id) || key.Variant != "error" || key.Operation != "pods_page" {
			t.Fatalf("incorrect attribution: %+v", key)
		}
		totals[key.Unit] += value
	}
	if totals["requests"] != 200 || totals["errors"] != 200 || totals["response_bytes"] != 1000 || c.lost != 0 {
		t.Fatal(totals, c.lost)
	}
}

func TestServingPolicyPausePurgesInFlightRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Setenv("DEFAULT_TENANT_ID", "a")
	c := NewCollector()
	c.SetPolicies([]Policy{{TenantID: "a", CollectionEnabled: true}})
	app := gin.New()
	app.Use(c.Middleware())
	app.GET("/api/v1/feed/pods", func(ctx *gin.Context) {
		c.SetPolicies([]Policy{{TenantID: "a", CollectionEnabled: false}})
		ctx.String(200, "done")
	})
	app.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/api/v1/feed/pods", nil))
	if len(c.values) != 0 || c.bytes != 0 {
		t.Fatal("paused request recreated counters")
	}
}
