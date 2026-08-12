package routes

import (
	"testing"

	"github.com/gin-gonic/gin"
)

func TestMediaSupplyStatusRouteIsRegisteredReadOnly(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	SetupAdminAuthRoutes(router, nil)

	want := map[string]bool{
		"GET /admin/media/circulation/supply":              false,
		"POST /admin/media/circulation/supply/evaluate":    false,
		"GET /admin/media/circulation/supply/episodes":     false,
		"GET /admin/media/circulation/supply/episodes/:id": false,
		"GET /admin/media/circulation/supply/events":       false,
	}
	for _, route := range router.Routes() {
		key := route.Method + " " + route.Path
		if _, ok := want[key]; ok {
			want[key] = true
		}
		if route.Path == "/admin/media/circulation/supply" && route.Method != "GET" {
			t.Fatalf("supply status must remain a read-only route, found %s", route.Method)
		}
	}
	for route, found := range want {
		if !found {
			t.Fatalf("Media Supply route was not registered: %s", route)
		}
	}
}
