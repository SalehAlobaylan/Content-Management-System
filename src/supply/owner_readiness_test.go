package supply

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSupplyOwnerReadinessRequiresStaticOwnerProofs(t *testing.T) {
	aggregation := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ready" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"status":"ready","dependencies":{"redis":"connected","cms":"reachable","workers":"healthy"}}`))
	}))
	defer aggregation.Close()
	media := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ready":
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case "/health/queue":
			_, _ = w.Write([]byte(`{"reachable":true,"worker_alive":true}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer media.Close()
	enrichment := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ready" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer enrichment.Close()

	t.Setenv("AGGREGATION_BASE_URL", aggregation.URL)
	t.Setenv("MEDIA_BASE_URL", media.URL)
	t.Setenv("ENRICHMENT_BASE_URL", enrichment.URL)
	now := time.Date(2026, time.August, 11, 10, 0, 0, 0, time.UTC)
	observeSupplyOwners(now)
	observed := SupplyOwnerReadinessAt(now)
	for _, owner := range []string{"aggregation", "media", "enrichment"} {
		if observed[owner].State != "ready" {
			t.Fatalf("%s should be ready, got %#v", owner, observed[owner])
		}
		if !SupplyActionOwnerReady(owner, now) {
			t.Fatalf("%s should admit a new handoff from fresh proof", owner)
		}
	}
	if !SupplyActionOwnerReady("aggregation_pipeline", now) || !SupplyActionOwnerReady("aggregation_atomization", now) {
		t.Fatal("Aggregation-specific protocols must use the static Aggregation readiness proof")
	}
}

func TestSupplyOwnerReadinessFailsClosedForMissingWorkerAndExpiredObservation(t *testing.T) {
	now := time.Date(2026, time.August, 11, 10, 0, 0, 0, time.UTC)
	staleAfter := now.Add(time.Minute)
	restore := SetSupplyOwnerReadinessForTest(map[string]SupplyOwnerReadiness{
		"aggregation": {State: "ready", ObservedAt: &now, StaleAfter: &staleAfter},
		"media":       {State: "stale", Detail: "Media ARQ worker is not live"},
	})
	defer restore()
	if SupplyActionOwnerReady("media", now) {
		t.Fatal("a stale Media owner must block a new action handoff")
	}
	if SupplyActionOwnerReady("enrichment", now) {
		t.Fatal("an unobserved owner must block a new action handoff")
	}
	if SupplyActionOwnerReady("aggregation", staleAfter.Add(time.Nanosecond)) {
		t.Fatal("an expired owner observation must block a new action handoff")
	}
}

func TestAggregationSupplyOwnerRejectsReadyWithoutHealthyWorkers(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":"ready","dependencies":{"redis":"connected","workers":"stale"}}`))
	}))
	defer server.Close()
	t.Setenv("AGGREGATION_BASE_URL", server.URL)
	if observation := observeAggregationSupplyOwner(time.Now().UTC()); observation.State != "stale" {
		t.Fatalf("expected stale observation, got %#v", observation)
	}
}

func TestAggregationSupplyOwnerDoesNotCreateCMSReadinessCycle(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":"not_ready","dependencies":{"redis":"connected","cms":"circuit_open","workers":"healthy"}}`))
	}))
	defer server.Close()
	t.Setenv("AGGREGATION_BASE_URL", server.URL)
	if observation := observeAggregationSupplyOwner(time.Now().UTC()); observation.State != "ready" {
		t.Fatalf("CMS circuit state must not invalidate independent Aggregation owner-worker proof: %#v", observation)
	}
}
