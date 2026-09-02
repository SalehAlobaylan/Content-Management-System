package supply

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func topologyJSON(now time.Time, status string, unavailable ...string) string {
	blocked := map[string]bool{}
	for _, capability := range unavailable {
		blocked[capability] = true
	}
	capabilities := ""
	for index, capability := range []string{
		"aggregation_dispatcher", "aggregation_receipt", "aggregation_pipeline",
		"aggregation_atomization", "news_processing", "pods_processing",
		"pods_media_execution", "legacy_drain",
	} {
		if index > 0 {
			capabilities += ","
		}
		roles := `["intake-control"]`
		if capability == "aggregation_pipeline" {
			roles = `["media-maintenance"]`
		} else if capability == "aggregation_atomization" || capability == "pods_media_execution" {
			roles = `["media-executor"]`
		} else if capability == "news_processing" {
			roles = `["news"]`
		} else if capability == "pods_processing" {
			roles = `["pods-control"]`
		} else if capability == "legacy_drain" {
			roles = `[]`
		}
		ready := !blocked[capability]
		reasons := `[]`
		if !ready {
			reasons = `["required role unavailable"]`
		}
		capabilities += fmt.Sprintf(`%q:{"ready":%t,"required_roles":%s,"reasons":%s}`, capability, ready, roles, reasons)
	}
	return fmt.Sprintf(`{"schema_version":"aggregation-topology-readiness/v1","topology_schema_version":"aggregation-role-topology/v1","topology_digest":"%s","captured_at":%q,"status":%q,"roles":{},"capabilities":{%s},"reasons":[]}`, strings.Repeat("a", 64), now.UTC().Format(time.RFC3339Nano), status, capabilities)
}

func aggregationTopologyServer(t *testing.T, body string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/readiness/topology" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Authorization") != "Bearer readiness-test" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(body))
	}))
}

func TestSupplyOwnerReadinessRequiresCapabilityProofs(t *testing.T) {
	now := time.Date(2026, time.August, 29, 10, 0, 0, 0, time.UTC)
	aggregation := aggregationTopologyServer(t, topologyJSON(now, "healthy"))
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
	t.Setenv("AGGREGATION_CMS_SERVICE_TOKEN", "readiness-test")
	t.Setenv("MEDIA_BASE_URL", media.URL)
	t.Setenv("ENRICHMENT_BASE_URL", enrichment.URL)
	observeSupplyOwners(now)
	observed := SupplyOwnerReadinessAt(now)
	for _, owner := range []string{
		"aggregation", "aggregation_dispatcher", "aggregation_receipt",
		"aggregation_pipeline", "aggregation_atomization", "media", "enrichment",
	} {
		if observed[owner].State != "ready" || !SupplyActionOwnerReady(owner, now) {
			t.Fatalf("%s should be ready, got %#v", owner, observed[owner])
		}
	}
}

func TestSupplyOwnerReadinessFailsClosedForMissingAndExpiredEvidence(t *testing.T) {
	now := time.Date(2026, time.August, 29, 10, 0, 0, 0, time.UTC)
	staleAfter := now.Add(time.Minute)
	restore := SetSupplyOwnerReadinessForTest(map[string]SupplyOwnerReadiness{
		"aggregation_atomization": {State: "ready", ObservedAt: &now, StaleAfter: &staleAfter},
		"media":                   {State: "stale", Detail: "Media ARQ worker is not live"},
	})
	defer restore()
	if SupplyActionOwnerReady("media", now) || SupplyActionOwnerReady("enrichment", now) {
		t.Fatal("stale or unobserved owners must block new handoffs")
	}
	if SupplyActionOwnerReady("aggregation_atomization", staleAfter.Add(time.Nanosecond)) {
		t.Fatal("an expired capability observation must block a new handoff")
	}
}

func TestMediaOwnerReadinessRequiresLiveWorker(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		queue string
	}{
		{name: "redis unavailable", queue: `{"reachable":false,"worker_alive":false}`},
		{name: "worker absent", queue: `{"reachable":true,"worker_alive":false}`},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/ready":
					_, _ = w.Write([]byte(`{"status":"ok"}`))
				case "/health/queue":
					_, _ = w.Write([]byte(testCase.queue))
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			t.Setenv("MEDIA_BASE_URL", server.URL)
			result := observeMediaSupplyOwner(time.Now().UTC())
			if result.State != "stale" {
				t.Fatalf("Media owner must fail closed, got %+v", result)
			}
		})
	}
}

func TestAggregationCapabilitiesDoNotShareUnrelatedRoleFailure(t *testing.T) {
	now := time.Now().UTC()
	server := aggregationTopologyServer(t, topologyJSON(now, "degraded", "aggregation_pipeline"))
	defer server.Close()
	t.Setenv("AGGREGATION_BASE_URL", server.URL)
	t.Setenv("AGGREGATION_CMS_SERVICE_TOKEN", "readiness-test")
	observed := observeAggregationSupplyOwners(now)
	if observed["aggregation"].State != "stale" || observed["aggregation_pipeline"].State != "stale" {
		t.Fatalf("global and pipeline readiness should be degraded: %#v", observed)
	}
	for _, capability := range []string{"aggregation_dispatcher", "aggregation_receipt", "aggregation_atomization"} {
		if observed[capability].State != "ready" {
			t.Fatalf("unrelated %s capability was incorrectly blocked: %#v", capability, observed[capability])
		}
	}
}

func TestAggregationTopologyFailsClosedForStaleOrMalformedEvidence(t *testing.T) {
	for _, body := range []string{
		topologyJSON(time.Now().UTC().Add(-time.Minute), "healthy"),
		`{"schema_version":"wrong"}`,
		strings.Replace(topologyJSON(time.Now().UTC(), "healthy"), strings.Repeat("a", 64), strings.Repeat("z", 64), 1),
	} {
		server := aggregationTopologyServer(t, body)
		t.Setenv("AGGREGATION_BASE_URL", server.URL)
		t.Setenv("AGGREGATION_CMS_SERVICE_TOKEN", "readiness-test")
		if observation := observeAggregationSupplyOwner(time.Now().UTC()); observation.State != "stale" {
			server.Close()
			t.Fatalf("expected stale observation, got %#v", observation)
		}
		server.Close()
	}
}

func TestAggregationOwnerProofDoesNotDependOnCMSReachability(t *testing.T) {
	now := time.Now().UTC()
	// The aggregate contract intentionally contains no CMS dependency. Its
	// capability proof was produced from owner_ready, not pod_ready.
	server := aggregationTopologyServer(t, topologyJSON(now, "degraded", "news_processing"))
	defer server.Close()
	t.Setenv("AGGREGATION_BASE_URL", server.URL)
	t.Setenv("AGGREGATION_CMS_SERVICE_TOKEN", "readiness-test")
	observed := observeAggregationSupplyOwners(now)
	if observed["aggregation_dispatcher"].State != "ready" {
		t.Fatalf("CMS availability must not invalidate cycle-free dispatcher proof: %#v", observed)
	}
}
