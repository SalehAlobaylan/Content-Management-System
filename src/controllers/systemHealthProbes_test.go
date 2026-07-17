package controllers

import (
	"content-management-system/src/models"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSystemDependencyStatusUsesExactContractValues(t *testing.T) {
	for _, value := range []string{"disconnected", "unreachable", "circuit_open", "not_ready", "false"} {
		if got := systemDependencyStatus(value); got != "unhealthy" {
			t.Fatalf("%q status = %q, want unhealthy", value, got)
		}
	}
	for _, value := range []string{"configured-but-unreachable", "unknown", ""} {
		if got := systemDependencyStatus(value); got != "unknown" {
			t.Fatalf("%q status = %q, want unknown", value, got)
		}
	}
}

func TestSystemHTTPProbeRejectsMalformedJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":`))
	}))
	defer server.Close()
	probe := systemHTTPProbe(context.Background(), server.URL, false)
	if probe.JSONObserved || !strings.Contains(probe.Error, "valid JSON") {
		t.Fatalf("malformed JSON probe = %+v", probe)
	}
}

func TestSystemAggregationProbeRequiresReadinessAndPreserves503Evidence(t *testing.T) {
	cases := []struct {
		name        string
		readyStatus int
		readyBody   string
		observed    bool
		wantVerdict string
	}{
		{name: "liveness only", readyStatus: http.StatusOK, readyBody: `{}`, observed: false},
		{name: "malformed readiness", readyStatus: http.StatusOK, readyBody: `{"status":`, observed: false},
		{name: "not ready with dependency evidence", readyStatus: http.StatusServiceUnavailable, readyBody: `{"status":"not_ready","dependencies":{"redis":"disconnected"}}`, observed: true, wantVerdict: models.SystemVerdictDependencyDown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/health":
					_, _ = w.Write([]byte(`{"status":"ok"}`))
				case "/ready":
					w.WriteHeader(tc.readyStatus)
					_, _ = w.Write([]byte(tc.readyBody))
				case "/internal/queues":
					if r.Header.Get("Authorization") != "Bearer system-probe-test" {
						http.Error(w, "unauthorized", http.StatusUnauthorized)
						return
					}
					_, _ = w.Write([]byte(`{"data":[]}`))
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			t.Setenv("AGGREGATION_BASE_URL", server.URL)
			t.Setenv("AGGREGATION_CMS_SERVICE_TOKEN", "system-probe-test")
			probe := checkSystemAggregation(context.Background())
			if probe.Status != "degraded" || probe.ReadinessObserved != tc.observed {
				t.Fatalf("aggregation probe = %+v", probe)
			}
			if tc.wantVerdict == "" {
				if len(probe.Verdicts) != 0 {
					t.Fatalf("unobserved readiness must not create a durable verdict: %+v", probe)
				}
				return
			}
			if len(probe.Verdicts) != 1 || probe.Verdicts[0] != tc.wantVerdict {
				t.Fatalf("verdicts = %+v, want %q", probe.Verdicts, tc.wantVerdict)
			}
			if len(probe.Deps) != 1 || probe.Deps[0].Status != "unhealthy" {
				t.Fatalf("503 readiness dependency evidence was lost: %+v", probe.Deps)
			}
		})
	}
}

func TestSystemMediaWorkerStallIsNeverDependencyDown(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case "/ready":
			_, _ = w.Write([]byte(`{"status":"ok","models":{"stt":true},"dependencies":{"cms":true}}`))
		case "/health/queue":
			_, _ = w.Write([]byte(`{"configured":true,"reachable":true,"worker_alive":false,"queued":3}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	t.Setenv("MEDIA_BASE_URL", server.URL)
	probe := checkSystemMedia(context.Background())
	if probe.Status != "degraded" || len(probe.Verdicts) != 1 || probe.Verdicts[0] != models.SystemVerdictWorkerStalled {
		t.Fatalf("worker stall probe = %+v", probe)
	}
	if isSystemHardDownVerdict(probe.Verdicts[0]) {
		t.Fatalf("worker stall must not be a containment input: %+v", probe)
	}
}

func TestSystemQueueProbeRejectsOversizedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer queue-probe-test" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(strings.Repeat("x", systemQueueProbeBodyLimit+1)))
	}))
	defer server.Close()
	t.Setenv("AGGREGATION_BASE_URL", server.URL)
	t.Setenv("AGGREGATION_CMS_SERVICE_TOKEN", "queue-probe-test")
	if _, err := fetchSystemAggregationQueueStats(context.Background()); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized queue response error = %v", err)
	}
}

func TestSystemMissingProbeIsAttentionOnly(t *testing.T) {
	probe := systemMissingProbe(systemProbeResult{Name: "media"}, "MEDIA_BASE_URL")
	if probe.Status != "unknown" || len(probe.Verdicts) != 0 {
		t.Fatalf("missing probe = %+v", probe)
	}
}

func TestSystemProbeFanoutKeepsOrderAndHonorsPhaseDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	services := collectSystemProbeResults(ctx, []systemProbeCheck{
		{name: "cms", check: func(context.Context) systemProbeResult { return systemProbeResult{Name: "cms", Status: "healthy"} }},
		{name: "slow", check: func(ctx context.Context) systemProbeResult {
			<-ctx.Done()
			return systemProbeResult{Name: "slow", Status: "unknown", RawError: ctx.Err().Error()}
		}},
		{name: "platform", check: func(context.Context) systemProbeResult { return systemProbeResult{Name: "platform", Status: "healthy"} }},
	})
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("phase fan-out exceeded bounded deadline: %s", elapsed)
	}
	if len(services) != 3 || services[0].Name != "cms" || services[1].Name != "slow" || services[2].Name != "platform" {
		t.Fatalf("probe result order changed: %+v", services)
	}
	if services[1].Status != "unknown" {
		t.Fatalf("timed-out probe = %+v", services[1])
	}
}
