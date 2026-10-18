package controllers

import (
	"content-management-system/src/models"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func systemTestTopology(failedRole string) map[string]interface{} {
	roles := map[string]interface{}{}
	for _, name := range []string{"intake-control", "news", "pods-control", "media-executor", "media-maintenance", "legacy-drain"} {
		roles[name] = map[string]interface{}{"required": name != "legacy-drain", "ready": name != failedRole && name != "legacy-drain", "reasons": []string{}}
	}
	caps := map[string]interface{}{}
	for name, required := range systemTopologyCapabilities {
		if required == nil {
			required = []string{}
		}
		ready := true
		for _, role := range required {
			ready = ready && role != failedRole
		}
		caps[name] = map[string]interface{}{"ready": ready, "required_roles": required, "reasons": []string{}}
	}
	status := "healthy"
	if failedRole != "" {
		status = "degraded"
	}
	raw, _ := json.Marshal(map[string]interface{}{"schema_version": "aggregation-topology-readiness/v1", "topology_digest": strings.Repeat("a", 64), "captured_at": time.Now().UTC().Format(time.RFC3339Nano), "status": status, "roles": roles, "capabilities": caps})
	var result map[string]interface{}
	_ = json.Unmarshal(raw, &result)
	return result
}

func TestSystemEvidenceRejectsMalformedTopology(t *testing.T) {
	for _, name := range []string{"valid", "valid degraded", "empty roles", "empty capabilities", "untyped ready", "missing capability", "missing role", "missing required roles", "inconsistent status", "stale"} {
		t.Run(name, func(t *testing.T) {
			body := systemTestTopology("")
			switch name {
			case "valid degraded":
				body = systemTestTopology("media-maintenance")
			case "empty roles":
				body["roles"] = map[string]interface{}{}
			case "empty capabilities":
				body["capabilities"] = map[string]interface{}{}
			case "untyped ready":
				asSystemRecord(asSystemRecord(body["capabilities"])["aggregation_pipeline"])["ready"] = "false"
			case "missing capability":
				delete(asSystemRecord(body["capabilities"]), "aggregation_dispatcher")
			case "missing role":
				delete(asSystemRecord(body["roles"]), "intake-control")
			case "missing required roles":
				delete(asSystemRecord(asSystemRecord(body["capabilities"])["aggregation_pipeline"]), "required_roles")
			case "inconsistent status":
				body["status"] = "degraded"
			case "stale":
				body["captured_at"] = time.Now().Add(-time.Minute).Format(time.RFC3339Nano)
			}
			if got := readinessAggregationBodyObserved(body); got != strings.HasPrefix(name, "valid") {
				t.Fatalf("observed=%v", got)
			}
		})
	}
}

func TestSystemEvidenceMediaUnknownCannotRecover(t *testing.T) {
	for _, payload := range []string{`{}`, `{"configured":true,"reachable":false,"worker_alive":false,"queued":0}`, `{"configured":true,"reachable":true,"worker_alive":"false","queued":0}`, `{"configured":true,"reachable":true,"worker_alive":true,"queued":-1}`} {
		t.Run(payload, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/health":
					_, _ = w.Write([]byte(`{"status":"ok"}`))
				case "/ready":
					_, _ = w.Write([]byte(`{"status":"ok","models":{"stt":true},"dependencies":{"cms":true}}`))
				case "/health/queue":
					_, _ = w.Write([]byte(payload))
				}
			}))
			defer server.Close()
			t.Setenv("MEDIA_BASE_URL", server.URL)
			probe := checkSystemMedia(context.Background())
			if probe.Status == "healthy" || len(probe.Verdicts) != 0 {
				t.Fatalf("unknown worker evidence became healthy/actionable: %+v", probe)
			}
			if systemEpisodeObservablyHealthy(models.SystemIncidentEpisode{RootService: "media"}, systemHealthSnapshot{Services: []systemProbeResult{probe}}) {
				t.Fatal("unknown worker evidence qualified recovery")
			}
		})
	}
}

func TestSystemEvidenceMalformedMLIsNotReadiness(t *testing.T) {
	for _, raw := range []string{`{"status":"ok","models":{},"dependencies":{}}`, `{"status":"ok","models":{"stt":"true"},"dependencies":{"cms":true}}`, `{"status":"ok","models":{"stt":true},"dependencies":{"cms":"true"}}`} {
		var body map[string]interface{}
		_ = json.Unmarshal([]byte(raw), &body)
		if readinessMLBodyObserved(body) {
			t.Fatalf("accepted %s", raw)
		}
	}
}

func TestSystemEvidenceRichModelsCannotEraseUnreadyModel(t *testing.T) {
	var body map[string]interface{}
	_ = json.Unmarshal([]byte(`{"status":"ok","models":{"stt":false},"dependencies":{"cms":true},"models_detail":[]}`), &body)
	if !hasUnloadedModels(mapSystemModels(body)) {
		t.Fatal("empty optional model detail erased required readiness failure")
	}
}

func TestSystemEvidenceCapabilityChangesResetQualification(t *testing.T) {
	makeAnomaly := func(capability string) systemAnomaly {
		return systemAnomaly{Key: "aggregation:dependency_down", Service: "aggregation", Verdict: models.SystemVerdictDependencyDown, Evidence: map[string]interface{}{"service": systemProbeResult{Name: "aggregation", EndpointURL: "http://aggregation", ContractIdentity: "topology/v1", Deps: []systemProbeDependency{{Name: "capability:" + capability, Status: "unhealthy"}}}}}
	}
	old, current := makeAnomaly("aggregation_pipeline"), makeAnomaly("aggregation_dispatcher")
	previous := []systemRunSnapshot{{Anomalies: []systemAnomaly{old}}}
	if len(confirmSystemAnomalies([]systemAnomaly{current}, previous, 2)) != 0 {
		t.Fatal("new capability reused old confirmation")
	}
	ep := models.SystemIncidentEpisode{RootService: "aggregation", Evidence: marshalAutopilotJSON(old.Evidence)}
	if !systemEpisodeScopeChanged(ep, current) || systemEpisodeScopeChanged(ep, old) {
		t.Fatal("scope change was lost or unchanged evidence churned")
	}
	currentProbe := systemProbeResult{Name: "aggregation", Status: "healthy", EndpointURL: "http://aggregation", ContractIdentity: "new topology"}
	oldProbe := currentProbe
	oldProbe.ContractIdentity = "old topology"
	if got := systemRecoverySamples(ep, systemHealthSnapshot{Services: []systemProbeResult{currentProbe}}, []systemRunSnapshot{{Services: []systemProbeResult{oldProbe}}, {Services: []systemProbeResult{oldProbe}}}, 3); got != 1 {
		t.Fatalf("contract change did not restart recovery: %d", got)
	}
	if got := systemRecoverySamples(ep, systemHealthSnapshot{Services: []systemProbeResult{currentProbe}}, []systemRunSnapshot{{Services: []systemProbeResult{currentProbe}}, {Services: []systemProbeResult{currentProbe}}}, 3); got != 3 {
		t.Fatalf("new contract cannot recover after fresh qualification: %d", got)
	}
}
