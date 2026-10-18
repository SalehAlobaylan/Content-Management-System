package controllers

import (
	"content-management-system/src/models"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// These names are the versioned aggregate readiness contract, not deployment
// tuning. Missing capabilities are unknown evidence, never an empty success.
var systemTopologyCapabilities = map[string][]string{
	"aggregation_dispatcher":  {"intake-control"},
	"aggregation_receipt":     {"intake-control"},
	"aggregation_pipeline":    {"media-maintenance"},
	"aggregation_atomization": {"media-executor"},
	"news_processing":         {"news"},
	"pods_processing":         {"pods-control"},
	"pods_media_execution":    {"media-executor"},
	"legacy_drain":            {},
}

func systemTopologyObserved(body map[string]interface{}, now time.Time) bool {
	status := systemString(body["status"])
	capturedAt, err := time.Parse(time.RFC3339Nano, systemString(body["captured_at"]))
	if systemString(body["schema_version"]) != "aggregation-topology-readiness/v1" ||
		(status != "healthy" && status != "degraded") || !validSystemTopologyDigest(systemString(body["topology_digest"])) ||
		err != nil || now.Sub(capturedAt) > 45*time.Second || capturedAt.After(now.Add(5*time.Second)) {
		return false
	}
	roles, ok := body["roles"].(map[string]interface{})
	if !ok || len(roles) == 0 {
		return false
	}
	capabilities, ok := body["capabilities"].(map[string]interface{})
	if !ok || len(capabilities) == 0 {
		return false
	}
	degraded := false
	for name, raw := range roles {
		role, ok := raw.(map[string]interface{})
		if !ok {
			return false
		}
		ready, readyOK := role["ready"].(bool)
		required, requiredOK := role["required"].(bool)
		if !readyOK || !requiredOK || !systemStringList(role["reasons"]) {
			return false
		}
		if name != "legacy-drain" && !required {
			return false
		}
		degraded = degraded || required && !ready
	}
	for name, expectedRoles := range systemTopologyCapabilities {
		capability, ok := capabilities[name].(map[string]interface{})
		if !ok {
			return false
		}
		ready, ok := capability["ready"].(bool)
		if !ok || !systemStringList(capability["reasons"]) || !systemStringList(capability["required_roles"]) {
			return false
		}
		required := capability["required_roles"].([]interface{})
		if name == "legacy_drain" {
			legacy, ok := roles["legacy-drain"].(map[string]interface{})
			if !ok {
				return false
			}
			if legacy["required"] == true {
				expectedRoles = []string{"legacy-drain"}
			}
		}
		if len(required) != len(expectedRoles) {
			return false
		}
		expectedReady := true
		for i, roleName := range expectedRoles {
			if required[i] != roleName {
				return false
			}
			role, ok := roles[roleName].(map[string]interface{})
			if !ok {
				return false
			}
			expectedReady = expectedReady && role["ready"] == true
		}
		if ready != expectedReady {
			return false
		}
	}
	return (status == "degraded") == degraded
}

func systemStringList(value interface{}) bool {
	items, ok := value.([]interface{})
	if !ok {
		return false
	}
	for _, item := range items {
		if _, ok := item.(string); !ok {
			return false
		}
	}
	return true
}

func systemMLReadinessObserved(body map[string]interface{}) bool {
	status := systemString(body["status"])
	models, modelsOK := body["models"].(map[string]interface{})
	deps, depsOK := body["dependencies"].(map[string]interface{})
	if (status != "ok" && status != "not_ready") || !modelsOK || len(models) == 0 || !depsOK || len(deps) == 0 {
		return false
	}
	for _, values := range []map[string]interface{}{models, deps} {
		for _, value := range values {
			if _, ok := value.(bool); !ok {
				return false
			}
		}
	}
	if raw, exists := body["models_detail"]; exists {
		items, ok := raw.([]interface{})
		if !ok {
			return false
		}
		for _, raw := range items {
			item, ok := raw.(map[string]interface{})
			if !ok || systemString(item["name"]) == "" {
				return false
			}
			if _, ok := item["loaded"].(bool); !ok {
				return false
			}
		}
	}
	return true
}

// The signature excludes measurements (latency/time), and includes the
// observation subject and contract. A different failed capability must earn
// its own confirmation even when its service/verdict enum is unchanged.
func systemAnomalyIdentity(anomaly systemAnomaly) string {
	return anomaly.Key + ":" + systemEvidenceIdentity(anomaly.Evidence)
}

func systemEvidenceIdentity(evidence map[string]interface{}) string {
	// Normalize in-memory typed members exactly like persisted JSON members.
	rawEvidence, _ := json.Marshal(evidence)
	_ = json.Unmarshal(rawEvidence, &evidence)
	parts := []string{}
	if endpoint := systemString(evidence["endpoint_url"]); endpoint != "" {
		parts = append(parts, "endpoint:"+endpoint)
	}
	if probe, exists := evidence["service"]; exists {
		raw, _ := json.Marshal(probe)
		var value systemProbeResult
		if json.Unmarshal(raw, &value) == nil {
			parts = append(parts, "service:"+value.Name, "endpoint:"+value.EndpointURL, "contract:"+value.ContractIdentity)
			for _, dep := range value.Deps {
				if strings.HasPrefix(dep.Name, "capability:") && dep.Status == "unhealthy" {
					parts = append(parts, dep.Name)
				}
			}
		}
	}
	if members, ok := evidence["members"].([]interface{}); ok {
		for _, member := range members {
			value := asSystemRecord(member)
			parts = append(parts, fmt.Sprint(value["service"])+":"+systemEvidenceIdentity(asSystemRecord(value["evidence"])))
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, "|")
}

func systemRecoveryContractsMatch(episode models.SystemIncidentEpisode, current, previous []systemProbeResult) bool {
	required := []string{episode.RootService}
	switch episode.RootService {
	case "redis":
		required = []string{"aggregation", "media"}
	case "postgres":
		required = []string{"cms", "iam"}
	case "cms":
		required = []string{"aggregation", "enrichment", "media"}
	}
	for _, name := range required {
		var latest, old *systemProbeResult
		for i := range current {
			if current[i].Name == name {
				latest = &current[i]
			}
		}
		for i := range previous {
			if previous[i].Name == name {
				old = &previous[i]
			}
		}
		if latest == nil || old == nil || latest.EndpointURL != old.EndpointURL || latest.ContractIdentity != old.ContractIdentity {
			return false
		}
	}
	return true
}
