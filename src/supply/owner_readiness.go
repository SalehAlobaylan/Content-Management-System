package supply

// This file is deliberately a small, static observer rather than a generic
// service-health client. Supply recovery has exactly three external owners:
// Aggregation dispatches source/pipeline/atomization work, Media owns its ARQ
// artifact worker, and Enrichment owns synchronous text/metadata recovery.
// The observer is cached process-local evidence. It never calls an owner on a
// Console request and it never retries or changes owner work.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	supplyOwnerReadinessInterval = 30 * time.Second
	supplyOwnerReadinessGrace    = 90 * time.Second
	supplyOwnerReadinessTimeout  = 3 * time.Second
	supplyOwnerReadinessBodyMax  = 64 << 10
)

// SupplyOwnerReadiness is intentionally evidence, not a promise that an
// owner will complete a future effect. A missing or stale observation blocks
// a new external handoff but never blocks cancellation or verification.
type SupplyOwnerReadiness struct {
	State      string     `json:"state"`
	ObservedAt *time.Time `json:"observed_at,omitempty"`
	StaleAfter *time.Time `json:"stale_after_at,omitempty"`
	Detail     string     `json:"detail,omitempty"`
}

type supplyOwnerReadinessSnapshot struct {
	owners map[string]SupplyOwnerReadiness
}

var supplyOwnerReadinessState = struct {
	sync.RWMutex
	snapshot supplyOwnerReadinessSnapshot
}{snapshot: supplyOwnerReadinessSnapshot{owners: map[string]SupplyOwnerReadiness{}}}

var supplyOwnerReadinessHTTPClient = &http.Client{Timeout: supplyOwnerReadinessTimeout}

// StartSupplyOwnerReadinessObserver is bounded process-local readiness. Its
// static endpoints are part of the service contract; callers cannot select a
// URL, route, queue, or owner at runtime.
func StartSupplyOwnerReadinessObserver() {
	observeSupplyOwners(time.Now().UTC())
	go func() {
		ticker := time.NewTicker(supplyOwnerReadinessInterval)
		defer ticker.Stop()
		for now := range ticker.C {
			observeSupplyOwners(now.UTC())
		}
	}()
}

func observeSupplyOwners(now time.Time) {
	observations := map[string]SupplyOwnerReadiness{
		"aggregation": observeAggregationSupplyOwner(now),
		"media":       observeMediaSupplyOwner(now),
		"enrichment":  observeEnrichmentSupplyOwner(now),
	}
	supplyOwnerReadinessState.Lock()
	supplyOwnerReadinessState.snapshot.owners = observations
	supplyOwnerReadinessState.Unlock()
}

// SupplyOwnerReadinessAt returns a snapshot with stale state derived at read
// time, so a stopped observer cannot leave a previously-ready owner green.
func SupplyOwnerReadinessAt(now time.Time) map[string]SupplyOwnerReadiness {
	supplyOwnerReadinessState.RLock()
	defer supplyOwnerReadinessState.RUnlock()
	result := make(map[string]SupplyOwnerReadiness, len(supplyOwnerReadinessState.snapshot.owners))
	for owner, item := range supplyOwnerReadinessState.snapshot.owners {
		if item.State == "ready" && item.StaleAfter != nil && now.UTC().After(*item.StaleAfter) {
			item.State = "stale"
			item.Detail = "owner readiness observation expired"
		}
		result[owner] = item
	}
	for _, owner := range []string{"aggregation", "media", "enrichment"} {
		if _, exists := result[owner]; !exists {
			result[owner] = SupplyOwnerReadiness{State: "not_started", Detail: "owner readiness has not been observed in this CMS process"}
		}
	}
	return result
}

// SupplyActionOwnerReady is deliberately strict for external owner effects.
// CMS-owned adapters use their own worker heartbeat; unknown owners are never
// admitted by the static registry and therefore fail closed here too.
func SupplyActionOwnerReady(owner string, now time.Time) bool {
	owner = strings.TrimSpace(owner)
	if owner == "cms" || owner == "cms_studio" {
		return SupplyActionWorkerHealthy(now)
	}
	// Protocol names remain intentionally specific in the action registry.
	// This map only identifies their static service authority for readiness; it
	// does not give one protocol permission to claim another protocol's work.
	switch owner {
	case "aggregation_dispatcher", "aggregation_receipt", "aggregation_pipeline", "aggregation_atomization":
		owner = "aggregation"
	}
	item, exists := SupplyOwnerReadinessAt(now)[owner]
	return exists && item.State == "ready"
}

func observeAggregationSupplyOwner(now time.Time) SupplyOwnerReadiness {
	body, err := getStaticSupplyOwnerJSON("AGGREGATION_BASE_URL", "/ready")
	if err != nil {
		return supplyOwnerNotReady(err)
	}
	// CMS needs proof that Aggregation's queue backbone and mandatory owner
	// workers can accept a handoff. It must not depend on Aggregation's view of
	// CMS health: doing so feeds CMS claim refusals back into Aggregation's CMS
	// circuit and creates a circular readiness lock.
	if nestedStringField(body, "dependencies", "redis") != "connected" || nestedStringField(body, "dependencies", "workers") != "healthy" {
		return supplyOwnerNotReady(fmt.Errorf("Aggregation /ready did not report a connected queue backbone and healthy mandatory workers"))
	}
	return supplyOwnerReady(now)
}

func observeMediaSupplyOwner(now time.Time) SupplyOwnerReadiness {
	ready, err := getStaticSupplyOwnerJSON("MEDIA_BASE_URL", "/ready")
	if err != nil {
		return supplyOwnerNotReady(err)
	}
	if stringField(ready, "status") != "ok" {
		return supplyOwnerNotReady(fmt.Errorf("Media /ready did not report ready"))
	}
	queue, err := getStaticSupplyOwnerJSON("MEDIA_BASE_URL", "/health/queue")
	if err != nil {
		return supplyOwnerNotReady(err)
	}
	if !boolField(queue, "reachable") || !boolField(queue, "worker_alive") {
		return supplyOwnerNotReady(fmt.Errorf("Media ARQ worker is not live"))
	}
	return supplyOwnerReady(now)
}

func observeEnrichmentSupplyOwner(now time.Time) SupplyOwnerReadiness {
	body, err := getStaticSupplyOwnerJSON("ENRICHMENT_BASE_URL", "/ready")
	if err != nil {
		return supplyOwnerNotReady(err)
	}
	if stringField(body, "status") != "ok" {
		return supplyOwnerNotReady(fmt.Errorf("Enrichment /ready did not report ready"))
	}
	return supplyOwnerReady(now)
}

func supplyOwnerReady(now time.Time) SupplyOwnerReadiness {
	observed := now.UTC()
	staleAfter := observed.Add(supplyOwnerReadinessGrace)
	return SupplyOwnerReadiness{State: "ready", ObservedAt: &observed, StaleAfter: &staleAfter}
}

func supplyOwnerNotReady(err error) SupplyOwnerReadiness {
	detail := "owner readiness unavailable"
	if err != nil {
		detail = err.Error()
	}
	return SupplyOwnerReadiness{State: "stale", Detail: detail}
}

func getStaticSupplyOwnerJSON(envKey, path string) (map[string]any, error) {
	base := strings.TrimRight(strings.TrimSpace(os.Getenv(envKey)), "/")
	if base == "" {
		return nil, fmt.Errorf("%s is not configured", envKey)
	}
	u, err := url.Parse(base)
	if err != nil || u.Scheme == "" || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("%s is not a valid service base URL", envKey)
	}
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, base+path, nil)
	if err != nil {
		return nil, fmt.Errorf("could not construct owner readiness request: %w", err)
	}
	response, err := supplyOwnerReadinessHTTPClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("owner readiness request failed: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("owner readiness returned HTTP %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, supplyOwnerReadinessBodyMax+1))
	if err != nil {
		return nil, fmt.Errorf("could not read owner readiness response: %w", err)
	}
	if len(body) > supplyOwnerReadinessBodyMax {
		return nil, fmt.Errorf("owner readiness response exceeds limit")
	}
	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("owner readiness response is malformed")
	}
	return result, nil
}

func stringField(body map[string]any, key string) string {
	value, _ := body[key].(string)
	return value
}

func boolField(body map[string]any, key string) bool {
	value, _ := body[key].(bool)
	return value
}

func nestedStringField(body map[string]any, parent, key string) string {
	nested, _ := body[parent].(map[string]any)
	return stringField(nested, key)
}

// SetSupplyOwnerReadinessForTest replaces cached observer evidence. It keeps
// unit tests hermetic; production code must use the static observer above.
func SetSupplyOwnerReadinessForTest(owners map[string]SupplyOwnerReadiness) func() {
	supplyOwnerReadinessState.Lock()
	previous := supplyOwnerReadinessState.snapshot.owners
	copy := make(map[string]SupplyOwnerReadiness, len(owners))
	for owner, item := range owners {
		copy[owner] = item
	}
	supplyOwnerReadinessState.snapshot.owners = copy
	supplyOwnerReadinessState.Unlock()
	return func() {
		supplyOwnerReadinessState.Lock()
		supplyOwnerReadinessState.snapshot.owners = previous
		supplyOwnerReadinessState.Unlock()
	}
}
