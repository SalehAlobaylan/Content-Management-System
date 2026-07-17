package utils

import (
	"fmt"
	"net/http"
	"slices"
)

// MachinePrincipal is the non-human identity attached to an internal request.
// It deliberately names a service, rather than a deployment or a token, so a
// credential rotation cannot widen the service's authority.
type MachinePrincipal string

const (
	MachinePrincipalAggregation MachinePrincipal = "aggregation"
	MachinePrincipalEnrichment  MachinePrincipal = "enrichment"
	MachinePrincipalMedia       MachinePrincipal = "media"
	MachinePrincipalLegacy      MachinePrincipal = "legacy-shared"
)

const (
	InternalPrincipalContextKey    = "internal_machine_principal"
	InternalCredentialIDContextKey = "internal_credential_id"
)

// InternalRoutePolicy is the checked-in service ownership matrix. Routes must
// be registered through RegisterInternalRoute so this policy is both the route
// inventory and the authorization source of truth.
type InternalRoutePolicy struct {
	Method              string
	Path                string
	Capability          string
	Principals          []MachinePrincipal
	LegacySharedAllowed bool
}

// InternalRoutePolicies is intentionally explicit. New /internal routes must
// add a row here before they can be registered.
func InternalRoutePolicies() []InternalRoutePolicy {
	agg := []MachinePrincipal{MachinePrincipalAggregation}
	enrich := []MachinePrincipal{MachinePrincipalEnrichment}
	media := []MachinePrincipal{MachinePrincipalMedia}
	return []InternalRoutePolicy{
		{http.MethodPost, "/source-suggestions", "discovery.write", agg, true},
		{http.MethodGet, "/discovery/config", "discovery.read", agg, true},
		{http.MethodGet, "/discovery/profiles", "discovery.read", agg, true},
		{http.MethodGet, "/circulation/policy", "circulation.read", agg, true},
		{http.MethodPost, "/circulation/claim-sources", "circulation.write", agg, true},
		{http.MethodPost, "/circulation/source-runs", "circulation.write", agg, true},
		{http.MethodGet, "/intel/corpus-citations", "intel.read", agg, true},
		{http.MethodGet, "/intel/approved-source-pages", "intel.read", agg, true},
		{http.MethodGet, "/intel/approved-telegram-channels", "intel.read", agg, true},
		{http.MethodGet, "/intel/approved-twitter-handles", "intel.read", agg, true},
		{http.MethodGet, "/intel/approved-youtube-channels", "intel.read", agg, true},
		{http.MethodGet, "/intel/approved-podcast-feeds", "intel.read", agg, true},
		{http.MethodPost, "/intel/candidates", "intel.write", agg, true},
		{http.MethodGet, "/intel/candidates", "intel.read", agg, true},
		{http.MethodGet, "/content-items", "content.read", agg, true},
		{http.MethodPost, "/redundancy/precheck", "content.read", agg, true},
		{http.MethodGet, "/content-items/:id", "content.read", []MachinePrincipal{MachinePrincipalAggregation, MachinePrincipalEnrichment}, true},
		{http.MethodGet, "/atomization/candidates", "atomization.read", agg, true},
		{http.MethodPost, "/atomization/repair-leaks", "atomization.write", agg, true},
		{http.MethodGet, "/content-items/:id/atomization", "atomization.read", agg, true},
		{http.MethodPost, "/content-items", "content.ingest", agg, true},
		{http.MethodPut, "/content-items/:id", "content.ingest", agg, true},
		{http.MethodPatch, "/content-items/:id/status", "content.lifecycle", agg, true},
		{http.MethodPatch, "/content-items/:id/artifacts", "content.artifacts", agg, true},
		{http.MethodPost, "/content-items/:id/atomization/plan", "atomization.write", agg, true},
		{http.MethodPost, "/content-items/:id/atomization/children", "atomization.write", agg, true},
		{http.MethodPost, "/content-items/:id/atomization/runs", "atomization.write", agg, true},
		{http.MethodPost, "/content-items/:id/request-stt", "transcription.request", agg, true},
		{http.MethodPatch, "/content-items/:id/quality", "quality.write", agg, true},
		{http.MethodGet, "/content-items/missing-embedding", "content.read", agg, true},
		{http.MethodGet, "/storage/policies", "storage.read", agg, true},
		{http.MethodGet, "/storage/candidates", "storage.read", agg, true},
		{http.MethodPost, "/storage/archive", "storage.write", agg, true},
		{http.MethodPost, "/storage/move-to-cold", "storage.write", agg, true},
		{http.MethodPost, "/storage/sweep-runs", "storage.write", agg, true},
		{http.MethodPost, "/storage/artifact-events", "storage.write", agg, true},
		{http.MethodPost, "/storage/op-metrics", "storage.write", agg, true},
		{http.MethodGet, "/storage/op-budget", "storage.read", agg, true},
		{http.MethodGet, "/quality/profiles/resolve", "quality.read", agg, true},
		{http.MethodGet, "/quality/profiles/:id", "quality.read", agg, true},

		{http.MethodPatch, "/content-items/:id/embedding", "embedding.write", enrich, true},
		{http.MethodPatch, "/content-items/:id/enrichment-metadata", "enrichment.write", enrich, true},
		{http.MethodPost, "/ai-spend/events", "ai-spend.write", enrich, true},
		{http.MethodGet, "/content-items/:id/embeddings", "embedding.read", enrich, true},
		{http.MethodPost, "/content-items/knn", "embedding.search", enrich, true},
		{http.MethodPost, "/content-items/knn-sparse", "embedding.search", enrich, true},
		{http.MethodPost, "/content-items/batch-text", "content.read", enrich, true},
		{http.MethodGet, "/ai-spend/allowance", "ai-spend.read", enrich, true},

		{http.MethodPost, "/transcripts", "transcript.write", media, true},
		{http.MethodPatch, "/content-items/:id/transcript", "transcript.write", media, true},
		{http.MethodPatch, "/content-items/:id/image-embedding", "image-embedding.write", media, true},
		{http.MethodPatch, "/transcription-jobs/:id", "transcript.write", media, true},
		{http.MethodPost, "/transcription-jobs/:id/complete", "transcript.write", media, true},
	}
}

func FindInternalRoutePolicy(method, path string) (InternalRoutePolicy, bool) {
	for _, policy := range InternalRoutePolicies() {
		if policy.Method == method && policy.Path == path {
			return policy, true
		}
	}
	return InternalRoutePolicy{}, false
}

func MustInternalRoutePolicy(method, path string) InternalRoutePolicy {
	policy, ok := FindInternalRoutePolicy(method, path)
	if !ok {
		panic(fmt.Sprintf("internal route %s %s has no capability policy", method, path))
	}
	return policy
}

func (p InternalRoutePolicy) Allows(principal MachinePrincipal) bool {
	if principal == MachinePrincipalLegacy {
		return p.LegacySharedAllowed
	}
	return slices.Contains(p.Principals, principal)
}
