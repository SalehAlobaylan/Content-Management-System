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
	MachinePrincipalIAM         MachinePrincipal = "iam"
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
	receiptProducers := []MachinePrincipal{MachinePrincipalAggregation, MachinePrincipalEnrichment, MachinePrincipalMedia}
	iam := []MachinePrincipal{MachinePrincipalIAM}
	return []InternalRoutePolicy{
		{http.MethodPost, "/source-suggestions", "discovery.write", agg, true},
		{http.MethodGet, "/discovery/config", "discovery.read", agg, true},
		{http.MethodGet, "/discovery/profiles", "discovery.read", agg, true},
		{http.MethodGet, "/circulation/policy", "circulation.read", agg, true},
		{http.MethodPost, "/circulation/claim-sources", "circulation.write", agg, true},
		{http.MethodPost, "/circulation/source-runs", "circulation.write", agg, true},
		{http.MethodPost, "/source-run-requests/:id/accepted", "circulation.write", agg, true},
		{http.MethodPost, "/source-run-receipts", "circulation.receipts.write", receiptProducers, false},
		{http.MethodPost, "/source-run-receipts/retain", "circulation.receipts.retain", receiptProducers, false},
		{http.MethodPost, "/source-run-receipts/delivered", "circulation.receipts.delivery", receiptProducers, false},
		{http.MethodPost, "/source-runs/claim", "source-runs.dispatch.claim", agg, false},
		{http.MethodPost, "/pipeline-repairs/claim", "pipeline-repairs.claim", agg, false},
		{http.MethodPost, "/pipeline-repairs/:id/begin", "pipeline-repairs.begin", agg, false},
		{http.MethodPost, "/pipeline-repairs/:id/heartbeat", "pipeline-repairs.heartbeat", agg, false},
		{http.MethodPost, "/pipeline-repairs/:id/terminal", "pipeline-repairs.terminal", agg, false},
		{http.MethodGet, "/pipeline-repairs/:id/cancellation", "pipeline-repairs.cancellation.read", agg, false},
		{http.MethodPost, "/artifact-coverage/media/claim", "artifact-coverage.media.claim", media, false},
		{http.MethodPost, "/artifact-coverage/media/:id/begin", "artifact-coverage.media.begin", media, false},
		{http.MethodPost, "/artifact-coverage/media/:id/heartbeat", "artifact-coverage.media.heartbeat", media, false},
		{http.MethodPost, "/artifact-coverage/media/:id/accepted", "artifact-coverage.media.accepted", media, false},
		{http.MethodPost, "/artifact-coverage/media/:id/uncertain", "artifact-coverage.media.uncertain", media, false},
		{http.MethodPost, "/content-stages/news/claim", "content-stages.news.claim", agg, false},
		{http.MethodPost, "/content-stages/pods/claim", "content-stages.pods.claim", agg, false},
		{http.MethodPost, "/content-stages/media/claim", "content-stages.media.claim", media, false},
		{http.MethodPost, "/content-stages/:id/begin", "content-stages.begin", []MachinePrincipal{MachinePrincipalAggregation, MachinePrincipalMedia}, false},
		{http.MethodPost, "/content-stages/:id/heartbeat", "content-stages.heartbeat", []MachinePrincipal{MachinePrincipalAggregation, MachinePrincipalMedia}, false},
		{http.MethodPost, "/content-stages/:id/accepted", "content-stages.accepted", []MachinePrincipal{MachinePrincipalAggregation, MachinePrincipalMedia}, false},
		{http.MethodPost, "/content-stages/:id/deferred", "content-stages.deferred", []MachinePrincipal{MachinePrincipalAggregation, MachinePrincipalMedia}, false},
		{http.MethodPost, "/content-stages/:id/uncertain", "content-stages.uncertain", []MachinePrincipal{MachinePrincipalAggregation, MachinePrincipalMedia}, false},
		{http.MethodPost, "/content-stages/:id/failed", "content-stages.failed", []MachinePrincipal{MachinePrincipalAggregation, MachinePrincipalMedia}, false},
		{http.MethodPost, "/content-stages/:id/atomization-not-required", "content-stages.atomization-not-required", []MachinePrincipal{MachinePrincipalAggregation}, false},
		{http.MethodGet, "/content-stages/items/:id/trace", "content-stages.trace.read", []MachinePrincipal{MachinePrincipalAggregation, MachinePrincipalEnrichment, MachinePrincipalMedia}, false},
		{http.MethodPost, "/atomization-work/claim", "atomization-work.claim", agg, false},
		{http.MethodPost, "/atomization-work/:id/begin", "atomization-work.begin", agg, false},
		{http.MethodPost, "/atomization-work/:id/heartbeat", "atomization-work.heartbeat", agg, false},
		{http.MethodPost, "/atomization-work/:id/checkpoint", "atomization-work.checkpoint", agg, false},
		{http.MethodPost, "/media-supply-actions/unit-adoptions/claim", "supply-actions.unit-adoption.claim", agg, false},
		{http.MethodPost, "/media-supply-actions/unit-adoptions/:action/prepare", "supply-actions.unit-adoption.prepare", agg, false},
		{http.MethodPost, "/media-supply-actions/unit-adoptions/:action/acknowledge", "supply-actions.unit-adoption.acknowledge", agg, false},
		{http.MethodPost, "/media-supply-actions/receipt-redeliveries/claim", "supply-actions.receipt-redelivery.claim", agg, false},
		{http.MethodPost, "/media-supply-actions/receipt-redeliveries/:action/prepare", "supply-actions.receipt-redelivery.prepare", agg, false},
		{http.MethodPost, "/media-supply-actions/receipt-redeliveries/:action/complete", "supply-actions.receipt-redelivery.complete", agg, false},
		{http.MethodPost, "/source-runs/:request/attempts/:attempt/units", "source-runs.units.authorize", agg, false},
		{http.MethodPost, "/source-runs/:request/attempts/:attempt/units/:unit/accepted", "source-runs.units.accept", agg, false},
		{http.MethodPost, "/source-runs/:request/attempts/:attempt/units/:unit/begin", "source-runs.units.begin", agg, false},
		{http.MethodPost, "/source-runs/:request/attempts/:attempt/units/:unit/heartbeat", "source-runs.units.heartbeat", agg, false},
		{http.MethodPost, "/source-runs/:request/attempts/:attempt/units/:unit/freeze", "source-runs.units.freeze", agg, false},
		{http.MethodPost, "/source-runs/:request/attempts/:attempt/units/:unit/upstream-observations", "source-runs.observations.write", agg, false},
		{http.MethodPost, "/source-runs/:request/attempts/:attempt/units/:unit/upstream-observations/:observation/disposition", "source-runs.observations.materialize", agg, false},
		{http.MethodPost, "/source-runs/:request/seal", "source-runs.manifest.seal", agg, false},
		{http.MethodPost, "/source-run-verification-tasks/claim", "source-runs.verification.claim", agg, false},
		{http.MethodPost, "/source-run-verification-tasks/claim-next", "source-runs.verification.claim_next", agg, false},
		{http.MethodPost, "/source-run-verification-tasks/:task/heartbeat", "source-runs.verification.heartbeat", agg, false},
		{http.MethodPost, "/source-run-verification-tasks/:task/terminal", "source-runs.verification.terminal", agg, false},
		{http.MethodPost, "/source-run-verification-tasks/:task/observe", "source-runs.verification.observe", agg, false},
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
		{http.MethodPost, "/storage/operation-sagas", "storage.write", agg, true},
		{http.MethodPost, "/storage/operation-sagas/:id/object-applied", "storage.write", agg, true},
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

		// IAM is the authority for account suspension. CMS stores only this
		// enforcement mirror so a previously-issued access JWT is rejected
		// immediately, without CMS taking ownership of user accounts.
		{http.MethodPut, "/auth/suspensions/:user_id", "auth-suspension.write", iam, false},
		{http.MethodDelete, "/auth/users/:user_id/product-data", "auth-deletion.write", iam, false},
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
