package routes

import (
	"content-management-system/src/controllers"
	"content-management-system/src/utils"
	"net/http"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// SetupInternalRoutes registers service-to-service routes from the capability
// matrix in utils.InternalRoutePolicies. A route cannot be added here without
// declaring its owning machine principal and capability first.
func SetupInternalRoutes(router *gin.Engine, db *gorm.DB) {
	internal := router.Group("/internal")
	internal.Use(utils.InternalAuthMiddleware())
	route := func(method, path string, handler gin.HandlerFunc) {
		policy := utils.MustInternalRoutePolicy(method, path)
		internal.Handle(method, path, utils.RequireInternalRoutePolicy(policy), handler)
	}

	route(http.MethodPost, "/source-suggestions", controllers.InternalCreateSourceSuggestions)
	route(http.MethodGet, "/discovery/config", controllers.InternalGetDiscoveryConfig)
	route(http.MethodGet, "/discovery/profiles", controllers.InternalListEnabledProfiles)
	route(http.MethodGet, "/circulation/policy", controllers.InternalGetCirculationPolicy)
	route(http.MethodPost, "/circulation/claim-sources", controllers.InternalClaimCirculationSources)
	route(http.MethodPost, "/circulation/source-runs", controllers.InternalReportSourceRun)

	route(http.MethodGet, "/intel/corpus-citations", controllers.InternalGetCorpusCitations)
	route(http.MethodGet, "/intel/approved-source-pages", controllers.InternalGetApprovedSourcePages)
	route(http.MethodGet, "/intel/approved-telegram-channels", controllers.InternalGetApprovedTelegramChannels)
	route(http.MethodGet, "/intel/approved-twitter-handles", controllers.InternalGetApprovedTwitterHandles)
	route(http.MethodGet, "/intel/approved-youtube-channels", controllers.InternalGetApprovedYouTubeChannels)
	route(http.MethodGet, "/intel/approved-podcast-feeds", controllers.InternalGetApprovedPodcastFeeds)
	route(http.MethodPost, "/intel/candidates", controllers.InternalUpsertCandidates)
	route(http.MethodGet, "/intel/candidates", controllers.InternalListCandidates)

	route(http.MethodGet, "/content-items", controllers.InternalListContentItems)
	route(http.MethodPost, "/redundancy/precheck", controllers.InternalRedundancyPrecheck)
	route(http.MethodGet, "/content-items/:id", controllers.InternalGetContentItem)
	route(http.MethodGet, "/atomization/candidates", controllers.InternalListAtomizationCandidates)
	route(http.MethodPost, "/atomization/repair-leaks", controllers.InternalRepairMediaAtomizationLeaks)
	route(http.MethodGet, "/content-items/:id/atomization", controllers.InternalGetAtomizationInput)
	route(http.MethodPost, "/content-items", controllers.InternalCreateContentItem)
	route(http.MethodPut, "/content-items/:id", controllers.InternalUpdateContentItem)
	route(http.MethodPatch, "/content-items/:id/enrichment-metadata", controllers.InternalMergeEnrichmentMetadata)
	route(http.MethodPatch, "/content-items/:id/status", controllers.InternalUpdateContentStatus)
	route(http.MethodPatch, "/content-items/:id/artifacts", controllers.InternalUpdateContentArtifacts)
	route(http.MethodPatch, "/content-items/:id/embedding", controllers.InternalUpdateContentEmbedding)
	route(http.MethodPatch, "/content-items/:id/image-embedding", controllers.InternalUpdateContentImageEmbedding)
	route(http.MethodPatch, "/content-items/:id/transcript", controllers.InternalLinkTranscript)
	route(http.MethodPost, "/content-items/:id/atomization/plan", controllers.InternalSaveAtomizationPlan)
	route(http.MethodPost, "/content-items/:id/atomization/children", controllers.InternalCreateAtomizedChildren)
	route(http.MethodPost, "/content-items/:id/atomization/runs", controllers.InternalReportAtomizationRun)
	route(http.MethodPost, "/content-items/:id/request-stt", controllers.InternalRequestSTT)
	route(http.MethodPatch, "/transcription-jobs/:id", controllers.InternalUpdateTranscriptionJob)
	route(http.MethodPost, "/transcription-jobs/:id/complete", controllers.InternalCompleteTranscriptionJob)

	route(http.MethodGet, "/content-items/:id/embeddings", controllers.InternalGetContentEmbeddings)
	route(http.MethodPost, "/content-items/knn", controllers.InternalKNNDense)
	route(http.MethodPost, "/content-items/knn-sparse", controllers.InternalKNNSparse)
	route(http.MethodPost, "/content-items/batch-text", controllers.InternalBatchText)
	route(http.MethodGet, "/content-items/missing-embedding", controllers.InternalListMissingEmbedding)
	route(http.MethodPost, "/transcripts", controllers.InternalCreateTranscript)
	route(http.MethodPost, "/ai-spend/events", controllers.InternalIngestAISpendEvents)
	route(http.MethodGet, "/ai-spend/allowance", controllers.InternalGetAISpendAllowance)

	route(http.MethodGet, "/storage/policies", controllers.InternalListStoragePolicies)
	route(http.MethodGet, "/storage/candidates", controllers.InternalListStorageCandidates)
	route(http.MethodPost, "/storage/archive", controllers.InternalArchiveItems)
	route(http.MethodPost, "/storage/move-to-cold", controllers.InternalMoveItemsToCold)
	route(http.MethodPost, "/storage/sweep-runs", controllers.InternalCreateSweepRun)
	route(http.MethodPost, "/storage/artifact-events", controllers.InternalRecordStorageArtifactEvent)
	route(http.MethodPost, "/storage/op-metrics", controllers.InternalWriteOpMetrics)
	route(http.MethodGet, "/storage/op-budget", controllers.InternalGetStorageOpBudget)

	route(http.MethodGet, "/quality/profiles/resolve", controllers.InternalResolveQualityProfile)
	route(http.MethodGet, "/quality/profiles/:id", controllers.InternalGetQualityProfile)
	route(http.MethodPatch, "/content-items/:id/quality", controllers.InternalUpdateContentItemQuality)
}
