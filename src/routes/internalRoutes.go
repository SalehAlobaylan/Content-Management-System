package routes

import (
	"content-management-system/src/controllers"
	"content-management-system/src/utils"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// SetupInternalRoutes registers internal service-to-service routes
func SetupInternalRoutes(router *gin.Engine, db *gorm.DB) {
	internal := router.Group("/internal")
	internal.Use(utils.InternalAuthMiddleware())

	// Feeds Finding — Aggregation posts discovered source candidates here,
	// and reads config + enabled profiles for scheduled sweeps
	internal.POST("/source-suggestions", controllers.InternalCreateSourceSuggestions)
	internal.GET("/discovery/config", controllers.InternalGetDiscoveryConfig)
	internal.GET("/discovery/profiles", controllers.InternalListEnabledProfiles)

	// News Circulation — Aggregation claims due news sources and reports run outcomes.
	internal.GET("/circulation/policy", controllers.InternalGetCirculationPolicy)
	internal.POST("/circulation/claim-sources", controllers.InternalClaimCirculationSources)
	internal.POST("/circulation/source-runs", controllers.InternalReportSourceRun)

	// Slice 4 — Source Intelligence Graph
	internal.GET("/intel/corpus-citations", controllers.InternalGetCorpusCitations)
	internal.GET("/intel/approved-source-pages", controllers.InternalGetApprovedSourcePages)
	internal.GET("/intel/approved-telegram-channels", controllers.InternalGetApprovedTelegramChannels)
	internal.GET("/intel/approved-twitter-handles", controllers.InternalGetApprovedTwitterHandles)
	internal.GET("/intel/approved-youtube-channels", controllers.InternalGetApprovedYouTubeChannels)
	internal.GET("/intel/approved-podcast-feeds", controllers.InternalGetApprovedPodcastFeeds)
	internal.POST("/intel/candidates", controllers.InternalUpsertCandidates)
	internal.GET("/intel/candidates", controllers.InternalListCandidates)

	internal.GET("/content-items", controllers.InternalListContentItems)
	internal.GET("/content-items/:id", controllers.InternalGetContentItem)
	internal.GET("/atomization/candidates", controllers.InternalListAtomizationCandidates)
	internal.POST("/atomization/repair-leaks", controllers.InternalRepairMediaAtomizationLeaks)
	internal.GET("/content-items/:id/atomization", controllers.InternalGetAtomizationInput)
	internal.POST("/content-items", controllers.InternalCreateContentItem)
	internal.PUT("/content-items/:id", controllers.InternalUpdateContentItem)
	internal.PATCH("/content-items/:id/status", controllers.InternalUpdateContentStatus)
	internal.PATCH("/content-items/:id/artifacts", controllers.InternalUpdateContentArtifacts)
	internal.PATCH("/content-items/:id/embedding", controllers.InternalUpdateContentEmbedding)
	internal.PATCH("/content-items/:id/image-embedding", controllers.InternalUpdateContentImageEmbedding)
	internal.PATCH("/content-items/:id/transcript", controllers.InternalLinkTranscript)
	internal.POST("/content-items/:id/atomization/plan", controllers.InternalSaveAtomizationPlan)
	internal.POST("/content-items/:id/atomization/children", controllers.InternalCreateAtomizedChildren)
	internal.POST("/content-items/:id/atomization/runs", controllers.InternalReportAtomizationRun)
	// Auto-STT path (Aggregation AI worker) — guard-enforced (toggle + budget).
	internal.POST("/content-items/:id/request-stt", controllers.InternalRequestSTT)
	internal.PATCH("/transcription-jobs/:id", controllers.InternalUpdateTranscriptionJob)
	internal.POST("/transcription-jobs/:id/complete", controllers.InternalCompleteTranscriptionJob)

	// Slice A hybrid retrieval — used by Enrichment-Service's /v1/related.
	// GET /:id/embeddings: fetch (dense, sparse) for an anchor.
	// POST /knn:           cosine kNN against `embedding` (1024-dim Qwen3-Embedding-0.6B dense).
	// POST /knn-sparse:    inner-product kNN against the legacy `embedding_sparse` (sparsevec) —
	//                      dead BGE-M3-era column, retained only for the legacy hybrid path.
	internal.GET("/content-items/:id/embeddings", controllers.InternalGetContentEmbeddings)
	internal.POST("/content-items/knn", controllers.InternalKNNDense)
	internal.POST("/content-items/knn-sparse", controllers.InternalKNNSparse)

	// Slice B — batch-fetch text for a small set of ids (typically the
	// post-RRF candidate pool that the cross-encoder reranker scores).
	internal.POST("/content-items/batch-text", controllers.InternalBatchText)

	// Reconciliation sweep — READY items still missing a dense embedding.
	internal.GET("/content-items/missing-embedding", controllers.InternalListMissingEmbedding)

	internal.POST("/transcripts", controllers.InternalCreateTranscript)

	// Storage management — used by Aggregation's storage worker
	internal.GET("/storage/policies", controllers.InternalListStoragePolicies)
	internal.GET("/storage/candidates", controllers.InternalListStorageCandidates)
	internal.POST("/storage/archive", controllers.InternalArchiveItems)
	internal.POST("/storage/move-to-cold", controllers.InternalMoveItemsToCold)
	internal.POST("/storage/sweep-runs", controllers.InternalCreateSweepRun)
	internal.POST("/storage/artifact-events", controllers.InternalRecordStorageArtifactEvent)
	internal.POST("/storage/op-metrics", controllers.InternalWriteOpMetrics)
	internal.GET("/storage/op-budget", controllers.InternalGetStorageOpBudget)

	// Quality / Ingest configuration — used by Aggregation media worker
	// (resolves the profile per ingest job) and the re-encode worker
	// invoked from Storage sweeps.
	internal.GET("/quality/profiles/resolve", controllers.InternalResolveQualityProfile)
	internal.GET("/quality/profiles/:id", controllers.InternalGetQualityProfile)
	internal.PATCH("/content-items/:id/quality", controllers.InternalUpdateContentItemQuality)
}
