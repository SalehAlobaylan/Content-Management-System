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

	internal.GET("/content-items", controllers.InternalListContentItems)
	internal.GET("/content-items/:id", controllers.InternalGetContentItem)
	internal.POST("/content-items", controllers.InternalCreateContentItem)
	internal.PUT("/content-items/:id", controllers.InternalUpdateContentItem)
	internal.PATCH("/content-items/:id/status", controllers.InternalUpdateContentStatus)
	internal.PATCH("/content-items/:id/artifacts", controllers.InternalUpdateContentArtifacts)
	internal.PATCH("/content-items/:id/embedding", controllers.InternalUpdateContentEmbedding)
	internal.PATCH("/content-items/:id/image-embedding", controllers.InternalUpdateContentImageEmbedding)
	internal.PATCH("/content-items/:id/transcript", controllers.InternalLinkTranscript)

	// Slice A hybrid retrieval — used by Enrichment-Service's /v1/related.
	// GET /:id/embeddings: fetch (dense, sparse) for an anchor.
	// POST /knn:           cosine kNN against `embedding` (1024-dim BGE-M3 dense).
	// POST /knn-sparse:    inner-product kNN against `embedding_sparse` (sparsevec).
	internal.GET("/content-items/:id/embeddings", controllers.InternalGetContentEmbeddings)
	internal.POST("/content-items/knn", controllers.InternalKNNDense)
	internal.POST("/content-items/knn-sparse", controllers.InternalKNNSparse)

	// Slice B — batch-fetch text for a small set of ids (typically the
	// post-RRF candidate pool that the cross-encoder reranker scores).
	internal.POST("/content-items/batch-text", controllers.InternalBatchText)

	internal.POST("/transcripts", controllers.InternalCreateTranscript)

	// Storage management — used by Aggregation's storage worker
	internal.GET("/storage/policies", controllers.InternalListStoragePolicies)
	internal.GET("/storage/candidates", controllers.InternalListStorageCandidates)
	internal.POST("/storage/archive", controllers.InternalArchiveItems)
	internal.POST("/storage/move-to-cold", controllers.InternalMoveItemsToCold)
	internal.POST("/storage/sweep-runs", controllers.InternalCreateSweepRun)
	internal.POST("/storage/op-metrics", controllers.InternalWriteOpMetrics)
	internal.GET("/storage/op-budget", controllers.InternalGetStorageOpBudget)

	// Quality / Ingest configuration — used by Aggregation media worker
	// (resolves the profile per ingest job) and the re-encode worker
	// invoked from Storage sweeps.
	internal.GET("/quality/profiles/resolve", controllers.InternalResolveQualityProfile)
	internal.GET("/quality/profiles/:id", controllers.InternalGetQualityProfile)
	internal.PATCH("/content-items/:id/quality", controllers.InternalUpdateContentItemQuality)
}
