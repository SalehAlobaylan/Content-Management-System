package routes

import (
	"content-management-system/src/controllers"
	"content-management-system/src/utils"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// SetupAdminAuthRoutes registers admin auth routes.
// Authentication is handled by IAM — CMS only validates IAM-issued JWTs.
func SetupAdminAuthRoutes(router *gin.Engine, db *gorm.DB) {
	adminGroup := router.Group("/admin")
	adminGroup.Use(utils.AdminAuthMiddleware(db))
	adminGroup.GET("/me", controllers.AdminMe)

	adminGroup.GET("/sources", controllers.ListContentSources)
	adminGroup.POST("/sources", controllers.CreateContentSource)
	adminGroup.POST("/sources/bulk", controllers.BulkCreateContentSources)
	adminGroup.POST("/sources/discover", controllers.DiscoverSourceFeeds)
	adminGroup.POST("/sources/preview", controllers.PreviewSource)
	adminGroup.GET("/sources/:id", controllers.GetContentSource)
	adminGroup.PUT("/sources/:id", controllers.UpdateContentSource)
	adminGroup.DELETE("/sources/:id", controllers.DeleteContentSource)
	adminGroup.POST("/sources/:id/run", controllers.RunContentSource)

	adminGroup.GET("/content", controllers.ListContentItems)
	adminGroup.GET("/content/source-names", controllers.ListDistinctSourceNames)
	adminGroup.GET("/content/status-counts", controllers.GetStatusCounts)
	adminGroup.GET("/content/:id", controllers.GetAdminContentItem)
	adminGroup.PATCH("/content/:id/status", controllers.UpdateContentStatus)
	adminGroup.POST("/content/bulk-delete", controllers.BulkDeleteContent)
	adminGroup.POST("/content/bulk-status", controllers.BulkStatusChange)

	// Intelligence — Modes
	adminGroup.GET("/intelligence/modes", controllers.GetModes)
	adminGroup.PUT("/intelligence/mode", controllers.SetMode)

	// Intelligence — Ranking Config (advanced)
	adminGroup.GET("/intelligence/ranking", controllers.GetRankingConfig)
	adminGroup.PUT("/intelligence/ranking", controllers.UpdateRankingConfig)

	// Intelligence — Content Flags
	adminGroup.GET("/intelligence/flags", controllers.ListContentFlags)
	adminGroup.GET("/intelligence/flags/:content_id", controllers.GetContentFlag)
	adminGroup.PUT("/intelligence/flags/:content_id", controllers.UpsertContentFlag)
	adminGroup.DELETE("/intelligence/flags/:content_id", controllers.DeleteContentFlag)
	adminGroup.POST("/intelligence/flags/bulk", controllers.BulkSetFlags)

	// Intelligence — Embeddings Explorer
	adminGroup.GET("/intelligence/embeddings/clusters", controllers.GetEmbeddingClusters)
	adminGroup.GET("/intelligence/embeddings/similar/:content_id", controllers.GetSimilarContent)
	adminGroup.GET("/intelligence/embeddings/stats", controllers.GetEmbeddingStats)

	// Intelligence — Analytics
	adminGroup.GET("/intelligence/analytics/score-distribution", controllers.GetScoreDistribution)
	adminGroup.GET("/intelligence/analytics/velocity", controllers.GetVelocityLeaderboard)
	adminGroup.GET("/intelligence/analytics/trending", controllers.GetTrendingItems)
	adminGroup.GET("/intelligence/analytics/source-performance", controllers.GetSourcePerformance)
	adminGroup.GET("/intelligence/analytics/signal-health", controllers.GetSignalHealth)

	// Intelligence — Feed Preview
	adminGroup.GET("/intelligence/preview/foryou", controllers.PreviewForYouFeed)
	adminGroup.GET("/intelligence/preview/news", controllers.PreviewNewsFeed)

	// Enrichment — On-demand enrichment management
	adminGroup.GET("/enrichment/stats", controllers.GetEnrichmentStats)
	adminGroup.GET("/enrichment/missing", controllers.GetMissingEnrichments)
	adminGroup.POST("/enrichment/trigger/:id", controllers.TriggerEnrichment)
	adminGroup.POST("/enrichment/trigger-batch", controllers.TriggerBatchEnrichment)
	adminGroup.GET("/enrichment/health", controllers.GetEnrichmentServiceHealth)

	// Storage management
	adminGroup.GET("/storage/stats", controllers.GetStorageStats)
	adminGroup.GET("/storage/candidates", controllers.GetStorageCandidates)
	adminGroup.POST("/storage/purge", controllers.PurgeStorage)
	adminGroup.POST("/storage/restore/:id", controllers.RestoreStorageItem)
	adminGroup.GET("/storage/policy", controllers.GetStoragePolicy)
	adminGroup.PUT("/storage/policy", controllers.UpdateStoragePolicy)
	adminGroup.POST("/storage/policy/run-now", controllers.RunSweepNow)
	adminGroup.GET("/storage/preview", controllers.GetSweepPreview)
	adminGroup.GET("/storage/policy/overrides", controllers.ListStoragePolicyOverrides)
	adminGroup.DELETE("/storage/policy/overrides/:tenant_id", controllers.DeleteStoragePolicyOverride)
	adminGroup.GET("/storage/sweep-runs", controllers.ListSweepRuns)
	adminGroup.POST("/storage/reconcile", controllers.ReconcileStorage)
	adminGroup.GET("/storage/operations", controllers.GetStorageOperations)

	// Quality / Ingest configuration. Phase 7: this is now a pure config
	// surface (Profiles + Resolve preview + a one-shot Probe diagnostic).
	// Re-encoding old content moved to the Storage system as
	// archive_action='re_encode' — there are no rules/candidates/history
	// endpoints here anymore.
	adminGroup.GET("/quality/profiles", controllers.ListQualityProfiles)
	adminGroup.POST("/quality/profiles", controllers.CreateQualityProfile)
	adminGroup.PUT("/quality/profiles/:id", controllers.UpdateQualityProfile)
	adminGroup.DELETE("/quality/profiles/:id", controllers.DeleteQualityProfile)
	adminGroup.GET("/quality/profiles/resolve", controllers.ResolveQualityProfile)
	adminGroup.POST("/quality/probe-item/:id", controllers.ProbeContentItem)
}
