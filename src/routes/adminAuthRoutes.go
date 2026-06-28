package routes

import (
	"content-management-system/src/controllers"
	"content-management-system/src/utils"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// SetupAdminAuthRoutes registers admin auth routes.
// Authentication is handled by IAM — CMS only validates IAM-issued JWTs.
// Authorization is enforced per route from the token's permission claims via
// utils.RequireAdminPermission (admin role bypasses; "resource:*"/"*:*" honored).
func SetupAdminAuthRoutes(router *gin.Engine, db *gorm.DB) {
	// perm is a short alias for the per-route permission gate. It runs after
	// AdminAuthMiddleware, which populates the AdminPrincipal in context.
	perm := utils.RequireAdminPermission

	adminGroup := router.Group("/admin")
	adminGroup.Use(utils.AdminAuthMiddleware(db))
	// /me only needs a valid principal (any authenticated user may read their own access).
	adminGroup.GET("/me", controllers.AdminMe)

	adminGroup.GET("/sources", perm("source", "read"), controllers.ListContentSources)
	adminGroup.POST("/sources", perm("source", "write"), controllers.CreateContentSource)
	adminGroup.POST("/sources/bulk", perm("source", "write"), controllers.BulkCreateContentSources)
	adminGroup.POST("/sources/discover", perm("source", "write"), controllers.DiscoverSourceFeeds)
	adminGroup.POST("/sources/preview", perm("source", "write"), controllers.PreviewSource)
	adminGroup.GET("/sources/podcast-search", perm("source", "write"), controllers.SearchPodcasts)
	adminGroup.GET("/sources/youtube-resolve", perm("source", "write"), controllers.ResolveYoutube)
	adminGroup.GET("/sources/stats", perm("source", "read"), controllers.GetSourceStats)
	adminGroup.GET("/sources/:id", perm("source", "read"), controllers.GetContentSource)
	adminGroup.PUT("/sources/:id", perm("source", "write"), controllers.UpdateContentSource)
	adminGroup.DELETE("/sources/:id", perm("source", "delete"), controllers.DeleteContentSource)
	adminGroup.POST("/sources/:id/run", perm("source", "write"), controllers.RunContentSource)

	// Feeds Finding — auto source discovery
	adminGroup.GET("/discovery/profiles", perm("source", "read"), controllers.ListDiscoveryProfiles)
	adminGroup.POST("/discovery/profiles", perm("source", "write"), controllers.CreateDiscoveryProfile)
	adminGroup.PUT("/discovery/profiles/:id", perm("source", "write"), controllers.UpdateDiscoveryProfile)
	adminGroup.DELETE("/discovery/profiles/:id", perm("source", "write"), controllers.DeleteDiscoveryProfile)
	adminGroup.POST("/discovery/profiles/:id/run", perm("source", "write"), controllers.RunDiscoveryProfile)
	adminGroup.POST("/discovery/suggest-profiles", perm("source", "write"), controllers.SuggestProfilesFromTopics)
	adminGroup.GET("/discovery/suggestions", perm("source", "read"), controllers.ListSourceSuggestions)
	adminGroup.POST("/discovery/suggestions/bulk-approve", perm("source", "write"), controllers.BulkApproveSuggestions)
	adminGroup.POST("/discovery/suggestions/bulk-reject", perm("source", "write"), controllers.BulkRejectSuggestions)
	adminGroup.POST("/discovery/suggestions/:id/approve", perm("source", "write"), controllers.ApproveSuggestion)
	adminGroup.POST("/discovery/suggestions/:id/reject", perm("source", "write"), controllers.RejectSuggestion)
	adminGroup.GET("/discovery/sources", perm("source", "read"), controllers.ListNewsSources)
	adminGroup.GET("/discovery/config", perm("source", "read"), controllers.GetDiscoveryConfig)
	adminGroup.PUT("/discovery/config", perm("source", "write"), controllers.UpdateDiscoveryConfig)
	adminGroup.POST("/discovery/sweep-now", perm("source", "write"), controllers.SweepNow)
	adminGroup.POST("/discovery/build-graph", perm("source", "write"), controllers.BuildGraph)
	adminGroup.POST("/discovery/import-youtube", perm("source", "write"), controllers.ImportYouTube)
	adminGroup.POST("/discovery/import-youtube-links", perm("source", "write"), controllers.ImportYouTubeLinks)
	adminGroup.GET("/discovery/authorities", perm("source", "read"), controllers.GetAuthorities)

	adminGroup.GET("/content", perm("content", "read"), controllers.ListContentItems)
	adminGroup.POST("/content", perm("content", "write"), controllers.CreateAdminContent)
	adminGroup.POST("/content/extract-url", perm("content", "write"), controllers.ExtractContentURL)
	adminGroup.POST("/content/import-feed", perm("content", "write"), controllers.ImportFeed)
	adminGroup.GET("/content/source-names", perm("content", "read"), controllers.ListDistinctSourceNames)
	adminGroup.GET("/content/status-counts", perm("content", "read"), controllers.GetStatusCounts)
	adminGroup.GET("/content/media-size-stats", perm("content", "read"), controllers.GetMediaSizeStats)
	adminGroup.GET("/content/stats", perm("content", "read"), controllers.GetContentStats)
	adminGroup.GET("/content/topics", perm("content", "read"), controllers.ListContentTopics)
	adminGroup.GET("/content/:id", perm("content", "read"), controllers.GetAdminContentItem)
	adminGroup.PATCH("/content/:id/status", perm("content", "write"), controllers.UpdateContentStatus)
	adminGroup.POST("/content/bulk-delete", perm("content", "delete"), controllers.BulkDeleteContent)
	adminGroup.POST("/content/bulk-status", perm("content", "write"), controllers.BulkStatusChange)
	adminGroup.POST("/content/bulk-tags", perm("content", "write"), controllers.BulkEditTags)
	adminGroup.POST("/content/bulk-topic", perm("content", "write"), controllers.BulkAssignTopic)

	// First-class topics (LLM-labeled) management
	adminGroup.PATCH("/topics/:id", perm("content", "write"), controllers.RenameTopic)
	adminGroup.DELETE("/topics/:id", perm("content", "write"), controllers.DeleteTopic)
	adminGroup.POST("/topics/merge", perm("content", "write"), controllers.MergeTopics)
	adminGroup.POST("/topics/reclassify", perm("content", "write"), controllers.ReclassifyTopics)
	adminGroup.POST("/topics/recluster", perm("content", "write"), controllers.ReclusterTopics)
	adminGroup.POST("/topics/label-batch", perm("content", "write"), controllers.LabelTopicsBatch)
	adminGroup.POST("/topics/summary-batch", perm("content", "write"), controllers.DigestTopicsBatch)

	// Saved syndication feeds (RSS/Atom/JSON output)
	adminGroup.GET("/feeds", perm("feed", "read"), controllers.ListRSSFeeds)
	adminGroup.POST("/feeds", perm("feed", "manage"), controllers.CreateRSSFeed)
	adminGroup.PUT("/feeds/:id", perm("feed", "manage"), controllers.UpdateRSSFeed)
	adminGroup.DELETE("/feeds/:id", perm("feed", "manage"), controllers.DeleteRSSFeed)

	// Intelligence — Modes
	adminGroup.GET("/intelligence/modes", perm("feed", "read"), controllers.GetModes)
	adminGroup.PUT("/intelligence/mode", perm("feed", "manage"), controllers.SetMode)

	// Intelligence — Ranking Config (advanced)
	adminGroup.GET("/intelligence/ranking", perm("feed", "read"), controllers.GetRankingConfig)
	adminGroup.PUT("/intelligence/ranking", perm("feed", "manage"), controllers.UpdateRankingConfig)

	// Intelligence — News-feed story snapshot (precompute mode) rebuild
	adminGroup.POST("/intelligence/news-snapshot/refresh", perm("feed", "manage"), controllers.RefreshNewsSnapshot)

	// News — Circulation Engine. Intelligence aliases remain for older Console builds.
	registerCirculationRoutes(adminGroup, "/news/circulation")
	registerCirculationRoutes(adminGroup, "/intelligence/circulation")

	// Media — Transcription/STT config (auto-STT toggle + budget cap)
	adminGroup.GET("/transcription-config", perm("content", "read"), controllers.GetTranscriptionConfig)
	adminGroup.PATCH("/transcription-config", perm("content", "write"), controllers.UpdateTranscriptionConfig)
	adminGroup.GET("/transcription/jobs", perm("content", "read"), controllers.ListTranscriptionJobs)
	adminGroup.POST("/transcription/jobs", perm("content", "write"), controllers.CreateTranscriptionJob)
	adminGroup.POST("/transcription/jobs/bulk", perm("content", "write"), controllers.BulkCreateTranscriptionJobs)
	adminGroup.POST("/transcription/batches", perm("content", "write"), controllers.CreateTranscriptionBatch)
	adminGroup.GET("/transcription/batches", perm("content", "read"), controllers.ListTranscriptionBatches)
	adminGroup.GET("/transcription/batches/:id", perm("content", "read"), controllers.GetTranscriptionBatch)
	adminGroup.POST("/transcription/batches/:id/cancel", perm("content", "write"), controllers.CancelTranscriptionBatch)
	adminGroup.GET("/transcription/quality", perm("content", "read"), controllers.ListTranscriptQuality)
	adminGroup.POST("/transcription/quality/repair-sweep", perm("content", "write"), controllers.RepairTranscriptionQualitySweep)

	// Media Atomization — operations dashboard and chapter review queue
	adminGroup.GET("/media-atomization/policy", perm("content", "read"), controllers.AdminGetMediaAtomizationPolicy)
	adminGroup.PATCH("/media-atomization/policy", perm("content", "write"), controllers.AdminUpdateMediaAtomizationPolicy)
	adminGroup.GET("/media-atomization/sources", perm("content", "read"), controllers.AdminListMediaAtomizationSources)
	adminGroup.PATCH("/media-atomization/sources/:id/policy", perm("content", "write"), controllers.AdminUpdateMediaAtomizationSourcePolicy)
	adminGroup.GET("/media-atomization/overview", perm("content", "read"), controllers.AdminGetMediaAtomizationOverview)
	adminGroup.GET("/media-atomization/feed-units", perm("content", "read"), controllers.AdminListMediaAtomizationFeedUnits)
	adminGroup.GET("/media-atomization/pipeline", perm("content", "read"), controllers.AdminGetMediaAtomizationPipeline)
	adminGroup.GET("/media-atomization/parents", perm("content", "read"), controllers.AdminListMediaAtomizationParents)
	adminGroup.GET("/media-atomization/chapters", perm("content", "read"), controllers.AdminListMediaAtomizationChapters)
	adminGroup.GET("/media-atomization/runs", perm("content", "read"), controllers.AdminListMediaAtomizationRuns)
	adminGroup.GET("/media-atomization/review", perm("content", "read"), controllers.AdminListAtomizationReview)
	adminGroup.POST("/media-atomization/repair-leaks", perm("content", "write"), controllers.AdminRepairMediaAtomizationLeaks)
	adminGroup.POST("/media-atomization/sweep-now", perm("content", "write"), controllers.AdminRunAtomizationSweepNow)
	adminGroup.PATCH("/media-atomization/parents/:id/override", perm("content", "write"), controllers.AdminUpdateMediaAtomizationParentOverride)
	adminGroup.POST("/media-atomization/parents/:id/atomize", perm("content", "write"), controllers.AdminAtomizeMediaParent)
	adminGroup.POST("/media-atomization/parents/:id/reatomize", perm("content", "write"), controllers.AdminReatomizeMediaParent)
	adminGroup.POST("/media-atomization/chapters/:chapter_id/approve", perm("content", "publish"), controllers.AdminApproveAtomizedChapter)
	adminGroup.POST("/media-atomization/chapters/:chapter_id/reject", perm("content", "publish"), controllers.AdminRejectAtomizedChapter)

	// Media Studio — per-item transcript + chapter editor
	adminGroup.GET("/content/:id/studio", perm("content", "read"), controllers.GetStudio)
	adminGroup.POST("/content/:id/chapters/generate", perm("content", "write"), controllers.GenerateChapters)
	adminGroup.PUT("/content/:id/chapters", perm("content", "write"), controllers.SaveChapters)
	adminGroup.PUT("/content/:id/transcript", perm("content", "write"), controllers.SaveTranscript)
	adminGroup.POST("/content/:id/transcript/approve", perm("content", "publish"), controllers.ApproveTranscript)
	adminGroup.DELETE("/content/:id/transcript/approve", perm("content", "publish"), controllers.UnapproveTranscript)
	adminGroup.GET("/content/:id/transcripts/compare", perm("content", "read"), controllers.CompareTranscripts)

	// Intelligence — Content Flags
	adminGroup.GET("/intelligence/flags", perm("content", "read"), controllers.ListContentFlags)
	adminGroup.GET("/intelligence/flags/:content_id", perm("content", "read"), controllers.GetContentFlag)
	adminGroup.PUT("/intelligence/flags/:content_id", perm("content", "write"), controllers.UpsertContentFlag)
	adminGroup.DELETE("/intelligence/flags/:content_id", perm("content", "write"), controllers.DeleteContentFlag)
	adminGroup.POST("/intelligence/flags/bulk", perm("content", "write"), controllers.BulkSetFlags)

	// Intelligence — Embeddings Explorer
	adminGroup.GET("/intelligence/embeddings/clusters", perm("content", "read"), controllers.GetEmbeddingClusters)
	adminGroup.GET("/intelligence/embeddings/similar/:content_id", perm("content", "read"), controllers.GetSimilarContent)
	adminGroup.GET("/intelligence/embeddings/stats", perm("content", "read"), controllers.GetEmbeddingStats)

	// Intelligence — Analytics
	adminGroup.GET("/intelligence/analytics/score-distribution", perm("content", "read"), controllers.GetScoreDistribution)
	adminGroup.GET("/intelligence/analytics/velocity", perm("content", "read"), controllers.GetVelocityLeaderboard)
	adminGroup.GET("/intelligence/analytics/trending", perm("content", "read"), controllers.GetTrendingItems)
	adminGroup.GET("/intelligence/analytics/source-performance", perm("content", "read"), controllers.GetSourcePerformance)
	adminGroup.GET("/intelligence/analytics/signal-health", perm("content", "read"), controllers.GetSignalHealth)

	// Intelligence — Feed Preview
	adminGroup.GET("/intelligence/preview/foryou", perm("content", "read"), controllers.PreviewForYouFeed)
	adminGroup.GET("/intelligence/preview/news", perm("content", "read"), controllers.PreviewNewsFeed)

	// Enrichment — On-demand enrichment management
	adminGroup.GET("/enrichment/stats", perm("content", "read"), controllers.GetEnrichmentStats)
	adminGroup.GET("/enrichment/missing-counts", perm("content", "read"), controllers.GetMissingEnrichmentCounts)
	adminGroup.GET("/enrichment/missing", perm("content", "read"), controllers.GetMissingEnrichments)
	adminGroup.POST("/enrichment/trigger/:id", perm("content", "write"), controllers.TriggerEnrichment)
	adminGroup.POST("/enrichment/trigger-batch", perm("content", "write"), controllers.TriggerBatchEnrichment)
	adminGroup.POST("/enrichment/trigger-all", perm("content", "write"), controllers.TriggerAllEnrichment)
	adminGroup.GET("/enrichment/bulk-status", perm("content", "read"), controllers.GetBulkEnrichStatus)
	adminGroup.GET("/enrichment/health", perm("content", "read"), controllers.GetEnrichmentServiceHealth)

	// Storage management
	adminGroup.GET("/storage/stats", perm("aggregation", "read"), controllers.GetStorageStats)
	adminGroup.GET("/storage/candidates", perm("aggregation", "read"), controllers.GetStorageCandidates)
	adminGroup.POST("/storage/purge", perm("aggregation", "manage"), controllers.PurgeStorage)
	adminGroup.POST("/storage/restore/:id", perm("aggregation", "manage"), controllers.RestoreStorageItem)
	adminGroup.GET("/storage/policy", perm("aggregation", "read"), controllers.GetStoragePolicy)
	adminGroup.PUT("/storage/policy", perm("aggregation", "manage"), controllers.UpdateStoragePolicy)
	adminGroup.POST("/storage/policy/run-now", perm("aggregation", "manage"), controllers.RunSweepNow)
	adminGroup.GET("/storage/preview", perm("aggregation", "read"), controllers.GetSweepPreview)
	adminGroup.GET("/storage/policy/overrides", perm("aggregation", "read"), controllers.ListStoragePolicyOverrides)
	adminGroup.DELETE("/storage/policy/overrides/:tenant_id", perm("aggregation", "manage"), controllers.DeleteStoragePolicyOverride)
	adminGroup.GET("/storage/sweep-runs", perm("aggregation", "read"), controllers.ListSweepRuns)
	adminGroup.POST("/storage/reconcile", perm("aggregation", "manage"), controllers.ReconcileStorage)
	adminGroup.GET("/storage/operations", perm("aggregation", "read"), controllers.GetStorageOperations)

	// Quality / Ingest configuration. Phase 7: this is now a pure config
	// surface (Profiles + Resolve preview + a one-shot Probe diagnostic).
	// Re-encoding old content moved to the Storage system as
	// archive_action='re_encode' — there are no rules/candidates/history
	// endpoints here anymore.
	adminGroup.GET("/quality/profiles", perm("content", "read"), controllers.ListQualityProfiles)
	adminGroup.POST("/quality/profiles", perm("content", "write"), controllers.CreateQualityProfile)
	adminGroup.PUT("/quality/profiles/:id", perm("content", "write"), controllers.UpdateQualityProfile)
	adminGroup.DELETE("/quality/profiles/:id", perm("content", "write"), controllers.DeleteQualityProfile)
	adminGroup.GET("/quality/profiles/resolve", perm("content", "read"), controllers.ResolveQualityProfile)
	adminGroup.POST("/quality/probe-item/:id", perm("content", "write"), controllers.ProbeContentItem)

	// Audit log — records admin-executed actions from Platform-Console.
	adminGroup.POST("/audit", perm("iam", "write"), controllers.CreateAuditLog)
	adminGroup.GET("/audit", perm("iam", "read"), controllers.ListAuditLogs)

	// Self-restart — exits the process so the supervisor restarts the service.
	// Super-admin only: not reachable via a granular permission grant.
	adminGroup.POST("/restart", utils.RequireAdminRole("admin"), controllers.RestartService)
}

func registerCirculationRoutes(adminGroup *gin.RouterGroup, prefix string) {
	perm := utils.RequireAdminPermission
	adminGroup.GET(prefix+"/policy", perm("feed", "read"), controllers.GetCirculationPolicy)
	adminGroup.PUT(prefix+"/policy", perm("feed", "manage"), controllers.UpdateCirculationPolicy)
	adminGroup.POST(prefix+"/presets/:preset", perm("feed", "manage"), controllers.ApplyCirculationPreset)
	adminGroup.GET(prefix+"/preview", perm("feed", "read"), controllers.PreviewCirculation)
	adminGroup.POST(prefix+"/preview", perm("feed", "read"), controllers.PreviewCirculation)
	adminGroup.GET(prefix+"/metrics", perm("feed", "read"), controllers.GetCirculationMetrics)
	adminGroup.GET(prefix+"/overrides", perm("feed", "read"), controllers.ListStoryOverrides)
	adminGroup.PUT(prefix+"/overrides/:story_id", perm("feed", "manage"), controllers.UpsertStoryOverride)
	adminGroup.DELETE(prefix+"/overrides/:story_id", perm("feed", "manage"), controllers.DeleteStoryOverride)
	adminGroup.GET(prefix+"/source-recommendations", perm("feed", "read"), controllers.ListSourceRecommendations)
	adminGroup.POST(prefix+"/source-recommendations/generate", perm("feed", "manage"), controllers.GenerateSourceRecommendations)
	adminGroup.POST(prefix+"/source-recommendations/:id/apply", perm("feed", "manage"), controllers.ApplySourceRecommendation)
	adminGroup.POST(prefix+"/sweep-now", perm("feed", "manage"), controllers.RunCirculationSweepNow)
	adminGroup.GET(prefix+"/autopilot/status", perm("feed", "read"), controllers.GetCirculationAutopilotStatus)
	adminGroup.PATCH(prefix+"/autopilot/settings", perm("feed", "manage"), controllers.UpdateCirculationAutopilotSettings)
	adminGroup.POST(prefix+"/autopilot/run", perm("feed", "manage"), controllers.RunCirculationAutopilotNow)
	adminGroup.POST(prefix+"/autopilot/boost", perm("feed", "manage"), controllers.BoostCirculationAutopilot)
	adminGroup.POST(prefix+"/autopilot/pause", perm("feed", "manage"), controllers.PauseCirculationAutopilot)
	adminGroup.GET(prefix+"/autopilot/runs", perm("feed", "read"), controllers.ListCirculationAutopilotRuns)
	adminGroup.GET(prefix+"/autopilot/runs/:id", perm("feed", "read"), controllers.GetCirculationAutopilotRun)
}
