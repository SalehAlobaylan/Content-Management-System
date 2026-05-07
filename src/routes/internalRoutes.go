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
	internal.POST("/content-items", controllers.InternalCreateContentItem)
	internal.PUT("/content-items/:id", controllers.InternalUpdateContentItem)
	internal.PATCH("/content-items/:id/status", controllers.InternalUpdateContentStatus)
	internal.PATCH("/content-items/:id/artifacts", controllers.InternalUpdateContentArtifacts)
	internal.PATCH("/content-items/:id/embedding", controllers.InternalUpdateContentEmbedding)
	internal.PATCH("/content-items/:id/transcript", controllers.InternalLinkTranscript)

	internal.POST("/transcripts", controllers.InternalCreateTranscript)

	// Storage management — used by Aggregation's storage worker
	internal.GET("/storage/policies", controllers.InternalListStoragePolicies)
	internal.GET("/storage/candidates", controllers.InternalListStorageCandidates)
	internal.POST("/storage/archive", controllers.InternalArchiveItems)
	internal.POST("/storage/move-to-cold", controllers.InternalMoveItemsToCold)
	internal.POST("/storage/sweep-runs", controllers.InternalCreateSweepRun)

	// Quality management — used by Aggregation's quality worker
	internal.GET("/quality/rules", controllers.InternalListQualityRules)
	internal.GET("/quality/profiles/default", controllers.InternalGetDefaultQualityProfile)
	internal.GET("/quality/profiles/:id", controllers.InternalGetQualityProfile)
	internal.GET("/quality/candidates", controllers.InternalListQualityCandidates)
	internal.POST("/quality/history", controllers.InternalWriteQualityHistory)
	internal.PATCH("/content-items/:id/quality", controllers.InternalUpdateContentItemQuality)
}
