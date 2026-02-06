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

	internal.POST("/content-items", controllers.InternalCreateContentItem)
	internal.PUT("/content-items/:id", controllers.InternalUpdateContentItem)
	internal.PATCH("/content-items/:id/status", controllers.InternalUpdateContentStatus)
	internal.PATCH("/content-items/:id/artifacts", controllers.InternalUpdateContentArtifacts)
	internal.PATCH("/content-items/:id/embedding", controllers.InternalUpdateContentEmbedding)
	internal.PATCH("/content-items/:id/transcript", controllers.InternalLinkTranscript)

	internal.POST("/transcripts", controllers.InternalCreateTranscript)
}
