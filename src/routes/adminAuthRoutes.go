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
	adminGroup.GET("/content/:id", controllers.GetAdminContentItem)
	adminGroup.PATCH("/content/:id/status", controllers.UpdateContentStatus)
	adminGroup.POST("/content/bulk-delete", controllers.BulkDeleteContent)
}
