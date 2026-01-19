package routes

import (
	"content-management-system/src/controllers"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// SetupContentRoutes registers the content API routes
func SetupContentRoutes(group *gin.RouterGroup, db *gorm.DB) {
	// Get a single content item by ID
	group.GET("/content/:id", controllers.GetContentItem)
}
