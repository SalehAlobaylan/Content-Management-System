package routes

import (
	"content-management-system/src/controllers"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// SetupInteractionRoutes registers the user interaction API routes
func SetupInteractionRoutes(group *gin.RouterGroup, db *gorm.DB) {
	// Create a new interaction (like, bookmark, view, share, complete)
	group.POST("/interactions", controllers.CreateInteraction)

	// Get user's bookmarked content
	group.GET("/interactions/bookmarks", controllers.GetBookmarks)

	// Delete an interaction (unlike, unbookmark)
	group.DELETE("/interactions/:id", controllers.DeleteInteraction)
}
