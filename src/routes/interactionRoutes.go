package routes

import (
	"content-management-system/src/controllers"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// SetupInteractionRoutes registers the user interaction API routes.
//
// All routes run behind OptionalUserAuthMiddleware: when a valid JWT is present
// the acting identity is derived from the verified token (set as user_id in the
// context); anonymous callers fall back to a client-supplied session_id. The
// handlers NEVER trust a client-supplied user_id, which previously allowed any
// caller to read/delete/forge another user's interactions.
func SetupInteractionRoutes(group *gin.RouterGroup, db *gorm.DB) {
	auth := controllers.OptionalUserAuthMiddleware()

	// Create a new interaction (like, bookmark, view, share, complete)
	group.POST("/interactions", auth, controllers.CreateInteraction)

	// Get user's bookmarked content
	group.GET("/interactions/bookmarks", auth, controllers.GetBookmarks)

	// Get user's liked content
	group.GET("/interactions/likes", auth, controllers.GetLikes)

	// Aggregate profile counts (saved/likes/listened/created)
	group.GET("/interactions/stats", auth, controllers.GetUserStats)

	// Watch history — list and clear
	group.GET("/interactions/history", auth, controllers.GetWatchHistory)
	group.DELETE("/interactions/history", auth, controllers.DeleteWatchHistory)

	// Delete an interaction (unlike, unbookmark)
	group.DELETE("/interactions", auth, controllers.DeleteInteractionByContext)
	group.DELETE("/interactions/:id", auth, controllers.DeleteInteraction)
}
