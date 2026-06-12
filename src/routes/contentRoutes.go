package routes

import (
	"content-management-system/src/controllers"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// SetupContentRoutes registers the content API routes
func SetupContentRoutes(group *gin.RouterGroup, db *gorm.DB) {
	// User-submitted content (JWT-authenticated). Registered BEFORE the
	// /content/:id catch-all so Gin matches the literal segments first.
	group.GET("/content/mine", controllers.UserAuthMiddleware(), controllers.GetMyContent)
	group.POST("/content/submit", controllers.UserAuthMiddleware(), controllers.SubmitUserContent)

	// Get a single content item by ID. OptionalUserAuth lets the per-user
	// interaction flags (is_liked / is_bookmarked) be derived from a verified
	// JWT rather than a spoofable ?user_id query param.
	group.GET("/content/:id", controllers.OptionalUserAuthMiddleware(), controllers.GetContentItem)

	// Comments on a content item (paginated, newest first). OptionalUserAuth
	// lets the is_mine flag be derived from the verified token.
	group.GET("/content/:id/comments", controllers.OptionalUserAuthMiddleware(), controllers.GetContentComments)

	// User-triggered transcription (JWT-authenticated, rate-limited)
	group.POST("/content/:id/transcribe", controllers.UserAuthMiddleware(), controllers.RequestTranscription)

	// User-triggered restore for archived items
	group.POST("/content/:id/request-restore", controllers.RequestRestore)
}
