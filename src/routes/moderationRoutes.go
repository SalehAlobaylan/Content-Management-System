package routes

import (
	"content-management-system/src/controllers"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func SetupModerationRoutes(group *gin.RouterGroup, db *gorm.DB) {
	// A report can come from a verified account or an opaque app installation.
	// Optional auth lets the controller derive the former from a valid JWT while
	// never trusting a user ID supplied by the client.
	group.POST("/moderation/reports", controllers.OptionalUserAuthMiddleware(), controllers.CreateModerationReport)
	auth := controllers.UserAuthMiddleware()
	group.POST("/moderation/blocks", auth, controllers.BlockAuthor)
	group.DELETE("/moderation/blocks/:authorID", auth, controllers.UnblockAuthor)
}
