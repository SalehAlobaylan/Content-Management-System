package routes

import (
	"content-management-system/src/controllers"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func SetupModerationRoutes(group *gin.RouterGroup, db *gorm.DB) {
	auth := controllers.UserAuthMiddleware()
	group.POST("/moderation/reports", auth, controllers.CreateModerationReport)
	group.POST("/moderation/blocks", auth, controllers.BlockAuthor)
	group.DELETE("/moderation/blocks/:authorID", auth, controllers.UnblockAuthor)
}
