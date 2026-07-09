package routes

import (
	"content-management-system/src/controllers"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func SetupPreferenceRoutes(group *gin.RouterGroup, db *gorm.DB) {
	auth := controllers.OptionalUserAuthMiddleware()

	group.GET("/topics/picker", controllers.GetTopicPicker)
	group.GET("/preferences", auth, controllers.GetPreferences)
	group.PUT("/preferences/topics", auth, controllers.PutPreferenceTopics)
	group.POST("/preferences/topics/:id/mute", auth, controllers.MutePreferenceTopic)
	group.DELETE("/preferences/topics/:id/mute", auth, controllers.UnmutePreferenceTopic)
}
