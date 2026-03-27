package routes

import (
	"content-management-system/src/controllers"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// SetupTranscriptRoutes registers the transcript API routes
func SetupTranscriptRoutes(group *gin.RouterGroup, db *gorm.DB) {
	group.GET("/transcripts/:id", controllers.GetTranscript)
}
