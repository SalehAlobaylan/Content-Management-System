package routes

import (
	"content-management-system/src/controllers"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// SetupFeedRoutes registers the feed API routes
func SetupFeedRoutes(group *gin.RouterGroup, db *gorm.DB) {
	// For You feed - audio/video content
	group.GET("/feed/foryou", controllers.GetForYouFeed)

	// News feed - magazine-style slides
	group.GET("/feed/news", controllers.GetNewsFeed)
}
