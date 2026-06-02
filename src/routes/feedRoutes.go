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

	// Syndication output — ad-hoc (per-topic) feeds in 3 formats…
	group.GET("/feed/rss.xml", controllers.GetRSSFeed)
	group.GET("/feed/atom.xml", controllers.GetAtomFeed)
	group.GET("/feed/feed.json", controllers.GetJSONFeed)
	// …and saved, named feeds resolved by slug.
	group.GET("/feed/saved/:slug", controllers.GetSavedFeed)
}
