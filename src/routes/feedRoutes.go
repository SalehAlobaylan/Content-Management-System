package routes

import (
	"content-management-system/src/controllers"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// SetupFeedRoutes registers the feed API routes
func SetupFeedRoutes(group *gin.RouterGroup, db *gorm.DB) {
	// OptionalUserAuth populates user_id from a verified JWT when present so the
	// personalized feeds derive interaction flags / seen-filtering from the
	// token rather than a spoofable ?user_id query param.
	auth := controllers.OptionalUserAuthMiddleware()

	// For You feed - audio/video content
	group.GET("/feed/foryou", auth, controllers.GetForYouFeed)
	group.POST("/feed/foryou/sessions", auth, controllers.CreateForYouFeedSession)
	group.GET("/feed/foryou/sessions/:id", auth, controllers.GetForYouFeedSessionPage)

	// News feed - magazine-style slides
	group.GET("/feed/news", auth, controllers.GetNewsFeed)

	// Syndication output — ad-hoc (per-topic) feeds in 3 formats…
	group.GET("/feed/rss.xml", controllers.GetRSSFeed)
	group.GET("/feed/atom.xml", controllers.GetAtomFeed)
	group.GET("/feed/feed.json", controllers.GetJSONFeed)
	// …and saved, named feeds resolved by slug.
	group.GET("/feed/saved/:slug", controllers.GetSavedFeed)
}
