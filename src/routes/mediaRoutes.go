package routes
import (
	"content-management-system/src/controllers"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func SetupMediaRoutes(router *gin.Engine, db *gorm.DB) {
	router.POST("/media", controllers.CreateMedia)
	router.GET("/media/:id", controllers.GetMedia)
	router.PUT("/media/:id", controllers.UpdateMedia)
	router.DELETE("/media/:id", controllers.DeleteMedia)
}

