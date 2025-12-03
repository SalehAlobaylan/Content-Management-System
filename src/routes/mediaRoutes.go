package routes

import (
	"content-management-system/src/controllers"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func SetupMediaRoutes(router gin.IRouter, db *gorm.DB) {
	router.POST("/media", controllers.CreateMedia)
	router.GET("/media", controllers.GetMedia)
	router.GET("/media/:id", controllers.GetMedia)
	router.DELETE("/media/:id", controllers.DeleteMedia)
}
