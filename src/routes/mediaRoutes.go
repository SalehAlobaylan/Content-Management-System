package routes

import (
	"content-management-system/src/controllers"
	"content-management-system/src/utils"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func SetupMediaRoutes(router gin.IRouter, db *gorm.DB) {
	// Mutations require an admin JWT — previously these were unauthenticated,
	// letting anyone create/delete media records.
	admin := utils.AdminAuthMiddleware(db)
	router.POST("/media", admin, controllers.CreateMedia)
	router.GET("/media", controllers.GetMedia)
	router.GET("/media/:id", controllers.GetMedia)
	router.DELETE("/media/:id", admin, controllers.DeleteMedia)
}
