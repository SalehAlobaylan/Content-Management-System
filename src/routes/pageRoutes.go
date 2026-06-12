package routes

import (
	"content-management-system/src/controllers"
	"content-management-system/src/utils"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func SetupPageRoutes(router gin.IRouter, db *gorm.DB) {
	// Mutations require an admin JWT — previously these were unauthenticated,
	// letting anyone create/overwrite/delete pages.
	admin := utils.AdminAuthMiddleware(db)
	router.POST("/pages", admin, controllers.CreatePage)
	router.GET("/pages", controllers.GetPages)
	router.GET("/pages/:id", controllers.GetPage)
	router.PUT("/pages/:id", admin, controllers.UpdatePage)
	router.DELETE("/pages/:id", admin, controllers.DeletePage)
}
