package routes

import (
	"content-management-system/src/controllers"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func SetupPageRoutes(router gin.IRouter, db *gorm.DB) {
	router.POST("/pages", controllers.CreatePage)
	router.GET("/pages/:id", controllers.GetPage)
	router.PUT("/pages/:id", controllers.UpdatePage)
	router.DELETE("/pages/:id", controllers.DeletePage)
}
