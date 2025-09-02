package routes

import (
	"content-management-system/src/controllers"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func SetupPostRoutes(router gin.IRouter, db *gorm.DB) {
	router.GET("/posts", controllers.GetPosts)
	router.POST("/posts", controllers.CreatePost)
	router.GET("/posts/:id", controllers.GetPost)
	router.PUT("/posts/:id", controllers.UpdatePost)
	router.DELETE("/posts/:id", controllers.DeletePost)
}
