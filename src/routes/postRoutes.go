package routes

import (
	"content-management-system/src/controllers"
	"content-management-system/src/utils"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func SetupPostRoutes(router gin.IRouter, db *gorm.DB) {
	// Mutations require an admin JWT — previously these were unauthenticated,
	// letting anyone create/overwrite/delete posts.
	admin := utils.AdminAuthMiddleware(db)
	router.GET("/posts", controllers.GetPosts)
	router.POST("/posts", admin, utils.RequireAdminPermission("content", "write"), controllers.CreatePost)
	router.GET("/posts/:id", controllers.GetPost)
	router.PUT("/posts/:id", admin, utils.RequireAdminPermission("content", "write"), controllers.UpdatePost)
	router.DELETE("/posts/:id", admin, utils.RequireAdminPermission("content", "delete"), controllers.DeletePost)
}
