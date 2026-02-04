package routes

import (
	"content-management-system/src/controllers"
	"content-management-system/src/utils"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// SetupAdminAuthRoutes registers admin auth routes
func SetupAdminAuthRoutes(router *gin.Engine, db *gorm.DB) {
	router.POST("/admin/login", controllers.AdminLogin)

	adminGroup := router.Group("/admin")
	adminGroup.Use(utils.AdminAuthMiddleware(db))
	adminGroup.GET("/me", controllers.AdminMe)
}
