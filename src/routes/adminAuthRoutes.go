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
	adminGroup.GET("/users", controllers.ListAdminUsers)
	adminGroup.POST("/users", controllers.CreateAdminUser)
	adminGroup.GET("/users/:id", controllers.GetAdminUser)
	adminGroup.PUT("/users/:id", controllers.UpdateAdminUser)
	adminGroup.DELETE("/users/:id", controllers.DeleteAdminUser)
	adminGroup.POST("/users/:id/password", controllers.ResetAdminUserPassword)
}
