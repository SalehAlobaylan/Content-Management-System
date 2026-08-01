package routes

import (
	"content-management-system/src/controllers"
	"content-management-system/src/utils"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// SetupOperatorRoutes is called only from the already-authenticated CMS admin
// group. Every handler requires an explicit tenant claim and derives current
// authority from durable CMS capability controls; legacy launch-mode fields
// are never a runtime authorization gate.
func SetupOperatorRoutes(adminGroup *gin.RouterGroup, _ *gorm.DB) {
	operatorGroup := adminGroup.Group("/operator")
	operatorGroup.Use(utils.RequireAdminRole("admin"))
	operatorGroup.GET("/status", controllers.GetOperatorStatus)
	operatorGroup.POST("/investigations", controllers.CreateOperatorInvestigation)
	operatorGroup.POST("/investigations/:id/cancel", controllers.CancelOperatorInvestigation)
	operatorGroup.GET("/investigations/:id/events", controllers.ListOperatorInvestigationEvents)
	operatorGroup.GET("/investigations/:id/eligible-actions", controllers.ListOperatorEligibleActions)
	operatorGroup.GET("/inbox", controllers.ListOperatorInbox)
	operatorGroup.GET("/recommendations", controllers.ListOperatorRecommendations)
	operatorGroup.POST("/investigations/:id/read", controllers.MarkOperatorInboxRead)
	operatorGroup.GET("/threads", controllers.ListOperatorThreads)
	operatorGroup.POST("/threads", controllers.CreateOperatorThread)
	operatorGroup.GET("/threads/:id", controllers.GetOperatorThread)
	operatorGroup.PATCH("/threads/:id", controllers.UpdateOperatorThread)
	operatorGroup.DELETE("/threads/:id", controllers.DeleteOperatorThread)
	operatorGroup.POST("/threads/:id/messages", controllers.AppendOperatorThreadMessage)
	operatorGroup.GET("/investigations/:id/shared-events", controllers.ListSharedOperatorInvestigationEvents)
	operatorGroup.GET("/investigations/:id/shares", controllers.ListOperatorInvestigationShares)
	operatorGroup.POST("/plans", controllers.CreateOperatorPlan)
	operatorGroup.GET("/plans/:id", controllers.GetOperatorPlan)
	operatorGroup.GET("/plans/:id/events", controllers.ListOperatorPlanEvents)
	operatorGroup.POST("/plans/:id/approve", controllers.ApproveOperatorPlan)
	operatorGroup.POST("/plans/:id/cancel", controllers.CancelOperatorPlan)
	// Operator-originated writes have one public mutation surface: signed action
	// plans. Native Console pages retain their own service-local workflows, but
	// the old direct schedule/share/feedback/control shortcuts are deliberately
	// not registered here and cannot bypass approval + the CMS worker.
	operatorGroup.GET("/schedules", controllers.ListOperatorSchedules)
	operatorGroup.GET("/schedules/:id", controllers.GetOperatorSchedule)
	operatorGroup.GET("/schedules/:id/runs", controllers.ListOperatorScheduleRuns)
	operatorGroup.GET("/controls", controllers.GetOperatorControls)
}
