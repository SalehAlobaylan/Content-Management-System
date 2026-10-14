package routes

import (
	"content-management-system/src/controllers"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// SetupExperienceRoutes registers the public RUX telemetry ingestion endpoint.
//
// This is NOT mounted on the admin group and NOT on /internal. It is a dedicated
// public-write path: Web uses its server-only BFF ingest token; native uses a
// short-lived, process/platform/build-bound telemetry admission ticket. Admin read/mutation surfaces
// (verdicts, incidents, policy) register separately on the admin group.
func SetupExperienceRoutes(v1 *gin.RouterGroup, db *gorm.DB) {
	v1.POST("/experience/native/admission", controllers.CreateNativeExperienceAdmission)
	v1.POST("/experience/native/events", controllers.NativeExperienceAdmissionMiddleware(), controllers.IngestExperienceEvents)
	rux := v1.Group("/experience")
	rux.Use(controllers.RuxIngestAuthMiddleware())
	rux.POST("/events", controllers.IngestExperienceEvents)
}
