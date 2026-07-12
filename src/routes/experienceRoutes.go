package routes

import (
	"content-management-system/src/controllers"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// SetupExperienceRoutes registers the public RUX telemetry ingestion endpoint.
//
// This is NOT mounted on the admin group and NOT on /internal. It is a dedicated
// public-write path guarded by the RUX ingest token, which the Wahb-Platform BFF
// attaches server-side (browsers never hold it). Admin read/mutation surfaces
// (verdicts, incidents, policy) register separately on the admin group.
func SetupExperienceRoutes(v1 *gin.RouterGroup, db *gorm.DB) {
	rux := v1.Group("/experience")
	rux.Use(controllers.RuxIngestAuthMiddleware())
	rux.POST("/events", controllers.IngestExperienceEvents)
}
