package routes
import (

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func SetupRoutes(router *gin.Engine, db *gorm.DB) {
	SetupPostRoutes(router, db)
	SetupMediaRoutes(router, db)
	SetupPageRoutes(router, db)
}


